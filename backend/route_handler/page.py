from uuid import UUID

from fastapi import Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import (
    DALAssets,
    DALPages,
    DALPagesAssetsRel,
    DAOPagesAssetsRelUpdate,
    DAOPagesUpdate,
    FilterOp,
    OrderDirection,
    locked_row_by_id,
    safe_commit,
    safe_transaction,
)
from backend.db.data_models import DAOPages
from backend.db.externals import (
    AssetsOverviewResponse,
    PagesAssetsRelOverviewResponse,
    PagesOverviewResponse,
)
from backend.db.utils.common import retrieve_available_asset_key_in_order_of
from backend.lib.asset_manager.base import AssetManager
from backend.route_handler.base import RouteHandler

from .base import enforce_response_model


class PageEditUserMessageRequest(BaseModel):
    user_message: str


class PhotoSlotItem(BaseModel):
    assetRelId: UUID
    assetId: UUID
    order: int


class PatchPhotoSlotsRequest(BaseModel):
    slots: list[PhotoSlotItem]


class PagesFullResponse(PagesOverviewResponse):
    assets: list[AssetsOverviewResponse]
    page_asset_rels: list[PagesAssetsRelOverviewResponse]

    @classmethod
    async def rendered_from_daos(
        cls,
        pages: list[DAOPages],
        db_session: AsyncSession,
        asset_manager: AssetManager,
    ) -> list["PagesFullResponse"]:
        page_ids = [page.id for page in pages]
        page_asset_rels = await DALPagesAssetsRel.list_all(
            db_session,
            filters={"page_id": (FilterOp.IN, page_ids)},
            order_by=[("order_index", OrderDirection.ASC)],
        )

        # Step 4: Collect all asset_ids used
        asset_ids = [rel.asset_id for rel in page_asset_rels if rel.asset_id]
        asset_list = await DALAssets.get_by_ids(db_session, asset_ids)
        assets_by_id = {asset.id: asset for asset in asset_list}

        # Step 5: Generate signed URLs for original asset keys
        uuid_asset_keys_map = {
            asset.id: retrieve_available_asset_key_in_order_of(
                asset,
                [
                    "asset_key_display",
                    "asset_key_original",
                    "asset_key_llm",
                ],
            )
            for asset in asset_list
        }
        signed_urls = await asset_manager.generate_signed_urls_batched(
            list(uuid_asset_keys_map.values())
        )

        # Step 6: Assemble response
        page_id_to_assets: dict[UUID, list[AssetsOverviewResponse]] = {}
        page_id_to_asset_rels: dict[UUID, list[PagesAssetsRelOverviewResponse]] = {}

        for rel in page_asset_rels:
            if rel.page_id and rel.asset_id:
                asset = assets_by_id[rel.asset_id]
                signed_url = signed_urls.get(uuid_asset_keys_map[asset.id])
                # Inject signed URL into the model
                asset_with_url = AssetsOverviewResponse(
                    **asset.model_dump(),
                    signed_asset_url=(
                        signed_url if isinstance(signed_url, str) else ""
                    ),
                )

                page_id_to_assets.setdefault(rel.page_id, []).append(asset_with_url)
                page_id_to_asset_rels.setdefault(rel.page_id, []).append(
                    PagesAssetsRelOverviewResponse.from_dao(rel)
                )
        return [
            cls(
                **PagesOverviewResponse.from_dao(page).model_dump(),
                assets=page_id_to_assets.get(page.id, []),
                page_asset_rels=page_id_to_asset_rels.get(page.id, []),
            )
            for page in pages
        ]


class PageAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.route(
            "/api/pages/{page_id}/user_message", "page_edit_user_message", ["POST"]
        )
        self.route(
            "/api/pages/{page_id}/photo_slots",
            "page_patch_photo_slots",
            ["PATCH"],
        )

    @enforce_response_model
    async def page_edit_user_message(
        self,
        request: Request,
        page_id: UUID,
        payload: PageEditUserMessageRequest,
    ) -> PagesOverviewResponse:
        async with self.app.new_db_session() as db_session:
            request_context = await self.get_request_context(request)
            page = await self.get_page_assert_owned_by_user(
                db_session, page_id, request_context.user_id
            )

            async with safe_commit(db_session):
                updated_page = await DALPages.update_by_id(
                    db_session,
                    page.id,
                    DAOPagesUpdate(user_message=payload.user_message),
                )
            return PagesOverviewResponse.from_dao(updated_page)

    @enforce_response_model
    async def page_patch_photo_slots(
        self,
        request: Request,
        page_id: UUID,
        payload: PatchPhotoSlotsRequest,
    ) -> PagesFullResponse:
        async with self.app.new_db_session() as db_session:
            request_context = await self.get_request_context(request)

            # All mutations + lock happen in one transaction
            async with safe_transaction(db_session, context="patch photo slots"):
                await self.get_page_assert_owned_by_user(
                    db_session, page_id, request_context.user_id
                )

                # 1) Lock the page row FOR UPDATE
                async with locked_row_by_id(db_session, DAOPages, page_id) as page:
                    # # Not checking against revision for now
                    # if payload.expectedPageRevision != page.revision:
                    #     raise HTTPException(
                    #         status_code=409,
                    #         detail=(
                    #             f"Revision mismatch: client had {payload.expectedPageRevision}, "
                    #             f"current is {page.revision}. Please reload."
                    #         ),
                    #     )
                    something_changed = False

                    # 2a) Delete any dropped rels
                    existing = await DALPagesAssetsRel.list_all(
                        db_session, filters={"page_id": (FilterOp.EQ, page.id)}
                    )
                    incoming_ids = {slot.assetRelId for slot in payload.slots}
                    to_delete = [
                        rel.id for rel in existing if rel.id not in incoming_ids
                    ]
                    if to_delete:
                        something_changed = True
                        await DALPagesAssetsRel.delete_many_by_ids(
                            db_session, to_delete
                        )

                    # 2c) REORDER all provided slots in one batch
                    updates = {
                        slot.assetRelId: DAOPagesAssetsRelUpdate(order_index=slot.order)
                        for slot in payload.slots
                    }
                    if updates:
                        something_changed = True
                        await DALPagesAssetsRel.update_many_by_ids(db_session, updates)

                    # 2d) Bump the page revision (optional)
                    if something_changed:
                        page.revision += 1
                        await db_session.flush()

            # 3) Return the fresh, fully-rendered page
            full = await PagesFullResponse.rendered_from_daos(
                [page], db_session, self.app.asset_manager
            )
            return full[0]
