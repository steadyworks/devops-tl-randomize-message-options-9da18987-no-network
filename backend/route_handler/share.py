from asyncio import gather
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import DALPhotobooks, DALPhotobookShare, DALUsers
from backend.db.dal.base import FilterOp, OrderDirection, safe_transaction
from backend.db.dal.schemas import DAOPhotobookShareCreate
from backend.db.data_models import (
    DAOPhotobooks,
    DAOPhotobookShare,
    DAOUsers,
    PhotobookStatus,
    ShareRole,
)

if TYPE_CHECKING:
    from backend.lib.request.context import RequestContext

from backend.route_handler.base import RouteHandler, enforce_response_model


class SharePhotobookRequest(BaseModel):
    raw_emails_to_share: list[str]
    invited_user_ids: list[UUID] = []
    custom_message: str = ""
    role: ShareRole = ShareRole.VIEWER


class AutoCompleteUser(BaseModel):
    email: Optional[str]
    username: Optional[str]
    user_id: UUID


class SharePhotobookResponse(BaseModel):
    already_shared_users: list[AutoCompleteUser]
    already_shared_emails: list[str]


class SharePhotobookAutocompleteResponse(BaseModel):
    users: list[AutoCompleteUser]
    raw_emails: list[str]
    already_shared_users: list[AutoCompleteUser]
    already_shared_emails: list[str]


class SharePhotobookRemoveRequest(BaseModel):
    email: str
    user_id: Optional[UUID] = None


class SharePhotobookRemoveResponse(BaseModel):
    already_shared_users: list[AutoCompleteUser]
    already_shared_emails: list[str]


class ShareAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.route(
            "/api/share/photobooks/{photobook_id}",
            "share_photobook",
            methods=["POST"],
        )
        self.route(
            "/api/share/get_share_autocomplete_options/{photobook_id}",
            "get_share_autocomplete_options",
            methods=["GET"],
        )
        self.route(
            "/api/share/remove_share/{photobook_id}",
            "remove_share",
            methods=["POST"],
        )

    async def _find_autocomplete_user_from_id(
        self,
        user_id: UUID,
        db_session: AsyncSession,
    ) -> Optional[AutoCompleteUser]:
        user: Optional[DAOUsers] = await DALUsers.get_by_id(db_session, user_id)
        if user:
            return AutoCompleteUser(
                email=user.email,
                username=user.name,
                user_id=user.id,
            )
        return None

    @enforce_response_model
    async def remove_share(
        self,
        photobook_id: UUID,
        request: Request,
        payload: SharePhotobookRemoveRequest,
    ) -> SharePhotobookRemoveResponse:
        request_context: RequestContext = await self.get_request_context(request)
        user_id: UUID = request_context.user_id
        async with self.app.new_db_session() as db_session:
            # Validate photobook ownership
            photobook: DAOPhotobooks | None = await DALPhotobooks.get_by_id(
                db_session, photobook_id
            )
            if not photobook or photobook.user_id != user_id:
                raise RuntimeError("Photobook not found or access denied")

            shares: list[DAOPhotobookShare] = []
            # Remove all shares for the photobook
            if payload.user_id:
                # Remove share by user ID
                shares = await DALPhotobookShare.list_all(
                    db_session,
                    filters={
                        "photobook_id": (FilterOp.EQ, photobook_id),
                        "invited_user_id": (FilterOp.EQ, payload.user_id),
                    },
                )
                if len(shares) > 0:
                    await DALPhotobookShare.delete_by_id(db_session, shares[0].id)
            else:
                # Remove share by email
                shares = await DALPhotobookShare.list_all(
                    db_session,
                    filters={
                        "photobook_id": (FilterOp.EQ, photobook_id),
                        "email": (FilterOp.EQ, payload.email),
                    },
                )
                if len(shares) > 0:
                    await DALPhotobookShare.delete_by_id(db_session, shares[0].id)
            # Fetch all shares for the photobook to return
            all_shares: list[DAOPhotobookShare] = await DALPhotobookShare.list_all(
                db_session,
                filters={"photobook_id": (FilterOp.EQ, photobook_id)},
            )
            return SharePhotobookRemoveResponse(
                already_shared_users=[
                    user
                    for user in await gather(
                        *[
                            self._find_autocomplete_user_from_id(
                                share.invited_user_id,
                                db_session=db_session,
                            )
                            for share in all_shares
                            if share.invited_user_id
                        ]
                    )
                    if user is not None
                ],
                already_shared_emails=[
                    share.email for share in all_shares if share.email
                ],
            )

    @enforce_response_model
    async def get_share_autocomplete_options(
        self,
        photobook_id: UUID,
        request: Request,
    ) -> SharePhotobookAutocompleteResponse:
        """
        Fetch the history of everyone the current user has shared photobooks with.
        """
        request_context: RequestContext = await self.get_request_context(request)
        async with self.app.new_db_session() as db_session:
            # fetch everything the current user has shared for auto complete
            photobooks: list[DAOPhotobooks] = await DALPhotobooks.list_all(
                db_session,
                {
                    "user_id": (FilterOp.EQ, request_context.user_id),
                    "status": (
                        FilterOp.NOT_IN,
                        [
                            PhotobookStatus.DELETED,
                            PhotobookStatus.PERMANENTLY_DELETED,
                        ],
                    ),
                },
                order_by=[("updated_at", OrderDirection.DESC)],
            )
            current_user_all_photobook_shares: list[
                DAOPhotobookShare
            ] = await DALPhotobookShare.list_all(
                db_session,
                filters={
                    "photobook_id": (
                        FilterOp.IN,
                        [pb.id for pb in photobooks],
                    )
                },
            )
            raw_emails: list[str] = [
                share.email
                for share in current_user_all_photobook_shares
                if share.email
            ]
            users_with_none: list[Optional[AutoCompleteUser]] = await gather(
                *[
                    self._find_autocomplete_user_from_id(
                        share.invited_user_id, db_session=db_session
                    )
                    for share in current_user_all_photobook_shares
                    if share.invited_user_id
                ]
            )
            # current photobook shares
            current_photobook_shares: list[
                DAOPhotobookShare
            ] = await DALPhotobookShare.list_all(
                db_session,
                filters={"photobook_id": (FilterOp.EQ, photobook_id)},
            )
            current_photobook_emails: list[str] = [
                share.email for share in current_photobook_shares if share.email
            ]
            current_photobook_viewers_with_none: list[
                Optional[AutoCompleteUser]
            ] = await gather(
                *[
                    self._find_autocomplete_user_from_id(
                        share.invited_user_id, db_session=db_session
                    )
                    for share in current_photobook_shares
                    if share.invited_user_id
                ]
            )

            return SharePhotobookAutocompleteResponse(
                users=[user for user in users_with_none if user is not None],
                raw_emails=raw_emails,
                already_shared_users=[
                    user
                    for user in current_photobook_viewers_with_none
                    if user is not None
                ],
                already_shared_emails=current_photobook_emails,
            )

    @enforce_response_model
    async def share_photobook(
        self,
        photobook_id: UUID,
        request: Request,
        payload: SharePhotobookRequest,
    ) -> SharePhotobookResponse:
        request_context: RequestContext = await self.get_request_context(request)
        user_id: UUID = request_context.user_id
        async with self.app.new_db_session() as db_session:
            async with safe_transaction(
                db_session, context="share photobook", raise_on_fail=True
            ):
                # Validate photobook ownership
                photobook: DAOPhotobooks | None = await DALPhotobooks.get_by_id(
                    db_session, photobook_id
                )
                if not photobook or photobook.user_id != user_id:
                    raise RuntimeError("Photobook not found or access denied")

                # let's process emails by checking if they belong to existing users
                existing_users: list[DAOUsers] = await DALUsers.list_all(
                    db_session,
                    filters={"email": (FilterOp.IN, payload.raw_emails_to_share)},
                )
                raw_emails = set(payload.raw_emails_to_share) - set(
                    [user.email for user in existing_users]
                )
                for email in raw_emails:
                    await DALPhotobookShare.create(
                        db_session,
                        DAOPhotobookShareCreate(
                            photobook_id=photobook_id,
                            email=email,
                            invited_user_id=None,  # Assuming email sharing
                            role=payload.role,
                            custom_message=payload.custom_message,
                        ),
                    )
                for user_id in set(
                    payload.invited_user_ids + [u.id for u in existing_users]
                ):
                    await DALPhotobookShare.create(
                        db_session,
                        DAOPhotobookShareCreate(
                            photobook_id=photobook_id,
                            email=None,  # Assuming user ID sharing
                            invited_user_id=user_id,
                            role=payload.role,
                            custom_message=payload.custom_message,
                        ),
                    )
            # current photobook shares
            current_photobook_shares: list[
                DAOPhotobookShare
            ] = await DALPhotobookShare.list_all(
                db_session,
                filters={"photobook_id": (FilterOp.EQ, photobook_id)},
            )
            current_photobook_emails: list[str] = [
                share.email for share in current_photobook_shares if share.email
            ]
            current_photobook_viewers_with_none: list[
                Optional[AutoCompleteUser]
            ] = await gather(
                *[
                    self._find_autocomplete_user_from_id(
                        share.invited_user_id, db_session=db_session
                    )
                    for share in current_photobook_shares
                    if share.invited_user_id
                ]
            )

            return SharePhotobookResponse(
                already_shared_users=[
                    user
                    for user in current_photobook_viewers_with_none
                    if user is not None
                ],
                already_shared_emails=current_photobook_emails,
            )
