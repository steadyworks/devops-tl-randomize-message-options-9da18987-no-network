import logging
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel

from backend.db.dal import (
    DALPhotobookBookmarks,
    DALPhotobooks,
    DALPhotobookShare,
    DALUsers,
    DAOPhotobookBookmarksCreate,
    DAOUsers,
    FilterOp,
    OrderDirection,
    safe_commit,
)
from backend.db.data_models import DAOPhotobookBookmarks, DAOPhotobooks, PhotobookStatus
from backend.db.externals import (
    PhotobookBookmarksOverviewResponse,
    PhotobooksOverviewResponse,
)
from backend.lib.utils.common import none_throws
from backend.route_handler.base import RouteHandler

from .base import enforce_response_model

if TYPE_CHECKING:
    from backend.lib.request.context import RequestContext


class UserBookmarkPhotobookInputPayload(BaseModel):
    photobook_id: UUID
    source_analytics: Optional[str] = None


class UserGetPhotobooksResponse(BaseModel):
    photobooks: list[PhotobooksOverviewResponse]


class UserGetSharedWithMePhotobooksResponse(BaseModel):
    photobooks: list[PhotobooksOverviewResponse]


class UserBookmarkPhotobookDeleteResponse(BaseModel):
    success: bool
    error_message: Optional[str] = None


class UserAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.route(
            "/api/user/photobooks",
            "user_get_photobooks",
            methods=["GET"],
        )
        self.route(
            "/api/user/photobooks/bookmarks",
            "user_get_bookmarked_photobooks",
            methods=["GET"],
        )
        self.route(
            "/api/user/photobooks/bookmark_new",
            "user_photobook_bookmark_new",
            methods=["POST"],
        )
        self.route(
            "/api/user/photobooks/bookmark_remove/{photobook_id}",
            "user_photobook_bookmark_remove",
            methods=["DELETE"],
        )
        self.route(
            "/api/user/shared_with_me",
            "get_shared_with_me_photobooks",
            ["GET"],
        )

    @enforce_response_model
    async def user_get_photobooks(
        self,
        request: Request,
    ) -> UserGetPhotobooksResponse:
        request_context: RequestContext = await self.get_request_context(request)

        async with self.app.new_db_session() as db_session:
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
            resp = UserGetPhotobooksResponse(
                photobooks=await PhotobooksOverviewResponse.rendered_from_daos(
                    photobooks, db_session, self.app.asset_manager
                )
            )
            return resp

    @enforce_response_model
    async def user_get_bookmarked_photobooks(
        self,
        request: Request,
    ) -> UserGetPhotobooksResponse:
        request_context: RequestContext = await self.get_request_context(request)

        async with self.app.new_db_session() as db_session:
            photobook_bookmarks: list[
                DAOPhotobookBookmarks
            ] = await DALPhotobookBookmarks.list_all(
                db_session,
                {"user_id": (FilterOp.EQ, request_context.user_id)},
                order_by=[("created_at", OrderDirection.DESC)],
            )
            photobooks: list[DAOPhotobooks] = await DALPhotobooks.list_all(
                db_session,
                filters={
                    "id": (
                        FilterOp.IN,
                        [bookmark.photobook_id for bookmark in photobook_bookmarks],
                    ),
                    "status": (
                        FilterOp.NOT_IN,
                        [
                            PhotobookStatus.DELETED,
                            PhotobookStatus.PERMANENTLY_DELETED,
                        ],
                    ),
                },
            )
            return UserGetPhotobooksResponse(
                photobooks=await PhotobooksOverviewResponse.rendered_from_daos(
                    photobooks, db_session, self.app.asset_manager
                )
            )

    @enforce_response_model
    async def user_photobook_bookmark_new(
        self,
        request: Request,
        payload: UserBookmarkPhotobookInputPayload,
    ) -> PhotobookBookmarksOverviewResponse:
        request_context: RequestContext = await self.get_request_context(request)

        async with self.app.new_db_session() as db_session:
            async with safe_commit(db_session):
                dao: DAOPhotobookBookmarks = await DALPhotobookBookmarks.create(
                    db_session,
                    DAOPhotobookBookmarksCreate(
                        user_id=request_context.user_id,
                        photobook_id=payload.photobook_id,
                        source=payload.source_analytics,
                    ),
                )
            return PhotobookBookmarksOverviewResponse.from_dao(dao)

    @enforce_response_model
    async def user_photobook_bookmark_remove(
        self,
        request: Request,
        photobook_id: UUID,
    ) -> UserBookmarkPhotobookDeleteResponse:
        request_context = await self.get_request_context(request)

        async with self.app.new_db_session() as db_session:
            try:
                bookmarks = await DALPhotobookBookmarks.list_all(
                    db_session,
                    filters={
                        "user_id": (FilterOp.EQ, request_context.user_id),
                        "photobook_id": (FilterOp.EQ, photobook_id),
                    },
                    limit=1,
                )

                if not bookmarks:
                    return UserBookmarkPhotobookDeleteResponse(
                        success=False,
                        error_message="Bookmark not found.",
                    )

                dao = bookmarks[0]

                async with safe_commit(db_session):
                    await DALPhotobookBookmarks.delete_by_id(db_session, dao.id)

                return UserBookmarkPhotobookDeleteResponse(success=True)
            except Exception as e:
                logging.exception(f"Failed to remove bookmark: {e}")
                return UserBookmarkPhotobookDeleteResponse(
                    success=False,
                    error_message="An unexpected error occurred.",
                )

    @enforce_response_model
    async def get_shared_with_me_photobooks(
        self, request: Request
    ) -> UserGetSharedWithMePhotobooksResponse:
        request_context: RequestContext = await self.get_request_context(request)
        async with self.app.new_db_session() as db_session:
            current_user: DAOUsers = none_throws(
                await DALUsers.get_by_id(db_session, request_context.user_id)
            )
            books_invited_by_user_id = await DALPhotobookShare.list_all(
                db_session,
                filters={
                    "invited_user_id": (FilterOp.EQ, current_user.id),
                },
            )
            books_invited_by_email = await DALPhotobookShare.list_all(
                db_session,
                filters={
                    "email": (FilterOp.EQ, current_user.email),
                },
            )
            book_ids_shared_with_me: set[UUID] = set(
                book.photobook_id
                for book in books_invited_by_user_id + books_invited_by_email
            )
            photobooks: list[DAOPhotobooks] = await DALPhotobooks.get_by_ids(
                db_session,
                list(book_ids_shared_with_me),
            )
            live_photobooks = [
                book
                for book in photobooks
                if book.status
                not in [
                    PhotobookStatus.DELETED,
                    PhotobookStatus.PERMANENTLY_DELETED,
                ]
            ]
            return UserGetSharedWithMePhotobooksResponse(
                photobooks=await PhotobooksOverviewResponse.rendered_from_daos(
                    live_photobooks, db_session, self.app.asset_manager
                )
            )
