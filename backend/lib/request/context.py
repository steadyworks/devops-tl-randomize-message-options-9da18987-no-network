import logging
from typing import Optional, Self
from uuid import UUID, uuid4

from fastapi import HTTPException, Request, WebSocket, WebSocketException
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import (
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
    WS_1008_POLICY_VIOLATION,
)

from backend.db.data_models import DAOUsers
from backend.env_loader import EnvLoader

SUPABASE_JWT_SECRET = EnvLoader.get("SUPABASE_JWT_SECRET")
SUPABASE_JWT_ALGO = "HS256"

if not SUPABASE_JWT_SECRET:
    raise RuntimeError("Missing SUPABASE_JWT_SECRET env var")


class SupabaseJWTClaims(BaseModel):
    sub: str  # maps to auth.users.id and public.users.id (UUID)
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    role: str
    iss: Optional[str] = None
    # You can extend with app_metadata, user_metadata if needed


class RequestContext:
    def __init__(
        self,
        claims: SupabaseJWTClaims,
        raw_token: str,
        user_row: Optional[DAOUsers] = None,
        request_id: UUID = uuid4(),
    ) -> None:
        self._claims = claims
        self._raw_token = raw_token
        self._user_row = user_row
        self._request_id = request_id

    @property
    def user_id(self) -> UUID:
        return UUID(self._claims.sub)

    @property
    def email(self) -> Optional[str]:
        return self._user_row.email if self._user_row else self._claims.email

    @property
    def role(self) -> str:
        return self._user_row.role if self._user_row else self._claims.role

    @property
    def name(self) -> Optional[str]:
        return self._user_row.name if self._user_row else None

    @property
    def user(self) -> Optional[DAOUsers]:
        return self._user_row

    @property
    def request_id(self) -> UUID:
        return self._request_id

    @classmethod
    async def from_request(
        cls,
        request: Request,
        db_session: Optional[AsyncSession] = None,
    ) -> Self:
        if hasattr(request.state, "ctx"):
            return request.state.ctx

        # If already present, reuse; otherwise generate and attach
        if not hasattr(request.state, "request_id"):
            request.state.request_id = uuid4()

        request_id: UUID = request.state.request_id

        # Step 1: Parse Authorization header
        auth_header = request.headers.get("authorization")
        if not auth_header or not auth_header.lower().startswith("bearer "):
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid Authorization header",
            )

        token = auth_header.removeprefix("Bearer ").strip()

        # Step 2: Decode JWT
        try:
            decoded = jwt.decode(
                token,
                SUPABASE_JWT_SECRET,
                algorithms=[SUPABASE_JWT_ALGO],
                audience="authenticated",
            )
        except JWTError as e:
            logging.warning("JWT decode failed: %s", str(e))
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token",
            )

        # Step 3: Parse into strongly typed claims
        try:
            claims = SupabaseJWTClaims.model_validate(decoded)
        except ValidationError as e:
            logging.error("JWT claims validation failed: %s", e)
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail="Malformed JWT claims",
            )

        # Step 4: Optionally load public.users
        user_row = None
        if db_session is not None:
            from backend.db.dal import DALUsers  # avoid circular imports

            try:
                user_row = await DALUsers.get_by_id(db_session, UUID(claims.sub))
            except Exception as e:
                logging.warning("Failed to load user from public.users: %s", e)

        ctx = cls(
            claims=claims, raw_token=token, user_row=user_row, request_id=request_id
        )
        request.state.ctx = ctx
        return ctx

    @classmethod
    async def from_websocket(
        cls,
        websocket: WebSocket,
        db_session: Optional[AsyncSession] = None,
    ) -> Self:
        """
        Extracts and verifies JWT token from WebSocket connection.
        Optionally loads user from DB if session is provided.
        """
        request_id = uuid4()

        # Step 1: Extract token from query param or Authorization header
        token = websocket.query_params.get("token")
        if not token:
            auth_header = websocket.headers.get("authorization")
            if auth_header and auth_header.lower().startswith("bearer "):
                token = auth_header.removeprefix("Bearer ").strip()

        if not token:
            logging.warning("[WS][%s] Missing token", request_id)
            raise WebSocketException(
                code=WS_1008_POLICY_VIOLATION,
                reason="Missing authentication token",
            )

        # Step 2: Decode JWT
        try:
            decoded = jwt.decode(
                token,
                SUPABASE_JWT_SECRET,
                algorithms=[SUPABASE_JWT_ALGO],
                audience="authenticated",
            )
        except JWTError as e:
            logging.warning("[WS][%s] JWT decode failed: %s", request_id, str(e))
            raise WebSocketException(
                code=WS_1008_POLICY_VIOLATION,
                reason="Invalid authentication token",
            )

        # Step 3: Parse claims
        try:
            claims = SupabaseJWTClaims.model_validate(decoded)
        except ValidationError as e:
            logging.error("[WS][%s] JWT claims validation failed: %s", request_id, e)
            raise WebSocketException(
                code=WS_1008_POLICY_VIOLATION,
                reason="Malformed JWT claims",
            )

        # Step 4: Load user from DB if available
        user_row = None
        if db_session:
            from backend.db.dal import DALUsers  # avoid circular import

            try:
                user_row = await DALUsers.get_by_id(db_session, UUID(claims.sub))
            except Exception as e:
                logging.warning(
                    "[WS][%s] Failed to load user from public.users: %s",
                    request_id,
                    e,
                )

        # Step 5: Construct context
        return cls(
            claims=claims,
            raw_token=token,
            user_row=user_row,
            request_id=request_id,
        )
