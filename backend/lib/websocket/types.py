from enum import Enum
from typing import Literal, Union
from uuid import UUID

from pydantic import BaseModel

# ---- WebSocket Event Types ----


class WebSocketEventType(str, Enum):
    # Client → Server
    ASSET_UPLOAD_STATUS_UPDATE = "asset_upload_status_update"

    # Server → Client
    ASSET_REJECTED_INVALID_MIME = "asset_rejected_invalid_mime"
    ASSET_REJECTED_CORRUPT = "asset_rejected_corrupt"
    ASSET_FAILED_PERMANENTLY = "asset_failed_permanently"


# ---- Client → Server Payloads ----


class AssetUploadStatusFailed(BaseModel):
    asset_id: UUID
    error_msg: str


class AssetUploadStatusPayload(BaseModel):
    succeeded: list[UUID]
    failed: list[AssetUploadStatusFailed]


class AssetUploadStatusMessage(BaseModel):
    event: Literal[WebSocketEventType.ASSET_UPLOAD_STATUS_UPDATE]
    payload: AssetUploadStatusPayload


ClientToServerMessageUnion = Union[AssetUploadStatusMessage,]


class ClientToServerEnvelope(BaseModel):
    event: WebSocketEventType
    payload: AssetUploadStatusPayload

    model_config = {"json_schema_extra": {"discriminator": "event"}}


# ---- Server → Client Payloads ----


class AssetRejectedInvalidMIMEPayload(BaseModel):
    image_id: UUID
    message: str = "Unsupported MIME type"


class AssetRejectedCorruptPayload(BaseModel):
    image_id: UUID
    message: str = "Corrupt or unreadable image file"


class AssetFailedPermanentlyPayload(BaseModel):
    image_id: UUID
    message: str
    retryable: bool = False


# ---- Server → Client Envelopes ----


class AssetRejectedInvalidMIMEMessage(BaseModel):
    event: Literal[WebSocketEventType.ASSET_REJECTED_INVALID_MIME]
    payload: AssetRejectedInvalidMIMEPayload


class AssetRejectedCorruptMessage(BaseModel):
    event: Literal[WebSocketEventType.ASSET_REJECTED_CORRUPT]
    payload: AssetRejectedCorruptPayload


class AssetFailedPermanentlyMessage(BaseModel):
    event: Literal[WebSocketEventType.ASSET_FAILED_PERMANENTLY]
    payload: AssetFailedPermanentlyPayload


ServerToClientMessageUnion = Union[
    AssetRejectedInvalidMIMEMessage,
    AssetRejectedCorruptMessage,
    AssetFailedPermanentlyMessage,
]


class ServerToClientEnvelope(BaseModel):
    event: WebSocketEventType
    payload: Union[
        AssetRejectedInvalidMIMEPayload,
        AssetRejectedCorruptPayload,
        AssetFailedPermanentlyPayload,
    ]

    model_config = {"json_schema_extra": {"discriminator": "event"}}
