from typing import Any, Optional

from pydantic import BaseModel


class PageMessageOption(BaseModel):
    tone: str
    message: str


class PageSchema(BaseModel):
    page_photos: list[str]
    page_message: PageMessageOption
    page_message_alternatives: list[PageMessageOption]

    @classmethod
    def get_page_message_alternatives_key(cls) -> str:
        return "page_message_alternatives"

    @classmethod
    def serialize_page_message_alternatives(
        cls, page_message_alternatives: list[PageMessageOption]
    ) -> dict[str, list[dict[str, str]]]:
        return {
            cls.get_page_message_alternatives_key(): [
                alt.model_dump() for alt in page_message_alternatives
            ]
        }

    @classmethod
    def deserialize_page_message_alternatives(
        cls, serialized_page_message_alternatives: Optional[dict[str, Any]]
    ) -> Optional[list[PageMessageOption]]:
        if not serialized_page_message_alternatives:
            return None

        key = cls.get_page_message_alternatives_key()
        if key not in serialized_page_message_alternatives:
            return None

        alternatives = serialized_page_message_alternatives[key]
        return [PageMessageOption.model_validate(alt) for alt in alternatives]


class ExtractedExif(BaseModel):
    make: str
    model: str
    datetime_original: str
    iso: int
    exposure_time: float  # seconds
    fnumber: float  # f-stop
    focal_length: float  # mm
    gps_latitude: Optional[float] = None  # decimal degrees
    gps_longitude: Optional[float] = None  # decimal degrees


class AssetMetadata(BaseModel):
    exif_radar_formatted_address: Optional[str] = None
    exif_radar_place_label: Optional[str] = None
    exif_radar_state_code: Optional[str] = None
    exif_radar_country_code: Optional[str] = None
