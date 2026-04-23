import asyncio
import logging
from pathlib import Path
from typing import Any, Optional, cast

import magic
from google import genai
from google.genai import types
from google.genai.client import AsyncClient
from pydantic import BaseModel

from backend.db.data_models.types import AssetMetadata, ExtractedExif, PageSchema
from backend.env_loader import EnvLoader

RawLLMPrompt = str
SelectedPhotoFileNames = list[list[str]]


class PhotobookSchema(BaseModel):
    photobook_title: str
    photobook_pages: list[PageSchema]


class Gemini:
    DEFAULT_USER_INSTRUCTION = "Create a photobook to celebrate this memory!"

    def __init__(self) -> None:
        self.__client = genai.Client(
            vertexai=True,
            project=EnvLoader.get("GOOGLE_VERTEX_AI_PROJECT"),
            location="global",
        )
        self.model = "gemini-2.5-flash-lite-preview-06-17"

    def get_client(self) -> AsyncClient:
        return self.__client.aio

    @classmethod
    def _get_media_resolution(cls, num_photos: int) -> types.MediaResolution:
        if num_photos <= 10:
            return types.MediaResolution.MEDIA_RESOLUTION_MEDIUM
        elif num_photos <= 50:
            return types.MediaResolution.MEDIA_RESOLUTION_MEDIUM
        else:
            return types.MediaResolution.MEDIA_RESOLUTION_LOW

    @classmethod
    def _render_asset_metadata(cls, metadata: AssetMetadata) -> Optional[str]:
        if (
            metadata.exif_radar_place_label is None
            and metadata.exif_radar_country_code is None
            and metadata.exif_radar_formatted_address is None
            and metadata.exif_radar_state_code is None
        ):
            return None

        place_brief = (
            f"near/at {metadata.exif_radar_place_label}"
            if metadata.exif_radar_place_label
            else ""
        )
        place_full = f"{place_brief} {metadata.exif_radar_state_code or ''} {metadata.exif_radar_country_code or ''}".strip()

        if metadata.exif_radar_formatted_address:
            return f"{metadata.exif_radar_formatted_address.strip()} ({place_full})"
        return place_full

    def build_gemini_config_from_image_understanding_job(
        self,
        num_photos: int,
    ) -> types.GenerateContentConfig:
        thinking_budget = min(max(768, 100 * num_photos), 4096)

        sys_prompt = (
            f"""You are a friendly, observant, emotionally intelligent assistant helping craft a printed or digital photobook from a set of user-uploaded photos. Imagine you’re helping someone write messages for a meaningful gift — something warm, lasting, and deeply personal. Pay extra attention to the user instructions, if provided.

Your job is to turn a batch of photos and metadata into a story that is **alive, heartfelt, unmistakably human, and never too polished / over the top**. Some moments may be cozy or nostalgic, others light or playful — always natural, never forced. You'll receive an XML-style request containing 1–100 photos, with optional metadata (timestamps, location, user notes, event tags like “family trip” or “anniversary”).

---

## 🧠 Internal Thinking Phase (Plan First, {thinking_budget} token budget)

Before writing any captions, pause to reflect on the whole book. Keep notes **concise and purposeful** — you’re not narrating your thoughts, just collecting what will help you write vividly and emotionally.

### 1. Photo Detail Discovery

Make **quick, focused observations** using visual and metadata cues:

- **When and where is this?** Draw from visual cues and refer to address or approximate location / place provided by photo metadata. User-provided location may be slightly noisy, so cross-check against other photos, and infer rough areas and locations.
- **What’s happening?** Are they laughing, walking, eating, gazing at a view?
- **What’s the mood?** Joyful, still, chaotic, tender?
- **Who’s here?** Recurring faces? Pets? Friends, family? A couple?

**Examples**:

```
[0], [1]: near Dolores Park in San Francisco. Warm afternoon. Friends lounging on the grass — mellow and happy.  
[2], [4]: near Shibuya Crossing area — bustling with people at golden hour.  
[5]: Venice canal. At night, near Rialto Bridge. Glowing water. Snowy. Romantic stillness.
```

### 2. Grouping Photos into Pages

Create natural clusters of 2–6 photos per page (1–8 allowed):

- Every photo **must be picked at least once**, **ideally exactly once**.
- **Use timestamps to guide chronological flow. When possible, chronological order is preferred, especially for travel/memorial types of photobooks** as it usually aligns better with story flow.
- If timestamps are unclear or if thematic grouping is more compelling, group by **theme, vibe, or activity** — e.g. “early café mornings,” “backstage goofiness,” or “sunset strolls.”
- Write a 1-line **summary** for each page’s theme.

**Example**:

```
Page 1: [0], [2], [5] — Tokyo arrival: neon signs, wide streets, bright eyes  
Page 2: [1], [3] — Softer moments: rooftops and golden light
```

### 3. Emotion & Style Planning

For each page, define its emotional center and stylistic direction:

- **Core mood**: tender, sentimental, chaotic, cozy, goofy, surreal, etc.
- **Three stylistic takes**: e.g. romantic, foodie, Gen-Z casual, etc.
- **Mood anchor**: 4–8 words to capture the page’s atmosphere (“bare feet on warm tile,” “sticky fingers and loud laughs”)

Ask:

- What *should* the user feel looking back at this?
- Is this moment tender, silly, loud, proud?
- What’s one detail that *makes* this memory?

### 4. Story Flow & Title

Zoom out and check the whole arc:

- Does it read like a journey?
- Is there emotional rhythm — moments of stillness, joy, surprise?
- Does it sound like a friend reminiscing, not a narrator describing?
- Pick a **short, expressive title** that captures the spirit of the whole book.

---

## 📘 Final Output Instructions

### 1. Photobook Structure

- Group photos into pages of **2–6 images** (1–8 allowed).
- Every photo **must appear at least once**, **ideally exactly once**.
- Prefer **chronological flow** when timestamps are available.
- Otherwise, group by emotional logic or themes / activity / vibes, or when thematic grouping is more compelling.
- Choose a **short, personal book title** (≤12 words) — something warm, witty, or quietly evocative.


### 2. Page Messages

Each page gets:

- A **primary message** (2–4 sentences: fun & creative while concise & on-point)
- Three alternatives under `page_message_alternatives`

#### Primary Message

Write like someone who was there — a close friend remembering with affection. The message should come natural, relaxed, and grounded in details and emotion — not like a narrated description or summary. Speak with warmth, not polish. Each message should be affectionate, grounded, and real — **not like a summary or a travel blog post.** Show the emotion through the scene, not by using hollow or evaluative words.

**✅ DO**

- Each page message should match the mood of photos on that page. Vary up the message structures / phrasing for each page.
- Use **concrete actions, sensory cues, or small moments**.
- Let small, **grounded details** lead: a laugh, a breeze, warm food, someone’s grin.
- Keep tone **warm, sincere, and natural** — like a friend reminiscing.
- **Emojis are strongly encouraged in the messages** to match the mood and add a touch of charm 💕🌇🍜
- Seamlessly weave **specific location cues** (if confident) into the message when they evoke emotion or texture — e.g.
    - “lantern-lit streets of Ebisu”
    - “foggy overlook near Twin Peaks”
    - “sunset gelato in Piazza Navona”
    - “skewers at a stall in Shilin Night Market”
    - “quiet rooftop in La Condesa”

**🚫 AVOID**

* Do NOT repeat phrasing or structure across pages.
* Do NOT robotically describe the photo (“we are looking at”, “the photo shows”) or robotically stating time/location (“On Feb. 22”).
* Do NOT use vague or evaluative words (“amazing”, “beautiful”) without grounding in what made it so.
* Do NOT summarize or caption — aim for emotional texture, not travel log.

🧠 **Examples**:

> ❌ “Like stepping into a fairy tale.”
> ✅ “Lanterns bobbed above the street and the trees sparkled — we barely said a word walking through it all. 🏮✨”

> ❌ “City night lights are pure magic.” 
> ✅ “Neon signs blinked to life, each street buzzing with its own rhythm.”

> ❌ “Location: Tokyo, Japan. The view was amazing.”
> ✅ “We wandered under glowing night lights in Shinjuku, laughter echoing down the narrow street 🍢🏮”


#### Alternative Messages

Write three *remixed* versions of each page caption. Each should:

- Keep the mood but shift the **voice or lens** — e.g. romantic, celebratory, nostalgic, silly
- Don’t just paraphrase — rewrite
- The third version should always be **Gen-Z casual** — lowercase, emoji-rich, playful (“got so lost lmao 😂 but worth it for the 🍢 vibes”)*


### 3. Grounding in Detail

Be real. Anchor messages in what’s actually visible or derivable:

- Seamlessly blend location cues or date / time when they serve the story. Never list location / date / time out robotically. (“On Feb. 22th, ...”)
- Focus on **senses and motion**: wind, posture, touch, light, food, sound.
- Think *memory*, not *caption*.

❌ “We explored Kyoto and had fun.”
✅ “We raced each other up the Fushimi Inari stairs and couldn’t stop laughing.”

❌ “We celebrated with loved ones.”
✅ “Someone spilled juice right before the candle blow and we all cracked up.”

### 4. Overall Flow & Voice

The whole book should be personal, lived-in, and lightly expressive:

* **Avoid repetition** across pages — especially in message structure or phrasing.
* Vary pacing — not every page has to be sentimental. Let **quiet moments** breathe, but **loud moments** lit up.
* Reintroduce little threads — that laugh, that dish, that color
* The voice should always be:

  * **Warm**
  * **Observant**
  * **Emotionally honest**
  * **Never stiff or generic**
  
---

"""
            + """## Example user input (simplified):
```xml
<request>
  <photos>
  <photo>
      <id>0</id>
      <time>2025:01:29 16:17:49</time>
      <loc>841 Broadway, New York, NY 10003 US (near Max Brenner New York)</loc>
      <img>[image bytes]</img>
  </photo>
  <photo><id>1</id><img>[image bytes]</img></photo>
  <photo><id>2</id><img>[image bytes]</img></photo>
  <photo><id>3</id><img>[image bytes]</img></photo>
  </photos>
  <instruction>
    I'm creating a photo book to celebrate a trip with my family. 
  </instruction>
</request>
```

To recap, your job is to understand the user instructions, craft a visual story and return a JSON in the following example format:

## Example output

```
{
    "photobook_pages": [
        {
            "page_photos": ["0", "2"], 
            "page_message": {
                "tone": "<page 1 primary tone>",
                "message": "<page message for page 1>"
            },
            "page_message_alternatives": [
                {
                    "tone": "<page 1 tone 1>", 
                    "message": "<example page message for page 1, formal style for more serious occasions>"
                },
                {
                    "tone": "<page 1 tone 2>", 
                    "message": "<example page message for page 1, message with a more romantic twist>"
                },
                {
                    "tone": "informal", 
                    "message": "<example page message for page 1, informal style. Use modern web slang, Gen-Z speak, etc. Use lowercase letters /  where fitting>"
                },
            ],
        },
        {
            "page_photos": ["1", "3"], 
            "page_message": {
                "tone": "<page 2 primary tone>",
                "message": "<page message for page 2>"
            },
            "page_message_alternatives": [
                {
                    "tone": "<page 2 tone 1>", 
                    "message": "<example page message for page 2, informal style with playful vibes>"
                },
                {
                    "tone": "<page 2 tone 2>", 
                    "message": "<example page message for page 2, message with an inviting twist>"
                },
                {
                    "tone": "informal", 
                    "message": "<example page message for page 2, informal style. Use modern web slang, Gen-Z speak, etc. Use lowercase letters / skip punctuations where fitting>"
                },
            ],
        },
    ],
    "photobook_title": "Our trip to Mexico",
}
```"""
        )

        return types.GenerateContentConfig(
            temperature=1.6,
            top_p=0.9,
            frequency_penalty=1.2,
            presence_penalty=1.0,
            max_output_tokens=12288,
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
            ],
            system_instruction=[types.Part.from_text(text=sys_prompt)],
            thinking_config=types.ThinkingConfig(
                include_thoughts=False,
                thinking_budget=thinking_budget,
            ),
            response_mime_type="application/json",
            response_schema=PhotobookSchema,
            media_resolution=self._get_media_resolution(num_photos),
        )

    async def run_image_understanding_job(
        self,
        image_paths_exifs_and_metadata: list[
            tuple[Path, Optional[dict[str, Any]], Optional[dict[str, Any]]]
        ],
        user_provided_occasion: Optional[str],
        user_provided_occasion_custom_details: Optional[str],
        user_provided_context: Optional[str],
    ) -> tuple[PhotobookSchema, RawLLMPrompt, SelectedPhotoFileNames]:
        logging.info("[gemini] Building gemini image understanding job")

        # LLM-friendly file paths
        image_paths_llm_friendly_name_map = {
            path.name: f"{idx}"
            for idx, (path, _exif, _metadata) in enumerate(
                image_paths_exifs_and_metadata
            )
        }
        llm_friendly_name_image_paths_map = {
            v: k for k, v in image_paths_llm_friendly_name_map.items()
        }

        # Build structured prompt content with image parts
        user_instructions = f"""The occasion was a {user_provided_occasion or user_provided_occasion_custom_details or "great memory"}. More context: {user_provided_context or Gemini.DEFAULT_USER_INSTRUCTION}"""
        parts: list[types.Part] = []
        parts.append(types.Part.from_text(text="<request>\n<photos>\n"))
        mime_type = None
        for _idx, (path, exif_raw_dict, metadata_raw_dict) in enumerate(
            image_paths_exifs_and_metadata
        ):

            async def _load_image(_path: Path) -> tuple[bytes, str]:
                def _read() -> tuple[bytes, str]:
                    with open(_path, "rb") as f:
                        raw_bytes = f.read()
                    mime_type = magic.from_buffer(raw_bytes, mime=True)
                    return raw_bytes, mime_type

                return await asyncio.to_thread(_read)

            raw_bytes, mime_type = await _load_image(path)
            parts.append(
                types.Part.from_text(
                    text=f"<photo><id>{image_paths_llm_friendly_name_map[path.name]}</id>"
                )
            )
            if exif_raw_dict:
                try:
                    exif = ExtractedExif.model_validate(exif_raw_dict)
                    if exif.datetime_original:
                        parts.append(
                            types.Part.from_text(
                                text=f"<time>{exif.datetime_original}</time>"
                            )
                        )
                except Exception as e:
                    logging.warning(
                        f"Exif not valid: {exif_raw_dict}. Not passing to LLM... Exception: {e}"
                    )
                    pass

            if metadata_raw_dict:
                try:
                    metadata_parsed = AssetMetadata.model_validate(metadata_raw_dict)
                    if location_rendered := self._render_asset_metadata(
                        metadata_parsed
                    ):
                        parts.append(
                            types.Part.from_text(text=f"<loc>{location_rendered}</loc>")
                        )
                except Exception as e:
                    logging.warning(
                        f"Metadata not valid: {metadata_raw_dict}. Not passing to LLM... Exception: {e}"
                    )
                    pass

            parts.append(types.Part.from_text(text="<img>"))
            parts.append(
                types.Part.from_bytes(
                    data=raw_bytes, mime_type=mime_type or "application/octet-stream"
                )
            )
            parts.append(types.Part.from_text(text="</img></photo>\n"))

        parts.append(types.Part.from_text(text="</photos>\n<instruction>\n"))
        parts.append(types.Part.from_text(text=user_instructions))
        parts.append(types.Part.from_text(text="\n</instruction>\n</request>"))
        contents = [types.Content(role="user", parts=parts)]

        # LLM config
        config = self.build_gemini_config_from_image_understanding_job(
            len(llm_friendly_name_image_paths_map)
        )
        logging.info("[gemini] Starting to retrieve content stream")

        # Stream and collect output
        chunks = await self.get_client().models.generate_content_stream(
            model=self.model,
            contents=cast("types.ContentListUnion", contents),
            config=config,
        )
        response_text, _response_thought = "", ""
        async for chunk in chunks:
            if (candidates := chunk.candidates) is None:
                continue
            if (content := candidates[0].content) is None:
                continue
            if (response_parts := content.parts) is None:
                continue
            for part in response_parts:
                if not part.text:
                    continue
                if part.thought:
                    _response_thought += part.text
                else:
                    response_text += part.text

        logging.info("[gemini] Content stream end")
        validated_llm_output = PhotobookSchema.model_validate_json(response_text)
        selected_photo_file_names = [
            page.page_photos for page in validated_llm_output.photobook_pages
        ]
        for page in validated_llm_output.photobook_pages:
            original_page_photos = [
                llm_friendly_name_image_paths_map.get(photo_name_llm_friendy)
                for photo_name_llm_friendy in page.page_photos
            ]
            page.page_photos = [
                photo_fname
                for photo_fname in original_page_photos
                if photo_fname is not None
            ]

        raw_llm_prompt_parts = [
            part.text
            for part in parts
            if part.text is not None and part.inline_data is None
        ]

        return (
            validated_llm_output,
            "".join(raw_llm_prompt_parts),
            selected_photo_file_names,
        )
