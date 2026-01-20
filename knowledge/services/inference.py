import asyncio
import base64
import contextlib
import logging
import numpy as np

from dataclasses import dataclass
from google import genai
from google.genai.errors import APIError as GeminiAPIError
from pydantic import BaseModel

from base.core.exceptions import ApiError
from base.models.content import ContentBlob, ContentText
from base.models.context import NdService
from base.models.rendered import Rendered
from base.resources.observation import Observation
from base.strings.auth import ServiceId
from base.strings.data import MimeType

from knowledge.config import KnowledgeConfig
from knowledge.models.exceptions import IngestionError
from knowledge.server.context import KnowledgeContext

logger = logging.getLogger(__name__)

SVC_INFERENCE = ServiceId.decode("svc-inference")


@dataclass(kw_only=True)
class SvcInference(NdService):
    service_id: ServiceId = SVC_INFERENCE

    @staticmethod
    def initialize(context: KnowledgeContext) -> "SvcInference":
        return SvcInferenceLlm.initialize(context)

    async def completion_json[T: BaseModel](
        self,
        *,
        system: str | None = None,
        response_schema: type[T],
        prompt: ContentText,
        observations: list[Observation],
    ) -> T | str | None:
        raise NotImplementedError("Subclasses must implement Inference.completion_json")

    async def embedding(self, content: str) -> list[float] | None:
        raise NotImplementedError("Subclasses must implement Inference.embedding")


##
## Stub
##


@dataclass(kw_only=True)
class SvcInferenceStub(SvcInference):
    async def completion_json[T: BaseModel](
        self,
        *,
        system: str | None = None,
        response_schema: type[T],
        prompt: ContentText,
        observations: list[Observation],
    ) -> T | str | None:
        words = prompt.as_str().split()
        return (
            f"stub completion: {' '.join(words[:5])}..."
            if len(words) > 5  # noqa: PLR2004
            else f"stub completion: {' '.join(words)}"
        )

    async def embedding(self, content: str) -> list[float] | None:
        return None


##
## API
##


RETRY_DELAY_SECS = [2, 30, 60] if KnowledgeConfig.is_kubernetes() else [30]

SUPPORTED_IMAGE_BLOB_TYPES = [
    MimeType.decode("image/png"),
    MimeType.decode("image/jpeg"),
    MimeType.decode("image/webp"),
    MimeType.decode("image/heic"),
    MimeType.decode("image/heif"),
]


@dataclass(kw_only=True)
class SvcInferenceLlm(SvcInference):
    client: genai.Client
    user_id: str | None

    @staticmethod
    def initialize(context: KnowledgeContext) -> "SvcInference":
        if not KnowledgeConfig.llm.gemini_api_key:
            raise ApiError("Cannot instanciate InferenceLlm without LLM configs")

        user_id = context.auth.tracking_user_id()

        headers: dict[str, str] = {}
        if user_id:
            headers["x-georges-user-id"] = user_id

        return SvcInferenceLlm(
            client=genai.Client(
                api_key=KnowledgeConfig.llm.gemini_api_key,
                http_options=genai.types.HttpOptions(
                    base_url=KnowledgeConfig.llm.gemini_api_key,
                    api_version="v1alpha",
                    extra_body={"model": "gemini-3-flash-preview"},
                    headers=headers,
                ),
            ),
            user_id=user_id,
        )

    async def completion_json[T: BaseModel](
        self,
        *,
        system: str | None = None,
        response_schema: type[T],
        prompt: ContentText,
        observations: list[Observation],
    ) -> T | str | None:
        contents = self._convert_content(prompt, observations)

        network_errors: int = 0
        while True:
            try:
                response = await self.client.aio.models.generate_content(
                    model="gemini-3-flash-preview",
                    contents=list(contents),
                    config=genai.types.GenerateContentConfig(
                        system_instruction=system,
                        media_resolution=genai.types.MediaResolution.MEDIA_RESOLUTION_LOW,
                        response_mime_type="application/json",
                        response_schema=response_schema,
                    ),
                )
                break
            except Exception as exc:
                if network_errors < len(RETRY_DELAY_SECS) and (
                    (
                        isinstance(exc, GeminiAPIError)
                        and exc.code == 429  # noqa: PLR2004
                    )
                    or "overloaded" in str(exc).lower()
                ):
                    if KnowledgeConfig.verbose:
                        logger.warning("Retrying after Gemini error: %s", str(exc))
                    else:
                        logger.warning("Retrying after Gemini error")

                    await asyncio.sleep(RETRY_DELAY_SECS[network_errors])
                    network_errors += 1
                else:
                    raise IngestionError(f"Gemini API error: {exc}") from exc

        # Return the parsed response when it matches the expected schema.
        with contextlib.suppress(Exception):
            if response.parsed is not None:
                if isinstance(response.parsed, response_schema):
                    return response.parsed
                else:
                    return response_schema.model_validate(response.parsed)

        # Otherwise, return the raw text completion.
        return response.text

    async def embedding(self, content: str) -> list[float] | None:
        """
        Returns a 768-dimensional vector that can be used for cosine similarity.
        NOTE: The vector is always normalized to unit length.
        """
        try:
            client = genai.Client(
                api_key=KnowledgeConfig.llm.gemini_api_key,
            )
            result = await client.aio.models.embed_content(
                model="gemini-embedding-001",
                contents=content,
                config=genai.types.EmbedContentConfig(
                    output_dimensionality=768,
                ),
            )
            assert result.embeddings
            assert result.embeddings[0].values

            np_values = np.array(result.embeddings[0].values)
            normed_values = np_values / np.linalg.norm(np_values)
            return normed_values.tolist()
        except Exception:
            logger.exception("Failed to generate embedding")
            return None

    def _convert_content(
        self,
        prompt: ContentText,
        observations: list[Observation],
    ) -> list[genai.types.Part]:
        rendered = Rendered.render(
            content=prompt,
            observations=observations,
        ).as_llm_inline(
            supports_media=SUPPORTED_IMAGE_BLOB_TYPES,
            limit_media=20,
        )
        return [
            (
                self._convert_blob(rendered_part)
                if isinstance(rendered_part, ContentBlob)
                else genai.types.Part(text=rendered_part)
            )
            for rendered_part in rendered
        ]

    def _convert_blob(self, blob: ContentBlob) -> genai.types.Part:
        return genai.types.Part(
            inline_data=genai.types.Blob(
                mime_type=str(blob.mime_type),
                data=base64.b64decode(blob.blob),
            )
        )
