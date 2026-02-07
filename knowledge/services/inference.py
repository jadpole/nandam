import asyncio
import base64
import logging
import numpy as np

from cerebras.cloud.sdk import AsyncCerebras
from cerebras.cloud.sdk.types.chat.chat_completion import ChatCompletionResponse  # noqa: TC002
from dataclasses import dataclass
from google import genai
from google.genai.errors import APIError as GeminiAPIError
from pydantic.json_schema import JsonSchemaValue
from termcolor import colored

from base.core.exceptions import ApiError
from base.core.schema import clean_jsonschema
from base.core.values import as_json
from base.models.content import ContentBlob
from base.models.context import NdService
from base.strings.auth import ServiceId
from base.strings.data import MimeType
from base.utils.completion import estimate_tokens

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

    async def completion_json(
        self,
        *,
        system: str | None = None,
        response_schema: JsonSchemaValue,
        prompt: list[str | ContentBlob],
    ) -> str:
        raise NotImplementedError("Subclasses must implement Inference.completion_json")

    async def embedding(self, content: str) -> list[float] | None:
        raise NotImplementedError("Subclasses must implement Inference.embedding")


##
## Stub
##


@dataclass(kw_only=True)
class SvcInferenceStub(SvcInference):
    """
    Stub implementation of the inference service for testing.

    When `stub_responses` is provided, `completion_json` will return a JSON object
    with values from `stub_responses` for each property in the response schema.
    Properties not in `stub_responses` will be set to null.
    """

    stub_completions: dict[str, list[str | None]]

    async def completion_json(
        self,
        *,
        system: str | None = None,
        response_schema: JsonSchemaValue,
        prompt: list[str | ContentBlob],
    ) -> str:
        # If stub_responses is provided, build a JSON response matching the schema.
        response: dict[str, str | None] = {}
        for prop_name in response_schema.get("properties", {}):
            if values := self.stub_completions.get(prop_name):
                response[prop_name] = values.pop(0)
            else:
                response[prop_name] = f"stub {prop_name}"
        return as_json(response)

    async def embedding(self, content: str) -> list[float] | None:
        return None


##
## API
##


REQUEST_TIMEOUT = 300
RETRY_DELAY_SECS = [2, 30, 60] if KnowledgeConfig.is_kubernetes() else [30]

SUPPORTED_IMAGE_BLOB_TYPES = [
    MimeType.decode("image/png"),
    MimeType.decode("image/jpeg"),
    MimeType.decode("image/webp"),
    MimeType.decode("image/heic"),
    MimeType.decode("image/heif"),
]

THRESHOLD_NUM_TOKENS_GEMINI = 85_000
"""
For text-only prompts, try to use "gpt-oss-120b" on Cerebras, whose context size
is 128k tokens and may respond with at most 40k tokens.  Thus, max input is 88k,
so we take a lower value to account for hidden scaffolding.
"""


@dataclass(kw_only=True)
class SvcInferenceLlm(SvcInference):
    user_id: str
    request_id: str

    @staticmethod
    def initialize(context: KnowledgeContext) -> "SvcInference":
        if not KnowledgeConfig.llm.gemini_api_key and not (
            KnowledgeConfig.llm.router_api_base and KnowledgeConfig.llm.router_api_key
        ):
            raise ApiError("InferenceLlm requires LLM_GEMINI_API_KEY environment")

        return SvcInferenceLlm(
            user_id=context.auth.tracking_user_id(),
            request_id=str(context.auth.request_id),
        )

    def _llm_headers(self) -> dict[str, str]:
        return {
            "x-georges-task-id": self.request_id,
            "x-georges-task-type": "knowledge-labels",
            "x-georges-user-id": self.user_id,
        }

    async def completion_json(
        self,
        *,
        system: str | None = None,
        response_schema: JsonSchemaValue,
        prompt: list[str | ContentBlob],
    ) -> str:
        has_blobs = any(isinstance(p, ContentBlob) for p in prompt)
        if not KnowledgeConfig.llm.cerebras_api_key or has_blobs:
            return await self._completion_json_gemini(system, response_schema, prompt)

        prompt_text = "\n\n".join(p for p in prompt if isinstance(p, str))
        num_tokens_text = (
            estimate_tokens(system or "")
            + estimate_tokens(as_json(response_schema))
            + estimate_tokens(prompt_text)
        )
        if num_tokens_text > THRESHOLD_NUM_TOKENS_GEMINI:
            return await self._completion_json_gemini(
                system, response_schema, [prompt_text]
            )
        else:
            return await self._completion_json_textual(
                system, response_schema, prompt_text
            )

    async def _completion_json_gemini(
        self,
        system: str | None,
        response_schema: JsonSchemaValue,
        prompt: list[str | ContentBlob],
    ) -> str:
        if KnowledgeConfig.llm.gemini_api_key:
            client = genai.Client(
                api_key=KnowledgeConfig.llm.gemini_api_key,
                http_options=genai.types.HttpOptions(
                    api_version="v1alpha",
                ),
            )
        elif KnowledgeConfig.llm.router_api_base and KnowledgeConfig.llm.router_api_key:
            client = genai.Client(
                api_key=KnowledgeConfig.llm.router_api_key,
                http_options=(
                    genai.types.HttpOptions(
                        base_url=f"{KnowledgeConfig.llm.router_api_base}/gemini",
                        api_version="v1alpha",
                        extra_body={"model": "gemini-3-flash-preview"},
                        headers=self._llm_headers(),
                    )
                ),
            )
        else:
            raise ApiError("InferenceLlm requires LLM_GEMINI_API_KEY")

        contents: genai.types.ContentListUnion = [
            (
                self._convert_blob_gemini(prompt_part)
                if isinstance(prompt_part, ContentBlob)
                else genai.types.Part(text=prompt_part)
            )
            for prompt_part in prompt
        ]

        network_errors: int = 0
        while True:
            try:
                response = await client.aio.models.generate_content(
                    model="gemini-3-flash-preview",
                    contents=contents,
                    config=genai.types.GenerateContentConfig(
                        system_instruction=system,
                        media_resolution=genai.types.MediaResolution.MEDIA_RESOLUTION_LOW,
                        response_mime_type="application/json",
                        response_json_schema=response_schema,
                    ),
                )
                break
            except Exception as exc:
                if network_errors < len(RETRY_DELAY_SECS) and (
                    (isinstance(exc, GeminiAPIError) and exc.code == 429)  # noqa: PLR2004
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

        return response.text or ""

    async def _completion_json_textual(
        self,
        system: str | None,
        response_schema: JsonSchemaValue,
        prompt: str,
    ) -> str:
        client = AsyncCerebras(
            api_key=KnowledgeConfig.llm.cerebras_api_key,
        )
        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "answer_schema",
                "strict": True,
                "schema": clean_jsonschema(
                    response_schema,
                    disallow_examples=True,
                    disallow_pattern=True,
                ),
            },
        }
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ]

        network_errors: int = 0
        while True:
            try:
                completion: ChatCompletionResponse = (
                    await client.chat.completions.create(  # type: ignore
                        model="gpt-oss-120b",
                        messages=messages,
                        reasoning_format="parsed",
                        reasoning_effort="low",
                        response_format=response_format,
                        temperature=1.0,
                        timeout=REQUEST_TIMEOUT,
                    )
                )
                answer: str = completion.choices[0].message.content or ""

                if KnowledgeConfig.verbose >= 2:  # noqa: PLR2004
                    completion_log = (
                        f"<think>\n{reasoning.rstrip()}\n</think>\n\n{answer}"
                        if (reasoning := completion.choices[0].message.reasoning)
                        else answer
                    )
                    print(colored(completion_log, "green"))

                return answer
            except Exception as exc:
                if network_errors < len(RETRY_DELAY_SECS) and (
                    (isinstance(exc, GeminiAPIError) and exc.code == 429)  # noqa: PLR2004
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

    def _convert_blob_gemini(self, blob: ContentBlob) -> genai.types.Part:
        return genai.types.Part(
            inline_data=genai.types.Blob(
                mime_type=str(blob.mime_type),
                data=base64.b64decode(blob.blob),
            )
        )
