"""
TODO: Test late and unexpected tool results.
"""

import pytest

from typing import Literal, get_args

from base.config import TEST_LLM
from base.core.values import as_json
from base.strings.auth import ServiceId, UserId

from backend.data.llm_models import get_llm_by_name, LlmModelName
from backend.llm.message import (
    LlmPart,
    LlmText,
    LlmThink,
    LlmToolCalls,
    LlmToolResult,
    system_instructions,
)
from backend.llm.model import LlmModel
from backend.models.process_status import ProcessSuccess

from tests.backend.utils_context import given_headless_process
from tests.data.samples import given_sample_media
from tests.data.tools import TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH


async def _callback_noop(messages: list[LlmPart]) -> None:
    pass


##
## Completion - Text
##


def _assert_valid_think(
    completion: list[LlmPart],
    llm: LlmModel,
    mode: Literal["batch", "stream"],  # noqa: ARG001
) -> None:
    # NOTE: "hidden" API does not return reasoning in Completion response.
    if llm.supports_think not in (None, "hidden"):
        thoughts = [p for p in completion if isinstance(p, LlmThink)]

        # DeepSeek-v3.1 and GPT-OSS thinking is optional.
        if llm.supports_think not in ("deepseek", "gpt-oss"):
            assert thoughts

        # Proprietary LLMs should include a thought signature.
        # Open weights LLMs should include a thought text.
        if llm.supports_think in ("anthropic", "gemini"):
            assert all(t.text for t in thoughts)
            assert any(t.signature for t in thoughts)
        elif llm.supports_think in ("deepseek", "gpt-oss"):
            assert all(t.text for t in thoughts)
        else:
            pytest.fail("unexpected supports_think: %s", llm.supports_think)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "mode", "stop"),
    [
        (model, mode, stop)
        for mode in ("batch", "stream")
        for model in get_args(LlmModelName)
        for stop in ("none", "stop")
        if (stop != "stop" or get_llm_by_name(model).supports_stop)
        and (mode != "stream" or get_llm_by_name(model).supports_stream)
    ],
)
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_text_follows_basic_instruction(
    mode: Literal["batch", "stream"],
    model: LlmModelName,
    stop: Literal["none", "stop"],
):
    llm = get_llm_by_name(model)
    completion, _ = await llm.get_completion(
        process=given_headless_process(),
        callback=_callback_noop if mode == "stream" else None,
        system="You are a helpful assistant.",
        messages=[
            LlmText.prompt(UserId.stub(), "Answer with 'boop' and nothing else."),
        ],
        temperature=0.0,
        stop=["stopword"] if stop == "stop" else [],
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )

    # The completion includes an answer.
    answers = [p for p in completion if isinstance(p, LlmText)]
    assert len(answers) == 1
    answer = answers[0].content.as_str().lower()
    assert answer

    # The completion includes thinking.
    _assert_valid_think(completion, llm, mode)

    assert answer.startswith("boop")
    assert not any(p for p in completion if not isinstance(p, LlmText | LlmThink))


@pytest.mark.asyncio
@pytest.mark.parametrize("model", get_args(LlmModelName))
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_text_follows_system(model: LlmModelName):
    llm = get_llm_by_name(model)
    completion, _ = await llm.get_completion_text(
        process=given_headless_process(),
        system="Translate to French and nothing else.",
        messages=[LlmText.prompt(UserId.stub(), "Hello!")],
        temperature=0.0,
    )
    print(f"<completion>\n{completion}\n</completion>")
    assert completion.lower().strip().startswith(("allo", "bonjour"))


##
## Completion - Inputs
##


@pytest.mark.asyncio
@pytest.mark.parametrize("model", get_args(LlmModelName))
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_text_describes_image(model: LlmModelName):
    """
    NOTE: Models with `supports_media` describe the image itself, whereas those
    that only support text receive a placeholder.
    """
    llm = get_llm_by_name(model)
    sample_media = given_sample_media()
    completion, _ = await llm.get_completion_text(
        process=given_headless_process(observations=[sample_media]),
        system=None,
        messages=[
            LlmText.prompt(
                UserId.stub(),
                f"What is playing?\n\n![]({sample_media.uri})",
            ),
        ],
        temperature=0.0,
    )
    print(f"<completion>\n{completion}\n</completion>")
    assert "sucker" in completion.lower()


##
## Completion - Tools
##


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "mode"),
    [
        (model, mode)
        for mode in ("batch", "stream")
        for model in get_args(LlmModelName)
        if (mode != "stream" or get_llm_by_name(model).supports_stream)
        and model not in ("o3", "o4-mini")
    ],
)
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_tool_call(
    model: LlmModelName,
    mode: Literal["batch", "stream"],
):
    llm = get_llm_by_name(model)
    completion, _ = await llm.get_completion(
        process=given_headless_process(),
        callback=_callback_noop if mode == "stream" else None,
        system=None,
        messages=[
            LlmText.prompt(
                UserId.stub(),
                "Generate an image with the exact prompt: a greenhouse on a spaceship.",
            )
        ],
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )
    tool_calls = [p for p in completion if isinstance(p, LlmToolCalls)]
    assert tool_calls

    # The completion includes thinking.
    _assert_valid_think(completion, llm, mode)

    # The completion includes the expected tool call.
    assert len(tool_calls) == 1
    assert len(tool_calls[0].calls) == 1
    assert tool_calls[0].calls[0].name == "generate_image"
    assert (
        tool_calls[0]
        .calls[0]
        .arguments["prompt"]
        .lower()
        .startswith("a greenhouse on a spaceship")
    )

    # The answer (if any) does not include hallucinations.
    answer = "\n".join(
        p.content.as_str().lower() for p in completion if isinstance(p, LlmText)
    )
    assert "<tool-result>" not in answer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "mode"),
    [
        (model, mode)
        for mode in ("batch", "stream")
        for model in get_args(LlmModelName)
        if (mode != "stream" or get_llm_by_name(model).supports_stream)
        and model not in ("o3", "o4-mini")
    ],
)
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_answer_then_tool_call(  # TODO
    model: LlmModelName,
    mode: Literal["batch", "stream"],
):
    llm = get_llm_by_name(model)
    completion, _ = await llm.get_completion(
        process=given_headless_process(),
        callback=_callback_noop if mode == "stream" else None,
        system=None,
        messages=[
            LlmText.prompt(
                UserId.stub(),
                """\
Before you invoke any tool, say 'hi'.
Then, without waiting for an answer, generate an image with the exact prompt: \
a greenhouse on a spaceship.\
""",
            )
        ],
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )
    tool_calls = [p for p in completion if isinstance(p, LlmToolCalls)]
    assert tool_calls

    # The completion includes thinking.
    _assert_valid_think(completion, llm, mode)

    # The completion includes the expected tool call.
    assert len(tool_calls) == 1
    assert len(tool_calls[0].calls) == 1
    assert tool_calls[0].calls[0].name == "generate_image"
    assert (
        tool_calls[0]
        .calls[0]
        .arguments["prompt"]
        .lower()
        .startswith("a greenhouse on a spaceship")
    )

    # The LLM answers before using the tool, as expected.
    # The answer includes no hallucinated tool result or user message.
    answer = "\n".join(
        p.content.as_str().lower() for p in completion if isinstance(p, LlmText)
    )
    assert "hi" in answer
    assert "<tool-result>" not in answer


##
## Completion - Multi-Turn
##


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "mode"),
    [
        (model, mode)
        for mode in ("batch", "stream")
        for model in get_args(LlmModelName)
        if (llm := get_llm_by_name(model)) and (mode != "stream" or llm.supports_stream)
    ],
)
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_propagates_thinking_with_answer(
    model: LlmModelName,
    mode: Literal["batch", "stream"],
):
    llm = get_llm_by_name(model)
    process = given_headless_process()

    # Start with "boop".
    completion, state = await llm.get_completion(
        process=process,
        callback=_callback_noop if mode == "stream" else None,
        system=None,
        messages=[
            LlmText.prompt(UserId.stub(), "Answer with 'boop' and nothing else."),
        ],
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )

    # Continue with "fizzbuzz".
    completion, _ = await llm.get_completion(
        process=process,
        callback=_callback_noop if mode == "stream" else None,
        state=state,
        system="You are a helpful assistant.",
        messages=[
            LlmText.prompt(UserId.stub(), "Answer with 'fizzbuzz' and nothing else."),
        ],
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )

    # The completion includes an answer.
    answers = [p for p in completion if isinstance(p, LlmText)]
    assert len(answers) == 1
    answer = answers[0].content.as_str().lower()
    assert answer.startswith("fizzbuzz")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "mode"),
    [
        (model, mode)
        for mode in ("batch", "stream")
        for model in get_args(LlmModelName)
        if (llm := get_llm_by_name(model)) and (mode != "stream" or llm.supports_stream)
    ],
)
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_propagates_thinking_with_tools(
    model: LlmModelName,
    mode: Literal["batch", "stream"],
):
    llm = get_llm_by_name(model)
    sample_media = given_sample_media()
    process = given_headless_process(observations=[sample_media])

    # Start with "generate_image".
    completion, state = await llm.get_completion(
        process=process,
        callback=_callback_noop if mode == "stream" else None,
        system=None,
        messages=[
            LlmText.prompt(
                UserId.stub(),
                f"What is playing in [web player]({sample_media.uri}). Use read_docs!",
            ),
        ],
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )

    tool_calls = [p for p in completion if isinstance(p, LlmToolCalls)]
    assert len(tool_calls) == 1
    assert len(tool_calls[0].calls) == 1
    assert tool_calls[0].calls[0].name == "read_docs"
    assert tool_calls[0].calls[0].process_id

    # Continue with "fizzbuzz".
    # NOTE: LLM APIs respond with an error when the signature is missing.
    # Therefore, this confirms that it was correctly propagated.  This also
    # checks that tool results with images are understood by the LLM.
    # TODO: Perhaps use read_docs instead, returning the sample image, then ask
    # the model 'what is playing?'
    completion, _ = await llm.get_completion(
        process=process,
        callback=_callback_noop if mode == "stream" else None,
        state=state,
        system=None,
        messages=[
            LlmToolResult(
                sender=ServiceId.stub("tool"),
                process_id=tool_calls[0].calls[0].process_id,
                name=tool_calls[0].calls[0].name,
                result=ProcessSuccess(
                    value={
                        "content": f"![]({sample_media.uri})",
                        "content_mode": "markdown",
                    },
                ),
            ),
        ],
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )

    # The completion includes an answer.
    answers = [p for p in completion if isinstance(p, LlmText)]
    assert len(answers) == 1
    answer = answers[0].content.as_str().lower()
    assert "sucker" in answer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "mode"),
    [
        (model, mode)
        for mode in ("batch", "stream")
        for model in get_args(LlmModelName)
        if (llm := get_llm_by_name(model)) and (mode != "stream" or llm.supports_stream)
    ],
)
@pytest.mark.skipif(not TEST_LLM, reason="LLM tests disabled by default")
async def test_get_completion_standard_system(
    model: LlmModelName,
    mode: Literal["batch", "stream"],
):
    llm = get_llm_by_name(model)
    sample_media = given_sample_media()
    process = given_headless_process(observations=[sample_media])

    # Start with "generate_image".
    completion, state = await llm.get_completion(
        process=process,
        callback=_callback_noop if mode == "stream" else None,
        system=system_instructions(
            llm.info(),
            mermaid=True,
            tags=[LlmToolResult],
        ),
        messages=[
            LlmText.prompt(
                UserId.stub(),
                f"What is playing in [web player]({sample_media.uri}). Use read_docs!",
            ),
        ],
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )

    tool_calls = [p for p in completion if isinstance(p, LlmToolCalls)]
    assert len(tool_calls) == 1
    assert len(tool_calls[0].calls) == 1
    assert tool_calls[0].calls[0].name == "read_docs"
    assert tool_calls[0].calls[0].process_id

    # Continue with "fizzbuzz".
    # NOTE: LLM APIs respond with an error when the signature is missing.
    # Therefore, this confirms that it was correctly propagated.  This also
    # checks that tool results with images are understood by the LLM.
    # TODO: Perhaps use read_docs instead, returning the sample image, then ask
    # the model 'what is playing?'
    completion, _ = await llm.get_completion(
        process=process,
        callback=_callback_noop if mode == "stream" else None,
        state=state,
        system="You are a helpful assistant.",
        messages=[
            LlmToolResult(
                sender=ServiceId.stub("tool"),
                process_id=tool_calls[0].calls[0].process_id,
                name=tool_calls[0].calls[0].name,
                result=ProcessSuccess(
                    value={
                        "content": f"![]({sample_media.uri})",
                        "content_mode": "markdown",
                    },
                ),
            ),
        ],
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )
    print(
        "\n".join(["<completion>", *[as_json(p) for p in completion], "</completion>"])
    )

    # The completion includes an answer.
    answers = [p for p in completion if isinstance(p, LlmText)]
    assert len(answers) == 1
    answer = answers[0].content.as_str().lower()
    assert "sucker" in answer
