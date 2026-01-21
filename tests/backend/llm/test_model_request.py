import base64
import pytest

from typing import Any

from base.core.values import as_json, as_value
from base.models.content import ContentText
from base.resources.aff_body import AffBodyMedia, ObsMedia
from base.strings.auth import UserId
from base.strings.resource import ObservableUri

from backend.data.llm_models import get_llm_by_name, LlmModelName
from backend.llm.message import (
    LlmPart,
    LlmText,
    LlmThink,
    LlmToolCall,
    LlmToolCalls,
    LlmToolResult,
)

from tests.backend.utils_context import given_headless_process
from tests.data.tools import TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH


##
## Conversion
## ---
## TODO: Test that the tool call ID is correctly translated into OpenAI.
## TODO: Test that the tool call ID is correctly translated into Anthropic.
##


async def _callback_noop(messages: list[LlmPart]) -> None:
    pass


def _given_fake_messages(
    input_image_uri: ObservableUri[AffBodyMedia],
    output_image_uri: ObservableUri[AffBodyMedia],
) -> list[LlmPart]:
    return [
        # "user" message with image.
        LlmText.prompt(
            UserId.stub(),
            f"""\
Say hi first, then generate an image with the exact prompt: a greenhouse on a spaceship.
Here is an image for inspiration:

![image]({input_image_uri})\
""",
        ),
        # "assistant" message with answer AND tool call.
        LlmThink.stub("some thought"),
        LlmText.parse_body("Hi."),
        LlmToolCalls(
            calls=[
                LlmToolCall.stub(
                    "generate_image",
                    "1111",
                    {"prompt": "a greenhouse on a spaceship"},
                ),
            ],
        ),
        # "tool" message with image.
        LlmToolResult.stub(
            "generate_image",
            "1111",
            {"content": as_value(ContentText.new_embed(output_image_uri, ""))},
        ),
        # "assistant" message with "embed" link without blob (text-only).
        LlmThink.stub("answer thought"),
        LlmText.parse_body(
            f"I have successfully generated the image:\n\n![image]({output_image_uri})"
        ),
        # "user" message with text only.
        LlmText.prompt(
            UserId.stub(),
            "Now do a web search for 'recent AI news'.",
        ),
        # "assistant" message with tool call only.
        LlmThink.stub("tool thought"),
        LlmToolCalls(
            calls=[
                LlmToolCall.stub(
                    "web_search",
                    "2222",
                    {"prompt": "recent AI news"},
                )
            ],
        ),
        # "tool" message with value, but no content/embeds.
        # TODO: Include `uri` in results and populate `resources`.
        LlmToolResult.stub(
            "web_search",
            "2222",
            {"results": [{"snippet": "news snippet 1"}, {"snippet": "news snippet 2"}]},
        ),
        # "assistant" and "user" messages, so the conversation history is "well-formed".
        # TODO: Use `[^citation]` syntax.
        LlmThink.stub("final thought"),
        LlmText.parse_body("You may be interested by news snippet 1. Anything else?"),
        LlmText.prompt(
            UserId.stub(),
            "That will be all. Thanks you!",
        ),
    ]


def _given_completion_params(model: LlmModelName) -> Any:
    input_image_uri = ObservableUri[AffBodyMedia].decode(
        "ndk://stub/-/input.png/$media"
    )
    output_image_uri = ObservableUri[AffBodyMedia].decode(
        "ndk://stub/-/output.png/$media"
    )
    llm = get_llm_by_name(model)
    return llm._get_completion_params(
        process=given_headless_process(
            observations=[
                ObsMedia.stub(str(input_image_uri), ""),
                ObsMedia.stub(str(output_image_uri), ""),
            ],
        ),
        system="system message",
        messages=_given_fake_messages(input_image_uri, output_image_uri),
        max_tokens=1,
        temperature=0.0,
        tools=[TOOL_GENERATE_IMAGE, TOOL_READ_DOCS, TOOL_WEB_SEARCH],
    )


@pytest.mark.parametrize("model", ["claude-haiku", "claude-opus", "claude-sonnet"])
def test_llm_model_get_completion_params_claude(model: LlmModelName):
    """
    TODO: Reasoning budget not passed!
    """
    llm = get_llm_by_name(model)
    params = _given_completion_params(model).params
    tools = params.pop("tools", [])
    messages = params.pop("messages", [])
    print(f"<params>\n{as_json(params, indent=2)}\n</params>")
    print(f"<tools>\n{as_json(tools, indent=2)}\n</tools>")
    print(f"<messages>\n{as_json(messages, indent=2)}\n</messages>")

    expected = {
        "extra_body": {
            "thinking": {
                "type": "enabled",
                "budget_tokens": 24_000,
            },
        },
        "extra_headers": {
            "x-georges-task-id": "request-stub00000000000000000000",
            "x-georges-task-type": "stub_process",
            "x-georges-user-id": "00000000-0000-0000-0000-4dbe39ac372d",
        },
        "max_tokens": 64_000,  # Hard-coded: must be higher than "budget_tokens".
        "model": llm.native_name,
        "timeout": 300,
        "tool_choice": "auto",
        # NOTE: "temperature" omitted because of reasoning.
    }
    assert params == expected
    assert [t["function"]["name"] for t in tools] == [
        "generate_image",
        "read_docs",
        "web_search",
    ]
    assert messages == [
        {
            "role": "system",
            "content": "system message",
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": """\
Say hi first, then generate an image with the exact prompt: a greenhouse on a spaceship.
Here is an image for inspiration:
<blob uri="ndk://stub/-/input.png/$media">\
""",
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC"
                    },
                },
                {
                    "type": "text",
                    "text": "</blob>",
                },
            ],
        },
        {
            "role": "assistant",
            "thinking_blocks": [
                {
                    "type": "thinking",
                    "thinking": "some thought",
                    "signature": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC",
                }
            ],
            "content": "Hi.",
            "tool_calls": [
                {
                    "type": "function",
                    "id": "call_000000000000000000001111",
                    "function": {
                        "name": "generate_image",
                        "arguments": r'{"prompt": "a greenhouse on a spaceship"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_000000000000000000001111",
            "content": r'{"content": "![](ndk://stub/-/output.png/$media)"}',
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": '<tool-result-embeds>\n<blob uri="ndk://stub/-/output.png/$media">',
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC"
                    },
                },
                {
                    "type": "text",
                    "text": "</blob>\n</tool-result-embeds>",
                },
            ],
        },
        {
            "role": "assistant",
            "thinking_blocks": [
                {
                    "type": "thinking",
                    "thinking": "answer thought",
                    "signature": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC",
                }
            ],
            "content": "I have successfully generated the image:\n\n![image](ndk://stub/-/output.png/$media)",
        },
        {
            "role": "user",
            "content": "Now do a web search for 'recent AI news'.",
        },
        {
            "role": "assistant",
            "thinking_blocks": [
                {
                    "type": "thinking",
                    "thinking": "tool thought",
                    "signature": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC",
                }
            ],
            "tool_calls": [
                {
                    "type": "function",
                    "id": "call_000000000000000000002222",
                    "function": {
                        "name": "web_search",
                        "arguments": r'{"prompt": "recent AI news"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_000000000000000000002222",
            "content": r'{"results": [{"snippet": "news snippet 1"}, {"snippet": "news snippet 2"}]}',
        },
        {
            "role": "assistant",
            "thinking_blocks": [
                {
                    "type": "thinking",
                    "thinking": "final thought",
                    "signature": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC",
                }
            ],
            "content": "You may be interested by news snippet 1. Anything else?",
        },
        {
            "role": "user",
            "content": "That will be all. Thanks you!",
        },
    ]


@pytest.mark.parametrize("model", ["gpt-oss"])
def test_llm_model_get_completion_params_cerebras_gpt_oss(model: LlmModelName):
    llm = get_llm_by_name(model)
    params = _given_completion_params(model).params
    tools = params.pop("tools", [])
    messages = params.pop("messages", [])
    print(f"<params>\n{as_json(params, indent=2)}\n</params>")
    print(f"<tools>\n{as_json(tools, indent=2)}\n</tools>")
    print(f"<messages>\n{as_json(messages, indent=2)}\n</messages>")

    expected = {
        "extra_body": {},
        "model": llm.native_name,
        "parallel_tool_calls": True,
        "reasoning_effort": "high",
        "reasoning_format": "parsed",
        "temperature": 1.0,
        "timeout": 300,
    }
    assert params == expected
    assert [t["function"]["name"] for t in tools] == [
        "generate_image",
        "read_docs",
        "web_search",
    ]
    assert messages == [
        {
            "role": "system",
            "content": "system message",
        },
        {
            "role": "user",
            "content": """\
Say hi first, then generate an image with the exact prompt: a greenhouse on a spaceship.
Here is an image for inspiration:
<blob uri="ndk://stub/-/input.png/$media" mimetype="image/png">
stub placeholder
</blob>\
""",
        },
        {
            "role": "assistant",
            "content": "some thought\n\nHi.",
            "tool_calls": [
                {
                    "type": "function",
                    "id": "call_000000000000000000001111",
                    "function": {
                        "name": "generate_image",
                        "arguments": r'{"prompt": "a greenhouse on a spaceship"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_000000000000000000001111",
            "content": r'{"content": "<blob uri=\"ndk://stub/-/output.png/$media\" mimetype=\"image/png\">\nstub placeholder\n</blob>"}',
        },
        {
            "role": "assistant",
            "content": "answer thought\n\nI have successfully generated the image:\n\n![image](ndk://stub/-/output.png/$media)",
        },
        {
            "role": "user",
            "content": "Now do a web search for 'recent AI news'.",
        },
        {
            "role": "assistant",
            "content": "tool thought",
            "tool_calls": [
                {
                    "type": "function",
                    "id": "call_000000000000000000002222",
                    "function": {
                        "name": "web_search",
                        "arguments": r'{"prompt": "recent AI news"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_000000000000000000002222",
            "content": r'{"results": [{"snippet": "news snippet 1"}, {"snippet": "news snippet 2"}]}',
        },
        {
            "role": "assistant",
            "content": "final thought\n\nYou may be interested by news snippet 1. Anything else?",
        },
        {
            "role": "user",
            "content": "That will be all. Thanks you!",
        },
    ]


@pytest.mark.parametrize("model", ["zai-glm", "zai-glm-fast"])
def test_llm_model_get_completion_params_cerebras_zai_glm_think(model: LlmModelName):
    llm = get_llm_by_name(model)
    params = _given_completion_params(model).params
    tools = params.pop("tools", [])
    messages = params.pop("messages", [])
    print(f"<params>\n{as_json(params, indent=2)}\n</params>")
    print(f"<tools>\n{as_json(tools, indent=2)}\n</tools>")
    print(f"<messages>\n{as_json(messages, indent=2)}\n</messages>")

    expected = {
        "extra_body": {},
        "model": "zai-glm-4.7",
        "parallel_tool_calls": True,
        "timeout": 300,
    }
    if llm.supports_think:
        expected["reasoning_format"] = "parsed"
        expected["temperature"] = 0.6
    else:
        expected["disable_reasoning"] = True
        expected["max_tokens"] = 1
        expected["temperature"] = 0.0

    assert params == expected
    assert [t["function"]["name"] for t in tools] == [
        "generate_image",
        "read_docs",
        "web_search",
    ]

    expected_messages = [
        {
            "role": "system",
            "content": "system message",
        },
        {
            "role": "user",
            "content": """\
Say hi first, then generate an image with the exact prompt: a greenhouse on a spaceship.
Here is an image for inspiration:
<blob uri="ndk://stub/-/input.png/$media" mimetype="image/png">
stub placeholder
</blob>\
""",
        },
        {
            "role": "assistant",
            "content": "Hi.",
            "tool_calls": [
                {
                    "type": "function",
                    "id": "call_000000000000000000001111",
                    "function": {
                        "name": "generate_image",
                        "arguments": r'{"prompt": "a greenhouse on a spaceship"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_000000000000000000001111",
            "content": r'{"content": "<blob uri=\"ndk://stub/-/output.png/$media\" mimetype=\"image/png\">\nstub placeholder\n</blob>"}',
        },
        {
            "role": "assistant",
            "content": "I have successfully generated the image:\n\n![image](ndk://stub/-/output.png/$media)",
        },
        {
            "role": "user",
            "content": "Now do a web search for 'recent AI news'.",
        },
        {
            "role": "assistant",
            "tool_calls": [
                {
                    "type": "function",
                    "id": "call_000000000000000000002222",
                    "function": {
                        "name": "web_search",
                        "arguments": r'{"prompt": "recent AI news"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_000000000000000000002222",
            "content": r'{"results": [{"snippet": "news snippet 1"}, {"snippet": "news snippet 2"}]}',
        },
        {
            "role": "assistant",
            "content": "You may be interested by news snippet 1. Anything else?",
        },
        {
            "role": "user",
            "content": "That will be all. Thanks you!",
        },
    ]
    if llm.supports_think:
        expected_thoughts = [
            (2, "some thought"),
            (4, "answer thought"),
            (6, "tool thought"),
            (8, "final thought"),
        ]
        for index, thought in expected_thoughts:
            assert expected_messages[index]["role"] == "assistant"
            prefix = f"<think>{thought}</think>"
            prev_content = expected_messages[index].get("content")
            if prev_content:
                expected_messages[index]["content"] = f"{prefix}\n\n{prev_content}"
            else:
                expected_messages[index]["content"] = prefix
    assert messages == expected_messages


@pytest.mark.parametrize("model", ["gpt-5", "gpt-5-mini", "o3", "o4-mini"])
def test_llm_model_get_completion_params_openai(model: LlmModelName):
    """
    TODO: Reasoning effort not passed!
    """
    llm = get_llm_by_name(model)
    params = _given_completion_params(model).params
    tools = params.pop("tools", [])
    messages = params.pop("messages", [])
    print(f"<params>\n{as_json(params, indent=2)}\n</params>")
    print(f"<tools>\n{as_json(tools, indent=2)}\n</tools>")
    print(f"<messages>\n{as_json(messages, indent=2)}\n</messages>")

    expected = {
        "extra_body": {},
        "extra_headers": {
            "x-georges-task-id": "request-stub00000000000000000000",
            "x-georges-task-type": "stub_process",
            "x-georges-user-id": "00000000-0000-0000-0000-4dbe39ac372d",
        },
        "model": llm.native_name,
        "reasoning_effort": "medium",
        "timeout": 300,
        "tool_choice": "auto",
        # NOTE: "max_tokens", "temperature" omitted because of reasoning.
    }
    assert params == expected
    assert [t["function"]["name"] for t in tools] == [
        "generate_image",
        "read_docs",
        "web_search",
    ]
    assert messages == [
        {
            "role": "developer",
            "content": "system message",
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": """\
Say hi first, then generate an image with the exact prompt: a greenhouse on a spaceship.
Here is an image for inspiration:
<blob uri="ndk://stub/-/input.png/$media">\
""",
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC",
                    },
                },
                {
                    "type": "text",
                    "text": "</blob>",
                },
            ],
        },
        {
            "role": "assistant",
            "content": "Hi.",
            "tool_calls": [
                {
                    "type": "function",
                    "id": "call_000000000000000000001111",
                    "function": {
                        "name": "generate_image",
                        "arguments": r'{"prompt": "a greenhouse on a spaceship"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_000000000000000000001111",
            "content": r'{"content": "![](ndk://stub/-/output.png/$media)"}',
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": '<tool-result-embeds>\n<blob uri="ndk://stub/-/output.png/$media">',
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC",
                    },
                },
                {
                    "type": "text",
                    "text": "</blob>\n</tool-result-embeds>",
                },
            ],
        },
        {
            "role": "assistant",
            "content": "I have successfully generated the image:\n\n![image](ndk://stub/-/output.png/$media)",
        },
        {
            "role": "user",
            "content": "Now do a web search for 'recent AI news'.",
        },
        {
            "role": "assistant",
            "tool_calls": [
                {
                    "type": "function",
                    "id": "call_000000000000000000002222",
                    "function": {
                        "name": "web_search",
                        "arguments": r'{"prompt": "recent AI news"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_000000000000000000002222",
            "content": r'{"results": [{"snippet": "news snippet 1"}, {"snippet": "news snippet 2"}]}',
        },
        {
            "role": "assistant",
            "content": "You may be interested by news snippet 1. Anything else?",
        },
        {
            "role": "user",
            "content": "That will be all. Thanks you!",
        },
    ]
