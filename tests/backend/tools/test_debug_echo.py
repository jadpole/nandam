import pytest

from backend.models.process_result import ProcessFailure, ProcessSuccess
from base.strings.process import ProcessUri

from backend.tools.debug.echo import Echo, EchoArguments, EchoReturn

from tests.backend.utils_context import assert_ndprocess_serialization, given_context


@pytest.mark.asyncio
async def test_debug_echo_generics() -> None:
    assert Echo.arguments_type() == EchoArguments
    assert Echo.return_type() == EchoReturn


@pytest.mark.asyncio
async def test_debug_echo_invoke_success_with_dict() -> None:
    context = given_context()
    tool = Echo.tool()
    assert tool is not None

    process = await context.spawn_tool(
        tool,
        process_uri=ProcessUri.stub(),
        arguments={"text": "Hello, world!"},
    )
    assert isinstance(process.arguments, EchoArguments)

    assert_ndprocess_serialization(process)
    result = await process.wait()
    assert_ndprocess_serialization(process)

    assert isinstance(result, ProcessSuccess)
    assert isinstance(result.value, EchoReturn)
    assert result.value.content == "Hello, world!"
    assert process.result == result


@pytest.mark.asyncio
async def test_debug_echo_invoke_success_with_value() -> None:
    context = given_context()
    tool = Echo.tool()
    assert tool is not None

    process = await context.spawn_tool(
        tool,
        process_uri=ProcessUri.stub(),
        arguments=EchoArguments(text="Hello, world!"),
    )
    assert isinstance(process.arguments, EchoArguments)

    assert_ndprocess_serialization(process)
    result = await process.wait()
    assert_ndprocess_serialization(process)

    assert isinstance(result, ProcessSuccess)
    assert isinstance(result.value, EchoReturn)
    assert result.value.content == "Hello, world!"
    assert process.result == result


@pytest.mark.asyncio
async def test_debug_echo_invoke_failure_with_dict() -> None:
    context = given_context()
    tool = Echo.tool()
    assert tool is not None

    process = await context.spawn_tool(
        tool,
        process_uri=ProcessUri.stub(),
        arguments={"text": "ERROR: error message"},
    )
    assert isinstance(process.arguments, EchoArguments)

    assert_ndprocess_serialization(process)
    result = await process.wait()
    assert_ndprocess_serialization(process)

    assert isinstance(result, ProcessFailure)
    assert result.error.message == "error message"
    assert process.result == result


@pytest.mark.asyncio
async def test_debug_echo_invoke_failure_with_value() -> None:
    context = given_context()
    tool = Echo.tool()
    assert tool is not None

    process = await context.spawn_tool(
        tool,
        process_uri=ProcessUri.stub(),
        arguments=EchoArguments(text="ERROR: error message"),
    )
    assert isinstance(process.arguments, EchoArguments)

    assert_ndprocess_serialization(process)
    result = await process.wait()
    assert_ndprocess_serialization(process)

    assert isinstance(result, ProcessFailure)
    assert result.error.message == "error message"
    assert process.result == result
