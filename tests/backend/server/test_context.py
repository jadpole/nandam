import pytest

from base.strings.process import ProcessName, ProcessUri

from backend.models.process_result import ProcessSuccess

from tests.backend.utils_context import (
    StubProcess,
    assert_ndprocess_serialization,
    given_context,
)


def test_ndprocess_generic_arg_default():
    assert StubProcess.arguments_type() is None


def test_ndprocess_generic_ret_default():
    assert StubProcess.return_type() is None


@pytest.mark.asyncio
async def test_ndprocess_dict_based_invoke():
    context = given_context()
    tool = StubProcess.tool(name=ProcessName.decode("stub_process"))
    assert tool is not None

    process = await context.spawn_tool(
        tool,
        process_uri=ProcessUri.stub(),
        arguments={"example": "argument"},
    )
    assert process.owner == "svc-stub"
    assert process.name == "stub_process"
    assert process.mode == "internal"
    assert isinstance(process.arguments, dict)
    assert process.arguments == {"example": "argument"}

    assert_ndprocess_serialization(process)
    await process._on_update(result={"example": "result"})
    result = await process.wait()
    assert_ndprocess_serialization(process)

    assert isinstance(result, ProcessSuccess)
    assert isinstance(result.value, dict)
    assert result.value == {"example": "result"}
    assert process.result == result
