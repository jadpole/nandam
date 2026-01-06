import json
import pytest

from pydantic import BaseModel
from typing import Any, Literal, TypeVar

from base.strings.file import FileName
from base.utils.completion import (
    extract_xml_struct_yaml,
    extract_xml_tags,
    split_xml,
)


##
## extract_xml_tags
##


def test_extract_xml_tags_empty() -> None:
    """Test extracting XML tags from an empty string"""
    content = ""
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == []


def test_extract_xml_tags_no_tags() -> None:
    """Test extracting XML tags when none exist"""
    content = "Text with no XML tags."
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == []


def test_extract_xml_tags_single() -> None:
    """Test extracting a single XML tag"""
    content = "Before <tag>content</tag> after."
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == ["content"]


def test_extract_xml_tags_multiple() -> None:
    """Test extracting multiple XML tags"""
    content = "<tag>content1</tag> middle <tag>content2</tag>"
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == ["content1", "content2"]


def test_extract_xml_tags_nested() -> None:
    """Test extracting XML tags with nested content"""
    content = "<tag>outer <inner>nested</inner> text</tag>"
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == ["outer <inner>nested</inner> text"]


def test_extract_xml_tags_multiline() -> None:
    """Test extracting XML tags with multiline content"""
    content = "<tag>\nline1\nline2\n</tag>"
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == ["\nline1\nline2\n"]


def test_extract_xml_tags_with_whitespace() -> None:
    """Test extracting XML tags with whitespace around content"""
    content = "<tag>  content with spaces  </tag>"
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == ["  content with spaces  "]


def test_extract_xml_tags_from_code_block() -> None:
    """Test extracting XML tags from code block format"""
    content = "```tag\ncontent\n```"
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == ["\ncontent\n"]


def test_extract_xml_tags_mixed_format() -> None:
    """Test extracting XML tags with mixed format"""
    content = "<tag>xml format</tag> and ```tag\ncode block format\n```"
    tag = "tag"
    result = extract_xml_tags(tag, content)
    assert result == ["xml format"]


##
## extract_xml_struct_yaml
##


T = TypeVar("T")


class SimpleModel(BaseModel):
    name: FileName
    value: int


def test_extract_xml_struct_yaml_empty() -> None:
    """Test extracting YAML from XML with no content"""
    content = ""
    result = extract_xml_struct_yaml("tag", dict[str, Any], content)
    assert result == []


def test_extract_xml_struct_yaml_single() -> None:
    """Test extracting YAML from a single XML tag"""
    content = """\
<tag>
name: test
value: 42
</tag>\
"""
    result = extract_xml_struct_yaml("tag", SimpleModel, content)

    assert len(result) == 1
    assert type(result[0]) is SimpleModel
    assert type(result[0].name) is FileName

    assert result[0].name == "test"
    assert result[0].value == 42


def test_extract_xml_struct_yaml_multiple() -> None:
    """Test extracting YAML from multiple XML tags"""
    content = """\
<tag>
name: first
value: 1
</tag>
<tag>
name: second
value: 2
</tag>\
"""
    result = extract_xml_struct_yaml("tag", SimpleModel, content)

    assert len(result) == 2
    assert type(result[0]) is SimpleModel
    assert type(result[1]) is SimpleModel
    assert type(result[0].name) is FileName
    assert type(result[1].name) is FileName

    assert result[0].name == "first"
    assert result[0].value == 1
    assert result[1].name == "second"
    assert result[1].value == 2


def test_extract_xml_struct_yaml_error_invalid() -> None:
    """Test extracting invalid YAML from XML tags"""
    content = """\
<tag>
name: 'invalid#identifier'
value: 1
</tag>\
"""

    with pytest.raises(ValueError, match="Failed to parse as SimpleModel"):
        extract_xml_struct_yaml("tag", SimpleModel, content)


##
## split_xml
##


Mode = Literal["think", "answer", "code"]


def _run_split_xml(
    completion: str,
    modes: tuple[Mode, ...],
    default_mode: Mode | None,
    expected: list[tuple[Mode, str]],
) -> None:
    actual = split_xml(completion, modes, default_mode)
    print("Actual: " + json.dumps([(mode, text) for mode, text in actual], indent=2))
    print(
        "Expected: " + json.dumps([(mode, text) for mode, text in expected], indent=2)
    )
    assert actual == expected


def test_split_xml_empty() -> None:
    """Test splitting an empty string with XML tags"""
    completion = ""
    modes = ("think", "answer")
    default_mode = "answer"
    expected: list[tuple[Mode, str]] = []
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_no_tags() -> None:
    """Test splitting string with no XML tags"""
    completion = "This is just text without any XML tags."
    modes = ("think", "answer")
    default_mode = "answer"
    expected: list[tuple[Mode, str]] = [("answer", completion)]
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_no_tags_no_default() -> None:
    """Test splitting string with no XML tags and no default mode"""
    completion = "This is just text without any XML tags."
    modes = ("think", "answer")
    default_mode = None
    expected: list[tuple[Mode, str]] = []
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_single_tag() -> None:
    """Test splitting string with a single XML tag"""
    completion = "Before <think>thinking content</think> after."
    modes = ("think", "answer")
    default_mode = "answer"
    expected: list[tuple[Mode, str]] = [
        ("answer", "Before "),
        ("think", "thinking content"),
        ("answer", " after."),
    ]
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_multiple_tags() -> None:
    """Test splitting string with multiple XML tags"""
    completion = (
        "Start <think>thinking 1</think> "
        "middle <answer>answer</answer> "
        "end <think>thinking 2</think>"
    )
    modes = ("think", "answer")
    default_mode = None
    expected: list[tuple[Mode, str]] = [
        ("think", "thinking 1"),
        ("answer", "answer"),
        ("think", "thinking 2"),
    ]
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_nested_code() -> None:
    """Test splitting string with XML tags containing code blocks"""
    completion = """\
<think>
Let me think about this problem.

```python
def solution():
    return 42
```

The solution seems correct.
</think>

<answer>The answer is:

```python
def solution():
    return 42
```
</answer>\
"""
    modes = ("think", "answer")
    default_mode = None
    expected: list[tuple[Mode, str]] = [
        (
            "think",
            "\nLet me think about this problem.\n\n```python\ndef solution():\n    return 42\n```\n\nThe solution seems correct.\n",
        ),
        (
            "answer",
            "The answer is:\n\n```python\ndef solution():\n    return 42\n```\n",
        ),
    ]
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_with_code_expressions() -> None:
    """Test splitting string with XML tags containing code expressions"""
    completion = """\
<think>
Let's use the `print()` function here.
</think>

<answer>
Use `print("Hello")` to display text.
</answer>\
"""
    modes = ("think", "answer")
    default_mode = None
    expected: list[tuple[Mode, str]] = [
        ("think", "\nLet's use the `print()` function here.\n"),
        ("answer", '\nUse `print("Hello")` to display text.\n'),
    ]
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_with_unclosed_tag() -> None:
    """Test splitting string with an unclosed XML tag at the end"""
    completion = "Before <think>thinking content</think> middle <answer>unclosed"
    modes = ("think", "answer")
    default_mode = "answer"
    expected: list[tuple[Mode, str]] = [
        ("answer", "Before "),
        ("think", "thinking content"),
        ("answer", " middle "),
        ("answer", "unclosed"),
    ]
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_with_code_blocks() -> None:
    """Test splitting string with code blocks outside XML tags"""
    completion = """\
<think>Thinking about code</think>

```python
def function():
    pass
```

<answer>Here's the answer</answer>\
"""
    modes = ("think", "answer")
    default_mode = "answer"
    expected: list[tuple[Mode, str]] = [
        ("think", "Thinking about code"),
        ("answer", "\n\n```python\ndef function():\n    pass\n```\n\n"),
        ("answer", "Here's the answer"),
    ]
    _run_split_xml(completion, modes, default_mode, expected)


def test_split_xml_complex_mixed() -> None:
    """Test splitting complex string with mixed content"""
    completion = """\
<think>
First thinking block with `code expression`.

```python
def example():
    return "This is in thinking"
```
</think>

Some text in between without tags.

<answer>
The answer with `another expression`.

```
Plain code block
```

More answer text.
</answer>

<think>Final thinking</think>\
"""
    modes = ("think", "answer")
    default_mode = "answer"
    expected: list[tuple[Mode, str]] = [
        (
            "think",
            '\nFirst thinking block with `code expression`.\n\n```python\ndef example():\n    return "This is in thinking"\n```\n',
        ),
        (
            "answer",
            "\n\nSome text in between without tags.\n\n",
        ),
        (
            "answer",
            "\nThe answer with `another expression`.\n\n```\nPlain code block\n```\n\nMore answer text.\n",
        ),
        (
            "think",
            "Final thinking",
        ),
    ]
    _run_split_xml(completion, modes, default_mode, expected)
