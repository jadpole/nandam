import re
import tiktoken

from base.config import IMAGE_TOKENS_ESTIMATE
from base.core.values import parse_yaml_as
from base.utils.markdown import markdown_split_code


##
## Estimates
##


def estimate_tokens(text: str, num_media: int = 0) -> int:
    tokens_text: int = 0
    if text:
        encoding = tiktoken.get_encoding("o200k_base")  # GPT-4o
        tokens_text = len(encoding.encode(text, disallowed_special=()))
    return tokens_text + IMAGE_TOKENS_ESTIMATE * num_media


##
## XML modes and tool calls
##


def extract_xml_tags(tag: str, content: str) -> list[str]:
    """
    Extract the textual content of an XML tag.

    NOTE: Returns one item per instance of the tag.

    NOTE: If the tag is open but never closed, we assume that the LLM response
    was cut and return everything following `<tag>`.

    NOTE: Sometimes, Gemini Flash wraps the answer in a ```tag ... ``` block
    instead of an `<tag>` XML tag.  When this happens, we replace the code block
    with the corresponding XML tag and retry.
    """
    if (
        f"<{tag}>" not in content
        and content.count(f"```{tag}\n") == 1
        and content.count("```") == 2  # noqa: PLR2004
    ):
        content = content.replace(f"```{tag}", f"<{tag}>").replace("```", f"</{tag}>")
        return extract_xml_tags(tag, content)

    return [tag_content for _, tag_content in split_xml(content, (tag,), None)]


def extract_xml_struct_yaml[S](tag: str, type_: type[S], content: str) -> list[S]:
    try:
        return [
            parse_yaml_as(type_, tag_content)
            for tag_content in extract_xml_tags(tag, content)
        ]
    except ValueError:
        raise ValueError(  # noqa: B904
            f'Failed to parse as {type_.__name__}: """\n{content}\n"""'
        )


def split_xml[M: str](  # noqa: C901
    completion: str,
    modes: tuple[M, ...],
    default_mode: M | None,
) -> list[tuple[M, str]]:
    """
    NOTE: Almost all whitespace is preserved!  This allows the original text to
    be recovered when parsing fails.  Therefore, the caller should almost always
    send the text through `strip_keep_identation`.
    """
    shift_mode_regex = re.compile(r"(</?(?:" + "|".join(modes) + r")>)")

    sections: list[tuple[M | None, str]] = []
    partial_mode: M | None = None
    partial_text: str = ""

    split_parts = markdown_split_code(completion, True)
    for index, (part_type, part_text) in enumerate(split_parts):
        # Ignore mode shifts in code blocks and expressions, adding their text
        # to the chunk content.
        if part_type == "code_block":
            if index != 0:
                partial_text += "\n"
            partial_text += part_text
            if index != len(split_parts) - 1:
                partial_text += "\n"
            continue
        if part_type == "code_expr":
            partial_text += part_text
            continue

        for text_index, text_chunk in enumerate(shift_mode_regex.split(part_text)):
            # Tags commit the partial text and switch the chunk mode.
            if text_index % 2 == 1:
                # NOTE: Discard the whitespace between subsequent tags.
                if partial_text.strip() or partial_mode:
                    sections.append((partial_mode, partial_text))
                partial_text = ""

                # When the tag is closed, always switch to the default mode.
                # Often, it is immediately followed by the new open tag.
                if text_chunk.startswith("</"):
                    partial_mode = None
                else:
                    partial_mode = next(
                        mode for mode in modes if text_chunk == f"<{mode}>"
                    )

                continue

            # Add the text between mode tags to the chunk content.
            partial_text += text_chunk

    if partial_text.strip():
        sections.append((partial_mode, partial_text))

    # If `default_mode` is None, then `partial_mode` can be None, and we thus
    # need to discard non-tagged text.
    return [
        (actual_mode, text)
        for mode, text in sections
        if (actual_mode := mode or default_mode) is not None
    ]
