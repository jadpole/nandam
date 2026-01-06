import string
from base.core.strings import normalize_str


def test_normalize_str_default():
    assert normalize_str("  Hello @ World4.pdf!!") == "HelloWorld4pdf"


def test_normalize_str_identifier_snake_case():
    actual = normalize_str(
        "  Hello @ World4.pdf!!",
        allowed_special_chars="_",
        disallowed_replacement="_",
        other_replacements={
            **{c: f"_{c.lower()}" for c in string.ascii_uppercase},
            ".": "",
        },
        remove_duplicate_chars="_",
        remove_prefix_chars="_",
        remove_suffix_chars="_",
        unquote_url=True,
    )
    assert actual == "hello_world4pdf"


def test_normalize_str_identifier_snake_case_no_dedup():
    actual = normalize_str(
        "  Hello @ World4.pdf!!",
        allowed_special_chars="_",
        disallowed_replacement="_",
        other_replacements={
            **{c: f"_{c.lower()}" for c in string.ascii_uppercase},
            ".": "",
        },
        remove_prefix_chars="_",
        remove_suffix_chars="_",
        unquote_url=True,
    )
    assert actual == "hello__world4pdf"


def test_normalize_str_identifier_snake_case_no_dedup_prefix_suffix():
    actual = normalize_str(
        "  Hello @ World4.pdf!!",
        allowed_special_chars="_",
        disallowed_replacement="_",
        other_replacements={
            **{c: f"_{c.lower()}" for c in string.ascii_uppercase},
            ".": "",
        },
        unquote_url=True,
    )
    assert actual == "_hello__world4pdf_"


def test_normalize_str_identifier_snake_case_no_prefix_suffix():
    actual = normalize_str(
        "  Hello @ World4.pdf!!",
        allowed_special_chars="_",
        disallowed_replacement="_",
        other_replacements={
            **{c: f"_{c.lower()}" for c in string.ascii_uppercase},
            ".": "",
        },
        remove_duplicate_chars="_",
        unquote_url=True,
    )
    assert actual == "_hello_world4pdf_"


def test_normalize_str_identifier_kebab_case():
    actual = normalize_str(
        "  Hello @ World4.pdf!!",
        allowed_special_chars="-",
        other_replacements={" ": "-", ".": "-", "_": "-"},
        remove_duplicate_chars="-",
        remove_prefix_chars="-",
        remove_suffix_chars="-",
    )
    assert actual == "Hello-World4-pdf"


def test_normalize_str_filename():
    actual = normalize_str(
        "  Hello %40 World4%2Epdf!!",
        allowed_special_chars="-._",
        disallowed_replacement="_",
        remove_duplicate_chars="-._",
        remove_prefix_chars="-_",
        remove_suffix_chars="-._",
        unquote_url=True,
    )
    assert actual == "Hello_World4.pdf"
