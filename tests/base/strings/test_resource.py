import pytest

from pydantic import BaseModel, TypeAdapter

from base.core.schema import as_jsonschema
from base.core.values import as_json
from base.resources.aff_body import AffBody, AffBodyChunk, AffBodyMedia
from base.resources.aff_collection import AffCollection
from base.resources.aff_file import AffFile
from base.resources.aff_plain import AffPlain
from base.strings.file import REGEX_FILENAME
from base.strings.resource import (
    Affordance,
    AffordanceUri,
    ExternalUri,
    KnowledgeSuffix,
    KnowledgeUri,
    Observable,
    ObservableUri,
    Reference,
    REGEX_EXTERNAL_URI,
    REGEX_KNOWLEDGE_URI,
    REGEX_REFERENCE,
    REGEX_RESOURCE_URI,
    REGEX_SUFFIX_FULL_URI,
    REGEX_WEB_URL,
    WebUrl,
)


##
## Reference
##


class DemoReference(BaseModel):
    reference: Reference


EXPECTED_SCHEMA_REFERENCE = {
    "type": "string",
    "pattern": f"^{REGEX_REFERENCE}$",
    "examples": [
        "https://example.com",
        "https://example.com/mypage.html?queryParam=42#fragment",
        "https://mycompany.atlassian.net/browse/PROJ-123",
        "ndk://jira/issue/PROJ-123",
        "ndk://stub/-/dir/example",
        "ndk://stub/-/dir/example/$body",
        "ndk://stub/-/dir/example/$chunk",
        "ndk://stub/-/dir/example/$chunk/01/02",
        "ndk://stub/-/dir/example/$collection",
        "ndk://stub/-/dir/example/$file",
        "ndk://stub/-/dir/example/$file/figures/filename.png",
        "ndk://stub/-/dir/example/$file/figures/image.png",
        "ndk://stub/-/dir/example/$file/main.tex",
        "ndk://stub/-/dir/example/$media",
        "ndk://stub/-/dir/example/$media/figure.png",
        "ndk://stub/-/dir/example/$media/figures/image.png",
        "ndk://stub/-/dir/example/$plain",
    ],
}


def test_reference_jsonschema() -> None:
    schema = as_jsonschema(Reference)
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema == EXPECTED_SCHEMA_REFERENCE


def test_reference_field_jsonschema() -> None:
    schema = as_jsonschema(DemoReference)
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema == {
        "type": "object",
        "properties": {"reference": EXPECTED_SCHEMA_REFERENCE},
        "required": ["reference"],
        "additionalProperties": False,
    }


@pytest.mark.parametrize("value", Reference._schema_examples())
def test_reference_decode_ok_examples(value: str) -> None:
    actual = TypeAdapter(Reference).validate_python(value)
    print(actual)
    assert isinstance(actual, Reference)
    assert type(actual) is not Reference  # subclass instanciated
    assert str(actual) == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(Reference).validate_json(value_json)
    assert type(actual_json) is type(actual)
    assert actual_json == actual

    actual_parsed = Reference.decode(value)
    assert actual_parsed == actual


##
## External URI
##


def test_externaluri_jsonschema() -> None:
    schema = as_jsonschema(ExternalUri)
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema == {
        "type": "string",
        "pattern": f"^{REGEX_EXTERNAL_URI}$",
        "examples": [
            "https://example.com",
            "https://example.com/mypage.html?queryParam=42#fragment",
            "https://mycompany.atlassian.net/browse/PROJ-123",
        ],
    }


@pytest.mark.parametrize("value", ExternalUri._schema_examples())
def test_externaluri_decode_ok_examples(value: str) -> None:
    actual = TypeAdapter(ExternalUri).validate_python(value)
    print(actual)
    assert isinstance(actual, ExternalUri)
    assert type(actual) is not ExternalUri  # subclass instanciated
    assert str(actual) == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(ExternalUri).validate_json(value_json)
    assert type(actual_json) is type(actual)
    assert actual_json == actual

    actual_parsed = ExternalUri.decode(value)
    assert actual_parsed == actual


##
## Web URL
##


def test_weburl_jsonschema() -> None:
    schema = as_jsonschema(ExternalUri)
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema == {
        "type": "string",
        "pattern": f"^{REGEX_WEB_URL}$",
        "examples": [
            "https://example.com",
            "https://example.com/mypage.html?queryParam=42#fragment",
            "https://mycompany.atlassian.net/browse/PROJ-123",
        ],
    }


@pytest.mark.parametrize("value", WebUrl._schema_examples())
def test_weburl_decode_ok_examples(value: str) -> None:
    actual = TypeAdapter(WebUrl).validate_python(value)
    print(actual)
    assert type(actual) is WebUrl
    assert str(actual) == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(WebUrl).validate_json(value_json)
    assert type(actual_json) is WebUrl
    assert actual_json == actual

    actual_parsed = Reference.decode(value)
    assert actual_parsed == actual


@pytest.mark.parametrize(
    "value",
    [example.removeprefix("https://") for example in WebUrl._schema_examples()],
)
def test_weburl_decode_err_non_url(value: str):
    with pytest.raises(ValueError, match="invalid WebUrl: expected pattern"):
        TypeAdapter(WebUrl).validate_python(value)


@pytest.mark.parametrize(
    "value",
    ["", "https://", "https://#", "https://?param=42"],
)
def test_weburl_decode_err_empty(value: str):
    with pytest.raises(ValueError, match="invalid WebUrl: expected pattern"):
        TypeAdapter(WebUrl).validate_python(value)


def test_weburl_decode_testrail():
    value = "https://testrail.mycompany.com/index.php?/suites/view/4252"
    url = WebUrl.decode(value)
    assert str(url) == value
    assert url.domain == "testrail.mycompany.com"
    assert url.path == "index.php"
    assert url.query_path == "/suites/view/4252"
    assert url.query == []
    assert url.fragment == ""


def test_weburl_decode_testrail_params():
    value = "https://testrail.mycompany.com/index.php?/suites/view/4252&group_by=cases:section_id&group_order=asc"
    url = WebUrl.decode(value)
    assert str(url) == value.replace("cases:section_id", "cases%3Asection_id")
    assert url.domain == "testrail.mycompany.com"
    assert url.path == "index.php"
    assert url.query_path == "/suites/view/4252"
    assert url.get_query("group_by") == "cases:section_id"
    assert url.get_query("group_order") == "asc"
    assert url.fragment == ""


def test_weburl_decode_testrail_params_encoded():
    value = "https://testrail.mycompany.com/index.php?/suites/view/4252&group_by=cases%3Asection_id&group_order=asc"
    url = WebUrl.decode(value)
    assert str(url) == value
    assert url.domain == "testrail.mycompany.com"
    assert url.path == "index.php"
    assert url.query_path == "/suites/view/4252"
    assert url.get_query("group_by") == "cases:section_id"  # Decodes query params.
    assert url.get_query("group_order") == "asc"
    assert url.fragment == ""


##
## Knowledge URI
##


class DemoKnowledgeUri(BaseModel):
    uri: KnowledgeUri


def test_knowledgeuri_jsonschema():
    schema = TypeAdapter(KnowledgeUri).json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema["type"] == "string"
    assert schema["title"] == "KnowledgeUri"
    assert schema["pattern"] == rf"^{REGEX_KNOWLEDGE_URI}$"


def test_knowledgeuri_field_jsonschema():
    schema = DemoKnowledgeUri.model_json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert "uri" in schema["properties"]
    assert "uri" in schema["required"]
    assert schema["properties"]["uri"] == {"$ref": "#/$defs/KnowledgeUri"}
    assert schema["$defs"]["KnowledgeUri"]["type"] == "string"
    assert schema["$defs"]["KnowledgeUri"]["title"] == "KnowledgeUri"
    assert schema["$defs"]["KnowledgeUri"]["pattern"] == rf"^{REGEX_KNOWLEDGE_URI}$"


@pytest.mark.parametrize("value", KnowledgeUri._schema_examples())
def test_knowledgeuri_decode_ok_examples(value: str) -> None:
    actual = TypeAdapter(KnowledgeUri).validate_python(value)
    print(actual)
    assert isinstance(actual, KnowledgeUri)
    assert type(actual) is not KnowledgeUri  # subclass instanciated
    assert str(actual) == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(KnowledgeUri).validate_json(value_json)
    assert type(actual_json) is type(actual)
    assert actual_json == actual

    actual_parsed = KnowledgeUri.decode(value)
    assert actual_parsed == actual


@pytest.mark.parametrize("value", ["", "ndk://", "ndk://jira", "ndk://jira/issue"])
def test_knowledgeuri_decode_err_empty(value: str):
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(KnowledgeUri).validate_python(value)


def test_knowledgeuri_decode_err_missing_affordance_slash():
    """
    Decode fails when the URI is "{ResourceUri}$affordance" instead of the
    expected "{ResourceUri}/$affordance".
    """
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(KnowledgeUri).validate_python("ndk://stub/-/dir/example$body")


@pytest.mark.parametrize(
    "value",
    [uri.removeprefix("ndk://") for uri in KnowledgeUri._schema_examples()],
)
def test_knowledgeuri_decode_err_missing_scheme(value: str):
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(KnowledgeUri).validate_python(value)


@pytest.mark.parametrize(
    "value",
    [f"{uri}/" for uri in KnowledgeUri._schema_examples()],
)
def test_knowledgeuri_decode_err_trailing_slash(value: str):
    """Rejects URIs with a trailing slash."""
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(KnowledgeUri).validate_python(value)


def test_knowledgeuri_decode_err_trailing_slash_dollar():
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(KnowledgeUri).validate_python("ndk://stub/-/dir/example/$")


def test_knowledgeuri_decode_err_unknown_affordance():
    with pytest.raises(ValueError, match="invalid KnowledgeUri: unknown suffix"):
        TypeAdapter(KnowledgeUri).validate_python("ndk://stub/-/dir/example/$unknown")


@pytest.mark.parametrize(
    "value",
    [
        "ndk://#",
        "ndk://jira/#",
        "ndk://jira/issue/PROJ/#",
        "ndk://jira/issue/PROJ#",
        "ndk://jira/issue/PROJ#-528",
        "ndk://jira/issue/PROJ#528",
    ],
)
def test_knowledgeuri_decode_err_special_chars(value: str):
    """Although Web URLs are quite permissive, Knowledge URIs are not."""
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(KnowledgeUri).validate_python(value)


##
## Affordance URI
##


class DemoAffordanceUri(BaseModel):
    uri: AffordanceUri


class DemoAffordanceUriBody(BaseModel):
    uri: AffordanceUri[AffBody]


def test_affordanceuri_jsonschema():
    schema = TypeAdapter(AffordanceUri).json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema["type"] == "string"
    assert schema["title"] == "AffordanceUri"
    assert schema["pattern"] == rf"^{REGEX_SUFFIX_FULL_URI}$"


def test_affordanceuri_field_jsonschema():
    schema = DemoAffordanceUri.model_json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert "uri" in schema["properties"]
    assert "uri" in schema["required"]
    assert schema["properties"]["uri"] == {"$ref": "#/$defs/AffordanceUri"}
    assert schema["$defs"]["AffordanceUri"]["type"] == "string"
    assert schema["$defs"]["AffordanceUri"]["title"] == "AffordanceUri"
    assert schema["$defs"]["AffordanceUri"]["pattern"] == rf"^{REGEX_SUFFIX_FULL_URI}$"


def test_affordanceuri_body_jsonschema():
    schema = TypeAdapter(AffordanceUri[AffBody]).json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema["type"] == "string"
    assert schema["title"] == "ResourceBodyUri"
    assert schema["pattern"] == rf"^{REGEX_RESOURCE_URI}/\$body$"


def test_affordanceuri_body_field_jsonschema():
    schema = DemoAffordanceUriBody.model_json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert "uri" in schema["properties"]
    assert "uri" in schema["required"]
    assert schema["properties"]["uri"] == {"$ref": "#/$defs/AffordanceUri_AffBody_"}
    assert schema["$defs"]["AffordanceUri_AffBody_"]["type"] == "string"
    assert schema["$defs"]["AffordanceUri_AffBody_"]["title"] == "ResourceBodyUri"
    assert (
        schema["$defs"]["AffordanceUri_AffBody_"]["pattern"]
        == rf"^{REGEX_RESOURCE_URI}/\$body$"
    )


@pytest.mark.parametrize("value", AffordanceUri._schema_examples())
def test_affordanceuri_decode_ok_examples(value: str):
    adapter = TypeAdapter(AffordanceUri)
    actual = adapter.validate_python(value)
    print(actual)
    assert type(actual) is AffordanceUri
    assert isinstance(actual.suffix, Affordance)
    assert type(actual.suffix) is not Affordance
    assert str(actual) == value

    value_json = f'"{value}"'.encode()
    actual_json = adapter.validate_json(value_json)
    assert type(actual_json) is AffordanceUri
    assert isinstance(actual_json.suffix, Affordance)
    assert type(actual_json.suffix) is not Affordance
    assert actual_json == actual

    parsed = Reference.decode(value)
    assert type(parsed) is AffordanceUri or type(parsed) is ObservableUri
    assert isinstance(parsed.suffix, Affordance)
    assert type(parsed.suffix) is not Affordance

    if isinstance(parsed, ObservableUri):
        parsed = parsed.affordance_uri()
    assert parsed == actual

    constructed = actual.resource_uri().child_affordance(actual.suffix)
    assert str(constructed) == value
    assert constructed == parsed


##
## Affordance URI - Variants
##


def _run_test_affordanceuri(aff_type: type[Affordance], value: str) -> None:
    adapter = TypeAdapter(AffordanceUri)
    actual = adapter.validate_python(value)
    print(actual)
    assert type(actual) is AffordanceUri
    assert type(actual.suffix) is aff_type
    assert str(actual) == value

    value_json = f'"{value}"'.encode()
    actual_json = adapter.validate_json(value_json)
    assert type(actual_json) is AffordanceUri
    assert type(actual_json.suffix) is aff_type
    assert actual_json == actual

    parsed = Reference.decode(value)
    assert type(parsed) is AffordanceUri or type(parsed) is ObservableUri
    assert type(parsed.suffix) is aff_type

    if isinstance(parsed, ObservableUri):
        parsed = parsed.affordance_uri()
    assert parsed == actual

    decoded = AffordanceUri[aff_type].decode(value)
    assert decoded == actual


@pytest.mark.parametrize("value", AffordanceUri[AffBody]._schema_examples())
def test_affordanceuri_body_decode_ok_examples(value: str):
    _run_test_affordanceuri(AffBody, value)


@pytest.mark.parametrize("value", AffordanceUri[AffCollection]._schema_examples())
def test_affordanceuri_collection_decode_ok_examples(value: str):
    _run_test_affordanceuri(AffCollection, value)


@pytest.mark.parametrize("value", AffordanceUri[AffFile]._schema_examples())
def test_affordanceuri_file_decode_ok_examples(value: str):
    _run_test_affordanceuri(AffFile, value)


@pytest.mark.parametrize("value", AffordanceUri[AffPlain]._schema_examples())
def test_affordanceuri_plain_decode_ok_examples(value: str):
    _run_test_affordanceuri(AffPlain, value)


##
## Observable URI
##


class DemoObservableUri(BaseModel):
    uri: ObservableUri


class DemoObservableUriBody(BaseModel):
    uri: ObservableUri[AffBody]


class DemoObservableUriMedia(BaseModel):
    uri: ObservableUri[AffBodyMedia]


def test_observableuri_jsonschema():
    schema = TypeAdapter(ObservableUri).json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema["type"] == "string"
    assert schema["title"] == "ObservableUri"
    assert schema["pattern"] == rf"^{REGEX_SUFFIX_FULL_URI}$"


def test_observableuri_field_jsonschema():
    schema = DemoObservableUri.model_json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert "uri" in schema["properties"]
    assert "uri" in schema["required"]
    assert schema["properties"]["uri"] == {"$ref": "#/$defs/ObservableUri"}
    assert schema["$defs"]["ObservableUri"]["type"] == "string"
    assert schema["$defs"]["ObservableUri"]["title"] == "ObservableUri"
    assert schema["$defs"]["ObservableUri"]["pattern"] == rf"^{REGEX_SUFFIX_FULL_URI}$"


def test_observableuri_body_jsonschema():
    schema = TypeAdapter(ObservableUri[AffBody]).json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema["type"] == "string"
    assert schema["title"] == "ResourceBodyUri"
    assert schema["pattern"] == rf"^{REGEX_RESOURCE_URI}/\$body$"


def test_observableuri_body_field_jsonschema():
    schema = DemoObservableUriBody.model_json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert "uri" in schema["properties"]
    assert "uri" in schema["required"]
    assert schema["properties"]["uri"] == {"$ref": "#/$defs/ObservableUri_AffBody_"}
    assert schema["$defs"]["ObservableUri_AffBody_"]["type"] == "string"
    assert schema["$defs"]["ObservableUri_AffBody_"]["title"] == "ResourceBodyUri"
    assert (
        schema["$defs"]["ObservableUri_AffBody_"]["pattern"]
        == rf"^{REGEX_RESOURCE_URI}/\$body$"
    )


def test_observableuri_media_jsonschema():
    schema = TypeAdapter(ObservableUri[AffBodyMedia]).json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema["type"] == "string"
    assert schema["title"] == "ResourceBodyMediaUri"
    assert schema["pattern"] == rf"^{REGEX_RESOURCE_URI}/\$media(?:/{REGEX_FILENAME})*$"


def test_observableuri_media_field_jsonschema():
    schema = DemoObservableUriMedia.model_json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert "uri" in schema["properties"]
    assert "uri" in schema["required"]
    assert schema["properties"]["uri"] == {
        "$ref": "#/$defs/ObservableUri_AffBodyMedia_"
    }
    assert schema["$defs"]["ObservableUri_AffBodyMedia_"]["type"] == "string"
    assert (
        schema["$defs"]["ObservableUri_AffBodyMedia_"]["title"]
        == "ResourceBodyMediaUri"
    )
    assert (
        schema["$defs"]["ObservableUri_AffBodyMedia_"]["pattern"]
        == rf"^{REGEX_RESOURCE_URI}/\$media(?:/{REGEX_FILENAME})*$"
    )


@pytest.mark.parametrize("value", ObservableUri._schema_examples())
def test_observableuri_decode_ok_examples(value: str):
    adapter = TypeAdapter(ObservableUri)
    actual = adapter.validate_python(value)
    print(actual)
    assert type(actual)
    assert isinstance(actual.suffix, Observable)
    assert type(actual.suffix) is not Affordance
    assert str(actual) == value
    assert actual.is_child_or(actual.affordance_uri())

    value_json = f'"{value}"'.encode()
    actual_json = adapter.validate_json(value_json)
    assert type(actual_json) is ObservableUri
    assert isinstance(actual_json.suffix, Observable)
    assert type(actual_json.suffix) is not Observable
    assert actual_json == actual

    parsed = Reference.decode(value)
    assert type(parsed) is ObservableUri
    assert isinstance(parsed.suffix, Observable)
    assert type(parsed.suffix) is not Observable
    assert parsed == actual

    constructed = actual.resource_uri().child_observable(actual.suffix)
    assert str(constructed) == value
    assert constructed == actual


##
## Observable URI - Variants
##


def _run_test_observableuri(aff_type: type[Observable], value: str) -> None:
    adapter = TypeAdapter(ObservableUri)
    actual = adapter.validate_python(value)
    print(actual)
    assert type(actual) is ObservableUri
    assert type(actual.suffix) is aff_type
    assert str(actual) == value
    assert actual.is_child_or(actual.affordance_uri())

    value_json = f'"{value}"'.encode()
    actual_json = adapter.validate_json(value_json)
    assert type(actual_json) is ObservableUri
    assert type(actual_json.suffix) is aff_type
    assert actual_json == actual

    parsed = Reference.decode(value)
    assert type(parsed) is ObservableUri or type(parsed) is AffordanceUri
    assert type(parsed.suffix) is aff_type
    assert str(parsed) == value
    if not isinstance(parsed, AffordanceUri):
        assert parsed == actual

    decoded = ObservableUri[aff_type].decode(value)
    assert decoded == actual


@pytest.mark.parametrize("value", ObservableUri[AffBody]._schema_examples())
def test_observableuri_body_decode_ok_examples(value: str):
    _run_test_observableuri(AffBody, value)


@pytest.mark.parametrize("value", ObservableUri[AffBodyChunk]._schema_examples())
def test_observableuri_chunk_decode_ok_examples(value: str):
    _run_test_observableuri(AffBodyChunk, value)


@pytest.mark.parametrize("value", ObservableUri[AffBodyMedia]._schema_examples())
def test_observableuri_media_decode_ok_examples(value: str):
    _run_test_observableuri(AffBodyMedia, value)


@pytest.mark.parametrize("value", ObservableUri[AffCollection]._schema_examples())
def test_observableuri_collection_decode_ok_examples(value: str):
    _run_test_observableuri(AffCollection, value)


@pytest.mark.parametrize("value", ObservableUri[AffFile]._schema_examples())
def test_observableuri_file_decode_ok_examples(value: str):
    _run_test_observableuri(AffFile, value)


@pytest.mark.parametrize("value", ObservableUri[AffPlain]._schema_examples())
def test_observableuri_plain_decode_ok_examples(value: str):
    _run_test_observableuri(AffPlain, value)


##
## Self URI
##


def _run_test_selfuri(aff_type: type, value: str) -> None:
    adapter = TypeAdapter(KnowledgeSuffix)
    actual = adapter.validate_python(value)
    print(actual)
    assert type(actual) is aff_type
    assert str(actual) == value

    value_json = f'"{value}"'.encode()
    actual_json = adapter.validate_json(value_json)
    assert type(actual_json) is aff_type
    assert actual_json == actual

    decoded = aff_type.decode(value)
    assert decoded == actual


@pytest.mark.parametrize("value", AffBody._schema_examples())
def test_selfuri_body_decode_ok_examples(value: str):
    _run_test_selfuri(AffBody, value)


@pytest.mark.parametrize("value", AffBodyChunk._schema_examples())
def test_selfuri_chunk_decode_ok_examples(value: str):
    _run_test_selfuri(AffBodyChunk, value)


@pytest.mark.parametrize("value", AffBodyMedia._schema_examples())
def test_selfuri_media_decode_ok_examples(value: str):
    _run_test_selfuri(AffBodyMedia, value)


@pytest.mark.parametrize("value", AffCollection._schema_examples())
def test_selfuri_collection_decode_ok_examples(value: str):
    _run_test_selfuri(AffCollection, value)


@pytest.mark.parametrize("value", AffFile._schema_examples())
def test_selfuri_file_decode_ok_examples(value: str):
    _run_test_selfuri(AffFile, value)


@pytest.mark.parametrize("value", AffPlain._schema_examples())
def test_selfuri_plain_decode_ok_examples(value: str):
    _run_test_selfuri(AffPlain, value)
