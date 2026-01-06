from base.core.values import as_yaml
from base.models.content import ContentBlob, ContentText, PartLink
from base.models.rendered import Rendered
from base.resources.aff_body import (
    AffBody,
    AffBodyChunk,
    BundleBody,
    ObsBody,
    ObsBodySection,
    ObsChunk,
    ObsMedia,
)
from base.resources.aff_collection import AffCollection, BundleCollection, ObsCollection
from base.resources.aff_file import AffFile, BundleFile, ObsFile
from base.resources.aff_plain import AffPlain, BundlePlain, ObsPlain
from base.resources.metadata import ObservationSection
from base.resources.observation import ObservationBundle
from base.strings.data import DataUri, MimeType
from base.strings.resource import ObservableUri, ResourceUri, WebUrl


def _run_test_bundle_render(
    *,
    bundle: ObservationBundle,
    embeds: list[ObservableUri],
    limit_media: int,
    supports_media: list[MimeType],
    expected: list[str],
) -> None:
    observations = bundle.observations()
    rendered = Rendered.render(
        ContentText.new([PartLink.new("embed", None, embed) for embed in embeds]),
        observations,
    )
    rendered_parts = rendered.as_llm_inline(
        supports_media=supports_media,
        limit_media=limit_media,
    )

    rendered_list = [
        f"@BLOB {part.uri}" if isinstance(part, ContentBlob) else part
        for part in rendered_parts
    ]
    print("<rendered>\n" + "\n---\n".join(rendered_list) + "\n</rendered>")
    assert rendered_list == expected


##
## Body - Single
##


def _given_bundle_single() -> BundleBody:
    resource_str = "ndk://public/arxiv/2303.11366v2"
    return BundleBody.make_single(
        resource_uri=ResourceUri.decode(resource_str),
        mode="markdown",
        media=[
            ObsMedia.stub(media_uri)
            for media_uri in [
                "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf",
            ]
        ],
        text="""\
# Introduction

![](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf)

# Related work

# Reflexion: reinforcement via verbal reflection

![image](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf)

# Experiments

## Sequential decision making: ALFWorld

#### Results

![](ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf)

![](ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf)

#### Analysis

## Reasoning: HotpotQA

#### Results

![](ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf)

![](ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf)

![](ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf)

#### Analysis

## Programming

# Limitations

## WebShop Limitation

![](ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf)

# Programming

# Reasoning

## Full example

## Chain-of-Thought + Reflexion

## HotPotQA Chain-of-Thought (GT) + Reflexion

## HotPotQA episodic memory (EPM) ablation prompts

### (EPM) Chain-of-Thought + Reflexion

### (EPM) Chain-of-Thought (GT) + Reflexion\
""",
        description="chunk description",
    )


RENDERED_SINGLE_01_TEXT = """\
<document uri="ndk://public/arxiv/2303.11366v2/$body">
# Introduction\
"""
RENDERED_SINGLE_02_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_SINGLE_03_TEXT = """\
# Related work

# Reflexion: reinforcement via verbal reflection\
"""
RENDERED_SINGLE_04_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_SINGLE_05_TEXT = """\
# Experiments

## Sequential decision making: ALFWorld

#### Results\
"""
RENDERED_SINGLE_06_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_SINGLE_07_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_SINGLE_08_TEXT = """\
#### Analysis

## Reasoning: HotpotQA

#### Results\
"""
RENDERED_SINGLE_09_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""

RENDERED_SINGLE_10_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_SINGLE_11_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_SINGLE_12_TEXT = """\
#### Analysis

## Programming

# Limitations

## WebShop Limitation\
"""
RENDERED_SINGLE_13_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_SINGLE_14_TEXT = """\
# Programming

# Reasoning

## Full example

## Chain-of-Thought + Reflexion

## HotPotQA Chain-of-Thought (GT) + Reflexion

## HotPotQA episodic memory (EPM) ablation prompts

### (EPM) Chain-of-Thought + Reflexion

### (EPM) Chain-of-Thought (GT) + Reflexion

</document>\
"""


def test_bundle_body_info_with_single() -> None:
    bundle = _given_bundle_single()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffBody.new()
    assert info.mime_type is None
    assert info.description == "chunk description"
    assert info.sections == []
    assert info.observations == []


def test_bundle_body_observations_with_single() -> None:
    bundle = _given_bundle_single()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 9
    assert type(observations[0]) is ObsBody
    assert observations[0].description == "chunk description"
    assert type(observations[0].content) is ContentText
    assert observations[0].sections == []
    assert all(type(observations[index]) is ObsMedia for index in range(1, 9))


def test_bundle_body_render_body_inline_with_single_all_media() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_single(),
        embeds=[ObservableUri.decode("ndk://public/arxiv/2303.11366v2/$body")],
        supports_media=[MimeType.decode("image/png")],
        limit_media=10,
        expected=[
            RENDERED_SINGLE_01_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
            RENDERED_SINGLE_03_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
            RENDERED_SINGLE_05_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
            RENDERED_SINGLE_08_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf",
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf",
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf",
            RENDERED_SINGLE_12_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf",
            RENDERED_SINGLE_14_TEXT,
        ],
    )


def test_bundle_body_render_body_inline_with_single_half_media() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_single(),
        embeds=[ObservableUri.decode("ndk://public/arxiv/2303.11366v2/$body")],
        supports_media=[MimeType.decode("image/png")],
        limit_media=4,
        expected=[
            RENDERED_SINGLE_01_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
            RENDERED_SINGLE_03_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
            RENDERED_SINGLE_05_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
            f"""\
{RENDERED_SINGLE_08_TEXT}


{RENDERED_SINGLE_09_BLOB}
{RENDERED_SINGLE_10_BLOB}
{RENDERED_SINGLE_11_BLOB}

{RENDERED_SINGLE_12_TEXT}


{RENDERED_SINGLE_13_BLOB}

{RENDERED_SINGLE_14_TEXT}\
""",
        ],
    )


def test_bundle_body_render_body_inline_with_single_no_media() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_single(),
        embeds=[ObservableUri.decode("ndk://public/arxiv/2303.11366v2/$body")],
        supports_media=[],
        limit_media=0,
        expected=[
            f"""\
{RENDERED_SINGLE_01_TEXT}


{RENDERED_SINGLE_02_BLOB}

{RENDERED_SINGLE_03_TEXT}


{RENDERED_SINGLE_04_BLOB}

{RENDERED_SINGLE_05_TEXT}


{RENDERED_SINGLE_06_BLOB}
{RENDERED_SINGLE_07_BLOB}

{RENDERED_SINGLE_08_TEXT}


{RENDERED_SINGLE_09_BLOB}
{RENDERED_SINGLE_10_BLOB}
{RENDERED_SINGLE_11_BLOB}

{RENDERED_SINGLE_12_TEXT}


{RENDERED_SINGLE_13_BLOB}

{RENDERED_SINGLE_14_TEXT}\
""",
        ],
    )


##
## Body - Media
##


def _given_bundle_media() -> BundleBody:
    resource_str = "ndk://www/example.com/image.png"
    return BundleBody.make_single_media(
        resource_uri=ResourceUri.decode(resource_str),
        description="stub description",
        placeholder="stub placeholder",
        mime_type=MimeType.decode("image/png"),
        blob="stub blob",
    )


def test_bundle_body_info_with_media() -> None:
    bundle = _given_bundle_media()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffBody.new()
    assert info.mime_type == "image/png"
    assert info.description == "stub description"
    assert info.sections == []
    assert info.observations == []


def test_bundle_body_observations_with_media() -> None:
    bundle = _given_bundle_media()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsBody
    assert observations[0].description == "stub description"
    assert type(observations[0].content) is ContentBlob
    assert observations[0].sections == []


def test_bundle_body_render_body_inline_with_media_supported() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_media(),
        embeds=[ObservableUri.decode("ndk://www/example.com/image.png/$body")],
        supports_media=[MimeType.decode("image/png")],
        limit_media=10,
        expected=["@BLOB ndk://www/example.com/image.png/$body"],
    )


def test_bundle_body_render_body_inline_with_media_unsupported() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_media(),
        embeds=[ObservableUri.decode("ndk://www/example.com/image.png/$body")],
        supports_media=[],
        limit_media=0,
        expected=[
            """\
<blob uri="ndk://www/example.com/image.png/$body" mimetype="image/png">
stub placeholder
</blob>\
""",
        ],
    )


##
## Body - Chunked
##


def _given_bundle_chunked() -> BundleBody:
    resource_str = "ndk://public/arxiv/2303.11366v2"
    return BundleBody.make_chunked(
        resource_uri=ResourceUri.decode(resource_str),
        description="root description",
        sections=[
            ObsBodySection(indexes=[3], heading="Experiments"),
            ObsBodySection(indexes=[6], heading="Reasoning"),
        ],
        media=[
            ObsMedia.stub(media_uri)
            for media_uri in [
                "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf",
                "ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf",
            ]
        ],
        chunks=[
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/00"),
                mode="markdown",
                text=(
                    "# Introduction\n\n"
                    "![](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf)"
                ),
            ),
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/01"),
                mode="markdown",
                text="# Related work",
            ),
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/02"),
                mode="markdown",
                text=(
                    "# Reflexion: reinforcement via verbal reflection\n\n"
                    "![image](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf)"
                ),
            ),
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/03/00"),
                mode="markdown",
                text="\n\n".join(  # noqa: FLY002
                    [
                        "## Sequential decision making: ALFWorld",
                        "#### Results",
                        "![](ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf)",
                        "![](ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf)",
                        "#### Analysis",
                        "## Reasoning: HotpotQA",
                        "#### Results",
                        "![](ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf)",
                        "![](ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf)",
                        "![](ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf)",
                        "#### Analysis",
                    ]
                ),
            ),
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/03/01"),
                mode="markdown",
                text="## Programming",
            ),
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/04"),
                mode="markdown",
                text="\n\n".join(  # noqa: FLY002
                    [
                        "# Limitations",
                        "## WebShop Limitation",
                        "![](ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf)",
                    ]
                ),
            ),
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/05"),
                mode="markdown",
                text="# Programming",
            ),
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/06/00"),
                mode="markdown",
                text="\n\n".join(  # noqa: FLY002
                    [
                        "## Full example",
                        "## Chain-of-Thought + Reflexion",
                        "## HotPotQA Chain-of-Thought (GT) + Reflexion",
                    ]
                ),
            ),
            ObsChunk.parse(
                uri=ObservableUri.decode(f"{resource_str}/$chunk/06/01"),
                mode="markdown",
                text="\n\n".join(  # noqa: FLY002
                    [
                        "## HotPotQA episodic memory (EPM) ablation prompts",
                        "### (EPM) Chain-of-Thought + Reflexion",
                        "### (EPM) Chain-of-Thought (GT) + Reflexion",
                    ]
                ),
            ),
        ],
    )


RENDERED_CHUNKED_01_TEXT = """\
<document uri="ndk://public/arxiv/2303.11366v2/$body">
<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/00">
# Introduction\
"""
RENDERED_CHUNKED_02_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_CHUNKED_03_TEXT = """\
</document-chunk>
<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/01">
# Related work

</document-chunk>
<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/02">
# Reflexion: reinforcement via verbal reflection\
"""
RENDERED_CHUNKED_04_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_CHUNKED_05_TEXT = """\
</document-chunk>

# Experiments

<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/03/00">
## Sequential decision making: ALFWorld

#### Results\
"""
RENDERED_CHUNKED_06_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_CHUNKED_07_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_CHUNKED_08_TEXT = """\
#### Analysis

## Reasoning: HotpotQA

#### Results\
"""
RENDERED_CHUNKED_09_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""

RENDERED_CHUNKED_10_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_CHUNKED_11_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_CHUNKED_12_TEXT = """\
#### Analysis

</document-chunk>
<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/03/01">
## Programming

</document-chunk>
<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/04">
# Limitations

## WebShop Limitation\
"""
RENDERED_CHUNKED_13_BLOB = """\
<blob uri="ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf" mimetype="image/png">
stub placeholder
</blob>\
"""
RENDERED_CHUNKED_14_TEXT = """\
</document-chunk>
<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/05">
# Programming

</document-chunk>

# Reasoning

<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/06/00">
## Full example

## Chain-of-Thought + Reflexion

## HotPotQA Chain-of-Thought (GT) + Reflexion

</document-chunk>
<document-chunk uri="ndk://public/arxiv/2303.11366v2/$chunk/06/01">
## HotPotQA episodic memory (EPM) ablation prompts

### (EPM) Chain-of-Thought + Reflexion

### (EPM) Chain-of-Thought (GT) + Reflexion

</document-chunk>
</document>\
"""


def test_bundle_body_info_with_chunked() -> None:
    bundle = _given_bundle_chunked()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffBody.new()
    assert info.mime_type is None
    assert info.description == "root description"
    assert info.sections == [
        ObservationSection.new_body([3], "Experiments"),
        ObservationSection.new_body([6], "Reasoning"),
    ]
    assert [info.suffix for info in info.observations] == [
        AffBodyChunk.new([0]),
        AffBodyChunk.new([1]),
        AffBodyChunk.new([2]),
        AffBodyChunk.new([3, 0]),
        AffBodyChunk.new([3, 1]),
        AffBodyChunk.new([4]),
        AffBodyChunk.new([5]),
        AffBodyChunk.new([6, 0]),
        AffBodyChunk.new([6, 1]),
    ]


def test_bundle_body_observations_with_chunked() -> None:
    bundle = _given_bundle_chunked()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 18
    assert type(observations[0]) is ObsBody
    assert observations[0].description == "root description"
    assert observations[0].content is None
    assert observations[0].sections == [
        ObsBodySection(indexes=[3], heading="Experiments"),
        ObsBodySection(indexes=[6], heading="Reasoning"),
    ]
    assert all(type(observations[index]) is ObsChunk for index in range(1, 10))
    assert all(type(observations[index]) is ObsMedia for index in range(10, 18))


def test_bundle_body_render_body_inline_with_chunked_all_media() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_chunked(),
        embeds=[ObservableUri.decode("ndk://public/arxiv/2303.11366v2/$body")],
        supports_media=[MimeType.decode("image/png")],
        limit_media=10,
        expected=[
            RENDERED_CHUNKED_01_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
            RENDERED_CHUNKED_03_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
            RENDERED_CHUNKED_05_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
            RENDERED_CHUNKED_08_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf",
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf",
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf",
            RENDERED_CHUNKED_12_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf",
            RENDERED_CHUNKED_14_TEXT,
        ],
    )


def test_bundle_body_render_body_inline_with_chunked_half_media() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_chunked(),
        embeds=[ObservableUri.decode("ndk://public/arxiv/2303.11366v2/$body")],
        supports_media=[MimeType.decode("image/png")],
        limit_media=4,
        expected=[
            RENDERED_CHUNKED_01_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
            RENDERED_CHUNKED_03_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
            RENDERED_CHUNKED_05_TEXT,
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
            "@BLOB ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
            f"""\
{RENDERED_CHUNKED_08_TEXT}


{RENDERED_CHUNKED_09_BLOB}
{RENDERED_CHUNKED_10_BLOB}
{RENDERED_CHUNKED_11_BLOB}

{RENDERED_CHUNKED_12_TEXT}


{RENDERED_CHUNKED_13_BLOB}
{RENDERED_CHUNKED_14_TEXT}\
""",
        ],
    )


def test_bundle_body_render_body_inline_with_chunked_no_media() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_chunked(),
        embeds=[ObservableUri.decode("ndk://public/arxiv/2303.11366v2/$body")],
        supports_media=[],
        limit_media=0,
        expected=[
            f"""\
{RENDERED_CHUNKED_01_TEXT}


{RENDERED_CHUNKED_02_BLOB}
{RENDERED_CHUNKED_03_TEXT}


{RENDERED_CHUNKED_04_BLOB}
{RENDERED_CHUNKED_05_TEXT}


{RENDERED_CHUNKED_06_BLOB}
{RENDERED_CHUNKED_07_BLOB}

{RENDERED_CHUNKED_08_TEXT}


{RENDERED_CHUNKED_09_BLOB}
{RENDERED_CHUNKED_10_BLOB}
{RENDERED_CHUNKED_11_BLOB}

{RENDERED_CHUNKED_12_TEXT}


{RENDERED_CHUNKED_13_BLOB}
{RENDERED_CHUNKED_14_TEXT}\
""",
        ],
    )


##
## Collection
##


def _given_bundle_collection_empty() -> BundleCollection:
    resource_str = "ndk://stub/-/folder"
    return BundleCollection(
        uri=ResourceUri.decode(resource_str).child_affordance(AffCollection.new()),
        results=[],
    )


def _given_bundle_collection_with_results() -> BundleCollection:
    resource_str = "ndk://stub/-/folder"
    return BundleCollection(
        uri=ResourceUri.decode(resource_str).child_affordance(AffCollection.new()),
        results=[
            ResourceUri.decode("ndk://stub/-/folder/doc1.md"),
            ResourceUri.decode("ndk://stub/-/folder/doc2.pdf"),
            ResourceUri.decode("ndk://stub/-/folder/subdir"),
        ],
    )


def test_bundle_collection_info_with_empty() -> None:
    bundle = _given_bundle_collection_empty()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffCollection.new()
    assert info.mime_type is None
    assert info.description is None
    assert info.sections == []
    assert info.observations == []


def test_bundle_collection_observations_with_empty() -> None:
    bundle = _given_bundle_collection_empty()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsCollection
    assert observations[0].results == []


def test_bundle_collection_render_body_with_empty() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_collection_empty(),
        embeds=[ObservableUri.decode("ndk://stub/-/folder/$collection")],
        supports_media=[],
        limit_media=0,
        expected=[
            """\
<collection uri="ndk://stub/-/folder/$collection">
empty
</collection>\
""",
        ],
    )


def test_bundle_collection_info_with_results() -> None:
    bundle = _given_bundle_collection_with_results()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffCollection.new()
    assert info.mime_type is None
    assert info.description is None
    assert info.sections == []
    assert info.observations == []


def test_bundle_collection_observations_with_results() -> None:
    bundle = _given_bundle_collection_with_results()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsCollection
    assert observations[0].results == [
        ResourceUri.decode("ndk://stub/-/folder/doc1.md"),
        ResourceUri.decode("ndk://stub/-/folder/doc2.pdf"),
        ResourceUri.decode("ndk://stub/-/folder/subdir"),
    ]


def test_bundle_collection_render_body_with_results() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_collection_with_results(),
        embeds=[ObservableUri.decode("ndk://stub/-/folder/$collection")],
        supports_media=[],
        limit_media=0,
        expected=[
            """\
<collection uri="ndk://stub/-/folder/$collection">
- <ndk://stub/-/folder/doc1.md>
- <ndk://stub/-/folder/doc2.pdf>
- <ndk://stub/-/folder/subdir>
</collection>\
""",
        ],
    )


##
## File
##


def _given_bundle_file_blob() -> BundleFile:
    resource_str = "ndk://stub/-/image.png"
    return BundleFile(
        uri=ResourceUri.decode(resource_str).child_affordance(AffFile.new()),
        description="An image with bytes",
        mime_type=MimeType.decode("image/png"),
        expiry=None,
        download_url=DataUri.stub(),
    )


def _given_bundle_file_web_url() -> BundleFile:
    resource_str = "ndk://stub/-/image.png"
    return BundleFile(
        uri=ResourceUri.decode(resource_str).child_affordance(AffFile.new()),
        description="An image hosted on the web",
        mime_type=MimeType.decode("image/png"),
        expiry=None,
        download_url=WebUrl.decode("https://example.com/files/image.png"),
    )


def test_bundle_file_info_with_blob() -> None:
    bundle = _given_bundle_file_blob()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffFile.new()
    assert info.mime_type == "image/png"
    assert info.description == "An image with bytes"
    assert info.sections == []
    assert info.observations == []


def test_bundle_file_observations_with_blob() -> None:
    bundle = _given_bundle_file_blob()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsFile
    assert observations[0].description == "An image with bytes"
    assert observations[0].mime_type == MimeType.decode("image/png")
    assert observations[0].expiry is None
    assert isinstance(observations[0].download_url, DataUri)


def test_bundle_file_info_with_web_url() -> None:
    bundle = _given_bundle_file_web_url()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffFile.new()
    assert info.mime_type == "image/png"
    assert info.description == "An image hosted on the web"
    assert info.sections == []
    assert info.observations == []


def test_bundle_file_observations_with_web_url() -> None:
    bundle = _given_bundle_file_web_url()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsFile
    assert observations[0].description == "An image hosted on the web"
    assert observations[0].mime_type == MimeType.decode("image/png")
    assert observations[0].expiry is None
    assert isinstance(observations[0].download_url, WebUrl)
    assert str(observations[0].download_url) == "https://example.com/files/image.png"


##
## Plain
##


def _given_bundle_plain_markdown() -> BundlePlain:
    resource_str = "ndk://stub/-/readme.md"
    return BundlePlain(
        uri=ResourceUri.decode(resource_str).child_affordance(AffPlain.new()),
        mime_type=MimeType.decode("text/markdown"),
        text="# Hello World\n\nThis is a **markdown** document.",
    )


def _given_bundle_plain_json() -> BundlePlain:
    resource_str = "ndk://stub/-/config.json"
    return BundlePlain(
        uri=ResourceUri.decode(resource_str).child_affordance(AffPlain.new()),
        mime_type=MimeType.decode("application/json"),
        text='{"name": "test", "version": "1.0.0"}',
    )


def _given_bundle_plain_text() -> BundlePlain:
    resource_str = "ndk://stub/-/notes.txt"
    return BundlePlain(
        uri=ResourceUri.decode(resource_str).child_affordance(AffPlain.new()),
        mime_type=MimeType.decode("text/plain"),
        text="Just some plain text content.\nWith multiple lines.",
    )


def test_bundle_plain_info_with_markdown() -> None:
    bundle = _given_bundle_plain_markdown()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffPlain.new()
    assert info.mime_type == "text/markdown"
    assert info.description is None
    assert info.sections == []
    assert info.observations == []


def test_bundle_plain_observations_with_markdown() -> None:
    bundle = _given_bundle_plain_markdown()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsPlain
    assert observations[0].mime_type == MimeType.decode("text/markdown")
    assert observations[0].text == "# Hello World\n\nThis is a **markdown** document."


def test_bundle_plain_render_body_with_markdown() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_plain_markdown(),
        embeds=[ObservableUri.decode("ndk://stub/-/readme.md/$plain")],
        supports_media=[],
        limit_media=0,
        expected=[
            """\
<plain uri="ndk://stub/-/readme.md/$plain" mimetype="text/markdown">
```markdown
# Hello World

This is a **markdown** document.
```
</plain>\
""",
        ],
    )


def test_bundle_plain_info_with_json() -> None:
    bundle = _given_bundle_plain_json()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffPlain.new()
    assert info.mime_type == "application/json"
    assert info.description is None
    assert info.sections == []
    assert info.observations == []


def test_bundle_plain_observations_with_json() -> None:
    bundle = _given_bundle_plain_json()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsPlain
    assert observations[0].mime_type == MimeType.decode("application/json")
    assert observations[0].text == '{"name": "test", "version": "1.0.0"}'


def test_bundle_plain_render_body_with_json() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_plain_json(),
        embeds=[ObservableUri.decode("ndk://stub/-/config.json/$plain")],
        supports_media=[],
        limit_media=0,
        expected=[
            """\
<plain uri="ndk://stub/-/config.json/$plain" mimetype="application/json">
```
{"name": "test", "version": "1.0.0"}
```
</plain>\
""",
        ],
    )


def test_bundle_plain_info_with_text() -> None:
    bundle = _given_bundle_plain_text()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffPlain.new()
    assert info.mime_type == "text/plain"
    assert info.description is None
    assert info.sections == []
    assert info.observations == []


def test_bundle_plain_observations_with_text() -> None:
    bundle = _given_bundle_plain_text()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsPlain
    assert observations[0].mime_type == MimeType.decode("text/plain")
    assert observations[0].text == "Just some plain text content.\nWith multiple lines."


def test_bundle_plain_render_body_with_text() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_plain_text(),
        embeds=[ObservableUri.decode("ndk://stub/-/notes.txt/$plain")],
        supports_media=[],
        limit_media=0,
        expected=[
            """\
<plain uri="ndk://stub/-/notes.txt/$plain" mimetype="text/plain">
```
Just some plain text content.
With multiple lines.
```
</plain>\
""",
        ],
    )
