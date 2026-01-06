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
from base.resources.metadata import ObservationSection
from base.resources.observation import ObservationBundle
from base.strings.data import MimeType
from base.strings.resource import ObservableUri, ResourceUri


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


def test_aff_body_bundle_single_info() -> None:
    bundle = _given_bundle_single()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffBody.new()
    assert info.mime_type is None
    assert info.description == "chunk description"
    assert info.sections == []
    assert info.observations == []


def test_aff_body_bundle_single_observations() -> None:
    bundle = _given_bundle_single()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 9
    assert type(observations[0]) is ObsBody
    assert observations[0].description == "chunk description"
    assert type(observations[0].content) is ContentText
    assert observations[0].sections == []
    assert all(type(observations[index]) is ObsMedia for index in range(1, 9))


def test_aff_body_bundle_single_rendered_inline_all_media() -> None:
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


def test_aff_body_bundle_single_rendered_inline_half_media() -> None:
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


def test_aff_body_bundle_single_rendered_inline_no_media() -> None:
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


def test_aff_body_bundle_media_info() -> None:
    bundle = _given_bundle_media()
    info = bundle.info()
    print(f"<bundle_info>\n{as_yaml(info)}\n</bundle_info>")
    assert info.suffix == AffBody.new()
    assert info.mime_type == "image/png"
    assert info.description == "stub description"
    assert info.sections == []
    assert info.observations == []


def test_aff_body_bundle_media_observations() -> None:
    bundle = _given_bundle_media()
    print(f"<bundle_obs>\n{as_yaml(bundle.observations())}\n</bundle_obs>")
    observations = bundle.observations()
    assert len(observations) == 1
    assert type(observations[0]) is ObsBody
    assert observations[0].description == "stub description"
    assert type(observations[0].content) is ContentBlob
    assert observations[0].sections == []


def test_aff_body_bundle_media_rendered_inline_supported() -> None:
    _run_test_bundle_render(
        bundle=_given_bundle_media(),
        embeds=[ObservableUri.decode("ndk://www/example.com/image.png/$body")],
        supports_media=[MimeType.decode("image/png")],
        limit_media=10,
        expected=["@BLOB ndk://www/example.com/image.png/$body"],
    )


def test_aff_body_bundle_media_rendered_inline_unsupported() -> None:
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


def test_aff_body_bundle_chunked_info() -> None:
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


def test_aff_body_bundle_chunked_observations() -> None:
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


def test_aff_body_bundle_chunked_rendered_inline_all_media() -> None:
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


def test_aff_body_bundle_chunked_rendered_inline_half_media() -> None:
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


def test_aff_body_bundle_chunked_rendered_inline_no_media() -> None:
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


# TODO: No results
# TODO: With results


##
## File
##


# TODO: Blob
# TODO: Web URL


##
## Plain
##


# TODO: Markdown
# TODO: JSON
# TODO: Text
