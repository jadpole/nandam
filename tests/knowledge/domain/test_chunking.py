from base.models.content import ContentBlob, ContentText
from base.models.rendered import Rendered
from base.resources.aff_body import ObsMedia
from base.strings.data import MimeType
from base.strings.resource import ResourceUri

from knowledge.domain.chunking import chunk_body_sync, unittest_configure
from knowledge.models.storage_observed import BundleBody

from tests.data.samples import read_2303_11366v2, read_lotto_6_49

PRINT_CHUNK_CONTENT = True


def _run_chunk_document(
    *,
    resource_uri: str,
    text: str,
    blobs: list[str],
    expected_media: list[str],
    expected_chunks: list[tuple[str, list[str]]],
    expected_sections: list[tuple[list[int], str]],
) -> BundleBody:
    media: list[ObsMedia] = []
    for self_uri in blobs:
        media_uri = f"{resource_uri}/$media/{self_uri.removeprefix('self://')}"
        text = text.replace(f"]({self_uri})", f"]({media_uri})")
        media.append(ObsMedia.stub(media_uri))

    chunked = chunk_body_sync(
        resource_uri=ResourceUri.decode(resource_uri),
        text=ContentText.parse(text),
        media=media,
    )

    if PRINT_CHUNK_CONTENT:
        observations = chunked.observations()
        rendered = Rendered.render(
            ContentText.new_embed(observations[0].uri),
            observations,
        )
        rendered_parts = rendered.as_llm_inline(
            supports_media=[MimeType.decode("image/png")],
            limit_media=20,
        )
        rendered_list = [
            f"@BLOB {part.uri}" if isinstance(part, ContentBlob) else part
            for part in rendered_parts
        ]
        print("<chunked>\n" + "\n---\n".join(rendered_list) + "\n</chunked>")
    else:
        rendered_info = ContentText.new(parts=chunked.observations()[0].render_info())
        print("<chunked>\n" + rendered_info.as_str() + "\n</chunked>")

    # Check that the output matches expectations.

    assert [str(c.uri) for c in chunked.chunks] == [uri for uri, _ in expected_chunks]
    assert [str(c.uri) for c in chunked.media] == expected_media

    for chunk_uri, chunk_contents in expected_chunks:
        chunk = next((c for c in chunked.chunks if str(c.uri) == chunk_uri), None)
        if not chunk:
            raise ValueError(f"chunk not found: {chunk_uri}")
        for chunk_content in chunk_contents:
            if chunk_content not in chunk.text.as_str():
                raise ValueError(
                    f"chunk content not found in {chunk_uri}: {chunk_content}"
                )

    for section_indexes, section_heading in expected_sections:
        section = next(
            (s for s in chunked.sections if s.indexes == section_indexes), None
        )
        if not section:
            raise ValueError(
                "section not found: "
                + "/".join(f"{index:02d}" for index in section_indexes)
            )
        assert section.heading == section_heading

    return chunked


def test_chunk_document_2303_11366v2_at_20k():
    text, header = read_2303_11366v2()
    _run_chunk_document(
        resource_uri="ndk://public/arxiv/2303.11366v2",
        text=text,
        blobs=header.blobs,
        expected_media=[
            "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf",
        ],
        expected_chunks=[
            (
                "ndk://public/arxiv/2303.11366v2/$chunk",
                [
                    "# Introduction",
                    "![](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf)",
                    "# Related work",
                    "#### Reasoning and decision-making",
                    "#### Programming",
                    "#### Self-reflection",
                    "# Reflexion: reinforcement via verbal reflection",
                    "![image](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf)",
                    "#### Actor",
                    "#### Evaluator",
                    "#### Self-reflection",
                    "#### Memory",
                    "#### The Reflexion process",
                    "# Experiments",
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
                    "## Programming",
                    "#### Results",
                    "#### Analysis",
                    "#### Ablation study",
                    "# Limitations",
                    "# Broader impact",
                    "# Conclusion",
                    "# Reproducibility",
                    "# Additional Information and Examples",
                    "# Decision-making",
                    "## WebShop Limitation",
                    "![](ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf)",
                    "# Programming",
                    "### Programming function implementation example (HumanEval Python)",
                    "### Reflexion Actor instruction",
                    "### Reflexion Self-reflection instruction and example",
                    "### Reflexion programming *no* Self-Reflection ablation example",
                    "### Reflexion programming *no* test generation ablation example",
                    "## Full example",
                    "## Chain-of-Thought + Reflexion",
                    "## HotPotQA Chain-of-Thought (GT) + Reflexion",
                    "## HotPotQA episodic memory (EPM) ablation prompts",
                    "### (EPM) Chain-of-Thought + Reflexion",
                    "### (EPM) Chain-of-Thought (GT) + Reflexion",
                ],
            ),
        ],
        expected_sections=[],
    )


def test_chunk_document_2303_11366v2_at_4k():
    """
    Confirm that chunks are correctly aggregated, by setting a lower chunking
    threshold and max chunk tokens.
    """
    text, header = read_2303_11366v2()
    unittest_configure(
        chunking_threshold_tokens=8_000,
        max_chunk_tokens=4_000,
    )
    _run_chunk_document(
        resource_uri="ndk://public/arxiv/2303.11366v2",
        text=text,
        blobs=header.blobs,
        expected_media=[
            "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf",
        ],
        expected_chunks=[
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/00",
                [
                    "# Introduction",
                    "![](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf)",
                    "# Related work",
                    "#### Reasoning and decision-making",
                    "#### Programming",
                    "#### Self-reflection",
                    "# Reflexion: reinforcement via verbal reflection",
                    "![image](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf)",
                    "#### Actor",
                    "#### Evaluator",
                    "#### Self-reflection",
                    "#### Memory",
                    "#### The Reflexion process",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/01",
                [
                    "# Experiments",
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
                    "## Programming",
                    "#### Results",
                    "#### Analysis",
                    "#### Ablation study",
                    "# Limitations",
                    "# Broader impact",
                    "# Conclusion",
                    "# Reproducibility",
                    "# Additional Information and Examples",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/02",
                [
                    "# Decision-making",
                    "## WebShop Limitation",
                    "![](ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf)",
                    "# Programming",
                    "### Programming function implementation example (HumanEval Python)",
                    "### Reflexion Actor instruction",
                    "### Reflexion Self-reflection instruction and example",
                    "### Reflexion programming *no* Self-Reflection ablation example",
                    "### Reflexion programming *no* test generation ablation example",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/03",
                [
                    "## Full example",
                    "## Chain-of-Thought + Reflexion",
                    "## HotPotQA Chain-of-Thought (GT) + Reflexion",
                    "## HotPotQA episodic memory (EPM) ablation prompts",
                    "### (EPM) Chain-of-Thought + Reflexion",
                    "### (EPM) Chain-of-Thought (GT) + Reflexion",
                ],
            ),
        ],
        expected_sections=[],
    )


def test_chunk_document_2303_11366v2_at_2k():
    """
    Confirm that chunks are correctly aggregated, by setting a lower chunking
    threshold and max chunk tokens.
    """
    text, header = read_2303_11366v2()
    unittest_configure(
        chunking_threshold_tokens=8_000,
        max_chunk_tokens=2_000,
    )
    _run_chunk_document(
        resource_uri="ndk://public/arxiv/2303.11366v2",
        text=text,
        blobs=header.blobs,
        expected_media=[
            "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_success.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_ablation.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_cot_gt.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/hotpotqa_success.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf",
            "ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf",
        ],
        expected_chunks=[
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/00",
                [
                    "# Introduction",
                    "![](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_tasks.pdf)",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/01",
                [
                    "# Related work",
                    "#### Reasoning and decision-making",
                    "#### Programming",
                    "#### Self-reflection",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/02",
                [
                    "# Reflexion: reinforcement via verbal reflection",
                    "![image](ndk://public/arxiv/2303.11366v2/$media/figures/reflexion_rl.pdf)",
                    "#### Actor",
                    "#### Evaluator",
                    "#### Self-reflection",
                    "#### Memory",
                    "#### The Reflexion process",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/03/00",
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
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/03/01",
                [
                    "## Programming",
                    "#### Results",
                    "#### Analysis",
                    "#### Ablation study",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/04",
                [
                    "# Limitations",
                    "# Broader impact",
                    "# Conclusion",
                    "# Reproducibility",
                    "# Additional Information and Examples",
                    "# Decision-making",
                    "## WebShop Limitation",
                    "![](ndk://public/arxiv/2303.11366v2/$media/figures/webshop_success.pdf)",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/05",
                [
                    "# Programming",
                    "### Programming function implementation example (HumanEval Python)",
                    "### Reflexion Actor instruction",
                    "### Reflexion Self-reflection instruction and example",
                    "### Reflexion programming *no* Self-Reflection ablation example",
                    "### Reflexion programming *no* test generation ablation example",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/06/00",
                [
                    "## Full example",
                    "## Chain-of-Thought + Reflexion",
                    "## HotPotQA Chain-of-Thought (GT) + Reflexion",
                ],
            ),
            (
                "ndk://public/arxiv/2303.11366v2/$chunk/06/01",
                [
                    "## HotPotQA episodic memory (EPM) ablation prompts",
                    "### (EPM) Chain-of-Thought + Reflexion",
                    "### (EPM) Chain-of-Thought (GT) + Reflexion",
                ],
            ),
        ],
        expected_sections=[
            ([3], "Experiments"),
            ([6], "Reasoning"),
        ],
    )


def test_chunk_document_lotto_6_49():
    text, header = read_lotto_6_49()
    print("expected_blobs: " + ", ".join(header.blobs))
    _run_chunk_document(
        resource_uri="ndk://www/loteries.espacejeux.com/amrtvexe4rwuzbxska5ph2uxaahv6jigz5dpfaxo",
        text=text,
        blobs=header.blobs,
        expected_media=[
            "ndk://www/loteries.espacejeux.com/amrtvexe4rwuzbxska5ph2uxaahv6jigz5dpfaxo/$media/W58d82bb9-66e8-47b9-aaa6-f642af28978a_.jpg",
            "ndk://www/loteries.espacejeux.com/amrtvexe4rwuzbxska5ph2uxaahv6jigz5dpfaxo/$media/W73ef4833-4e22-495d-b0f4-894c670bb821_.png",
            "ndk://www/loteries.espacejeux.com/amrtvexe4rwuzbxska5ph2uxaahv6jigz5dpfaxo/$media/Wfc235dd7-18d8-448d-8599-307e71691b21_.png",
        ],
        expected_chunks=[
            (
                "ndk://www/loteries.espacejeux.com/amrtvexe4rwuzbxska5ph2uxaahv6jigz5dpfaxo/$chunk",
                [
                    "LA BOULE D'OR\n26 million$\\*",
                    "![](ndk://www/loteries.espacejeux.com/amrtvexe4rwuzbxska5ph2uxaahv6jigz5dpfaxo/$media/W58d82bb9-66e8-47b9-aaa6-f642af28978a_.jpg)",
                    "![Lotto 6/49](ndk://www/loteries.espacejeux.com/amrtvexe4rwuzbxska5ph2uxaahv6jigz5dpfaxo/$media/Wfc235dd7-18d8-448d-8599-307e71691b21_.png)",
                    "![Lotto 6/49](ndk://www/loteries.espacejeux.com/amrtvexe4rwuzbxska5ph2uxaahv6jigz5dpfaxo/$media/W73ef4833-4e22-495d-b0f4-894c670bb821_.png)",
                ],
            ),
        ],
        expected_sections=[],
    )
