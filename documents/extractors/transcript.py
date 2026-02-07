import asyncio
import logging
import openai
import re
import sys
import tempfile

from math import ceil
from pathlib import Path
from typing import Literal

from base.api.documents import TranscriptFormat

from documents.config import DocumentsConfig
from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted, DownloadedFile
from documents.models.processing import ExtractOptions, Extractor
from documents.models.transcript import (
    merge_segments_densely,
    merge_segments_sparsely,
    TranscriptSegment,
)

logger = logging.getLogger(__name__)

OPENAI_CLIENT = openai.AsyncClient(
    api_key=DocumentsConfig.llm.router_api_key,
    base_url=DocumentsConfig.llm.router_api_base,
)

MAX_ATTEMPTS = 3
RETRY_DELAY_SECS = [2, 10, 30, 60, 120, 240]


class TranscriptExtractor(Extractor):
    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(downloaded.mime_type and downloaded.mime_type.mode() == "media")

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        if not downloaded.mime_type:
            raise ExtractError.fail("transcript", "requires mime_type")
        if not isinstance(downloaded, DownloadedFile):
            raise ExtractError.fail("transcript", "requires DownloadedFile")

        transcript = await extract_transcript(
            downloaded,
            options.transcript.deduplicate,
            options.transcript.format,
            options.transcript.language,
        )
        return Extracted(
            mode="markdown",
            name=None,
            path=None,
            mime_type=downloaded.mime_type,
            blobs={},
            text=transcript,
        )


CHUNK_MAX_SIZE = 25 * 1000 * 1000  # 25 MB
CHUNK_MAX_DURATION = 600  # 10 minutes
MAX_PARALLEL_CHUNKS = 6  # 60 minutes
SEGMENT_MERGE_THRESHOLD = 600  # 10 minutes


async def extract_transcript(
    downloaded: DownloadedFile,
    srt_deduplicate: bool,
    srt_format: TranscriptFormat | None,
    language: str | None = None,
) -> str:
    try:
        basename = (
            Path(downloaded.filename).stem if downloaded.filename else "transcript"
        )
        segments, total_time_secs = await _extract_audio(
            downloaded.tempfile_path,
            basename,
            language,
        )
        segments = _fix_known_hallucinations(segments, basename, srt_deduplicate)
        return format_transcript(segments, srt_format, total_time_secs)
    except ExtractError:
        raise
    except Exception as exc:
        raise ExtractError.unexpected(str(exc))  # noqa: B904


def format_transcript(
    segments: list[TranscriptSegment],
    srt_format: TranscriptFormat | None,
    total_time_secs: float,
) -> str:
    # The default format is "srt-dense" (see `base.api.documents`).
    srt_format = srt_format or "srt-dense"

    if srt_format == "text":
        return TranscriptSegment.format_text(segments)

    merged: list[TranscriptSegment] = segments
    if total_time_secs > SEGMENT_MERGE_THRESHOLD:
        if srt_format == "srt-dense":
            merged = merge_segments_densely(segments)
        elif srt_format == "srt-sparse":
            merged = merge_segments_sparsely(segments)

    # If srt_format is "original" or the transcript is too short to merge, then
    # return the extracted SRT directly.
    return TranscriptSegment.format_srt(merged)


async def _extract_audio(
    path: Path,
    basename: str,
    language: str | None = None,
) -> tuple[list[TranscriptSegment], float]:
    total_time_secs, filesize_bytes = await _ffprobe(path)

    if filesize_bytes < CHUNK_MAX_SIZE and total_time_secs < CHUNK_MAX_DURATION:
        srt = await transcribe(str(path), basename, "srt", language)
        return TranscriptSegment.parse_srt(srt), total_time_secs

    else:
        # Otherwise, break it down into 10 minutes chunks.
        # Calculate the number of chunks needed.
        num_chunks = ceil(total_time_secs / CHUNK_MAX_DURATION)

        # Process each chunk (in parallel on Linux, sequentially on Windows).
        chunks: list[list[TranscriptSegment]]
        if sys.platform.startswith("win"):
            chunks = [
                await _extract_chunk(path, basename, i * CHUNK_MAX_DURATION, language)
                for i in range(num_chunks)
            ]
        else:
            # If the video is longer than 6 chunks (i.e., 60 minutes), then only
            # process at most 6 chunks in parallel to avoid "out of memory".
            chunks = []
            for start_index in range(0, num_chunks, MAX_PARALLEL_CHUNKS):
                chunk_jobs = [
                    _extract_chunk(path, basename, i * CHUNK_MAX_DURATION, language)
                    for i in range(
                        start_index,
                        min(num_chunks, start_index + MAX_PARALLEL_CHUNKS),
                    )
                ]
                chunks.extend(await asyncio.gather(*chunk_jobs))

        # If the audio file is > 10 minutes, we merge the segments into chunks
        # of 5 minutes to save on tokens.  That way, songs include very specific
        # timestamps (for content-flagging), but for meeting recordings, we have
        # only a rough idea of when something was spoken, which is good enough.
        #
        # The cutoff is based on the start time of the segment, so each "merged
        # segment" has the same duration (more time resolution), but may have a
        # different density of information.
        return [segment for chunk in chunks for segment in chunk], total_time_secs


async def _extract_chunk(
    path: Path,
    basename: str,
    start_time_secs: int,
    language: str | None = None,
) -> list[TranscriptSegment]:
    # check the type of the file, if wav use ffmpeg to convert to mp3
    # otherwise copy chunk of the file
    if path.suffix == ".wav":
        temp_file = tempfile.NamedTemporaryFile(  # noqa: SIM115
            delete=False,
            suffix=".mp3",
        )
        temp_file.close()
        await _run_shell(
            command=[
                "ffmpeg",
                "-y",
                "-i",
                str(path),
                "-ss",
                str(start_time_secs),
                "-t",
                str(CHUNK_MAX_DURATION + 10),
                "-vn",
                "-acodec",
                "libmp3lame",
                "-ar",
                "12000",  # 12 kHz sample rate
                "-ab",
                "16k",  # 16 kbps bitrate
                "-ac",
                "1",  # Mono (1 channel)
                "-q:a",
                "9",  # Force lowest quality
                temp_file.name,
            ],
        )
    else:
        temp_file = tempfile.NamedTemporaryFile(  # noqa: SIM115
            delete=False,
            suffix=".mp4",
        )
        temp_file.close()
        await _run_shell(
            command=[
                "ffmpeg",
                "-y",
                "-i",
                str(path),
                "-ss",
                str(start_time_secs),
                "-t",
                str(CHUNK_MAX_DURATION + 10),
                "-vn",
                "-acodec",
                "copy",
                temp_file.name,
            ],
        )

    try:
        # Transcribe the chunk
        srt = await transcribe(temp_file.name, basename, "srt", language)
        return [
            segment.shift_time(start_time_secs)
            for segment in TranscriptSegment.parse_srt(srt)
        ]
    finally:
        # Delete the chunk
        Path(temp_file.name).unlink(missing_ok=True)


##
## Commands
##


async def transcribe(
    path: str,
    basename: str,
    response_format: Literal["srt", "text"] = "srt",
    language: str | None = None,
) -> str:
    with open(path, "rb") as file_obj:  # noqa: ASYNC230
        network_errors = 0
        while True:
            try:
                if DocumentsConfig.verbose:
                    logger.info(
                        "Calling whisper on %s (%s retry %d)...",
                        basename,
                        path,
                        network_errors,
                    )

                return await OPENAI_CLIENT.audio.transcriptions.create(
                    file=file_obj,
                    model="whisper-1",
                    prompt=f"Filename: {basename}",
                    response_format=response_format,
                    language=language or openai.omit,
                )
            except Exception:
                # Reset the file position for retry
                file_obj.seek(0)
                network_errors += 1
                if network_errors >= MAX_ATTEMPTS:
                    raise
                else:
                    await asyncio.sleep(RETRY_DELAY_SECS[network_errors - 1])


async def _ffprobe(path: Path) -> tuple[float, int]:
    result = await _run_shell(
        command=[
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
    )
    assert result
    total_time_secs = float(result)
    filesize_bytes = path.stat().st_size
    return total_time_secs, filesize_bytes


async def _run_shell(command: list[str]) -> str:
    if sys.platform.startswith("win"):
        # Use asyncio.create_subprocess_exec for Windows instead of subprocess.run
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        status_code = await process.wait()
        assert process.stdout
        output = (await process.stdout.read()).decode("utf-8", errors="ignore")
        if status_code != 0:
            raise ExtractError.fail(
                "transcript",
                f"{command[0]} returned non-zero status code {status_code}: {output}",
            )

        return output
    else:
        result = await asyncio.create_subprocess_shell(
            " ".join(command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        status_code = await result.wait()
        assert result.stdout
        output = (await result.stdout.read()).decode("utf-8", errors="ignore")

        if status_code != 0:
            raise ExtractError.fail(
                "transcript",
                f"{command[0]} returned non-zero status code {status_code}: {output}",
            )

        return output


##
## Hallucinations
## ---
##
## References:
## - https://github.com/openai/whisper/discussions/928
##
## These hallucinations are usually generated on silence.
## You can therefore discard the entire segment when they appear.
##
## TODO It would be better to not generate the transcript when we detect an
##    'absence of speech' instead.  To solve most cases, just find the start and
##    end timestamps of speech.
##

KNOWN_HALLUCINATIONS = [
    "¡Incluso, yo lo compasto así!",  # I even compose it like this!
    "37670673",
    "376706763",
    "All-American Theme Song",
    "BEATS EVERY WEEK",
    "GX-70000KBMPS",
    "GX-70000KBPS",
    "GX-7000KBPS",
    "GX-7800KBPS",
    "Gxx-1000KBPS",
    "Gxx-2000KBPS",
    "Gxx-5000KBPS",
    "Intro",
    "Pa' qué respirare y reírme",  # So I'll breathe and laugh (with typo)
    "Sawgaw",
    "We are giving over the globe thanks to you.",
    "We hope that you are enjoying our drive today.",
]

KNOWN_HALLUCINATIONS_PATTERN = [
    # Websites
    "www.mooji.org",
    "www.multi-moto.eu",
    "amara.org",
    # Turns of phrase
    "click the like button",
    "clicking the like button",
    "give this video a like",
    "in the description",
    "in the next video",
    "purchase & download",
    "thank you for joining",
    "thanks for watching",
    "このビデオが好きな方は",  # If you like this video
    "ご視聴ありがとう",  # Thank you for watching
    "作詞・作曲・編曲",  # Lyrics, composition, arrangement
    # Miscellaneous
    "Baguio Botanical Garden",
    "commentary is end",
    "cuando la luz de la luz",  # When the light of the light
    "without heavens, there is no reality",
]

KNOWN_HALLUCINATIONS_PREFIX = [
    # DE
    "copyright",
    "ondertiteld",
    "ondertiteling",
    "ondertitels ingediend door",
    "swr",  # Southwest Broadcasting
    "untertitel",
    # EN
    "thank you so much",
    "video by",
    "we are now at",
    # ES
    "más información",
    "subtitulado por",
    "subtítulos creados por",
    "subtítulos en",
    "subtítulos por",
    "subtítulos realizados por la",
    # FR
    "❤️ par",
    "cliquez-vous sur les sous-titres",
    "sous-titrage",
    "sous-titres",
    # IT
    "sottotitoli a cura",
    "sottotitoli creati",
    "sottotitoli di",
    "sottotitoli e revisione",
    # LA
    "sottotitoli creati",
    # PT
    "legendas pela",
    "transcrição e Legendas pela",
    # RU
    "Редактор субтитров",
    # ZH
    "字幕由",  # Subtitles by
    "小編字幕由",  # Editor subtitles by
]


def _fix_known_hallucinations(
    segments: list[TranscriptSegment],
    basename: str | None = None,
    deduplicate: bool = True,
) -> list[TranscriptSegment]:
    deduplicate_regex = re.compile(r"\b(.+)( \1)+\b")
    known_hallucinations = [s.lower() for s in KNOWN_HALLUCINATIONS]
    known_hallucinations_patterns = [s.lower() for s in KNOWN_HALLUCINATIONS_PATTERN]
    known_hallucinations_prefix = [s.lower() for s in KNOWN_HALLUCINATIONS_PREFIX]
    if basename:
        known_hallucinations.append(basename.lower())  # sometimes repeats filename

    # First, remove repetitions within a segment and "known hallucinations"
    clean_segments: list[TranscriptSegment] = []
    for segment in segments:
        text = segment.text
        if deduplicate:
            text = deduplicate_regex.sub(r"\1", text).strip()

        if (
            not text
            or text.lower() in known_hallucinations
            or any(s in text.lower() for s in known_hallucinations_patterns)
            or any(text.lower().startswith(s) for s in known_hallucinations_prefix)
        ):
            continue

        clean_segments.append(
            TranscriptSegment(
                text=text,
                start_secs=segment.start_secs,
                end_secs=segment.end_secs,
            ),
        )

    if not deduplicate:
        return clean_segments

    # Second, deduplicate neighboring segments (keep the first one)
    output: list[TranscriptSegment] = []
    for segment in clean_segments:
        if output and output[-1].text.lower() == segment.text.lower():
            continue

        output.append(segment)

    return output
