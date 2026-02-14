from dataclasses import dataclass
from datetime import datetime, UTC

from base.utils.markdown import markdown_normalize


@dataclass(kw_only=True)
class TranscriptSegment:
    text: str
    start_secs: float
    end_secs: float

    @staticmethod
    def format_srt(segments: list[TranscriptSegment]) -> str:
        """Stringify a list of `TranscriptSegment` into SRT format."""
        return "\n\n".join(
            f"{i + 1}\n{_format_srt_time(segment.start_secs)} --> {_format_srt_time(segment.end_secs)}\n{segment.text}"
            for i, segment in enumerate(segments)
        )

    @staticmethod
    def format_text(segments: list[TranscriptSegment]) -> str:
        return "\n".join(s.text for s in segments)

    @staticmethod
    def parse_srt(srt: str) -> list[TranscriptSegment]:
        """Parse the SRT output into a list of `TranscriptSegment`."""
        srt = markdown_normalize(srt)
        segments = []
        for segment in srt.split("\n\n"):
            # Segments may be empty, either because Whisper said so or because
            # it contained only emoji which were stripped during normalization.
            segment_parts = segment.strip().split("\n")
            if len(segment_parts) != 3:  # noqa: PLR2004
                continue
            _, timestamps, text = segment_parts

            start_time, end_time = timestamps.split(" --> ")
            segments.append(
                TranscriptSegment(
                    text=text,
                    start_secs=_parse_srt_time(start_time),
                    end_secs=_parse_srt_time(end_time),
                ),
            )

        return segments

    def shift_time(self, seconds: float) -> TranscriptSegment:
        """Shift the segment start and end times by `seconds`."""
        return TranscriptSegment(
            text=self.text,
            start_secs=self.start_secs + seconds,
            end_secs=self.end_secs + seconds,
        )


def merge_segments_densely(
    segments: list[TranscriptSegment],
    target_duration_secs: int = 300,
) -> list[TranscriptSegment]:
    """
    To save on tokens, we merge the segments of transcripts > 10 minutes into
    segments of ~ 5 minutes each.  This allows us to get a rough idea for when
    something was said, while using roughly half the tokens.

    NOTE: This builds segments of 5 minutes of **information** each.
    Don't simply cut when start_secs passes the threshold, but instead, ignore
    gaps between segments to keep a good information density.
    """
    merged_segments = []
    current_segment_text = []
    current_start_time = 0
    current_end_time = 0
    current_duration = 0

    if segments:
        current_start_time = segments[0].start_secs

    for segment in segments:
        segment_duration = segment.end_secs - segment.start_secs

        if current_duration + segment_duration > target_duration_secs:
            # Merge the current segments and reset for the next group
            merged_segments.append(
                TranscriptSegment(
                    text=" ".join(current_segment_text),
                    start_secs=current_start_time,
                    end_secs=current_start_time + current_end_time,
                ),
            )
            current_segment_text = []
            current_duration = 0
            current_start_time = segment.start_secs

        # Add the segment to the current block
        current_segment_text.append(segment.text)
        current_end_time = segment.end_secs
        current_duration += segment.end_secs - segment.start_secs

    # Add the last segment if there's any remaining text
    if current_segment_text:
        merged_segments.append(
            TranscriptSegment(
                text=" ".join(current_segment_text),
                start_secs=current_start_time,
                end_secs=current_end_time,
            ),
        )

    return merged_segments


def merge_segments_sparsely(
    segments: list[TranscriptSegment],
    target_duration_secs: int = 300,
) -> list[TranscriptSegment]:
    """
    To save on tokens, we merge the segments of transcripts > 10 minutes into
    segments of ~ 5 minutes each.  This allows us to get a rough idea for when
    something was said, while using roughly half the tokens.

    NOTE: This builds segments of 5 minutes of **chronology** each.
    We cut when start_secs passes the threshold.  The tradeoff: less information
    density, to get a better idea of when a statement was spoken.
    """
    merged_segments = []
    current_segment_text = []
    current_start_time = 0
    current_end_time = 0

    if segments:
        current_start_time = segments[0].start_secs

    for segment in segments:
        segment_block = segment.start_secs // target_duration_secs

        # The segment belongs to a new block; append the accummulated text
        if segment_block > len(merged_segments) and current_segment_text:
            merged_segments.append(
                TranscriptSegment(
                    text=" ".join(current_segment_text),
                    start_secs=current_start_time,
                    end_secs=current_end_time,
                ),
            )
            current_segment_text = []
            current_start_time = segment.start_secs

        # Accumuate text for the current block
        current_segment_text.append(segment.text)
        current_end_time = segment.end_secs

    # Add the last block if there's any remaining text
    if current_segment_text:
        merged_segments.append(
            TranscriptSegment(
                text=" ".join(current_segment_text),
                start_secs=current_start_time,
                end_secs=current_end_time,
            ),
        )

    return merged_segments


def _parse_srt_time(time: str) -> float:
    parsed = datetime.strptime(time, "%H:%M:%S,%f")
    return (
        parsed.hour * 3600
        + parsed.minute * 60
        + parsed.second
        + parsed.microsecond / 1e6
    )


def _format_srt_time(secs: float) -> str:
    return datetime.fromtimestamp(secs, UTC).strftime("%H:%M:%S,%f")[:-3]
