from documents.models.transcript import (
    TranscriptSegment,
    merge_segments_densely,
    merge_segments_sparsely,
)


##
## TranscriptSegment.format_srt
##


def test_transcript_segment_format_srt_single():
    segments = [
        TranscriptSegment(text="Hello, world!", start_secs=0.0, end_secs=2.5),
    ]
    actual = TranscriptSegment.format_srt(segments)
    expected = "1\n00:00:00,000 --> 00:00:02,500\nHello, world!"
    assert actual == expected


def test_transcript_segment_format_srt_multiple():
    segments = [
        TranscriptSegment(text="Hello, world!", start_secs=0.0, end_secs=2.5),
        TranscriptSegment(text="How are you?", start_secs=3.0, end_secs=5.0),
        TranscriptSegment(text="I am fine.", start_secs=6.0, end_secs=8.5),
    ]
    actual = TranscriptSegment.format_srt(segments)
    expected = (
        "1\n00:00:00,000 --> 00:00:02,500\nHello, world!\n\n"
        "2\n00:00:03,000 --> 00:00:05,000\nHow are you?\n\n"
        "3\n00:00:06,000 --> 00:00:08,500\nI am fine."
    )
    assert actual == expected


def test_transcript_segment_format_srt_empty():
    segments: list[TranscriptSegment] = []
    actual = TranscriptSegment.format_srt(segments)
    assert actual == ""


##
## TranscriptSegment.format_text
##


def test_transcript_segment_format_text_single():
    segments = [
        TranscriptSegment(text="Hello, world!", start_secs=0.0, end_secs=2.5),
    ]
    actual = TranscriptSegment.format_text(segments)
    assert actual == "Hello, world!"


def test_transcript_segment_format_text_multiple():
    segments = [
        TranscriptSegment(text="Hello, world!", start_secs=0.0, end_secs=2.5),
        TranscriptSegment(text="How are you?", start_secs=3.0, end_secs=5.0),
        TranscriptSegment(text="I am fine.", start_secs=6.0, end_secs=8.5),
    ]
    actual = TranscriptSegment.format_text(segments)
    assert actual == "Hello, world!\nHow are you?\nI am fine."


def test_transcript_segment_format_text_empty():
    segments: list[TranscriptSegment] = []
    actual = TranscriptSegment.format_text(segments)
    assert actual == ""


##
## TranscriptSegment.parse_srt
##


def test_transcript_segment_parse_srt_single():
    srt = "1\n00:00:00,000 --> 00:00:02,500\nHello, world!"
    actual = TranscriptSegment.parse_srt(srt)
    assert len(actual) == 1
    assert actual[0].text == "Hello, world!"
    assert actual[0].start_secs == 0.0
    assert actual[0].end_secs == 2.5


def test_transcript_segment_parse_srt_multiple():
    srt = (
        "1\n00:00:00,000 --> 00:00:02,500\nHello, world!\n\n"
        "2\n00:00:03,000 --> 00:00:05,000\nHow are you?\n\n"
        "3\n00:00:06,000 --> 00:00:08,500\nI am fine."
    )
    actual = TranscriptSegment.parse_srt(srt)
    assert len(actual) == 3
    assert actual[0].text == "Hello, world!"
    assert actual[0].start_secs == 0.0
    assert actual[0].end_secs == 2.5
    assert actual[1].text == "How are you?"
    assert actual[1].start_secs == 3.0
    assert actual[1].end_secs == 5.0
    assert actual[2].text == "I am fine."
    assert actual[2].start_secs == 6.0
    assert actual[2].end_secs == 8.5


def test_transcript_segment_parse_srt_empty():
    srt = ""
    actual = TranscriptSegment.parse_srt(srt)
    assert actual == []


def test_transcript_segment_parse_srt_roundtrip():
    segments = [
        TranscriptSegment(text="Hello, world!", start_secs=0.0, end_secs=2.5),
        TranscriptSegment(text="How are you?", start_secs=3.0, end_secs=5.0),
    ]
    srt = TranscriptSegment.format_srt(segments)
    parsed = TranscriptSegment.parse_srt(srt)
    assert len(parsed) == 2
    assert parsed[0].text == segments[0].text
    assert parsed[0].start_secs == segments[0].start_secs
    assert parsed[0].end_secs == segments[0].end_secs
    assert parsed[1].text == segments[1].text
    assert parsed[1].start_secs == segments[1].start_secs
    assert parsed[1].end_secs == segments[1].end_secs


##
## TranscriptSegment.shift_time
##


def test_transcript_segment_shift_time_positive():
    segment = TranscriptSegment(text="Hello", start_secs=10.0, end_secs=15.0)
    shifted = segment.shift_time(5.0)
    assert shifted.text == "Hello"
    assert shifted.start_secs == 15.0
    assert shifted.end_secs == 20.0


def test_transcript_segment_shift_time_negative():
    segment = TranscriptSegment(text="Hello", start_secs=10.0, end_secs=15.0)
    shifted = segment.shift_time(-5.0)
    assert shifted.text == "Hello"
    assert shifted.start_secs == 5.0
    assert shifted.end_secs == 10.0


def test_transcript_segment_shift_time_zero():
    segment = TranscriptSegment(text="Hello", start_secs=10.0, end_secs=15.0)
    shifted = segment.shift_time(0.0)
    assert shifted.text == "Hello"
    assert shifted.start_secs == 10.0
    assert shifted.end_secs == 15.0


##
## merge_segments_densely
##


def test_merge_segments_densely_empty():
    segments: list[TranscriptSegment] = []
    actual = merge_segments_densely(segments)
    assert actual == []


def test_merge_segments_densely_single_short():
    segments = [
        TranscriptSegment(text="Hello", start_secs=0.0, end_secs=2.0),
    ]
    actual = merge_segments_densely(segments, target_duration_secs=300)
    assert len(actual) == 1
    assert actual[0].text == "Hello"


def test_merge_segments_densely_multiple_within_target():
    segments = [
        TranscriptSegment(text="Hello", start_secs=0.0, end_secs=60.0),
        TranscriptSegment(text="world", start_secs=60.0, end_secs=120.0),
        TranscriptSegment(text="again", start_secs=120.0, end_secs=180.0),
    ]
    actual = merge_segments_densely(segments, target_duration_secs=300)
    assert len(actual) == 1
    assert actual[0].text == "Hello world again"
    assert actual[0].start_secs == 0.0
    assert actual[0].end_secs == 180.0


def test_merge_segments_densely_splits_at_target():
    """Each segment is 100 secs. With target 150, only one fits per block."""
    segments = [
        TranscriptSegment(text="Part 1", start_secs=0.0, end_secs=100.0),
        TranscriptSegment(text="Part 2", start_secs=100.0, end_secs=200.0),
        TranscriptSegment(text="Part 3", start_secs=200.0, end_secs=300.0),
        TranscriptSegment(text="Part 4", start_secs=300.0, end_secs=400.0),
    ]
    actual = merge_segments_densely(segments, target_duration_secs=150)
    # Each 100-sec segment triggers a new block after the first
    assert len(actual) == 4
    assert actual[0].text == "Part 1"
    assert actual[1].text == "Part 2"
    assert actual[2].text == "Part 3"
    assert actual[3].text == "Part 4"


##
## merge_segments_sparsely
##


def test_merge_segments_sparsely_empty():
    segments: list[TranscriptSegment] = []
    actual = merge_segments_sparsely(segments)
    assert actual == []


def test_merge_segments_sparsely_single_short():
    segments = [
        TranscriptSegment(text="Hello", start_secs=0.0, end_secs=2.0),
    ]
    actual = merge_segments_sparsely(segments, target_duration_secs=300)
    assert len(actual) == 1
    assert actual[0].text == "Hello"


def test_merge_segments_sparsely_splits_by_chronology():
    segments = [
        TranscriptSegment(text="Part 1", start_secs=0.0, end_secs=100.0),
        TranscriptSegment(text="Part 2", start_secs=150.0, end_secs=200.0),
        TranscriptSegment(text="Part 3", start_secs=350.0, end_secs=400.0),
        TranscriptSegment(text="Part 4", start_secs=500.0, end_secs=550.0),
    ]
    actual = merge_segments_sparsely(segments, target_duration_secs=300)
    # First block: 0-300 (Part 1, Part 2)
    # Second block: 300-600 (Part 3, Part 4)
    assert len(actual) == 2
    assert actual[0].text == "Part 1 Part 2"
    assert actual[0].start_secs == 0.0
    assert actual[0].end_secs == 200.0
    assert actual[1].text == "Part 3 Part 4"
    assert actual[1].start_secs == 350.0
    assert actual[1].end_secs == 550.0
