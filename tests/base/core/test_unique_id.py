import dateutil.parser

from base.core.unique_id import (
    unique_id_from_str,
    _unique_id_from_int,
    unique_id_random,
    unique_id_from_datetime,
)


##
## unique_id_from_str
##


def _run_unique_id_from_str(
    value: str,
    num_chars: int,
    salt: str | None,
    expected: str,
) -> None:
    actual = unique_id_from_str(value, num_chars=num_chars, salt=salt)
    print(actual)
    assert len(actual) == num_chars
    assert actual == expected


def test_unique_id_from_str_defaults() -> None:
    _run_unique_id_from_str(
        value="testing",
        num_chars=44,
        salt=None,
        expected="z4dniu1ilv0vj9fccvzvae5jjlkoser9lccw6h38mpa0",
    )


def test_unique_id_from_str_len20() -> None:
    _run_unique_id_from_str(
        value="testing",
        num_chars=20,
        salt=None,
        expected="z4dniu1ilv0vj9fccvzv",
    )


def test_unique_id_from_str_salted() -> None:
    _run_unique_id_from_str(
        value="testing",
        num_chars=44,
        salt="salt",
        expected="d6aabtj00s2jz70fm8fgbjybymncud5ool1um1fazga0",
    )


def test_unique_id_from_str_salted_len20() -> None:
    _run_unique_id_from_str(
        value="testing",
        salt="salt",
        num_chars=20,
        expected="d6aabtj00s2jz70fm8fg",
    )


##
## _unique_id_from_int
##


def _run__unique_id_from_int(value: int, num_chars: int, expected: str) -> None:
    actual = _unique_id_from_int(value, num_chars)
    print(actual)
    assert len(actual) == num_chars
    assert actual == expected


def test__unique_id_from_int() -> None:
    _run__unique_id_from_int(
        value=1_167_609_600,
        num_chars=6,
        expected="jb5xc0",
    )


def test__unique_id_from_int_len4() -> None:
    _run__unique_id_from_int(
        value=1_167_609_600,
        num_chars=4,
        expected="jb5x",
    )


def test__unique_id_from_int_len8() -> None:
    _run__unique_id_from_int(
        value=1_167_609_600,
        num_chars=8,
        expected="00jb5xc0",
    )


##
## unique_id_random
##


def test_unique_id_random() -> None:
    actual = unique_id_random(16)
    assert actual.isalnum()
    assert actual.islower()
    assert len(actual) == 16


def test_unique_id_random_len20() -> None:
    actual = unique_id_random(20)
    assert actual.isalnum()
    assert actual.islower()
    assert len(actual) == 20


##
## unique_id_from_datetime
##


def _run_unique_id_from_datetime(
    input_date: str,
    num_chars: int,
    expected: str,
) -> None:
    timestamp = dateutil.parser.parse(input_date + "T00:00:00Z")
    actual = unique_id_from_datetime(timestamp, num_chars)
    print(actual)
    assert len(actual) == num_chars
    assert actual.isalnum()
    assert actual.islower()
    assert actual.startswith(expected)

    # Check that the random suffix "looks random".
    if (num_rand := num_chars - len(expected)) > 0:
        rand_chars = actual[len(expected) :]
        assert not rand_chars.startswith(expected[:num_rand])
        assert not all(c == rand_chars[0] for c in rand_chars)


def test_unique_id_from_datetime_epoch() -> None:
    _run_unique_id_from_datetime(
        input_date="2007-01-01",
        num_chars=12,
        expected="000000",
    )


def test_unique_id_from_datetime_len6() -> None:
    _run_unique_id_from_datetime(
        input_date="2025-01-01",
        num_chars=6,
        expected="9e7xc0",
    )


def test_unique_id_from_datetime_len12() -> None:
    _run_unique_id_from_datetime(
        input_date="2025-01-01",
        num_chars=12,
        expected="9e7xc0",
    )


def test_unique_id_from_datetime_len20() -> None:
    _run_unique_id_from_datetime(
        input_date="2025-01-01",
        num_chars=20,
        expected="9e7xc0",
    )
