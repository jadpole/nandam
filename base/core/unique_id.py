import base64
import secrets
import string

from datetime import datetime, UTC
from hashlib import sha256


BASE36_CHARS = string.digits + string.ascii_lowercase
"""
To make int-generated IDs ordered, we keep only lowercase alphanumeric chars,
in the order of their ASCII values.
"""

NANDAM_EPOCH = 1_167_609_600
"""
Time-ordered IDs start counting from January 1st 2007 00:00:00 UTC.
"""


def unique_id_from_str(
    value: str,
    num_chars: int,
    salt: str | None,
) -> str:
    """
    Generate a base36 string (lowercase alphanumeric) from the SHA256 hash of
    the value, then pick the first `num_chars` characters (max 44).

    Since we lowercase the base64 of the hash and replace "+/=", there are fewer
    possible IDs (and a higher collision risk).  Pick `num_chars` based on what
    is "sufficiently random":

    - At `num_chars = 6`,
        - `36^8 = 2.2e9` possibilities.
        - `46,656` items to reach a 50% risk of collision.
    - At `num_chars = 8`,
        - `36^8 = 2.8e12` possibilities.
        - `1,679,616` items to reach a 50% risk of collision.
    - At `num_chars = 12`,
        - `36^12 = 4.7e18` possibilities.
        - `2,176,782,336` items to reach a 50% risk of collision.
    - At `num_chars = 24`,
        - `36^24 = 2.2e37` possibilities.
        - `3.7e18` items to reach a 50% risk of collision.
    - At `num_chars = 44`,
        - `36^44 = 3.0e66` possibilities (or a bit less, due to "=" padding).
        - `1.7e34` items to reach a 50% risk of collision.
    """
    assert 0 <= num_chars <= 44  # noqa: PLR2004
    salted = f"{salt}:{value}" if salt else value
    digest = sha256(salted.encode()).digest()
    return _unique_id_from_digest(digest, num_chars)


def unique_id_from_datetime(
    dt: datetime | None = None,
    num_chars: int = 12,
) -> str:
    assert num_chars >= 6  # noqa: PLR2004
    time_chars = 6
    rand_chars = num_chars - time_chars
    dt = dt or datetime.now(UTC)
    dt_secs = int(dt.timestamp()) - NANDAM_EPOCH
    return f"{_unique_id_from_int(dt_secs, time_chars)}{unique_id_random(rand_chars)}"


def unique_id_random(num_chars: int = 16) -> str:
    return "".join(secrets.choice(BASE36_CHARS) for _ in range(num_chars))


def _unique_id_from_digest(digest: bytes, num_chars: int = 40) -> str:
    data_base64 = base64.b64encode(digest).decode("utf-8")
    alphanum = data_base64.lower().replace("+", "a").replace("/", "b").replace("=", "0")
    return alphanum[0:num_chars]


def _unique_id_from_int(value: int, num_chars: int = 6) -> str:
    """
    Convert an integer to a fixed-length alphanumeric string.

    For example, using a duration in seconds,

    - A whole year uses 5 characters.
    - Since the Unix epoch, only 6 characters are required, and this will remain
      true until December 24, 2038.
      - Using base 62, it would hold until December 5, 3769.
    - Since the "Nandam epoch", 6 characters are required, and this will
      remain true until December 24, 2075 (+ 37 years).
      - For example, "January 1st 2025 00:00:00 UTC" maps to "9e7xc0".

    Hence, the default of 6 should cover most cases.
    """
    base = len(BASE36_CHARS)
    result = []

    while value:
        value, remainder = divmod(value, base)
        result.append(BASE36_CHARS[remainder])

    if len(result) < num_chars:
        result.extend(["0"] * (num_chars - len(result)))

    return "".join(reversed(result))[0:num_chars]
