import bisect

from collections.abc import Callable
from typing import Literal


def bisect_find[T, K](xs: list[T], k: K, key: Callable[[T], K]) -> T | None:
    index = bisect.bisect_left(xs, k, key=key)  # type: ignore
    if index != len(xs) and key(xs[index]) == k:
        return xs[index]
    else:
        return None


def bisect_insert[T, K](
    xs: list[T],
    x: T,
    key: Callable[[T], K],
    on_conflict: Literal["keep", "replace"] | Callable[[T, T], T] = "replace",
) -> T | None:
    """
    Insert `x` into `xs` at the correct index, sorted by `key`.
    Use `on_conflict` to pick the resolution strategy when the key is equal.

    NOTE: When a conflict occurs:
    - If `on_conflict` is "keep", then return the new value (not inserted).
    - Otherwise, return the previous value.
    """
    index = bisect.bisect_left(xs, key(x), key=key)  # type: ignore
    if index == len(xs):
        xs.append(x)
        return None
    elif key(xs[index]) != key(x):
        xs.insert(index, x)
        return None
    elif on_conflict == "keep":
        return x
    else:
        prev_x = xs[index]
        xs[index] = x if on_conflict == "replace" else on_conflict(prev_x, x)
        return prev_x


def bisect_make[T, K](
    xs: list[T],
    key: Callable[[T], K],
    on_conflict: Literal["keep", "replace"] | Callable[[T, T], T] = "replace",
) -> list[T]:
    """
    Return a copy of the list, sorted and deduplicated by `key`.
    """
    result: list[T] = []
    for x in xs:
        bisect_insert(result, x, key, on_conflict)
    return result
