from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from base.core.values import as_yaml
from base.utils.sorted_list import bisect_find, bisect_insert, bisect_make


@dataclass(kw_only=False)
class Item:
    id: int
    val: str


def _get_key(item: Item) -> int:
    return item.id


##
## bisect_find
##


def test_bisect_find_empty() -> None:
    xs: list[Item] = []
    assert bisect_find(xs, 1, key=_get_key) is None


def test_bisect_find_present() -> None:
    xs = [Item(1, "a"), Item(3, "b"), Item(5, "c")]
    # Start
    assert bisect_find(xs, 1, key=_get_key) == Item(1, "a")
    # Middle
    assert bisect_find(xs, 3, key=_get_key) == Item(3, "b")
    # End
    assert bisect_find(xs, 5, key=_get_key) == Item(5, "c")


def test_bisect_find_absent() -> None:
    xs = [Item(1, "a"), Item(3, "b"), Item(5, "c")]
    # Before start
    assert bisect_find(xs, 0, key=_get_key) is None
    # Between
    assert bisect_find(xs, 2, key=_get_key) is None
    # After end
    assert bisect_find(xs, 6, key=_get_key) is None


##
## bisect_insert
##


def _run_bisect_insert(
    xs: list[Item],
    x: Item,
    on_conflict: Literal["keep", "replace"] | Callable,
    expected_list: list[Item],
    expected_return: Item | None,
) -> None:
    actual_list = list(xs)
    actual_return = bisect_insert(actual_list, x, key=_get_key, on_conflict=on_conflict)
    print(f"<actual>\n{as_yaml(actual_list)}\n</actual>")
    print(f"<expected>\n{as_yaml(expected_list)}\n</expected>")
    assert actual_list == expected_list
    assert actual_return == expected_return


def test_bisect_insert_empty() -> None:
    _run_bisect_insert(
        xs=[],
        x=Item(1, "a"),
        on_conflict="replace",
        expected_list=[Item(1, "a")],
        expected_return=None,
    )


def test_bisect_insert_start() -> None:
    _run_bisect_insert(
        xs=[Item(2, "b")],
        x=Item(1, "a"),
        on_conflict="replace",
        expected_list=[Item(1, "a"), Item(2, "b")],
        expected_return=None,
    )


def test_bisect_insert_middle() -> None:
    _run_bisect_insert(
        xs=[Item(1, "a"), Item(3, "c")],
        x=Item(2, "b"),
        on_conflict="replace",
        expected_list=[Item(1, "a"), Item(2, "b"), Item(3, "c")],
        expected_return=None,
    )


def test_bisect_insert_end() -> None:
    _run_bisect_insert(
        xs=[Item(1, "a")],
        x=Item(2, "b"),
        on_conflict="replace",
        expected_list=[Item(1, "a"), Item(2, "b")],
        expected_return=None,
    )


def test_bisect_insert_duplicate_replace() -> None:
    _run_bisect_insert(
        xs=[Item(1, "old")],
        x=Item(1, "new"),
        on_conflict="replace",
        expected_list=[Item(1, "new")],
        expected_return=Item(1, "old"),
    )


def test_bisect_insert_duplicate_keep() -> None:
    _run_bisect_insert(
        xs=[Item(1, "old")],
        x=Item(1, "new"),
        on_conflict="keep",
        expected_list=[Item(1, "old")],
        expected_return=Item(1, "new"),
    )


def test_bisect_insert_duplicate_conflict() -> None:
    _run_bisect_insert(
        xs=[Item(1, "a")],
        x=Item(1, "b"),
        on_conflict=lambda old, new: Item(old.id, old.val + new.val),
        expected_list=[Item(1, "ab")],
        expected_return=Item(1, "a"),
    )


##
## bisect_make
##


def _run_bisect_make(items: list[Item], expected: list[Item]) -> None:
    result = bisect_make(items, key=_get_key)  # default on_conflict="replace"
    print(f"<actual>\n{as_yaml(result)}\n</actual>")
    print(f"<expected>\n{as_yaml(expected)}\n</expected>")
    assert result == expected


def test_bisect_make_empty() -> None:
    _run_bisect_make([], [])


def test_bisect_make_sorted() -> None:
    items = [Item(1, "a"), Item(2, "b")]
    _run_bisect_make(items, items)


def test_bisect_make_unsorted() -> None:
    items = [Item(2, "b"), Item(1, "a"), Item(3, "c")]
    expected = [Item(1, "a"), Item(2, "b"), Item(3, "c")]
    _run_bisect_make(items, expected)


def test_bisect_make_duplicates() -> None:
    # Last one wins
    items = [Item(1, "a"), Item(2, "b"), Item(1, "c")]
    expected = [Item(1, "c"), Item(2, "b")]
    _run_bisect_make(items, expected)
