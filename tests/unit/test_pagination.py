from __future__ import annotations

from qdarchive_seeding.infra.http.pagination import (
    CursorPagination,
    OffsetPagination,
    PagePagination,
)


def test_page_pagination_sequence() -> None:
    pag = PagePagination(page_param="page", size_param="size", start_page=1)
    pages = []
    for i, params in enumerate(pag.iter_params({"size": 10})):
        pages.append(params)
        if i >= 2:
            break
    assert pages[0] == {"size": 10, "page": 1}
    assert pages[1] == {"size": 10, "page": 2}
    assert pages[2] == {"size": 10, "page": 3}


def test_offset_pagination_sequence() -> None:
    pag = OffsetPagination(offset_param="offset", size_param="limit")
    pages = []
    for i, params in enumerate(pag.iter_params({"limit": 50})):
        pages.append(params)
        if i >= 2:
            break
    assert pages[0]["offset"] == 0
    assert pages[1]["offset"] == 50
    assert pages[2]["offset"] == 100


def test_offset_pagination_without_size_param() -> None:
    pag = OffsetPagination(offset_param="offset", size_param="limit")
    pages = []
    for i, params in enumerate(pag.iter_params({})):
        pages.append(params)
        if i >= 2:
            break
    assert pages[0]["offset"] == 0
    assert pages[1]["offset"] == 1
    assert pages[2]["offset"] == 2


def test_cursor_pagination_first_page_no_cursor() -> None:
    pag = CursorPagination(cursor_param="cursor")
    params_iter = pag.iter_params({"q": "test"})
    first = next(params_iter)
    assert "cursor" not in first
    assert first["q"] == "test"


def test_cursor_pagination_updates_cursor() -> None:
    pag = CursorPagination(cursor_param="cursor")
    params_iter = pag.iter_params({"q": "test"})

    first = next(params_iter)
    assert "cursor" not in first

    pag.update_cursor("abc")
    second = next(params_iter)
    assert second["cursor"] == "abc"

    pag.update_cursor(None)
    try:
        next(params_iter)
        assert False, "Iterator should stop when cursor is None"
    except StopIteration:
        assert True
