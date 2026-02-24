from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Literal


@dataclass(slots=True)
class PagePagination:
    page_param: str = "page"
    size_param: str = "size"
    start_page: int = 1

    def iter_params(self, base_params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        page = self.start_page
        while True:
            params = {**base_params, self.page_param: page}
            yield params
            page += 1


@dataclass(slots=True)
class OffsetPagination:
    offset_param: str = "offset"
    size_param: str = "limit"
    start_offset: int = 0

    def iter_params(self, base_params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        offset = self.start_offset
        size = base_params.get(self.size_param)
        while True:
            params = {**base_params, self.offset_param: offset}
            yield params
            if size is None:
                offset += 1
            else:
                offset += int(size)


@dataclass(slots=True)
class CursorPagination:
    cursor_param: str = "cursor"
    next_cursor: str | None = None

    def iter_params(self, base_params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        cursor = self.next_cursor
        while True:
            params = {**base_params}
            if cursor:
                params[self.cursor_param] = cursor
            yield params
            cursor = None


PaginationType = Literal["page", "offset", "cursor"]
