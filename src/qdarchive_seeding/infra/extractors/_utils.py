from __future__ import annotations


def extract_year(date_string: object) -> int | None:
    """Extract year as int from a date/timestamp string like '2024-01-15'."""
    if not date_string or not isinstance(date_string, str):
        return None
    try:
        return int(date_string[:4])
    except (ValueError, IndexError):
        return None
