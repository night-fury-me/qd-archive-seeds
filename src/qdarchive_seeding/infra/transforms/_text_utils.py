"""Robust text normalisation for dataset titles and descriptions."""

from __future__ import annotations

import re

import ftfy
from unidecode import unidecode


def normalize_text(text: str) -> str:
    """Normalise ill-formatted dataset text for keyword scoring and embedding.

    Handles underscores, camelCase, HTML tags, encoding issues, and
    special characters commonly found in research dataset metadata.
    """
    if not text:
        return ""

    # Fix encoding issues (mojibake, HTML entities, etc.)
    text = ftfy.fix_text(text)

    # Transliterate unicode to ASCII equivalents
    text = unidecode(text)

    # Replace underscores and hyphens with spaces
    text = text.replace("_", " ").replace("-", " ")

    # Split camelCase boundaries: "camelCase" → "camel Case"
    text = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)

    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", text)

    # Collapse special characters and excess whitespace
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text.lower()
