"""Hybrid keyword + embedding relevance filter for qualitative research datasets."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms._text_utils import normalize_text
from qdarchive_seeding.infra.transforms.base import BaseTransform

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default keyword signal dictionaries
# ---------------------------------------------------------------------------
DEFAULT_POSITIVE_SIGNALS: dict[str, float] = {
    # Tier 1: Definitive QDA signals
    "nvivo": 10,
    "atlas ti": 10,
    "maxqda": 10,
    "dedoose": 10,
    "refi qda": 10,
    "interview transcript": 10,
    "focus group transcript": 10,
    # Tier 2: Strong signals
    "thematic analysis": 8,
    "grounded theory": 8,
    "qualitative coding": 8,
    "in depth interview": 7,
    "discourse analysis": 6,
    "narrative analysis": 6,
    "semi structured": 6,
    "participant observation": 6,
    "codebook": 6,
    "coding framework": 5,
    "coded data": 5,
    "interview": 5,
    "focus group": 5,
    "ethnograph": 5,
    "field notes": 5,
    # Tier 3: Moderate signals
    "qualitative": 4,
    "transcript": 4,
    "oral history": 4,
    "phenomenolog": 3,
    "mixed methods": 3,
    "case study": 2,
    "action research": 2,
}

DEFAULT_NEGATIVE_SIGNALS: dict[str, float] = {
    "genome": -8,
    "genomic": -8,
    "nucleotide": -8,
    "gene expression": -8,
    "molecular dynamics": -8,
    "particle physics": -8,
    "astrophys": -8,
    "mass spectrometry": -8,
    "spectroscop": -6,
    "crystallograph": -6,
    "phylogenet": -6,
    "remote sensing": -6,
    "climate model": -6,
    "x ray diffraction": -6,
    "electron microscop": -6,
    "fluid dynamics": -6,
    "dna sequenc": -6,
    "rna sequenc": -6,
    "protein": -6,
    "geospatial": -5,
    "atmospheric": -5,
    "seismic": -6,
    "finite element": -5,
    "chemical compound": -5,
    "quantum": -6,
    "neural network": -4,
    "machine learning": -3,
    "simulation": -3,
}


def _combine_text(record: DatasetRecord) -> str:
    """Combine title, description, and keywords into a single string."""
    parts: list[str] = []
    if record.title:
        parts.append(record.title)
    if record.description:
        parts.append(record.description)
    if record.keywords:
        parts.append(" ".join(record.keywords))
    return " ".join(parts)


@dataclass(slots=True)
class MetadataRelevanceFilter(BaseTransform):
    """Hybrid keyword + embedding relevance filter.

    **Stage A** — fast keyword scoring for clear-cut decisions:
      * score >= *keyword_keep_threshold* → keep immediately
      * score <= *keyword_drop_threshold* → drop immediately

    **Stage B** — sentence-embedding cosine similarity for ambiguous cases:
      * similarity >= *embedding_similarity_threshold* → keep
      * otherwise → drop

    Parameters
    ----------
    positive_signals / negative_signals : dict[str, float]
        Term → weight mappings for keyword scoring.
    keyword_keep_threshold : float
        Score at or above which a dataset is kept without embedding.
    keyword_drop_threshold : float
        Score at or below which a dataset is dropped without embedding.
    embedding_similarity_threshold : float
        Minimum cosine similarity to the QDA reference centroid.
    reference_embeddings_path : str
        Path to the ``.npz`` file with pre-computed reference centroid.
    bypass_with_analysis_data : bool
        Never drop datasets that already contain a QDA analysis file.
    """

    positive_signals: dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_POSITIVE_SIGNALS)
    )
    negative_signals: dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_NEGATIVE_SIGNALS)
    )
    keyword_keep_threshold: float = 8.0
    keyword_drop_threshold: float = -4.0
    embedding_similarity_threshold: float = 0.35
    reference_embeddings_path: str = "metadata/reference_embeddings.npz"
    bypass_with_analysis_data: bool = True
    _model: Any = field(default=None, repr=False)
    _centroid: np.ndarray | None = field(default=None, repr=False)

    def _ensure_model_loaded(self) -> None:
        """Lazily load the sentence-transformer model and reference centroid."""
        if self._model is not None:
            return
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer("all-mpnet-base-v2")
        data = np.load(self.reference_embeddings_path)
        self._centroid = data["centroid"].astype(np.float32)
        logger.debug(
            "Loaded reference centroid from %s (dim=%d)",
            self.reference_embeddings_path,
            self._centroid.shape[0],
        )

    def apply(self, record: DatasetRecord) -> DatasetRecord | None:  # noqa: D401
        dataset_id = record.source_dataset_id or record.source_url
        title_short = (record.title or "")[:80]

        # Bypass: always keep datasets with QDA-specific files
        if self.bypass_with_analysis_data and any(
            a.asset_type == "analysis_data" for a in record.assets
        ):
            logger.debug("[relevance] BYPASS (analysis_data) %s", dataset_id)
            return record

        text = normalize_text(_combine_text(record))
        if not text:
            logger.debug("[relevance] DROP (empty text) %s", dataset_id)
            return None

        # Stage A: keyword scoring
        score = self._keyword_score(text)
        if score >= self.keyword_keep_threshold:
            logger.debug(
                "[relevance] KEEP (keyword score=%.1f) %s — %s",
                score,
                dataset_id,
                title_short,
            )
            return record
        if score <= self.keyword_drop_threshold:
            logger.debug(
                "[relevance] DROP (keyword score=%.1f) %s — %s",
                score,
                dataset_id,
                title_short,
            )
            return None

        # Stage B: embedding similarity (ambiguous cases only)
        self._ensure_model_loaded()
        assert self._centroid is not None  # guaranteed by _ensure_model_loaded
        embedding = self._model.encode(text, normalize_embeddings=True)
        similarity = float(np.dot(embedding, self._centroid))

        if similarity >= self.embedding_similarity_threshold:
            logger.info(
                "[relevance] KEEP (embedding sim=%.3f, keyword=%.1f) %s — %s",
                similarity,
                score,
                dataset_id,
                title_short,
            )
            return record

        logger.info(
            "[relevance] DROP (embedding sim=%.3f, keyword=%.1f) %s — %s",
            similarity,
            score,
            dataset_id,
            title_short,
        )
        return None

    def _keyword_score(self, text: str) -> float:
        score = 0.0
        for term, weight in self.positive_signals.items():
            if term in text:
                score += weight
        for term, weight in self.negative_signals.items():
            if term in text:
                score += weight
        return score
