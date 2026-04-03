#!/usr/bin/env python
"""One-time script to build reference embeddings for the metadata relevance filter.

Usage::

    python -m qdarchive_seeding.infra.transforms._build_reference_embeddings \\
        --output metadata/reference_embeddings.npz

The script combines three sources to create a robust reference centroid:
1. Existing QDA dataset titles/descriptions from the SQLite DB (if available)
2. Curated synthetic positive references with high variance
3. Curated negative examples (for validation, not used in centroid)
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path

import numpy as np
import yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

_DEFAULT_REFERENCES_YAML = "configs/reference_descriptions.yaml"


def load_reference_descriptions(
    path: str = _DEFAULT_REFERENCES_YAML,
) -> tuple[list[str], list[str]]:
    """Load positive and negative reference descriptions from a YAML file."""
    yaml_path = Path(path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"Reference descriptions file not found: {path}")
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    positive = data.get("positive_references", [])
    negative = data.get("negative_references", [])
    if not positive:
        raise ValueError(f"No positive_references found in {path}")
    logger.info(
        "Loaded %d positive, %d negative references from %s",
        len(positive),
        len(negative),
        path,
    )
    return positive, negative


POSITIVE_REFERENCES: list[str] = [
    "Semi-structured interview transcripts with healthcare workers",
    "NVivo project with coded interview data",
    "Thematic analysis codebook for patient narratives",
    "Ethnographic field notes from participant observation",
    "Focus group discussion transcripts",
]

NEGATIVE_REFERENCES: list[str] = [
    "Molecular dynamics simulation of protein folding",
    "Genome-wide association study data",
    "Remote sensing satellite imagery",
    "Crystal structure measurements",
    "Neural network training data for image classification",
]


def load_db_references(db_path: str) -> list[str]:
    """Extract titles and descriptions from known QDA datasets in the SQLite DB."""
    path = Path(db_path)
    if not path.exists():
        logger.info("SQLite DB not found at %s, skipping DB references", db_path)
        return []

    conn = sqlite3.connect(str(path))
    try:
        cursor = conn.execute(
            "SELECT title, description FROM projects WHERE title IS NOT NULL LIMIT 500"
        )
        texts: list[str] = []
        for title, description in cursor:
            parts = []
            if title:
                parts.append(str(title))
            if description:
                parts.append(str(description)[:500])  # Truncate long descriptions
            if parts:
                texts.append(" ".join(parts))
        logger.info("Loaded %d references from SQLite DB", len(texts))
        return texts
    finally:
        conn.close()


def build_reference_embeddings(
    output_path: str,
    db_path: str = "metadata/qdarchive.sqlite",
    model_name: str = "all-mpnet-base-v2",
    references_yaml: str = _DEFAULT_REFERENCES_YAML,
) -> None:
    """Build and save reference embeddings to a compressed numpy archive."""
    from sentence_transformers import SentenceTransformer

    # Load curated references from YAML (fall back to inline constants)
    try:
        pos_refs, neg_refs = load_reference_descriptions(references_yaml)
    except FileNotFoundError:
        logger.warning("YAML not found at %s, using inline fallback", references_yaml)
        pos_refs = list(POSITIVE_REFERENCES)
        neg_refs = list(NEGATIVE_REFERENCES)

    logger.info("Loading model: %s", model_name)
    model = SentenceTransformer(model_name)

    # Combine curated + DB references for the positive set
    positive_texts = list(pos_refs)
    db_texts = load_db_references(db_path)
    if db_texts:
        positive_texts.extend(db_texts)

    logger.info(
        "Encoding %d positive references (%d curated + %d from DB)",
        len(positive_texts),
        len(pos_refs),
        len(db_texts),
    )
    positive_embeddings = model.encode(
        positive_texts, normalize_embeddings=True, show_progress_bar=True
    )

    logger.info("Encoding %d negative references", len(neg_refs))
    negative_embeddings = model.encode(neg_refs, normalize_embeddings=True, show_progress_bar=True)

    # Compute centroid of positive references (L2-normalised)
    centroid = positive_embeddings.mean(axis=0).astype(np.float32)
    centroid = centroid / np.linalg.norm(centroid)

    # Validate: positive examples should be closer to centroid than negatives
    pos_similarities = positive_embeddings @ centroid
    neg_similarities = negative_embeddings @ centroid
    logger.info(
        "Positive similarity — mean: %.3f, min: %.3f, max: %.3f",
        pos_similarities.mean(),
        pos_similarities.min(),
        pos_similarities.max(),
    )
    logger.info(
        "Negative similarity — mean: %.3f, min: %.3f, max: %.3f",
        neg_similarities.mean(),
        neg_similarities.min(),
        neg_similarities.max(),
    )

    if neg_similarities.mean() >= pos_similarities.mean():
        logger.warning(
            "Negative examples are closer to centroid than positive! Review the reference corpus."
        )

    # Save
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        str(output),
        centroid=centroid,
        positive_embeddings=positive_embeddings.astype(np.float32),
        negative_embeddings=negative_embeddings.astype(np.float32),
    )
    logger.info("Saved reference embeddings to %s", output_path)
    logger.info(
        "  centroid: (%d,), positives: %s, negatives: %s",
        centroid.shape[0],
        positive_embeddings.shape,
        negative_embeddings.shape,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build reference embeddings for QDA relevance filter"
    )
    parser.add_argument(
        "--output",
        default="metadata/reference_embeddings.npz",
        help="Output path for the .npz file (default: metadata/reference_embeddings.npz)",
    )
    parser.add_argument(
        "--db",
        default="metadata/qdarchive.sqlite",
        help="Path to the SQLite DB for extracting existing QDA dataset references",
    )
    parser.add_argument(
        "--model",
        default="all-mpnet-base-v2",
        help="Sentence-transformer model name (default: all-mpnet-base-v2)",
    )
    parser.add_argument(
        "--references",
        default=_DEFAULT_REFERENCES_YAML,
        help="Path to YAML with reference descriptions "
        "(default: configs/reference_descriptions.yaml)",
    )
    args = parser.parse_args()
    build_reference_embeddings(
        args.output, db_path=args.db, model_name=args.model, references_yaml=args.references
    )


if __name__ == "__main__":
    main()
