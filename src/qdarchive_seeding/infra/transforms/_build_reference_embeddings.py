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

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Curated reference descriptions — diverse qualitative research topics
# ---------------------------------------------------------------------------
POSITIVE_REFERENCES: list[str] = [
    # Interview studies
    "Semi-structured interview transcripts with healthcare workers about burnout experiences",
    "In-depth interviews with refugees on resettlement challenges and cultural adaptation",
    "Expert interviews on climate policy governance and stakeholder engagement",
    "Narrative interviews with cancer survivors about treatment decision-making",
    "Key informant interviews with community leaders on rural development",
    "Interview transcripts from a study of teacher experiences during school closures",
    "Qualitative interviews with social workers about child protection practices",
    "Life history interviews with elderly immigrants about identity and belonging",
    "Interviews with parents of children with autism about educational support",
    "Telephone interviews with nurses about workplace violence experiences",
    # Focus groups
    "Focus group discussion transcripts on adolescent mental health stigma",
    "Community focus group data on food security perceptions in urban areas",
    "Focus group transcripts exploring patient experiences with telehealth",
    "Youth focus group discussions about social media and body image",
    "Focus group data from women entrepreneurs in developing countries",
    "Stakeholder focus groups on public transportation accessibility",
    # Ethnography & fieldwork
    "Ethnographic field notes from participant observation in urban schools",
    "Autoethnographic reflections on disability and identity in academia",
    "Ethnographic study of workplace culture in tech startups",
    "Field notes from community-based participatory research in Indigenous communities",
    "Participant observation data from a study of religious communities",
    "Ethnographic research on informal economies in Southeast Asia",
    # Coding & analysis
    "Thematic analysis codebook for patient narratives about chronic pain",
    "NVivo project with coded interview data on workplace discrimination",
    "Grounded theory analysis of nursing care practices in palliative settings",
    "ATLAS.ti project file with coded focus group data on housing policy",
    "Qualitative codebook for analysing political discourse in social media",
    "MAXQDA project with coded fieldwork observations on refugee integration",
    "Coding framework for content analysis of news coverage on climate change",
    "Deductive coding scheme for analysis of policy documents on education reform",
    "Inductive coding of patient diary entries about living with diabetes",
    # Mixed methods & surveys
    "Qualitative survey responses about teacher experiences during COVID-19",
    "Mixed methods study combining interview and survey data on housing insecurity",
    "Open-ended questionnaire responses on employee wellbeing",
    "Qualitative data from a convergent mixed methods study on substance use",
    # Various domains
    "Phenomenological study of first-generation college students experiences",
    "Discourse analysis of parliamentary debates on immigration policy",
    "Conversation analysis transcripts of doctor-patient interactions",
    "Oral history interviews with civil rights movement participants",
    "Narrative inquiry into the experiences of LGBTQ youth in foster care",
    "Qualitative exploration of barriers to mental health help-seeking",
    "Case study of community resilience after natural disasters",
    "Action research on participatory design with persons with disabilities",
    "Qualitative study of professional identity formation in medical education",
    "Framework analysis of health policy implementation in low-income settings",
    "Interpretive phenomenological analysis of grief and bereavement",
    "Critical discourse analysis of media representations of poverty",
    "Photovoice project with homeless youth documenting daily challenges",
    "Qualitative research on cultural competency in healthcare delivery",
    "Interview study of decision-making processes in juvenile justice",
    "Qualitative dataset on community perspectives about water management",
    "Longitudinal qualitative study of recovery from substance addiction",
    "Research journal and reflexive notes from an ethnographic study of policing",
    # Less common but valid qualitative contexts
    "Transcript data from think-aloud protocol study on reading comprehension",
    "Qualitative content analysis of online support forum discussions",
    "Digital ethnography of political activism on social media platforms",
    "Diary study data on daily coping strategies of informal caregivers",
    "Observational notes from classroom interaction analysis",
    "Qualitative evaluation of a community health intervention programme",
    "Biographical narrative interviews with Holocaust survivors",
    "Hermeneutic analysis of religious texts and practitioner interviews",
    "Participatory action research with Indigenous land rights activists",
    "Visual methods data from a photo-elicitation study with migrant workers",
    # Short/minimal descriptions (test robustness)
    "Interview transcripts qualitative study",
    "Focus group discussion data",
    "NVivo project coded interviews",
    "Thematic analysis codebook",
    "Ethnographic field notes",
    "Qualitative interview data education",
    "Grounded theory healthcare study",
    "Semi-structured interviews social work",
    "Qualitative coding scheme",
    "Interview data and codebook",
    # Domain-specific qualitative research
    "Qualitative nursing research on end-of-life care decisions",
    "Social work practice interviews with foster care families",
    "Education research interviews with teachers about curriculum reform",
    "Anthropological fieldwork on ritual practices in West Africa",
    "Political science interview data on voter behaviour and motivation",
    "Communication studies analysis of organizational discourse",
    "Criminology interview transcripts with formerly incarcerated individuals",
    "Public health qualitative data on vaccine hesitancy in rural communities",
    "Geography fieldwork interviews on climate change adaptation strategies",
    "Psychology qualitative study on adolescent identity development",
    "Sociology interview transcripts on social mobility and class",
    "Business research qualitative data on entrepreneurship motivations",
    "Law and society interview data on access to justice",
    "Gender studies interview transcripts on reproductive rights",
    "Disability studies qualitative data on inclusive education policies",
]

NEGATIVE_REFERENCES: list[str] = [
    "Molecular dynamics simulation of protein folding in aqueous solutions",
    "Genome-wide association study of cardiovascular disease risk factors",
    "Remote sensing satellite imagery of Amazon deforestation patterns",
    "Crystal structure measurements of novel organic semiconductor compounds",
    "Phylogenetic analysis of mitochondrial DNA in avian species",
    "Climate model output for global temperature projections under RCP scenarios",
    "Mass spectrometry data from metabolomics profiling of plasma samples",
    "X-ray diffraction patterns of metallic alloy nanoparticles",
    "Electron microscopy images of neural tissue cross-sections",
    "Finite element analysis of structural stress in bridge components",
    "Seismic wave propagation data from earthquake monitoring stations",
    "DNA sequencing reads from whole-genome shotgun metagenomics",
    "Atmospheric chemistry measurements of ozone depletion indicators",
    "Quantum computing benchmark results for variational algorithms",
    "Neural network training data for image classification tasks",
    "Fluid dynamics simulation of turbulent flow in pipe networks",
    "Spectroscopic analysis of stellar atmospheres in binary systems",
    "Geospatial analysis of land use change using LIDAR point clouds",
    "Chemical synthesis data for novel pharmaceutical drug candidates",
    "Particle accelerator collision event data from the LHC",
    "RNA sequencing expression profiles across mouse brain regions",
    "Machine learning model predictions for material properties",
    "Astronomical photometry of variable stars in globular clusters",
    "Protein crystallography data for enzyme active site characterisation",
    "Computational fluid dynamics results for aerodynamic optimization",
    "Numerical simulation output for galaxy formation and evolution",
    "Bioinformatics pipeline results for variant calling analysis",
    "Sensor network measurements of soil moisture and temperature",
    "Geological survey data of mineral compositions in sedimentary rocks",
    "High-throughput screening results for antimicrobial compound activity",
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
) -> None:
    """Build and save reference embeddings to a compressed numpy archive."""
    from sentence_transformers import SentenceTransformer

    logger.info("Loading model: %s", model_name)
    model = SentenceTransformer(model_name)

    # Combine curated + DB references for the positive set
    positive_texts = list(POSITIVE_REFERENCES)
    db_texts = load_db_references(db_path)
    if db_texts:
        positive_texts.extend(db_texts)

    logger.info(
        "Encoding %d positive references (%d curated + %d from DB)",
        len(positive_texts),
        len(POSITIVE_REFERENCES),
        len(db_texts),
    )
    positive_embeddings = model.encode(
        positive_texts, normalize_embeddings=True, show_progress_bar=True
    )

    logger.info("Encoding %d negative references", len(NEGATIVE_REFERENCES))
    negative_embeddings = model.encode(
        NEGATIVE_REFERENCES, normalize_embeddings=True, show_progress_bar=True
    )

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
    args = parser.parse_args()
    build_reference_embeddings(args.output, db_path=args.db, model_name=args.model)


if __name__ == "__main__":
    main()
