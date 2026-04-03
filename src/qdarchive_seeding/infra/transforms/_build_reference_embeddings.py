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
    # ── Interview studies (diverse types, populations, domains) ──
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
    "Structured interviews with policymakers about healthcare reform priorities",
    "Unstructured conversational interviews with artists about creative process",
    "Elite interviews with politicians on legislative decision-making",
    "Dyadic interviews with couples navigating infertility treatment",
    "Walking interviews with residents about neighbourhood safety perceptions",
    "Video-recorded interviews with dementia patients and their caregivers",
    "Email interviews with international students about remote learning",
    "Repeat interviews with homeless individuals tracked over two years",
    "Group interviews with farm workers about pesticide exposure concerns",
    "Photo-elicitation interviews with youth about urban space and belonging",
    "Interviews with midwives about birth practices in rural clinics",
    "Interview data from disaster survivors about long-term recovery",
    "Interviews with transgender individuals about healthcare access barriers",
    "Interview transcripts with peacekeepers about conflict zone experiences",
    "Exit interviews with employees about organisational culture and turnover",
    # ── Focus groups (diverse populations and topics) ──
    "Focus group discussion transcripts on adolescent mental health stigma",
    "Community focus group data on food security perceptions in urban areas",
    "Focus group transcripts exploring patient experiences with telehealth",
    "Youth focus group discussions about social media and body image",
    "Focus group data from women entrepreneurs in developing countries",
    "Stakeholder focus groups on public transportation accessibility",
    "Focus group discussions with elderly residents on aging in place",
    "Parent focus groups on school bullying prevention strategies",
    "Focus group data from migrant farmworkers on housing conditions",
    "Clinician focus groups on barriers to implementing palliative care",
    "Focus group transcripts on community perceptions of policing",
    "Teacher focus groups on inclusive classroom practices for special needs",
    "Online focus group data on consumer attitudes toward sustainable fashion",
    "Focus group discussions with veterans about reintegration challenges",
    "Faith community focus groups on social justice and civic engagement",
    # ── Ethnography & fieldwork ──
    "Ethnographic field notes from participant observation in urban schools",
    "Autoethnographic reflections on disability and identity in academia",
    "Ethnographic study of workplace culture in tech startups",
    "Field notes from community-based participatory research in Indigenous communities",
    "Participant observation data from a study of religious communities",
    "Ethnographic research on informal economies in Southeast Asia",
    "Multi-sited ethnography of transnational migration networks",
    "Institutional ethnography of social services for homeless populations",
    "Sensory ethnography data from a study of food markets in Mexico City",
    "Digital ethnography of online gaming communities and identity",
    "Visual ethnography of street art and urban regeneration",
    "Ethnographic fieldwork on traditional healing practices in rural Ghana",
    "School ethnography examining peer relationships and social hierarchies",
    "Workplace ethnography of hospital emergency department routines",
    "Ethnographic observation notes from refugee camp daily life",
    "Netnography of online patient communities for rare diseases",
    # ── Coding & QDA tool projects ──
    "Thematic analysis codebook for patient narratives about chronic pain",
    "NVivo project with coded interview data on workplace discrimination",
    "Grounded theory analysis of nursing care practices in palliative settings",
    "ATLAS.ti project file with coded focus group data on housing policy",
    "Qualitative codebook for analysing political discourse in social media",
    "MAXQDA project with coded fieldwork observations on refugee integration",
    "Coding framework for content analysis of news coverage on climate change",
    "Deductive coding scheme for analysis of policy documents on education reform",
    "Inductive coding of patient diary entries about living with diabetes",
    "NVivo coded dataset of parliamentary committee hearing transcripts",
    "ATLAS.ti hermeneutic unit with memo network on teacher identity",
    "MAXQDA coded project on community responses to environmental hazards",
    "Dedoose project with mixed methods coding of survey and interview data",
    "HyperRESEARCH project with coded narratives of immigrant integration",
    "Qualitative coding output from REFI-QDA exchange format project",
    "Codebook and coded transcripts for constructivist grounded theory study",
    "NVivo classification sheets and node hierarchy for education research",
    "ATLAS.ti network views and code co-occurrence analysis of health data",
    "Inter-coder reliability data and reconciled coding for discourse analysis",
    "MAXQDA teamwork project on organisational change management study",
    # ── Analysis methods ──
    "Phenomenological study of first-generation college students experiences",
    "Discourse analysis of parliamentary debates on immigration policy",
    "Conversation analysis transcripts of doctor-patient interactions",
    "Oral history interviews with civil rights movement participants",
    "Narrative inquiry into the experiences of LGBTQ youth in foster care",
    "Framework analysis of health policy implementation in low-income settings",
    "Interpretive phenomenological analysis of grief and bereavement",
    "Critical discourse analysis of media representations of poverty",
    "Content analysis of newspaper editorials on gun control legislation",
    "Semiotic analysis of advertising images and consumer identity",
    "Situational analysis mapping of reproductive justice advocacy",
    "Constructivist grounded theory of resilience in childhood adversity",
    "Thematic network analysis of stakeholder views on renewable energy",
    "Template analysis of employee perceptions of workplace flexibility",
    "Reflexive thematic analysis of doctoral student supervision experiences",
    "Multimodal discourse analysis of political campaign materials",
    "Qualitative comparative analysis of welfare state policy configurations",
    "Narrative thematic analysis of illness trajectories in chronic disease",
    # ── Mixed methods & surveys ──
    "Qualitative survey responses about teacher experiences during COVID-19",
    "Mixed methods study combining interview and survey data on housing insecurity",
    "Open-ended questionnaire responses on employee wellbeing",
    "Qualitative data from a convergent mixed methods study on substance use",
    "Sequential explanatory mixed methods data on health literacy",
    "Embedded mixed methods qualitative strand on parental engagement",
    "Open-ended survey responses about citizen satisfaction with public services",
    "Qualitative follow-up interviews from a randomised controlled trial",
    "Mixed methods evaluation data for a school mentoring programme",
    "Exploratory mixed methods data on digital divide in rural education",
    # ── Data collection artefacts ──
    "Photovoice project with homeless youth documenting daily challenges",
    "Visual methods data from a photo-elicitation study with migrant workers",
    "Diary study data on daily coping strategies of informal caregivers",
    "Transcript data from think-aloud protocol study on reading comprehension",
    "Audio recordings and transcripts of community town hall meetings",
    "Video recordings of therapy sessions for interaction analysis",
    "Research journal and reflexive notes from an ethnographic study of policing",
    "Participant-generated drawings from a study on childhood illness experience",
    "Body mapping data from a study with HIV-positive women in South Africa",
    "Digital storytelling artefacts from a youth empowerment project",
    "Observation schedules and field notes from playground interaction study",
    "Document collection and analysis of organisational policy texts",
    "Social media data collected for qualitative analysis of activism",
    "WhatsApp message transcripts from a health communication study",
    # ── Diverse academic disciplines ──
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
    "Theology qualitative study on religious conversion experiences",
    "Architecture research interviews on participatory urban design",
    "Library science interview data on information-seeking behaviour",
    "Sports science qualitative data on athlete retirement transitions",
    "Music education interview transcripts on culturally responsive pedagogy",
    "Tourism studies qualitative data on host community perceptions",
    "Development studies interview data on microfinance and women empowerment",
    "Environmental studies focus group data on community conservation attitudes",
    "Peace and conflict studies interview transcripts with ex-combatants",
    "Linguistics qualitative data on language attitudes and identity",
    "Media studies interview transcripts on journalism ethics and practice",
    "Agricultural extension qualitative data on farmer knowledge sharing",
    "Gerontology interview data on aging and technology adoption",
    "Urban planning qualitative study on gentrification and displacement",
    "Occupational therapy qualitative data on return-to-work experiences",
    # ── Qualitative research actions & processes ──
    "Qualitative exploration of barriers to mental health help-seeking",
    "Case study of community resilience after natural disasters",
    "Action research on participatory design with persons with disabilities",
    "Qualitative study of professional identity formation in medical education",
    "Qualitative research on cultural competency in healthcare delivery",
    "Interview study of decision-making processes in juvenile justice",
    "Qualitative dataset on community perspectives about water management",
    "Longitudinal qualitative study of recovery from substance addiction",
    "Qualitative content analysis of online support forum discussions",
    "Observational notes from classroom interaction analysis",
    "Qualitative evaluation of a community health intervention programme",
    "Biographical narrative interviews with Holocaust survivors",
    "Hermeneutic analysis of religious texts and practitioner interviews",
    "Participatory action research with Indigenous land rights activists",
    "Community-based participatory research on environmental justice",
    "Collaborative autoethnography of women academics navigating motherhood",
    "Qualitative process evaluation of a public health intervention",
    "Realist evaluation interview data on housing first programme mechanisms",
    "Qualitative systematic review data extraction and synthesis tables",
    "Rapid qualitative assessment of emergency response coordination",
    # ── Short/minimal descriptions (robustness against sparse metadata) ──
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
    "Coded transcripts thematic analysis",
    "Qualitative research dataset",
    "Interview recordings and transcriptions",
    "Fieldwork observations and reflections",
    "Qualitative data collection instruments and transcripts",
    "Participant narratives and themes",
    "Research memos and coded data",
    "Stakeholder interview summaries",
    "Qualitative findings and supporting data",
    "Coded qualitative dataset with codebook",
]

NEGATIVE_REFERENCES: list[str] = [
    # ── Physics & Astrophysics ──
    "Molecular dynamics simulation of protein folding in aqueous solutions",
    "Particle accelerator collision event data from the LHC",
    "Numerical simulation output for galaxy formation and evolution",
    "Astronomical photometry of variable stars in globular clusters",
    "Spectroscopic analysis of stellar atmospheres in binary systems",
    "Quantum computing benchmark results for variational algorithms",
    "Gravitational wave strain data from LIGO detector observations",
    "Neutron scattering measurements of magnetic material phase transitions",
    "Lattice QCD calculations of hadron mass spectrum",
    "Plasma physics diagnostic data from tokamak fusion experiments",
    # ── Chemistry & Materials Science ──
    "Crystal structure measurements of novel organic semiconductor compounds",
    "X-ray diffraction patterns of metallic alloy nanoparticles",
    "Chemical synthesis data for novel pharmaceutical drug candidates",
    "Protein crystallography data for enzyme active site characterisation",
    "Raman spectroscopy measurements of carbon nanotube samples",
    "Thermogravimetric analysis data for polymer degradation kinetics",
    "Electrochemistry cyclic voltammetry data for battery electrode materials",
    "Nuclear magnetic resonance spectra of synthesised organic compounds",
    "Density functional theory calculations of electronic band structures",
    # ── Biology & Genomics ──
    "Genome-wide association study of cardiovascular disease risk factors",
    "Phylogenetic analysis of mitochondrial DNA in avian species",
    "DNA sequencing reads from whole-genome shotgun metagenomics",
    "RNA sequencing expression profiles across mouse brain regions",
    "High-throughput screening results for antimicrobial compound activity",
    "Bioinformatics pipeline results for variant calling analysis",
    "Mass spectrometry data from metabolomics profiling of plasma samples",
    "Flow cytometry data for immune cell phenotyping in clinical samples",
    "Cryo-electron microscopy reconstructions of viral capsid structures",
    "Single-cell RNA sequencing atlas of human liver cell populations",
    "CRISPR gene editing efficiency data across multiple cell lines",
    "Microbiome 16S rRNA amplicon sequencing of gut bacteria diversity",
    "ChIP-seq peak calling results for histone modification mapping",
    # ── Earth & Environmental Sciences ──
    "Remote sensing satellite imagery of Amazon deforestation patterns",
    "Climate model output for global temperature projections under RCP scenarios",
    "Seismic wave propagation data from earthquake monitoring stations",
    "Atmospheric chemistry measurements of ozone depletion indicators",
    "Geospatial analysis of land use change using LIDAR point clouds",
    "Geological survey data of mineral compositions in sedimentary rocks",
    "Sensor network measurements of soil moisture and temperature",
    "Ocean buoy time series data of sea surface temperature and salinity",
    "Ice core isotope ratio measurements for paleoclimate reconstruction",
    "Radar altimetry data for glacier ice sheet thickness monitoring",
    "Volcanic ash dispersal modelling output for eruption scenarios",
    # ── Engineering & Computing ──
    "Finite element analysis of structural stress in bridge components",
    "Neural network training data for image classification tasks",
    "Fluid dynamics simulation of turbulent flow in pipe networks",
    "Computational fluid dynamics results for aerodynamic optimization",
    "Machine learning model predictions for material properties",
    "Embedded systems sensor telemetry from autonomous vehicle testing",
    "Power grid load forecasting time series from smart meter networks",
    "3D printing layer thickness optimisation experimental measurements",
    "Robotic arm trajectory planning simulation data",
    "Network traffic packet capture data for intrusion detection analysis",
    # ── Medical imaging & clinical data (quantitative) ──
    "Electron microscopy images of neural tissue cross-sections",
    "Magnetic resonance imaging brain scan volumetric measurements",
    "Computed tomography angiography data for coronary artery analysis",
    "Positron emission tomography radiotracer uptake quantification data",
    "Retinal fundus photography dataset for diabetic retinopathy screening",
    # ── Mathematics & Statistics ──
    "Monte Carlo simulation results for stochastic process modelling",
    "Bayesian inference posterior samples from hierarchical regression models",
    "Graph theory benchmark datasets for community detection algorithms",
    "Numerical optimisation convergence results for large-scale problems",
    "Time series forecasting model comparison on economic indicators",
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
