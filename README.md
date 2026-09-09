# HDB resale data engineering assessment

A reproducible Python ETL pipeline, an executed Jupyter notebook and two AWS architecture designs. **The only data inputs are the five supplied CSV files. No dataset is downloaded at runtime.**

## Start here

1. Open [the executed notebook](hdb_resale_assessment.ipynb) for the walkthrough and cell outputs.
2. Read [assumptions and observed findings](docs/assumptions_and_insights.md), especially the frozen January reference and non-unique business identifier.
3. Review [batch ingestion](architecture/01_batch_ingestion.png) and [private analytics](architecture/02_private_analytics.png), with the [architecture rationale](docs/architecture.md).
4. Use the [requirements matrix](docs/requirements_matrix.md) to locate each deliverable.

## Results from the supplied snapshots

| Stage | Rows |
|---|---:|
| Five original CSVs | 986,276 |
| Outside January 2012 to December 2016 | 893,732 |
| Scoped master, retaining the union of 11 source attributes | 92,544 |
| January 2012 reference | 1,559 |
| Quarantined: validation | 7,411 |
| Quarantined: duplicate suppression | 1,383 |
| Quarantined: potential price anomaly | 138 |
| **Total quarantined** | **8,932** |
| **Cleaned / Transformed / Hashed, each** | **83,612** |

The 92,544 master rows reconcile exactly to 83,612 cleaned plus 8,932 quarantined. Out-of-period rows remain in Raw and are recorded in the source inventory; they are not classified as bad data.

**Material findings:** the strict January storey vocabulary excludes all March-May 2012 rows. The required nine-character identifier produces 71,678 distinct values across 83,612 cleaned records. Its literal hash is supplied, alongside a separate unique composite-key hash. These limitations are documented rather than hidden by changing the rules.

## Run it

Tested with CPython 3.12 on Linux. Allow about 1 GB of working disk space and 2 GB of memory for a comfortable rerun of these files. Chunked ingestion limits raw-read memory; deduplication and peer statistics operate on the scoped data in memory.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-lock.txt
python -m pip install -e . --no-deps

python -m hdb_resale.pipeline
python -m pytest -q
python scripts/execute_notebook.py
python scripts/verify_submission.py
```

On Windows, use `.venv\Scripts\activate` instead of the `source` command. The exact dependency versions used for this submission are in `requirements-lock.txt`; `pyproject.toml` contains the supported ranges. Rendering the diagrams also needs the Cairo shared library used by CairoSVG. The already-rendered SVG/PNG files are included, so rendering is optional when reviewing the ETL.

The build host prohibits local socket creation. Consequently, the submitted notebook was executed top-to-bottom in a **fresh IPython process**, with genuine rich displays and printed outputs captured into the notebook. All nine code cells and their assertions passed. To reproduce that execution method:

```bash
python scripts/execute_notebook.py --in-process
```

The conventional `nbclient` kernel execution route is provided, but was not validated on this restricted host. The fallback is suitable for the Python-only cells here; it is not a general replacement for interactive widget protocols.

To change the date window or input folder:

```bash
python -m hdb_resale.pipeline --input-dir data/raw --output-dir data/outputs \
  --start-month 2012-01 --end-month 2016-12 --reference-month 2012-01
```

Run a single writer at a time. The pipeline stages outputs, checks row conservation, verifies gzip integrity and copied-file checksums, then writes `_SUCCESS.json` last. This prevents a failed calculation from being labelled successful; it is not an atomic transaction spanning all output files. Concurrent production publishing needs the versioned manifest pattern in the architecture notes.

## Files and output groups

| Location | Purpose |
|---|---|
| `data/raw/*.csv` | All five supplied input files, with unchanged names and bytes |
| `data/outputs/master/master.parquet` | Scoped master with every source attribute and row lineage |
| `data/outputs/cleaned/` | Valid, deduplicated, non-flagged rows with lease and audit fields |
| `data/outputs/transformed/` | Cleaned rows plus group average and `Resale Identifier` |
| `data/outputs/quarantined/` | Rejected rows, original source attributes, reasons and supporting evidence |
| `data/outputs/hashed/` | Cleaned rows plus literal identifier hash and unique composite-key hash |
| `data/outputs/reports/` | Profiles, source inventory, reference domains, reason counts and monthly quality |
| `hdb_resale_assessment.ipynb` | Executed reviewer notebook |
| `src/hdb_resale/pipeline.py` | Shared ETL implementation used by the notebook, CLI and tests |
| `tests/test_pipeline.py` | 38 tests, including parameterized malformed inputs and an offline integration run |
| `architecture/` | Two submission PNGs, editable SVGs and official AWS icon assets |
| `docs/` | Assumptions, data dictionary, architecture and requirements mapping |
| `scripts/` | Notebook execution, diagram rendering and submission verification |

Each derived group contains both Snappy Parquet and a gzip-compressed CSV. Parquet preserves types; gzip CSV provides an interoperable equivalent. For example:

```python
import pandas as pd
cleaned = pd.read_parquet("data/outputs/cleaned/cleaned.parquet")
quarantined = pd.read_csv("data/outputs/quarantined/quarantined.csv.gz")
```

All configuration is explicit. There are no hardcoded domain lists, per-record fixes, source-row deletions, download URLs or AWS credentials in the ETL.

## Submission and publication

The assessment PDF is intentionally excluded. Input snapshots, notebook outputs and architecture PNGs are included. The local Git repository is prepared for publication; public upload requires a destination repository under the submitter's control. No public upload or AWS deployment is claimed here.

For a newly created, empty public GitHub repository, use GitHub CLI from a clone or extracted copy:

```bash
git init -b main
git add .
git commit -m "Complete HDB resale ETL and AWS architecture assessment"
gh repo create hdb-resale-assessment --public --source=. --remote=origin --push
```

If a Git repository already exists locally, skip initialization and commit only changes. If an `origin` already exists, verify its destination rather than replacing it. Open the public repository while signed out and confirm that the notebook outputs and both PNGs render. Do not add the assessment brief.

The CSVs are supplied HDB/data.gov.sg snapshots. AWS icons remain the property of Amazon Web Services; see [asset attribution](architecture/icons/NOTICE.md).
