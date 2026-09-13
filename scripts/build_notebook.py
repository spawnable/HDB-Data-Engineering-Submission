"""Build the reviewer notebook with nbformat, then execute it with execute_notebook.py."""
from pathlib import Path
import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
md = nbf.v4.new_markdown_cell
code = nbf.v4.new_code_cell
cells = [
    md("""# HDB resale data engineering assessment

## Goal
Prepare the supplied resale snapshots for January 2012 to December 2016, with a
traceable decision for every row. The Python package performs the ETL; this
notebook runs it and presents the evidence. **No data are fetched from a URL.**

The submitted execution produces **83,612 cleaned records** and **8,932
quarantined records** from **92,544** scoped rows. The original five CSV files
contain **986,276** rows; other periods remain unchanged in Raw.

The two AWS architecture designs are in `architecture/`; detailed decisions and
sources are in `docs/architecture.md`. The assessment brief itself is excluded.
"""),
    md("""## Setup
From the repository root, install the pinned environment with
`python -m pip install -r requirements-lock.txt` followed by
`python -m pip install -e . --no-deps`.

The notebook can be executed with `python scripts/execute_notebook.py`.
It does not download data, install packages from a cell, or require AWS credentials.
"""),
    code("""from pathlib import Path
import importlib.metadata
import json
import sys
import pandas as pd
from IPython.display import display

# Resolve the project from this notebook's working directory.
ROOT = Path.cwd()
if not (ROOT / 'pyproject.toml').exists():
    raise RuntimeError('Start the notebook from the repository root')
sys.path.insert(0, str(ROOT / 'src'))
from hdb_resale.pipeline import Config, run_pipeline, sha256_file

INPUT_DIR = ROOT / 'data' / 'raw'
OUTPUT_DIR = ROOT / 'data' / 'outputs'
CONFIG = Config(start_month='2012-01', end_month='2016-12', reference_month='2012-01')
display(pd.DataFrame({'package': ['pandas', 'numpy', 'pyarrow'],
                      'version': [importlib.metadata.version(p) for p in ['pandas', 'numpy', 'pyarrow']]}))
"""),
    md("""## Key assumptions

- Date inherits the January 2012 **format** (`YYYY-MM`), while its values may span
  the requested 60 months. Town, flat type, flat model and storey range must belong
  to the normalized January 2012 domains. Unknown categories are quarantined.
- Unicode, case and whitespace normalization correct representation only. Five-storey
  bands are not converted into three-storey bands because the exact floor is unknown.
- Remaining lease is measured **at the transaction month**, assuming a 99-year
  term starting on 1 January of the supplied commencement year. Months use floor
  division; the source `remaining_lease` is preserved separately. This is an estimate.
- The composite key contains every source attribute except `resale_price`, including
  the union-schema `remaining_lease`. Lineage and derived attributes are excluded.
- Validation runs before maximum-price deduplication; potential anomalies are then
  quarantined without promoting a lower-price duplicate. The group mean for the
  identifier is calculated on the final cleaned records.
- A nine-character business code is not a unique transaction key. The literal
  SHA-256 code hash is supplied along with a unique SHA-256 composite-key hash.
"""),
    md("""## Steps
### 1. Execute the pipeline
Read the CSVs as-is in chunks; union their attributes; retain the requested period;
profile and validate; select the maximum-price record per key; flag price anomalies;
generate identifiers and hashes; write the four derived output groups and reports.

Raw is `data/raw/`. Well-formed out-of-period rows are scope exclusions, not bad
records. A malformed date would be routed into the master for quarantine.
"""),
    code("""# Record raw checksums before execution; the pipeline never writes to INPUT_DIR.
raw_before = {p.name: sha256_file(p) for p in sorted(INPUT_DIR.glob('*.csv'))}
summary = run_pipeline(INPUT_DIR, OUTPUT_DIR, CONFIG)
counts = summary['counts']
display(pd.DataFrame(counts.items(), columns=['metric', 'rows_or_count']))
"""),
    md("""### 2. Inspect source coverage and the union schema
All five inputs are retained. The two files outside the requested period are still
scanned for their schemas and included in the raw inventory. An absent attribute
becomes null in the combined master; no source column is discarded.
"""),
    code("""inventory = json.loads((OUTPUT_DIR / 'reports/source_inventory.json').read_text())
display(pd.DataFrame(inventory)[['file', 'rows', 'in_scope_rows', 'out_of_scope_rows', 'min_month', 'max_month']])
print('Union attributes:', ', '.join(summary['source_columns']))
print('Composite-key attributes:', ', '.join(summary['composite_key_columns']))
"""),
    md("""### 3. Review the authoritative domains and profiling
The reference is generated from the 1,559 January 2012 records. Later additions are
not silently folded into the reference. The complete profiles include missingness,
distinct values, top values and numeric distributions, before and after cleaning.
"""),
    code("""reference = json.loads((OUTPUT_DIR / 'reports/reference_domains.json').read_text())
display(pd.DataFrame([{'attribute': k, 'distinct_reference_values': len(v), 'allowed_values': ', '.join(v)}
                      for k, v in reference['domains'].items()]))
profiles = json.loads((OUTPUT_DIR / 'reports/data_profiles.json').read_text())
display(pd.DataFrame(profiles['master']['columns'])[['column', 'missing', 'missing_pct', 'distinct_non_null']])
"""),
    md("""### 4. Review quarantined records
Rows have one terminal quarantine stage and can have multiple validation reasons.
The 492 flat-model failures and 6,975 storey-range failures overlap, so their sum
does not equal the 7,411 validation-rejected rows. These can be legitimate source
changes; they are reference-policy failures, not evidence of corrupt transactions.
"""),
    code("""cleaned = pd.read_parquet(OUTPUT_DIR / 'cleaned/cleaned.parquet')
transformed = pd.read_parquet(OUTPUT_DIR / 'transformed/transformed.parquet')
quarantined = pd.read_parquet(OUTPUT_DIR / 'quarantined/quarantined.parquet')
hashed = pd.read_parquet(OUTPUT_DIR / 'hashed/hashed.parquet')
display(pd.read_csv(OUTPUT_DIR / 'reports/quarantine_by_reason.csv'))
display(pd.read_csv(OUTPUT_DIR / 'reports/unseen_categories.csv'))
display(quarantined[['month', 'town', 'flat_type', 'resale_price', '_quarantine_stage', '_quarantine_reasons']].head(8))
"""),
    md(r"""### 5. Check the remaining lease calculation
For a transaction in February 2015 with a commencement year of 1980:

$$R_m = 12(1980 + 99 - 2015) - (2-1) = 767\text{ months}$$
$$R_y=\lfloor 767/12\rfloor=63,\qquad R_{m,\mathrm{remainder}}=767\bmod12=11.$$

The result is 63 years 11 months. The start-day/month is not supplied, so exact legal
lease expiry cannot be inferred. Supplied remaining-lease values are retained; a
difference beyond 12 months is a review warning, not an automatic correction.
"""),
    code("""lease_columns = ['month', 'town', 'block', 'lease_commence_date', 'remaining_lease',
                 'remaining_lease_years_months', '_provided_lease_difference_months']
display(cleaned.loc[cleaned.month.ge('2015-01'), lease_columns].head(6))
display(cleaned.loc[cleaned._provided_lease_difference_months.abs().gt(12), lease_columns])
"""),
    md(r"""### 6. Review price anomaly candidates
For each deduplicated valid row, score $x=\log(\mathrm{resale\ price}/\mathrm{floor\ area})$.
Use outer Tukey fences $[Q_1-3\,IQR,\;Q_3+3\,IQR]$ within town / flat type / year,
requiring at least 20 peers and nonzero spread. Fall back to town / flat type over
the full period, then flat type / year. If no peer group qualifies, mark the row
unassessed instead of fabricating a score.

138 records are flagged for review, including legitimate premium or unusual units.
21 retained records have no usable peer group. This retrospective heuristic does
not constitute a predictive model, a fraud label or a validated valuation.
"""),
    code("""anomalies = quarantined.loc[quarantined._quarantine_stage.eq('anomaly')]
display(anomalies[['month', 'town', 'flat_type', 'resale_price', 'floor_area_sqm', 'price_per_sqm',
                   '_anomaly_peer_level', '_anomaly_peer_count', '_anomaly_lower_ppsqm', '_anomaly_upper_ppsqm']].head(8))
display(cleaned._anomaly_peer_level.value_counts().rename_axis('peer_level').to_frame('rows'))
"""),
    md("""### 7. Inspect identifiers and hashes
The format is `S` + three block digits + two leading group-average-price digits +
two month digits + town initial. For block 19, group mean $230,000, January and
Ang Mo Kio, the result is `S0192301A`.

The cleaned data contain 71,678 distinct codes across 83,612 rows. A SHA-256 hash
of an identical code is identical, so a hash alone cannot repair this ambiguity.
`resale_identifier_hash` follows the literal hashing instruction; `resale_record_hash`
hashes the unambiguous canonical composite key and is unique in this output.
The pipeline checks for a hash collision and stops if one is detected.
"""),
    code("""display(transformed[['month', 'town', 'flat_type', 'block', 'resale_price',
                     'group_average_resale_price', 'Resale Identifier']].head(8))
display(hashed[['month', 'town', 'block', 'resale_identifier_hash', 'resale_record_hash']].head(3))
collisions = transformed.loc[transformed['Resale Identifier'].duplicated(keep=False)]
example_code = collisions['Resale Identifier'].iloc[0]
display(collisions.loc[collisions['Resale Identifier'].eq(example_code),
                       ['month', 'town', 'block', 'street_name', 'storey_range', 'floor_area_sqm', 'Resale Identifier']])
"""),
    md("""## Checks
Validate row conservation, unchanged raw bytes, complete schema, date coverage,
lease arithmetic, reference adherence, output alignment and unique record hashes.
The separate pytest suite covers malformed values, deduplication ties, identifier
collisions, price outliers, missing inputs and a full synthetic offline rerun.
"""),
    code("""raw_after = {p.name: sha256_file(p) for p in sorted(INPUT_DIR.glob('*.csv'))}
assert raw_before == raw_after
assert counts['raw_rows'] == counts['out_of_scope_rows'] + counts['master_rows']
assert len(cleaned) + len(quarantined) == counts['master_rows']
assert set(cleaned._source_record_id).isdisjoint(quarantined._source_record_id)
assert len(cleaned) == len(transformed) == len(hashed)
assert set(summary['source_columns']).issubset(cleaned.columns)
assert cleaned.month.between(CONFIG.start_month, CONFIG.end_month).all()
assert (cleaned.remaining_lease_years * 12 + cleaned.remaining_lease_months).equals(cleaned.remaining_lease_total_months)
assert cleaned.remaining_lease_months.between(0, 11).all()
for attribute, allowed in reference['domains'].items():
    assert cleaned[attribute].isin(allowed).all()
assert hashed.resale_record_hash.is_unique
assert transformed['Resale Identifier'].str.fullmatch(r'S[0-9]{7}[A-Z]').all()
assert (OUTPUT_DIR / '_SUCCESS.json').exists()
print('All notebook checks passed. Raw files unchanged; every scoped row accounted for.')
"""),
    md("""## Takeaways
- The strict January reference removes genuine later categories; do not use the
  cleaned data as an unbiased population sample. Version an approved reference
  expansion in production instead of changing labels silently.
- The specified duplicate key omits a transaction ID or unit number. It can merge
  real, separate sales. The maximum-price rule is followed exactly for valid rows,
  and all suppressed records remain available for audit.
- The short identifier has observed collisions. Use `resale_record_hash` for joins;
  the business code and its literal hash are descriptive fields.
- Lease expiry is estimated from coarse inputs. One supplied value differs from
  the estimate by 13 months and is explicitly marked for review.
- AWS deployment is an architecture deliverable only. Private Athena connectivity,
  IAM, driver compatibility and performance must be validated in the target account.

## Next steps
Read `README.md`, `docs/assumptions_and_insights.md` and `docs/architecture.md`.
The PNG diagrams and their SVG sources are included in `architecture/`.
"""),
]

notebook = nbf.v4.new_notebook(cells=cells, metadata={
    'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'},
    'language_info': {'name': 'python', 'version': '3.12'},
})
nbf.validate(notebook)
nbf.write(notebook, ROOT / 'hdb_resale_assessment.ipynb')
print('Built hdb_resale_assessment.ipynb')
