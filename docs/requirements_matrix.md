# Requirements coverage

This matrix describes the implementation and design without reproducing the assessment brief.

| Requirement | Evidence / resolution |
|---|---|
| Python ETL; programmatic processing | `src/hdb_resale/pipeline.py`; notebook and CLI use the same functions |
| Use supplied data only | Five unchanged files in `data/raw/`; no runtime network client or downloader |
| 2012-01 through 2016-12 | Configured scope; 92,544-row master; all 60 months accounted for |
| Union of source attributes | `load_inputs`; all 11 attributes, including nullable `remaining_lease` |
| Data profiling | `data/outputs/reports/data_profiles.json`; notebook displays; before/after summaries |
| January reference validation | Generated `reference_domains.json`; strict date/domain rules; no hardcoded domain list |
| 99-year remaining lease in years/months | Derived total, quotient, remainder and display fields; documented Jan-1 assumption |
| Maximum-price reference per composite key | `add_keys` / `deduplicate`; deterministic ties and reference IDs in quarantine |
| Potential price anomalies | Log-price-per-sqm peer IQR heuristic, bounds/counts and 138 review candidates |
| Additional quality rules | Missing/type/finite/positive checks, lease bounds, block digits, schema integrity, row conservation |
| Markdown assumptions and insights | `docs/assumptions_and_insights.md` |
| Prescribed readable identifier | Exact `Resale Identifier` field in Transformed; mean based on final cleaned rows |
| Irreversible hashing and uniqueness | Literal SHA-256 code hash plus unique canonical-key hash; observed conflict explained rather than hidden |
| Raw | Five unchanged input CSV files |
| Cleaned | 83,612 rows; Parquet and gzip CSV |
| Transformed | 83,612 rows; Parquet and gzip CSV |
| Quarantined | 8,932 rows; Parquet and gzip CSV; original source attributes and evidence |
| Hashed | 83,612 rows; Parquet and gzip CSV; unique `resale_record_hash` |
| Notebook with cell outputs and guidance | `hdb_resale_assessment.ipynb`; nine executed Python cells |
| Software engineering and reviewability | Parameterized package, pinned environment, deterministic behavior, 38 tests, verifier, README |
| AWS public batch source; large files | Diagram 01 and architecture sections 1-2; bounded streaming, multipart, manifests and replay |
| Private platform segmentation | Separate ingestion/inspection/egress/isolated-processing zones and task roles |
| Tableau Athena integration | Diagram 02; compatible driver, private DNS, IAM role, workgroup, catalog and results |
| Private query/result traffic | Athena/Glue interface endpoints; separate S3 gateway per VPC; 443/444 coverage and S3 result fetch |
| Security/scalability/maintainability/performance | Architecture sections 2-5, with assumptions and proposed acceptance tests |
| AWS icons; PNG deliverable | Official AWS SVG assets embedded in two editable SVGs; two rendered PNGs |
| Same public Git repository | Repository content prepared; public upload requires a destination repository owned by the submitter |
| Exclude the assignment document | No assessment PDF included; `.gitignore` and verifier guard against inclusion |

## Validation status

The Python pipeline executed on the real supplied files, all 38 pytest cases passed, and all nine notebook code cells executed with outputs in a fresh IPython process. Raw-byte checks and actual-output contract checks are in `scripts/verify_submission.py` and its generated report. Conventional Jupyter socket execution was unavailable on the build host; it is documented, not claimed as tested. The AWS architecture was reviewed against primary documentation; no AWS deployment or Tableau connection was performed.
