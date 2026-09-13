# Data dictionary

The master keeps original string values plus lineage. Cleaned, Transformed and Hashed use normalized strings and typed numeric fields. Quarantined restores original source attributes and appends decision evidence.

## Source attributes retained in all output groups

| Field | Cleaned type | Meaning / rule |
|---|---|---|
| `month` | string | Recorded approval/registration month, `YYYY-MM` |
| `town` | string | Normalized January-reference town |
| `flat_type` | string | Normalized January-reference flat type |
| `block` | string | Block designation; alphanumeric content retained |
| `street_name` | string | Normalized street name |
| `storey_range` | string | Normalized, validated published range; not an exact floor |
| `floor_area_sqm` | float64 | Positive floor area, square metres |
| `flat_model` | string | Normalized January-reference flat model |
| `lease_commence_date` | int64 | Four-digit commencement year, not a full date |
| `remaining_lease` | nullable string | Original supplied lease text; null when absent from the source file |
| `resale_price` | float64 | Price in SGD; validated and processed as integer cents internally |

Any additional input attribute is preserved in the union and included in the composite key unless it is `resale_price`. Reserved derived names and source names beginning with `_` cause a schema error rather than overwriting data.

## Derived and identity attributes

| Field | Type | Meaning |
|---|---|---|
| `transaction_year`, `transaction_month` | int64 | Parsed components of `month` |
| `remaining_lease_total_months` | int64 | Complete months under the Jan-1 commencement assumption |
| `remaining_lease_years`, `remaining_lease_months` | int64 | Quotient and remainder of total months divided by 12 |
| `remaining_lease_years_months` | string | Human-readable lease estimate |
| `price_per_sqm` | float64 | SGD per square metre, used by the heuristic |
| `group_average_resale_price` | float64 | Final cleaned mean by month/town/flat type; Transformed only |
| `Resale Identifier` | string, length 9 | Prescribed business code; Transformed only; not unique |
| `resale_identifier_hash` | string, length 64 | SHA-256 of the exact business code; Hashed only; not unique |
| `resale_record_hash` | string, length 64 | SHA-256 of canonical source composite key; Hashed only; checked unique |

The assessment field `Resale Identifier` retains its requested display name. A production serving schema can deliberately alias it to `resale_identifier`; do this in a controlled publication adapter or view, not by accidentally dropping or reordering columns.

## Lineage and audit attributes

| Field | Meaning |
|---|---|
| `_source_file` | Original input filename |
| `_source_row` | One-based CSV data record number, excluding header |
| `_source_sha256` | Digest of unchanged file bytes |
| `_source_record_id` | Digest of filename, file digest and data-row number; lineage identity, not a real-world transaction ID |
| `_date_basis` | `approval` before March 2012, otherwise `registration` |
| `_provided_lease_months` | Source lease parsed to months; integer years are represented as years x 12 |
| `_provided_lease_difference_months` | Source months minus calculated total months |
| `_lease_comparison_note` | Absent source value, within coarse precision allowance, or review required |
| `_anomaly_peer_level` | Selected grouping or `unassessed` |
| `_anomaly_peer_count` | Number of records in the eligible group; includes the scored row |
| `_anomaly_lower_ppsqm`, `_anomaly_upper_ppsqm` | Back-transformed log-IQR fences in SGD/m2; null when unassessed |
| `_quarantine_stage` | One of `validation`, `duplicate`, `anomaly` |
| `_quarantine_reasons` | JSON array of all applicable reasons within the terminal stage |
| `_reference_source_record_id` | Maximum-price reference for a suppressed duplicate; can point to an anomaly row |

Fields belonging to later stages can be null on earlier-stage rejections. Internal canonical JSON and integer-cent work columns are omitted from published datasets to avoid unnecessary duplication.
