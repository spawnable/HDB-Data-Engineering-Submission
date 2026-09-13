# Assumptions, rules and observed findings

## 1. Source scope and provenance

The five user-supplied CSVs are the sole inputs. The pipeline scans filenames with `Path.glob`, reads each complete file in chunks and preserves every source attribute in a union schema. The originals are kept unchanged in `data/raw/`; the source inventory records filename, bytes, SHA-256, schema, row counts and month coverage.

| Supplied period | Raw rows | Requested-period rows |
|---|---:|---:|
| 1990-1999 | 287,196 | 0 |
| 2000-February 2012 | 369,651 | 3,188 |
| March 2012-December 2014 | 52,203 | 52,203 |
| January 2015-December 2016 | 37,153 | 37,153 |
| January 2017 onwards | 240,073 | 0 |
| **Total** | **986,276** | **92,544** |

The two files wholly outside scope still contribute to schema discovery and the inventory. No values from 2017 onward are used for the January reference, price heuristic, deduplication or group means. The scoped master retains all 11 source attributes; `remaining_lease` is null where absent from the older schemas.

Well-formed dates outside 2012-01 through 2016-12 are scope exclusions. Malformed dates cannot be assigned safely and are retained for explicit quarantine. No malformed date is present in these inputs. The master covers all 60 requested months.

The early snapshot is labelled by approval date and the later snapshots by registration date. `_date_basis` retains this distinction. A discontinuity around March 2012 must not automatically be interpreted as a market change. No deduplication across a different month is attempted to infer a common underlying transaction.

## 2. January 2012 is a frozen reference

The reference is derived programmatically from 1,559 January 2012 records. Text normalization applies Unicode NFKC, trimming, collapsed whitespace and uppercase. It corrects presentation, not meaning: no manually invented spelling aliases or storey conversions are applied.

| Attribute | Validation rule | January distinct values |
|---|---|---:|
| Date / `month` | Exact `YYYY-MM`, parseable calendar month, requested time window | Format learned/confirmed; values are not restricted to January |
| `town` | Membership in normalized January values | 26 |
| `flat_type` | Membership in normalized January values | 7 |
| `flat_model` | Membership in normalized January values | 13 |
| `storey_range` | Membership plus two-digit lower/upper syntax, positive lower bound and lower <= upper | 12 |

The generated list of allowed values is in `data/outputs/reports/reference_domains.json`. This interpretation allows the remaining 59 months while retaining the specified categorical authority. Treating `2012-01` itself as the only allowed date would contradict the five-year scope.

**Observed consequence:** 6,975 records have a storey band absent from January; 492 have a flat model absent from January; 56 fail both. Therefore 7,411 distinct rows fail validation. No town or flat-type membership failures occur.

All 6,838 records from **March, April and May 2012** are quarantined because the data use broader bands such as `01 TO 05` and `06 TO 10`. Later high-floor bands also fall outside the frozen reference. Newer models include DBSS, TYPE S1 and TYPE S2. These may be legitimate records. The strict policy is implemented as requested, but creates selection bias and leaves the cleaned dataset with only 57 represented months. The notebook and monthly quality report expose this rather than pretending those months had no transactions.

In a production change, a steward should approve and version an expanded reference. A five-floor band cannot be converted exactly to a three-floor band without the actual storey. That expansion is deliberately not applied to this submission.

## 3. Remaining lease is measured at the transaction

Assume a 99-year lease commencing on **1 January** of the given commencement year, and assess it at the start of the recorded transaction month. The source lacks exact commencement month/day, so this is an estimate, not legal expiry information. Using today's date would make historical results drift on every rerun and is not chosen.

For commencement year `L`, transaction year `Y`, and month `M`:

```text
remaining_total_months = 12 * (L + 99 - Y) - (M - 1)
remaining_years        = remaining_total_months // 12
remaining_months       = remaining_total_months % 12
```

The arithmetic is in complete months; floor division rounds the years down. Example: commencement 1980 and transaction 2015-02 gives 767 months, or **63 years 11 months**. Values <= 0 or > 1,188 months, future commencements and invalid commencement years are rejected.

The original `remaining_lease` is retained, including its nulls. Where supplied, it is parsed separately for comparison, accepting either integer years or a years/months string. Unknown commencement month can explain differences approaching a year, and integer-only source values are coarse. Differences over 12 months produce an audit warning without silently rewriting the source.

One cleaned row needs review: **2016-02, PASIR RIS, block 513, 5 ROOM**, commencement 1993. The source says 77 years; the stated assumption yields 75 years 11 months, a 13-month difference. This is a review observation, not a conclusion about which value is correct.

## 4. Validation, duplicates and precedence

Required source attributes must be present and nonempty. Floor area must be numeric, finite and positive. Price must be positive, finite and representable in whole cents; invalid monetary strings are rejected. Blocks must contain at least one digit. Optional supplied lease values must have a valid representation and fall within a 99-year term. Missing required columns or duplicate CSV headers stop the run instead of generating a misleading output.

The order is:

1. Normalize and validate; retain all reasons for a rejected row.
2. On valid rows, form the composite key from **all union-schema source columns except `resale_price`**.
3. Keep the highest price in each group. Equal-price ties resolve by filename then data-row number, independently of processing/chunk order.
4. Evaluate price anomalies on the retained references. If the reference is flagged, retain it in Quarantined; do not promote a lower-price duplicate as a replacement.
5. Calculate identifiers from the final cleaned data.

The key includes `remaining_lease` because it is a source attribute. It excludes all lineage, price scores, lease estimates, identifiers and hashes. Numeric representations such as 60 and 60.0 compare equally after typed normalization. The key is a JSON sequence of named attributes, with explicit nulls; it is not ambiguous delimiter concatenation.

There are **1,383** suppressed duplicate records. Each records the selected reference's source-record ID, including when that reference subsequently becomes an anomaly candidate. Raw values are retained in Quarantined.

The prescribed key is not a verified real-world transaction ID: the source has no unit number or unique transaction ID. Distinct real sales with identical published attributes may be merged. Selecting the maximum price also biases the retained price distribution upward. The implementation follows the requested policy and documents these limitations.

## 5. Price anomaly heuristic

Use the log of price per square metre to account for size and dampen price skew. On deduplicated, valid rows, compute the 25th and 75th percentiles and apply conservative outer Tukey fences:

```text
x = log(resale_price / floor_area_sqm)
IQR = Q3 - Q1
flag if x < Q1 - 3*IQR or x > Q3 + 3*IQR
```

Choose the first eligible peer group in this hierarchy: town + flat type + year; town + flat type over the full requested period; flat type + year. A group needs at least 20 records and nonzero spread. A row without eligible peers is marked **unassessed**, not certified non-anomalous. Bounds, peer level and peer count are output with the records.

The heuristic flags **138** potential anomalies; **21** retained rows are unassessed. Among retained rows, 82,581 use town/type/year, 918 use town/type over all years, and 92 use type/year. Flags include Model A and Terrace units; high price or low price per square metre may reflect genuine property characteristics. The criterion is transparent but has no labelled ground truth here, and no precision/recall claim is made. It is retrospective, may use peers from later in the same year, and is not a point-in-time valuation model.

Candidates are quarantined because the output contract explicitly includes potentially anomalous data. They are not deleted, winsorized or called fraudulent. A production review/release workflow should preserve the decision, reviewer and rule version.

## 6. Identifier construction and the uniqueness conflict

The transformation bullets are interpreted as defining the readable identifier, followed by its cryptographic hash. The repeated duplicate instruction is implemented once upstream; deduplication is not run on the short identifier.

`Resale Identifier` has nine characters:

| Component | Rule | Example |
|---|---|---|
| Prefix | Literal `S` | S |
| Block | Remove non-digits; left-pad to three digits; retain first three | `A19B` -> 019; `1234A` -> 123 |
| Price | First two digits of the integer part of the cleaned month/town/flat-type mean | 230000 -> 23 |
| Month | Two-digit transaction month | 2012-01 -> 01 |
| Town | First character of normalized town | ANG MO KIO -> A |
| **Combined** | Concatenate | **S0192301A** |

The mean uses exact integer cents for aggregation and prefix extraction, avoiding floating-point rounding into the next dollar. Fractional dollars are discarded only for the two leading digits. For a hypothetical mean below 10 dollars, the integer part is zero-padded to two digits. No such low-price case occurs here.

The required format omits the year and many key attributes, and discards information from block and town. It therefore cannot guarantee uniqueness. The actual data contain **71,678 distinct codes, 9,700 repeated-code groups, 21,634 rows in those groups and 11,934 excess repeated occurrences**.

- `resale_identifier_hash = SHA256(UTF8(Resale Identifier))` is the literal requested digest. Repeated codes correctly yield repeated digests.
- `resale_record_hash = SHA256(UTF8(canonical composite key))` is the separate, unique record identity: **83,612 distinct hashes** for 83,612 rows. It is independent of price and group-mean changes when key attributes do not change.

This is an explicit resolution of conflicting requirements, not a claim that hashing repairs non-unique input. No extra suffix is inserted into the specified nine-character field. The pipeline checks for distinct canonical keys mapping to the same SHA-256 value and fails if detected; a finite hash has no absolute mathematical collision guarantee. Unkeyed hashes of public, low-entropy attributes are also not an anonymization guarantee.

## 7. Profiling and reproducibility evidence

The saved profiles report types, missingness, distinct counts, frequent values and numeric descriptive statistics for the master, cleaned and quarantined frames. Schema-level absence of `remaining_lease` is kept visible. Quarantine reason totals can exceed rejected-row totals because all applicable validation reasons are retained.

After the specified rules, the cleaned resale-price median is **S$425,000**, mean **S$448,825.55**, and range **S$190,000 to S$1,150,000**. These describe the selected output, not an unbiased Singapore resale population.

All raw checksums match before and after execution. Every master row belongs to exactly one of Cleaned and Quarantined. Transformed and Hashed align one-to-one with Cleaned. The test suite covers 38 cases, including invalid dates/prices, future/expired leases, unknown domains, deterministic maximum-price ties, ambiguous concatenation, zero-spread peers, code collisions, schema union and idempotence across reader chunk sizes.

All nine notebook code cells executed in a fresh IPython process, and the notebook preserves real outputs. Jupyter kernel socket execution was blocked by the build host; the conventional kernel runner is provided for an unrestricted workstation. AWS resources and Tableau connectivity were not deployed or tested because Part 2 is a design exercise.
