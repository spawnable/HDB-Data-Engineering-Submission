"""Auditable, deterministic, offline ETL for the supplied HDB resale CSV files.

Business rules are functions so the notebook, CLI and tests execute the same code.
Raw CSV bytes are never modified. Profiling covers the scoped master and outputs;
the source inventory reconciles every row in every supplied file.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import gzip
import json
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any

import numpy as np
import pandas as pd


REQUIRED = {
    "month", "town", "flat_type", "block", "street_name", "storey_range",
    "floor_area_sqm", "flat_model", "lease_commence_date", "resale_price",
}
DOMAINS = ("town", "flat_type", "flat_model", "storey_range")
TEXT_FIELDS = (*DOMAINS, "block", "street_name")
MONTH_PATTERN = r"[0-9]{4}-(?:0[1-9]|1[0-2])"
RESERVED = {
    "transaction_year", "transaction_month", "remaining_lease_years",
    "remaining_lease_months", "remaining_lease_total_months",
    "remaining_lease_years_months", "price_per_sqm", "Resale Identifier",
    "group_average_resale_price", "resale_identifier_hash", "resale_record_hash",
}


@dataclass(frozen=True)
class Config:
    start_month: str = "2012-01"
    end_month: str = "2016-12"
    reference_month: str = "2012-01"
    lease_years: int = 99
    chunk_size: int = 100_000
    anomaly_min_peers: int = 20
    anomaly_iqr_multiplier: float = 3.0

    def __post_init__(self) -> None:
        for value in (self.start_month, self.end_month, self.reference_month):
            if not re.fullmatch(MONTH_PATTERN, value):
                raise ValueError(f"Invalid configured month: {value}")
        if not self.start_month <= self.reference_month <= self.end_month:
            raise ValueError("Reference month must be inside the requested period")
        if self.lease_years <= 0 or self.chunk_size <= 0:
            raise ValueError("Lease duration and chunk size must be positive")
        if self.anomaly_min_peers < 4 or self.anomaly_iqr_multiplier <= 0:
            raise ValueError("Anomaly rule needs at least 4 peers and a positive multiplier")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def copy_verified(source: Path | str, destination: Path | str) -> str:
    """Publish through a buffered copy and verify all bytes before marking success."""
    source, destination = Path(source), Path(destination)
    expected_bytes = source.stat().st_size
    digest, copied = hashlib.sha256(), 0
    with source.open('rb') as incoming, destination.open('wb') as outgoing:
        for chunk in iter(lambda: incoming.read(1024 * 1024), b''):
            outgoing.write(chunk)
            digest.update(chunk)
            copied += len(chunk)
    if copied != expected_bytes or sha256_file(destination) != digest.hexdigest():
        raise IOError(f'Incomplete or corrupted output copy: {destination.name}')
    return str(destination)


def normalize_text(series: pd.Series) -> pd.Series:
    """Only normalize representation: Unicode, whitespace and case; no category aliases."""
    return (series.astype("string").str.normalize("NFKC").str.strip()
            .str.replace(r"\s+", " ", regex=True).str.upper().replace("", pd.NA))


def load_inputs(input_dir: Path, config: Config) -> tuple[pd.DataFrame, list[dict], list[str]]:
    """Read every CSV in chunks, union all attributes and retain only the requested scope.

    Malformed dates cannot be safely assigned to a period, so they enter the master
    for explicit quarantine. Well-formed dates outside the scope are counted only.
    """
    paths = sorted(input_dir.glob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"No CSV inputs in {input_dir}")
    source_columns: set[str] = set()
    frames: list[pd.DataFrame] = []
    inventory = []
    for path in paths:
        # Inspect the real header before pandas can silently mangle duplicate names.
        import csv
        with path.open(encoding="utf-8-sig", newline="") as stream:
            header = next(csv.reader(stream))
        if len(header) != len(set(header)):
            raise ValueError(f"Duplicate CSV column names: {path.name}")
        if REQUIRED - set(header):
            raise ValueError(f"Missing required columns in {path.name}: {sorted(REQUIRED - set(header))}")
        if any(c.startswith("_") for c in header) or set(header) & RESERVED:
            raise ValueError(f"Source columns collide with derived fields: {path.name}")
        source_columns.update(header)
        checksum = sha256_file(path)
        total = in_scope = invalid_date = outside = 0
        min_month, max_month = None, None
        for frame in pd.read_csv(path, dtype="string", keep_default_na=False,
                                 encoding="utf-8-sig", chunksize=config.chunk_size):
            month = frame["month"].str.strip()
            valid_format = month.str.fullmatch(MONTH_PATTERN, na=False)
            valid_format &= pd.to_datetime(month.where(valid_format), format="%Y-%m", errors="coerce").notna()
            requested = valid_format & month.between(config.start_month, config.end_month)
            selected = requested | ~valid_format
            date_values = month[valid_format]
            if len(date_values):
                lo, hi = str(date_values.min()), str(date_values.max())
                min_month = min(min_month, lo) if min_month else lo
                max_month = max(max_month, hi) if max_month else hi
            part = frame.loc[selected].copy()
            # CSV data row number, excluding the header; stable across chunk sizes.
            part["_source_row"] = np.arange(total + 1, total + len(frame) + 1)[selected.to_numpy()]
            part["_source_file"] = path.name
            part["_source_sha256"] = checksum
            part["_source_record_id"] = [
                hashlib.sha256(json_text([path.name, checksum, int(row)]).encode()).hexdigest()
                for row in part["_source_row"]
            ]
            frames.append(part)
            total += len(frame)
            in_scope += int(requested.sum())
            invalid_date += int((~valid_format).sum())
            outside += int((valid_format & ~requested).sum())
        inventory.append({
            "file": path.name, "bytes": path.stat().st_size, "sha256": checksum,
            "rows": total, "in_scope_rows": in_scope, "invalid_date_rows": invalid_date,
            "out_of_scope_rows": outside, "min_month": min_month, "max_month": max_month,
            "columns": header, "source": "user-supplied CSV; no network download",
        })
    columns = sorted(source_columns)
    master = pd.concat(frames, ignore_index=True, sort=False)
    for col in columns:
        if col not in master:
            master[col] = pd.Series(pd.NA, index=master.index, dtype="string")
        master[col] = master[col].astype("string")
    metadata = [c for c in master.columns if c.startswith("_")]
    master = master[columns + metadata].reset_index(drop=True)
    if master.empty:
        raise ValueError("No rows in the requested scope")
    return master, inventory, columns


def build_reference(master: pd.DataFrame, config: Config) -> dict:
    january = master.loc[master["month"].str.strip().eq(config.reference_month)]
    if january.empty:
        raise ValueError(f"Authoritative month {config.reference_month} is absent")
    domains = {}
    for column in DOMAINS:
        values = normalize_text(january[column])
        if values.isna().any():
            raise ValueError(f"Reference month has missing {column}; review source before proceeding")
        domains[column] = sorted(values.unique().tolist())
    if not all(re.fullmatch(r"[0-9]{2} TO [0-9]{2}", s) for s in domains["storey_range"]):
        raise ValueError("Reference month has an invalid storey representation")
    return {
        "reference_month": config.reference_month, "reference_rows": len(january),
        "date_format": "YYYY-MM", "date_scope": [config.start_month, config.end_month],
        "domains": domains,
    }


def money_cents(value: Any) -> int | None:
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number <= 0:
            return None
        cents = number * 100
        if cents != cents.to_integral_value() or cents >= 2**63:
            return None
        return int(cents)
    except (InvalidOperation, ValueError):
        return None


def parse_provided_lease(value: Any, maximum: int = 1188) -> int | None:
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if re.fullmatch(r"[0-9]{1,3}", text):
        months = int(text) * 12
    else:
        match = re.fullmatch(r"([0-9]{1,3}) years?(?: ([0-9]{1,2}) months?)?", text)
        if not match or int(match.group(2) or 0) > 11:
            return None
        months = int(match.group(1)) * 12 + int(match.group(2) or 0)
    return months if 0 <= months <= maximum else None


def validate(master: pd.DataFrame, reference: dict, config: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize and validate, recording every failed rule for each rejected row."""
    data = master.copy()
    for c in TEXT_FIELDS:
        data[c] = normalize_text(data[c])
    for c in data.select_dtypes(include="string").columns:
        if not c.startswith("_"):
            data[c] = data[c].str.strip().replace("", pd.NA)
    reasons: list[list[str]] = [[] for _ in range(len(data))]

    def flag(mask: pd.Series, reason: str) -> None:
        for i in np.flatnonzero(mask.fillna(True).to_numpy(dtype=bool)):
            reasons[int(i)].append(reason)

    for c in sorted(REQUIRED):
        flag(data[c].isna(), f"missing:{c}")
    valid_date = data.month.str.fullmatch(MONTH_PATTERN, na=False)
    dates = pd.to_datetime(data.month.where(valid_date), format="%Y-%m", errors="coerce")
    flag(~valid_date | dates.isna(), "invalid_date")
    flag(valid_date & ~data.month.between(config.start_month, config.end_month), "outside_requested_period")
    for c in DOMAINS:
        flag(~data[c].isin(reference["domains"][c]), f"not_in_reference:{c}")
    storeys = data.storey_range.str.extract(r"^([0-9]{2}) TO ([0-9]{2})$").apply(pd.to_numeric, errors="coerce")
    flag(storeys.isna().any(axis=1) | (storeys[0] < 1) | (storeys[0] > storeys[1]), "invalid_storey_range")
    flag(~data.block.str.contains(r"[0-9]", regex=True, na=False), "block_has_no_digits")
    area = pd.to_numeric(data.floor_area_sqm, errors="coerce").astype("Float64")
    flag(area.isna() | ~np.isfinite(area.fillna(0)) | area.le(0), "invalid_floor_area")
    lease_year = pd.to_numeric(data.lease_commence_date, errors="coerce").astype("Float64")
    good_year = data.lease_commence_date.str.fullmatch(r"[0-9]{4}", na=False)
    flag(~good_year, "invalid_lease_commence_year")
    # Transaction is observed only to month; assume Jan 1 in the commencement year.
    total_months = ((lease_year + config.lease_years - dates.dt.year) * 12 - (dates.dt.month - 1))
    flag(total_months.isna() | total_months.le(0) | total_months.gt(config.lease_years * 12), "lease_outside_0_to_99_years")
    cents = data.resale_price.map(money_cents).astype("Int64")
    flag(cents.isna(), "invalid_resale_price")
    if "remaining_lease" in data:
        provided = data.remaining_lease.map(lambda x: parse_provided_lease(x, config.lease_years * 12))
        flag(data.remaining_lease.notna() & provided.isna(), "invalid_provided_remaining_lease")
    else:
        provided = pd.Series(np.nan, index=data.index)
    valid = pd.Series([not r for r in reasons], index=data.index)
    rejected = master.loc[~valid].copy()
    rejected["_quarantine_stage"] = "validation"
    rejected["_quarantine_reasons"] = [json_text(reasons[i]) for i in np.flatnonzero(~valid)]
    accepted = data.loc[valid].copy()
    accepted["resale_price"] = (cents[valid] / 100).astype("float64")
    accepted["floor_area_sqm"] = area[valid].astype("float64")
    accepted["lease_commence_date"] = lease_year[valid].astype("int64")
    accepted["_price_cents"] = cents[valid].astype("int64")
    accepted["transaction_year"] = dates[valid].dt.year.astype("int64")
    accepted["transaction_month"] = dates[valid].dt.month.astype("int64")
    accepted["remaining_lease_total_months"] = total_months[valid].astype("int64")
    accepted["remaining_lease_years"] = accepted.remaining_lease_total_months // 12
    accepted["remaining_lease_months"] = accepted.remaining_lease_total_months % 12
    accepted["remaining_lease_years_months"] = (
        accepted.remaining_lease_years.astype(str) + " years " +
        accepted.remaining_lease_months.astype(str).str.zfill(2) + " months"
    )
    accepted["price_per_sqm"] = accepted.resale_price / accepted.floor_area_sqm
    accepted["_date_basis"] = np.where(accepted.month < "2012-03", "approval", "registration")
    accepted["_provided_lease_months"] = provided[valid].astype("Float64")
    accepted["_provided_lease_difference_months"] = (
        accepted._provided_lease_months - accepted.remaining_lease_total_months
    )
    # Source lease is retained, not overwritten. A difference is not proof of bad data.
    accepted["_lease_comparison_note"] = np.where(
        accepted._provided_lease_months.isna(), "source value absent",
        np.where(accepted._provided_lease_difference_months.abs().fillna(0) > 12,
                 "review: difference exceeds 12 months", "within year/month precision allowance"),
    )
    return accepted.reset_index(drop=True), rejected.reset_index(drop=True)


def canonical_value(value: Any) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        return format(Decimal(str(value)).normalize(), "f")
    return str(value)


def add_keys(data: pd.DataFrame, source_columns: list[str]) -> pd.DataFrame:
    key_columns = sorted(set(source_columns) - {"resale_price"})
    result = data.copy()
    result["_canonical_key"] = [
        json_text([[c, canonical_value(v)] for c, v in zip(key_columns, row)])
        for row in result[key_columns].itertuples(index=False, name=None)
    ]
    result["_key_hash"] = result._canonical_key.map(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())
    # Finite hashes cannot have an absolute mathematical uniqueness guarantee.
    if (result.groupby("_key_hash")._canonical_key.nunique() > 1).any():
        raise RuntimeError("SHA-256 collision detected; publication aborted")
    return result


def deduplicate(data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Maximum valid price wins; ties resolve by filename and source row, deterministically."""
    ranked = data.sort_values(["_price_cents", "_source_file", "_source_row"],
                              ascending=[False, True, True], kind="stable")
    loser = ranked.duplicated("_canonical_key", keep="first")
    winners = ranked.loc[~loser].copy()
    rejected = ranked.loc[loser].copy()
    refs = winners.set_index("_canonical_key")._source_record_id
    rejected["_reference_source_record_id"] = rejected._canonical_key.map(refs)
    rejected["_quarantine_stage"] = "duplicate"
    rejected["_quarantine_reasons"] = json_text(["duplicate_non_maximum_or_tied_price"])
    return winners.reset_index(drop=True), rejected.reset_index(drop=True)


def detect_anomalies(data: pd.DataFrame, config: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Conservative outer Tukey fences on log(price/m2), with explicit peer fallbacks.

    A 3-IQR rule flags candidates for review, never claims fraud or imputes a price.
    Zero-spread or small groups fall back; no usable peer group means unassessed.
    """
    scored = data.copy()
    scored["_log_price_per_sqm"] = np.log(scored.price_per_sqm)
    scored["_anomaly_peer_level"] = "unassessed"
    scored["_anomaly_peer_count"] = 0
    scored["_anomaly_lower_ppsqm"] = np.nan
    scored["_anomaly_upper_ppsqm"] = np.nan
    levels = [
        ("town_flat_type_year", ["town", "flat_type", "transaction_year"]),
        ("town_flat_type_all_years", ["town", "flat_type"]),
        ("flat_type_year", ["flat_type", "transaction_year"]),
    ]
    for label, columns in levels:
        group = scored.groupby(columns, dropna=False)["_log_price_per_sqm"]
        count = group.transform("size")
        q1 = group.transform("quantile", q=0.25)
        q3 = group.transform("quantile", q=0.75)
        iqr = q3 - q1
        use = scored._anomaly_peer_level.eq("unassessed") & count.ge(config.anomaly_min_peers) & iqr.gt(1e-12)
        scored.loc[use, "_anomaly_peer_level"] = label
        scored.loc[use, "_anomaly_peer_count"] = count[use]
        scored.loc[use, "_anomaly_lower_ppsqm"] = np.exp(q1[use] - config.anomaly_iqr_multiplier * iqr[use])
        scored.loc[use, "_anomaly_upper_ppsqm"] = np.exp(q3[use] + config.anomaly_iqr_multiplier * iqr[use])
    anomaly = (scored.price_per_sqm.lt(scored._anomaly_lower_ppsqm) |
               scored.price_per_sqm.gt(scored._anomaly_upper_ppsqm))
    rejected = scored.loc[anomaly].copy()
    rejected["_quarantine_stage"] = "anomaly"
    rejected["_quarantine_reasons"] = json_text(["potential_price_anomaly"])
    accepted = scored.loc[~anomaly].copy()
    return accepted.reset_index(drop=True), rejected.reset_index(drop=True)


def transform(cleaned: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    result = cleaned.copy()
    groups = result.groupby(["month", "town", "flat_type"], dropna=False)["_price_cents"]
    # Python integers avoid overflow in group sums; Decimal avoids leading-digit
    # errors from rounding a group mean to the next dollar or scientific notation.
    sums = groups.transform(lambda x: sum(int(v) for v in x)).astype(object)
    counts = groups.transform("size")
    whole_dollars = pd.Series([int(s) // (int(n) * 100) for s, n in zip(sums, counts)], index=result.index)
    result["group_average_resale_price"] = [int(s) / (int(n) * 100) for s, n in zip(sums, counts)]
    price_digits = whole_dollars.astype(str).str.zfill(2).str[:2]
    block_digits = result.block.str.replace(r"[^0-9]", "", regex=True).str.zfill(3).str[:3]
    result["Resale Identifier"] = "S" + block_digits + price_digits + result.month.str[-2:] + result.town.str[:1]
    if not result["Resale Identifier"].str.fullmatch(r"S[0-9]{7}[A-Z]").all():
        raise RuntimeError("Identifier does not satisfy the required nine-character format")
    hashed = cleaned.copy()
    # Both requirements are explicit: the literal code hash, plus a unique record
    # identity. Hashing the short business code alone cannot remove its collisions.
    hashed["resale_identifier_hash"] = result["Resale Identifier"].map(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())
    hashed["resale_record_hash"] = cleaned["_key_hash"]
    if hashed.resale_record_hash.duplicated().any():
        raise RuntimeError("Record hash is not unique; publication aborted")
    return result, hashed


INTERNAL = {"_canonical_key", "_key_hash", "_price_cents", "_log_price_per_sqm"}


def public_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop(columns=list(INTERNAL), errors="ignore").sort_values(
        ["month", "_source_file", "_source_row"], kind="stable", na_position="last"
    ).reset_index(drop=True)


def profile(frame: pd.DataFrame) -> dict:
    columns = []
    for c in frame:
        values = frame[c]
        missing = values.isna() | values.astype("string").str.strip().eq("").fillna(False)
        item = {"column": c, "dtype": str(values.dtype), "missing": int(missing.sum()),
                "missing_pct": round(float(missing.mean() * 100), 4) if len(frame) else 0.0,
                "distinct_non_null": int(values.nunique(dropna=True))}
        if pd.api.types.is_numeric_dtype(values) or c in {"resale_price", "floor_area_sqm", "lease_commence_date"}:
            numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
            numeric = numeric[np.isfinite(numeric)]
            item["numeric_parseable_count"] = len(numeric)
            item["numeric_summary"] = ({str(k): float(v) if np.isfinite(v) else None for k, v in numeric.describe().items()}
                                       if len(numeric) else {})
        else:
            item["top_values"] = {str(k): int(v) for k, v in values.value_counts().head(5).items()}
        columns.append(item)
    return {"rows": len(frame), "columns": columns}


def run_pipeline(input_dir: Path, output_dir: Path, config: Config | None = None) -> dict:
    config = config or Config()
    input_dir, output_dir = input_dir.resolve(), output_dir.resolve()
    if input_dir == output_dir or input_dir.is_relative_to(output_dir) or output_dir.is_relative_to(input_dir):
        raise ValueError("Output directory cannot contain the raw inputs")
    master, inventory, source_columns = load_inputs(input_dir, config)
    reference = build_reference(master, config)
    valid, invalid = validate(master, reference, config)
    keyed = add_keys(valid, source_columns)
    unique, duplicates = deduplicate(keyed)
    clean, anomalies = detect_anomalies(unique, config)
    if clean.empty:
        raise ValueError("All rows were rejected; review validation reference and inputs")
    transformed, hashed = transform(clean)
    # Restore exact original attributes in quarantine; retain computed audit fields.
    quarantine = pd.concat([invalid, duplicates, anomalies], ignore_index=True, sort=False)
    originals = master.set_index("_source_record_id")
    for col in source_columns:
        quarantine[col] = quarantine._source_record_id.map(originals[col])
    outputs = {"cleaned": public_frame(clean), "transformed": public_frame(transformed),
               "hashed": public_frame(hashed), "quarantined": public_frame(quarantine)}
    kept_ids = set(outputs["cleaned"]._source_record_id)
    rejected_ids = set(outputs["quarantined"]._source_record_id)
    if kept_ids & rejected_ids or kept_ids | rejected_ids != set(master._source_record_id):
        raise RuntimeError("Row reconciliation failed")
    if len(kept_ids) + len(rejected_ids) != len(master):
        raise RuntimeError("Rows are duplicated or lost in the output groups")
    reason_counts = Counter(reason for text in quarantine._quarantine_reasons for reason in json.loads(text))
    code_counts = transformed["Resale Identifier"].value_counts()
    counts = {
        "raw_rows": sum(i["rows"] for i in inventory),
        "out_of_scope_rows": sum(i["out_of_scope_rows"] for i in inventory),
        "in_scope_rows": sum(i["in_scope_rows"] for i in inventory),
        "invalid_date_rows": sum(i["invalid_date_rows"] for i in inventory),
        "master_rows": len(master), "reference_rows": reference["reference_rows"],
        "validation_rejected_rows": len(invalid), "validation_passed_rows": len(valid),
        "duplicate_rejected_rows": len(duplicates), "anomaly_candidates": len(anomalies),
        "cleaned_rows": len(clean), "transformed_rows": len(transformed),
        "quarantined_rows": len(quarantine), "hashed_rows": len(hashed),
        "anomaly_unassessed_rows": int(clean._anomaly_peer_level.eq("unassessed").sum()),
        "business_identifier_distinct": int(len(code_counts)),
        "business_identifier_duplicate_excess": int(code_counts.sub(1).clip(lower=0).sum()),
        "business_identifier_collision_groups": int(code_counts.gt(1).sum()),
        "business_identifier_rows_in_collisions": int(code_counts[code_counts.gt(1)].sum()),
        "unique_record_hashes": int(hashed.resale_record_hash.nunique()),
        "provided_lease_difference_gt_12_months": int(clean._provided_lease_difference_months.abs().gt(12).sum()),
    }
    if counts["raw_rows"] != counts["out_of_scope_rows"] + counts["master_rows"]:
        raise RuntimeError("Raw/master scope accounting failed")
    summary = {"pipeline_version": "1.0.0", "source_mode": "uploaded_files_only",
               "config": asdict(config), "source_columns": source_columns,
               "composite_key_columns": sorted(set(source_columns) - {"resale_price"}),
               "counts": counts, "quarantine_reason_counts": dict(sorted(reason_counts.items())),
               "cleaned_price_summary": {str(k): float(v) if np.isfinite(v) else None for k, v in clean.resale_price.describe().items()},
               "anomaly_peer_levels": {str(k): int(v) for k, v in clean._anomaly_peer_level.value_counts().items()}}
    # Build all outputs in a staging directory, then expose a success marker last.
    # This is a single-writer local workflow, not a cross-directory atomic commit.
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hdb-etl-", dir=output_dir.parent) as temporary:
        stage = Path(temporary)
        (stage / "master").mkdir()
        master.to_parquet(stage / "master/master.parquet", index=False, compression="snappy")
        for group, frame in outputs.items():
            dest = stage / group
            dest.mkdir()
            frame.to_parquet(dest / f"{group}.parquet", index=False, compression="snappy")
            frame.to_csv(dest / f"{group}.csv.gz", index=False,
                         compression={"method": "gzip", "compresslevel": 6, "mtime": 0})
            # A complete read checks the gzip footer and CRC before publication.
            with gzip.open(dest / f"{group}.csv.gz", 'rb') as compressed:
                while compressed.read(1024 * 1024):
                    pass
        reports = stage / "reports"
        reports.mkdir()
        write_json(reports / "source_inventory.json", inventory)
        write_json(reports / "reference_domains.json", reference)
        write_json(reports / "run_summary.json", summary)
        write_json(reports / "data_profiles.json", {"master": profile(master),
                   "cleaned": profile(outputs["cleaned"]), "quarantined": profile(outputs["quarantined"])})
        pd.DataFrame(sorted(reason_counts.items()), columns=["reason", "rows"]).to_csv(reports / "quarantine_by_reason.csv", index=False)
        statuses = pd.concat([
            clean[["month", "_source_record_id"]].assign(status="cleaned"),
            quarantine[["month", "_source_record_id", "_quarantine_stage"]].rename(columns={"_quarantine_stage": "status"}),
        ])
        monthly = statuses.groupby(["month", "status"], dropna=False).size().unstack(fill_value=0)
        monthly.to_csv(reports / "quality_by_month.csv")
        unseen = []
        for c in DOMAINS:
            values = normalize_text(master[c])
            unseen.extend({"column": c, "value": str(v), "rows": int(n)}
                          for v, n in values[~values.isin(reference["domains"][c])].value_counts().items())
        pd.DataFrame(unseen, columns=["column", "value", "rows"]).to_csv(reports / "unseen_categories.csv", index=False)
        write_json(stage / "_SUCCESS.json", {"counts": counts,
                   "input_sha256": {i["file"]: i["sha256"] for i in inventory}})
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "_SUCCESS.json").unlink(missing_ok=True)
        for item in sorted(stage.iterdir()):
            if item.name != "_SUCCESS.json":
                if item.is_dir():
                    shutil.copytree(item, output_dir / item.name, dirs_exist_ok=True, copy_function=copy_verified)
                else:
                    copy_verified(item, output_dir / item.name)
        copy_verified(stage / "_SUCCESS.json", output_dir / "_SUCCESS.json")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/outputs"))
    parser.add_argument("--start-month", default="2012-01")
    parser.add_argument("--end-month", default="2016-12")
    parser.add_argument("--reference-month", default="2012-01")
    args = parser.parse_args()
    result = run_pipeline(args.input_dir, args.output_dir, Config(
        start_month=args.start_month, end_month=args.end_month, reference_month=args.reference_month))
    print(json.dumps(result["counts"], indent=2))


if __name__ == "__main__":
    main()
