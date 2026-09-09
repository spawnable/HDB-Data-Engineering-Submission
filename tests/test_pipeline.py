"""Behavioral tests for ambiguous rules, adversarial rows and the ETL contract."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hdb_resale.pipeline import (
    Config, add_keys, build_reference, deduplicate, detect_anomalies, load_inputs,
    money_cents, parse_provided_lease, profile, run_pipeline, sha256_file, transform, validate,
)


BASE = dict(month="2012-01", town="ANG MO KIO", flat_type="3 ROOM", block="19",
            street_name="ANG MO KIO AVE 1", storey_range="01 TO 03", floor_area_sqm="60",
            flat_model="IMPROVED", lease_commence_date="1980", resale_price="230000")


def frame(*changes):
    records = [{**BASE, **change} for change in changes] or [BASE]
    result = pd.DataFrame(records, dtype="string")
    result["_source_file"] = "source.csv"
    result["_source_row"] = np.arange(1, len(result) + 1)
    result["_source_record_id"] = [f"row-{i}" for i in range(1, len(result) + 1)]
    result["_source_sha256"] = "source-hash"
    return result


def accepted(*changes):
    data = frame(*changes)
    reference = build_reference(frame(), Config())
    valid, _ = validate(data, reference, Config())
    return add_keys(valid, [c for c in data if not c.startswith("_")])


def test_reference_is_discovered_not_hardcoded():
    ref = build_reference(frame({"town": "CUSTOM TOWN"}, {"month": "2012-02", "town": "OTHER TOWN"}), Config())
    assert ref["domains"]["town"] == ["CUSTOM TOWN"]


def test_missing_reference_stops_instead_of_guessing():
    with pytest.raises(ValueError, match="absent"):
        build_reference(frame({"month": "2012-02"}), Config())


def test_valid_later_date_is_not_restricted_to_january():
    valid = accepted({"month": "2016-12"})
    assert len(valid) == 1
    assert valid.iloc[0].transaction_month == 12


@pytest.mark.parametrize("value", ["2012-13", "2012-1", "2012/01", "2012-01-01", "not a date", ""])
def test_bad_dates_are_rejected(value):
    valid, rejected = validate(frame({"month": value}), build_reference(frame(), Config()), Config())
    assert valid.empty
    assert "invalid_date" in json.loads(rejected.iloc[0]._quarantine_reasons)


def test_new_storey_band_is_not_falsely_rebinned():
    _, rejected = validate(frame({"storey_range": "01 TO 05"}), build_reference(frame(), Config()), Config())
    assert "not_in_reference:storey_range" in json.loads(rejected.iloc[0]._quarantine_reasons)


def test_all_validation_failures_are_kept():
    _, rejected = validate(frame({"town": "UNKNOWN", "flat_model": "NEW MODEL", "floor_area_sqm": "0"}),
                           build_reference(frame(), Config()), Config())
    reasons = json.loads(rejected.iloc[0]._quarantine_reasons)
    assert {"not_in_reference:town", "not_in_reference:flat_model", "invalid_floor_area"} <= set(reasons)


def test_normalization_does_not_change_semantic_identity():
    data = accepted({"town": "  ang   mo kio ", "flat_model": "improved", "floor_area_sqm": "60.0"}, {})
    assert data._canonical_key.nunique() == 1


def test_lease_uses_transaction_month_and_january_start():
    data = accepted({"month": "2015-02"}, {"month": "2015-12"})
    assert list(data.remaining_lease_total_months) == [767, 757]
    assert list(zip(data.remaining_lease_years, data.remaining_lease_months)) == [(63, 11), (63, 1)]


@pytest.mark.parametrize("year", ["2013", "1913", "1980.5", "bad"])
def test_future_expired_or_invalid_lease_is_rejected(year):
    valid, rejected = validate(frame({"lease_commence_date": year}), build_reference(frame(), Config()), Config())
    assert valid.empty and len(rejected) == 1


@pytest.mark.parametrize("price", ["0", "-1", "NaN", "Infinity", "230000.001", "abc"])
def test_invalid_money_is_rejected(price):
    assert money_cents(price) is None


def test_money_keeps_cents_exact():
    assert money_cents("230000.01") == 23_000_001


def test_provided_lease_formats_and_bounds():
    assert parse_provided_lease("70") == 840
    assert parse_provided_lease("61 years 04 months") == 736
    assert parse_provided_lease("61 years 12 months") is None
    assert parse_provided_lease("100") is None


def test_max_price_and_deterministic_tie_break():
    data = accepted({"resale_price": "240000"}, {"resale_price": "250000"}, {"resale_price": "250000"})
    winners, losers = deduplicate(data.sample(frac=1, random_state=7))
    assert winners.iloc[0].resale_price == 250000
    assert winners.iloc[0]._source_record_id == "row-2"
    assert set(losers._reference_source_record_id) == {"row-2"}


def test_composite_key_includes_optional_source_attributes():
    data = accepted({"remaining_lease": "65"}, {"remaining_lease": "66"})
    winners, _ = deduplicate(data)
    assert len(winners) == 2


def test_key_is_not_delimiter_concatenation():
    data = accepted({"extra_a": "x|y", "extra_b": "z"}, {"extra_a": "x", "extra_b": "y|z"})
    assert data._canonical_key.nunique() == 2


def test_price_outlier_is_flagged_with_peer_evidence():
    data = accepted(*[{"block": str(i), "resale_price": str(210000 + i * 1000)} for i in range(60)],
                    {"block": "999", "resale_price": "9000000"})
    clean, outliers = detect_anomalies(data, Config())
    assert "999" in set(outliers.block)
    assert outliers._anomaly_peer_count.min() >= 20
    assert clean.resale_price.max() < 9000000


def test_sparse_or_zero_spread_groups_are_unassessed():
    clean, outliers = detect_anomalies(accepted({}, {"block": "20"}), Config())
    assert outliers.empty
    assert set(clean._anomaly_peer_level) == {"unassessed"}


def test_identifier_examples_padding_truncation_and_average():
    data = accepted({"block": "A19B", "resale_price": "229999.99"},
                    {"block": "1234A", "resale_price": "230000.01"})
    transformed, _ = transform(data)
    assert list(transformed["Resale Identifier"]) == ["S0192301A", "S1232301A"]


def test_hashing_short_codes_does_not_hide_collisions():
    data = accepted({}, {"street_name": "ANOTHER STREET"})
    transformed, hashed = transform(data)
    assert transformed["Resale Identifier"].nunique() == 1
    assert hashed.resale_identifier_hash.nunique() == 1
    assert hashed.resale_record_hash.nunique() == 2
    assert hashed.iloc[0].resale_identifier_hash == hashlib.sha256(b"S0192301A").hexdigest()


def test_record_hash_ignores_price_but_not_source_business_attributes():
    a = accepted({})
    b = accepted({"resale_price": "999999"})
    assert a.iloc[0]._key_hash == b.iloc[0]._key_hash


def test_profile_supports_zero_quarantine_rows():
    json.dumps(profile(pd.DataFrame({"x": pd.Series(dtype="string")})), allow_nan=False)


def test_offline_pipeline_preserves_raw_bytes_unions_schema_and_reconciles(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    # Out-of-scope file contributes an attribute to the union without adding rows.
    pd.DataFrame([{**BASE, "month": "2017-01", "future_attribute": "retain schema"}]).to_csv(raw / "future.csv", index=False)
    pd.DataFrame([BASE, {**BASE, "resale_price": "240000"}, {**BASE, "month": "2012-02", "block": "20", "town": "UNKNOWN"},
                  {**BASE, "month": "invalid"}]).to_csv(raw / "current.csv", index=False)
    before = {p.name: sha256_file(p) for p in raw.glob("*.csv")}
    summary = run_pipeline(raw, tmp_path / "outputs", Config(chunk_size=2))
    counts = summary["counts"]
    assert counts["raw_rows"] == 5
    assert counts["out_of_scope_rows"] == 1
    assert counts["cleaned_rows"] + counts["quarantined_rows"] == 4
    assert "future_attribute" in summary["source_columns"]
    assert before == {p.name: sha256_file(p) for p in raw.glob("*.csv")}
    for group in ["cleaned", "transformed", "hashed", "quarantined"]:
        assert (tmp_path / f"outputs/{group}/{group}.parquet").exists()
    # Stable output and idempotent row set after a change in reader chunk size.
    first = pd.read_parquet(tmp_path / "outputs/hashed/hashed.parquet")
    rerun = run_pipeline(raw, tmp_path / "outputs", Config(chunk_size=100))
    pd.testing.assert_frame_equal(first, pd.read_parquet(tmp_path / "outputs/hashed/hashed.parquet"))
    assert counts == rerun["counts"]


def test_no_output_inside_raw_or_raw_inside_output(tmp_path):
    with pytest.raises(ValueError, match="cannot contain"):
        run_pipeline(tmp_path / "data/raw", tmp_path / "data")


def test_missing_required_column_fails_fast(tmp_path):
    pd.DataFrame([{"month": "2012-01"}]).to_csv(tmp_path / "incomplete.csv", index=False)
    with pytest.raises(ValueError, match="Missing required"):
        load_inputs(tmp_path, Config())


def test_duplicate_csv_header_fails_fast(tmp_path):
    (tmp_path / "bad.csv").write_text("month,month\n2012-01,2012-01\n")
    with pytest.raises(ValueError, match="Duplicate CSV column"):
        load_inputs(tmp_path, Config())
