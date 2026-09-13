"""Check the actual submitted files and output contracts; write an auditable report."""
from pathlib import Path
import hashlib
import json
import sys

import nbformat
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from hdb_resale.pipeline import sha256_file

out = ROOT / 'data/outputs'
summary = json.loads((out / 'reports/run_summary.json').read_text())
inventory = json.loads((out / 'reports/source_inventory.json').read_text())
reference = json.loads((out / 'reports/reference_domains.json').read_text())
checks = []


def require(name, condition):
    if not bool(condition):
        raise AssertionError(name)
    checks.append({'check': name, 'status': 'passed'})


for source in inventory:
    path = ROOT / 'data/raw' / source['file']
    require(f"Unchanged raw checksum: {source['file']}", sha256_file(path) == source['sha256'])

master = pd.read_parquet(out / 'master/master.parquet')
frames = {name: pd.read_parquet(out / name / f'{name}.parquet')
          for name in ['cleaned', 'transformed', 'quarantined', 'hashed']}
clean, transformed, quarantine, hashed = (frames[n] for n in ['cleaned', 'transformed', 'quarantined', 'hashed'])
require('All supplied files inventoried', len(inventory) == len(list((ROOT / 'data/raw').glob('*.csv'))))
require('Raw row accounting', sum(s['rows'] for s in inventory) == len(master) + sum(s['out_of_scope_rows'] for s in inventory))
require('Every master row appears once in Cleaned or Quarantined',
        len(clean) + len(quarantine) == len(master) and
        len(set(clean._source_record_id) | set(quarantine._source_record_id)) == len(master) and
        set(clean._source_record_id).isdisjoint(quarantine._source_record_id) and
        set(clean._source_record_id) | set(quarantine._source_record_id) == set(master._source_record_id))
require('Transformed and Hashed align with Cleaned',
        clean._source_record_id.equals(transformed._source_record_id) and clean._source_record_id.equals(hashed._source_record_id))
require('Every source attribute retained', all(set(summary['source_columns']) <= set(f.columns) for f in frames.values()))
require('No duplicate composite keys remain', not clean.duplicated(summary['composite_key_columns']).any())
require('Lease quotient/remainder agrees', ((clean.remaining_lease_years*12+clean.remaining_lease_months) == clean.remaining_lease_total_months).all())
require('Lease residual months in 0..11', clean.remaining_lease_months.between(0,11).all())
require('Recorded transaction period respected', clean.month.between(summary['config']['start_month'],summary['config']['end_month']).all())
for field, allowed in reference['domains'].items():
    require(f'Cleaned reference membership: {field}', clean[field].isin(allowed).all())

original = master.set_index('_source_record_id')
q = quarantine.set_index('_source_record_id')
for col in summary['source_columns']:
    pd.testing.assert_series_equal(q[col].astype('string'), original.loc[q.index,col].astype('string'),check_names=False)
checks.append({'check': 'Quarantined source attributes match original master values', 'status': 'passed'})

duplicates = quarantine.loc[quarantine._quarantine_stage.eq('duplicate')]
prices = pd.to_numeric(original.resale_price)
reference_prices = duplicates._reference_source_record_id.map(prices)
duplicate_prices = pd.to_numeric(duplicates.resale_price)
require('Suppressed duplicates never exceed their reference price', (reference_prices.to_numpy() >= duplicate_prices.to_numpy()).all())
require('Duplicate references survive as Cleaned or Anomaly',
        set(duplicates._reference_source_record_id) <=
        set(clean._source_record_id) | set(quarantine.loc[quarantine._quarantine_stage.eq('anomaly'),'_source_record_id']))

mean_check = clean.groupby(['month','town','flat_type']).resale_price.transform('mean')
require('Identifier group means recomputed from final Cleaned', np.allclose(mean_check,transformed.group_average_resale_price,rtol=0,atol=1e-8))
require('Readable identifier is exactly the prescribed format', transformed['Resale Identifier'].str.fullmatch(r'S[0-9]{7}[A-Z]').all())
literal_hashes = transformed['Resale Identifier'].map(lambda value: hashlib.sha256(value.encode('utf-8')).hexdigest())
require('Literal SHA-256 matches every business identifier', literal_hashes.equals(hashed.resale_identifier_hash))
require('Composite-key record hashes are unique', hashed.resale_record_hash.is_unique)

for group, frame in frames.items():
    # CSV is an interoperable output too, not merely an empty placeholder.
    csv_frame = pd.read_csv(out / group / f'{group}.csv.gz', dtype='string', keep_default_na=False)
    require(f'{group}: CSV/Parquet row and column agreement', len(csv_frame)==len(frame) and list(csv_frame.columns)==list(frame.columns))
    require(f'{group}: CSV/Parquet lineage agreement', list(csv_frame._source_record_id)==list(frame._source_record_id))

nb = nbformat.read(ROOT / 'hdb_resale_assessment.ipynb',as_version=4)
nbformat.validate(nb)
code_cells = [cell for cell in nb.cells if cell.cell_type=='code']
require('All notebook code cells have execution counts and outputs',
        all(c.execution_count is not None and c.outputs for c in code_cells))
require('No notebook error outputs', not any(o.output_type=='error' for c in code_cells for o in c.outputs))
require('No broken control characters in notebook prose',
        not any(ord(char)<32 and char not in '\n\t' for c in nb.cells for char in c.source))
require('Two architecture PNGs and two SVG sources are present',
        all((ROOT / 'architecture' / f'{name}.{extension}').is_file()
            for name in ['01_batch_ingestion','02_private_analytics'] for extension in ['png','svg']))
require('Assessment PDF excluded', not list(ROOT.rglob('*.pdf')))
require('Pipeline success marker present', (out / '_SUCCESS.json').exists())

report = {'status':'passed', 'checks_passed':len(checks), 'notebook_code_cells':len(code_cells),
          'notebook_execution_method':nb.metadata.get('execution_method'), 'counts':summary['counts'], 'checks':checks}
(ROOT / 'docs/verification.json').write_text(json.dumps(report,indent=2)+'\n')
print(f'Passed {len(checks)} submission checks; {len(code_cells)} executed notebook code cells')
