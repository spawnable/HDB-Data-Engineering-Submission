"""Execute every notebook cell and save genuine outputs.

Default: a fresh Jupyter kernel via nbclient.
--in-process: a fresh IPython shell for hosts that prohibit local sockets.
This fallback executes Python cells; it does not emulate arbitrary Jupyter widgets.
"""
import argparse
import os
from pathlib import Path
import nbformat

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--in-process', action='store_true')
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
path = root / 'hdb_resale_assessment.ipynb'
notebook = nbformat.read(path, as_version=4)

if args.in_process:
    from IPython.core.interactiveshell import InteractiveShell
    from IPython.utils.capture import capture_output
    shell = InteractiveShell.instance()
    os.chdir(root)
    count = 0
    for cell in notebook.cells:
        if cell.cell_type != 'code':
            continue
        count += 1
        with capture_output(stdout=True, stderr=True, display=True) as captured:
            result = shell.run_cell(cell.source, store_history=True)
        if result.error_before_exec or result.error_in_exec:
            raise RuntimeError(f'Cell {count} failed: {captured.stdout}\n{captured.stderr}') from (
                result.error_before_exec or result.error_in_exec)
        cell.outputs = [nbformat.v4.new_output('display_data', data=o.data, metadata=o.metadata or {})
                        for o in captured.outputs]
        if captured.stdout:
            cell.outputs.append(nbformat.v4.new_output('stream', name='stdout', text=captured.stdout))
        if captured.stderr:
            cell.outputs.append(nbformat.v4.new_output('stream', name='stderr', text=captured.stderr))
        cell.execution_count = count
    notebook.metadata['execution_method'] = 'Fresh IPython in-process shell; local socket creation unavailable in build host'
else:
    from nbclient import NotebookClient
    NotebookClient(notebook, timeout=600, kernel_name='python3',
                   resources={'metadata': {'path': str(root)}}).execute()
    notebook.metadata['execution_method'] = 'Fresh Jupyter kernel via nbclient'

nbformat.validate(notebook)
nbformat.write(notebook, path)
print(f'Executed {sum(c.cell_type == "code" for c in notebook.cells)} code cells successfully')
