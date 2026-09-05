"""Plotting must distinguish missing inputs from newly generated results."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("plot_all_results.py")


class PlotTests(unittest.TestCase):
    def test_empty_results_report_stale_plot_without_deleting_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results, plots = root / "results", root / "plots"
            results.mkdir()
            plots.mkdir()
            stale = plots / "fig5_scaling.pdf"
            stale.write_bytes(b"previous run")
            run = subprocess.run(
                [sys.executable, str(SCRIPT), "--results-dir", str(results),
                 "--plots-dir", str(plots)], capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertIn("is stale", run.stdout)
            self.assertEqual(stale.read_bytes(), b"previous run")
            manifest = json.loads((plots / "plot_manifest.json").read_text())
            self.assertTrue(manifest["plots"])
            self.assertTrue(all(p["status"] == "skipped" for p in manifest["plots"].values()))

    def test_missing_matplotlib_is_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "matplotlib.py").write_text("raise ImportError('test')\n")
            import os
            run = subprocess.run([sys.executable, str(SCRIPT)],
                                 env=dict(os.environ, PYTHONPATH=directory),
                                 capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)
            self.assertIn("ERROR", run.stdout)


if __name__ == "__main__":
    unittest.main()
