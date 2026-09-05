"""Failure-path regression checks; isolated from real results and toolchains."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class DevRunnerTests(unittest.TestCase):
    def test_build_failure_stops_without_publishing_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative in ("scripts/eval/run_zkvm_dev_mode.sh", "scripts/lib/common.sh"):
                destination = root / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, destination)
            bindir = root / "bin"
            bindir.mkdir()
            cargo = bindir / "cargo"
            cargo.write_text("#!/bin/sh\nexit 42\n")
            cargo.chmod(0o755)
            env = dict(os.environ, PATH=f"{bindir}:/usr/bin:/bin",
                       RUN_AGGREGATION="1", RUN_QUERY="0",
                       FIG6_MODES="samples", FIG6_KEYS="256")
            result = subprocess.run(
                ["bash", str(root / "scripts/eval/run_zkvm_dev_mode.sh")],
                env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertFalse((root / "results/zkvm_dev_aggregation.csv").exists())


if __name__ == "__main__":
    unittest.main()
