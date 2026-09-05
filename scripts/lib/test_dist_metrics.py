"""Regression checks for rejecting incomplete distributed runs (no cluster)."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class MetricsTests(unittest.TestCase):
    def run_fixture(self, *, missing_node=False, bad_count=False, bad_query=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for i in range(2):
                if not (missing_node and i == 1):
                    (root / f"agg_{i}.log").write_text(
                        "[e2e-timing] epoch_total_ms=10 prove_ms=8 verify_ms=1\n")
                (root / f"consumer_{i}.log").write_text(
                    "[kafka-consumer] ingested batches=1 events=8 pending_sources=0\n")
            (root / "querier.log").write_text(
                "bench query=histogram_p90 db_ms=1 merge_ms=2\n"
                "bench kind=histogram_p90 prove_ms=4 verify_ms=1\n")
            for attempt in (1, 2, 3):
                (root / f"query_response_{attempt}.json").write_text(json.dumps(
                    {"error": "failed"} if bad_query else {"proof": {}, "total_count": 16}))
            metrics = root / "metrics.jsonl"
            result = subprocess.run([
                "python3", str(Path(__file__).with_name("_parse_dist_cell.py")),
                "--dataset", "vehicle", "--agg-type", "histogram", "--mode", "zk",
                "--workdir", directory, "--logdir", directory, "--metrics", str(metrics),
                "--epoch-size", "8", "--num-aggregators", "2",
                "--expected-logs", "17" if bad_count else "16",
            ], env={**os.environ, "RISC0_DEV_MODE": "1"}, capture_output=True, text=True)
            records = [json.loads(x) for x in metrics.read_text().splitlines()] if metrics.exists() else []
            return result, records

    def test_valid_dev_run(self):
        result, records = self.run_fixture()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(records[0]["epochs_per_node"], {"0": 1, "1": 1})
        self.assertEqual(records[0]["ingested_logs"], 16)
        self.assertEqual(records[0]["components_s"]["verify"], 0)

    def test_missing_node(self):
        result, records = self.run_fixture(missing_node=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(records, [])

    def test_missing_input(self):
        result, records = self.run_fixture(bad_count=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(records, [])

    def test_query_error(self):
        result, records = self.run_fixture(bad_query=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(records, [])


if __name__ == "__main__":
    unittest.main()
