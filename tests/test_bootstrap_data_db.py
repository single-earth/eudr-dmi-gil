import tempfile
import unittest
from pathlib import Path

import importlib.util


class TestBootstrapDataDb(unittest.TestCase):
    def test_bootstrap_from_csv_seeds_creates_tables(self) -> None:
        try:
            import duckdb  # type: ignore
        except Exception as e:  # pragma: no cover
            self.skipTest(f"duckdb not installed: {e}")

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            data_dir = repo_root / "data_db"
            data_dir.mkdir(parents=True, exist_ok=True)

            # Minimal seed CSVs (header-only is acceptable)
            (data_dir / "dataset_catalogue_auto.csv").write_text(
                "dataset_id,name\n",
                encoding="utf-8",
            )
            (data_dir / "dataset_families_summary.csv").write_text(
                "dataset_id,family\n",
                encoding="utf-8",
            )

            # Load the real bootstrap script (scripts/ is not a Python package).
            bootstrap_path = (
                Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_data_db.py"
            )
            spec = importlib.util.spec_from_file_location(
                "bootstrap_data_db", str(bootstrap_path)
            )
            assert spec is not None and spec.loader is not None
            bootstrap = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(bootstrap)

            bootstrap.repo_root = lambda: repo_root  # type: ignore[assignment]

            rc = bootstrap.main(
                [
                    "--data-dir",
                    "data_db",
                    "--db-path",
                    "data_db/test_geodata_catalogue.duckdb",
                ]
            )
            self.assertEqual(rc, 0)

            db_path = data_dir / "test_geodata_catalogue.duckdb"
            self.assertTrue(db_path.exists())

            con = duckdb.connect(str(db_path))
            try:
                tables = {
                    r[0]
                    for r in con.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                    ).fetchall()
                }
            finally:
                con.close()

            self.assertIn("dataset_catalogue_auto", tables)
            self.assertIn("dataset_families_summary", tables)
            # Joined table should exist because both seeds include dataset_id.
            self.assertIn("dataset_catalogue_with_families", tables)
