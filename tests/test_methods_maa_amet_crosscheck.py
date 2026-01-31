import unittest


class TestMaaAmetCrosscheckShim(unittest.TestCase):
    def test_dependency_record_matches_dt_contract(self) -> None:
        from eudr_dmi.methods import maa_amet_crosscheck

        record = dict(maa_amet_crosscheck.get_dependency_source_record())

        self.assertEqual(record["id"], "maa-amet/forest/v1")
        self.assertEqual(record["url"], "https://gsavalik.envir.ee/geoserver/wfs")
        self.assertEqual(record["expected_content_type"], "application/xml")
        self.assertEqual(
            record["server_audit_path"],
            "/Users/server/audit/eudr_dmi/dependencies/maa_amet_forest_v1",
        )

    def test_crosscheck_returns_stable_shape(self) -> None:
        from eudr_dmi.methods.maa_amet_crosscheck import BBox, crosscheck_forest_area

        out = crosscheck_forest_area(
            bbox=BBox(min_lon=24.95, min_lat=58.55, max_lon=25.05, max_lat=58.65),
            observed_forest_area_m2=1234.0,
            tolerance_ratio=0.05,
        )

        self.assertIn("dependency", out)
        self.assertIn("comparison", out)
        self.assertIn("params", out)
        self.assertEqual(out["dependency"]["id"], "maa-amet/forest/v1")


if __name__ == "__main__":
    unittest.main()
