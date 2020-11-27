import unittest

from django.test import SimpleTestCase

from app.scheduler.tasks.export_definitions.config_scraper import ExportConfig
from app.tests.tasks.query_utils import *


class ExportConfigTest(SimpleTestCase):
    def setUp(self) -> None:
        self.maxDiff = None
        self.all_sql_query = ExportConfig().config

    def test_accumuli_queries_should_be_the_expected_one(self):
        actual = [
            x["sql_sources"] for x in self.all_sql_query if x["sheet"] == "Accumuli"
        ]
        # expect to have 3 queries
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_accumuli_query, actual[0][0])
        self.assertMultiLineEqual(expected_a_accumuli_query, actual[0][1])
        self.assertMultiLineEqual(expected_accumuli_spatial_query, actual[0][2])

    def test_addut_tronchi_queries_should_be_the_expected_one(self):
        actual = [
            x["sql_sources"] for x in self.all_sql_query if x["sheet"] == "Addut_tronchi"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_addut_tronchi_query, actual[0][0])
        self.assertMultiLineEqual(expected_addut_tronchi_condotta_query, actual[0][1])

    def test_adduttrici_queries_should_be_the_expected_one(self):
        actual = [
            x["sql_sources"] for x in self.all_sql_query if x["sheet"] == "Adduttrici"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_addut_tronchi_query, actual[0][0])
        self.assertMultiLineEqual(expected_addut_tronchi_condotta_query, actual[0][1])

    """
    @patch("app.scheduler.tasks.export_definitions.export_xls.Domains")
    def test_export_configuration_should_return_the_expected_query_strings(self, mocked_domain):
        mocked_domain._data = {
            "D_T_CLORAZ": {
"CLO": 2,
"ALT": 5,
"NES": 1,
"BIO": 4,
"IPO": 3,
"MIS": 5
            }
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_export_directory = pathlib.Path(tmp_dir)

            x = ExportXls(export_dir=tmp_export_directory, orm_task=self.task).run()
            print(x)
        self.assertEqual(True, False)
"""


if __name__ == "__main__":
    unittest.main()
