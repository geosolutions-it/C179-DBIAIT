import unittest

from django.test import SimpleTestCase

from app.scheduler.tasks.export_definitions.config_scraper import ExportConfig
from app.tests.tasks.query_utils import *

all_sql_query = ExportConfig().config


class ExportConfigTest(SimpleTestCase):
    def setUp(self) -> None:
        self.maxDiff = None

    def test_accumuli_queries_should_be_the_expected_one(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Accumuli"
        ]
        # expect to have 3 queries
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_a_accumuli_query, actual[0][0])
        self.assertMultiLineEqual(expected_accumuli_query, actual[0][1])
        self.assertMultiLineEqual(expected_accumuli_spatial_query, actual[0][2])

    def test_addut_tronchi_queries_should_be_the_expected_one(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Addut_tronchi"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_addut_tronchi_condotta_query, actual[0][0])
        self.assertMultiLineEqual(expected_addut_tronchi_query, actual[0][1])

    def test_adduttrici_queries_should_be_the_expected_one(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Adduttrici"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_a_adduttrici_query, actual[0][0])
        self.assertMultiLineEqual(expected_adduttrici_query, actual[0][1])

    def test_collett_tronchi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Collett_tronchi"
        ]
        # expect to have 3 queries
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_collett_tronchi_query, actual[0][0])
        self.assertMultiLineEqual(expected_collett_tronchi_condotta_query, actual[0][1])
        self.assertMultiLineEqual(expected_collett_tronchi_fogna_query, actual[0][2])

    def test_collettori_tronchi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Collettori"
        ]
        # expect to have 3 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_a_fgn_collettori, actual[0][0])
        self.assertMultiLineEqual(expected_fgn_collettori, actual[0][1])

    def test_depurato_pompe_query_should_be_the_expected_one(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Depurato_pompe"
        ]
        # expect to have 1 query
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_depurato_pompe_query, actual[0][0])

    def test_depuratori_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Depuratori"
        ]
        # expect to have 3 queries
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_a_depuratori_query, actual[0][0])
        self.assertMultiLineEqual(expected_depuratori_query, actual[0][1])
        self.assertMultiLineEqual(expected_depuratori_spatial_query, actual[0][2])

    def test_distrib_tronchi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Distrib_tronchi"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_distrib_trochi_acq_condotta, actual[0][0])
        self.assertMultiLineEqual(expected_distrib_tronchi, actual[0][1])

    def test_distribuzioni_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Distribuzioni"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_a_distribuzioni_query, actual[0][0])
        self.assertMultiLineEqual(expected_distribuzioni_query, actual[0][1])

    def test_fiumi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Fiumi"
        ]
        # expect to have 7 queries
        self.assertEqual(7, len(actual[0]))
        self.assertMultiLineEqual(expected_a_fiumi_acq_capt_conces_query, actual[0][0])
        self.assertMultiLineEqual(expected_a_fiumi_query, actual[0][1])
        self.assertMultiLineEqual(expected_a_fiumi_spatial_query, actual[0][2])
        self.assertMultiLineEqual(expected_fiumi_cq_capt_conces_query, actual[0][3])
        self.assertMultiLineEqual(expected_fiumi_inpotab_query, actual[0][4])
        self.assertMultiLineEqual(expected_fiumi_query, actual[0][5])
        self.assertMultiLineEqual(expected_fiumi_spatial_query, actual[0][6])

    def test_fognat_com_serv_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Fognat_com_serv"
        ]
        # expect to have 1 queries
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_fognat_com_serv_query, actual[0][0])

    def test_fognat_loc_serv_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Fognat_loc_serv"
        ]
        # expect to have 1 queries
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_fognat_loc_serv_query, actual[0][0])

    def test_fognat_tronchi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Fognat_tronchi"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_fognat_tronchi_condotta_query, actual[0][0])
        self.assertMultiLineEqual(expected_fognat_tronchi_query, actual[0][1])

    def test_fognature_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in all_sql_query if x["sheet"] == "Fognature"
        ]
        # expect to have 3 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_a_fognature_query, actual[0][0])
        self.assertMultiLineEqual(expected_fognature_query, actual[0][1])


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
