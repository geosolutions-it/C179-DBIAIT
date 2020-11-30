import unittest

from django.test import SimpleTestCase

from app.scheduler.tasks.export_definitions.config_scraper import ExportConfig
from app.tests.tasks.query_utils import *


class ExportConfigTest(SimpleTestCase):
    all_sql_query = ExportConfig().config

    def setUp(self) -> None:
        self.maxDiff = None

    def test_accumuli_queries_should_be_the_expected_one(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Accumuli"
        ]
        # expect to have 3 queries
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_a_accumuli_query, actual[0][0])
        self.assertMultiLineEqual(expected_accumuli_query, actual[0][1])
        self.assertMultiLineEqual(expected_accumuli_spatial_query, actual[0][2])

    def test_addut_tronchi_queries_should_be_the_expected_one(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Addut_tronchi"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_addut_tronchi_condotta_query, actual[0][0])
        self.assertMultiLineEqual(expected_addut_tronchi_query, actual[0][1])

    def test_adduttrici_queries_should_be_the_expected_one(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Adduttrici"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_a_adduttrici_query, actual[0][0])
        self.assertMultiLineEqual(expected_adduttrici_query, actual[0][1])

    def test_collett_tronchi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Collett_tronchi"
        ]
        # expect to have 3 queries
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_collett_tronchi_query, actual[0][0])
        self.assertMultiLineEqual(expected_collett_tronchi_condotta_query, actual[0][1])
        self.assertMultiLineEqual(expected_collett_tronchi_fogna_query, actual[0][2])

    def test_collettori_tronchi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Collettori"
        ]
        # expect to have 3 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_a_fgn_collettori, actual[0][0])
        self.assertMultiLineEqual(expected_fgn_collettori, actual[0][1])

    def test_depurato_pompe_query_should_be_the_expected_one(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Depurato_pompe"
        ]
        # expect to have 1 query
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_depurato_pompe_query, actual[0][0])

    def test_depuratori_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Depuratori"
        ]
        # expect to have 3 queries
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_a_depuratori_query, actual[0][0])
        self.assertMultiLineEqual(expected_depuratori_query, actual[0][1])
        self.assertMultiLineEqual(expected_depuratori_spatial_query, actual[0][2])

    def test_distrib_tronchi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Distrib_tronchi"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_distrib_trochi_acq_condotta, actual[0][0])
        self.assertMultiLineEqual(expected_distrib_tronchi, actual[0][1])

    def test_distribuzioni_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Distribuzioni"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_a_distribuzioni_query, actual[0][0])
        self.assertMultiLineEqual(expected_distribuzioni_query, actual[0][1])

    def test_fiumi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Fiumi"
        ]
        # expect to have 7 queries
        self.assertEqual(4, len(actual[0]))
        self.assertMultiLineEqual(expected_a_fiumi_query, actual[0][0])
        self.assertMultiLineEqual(expected_fiumi_query, actual[0][1])
        self.assertMultiLineEqual(expected_fiumi_inpotab_query, actual[0][2])
        self.assertMultiLineEqual(expected_fiumi_spatial_query, actual[0][3])

    def test_fognat_com_serv_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Fognat_com_serv"
        ]
        # expect to have 1 queries
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_fognat_com_serv_query, actual[0][0])

    def test_fognat_loc_serv_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Fognat_loc_serv"
        ]
        # expect to have 1 queries
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_fognat_loc_serv_query, actual[0][0])

    def test_fognat_tronchi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Fognat_tronchi"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_fognat_tronchi_condotta_query, actual[0][0])
        self.assertMultiLineEqual(expected_fognat_tronchi_query, actual[0][1])

    def test_fognature_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Fognature"
        ]
        # expect to have 2 queries
        self.assertEqual(2, len(actual[0]))
        self.assertMultiLineEqual(expected_a_fognature_query, actual[0][0])
        self.assertMultiLineEqual(expected_fognature_query, actual[0][1])

    def test_laghi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Laghi"
        ]
        # expect to have 4 queries
        self.assertEqual(4, len(actual[0]))
        self.assertMultiLineEqual(expected_a_laghi_query, actual[0][0])
        self.assertMultiLineEqual(expected_laghi_query, actual[0][1])
        self.assertMultiLineEqual(expected_laghi_inpotab_query, actual[0][2])
        self.assertMultiLineEqual(expected_laghi_spatial_query, actual[0][3])

    def test_pompaggi_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Pompaggi"
        ]
        # expect to have 2 queries
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_a_pompaggi_query, actual[0][0])
        self.assertMultiLineEqual(expected_pompaggi_query, actual[0][1])

    def test_pompaggi_pompe_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Pompaggi_pompe"
        ]
        # expect to have 1 query
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_pompaggi_pompe_query, actual[0][0])

    def test_potabilizzatori_pompe_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Potabilizzatori"
        ]
        # expect to have 3 query
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_a_potabilizzatori_query, actual[0][0])
        self.assertMultiLineEqual(expected_potabilizzatori_query, actual[0][1])
        self.assertMultiLineEqual(expected_potabilizzatori_spatial_query, actual[0][2])

    def test_pozzi_pompe_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Pozzi_pompe"
        ]
        # expect to have 1 query
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_pozzi_pompe_query, actual[0][0])

    def test_scaricatori_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Scaricatori"
        ]
        # expect to have 3 query
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_a_scaricatori_query, actual[0][0])
        self.assertMultiLineEqual(expected_scaricatori_query, actual[0][1])
        self.assertMultiLineEqual(expected_scaricatori_spatial, actual[0][2])

    def test_sollev_pompe_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Sollev_pompe"
        ]
        # expect to have 1 query
        self.assertEqual(1, len(actual[0]))
        self.assertMultiLineEqual(expected_sollev_pompe_query, actual[0][0])

    def test_sollevamenti_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Sollevamenti"
        ]
        # expect to have 3 query
        self.assertEqual(3, len(actual[0]))
        self.assertMultiLineEqual(expected_a_sollevamenti_query, actual[0][0])
        self.assertMultiLineEqual(expected_sollevamenti_query, actual[0][1])
        self.assertMultiLineEqual(expected_sollevamenti_spatial_query, actual[0][2])

    def test_sorgenti_queries_should_be_the_expected_ones(self):
        actual = [
            sorted(x["sql_sources"]) for x in self.all_sql_query if x["sheet"] == "Sorgenti"
        ]
        # expect to have 4 query
        self.assertEqual(4, len(actual[0]))
        self.assertMultiLineEqual(expected_a_sorgenti_query, actual[0][0])
        self.assertMultiLineEqual(expected_sorgenti_query, actual[0][1])
        self.assertMultiLineEqual(expected_sorgenti_inpotab_query, actual[0][2])
        self.assertMultiLineEqual(expected_sorgenti_spatial_query, actual[0][3])


if __name__ == "__main__":
    unittest.main()
