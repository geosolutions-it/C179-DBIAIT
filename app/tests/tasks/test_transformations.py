import unittest
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase
from app.scheduler.tasks.export_definitions.domains_parser import Domains
from app.scheduler.tasks.export_definitions.transformations import TransformationFactory, EmptyTransformation


class TransformationTestCase(SimpleTestCase):
    def setUp(self) -> None:
        self.sut = TransformationFactory

    def test_given_transformation_name_EMPTY_should_return_the_expected_output(self):
        actual = self.sut.from_name('EMPTY', {}).apply()
        expected = ""
        self.assertEqual(expected, actual)

    def test_given_transformation_name_CONST_should_return_the_expected_output(self):
        actual = self.sut.from_name('CONST', {"value": "my_field"}).apply()
        expected = "my_field"
        self.assertEqual(expected, actual)

    def test_given_transformation_name_DIRECT_should_return_the_expected_output(self):
        actual = self.sut.from_name('DIRECT', {"field": "my_field"}).apply({"my_field": 123})
        self.assertEqual(123, actual)

    @patch("app.scheduler.tasks.export_definitions.domains_parser.Domains.fetch_domains")
    def test_given_transformation_name_DOMAIN_should_return_the_expected_output(self, mocked_domain):
        mocked_domain.return_value = {
            "dominio_gis": {
                "valore_gis": "dominio_netsic"
            }
        }

        actual = self.sut.from_name('DOMAIN', {"field": "my_field", "domain_name": "valore_gis"})\
            .apply(row={"my_field": "dominio_netsic"}, domains=Domains)
        print(actual)
        expected = ""
        self.assertEqual(expected, actual)

    '''
    def test_given_transformation_name_LSTRIP_should_return_the_expected_output(self):
        actual = self.sut.from_name('LSTRIP', {"field": "my_field"}).apply()
        expected = "my_field"
        self.assertEqual(expected, actual)

    def test_given_transformation_name_EXPR_should_return_the_expected_output(self):
        actual = self.sut.from_name('EXPR', {"field": "my_field"}).apply()
        expected = "my_field"
        self.assertEqual(expected, actual)

    def test_given_transformation_name_CASE_should_return_the_expected_output(self):
        actual = self.sut.from_name('CASE', {"field": "my_field"}).apply()
        expected = "my_field"
        self.assertEqual(expected, actual)

    def test_given_transformation_name_IF_should_return_the_expected_output(self):
        actual = self.sut.from_name('IF', {"field": "my_field"}).apply()
        expected = "my_field"
        self.assertEqual(expected, actual)

    def test_given_invalid_transformation_should_raise_ExportConfigError(self):
        actual = self.sut.from_name('IF', {"field": "my_field"}).apply()
        expected = "my_field"
        self.assertEqual(expected, actual)
'''

if __name__ == '__main__':
    unittest.main()
