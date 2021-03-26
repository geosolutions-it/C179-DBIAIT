import unittest
from unittest.mock import patch

from django.test import SimpleTestCase
from schema import SchemaError

from app.scheduler.tasks.export_definitions.domains_parser import Domains
from app.scheduler.tasks.export_definitions.exceptions import ExportConfigError
from app.scheduler.tasks.export_definitions.transformations import TransformationFactory


class TransformationTestCase(SimpleTestCase):
    def setUp(self) -> None:
        self.sut = TransformationFactory

    def test_given_transformation_name_EMPTY_should_return_the_expected_output(self):
        actual = self.sut.from_name('EMPTY', {}).apply()
        expected = ""
        self.assertEqual(expected, actual)

    def test_given_transformation_name_CONST_should_return_the_expected_output(self):
        actual = self.sut.from_name('CONST', {"value": "foo"}).apply()
        expected = "foo"
        self.assertEqual(expected, actual)

    def test_given_transformation_name_DIRECT_should_return_the_expected_output(self):
        actual = self.sut.from_name('DIRECT', {"field": "foo"}).apply({"foo": 123})
        self.assertEqual(123, actual)

    @patch("app.scheduler.tasks.export_definitions.domains_parser.Domains.fetch_domains")
    def test_given_transformation_name_DOMAIN_should_return_the_expected_output(self, mocked_fetch_domains):
        mocked_fetch_domains.return_value = ""
        mocked_domains = Domains()
        mocked_domains._data = {
                "D_T_CLORAZ": {
                    "CLO": 2,
                    "ALT": 5,
                    "NES": 1,
                    "BIO": 4,
                    "IPO": 3,
                    "MIS": 5
                }
        }
        actual = self.sut.from_name('DOMAIN', {"field": "d_tipo_cloraz", "domain_name": "D_T_CLORAZ"})\
            .apply(row={"d_tipo_cloraz": "CLO"}, domains=mocked_domains)
        self.assertEqual(2, actual)

    def test_given_transformation_name_LSTRIP_should_return_the_expected_output(self):
        actual = self.sut.from_name('LSTRIP', {"field": "cod_comune", "char": "0"}) \
            .apply(row={"cod_comune": "0Foo"})
        self.assertEqual("Foo", actual)

    def test_given_transformation_name_EXPR_should_return_the_expected_output(self):
        actual = self.sut.from_name('EXPR', {"field": "volume_medio_prel", "expr": "_value_*1000/365/3600/24"})\
            .apply(row={"volume_medio_prel": 1})
        x = 1*1000/365/3600/24
        self.assertEqual(x, actual)

    def test_given_transformation_name_IF_gt_should_return_the_expected_output(self):
        if_condition = {
            "field": "foo",
            "cond": {
                "operator": ">",
                "value": 0,
                "result": 2,
                "else": 1
            }
        }
        actual = self.sut.from_name('IF', if_condition).apply(row={"foo": 1})
        self.assertEqual(2, actual)

    def test_given_transformation_name_ROUND_should_return_the_expected_output(self):
        if_condition = {
            "field": "foo",
            "ndigits": 3
        }
        actual = self.sut.from_name('ROUND', if_condition).apply(row={"foo": 1.2})
        self.assertEqual(1.2, actual)

    def test_given_transformation_name_ROUND_should_return_the_expected_output_with_truncated_digits(self):
        if_condition = {
            "field": "foo",
            "ndigits": 3
        }
        actual = self.sut.from_name('ROUND', if_condition).apply(row={"foo": 1.22345678})
        self.assertEqual(1.223, actual)

    def test_given_transformation_name_IF_ne_should_return_the_expected_output(self):
        if_condition = {
            "field": "foo",
            "cond": {
                "operator": "!=",
                "value": 0,
                "result": 2,
                "else": 1
            }
        }
        actual = self.sut.from_name('IF', if_condition).apply(row={"foo": 0})
        self.assertEqual(1, actual)

    def test_given_transformation_name_CASE_should_return_the_expected_output(self):
        case_option = {"field": "bar_field", "cond": [
             {"case": "WHEN", "operator": "=", "value": "1", "result": "X"},
             {"case": "WHEN", "operator": "!=", "value": "1", "result": "A"}
         ]}

        actual = self.sut.from_name('CASE', case_option).apply({"bar_field": "1"})
        self.assertEqual("X", actual)

        actual = self.sut.from_name('CASE', case_option).apply({"bar_field": "2"})
        self.assertEqual("A", actual)

    def test_given_invalid_transformation_should_raise_ExportConfigError(self):
        with self.assertRaises(ExportConfigError):
            self.sut.from_name('NOT_EXISTING_CONFIG', {}).apply()

    def test_given_transformation_name_IF_gt_should_return_999_with_None_value(self):
        if_condition = {
            "field": "anno",
            "cond": {
                "operator": "=",
                "value": "",
                "result": "9999",
                "else": "{anno}"
            }
        }
        actual = self.sut.from_name('IF', if_condition).apply(row={"anno": None})
        self.assertEqual(9999, actual)


class TransformationSchemaErrors(SimpleTestCase):
    def setUp(self) -> None:
        self.sut = TransformationFactory

    def test_given_invalid_CONST_schema_should_rase_exception(self):
        with self.assertRaises(SchemaError):
            self.sut.from_name('CONST', {"foo": "bar"}).apply()

    def test_given_invalid_DIRECT_schema_should_rase_exception(self):
        with self.assertRaises(SchemaError):
            self.sut.from_name('DIRECT', {"foo": "bar"}).apply()

    def test_given_invalid_DOMAIN_schema_should_rase_exception(self):
        with self.assertRaises(SchemaError):
            self.sut.from_name('DOMAIN', {"foo": "bar"}).apply()

    def test_given_invalid_EXPR_schema_should_rase_exception(self):
        with self.assertRaises(SchemaError):
            self.sut.from_name('EXPR', {"foo": "bar"}).apply()

    def test_given_invalid_IF_schema_should_rase_exception(self):
        with self.assertRaises(SchemaError):
            self.sut.from_name('IF', {"foo": "bar"}).apply()

    def test_given_invalid_CASE_schema_should_rase_exception(self):
        with self.assertRaises(SchemaError):
            self.sut.from_name('CASE', {"foo": "bar"}).apply()


if __name__ == '__main__':
    unittest.main()
