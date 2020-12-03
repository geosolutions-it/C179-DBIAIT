import unittest
from unittest import skip

from django.test import SimpleTestCase

from app.scheduler.tasks.export_definitions.validations import ValidationFactory


class ValidationTestCase(SimpleTestCase):
    def setUp(self) -> None:
        self.validate = ValidationFactory

    def test_given_transformation_name_IF_AND_with_true_cond_should_return_the_expected_output(self):
        condition_schema = {
            "cond": {
                "and": [
                    {"operator": "<", "value": "2"}
                ]
            }
        }
        actual = self.validate.from_name("IF", condition_schema).validate("1")
        self.assertTrue(actual)

    def test_given_transformation_name_IF_AND_with_false_cond_should_return_the_expected_output(self):
        condition_schema = {
            "cond": {
                "and": [
                    {"operator": "<", "value": "2"},
                    {"operator": ">", "value": "10"},
                ]
            }
        }
        actual = self.validate.from_name("IF", condition_schema).validate("1")
        self.assertFalse(actual)

    def test_given_transformation_name_IF_OR_with_true_cond_should_return_the_expected_output(self):
        condition_schema = {
            "cond": {
                "or": [
                    {"operator": "<", "value": "2"},
                    {"operator": ">", "value": "0"},
                ]
            }
        }
        actual = self.validate.from_name("IF", condition_schema).validate("1")
        self.assertTrue(actual)

    def test_given_transformation_name_IF_OR_with_false_cond_should_return_the_expected_output(self):
        condition_schema = {
            "cond": {
                "or": [
                    {"operator": ">", "value": "2"},
                    {"operator": ">", "value": "10"},
                ]
            }
        }
        actual = self.validate.from_name("IF", condition_schema).validate("1")
        self.assertFalse(actual)


if __name__ == "__main__":
    unittest.main()
