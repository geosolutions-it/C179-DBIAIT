import unittest
from unittest import skip

from django.test import SimpleTestCase

from app.scheduler.tasks.export_definitions.validations import ValidationFactory


class ValidationTestCase(SimpleTestCase):
    def setUp(self) -> None:
        self.validate = ValidationFactory
        self.field = {"foo_field": 1, "bar_field": 12}

    def test_given_transformation_name_IF_AND_with_true_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{"and": [{"operator": "<", "value": 2}]}]
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_given_transformation_name_IF_AND_with_true_cond_and_not_matching_regex_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{"and": [{"lookup": "{bar_field}", "operator": "<", "value": 2}]}],
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_given_transformation_name_IF_AND_with_false_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                "and": [
                    {"operator": "<", "value": 2},
                    {"operator": ">", "value": 10},
                ]
            }],
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_given_transformation_name_IF_OR_with_true_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                "or": [
                    {"operator": "<", "value": 2},
                    {"operator": ">", "value": 0},
                ]
            }],
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_given_transformation_name_IF_OR_with_false_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                "or": [
                    {"operator": ">", "value": 2},
                    {"operator": ">", "value": 10},
                ]
            }],
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_given_transformation_name_IF_AND_OR_with_false_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                "or": [
                    {"operator": ">", "value": 2},
                    {"operator": ">", "value": 10},
                ],
                "and": [
                    {"operator": "<", "value": 2},
                    {"operator": ">", "value": 10},
                ],
            }],
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_given_transformation_name_IF_AND_OR_with_true_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                "or": [
                    {"operator": "=", "value": 1},
                    {"operator": "<", "value": 100},
                ],
                "and": [
                    {"operator": "<", "value": 10},
                    {"operator": ">", "value": 0},
                ],
            }],
        }
        self.field = {"foo_field": 5, "bar_field": 12}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_given_transformation_name_IF_AND_with_true_multiple_cond_and_not_matching_regex_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{"and": [{"operator": "<", "value": 2}]}, {"and": [{"operator": "<", "value": 5}]}],
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_given_transformation_name_IF_AND_with_false_multiple_cond_and_not_matching_regex_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{"and": [{"operator": "<", "value": 2}]}, {"and": [{"operator": "<", "value": 5}]}],
        }
        self.field = {"foo_field": 100}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)


if __name__ == "__main__":
    unittest.main()
