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

    def test_given_transformation_name_IF_AND_with_None_value_and_true_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{"and": [{"lookup": "{bar_field}", "operator": "!=", "value": 1}]}],
        }
        self.field = {"foo_field": 1, "bar_field": None}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_given_transformation_name_IF_AND_with_ref_netsic_value_and_true_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                    "and": [
                      {"lookup": "{48700}", "operator": ">=", "value": 2002},
                      {"lookup": "{48700}", "operator": "!=", "value": 9999},
                      {"operator": "=", "value": "A"}
                    ]
                }],
        }
        self.field = {"48700": "9998", "foo_field": "A"}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_given_transformation_name_IF_with_first_cond_true_for_ref_year_netsic_accumuli(
        self,
    ):
        condition_schema = {
                        "field": "44700",
                        "cond": [{
                            "and": [
                                {"lookup": "{44600}", "operator": ">=", "value": 2014},
                                {"operator": ">=", "value": 3}
                            ]
                        }, {
                            "and": [
                                {"lookup": "{44600}", "operator": "<", "value": 2014}
                            ]
                        }]
                    }
        self.field = {"44600": "9998", "44700": 4}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_given_transformation_name_IF_with_second_cond_true_for_ref_year_netsic_accumuli(
        self,
    ):
        condition_schema = {
                        "field": "44700",
                        "cond": [{
                            "and": [
                                {"lookup": "{44600}", "operator": ">=", "value": 2014},
                                {"operator": ">=", "value": 3}
                            ]
                        }, {
                            "and": [
                                {"lookup": "{44600}", "operator": "<", "value": 2014}
                            ]
                        }]
                    }
        self.field = {"44600": "2013", "44700": 4}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_given_transformation_name_IF_with_false_conditions_for_ref_year_netsic_accumuli(
        self,
    ):
        condition_schema = {
                        "field": "44700",
                        "cond": [{
                            "and": [
                                {"lookup": "{44600}", "operator": ">=", "value": 2014},
                                {"operator": ">=", "value": 3}
                            ]
                        }, {
                            "and": [
                                {"lookup": "{44600}", "operator": "<", "value": 2014}
                            ]
                        }]
                    }
        self.field = {"44600": "2015", "44700": 2}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_given_transformation_name_IF_AND_with_true_cond_and_not_matching_regex_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{"and": [{"lookup": "{bar_field}", "operator": "<", "value": 2}]}],
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_given_transformation_name_IF_AND_multiple_lookup_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{"and": [{"lookup": "{bar_field}", "operator": "!=", "value": "{foo_field}"}]}],
        }
        self.field = {"foo_field": 1, "bar_field": 2}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

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

    def test_given_transformation_name_IF_with_REF_YEAR_with_true_cond_should_return_the_expected_output(
        self,
    ):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                "and": [
                    {"operator": "<", "value": "{REF_YEAR}"},
                    {"operator": ">", "value": 0},
                ]
            }],
        }
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)

    def test_validation_adduttrici_40200_condition_1_case_ok_1(
        self,
    ):
        condition_schema = {
            "field": "40200",
            "cond": [
                {
                "and": [
                  {"lookup": "{40000}", "operator": "=", "value": 1},
                  {"operator": "=", "value": 9800}
                ]
            },{
               "and": [
                  {"lookup": "{40000}", "operator": "!=", "value": 1}
               ]
            }]
        }
        self.field = {"40000": 1, "40200": 9800}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_accumuli_40200_less_than_refyear(
        self,
    ):
        condition_schema = {
            "field": "44500",
            "cond": [{
                "or": [
                    {"operator": "<=", "value": 2020},
                    {"operator": "=", "value": 9999}
                ]
            }]
        }
        self.field = {"44500": 2019}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_accumuli_40200_great_than_refyear(
        self,
    ):
        condition_schema = {
            "field": "44500",
            "cond": [{
                "or": [
                    {"operator": "<=", "value": 2020},
                    {"operator": "=", "value": 9999}
                ]
            }]
        }
        self.field = {"44500": 2021}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_validation_accumuli_40200_case_9999(
        self,
    ):
        condition_schema = {
            "field": "44500",
            "cond": [{
                "or": [
                    {"operator": "<=", "value": 2020},
                    {"operator": "=", "value": 9999}
                ]
            }]
        }
        self.field = {"44500": 9999}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_accumuli_44700_case_ok(
        self,
    ):
        condition_schema = {
            "field": "44700",
            "cond": [{
                "and": [
                    {"lookup": "{44600}", "operator": ">=", "value": 2014},
                    {"operator": ">=", "value": 3}
                ]
            },{
                "and": [
                    {"lookup": "{44600}" , "operator": "<", "value": 2014}
                ]
            }]
        }
        self.field = {"44600": 2014, "44700": 3}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_accumuli_44700_case_ko(
        self,
    ):
        condition_schema = {
            "field": "44700",
            "cond": [{
                "and": [
                    {"lookup": "{44600}", "operator": ">=", "value": 2014},
                    {"operator": ">=", "value": 3}
                ]
            },{
                "and": [
                    {"lookup": "{44600}", "operator": "<", "value": 2014}
                ]
            }]
        }
        self.field = {"44600": 2014, "44700": 2}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_validation_accumuli_44700_case_less_than_2014(
        self,
    ):
        condition_schema = {
            "field": "44700",
            "cond": [{
                "and": [
                    {"lookup": "{44600}", "operator": ">=", "value": 2014},
                    {"operator": ">=", "value": 3}
                ]
            },{
                "and": [
                    {"lookup": "{44600}", "operator": "<", "value": 2014}
                ]
            }]
        }
        self.field = {"44600": 2013, "44700": 2}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_accumuli_45600_condition_1_ok(
        self,
    ):
        condition_schema = {
            "field": "45600",
            "cond": [{
                "and": [
                    {"lookup": "{45500}", "operator": "=", "value": 1},
                    {"operator": "=", "value": 9800}
                ]
            },{
                "and": [
                    {"lookup": "{45500}", "operator": "!=", "value": 1}
                ]
            }]
        }
        self.field = {"45500": 1, "45600": 9800}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_accumuli_45600_condition_1_ko(
        self,
    ):
        condition_schema = {
            "field": "45600",
            "cond": [{
                "and": [
                    {"lookup": "{45500}", "operator": "=", "value": 1},
                    {"operator": "=", "value": 9800}
                ]
            },{
                "and": [
                    {"lookup": "{45500}", "operator": "!=", "value": 1}
                ]
            }]
        }
        self.field = {"45500": 1, "45600": 1234}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_validation_accumuli_45600_condition_1_ok_value_is_not_one(
        self,
    ):
        condition_schema = {
            "field": "45600",
            "cond": [{
                "and": [
                    {"lookup": "{45500}", "operator": "=", "value": 1},
                    {"operator": "=", "value": 9800}
                ]
            },{
                "and": [
                    {"lookup": "{45500}", "operator": "!=", "value": 1}
                ]
            }]
        }
        self.field = {"45500": 2, "45600": 1234}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_accumuli_45900_condition_2_ok(
        self,
    ):
        condition_schema = {
            "field": "45900",
            "cond": [{
                "and": [
                    {"lookup": "{44500}", "operator": ">=", "value": 2002},
                    {"lookup": "{44500}", "operator": "!=", "value": 9999},
                    {"operator": "=", "value": "A"}
                ]}, {
                "or": [
                    {"lookup": "{44500}", "operator": "<", "value": 2002},
                    {"lookup": "{44500}", "operator": "=", "value": 9999}
                ]
            }]
        }
        self.field = {"44500": 2003, "45900": "A"}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

        self.field = {"44500": 9999, "45900": "B"}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

        self.field = {"44500": 2001, "45900": "B"}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_accumuli_45900_condition_2_ko(
        self,
    ):
        condition_schema = {
            "field": "45900",
            "cond": [{
                "and": [
                    {"lookup": "{44500}", "operator": ">=", "value": 2002},
                    {"lookup": "{44500}", "operator": "!=", "value": 9999},
                    {"operator": "=", "value": "A"}
                ]}, {
                "or": [
                    {"lookup": "{44500}", "operator": "<", "value": 2002},
                    {"lookup": "{44500}", "operator": "=", "value": 9999}
                ]
            }]
        }
        self.field = {"44500": 2002, "45900": "B"}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_validation_addut_tronchi_41500_ok(
        self,
    ):
        condition_schema = {
            "field": "41500",
            "cond": [{
                "or": [
                  {"operator": "<=", "value": 2021},
                  {"operator": "=", "value": 9999}
                ]
            }]
        }
        self.field = {"41500": 2002}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

        self.field = {"41500": 9999}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_addut_tronchi_41500_ko(
        self,
    ):
        condition_schema = {
            "field": "41500",
            "cond": [{
                "or": [
                  {"operator": "<=", "value": 2021},
                  {"operator": "=", "value": 9999}
                ]
            }]
        }
        self.field = {"41500": 2099}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_validation_fiumi_3500_ok(
        self,
    ):
        condition_schema = {
            "field": "3500",
            "cond": [{
                "and": [
                  {"operator": "<=", "value": "{2200}"}
                ]
            }]
        }
        self.field = {"3500": 1000, "2200": 2000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)
        self.field = {"3500": 2000, "2200": 2000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_fiumi_3500_ko(
        self,
    ):
        condition_schema = {
            "field": "3500",
            "cond": [{
                "and": [
                  {"operator": "<=", "value": "{2200}"}
                ]
            }]
        }
        self.field = {"3500": 3000, "2200": 2000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_validation_laghi_9200_ok(
        self,
    ):
        condition_schema = {
            "field": "9200",
            "cond": [{
                "and": [
                  {"operator": ">=", "value": "{10500}"}
                ]
            }]
        }
        self.field = {"9200": 3000, "10500": 2000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)
        self.field = {"9200": 3000, "10500": 3000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_laghi_9200_ko(
        self,
    ):
        condition_schema = {
            "field": "9200",
            "cond": [{
                "and": [
                  {"operator": ">=", "value": "{10500}"}
                ]
            }]
        }
        self.field = {"9200": 2000, "10500": 3000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_validation_laghi_10500_ok(
        self,
    ):
        condition_schema = {
            "field": "10500",
            "cond": [{
                "and": [
                  {"operator": "<=", "value": "{9200}"}
                ]
            }]
        }
        self.field = {"10500": 2000, "9200": 3000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)
        self.field = {"10500": 2000, "9200": 2000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_laghi_10500_ko(
        self,
    ):
        condition_schema = {
            "field": "10500",
            "cond": [{
                "and": [
                  {"operator": "<=", "value": "{9200}"}
                ]
            }]
        }
        self.field = {"10500": 3000, "9200": 1000}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)

    def test_validation_sorgenti_28000_ok(
        self,
    ):
        condition_schema = {
              "field": "28000",
              "cond": [{
                  "and": [
                    {"lookup": "{26300}", "operator": ">", "value": 0},
                    {"lookup": "{26600}", "operator": "=", "value": 0},
                    {"operator": "=", "value": "C"}
                  ]
              }, {
                  "or": [
                    {"lookup": "{26300}", "operator": "<=", "value": 0},
                    {"lookup": "{26600}", "operator": "!=", "value": 0}
                  ]
              }]
          }
        self.field = {"28000": "C", "26300": 1, "26600": 0}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)
        self.field = {"28000": "Z", "26300": 0, "26600": 0}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)
        self.field = {"28000": "Z", "26300": 1, "26600": 1}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertTrue(actual)

    def test_validation_sorgenti_28000_ko(
        self,
    ):
        condition_schema = {
              "field": "28000",
              "cond": [{
                  "and": [
                    {"lookup": "{26300}", "operator": ">", "value": 0},
                    {"lookup": "{26600}", "operator": "=", "value": 0},
                    {"operator": "=", "value": "C"}
                  ]
              }, {
                  "or": [
                    {"lookup": "{26300}", "operator": "<=", "value": 0},
                    {"lookup": "{26600}", "operator": "!=", "value": 0}
                  ]
              }]
          }
        self.field = {"28000": "X", "26300": 1, "26600": 0}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field)
        self.assertFalse(actual)


if __name__ == "__main__":
    unittest.main()
