import unittest
from unittest import skip

from django.test import SimpleTestCase

from app.scheduler.tasks.export_definitions.validations import ValidationFactory


class ValidationTestCase(SimpleTestCase):
    def setUp(self) -> None:
        self.validate = ValidationFactory
        self.field = {"foo_field": 1, "bar_field": 12}

    def test_potabilizzatori_31800(self,):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                    "and": [
                      {"operator": ">", "value": "{REF_YEAR}"},
                      {"operator":  "=", "value": 9800}
                    ]
                }, {
                    "and": [
                      {"operator": "<", "value": "{REF_YEAR}"},
                      {"operator":  "!=", "value": 9800}
                    ]
                }],
        }
        self.field = {"foo_field": 9800}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)

    def test_potabilizzatori_32020(self,):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                        "and": [
                          {"lookup": "{32010}", "operator": ">=", "value": 2014},
                          {"operator": ">=", "value": 3}
                        ]
                    }, {
                        "and": [
                          {"lookup": "{32010}", "operator": "<", "value": 2014}
                        ]
                    }],
        }
        self.field = {"foo_field": 2, '32010': 2013}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)

    def test_potabilizzatori_32200(self,):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                        "and": [
                          {"lookup": "{35500}", "operator": "=", "value": 1},
                          {"operator": ">", "value": 0}
                        ]
                    }, {
                        "and": [
                          {"lookup": "{35500}", "operator": "!=", "value": 1}
                        ]
                    }],
        }
        self.field = {"foo_field": 0, '35500': 12}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)

    def test_potabilizzatori_35600(self,):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                    "and": [
                      {"lookup": "{31800}", "operator": "=", "value": 9999},
                      {"operator": "=", "value": "X"}
                    ]
                },{
                    "and": [
                      {"lookup": "{31800}", "operator": "!=", "value": 9999}
                    ]
                }
            ],
        }
        self.field = {"foo_field": "X", '31800': 9998}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)

    def test_potabilizzatori_35900(self,):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                    "and": [
                      {"lookup": "{32200}", "operator": "=", "value": 0},
                      {"operator": "=", "value": "A"}
                    ]
                }, {
                    "and": [
                      {"lookup": "{32200}", "operator": ">", "value": 0},
                      {"lookup": "{98400}", "operator": "=", "value": 1},
                      {"operator": "=", "value": "A"}
                    ]
                }, {
                    "and": [
                      {"lookup": "{32200}", "operator": ">", "value": 0},
                      {"lookup": "{98400}", "operator": "=", "value": 0},
                      {"operator": "=", "value": "C"}
                    ]
                }
            ],
        }
        self.field = {"foo_field": "C", '32200': 1, '98400': 0}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)

    def test_pozzi_15500(self,):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                      "and": [
                        {"operator": "!=", "value": ""}
                      ]
                  }
            ],
        }
        self.field = {"foo_field": "s"}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)

    def test_pozzi_15600(self,):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                      "and": [
                        {"operator": ">=", "value": "{17200}"}
                      ]
                  }, {
                      "and": [
                        {"operator": "!=", "value": ""}
                      ]
                  }
            ],
        }
        self.field = {"foo_field": 1, "17200": 0}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)

    def test_pozzi_18100(self,):
        condition_schema = {
            "field": "foo_field",
            "cond": [{
                      "and": [
                        {"lookup": "{18200}", "operator": "!=", "value": 9800},
                        {"operator": "!=", "value": 1}
                      ]
                  }
            ],
        }
        self.field = {"foo_field": 2, "18200": 0}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertTrue(actual)
        self.field = {"foo_field": 1, "18200": 9800}
        actual = self.validate.from_name("IF", condition_schema).validate(self.field, 2020)
        self.assertFalse(actual)


if __name__ == "__main__":
    unittest.main()
