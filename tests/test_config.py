from config import Config
from jsonschema.exceptions import ValidationError
import pytest

class TestConfig:
    def test_valid__error(self):
        with pytest.raises(ValidationError):
            invalid_conf = {
                    "init": {
                        "service": "bigquery",
                     },
                    "in": {
                        "query": "select * from Test.users",
                    },
            }
            conf = Config(invalid_conf)
            conf.valid()

    def test_valid__success(self):
            valid_conf = {
                    "init": {
                        "service": "bigquery",
                     },
                    "in": {
                        "query": "select * from Test.users",
                    },
                    "out": {
                        "project": "project",
                        "database": "database",
                        "table": "table",
                        "mode": "replace",
                    },
            }
            conf = Config(valid_conf)
            conf.valid()
