import unittest
import pandas
import pytest
import yaml
from src.main import MainClass


class MainClassTest(unittest.TestCase):
    def setUp(self):
        main = MainClass("./src/tests/test_files/test_config.yml")
        print("Starting")

    def test_setup(self):
        print("Setup")
    def test_sql_format(self):
        with open("./src/tests/test_files/test_config.yml", "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        self.admin_only_domain_id = config["COLLIBRA_DETAILS"]["ADMIN_DOMAIN_ID"]
        self.systems_domain_id = config["COLLIBRA_DETAILS"]["Systems_Domain_ID"]
        self.update_sql_query = config["MYSQL_CONNECTION_DETAILS"][
            "UPDATE_SQL_QUERY"
        ].format(self.admin_only_domain_id, self.systems_domain_id)
        print(self.update_sql_query)
