import unittest
import pandas
import pytest
import yaml
import os
import json


from src.SQL_Operations import SQL_Operations


class SqlOperationsTest(unittest.TestCase):
    def setUp(self):
        with open("./src/tests/test_files/test_config.yml", "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        try:
            self.token_auth = config["AUTH"]["token_auth_header"]
            self.sql_query = config["MYSQL_CONNECTION_DETAILS"]["SQL_QUERY"]
            self.environment = config["ENVIRONMENT"]["gore"]
            self.admin_only_domain_id = config["COLLIBRA_DETAILS"]["ADMIN_DOMAIN_ID"]
            self.token_auth = config["AUTH"]["token_auth_header"]
        except KeyError:
            print("The config file is incorrectly setup")
            os._exit(1)

        self.sql_operations = SQL_Operations(
            "test",
            "test",
            "test",
            "test",
            self.token_auth,
            self.sql_query,
            self.admin_only_domain_id,
            self.environment,
        )
        self.test_dataframe = pandas.read_csv("src/tests/test_files/test.csv")
        self.small_test_df = pandas.read_csv("src/tests/test_files/small_test.csv")
        self.empty_test_df = pandas.DataFrame()

