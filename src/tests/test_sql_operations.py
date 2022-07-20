import unittest
import pandas
import pytest
import yaml
import os
import json


from src.SQLOperations import SQLOperations


class SqlOperationsTest(unittest.TestCase):
    def setUp(self):
        with open("./src/tests/test_files/test_config.yml", "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        try:
            self.token_auth = config["AUTH"]["token_auth_header"]
            self.create_sql_query = config["MYSQL_CONNECTION_DETAILS"][
                "CREATE_SQL_QUERY"
            ].format("hello")
            self.create_sql_query = config["MYSQL_CONNECTION_DETAILS"][
                "CREATE_SQL_QUERY"
            ].format(config["COLLIBRA_DETAILS"]["ADMIN_DOMAIN_ID"])
            self.update_sql_query = config["MYSQL_CONNECTION_DETAILS"][
                "UPDATE_SQL_QUERY"
            ]
            self.environment = config["ENVIRONMENT"]["gore"]
            self.admin_only_domain_id = config["COLLIBRA_DETAILS"]["ADMIN_DOMAIN_ID"]
            self.token_auth = config["AUTH"]["token_auth_header"]
        except KeyError as e:
            print("The test config file is incorrectly setup: " + str(e))
            os._exit(1)

        self.test_dataframe = pandas.read_csv("src/tests/test_files/test.csv")
        self.small_test_df = pandas.read_csv("src/tests/test_files/small_test.csv")
        self.empty_test_df = pandas.DataFrame()

    def test_sql_format(self):
        print("setup")
        print(self.create_sql_query)
