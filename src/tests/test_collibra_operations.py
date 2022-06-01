import unittest
import pandas
import pytest
import yaml
import os
import json


from src.SQLOperations import SQLOperations
from src.Collibra_Operations import Collibra_Operations


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

        self.sql_operations = SQLOperations(
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
        self.ten_test_df = pandas.read_csv("src/tests/test_files/10_rows_test.csv")
        self.six_test = pandas.read_csv("src/tests/test_files/six_test.csv")
        self.empty_test_df = pandas.DataFrame()
        self.collibra_operations = Collibra_Operations(
            self.admin_only_domain_id, self.environment, self.token_auth, "./src/tests/test_files/test_config.yml"
        )
        self.test_2 = pandas.read_csv("src/tests/test_files/test_2.csv")

    def test_create_assets_and_attributes(self):
        self.collibra_operations.create_assets_and_attributes(self.six_test)

    def test_make_all_assets(self):
        self.collibra_operations.create_assets_and_attributes(self.small_test_df)

    def test_add_collibra_attribute(self):
        test_id = "9fbc86d2-f69b-420d-a8a3-456172027a87"
        test_attribute = "00000000-0000-0000-0000-000000000258"
        test_value = "google.com3"
        self.collibra_operations.add_collibra_attributes(
            test_id, test_attribute, test_value
        )

    def test_add_asset_ids_to_df(self):
        dataframe = self.small_test_df
        json_response = json.load(open("src/tests/test_files/small_test_response.json"))
        self.collibra_operations.add_asset_ids_to_df(dataframe, json_response)

    def to_delete_test_remove_all_assets_from_domain(self):
        import requests

        token_request = self.collibra_operations.collibra_auth
        url = "https://wlgore-dev.collibra.com/rest/2.0/assets?offset=0&limit=0&countLimit=-1&nameMatchMode=ANYWHERE&domainId=ba4a4398-1ced-40ed-9869-d463bc6ccf53&typeInheritance=true&excludeMeta=true&sortField=NAME&sortOrder=ASC"

        payload = ""
        headers = {
            "Authorization": "Bearer " + token_request,
            "Cookie": "AWSALBTG=HfJKzmujWnqqP2yMWhcH8DbOeivMW9m1E9oIHA43ZAOqAz9bfwUwkmoTAQfqt7tcN691yuxci9+0JGY9H7OaGWNcA+88mI3cZGAYIacG0Wp2g7ojTiEyT0kLGSU6L5DVFM8pIwFcswGF1XJN4QAAmgtntqVorfP8uPDLx0KG8xI0fA5BXhc=; AWSALBTGCORS=HfJKzmujWnqqP2yMWhcH8DbOeivMW9m1E9oIHA43ZAOqAz9bfwUwkmoTAQfqt7tcN691yuxci9+0JGY9H7OaGWNcA+88mI3cZGAYIacG0Wp2g7ojTiEyT0kLGSU6L5DVFM8pIwFcswGF1XJN4QAAmgtntqVorfP8uPDLx0KG8xI0fA5BXhc=",
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        assets = response.json()["results"]
        asset_ids = [d["id"] for d in assets]
        print(response.text)
        url = "https://wlgore-dev.collibra.com/rest/2.0/assets/bulk"
        response = requests.request("DELETE", url, headers=headers, data=asset_ids)
        print("response")
