import unittest
import pandas
import pytest
import yaml
import os
import json
import requests
from src.SQLOperations import SQLOperations
from src.Collibra_Operations import Collibra_Operations
from src.Access_Token import AccessToken


class SqlOperationsTest(unittest.TestCase):
    def setUp(self):
        with open("./src/tests/test_files/test_config.yml", "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        try:
            self.token_auth = config["AUTH"]["token_auth_header"]
            self.sql_query = config["MYSQL_CONNECTION_DETAILS"]["CREATE_SQL_QUERY"]
            self.environment = config["ENVIRONMENT"]["gore"]
            self.admin_only_domain_id = config["COLLIBRA_DETAILS"]["ADMIN_DOMAIN_ID"]
            self.token_auth = config["AUTH"]["token_auth_header"]
            self.status_attribute_id = config["COLLIBRA_DETAILS"]["Install_Status"]
        except KeyError:
            print("The config file is incorrectly setup: " + str(KeyError))
            os._exit(1)

        self.sql_operations = SQLOperations(
            "test",
            "test",
            "test",
            "test",
            self.token_auth,
            self.admin_only_domain_id,
            self.environment,
        )
        self.six_test = pandas.read_csv("src/tests/test_files/six_test.csv")
        self.empty_test_df = pandas.DataFrame()
        self.access_token = AccessToken(self.token_auth)
        self.collibra_operations = Collibra_Operations(
            self.admin_only_domain_id,
            self.environment,
            self.token_auth,
            "./src/tests/test_files/test_config.yml",
        )
        self.full_create_df = pandas.read_csv(
            "src/tests/test_files/full_asset_test.csv"
        )
        self.error_attribute_update = pandas.read_csv(
            "src/tests/test_files/erroneous_attribute_update.csv"
        )
        self.create_1_df = pandas.read_csv("src/tests/test_files/create_1.csv")
        self.error_test = pandas.read_csv("src/tests/test_files/erroring_test.csv")
        self.delete_test = pandas.read_csv("src/tests/test_files/test_delete.csv")
        self.token = "Bearer " + self.access_token.get_bearer_token()

    def test_create_and_update(self):
        #Empty test
        self.collibra_operations.create_assets(self.empty_test_df)
        assert self.collibra_operations.create_assets_result
        self.delete_collibra_test_assets()
        self.collibra_operations.create_assets_result = False

        self.collibra_operations.create_assets(self.six_test)
        self.update_collibra()
        assert self.collibra_operations.update_attributes_result
        assert self.collibra_operations.update_assets_result
        assert self.collibra_operations.create_assets_result
        self.collibra_operations.create_assets_result = False
        self.collibra_operations.update_assets_result = False
        self.collibra_operations.update_attributes_result = False
        #  Assert duplication fails
        self.collibra_operations.create_assets(self.six_test)
        assert not self.collibra_operations.create_assets_result

        self.delete_collibra_test_assets()
        self.collibra_operations.create_assets_result = False
        self.delete_collibra_test_assets()
        self.collibra_operations.update_attributes(self.error_attribute_update)
        assert not self.collibra_operations.update_attributes_result

        self.delete_collibra_test_assets()
        self.collibra_operations.create_assets(self.error_test)

    def test_fill_test_domain(self):
        self.delete_collibra_test_assets()
        self.collibra_operations.create_assets(self.create_1_df)
        assert self.collibra_operations.create_assets_result
        self.update_collibra()
        assert self.collibra_operations.update_attributes_result
        assert self.collibra_operations.update_assets_result
        self.delete_collibra_test_assets()

    def update_collibra(self):
        ids = self.get_snow_assets()
        update_dataframe = self.create_update_dataframe(ids)
        self.collibra_operations.update_attributes(update_dataframe)

    def create_update_dataframe(self, ids):
        attributes_ids = []
        for i in range(3):
            id = ids[i]
            url = (
                "https://wlgore-dev.collibra.com/rest/2.0/attributes?offset=0&limit=0&countLimit=-1&assetId="
                + id
            )

            payload = ""
            headers = {
                "Authorization": self.token,
                "Cookie": "AWSALBTG=2YOdjNyLf3WKfSVeBJSZMZbMFnYoDLRtRLQvA/lFM3tNe9oxHBg7vLPGwV46nLg9eE+Mv2G0ZEd3Jg43UCgPwMR7WgWnVB7T+RiR/muB6rgmhHQcbXXQctbJQx92G+8JpDbxZ+jJYNzefDgJjB22vedKHE1TdamvyFmY9saPcsgVvT9LSC4=; AWSALBTGCORS=2YOdjNyLf3WKfSVeBJSZMZbMFnYoDLRtRLQvA/lFM3tNe9oxHBg7vLPGwV46nLg9eE+Mv2G0ZEd3Jg43UCgPwMR7WgWnVB7T+RiR/muB6rgmhHQcbXXQctbJQx92G+8JpDbxZ+jJYNzefDgJjB22vedKHE1TdamvyFmY9saPcsgVvT9LSC4=",
            }

            response = requests.request("GET", url, headers=headers, data=payload)
            for item in response.json()["results"]:
                attributes_ids.append(item["id"])

        data = {"attribute_id": attributes_ids, "sn_value": "New value"}
        attribute_dataframe = pandas.DataFrame(data)
        return attribute_dataframe





    def get_status_attribute(self, type_id, asset_id ):
        url = "https://wlgore-dev.collibra.com/rest/2.0/attributes?offset=0&limit=0&countLimit=-1&typeIds=" + type_id + "&assetId=" + asset_id

        payload = ""
        headers = {
            'Authorization': self.token,
            'Cookie': 'AWSALBTG=PGaX74x4Pac8jXXw6SfC60hQ9Ct/2A/xV+VpsonLHUUnyvkk54pml/IS2X+F3aQ+WRxKfUuzXLk6J18z6EGHxQNnKePxua92G7ylsHRbbhKgPWCQ3tw9FU61KPejPOwY4oHIBCnZSZ9dKe4dISktnHjzl56/za/Gth6RZYq/6VkeBe4ceY0=; AWSALBTGCORS=PGaX74x4Pac8jXXw6SfC60hQ9Ct/2A/xV+VpsonLHUUnyvkk54pml/IS2X+F3aQ+WRxKfUuzXLk6J18z6EGHxQNnKePxua92G7ylsHRbbhKgPWCQ3tw9FU61KPejPOwY4oHIBCnZSZ9dKe4dISktnHjzl56/za/Gth6RZYq/6VkeBe4ceY0='
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        json_response = response.json()
        return json_response["results"][0]["id"]

    def test_delete_assets(self):
        self.delete_collibra_test_assets()
        self.collibra_operations.create_assets(self.six_test)

        ids = self.get_snow_assets()
        status_id = self.get_status_attribute(self.status_attribute_id, ids[0])
        data = {
            "Attribute_ID": [status_id],
            "Asset_ID": [ids[0]]
        }
        self.test_df = pandas.DataFrame(data)
        self.collibra_operations.delete_assets(self.test_df)
        assert(self.collibra_operations.delete_asset_response )
        self.collibra_operations.delete_asset_response = False
        self.collibra_operations.delete_assets(self.empty_test_df)

    def get_snow_domain(self):
        url = (
            "https://wlgore-dev.collibra.com/rest/2.0/domains?offset=0&limit=100000&countLimit=100000&"
            "name=ServiceNow%20Applications&nameMatchMode=EXACT&excludeMeta=true&includeSubCommunities=false"
        )

        payload = json.dumps(
            {
                "sourceId": "f5b47f1b-8977-4361-b22a-1f555f774b3a",
                "targetId": "2be0d8f4-4356-44b2-af6d-f99b082e8666",
                "typeId": "00000000-0000-0000-0000-000000007021",
                "startingDate": 1488016800,
                "endingDate": 1658021800,
            }
        )
        headers = {
            "accept": "application/json",
            "Authorization": self.token,
            "Content-Type": "application/json",
            "Cookie": "AWSALBTG=GWm6VeMc+axfFnucFaWXhD8yOnrFEmMyGG51aScZNlX7cLcuVm33dayv89R1KLcDhqK3ASotGOZHrEPY25onpaR2+Yw+zLotv74rt0Gzr7jWBw3bXwtfd6can6JJ3z1cTljG4sEC/93cHZ+2mcHCgFYPL1M4G5WBd9mnREsqA9VrA23A9zw=; AWSALBTGCORS=GWm6VeMc+axfFnucFaWXhD8yOnrFEmMyGG51aScZNlX7cLcuVm33dayv89R1KLcDhqK3ASotGOZHrEPY25onpaR2+Yw+zLotv74rt0Gzr7jWBw3bXwtfd6can6JJ3z1cTljG4sEC/93cHZ+2mcHCgFYPL1M4G5WBd9mnREsqA9VrA23A9zw=",
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        return response.json()["results"][0]["id"]

    def get_snow_assets(
        self,
    ):
        snow_domain_id = self.get_snow_domain()
        url = (
            "https://wlgore-dev.collibra.com/rest/2.0/assets?offset=0&limit=100000&countLimit=-1&nameMatchMode=ANYWHERE&domainId="
            + snow_domain_id
        )

        payload = ""
        headers = {
            "Authorization": self.token,
            "Cookie": "AWSALBTG=NGu2Y1tqf3kx2cvru7Q7R+Xl26ieD8wIlmz2GXPSO3lkHSPkP78g1+avOYLNvzboeg57tcX9T1RAlcMljiKacW1QRAH1ihTNvdvXOQeE/SZSqbTpEIJ7/rMIysDOsab5hLCp8jtoPZZzx4BSL3XApJwNDVHmw11VF93iG4Zy2vpiQ+jXAuU=; AWSALBTGCORS=NGu2Y1tqf3kx2cvru7Q7R+Xl26ieD8wIlmz2GXPSO3lkHSPkP78g1+avOYLNvzboeg57tcX9T1RAlcMljiKacW1QRAH1ihTNvdvXOQeE/SZSqbTpEIJ7/rMIysDOsab5hLCp8jtoPZZzx4BSL3XApJwNDVHmw11VF93iG4Zy2vpiQ+jXAuU=",
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        ids = []
        for asset in response.json()["results"]:
            ids.append(asset["id"])
        return ids

    def delete_collibra_test_assets(self):
        ids = self.get_snow_assets()

        url = "https://wlgore-dev.collibra.com/rest/2.0/assets/bulk"

        payload = json.dumps(ids)
        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "Cookie": "AWSALBTG=DMCQl/00w7h7BumdGw1kkyCsxPouua2BuMlFFwfDzcnoRuewJufXA/IccTs2C97xtrYtQrLeh0zErhQ3xWHk2i1WoKAvxMwxm5PoLWADqr7+3KwvXLap9heDB2hvEBSkTvwUnu7xJ9YrDB6Ayt/5gGMiI9puTHzT1ZlPLPleUGDhh4HcIpY=; AWSALBTGCORS=DMCQl/00w7h7BumdGw1kkyCsxPouua2BuMlFFwfDzcnoRuewJufXA/IccTs2C97xtrYtQrLeh0zErhQ3xWHk2i1WoKAvxMwxm5PoLWADqr7+3KwvXLap9heDB2hvEBSkTvwUnu7xJ9YrDB6Ayt/5gGMiI9puTHzT1ZlPLPleUGDhh4HcIpY=",
        }

        response = requests.request("DELETE", url, headers=headers, data=payload)

        print(response.text)

        print("Done")

    def test_collibra_api(self):
        url = "https://wlgore-dev.collibra.com/rest/2.0/assets?offset=0&limit=10&countLimit=10&"
        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "Cookie": "AWSALBTG=DMCQl/00w7h7BumdGw1kkyCsxPouua2BuMlFFwfDzcnoRuewJufXA/IccTs2C97xtrYtQrLeh0zErhQ3xWHk2i1WoKAvxMwxm5PoLWADqr7+3KwvXLap9heDB2hvEBSkTvwUnu7xJ9YrDB6Ayt/5gGMiI9puTHzT1ZlPLPleUGDhh4HcIpY=; AWSALBTGCORS=DMCQl/00w7h7BumdGw1kkyCsxPouua2BuMlFFwfDzcnoRuewJufXA/IccTs2C97xtrYtQrLeh0zErhQ3xWHk2i1WoKAvxMwxm5PoLWADqr7+3KwvXLap9heDB2hvEBSkTvwUnu7xJ9YrDB6Ayt/5gGMiI9puTHzT1ZlPLPleUGDhh4HcIpY=",
        }

        response = requests.request("GET", url, headers=headers)

        print(response.text)