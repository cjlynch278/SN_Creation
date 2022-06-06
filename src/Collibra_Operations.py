import logging
from urllib.parse import quote_plus as url_quote
import json

import pandas
import requests
from src.Access_Token import AccessToken
import os
import yaml


class Collibra_Operations:
    def __init__(self, admin_only_id, environment, token_auth, config_file):
        with open(config_file, "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        try:
            self.asset_name = config["COLLIBRA_DETAILS"]["asset_name"]
            self.ci_number = config["COLLIBRA_DETAILS"]["CI_Number"]
            self.install_status = config["COLLIBRA_DETAILS"]["Install_Status"]
            self.business_criticality = config["COLLIBRA_DETAILS"]["Business_Criticality"]
            self.url = config["COLLIBRA_DETAILS"]["URL"]
            self.owned_by = config["COLLIBRA_DETAILS"]["Owned_By"]
            self.it_owner = config["COLLIBRA_DETAILS"]["IT_Owner"]
            self.supported_by = config["COLLIBRA_DETAILS"]["Supported_By"]
            self.export_control = config["COLLIBRA_DETAILS"]["Export_Control"]
            self.legal_hold = config["COLLIBRA_DETAILS"]["Legal_Hold"]
            self.apm_data_sensitivity = config["COLLIBRA_DETAILS"]["APM_Data_Sensitivity"]
            self.disaster_recovery_gab = config["COLLIBRA_DETAILS"]["Disaster_Recovery_Gap"]
            self.records_retention = config["COLLIBRA_DETAILS"]["Records_Retention"]

        except KeyError as e:
            print("The config file is incorrectly setup: " + str(e))
            os._exit(1)

        self.token_auth = token_auth
        access_token_class = AccessToken(self.token_auth)
        self.collibra_auth = "Bearer " + access_token_class.get_bearer_token()

        self.admin_only_id = admin_only_id
        self.environment = environment
        self.column_map = {
            "asset_name": self.asset_name,
            "CI_Number": self.ci_number,
            "Install_Status": self.install_status,
            "Business_Criticality": self.business_criticality,
            "URL": self.url,
            "Owned_By": self.owned_by,
            "IT_Owner": self.it_owner,
            "Supported_By": self.supported_by,
            "Export_Control": self.export_control,
            "Legal_Hold": self.legal_hold,
            "APM_Data_Sensitivity": self.apm_data_sensitivity,
            "Disaster_Recovery_Gap": self.disaster_recovery_gab,
            "Records_Retention": self.records_retention,
        }
        self.target_domain_id = self.admin_only_id

    def make_collibra_assets(self, asset_list):
        url = "https://" + self.environment + "/rest/2.0/assets/bulk"

        payload = json.dumps(asset_list)
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.collibra_auth,
            "Cookie": "AWSALBTG=Xx66ouqWMBuXDDec5OSvqFCQP/PUHcQk4/bdJiR94rGzG/V5WRcMBzuU3pJlVYu4HU/n7EHRJzBq62YNY3YiIq8OMg9muLvH/0Lvx0LTA1YmNk+cncExFCbfBICAgwfP2CNp8y1lJYd4waxnTeYxClL7N8tdx1vyud+OkNC3BYKjOmzkz8I=; AWSALBTGCORS=Xx66ouqWMBuXDDec5OSvqFCQP/PUHcQk4/bdJiR94rGzG/V5WRcMBzuU3pJlVYu4HU/n7EHRJzBq62YNY3YiIq8OMg9muLvH/0Lvx0LTA1YmNk+cncExFCbfBICAgwfP2CNp8y1lJYd4waxnTeYxClL7N8tdx1vyud+OkNC3BYKjOmzkz8I=",
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print("Adding assets was unsuccessful")
            logging.info("Adding assets was unsuccessful")
            print(response.json()["titleMessage"])
            logging.info(response.json()["titleMessage"])
            print(response.json()["userMessage"])
            logging.info(response.json()["userMessage"])
            print(json.dumps(asset_list))
            logging.info(json.dumps(asset_list))

            os._exit(1)

        print(response.status_code)
        logging.info("Assets Created: " + response.text)
        return response.json()

    def add_collibra_attributes(self, attribute_dict):
        url = "https://" + self.environment + "/rest/2.0/attributes/bulk"

        payload = json.dumps(attribute_dict)
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.collibra_auth,
            "Cookie": "AWSALBTG=hTSJN5tR+qrcy/5TGPlei2wxCvnVmYpT+BhxuML79+Jes7EGaoDtfmZScPgxcvqw7ZpbdUlu2cifoe/9ycd51Ni2OBPXr0MPn+KQGO+0bQoz775F3TtsUHjzrZJZ4Z9aKy9TWKjTPtlFeAF5JJCvJPkvYJTLp6aYx6TjsUX2z89U5dJy7Zo=; AWSALBTGCORS=hTSJN5tR+qrcy/5TGPlei2wxCvnVmYpT+BhxuML79+Jes7EGaoDtfmZScPgxcvqw7ZpbdUlu2cifoe/9ycd51Ni2OBPXr0MPn+KQGO+0bQoz775F3TtsUHjzrZJZ4Z9aKy9TWKjTPtlFeAF5JJCvJPkvYJTLp6aYx6TjsUX2z89U5dJy7Zo=",
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)
        logging.info("Attributes Created: " + response.text)

    def add_asset_ids_to_df(self, dataframe, json_response):
        # Add asset_id column
        dataframe["asset_id"] = None
        for asset in json_response:
            # add asset_ids from json response to dataframe
            dataframe_row = dataframe.loc[dataframe["asset_name"] + "_" + dataframe["SN_System_ID"] == asset["name"], "asset_id"]
            if not dataframe_row.empty:
                dataframe.loc[dataframe["asset_name"] + "_" + dataframe["SN_System_ID"] == asset["name"], "asset_id"] = asset[
                    "id"
                ]
            #Accounts for rows where assset_name is empty
            else:

                dataframe.loc["_" + dataframe["SN_System_ID"] == asset["name"], "asset_id"] = asset[
                    "id"
                ]
        return dataframe

    def create_assets_and_attributes(self, dataframe):
        # get all columns except asset name for attributes
        attribute_columns = dataframe.drop(columns = ["asset_name", "SN_System_ID"])


        asset_list = []
        for index, row in dataframe.iterrows():
            if not (row["asset_name"] in ["Unknown", "None", None, "nan", ""]):
                print("Adding row: " + str(row["asset_name"]))
                asset_name = str(row["asset_name"])
                asset_backend_name = str(row["asset_name"]) + "_" + str(row["SN_System_ID"])
            else:
                asset_name = "_" + str(row["SN_System_ID"])
                asset_backend_name = "_" + str(row["SN_System_ID"])
            current_asset_dict = {
                "name": asset_backend_name,
                "displayName": asset_name,
                "domainId": self.target_domain_id,
                "typeId": "00000000-0000-0000-0000-000000031302",
                "statusId": "a669d696-06c4-46cc-abac-cd95bc27d374"
                # "excludedFromAutoHyperlinking": "true",
            }
            asset_list.append(current_asset_dict)
        asset_response = self.make_collibra_assets(asset_list)
        dataframe = self.add_asset_ids_to_df(dataframe, asset_response)
        logging.info("Assets Created Dataframe: " + dataframe["asset_name"])
        attribute_list = []
        for index, row in dataframe.iterrows():

            for attribute in attribute_columns:
                value = row[attribute]
                if not (value in ["Unknown", "None", None, "nan"]) and value == value:
                    current_attribute_dict = {
                        "assetId": row["asset_id"],
                        "typeId": self.column_map[attribute],
                        "value": value,
                    }
                    attribute_list.append(current_attribute_dict)

        return self.add_collibra_attributes(attribute_list)
