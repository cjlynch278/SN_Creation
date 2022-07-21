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
            self.description = config["COLLIBRA_DETAILS"]["Description"]
            self.install_status = config["COLLIBRA_DETAILS"]["Install_Status"]
            self.business_criticality = config["COLLIBRA_DETAILS"][
                "Business_Criticality"
            ]
            self.url = config["COLLIBRA_DETAILS"]["URL"]
            self.owned_by = config["COLLIBRA_DETAILS"]["Owned_By"]
            self.it_owner = config["COLLIBRA_DETAILS"]["IT_Owner"]
            self.supported_by = config["COLLIBRA_DETAILS"]["Supported_By"]
            self.export_control = config["COLLIBRA_DETAILS"]["Export_Control"]
            self.legal_hold = config["COLLIBRA_DETAILS"]["Legal_Hold"]
            self.apm_data_sensitivity = config["COLLIBRA_DETAILS"][
                "APM_Data_Sensitivity"
            ]
            self.disaster_recovery_gap = config["COLLIBRA_DETAILS"][
                "Disaster_Recovery_Gap"
            ]
            self.regulatory_and_compliance_standards = config["COLLIBRA_DETAILS"][
                "Regulatory_And_Compliance_Standards"
            ]
            self.records_retention = config["COLLIBRA_DETAILS"]["Records_Retention"]
            self.description = config["COLLIBRA_DETAILS"]["Description"]
            self.system_asset_type_id = config["COLLIBRA_DETAILS"][
                "System_Asset_Type_ID"
            ]
            self.system_status_id = config["COLLIBRA_DETAILS"]["System_Status_ID"]

            self.attributes_map = {
                "URL": "URL",
                "Number": "CI_Number",
                "Application Status": "Install_Status",
                "Business Owner": "Owned_By",
                "IT Application Owner": "IT_Owner",
                "Application Contact": "Supported_By",
                "Regulatory And Compliance Standards": "Regulatory_And_Compliance_Standards",
                "Legal Hold": "Legal_Hold",
                "Data Sensitivity": "APM_Data_Sensitivity",
                "Disaster Recovery Required": "Disaster_Recovery_Gap",
                "Records Retention": "Records_Retention",
                "Business Criticality": "Business_Criticality",
                "Export Control": "Export_Control",
            }

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
            "Description": self.description,
            "Install_Status": self.install_status,
            "Business_Criticality": self.business_criticality,
            "Regulatory_And_Compliance_Standards": self.regulatory_and_compliance_standards,
            "URL": self.url,
            "Owned_By": self.owned_by,
            "IT_Owner": self.it_owner,
            "Supported_By": self.supported_by,
            "Export_Control": self.export_control,
            "Legal_Hold": self.legal_hold,
            "APM_Data_Sensitivity": self.apm_data_sensitivity,
            "Disaster_Recovery_Gap": self.disaster_recovery_gap,
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

        print("Assets Created Status Code: " + str(response.status_code))
        logging.info("Assets Created: " + str(response.status_code))
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


        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print("Adding assets was unsuccessful")
            logging.info("Adding assets was unsuccessful")
            print(response.json()["titleMessage"])
            logging.info(response.json()["titleMessage"])
            print(response.json()["userMessage"])
            logging.info(response.json()["userMessage"])
            logging.info(json.dumps(attribute_dict))

            os._exit(1)
        print("Attributes Created response: " + str(response.status_code))
        logging.info("Attributes Created response: " + str(response.status_code))

    def add_asset_ids_to_df(self, dataframe, json_response):
        # Add asset_id column
        dataframe["asset_id"] = None
        for asset in json_response:
            # add asset_ids from json response to dataframe
            dataframe_row = dataframe.loc[
                dataframe["asset_name"] + "_" + dataframe["SN_System_ID"]
                == asset["name"],
                "asset_id",
            ]
            if not dataframe_row.empty:
                dataframe.loc[
                    dataframe["asset_name"] + "_" + dataframe["SN_System_ID"]
                    == asset["name"],
                    "asset_id",
                ] = asset["id"]
            # Accounts for rows where assset_name is empty
            else:

                dataframe.loc[
                    "_" + dataframe["SN_System_ID"] == asset["name"], "asset_id"
                ] = asset["id"]
        return dataframe

    def create_assets_and_attributes(self, dataframe):
        # get all columns except asset name for attributes
        attribute_columns = dataframe.drop(columns=["asset_name", "SN_System_ID"])

        asset_list = []
        for index, row in dataframe.iterrows():
            if not (row["asset_name"] in ["Unknown", "None", None, "nan", ""]):
                print("Adding row: " + str(row["asset_name"]))
                asset_name = str(row["asset_name"])
            else:
                asset_name = str(row["SN_System_ID"])
            asset_backend_name = str(row["SN_System_ID"])

            current_asset_dict = {
                "name": asset_backend_name,
                "displayName": asset_name,
                "domainId": self.target_domain_id,
                "typeId": self.system_asset_type_id,
                "statusId": self.system_status_id
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

    def update_collibra(self, dataframe):
        logging.info("--------------------------------------")
        logging.info("-----------------Updates-----------------")
        logging.info("--------------------------------------")

        update_list = []
        create_list = []
        for index, row in dataframe.iterrows():
            # Update here attribute_ID not null

            if "attribute_id" in row and not pandas.isnull(row["attribute_id"]):
                current_attribute_dict = {
                    "id": row["attribute_id"],
                    "value": row["sn_value"],
                }
                update_list.append(current_attribute_dict)

            if (
                not (row["sn_value"] in ["Unknown", "None", None, "nan"])
                and row["sn_value"] == row["sn_value"]
            ):
                current_attribute_dict = {
                    "assetId": row["Asset_ID"],
                    "typeId": self.column_map[row["attribute_type"]],
                    "value": row["sn_value"],
                }
                create_list.append(current_attribute_dict)

        self.add_collibra_attributes(create_list)
        self.collibra_attribute_patch(update_list)

        logging.info("Attributes Created")
        for dict in create_list:
            logging.info(
                dict["typeId"]
                + "Attribute added to asset "
                + dict["assetId"]
                + " with value "
                + dict["value"]
            )

        logging.info("Attributes Updated")
        for dict in create_list:
            logging.info(
                dict["typeId"]
                + "Attribute added to asset "
                + dict["assetId"]
                + " with value "
                + dict["value"]
            )

    def collibra_attribute_patch(self, update_list):
        url = "https://" + self.environment + "/rest/2.0/attributes/bulk"

        payload = json.dumps(update_list)
        headers = {
            "Authorization": self.collibra_auth,
            "Content-Type": "application/json",
            "Cookie": "AWSALBTG=fXKe2gPjziB8JKidvImZqUflXpwoukyxOAQdWJWinBToRlwy0jAnUFitKSv7+fpqgif8y9WUYDnejXAtxw+p4SJlhwUE3yAkaj2VRj3iZgpDOzhH0cGYRod650nDcTnWTuMJ/y9x68Y6KvkhUvs550iE9t1L62RfphNjhkhiMCYOhLT4zq4=; AWSALBTGCORS=fXKe2gPjziB8JKidvImZqUflXpwoukyxOAQdWJWinBToRlwy0jAnUFitKSv7+fpqgif8y9WUYDnejXAtxw+p4SJlhwUE3yAkaj2VRj3iZgpDOzhH0cGYRod650nDcTnWTuMJ/y9x68Y6KvkhUvs550iE9t1L62RfphNjhkhiMCYOhLT4zq4=; JSESSIONID=918efa55-b2b6-439f-877c-5967cef63ce2",
        }
        try:
            response = requests.request("PATCH", url, headers=headers, data=payload)

        # Retry request if ssl error
        except requests.exceptions.SSLError:
            response = requests.request("PATCH", url, headers=headers, data=payload)

        if response.status_code == 404:
            logging.error("Error when modifying attributes")
            print("Error when modifying attributes")
            return response.text
