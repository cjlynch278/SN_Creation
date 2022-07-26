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
                "Description": "Description",
                "Application Status": "Install_Status",
                "Business Owner": "Owned_By",
                "IT Application Owner": "IT_Owner",
                #channge from application contact to supported by
                "Supported By": "Supported_By",
                "SN Regulatory & Compliance Standards": "Regulatory_And_Compliance_Standards",
                "Legal Hold": "Legal_Hold",
                "Data Sensitivity": "APM_Data_Sensitivity",
                "Disaster Recovery Gap": "Disaster_Recovery_Gap",
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
        self.bulk_attributes_url = (
            "https://" + self.environment + "/rest/2.0/attributes/bulk"
        )
        self.bulk_assets_url = "https://" + self.environment + "/rest/2.0/assets/bulk"

    def create_assets(self, dataframe):
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
        self.collibra_api_call(
            "POST", self.bulk_assets_url, asset_list
        )
        logging.info("Assets Created Dataframe: " + dataframe["asset_name"])


    # Go through all attributes that need to be updated
    def update_collibra(self, dataframe):
        logging.info("--------------------------------------")
        logging.info("-----------------Updates-----------------")
        logging.info("--------------------------------------")

        update_list = []
        create_list = []
        for index, row in dataframe.iterrows():
            if "attribute_id" in row and not pandas.isnull(row["attribute_id"]):
                current_attribute_dict = {
                    "id": row["attribute_id"],
                    "value": row["sn_value"],
                }
                update_list.append(current_attribute_dict)

            elif (
                not (row["sn_value"] in ["Unknown", "None", None, "nan"])
                and row["sn_value"] == row["sn_value"]
            ):
                current_attribute_dict = {
                    "assetId": row["Asset_ID"],
                    "typeId": self.column_map[
                        self.attributes_map[row["attribute_type"]]
                    ],
                    "value": row["sn_value"],
                }
                create_list.append(current_attribute_dict)

        self.collibra_api_call("POST", self.bulk_attributes_url, create_list)
        self.collibra_api_call("PATCH", self.bulk_attributes_url, update_list)

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

    def collibra_api_call(self, method_type, url, item_list):
        """
        This method generically calls Collibra's api
        The url param is to specify which collibra needs to be called
        the item list attribute is a list containing dicts of what should be in the
        body of the api call
        The method_type paramter determines what type of api call is being sent e.g. patch,
        post, get etc..
        """

        payload = json.dumps(item_list)
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.collibra_auth,
            "Cookie": "AWSALBTG=Xx66ouqWMBuXDDec5OSvqFCQP/PUHcQk4/bdJiR94rGzG/V5WRcMBzuU3pJlVYu4HU/n7EHRJzBq62YNY3YiIq8OMg9muLvH/0Lvx0LTA1YmNk+cncExFCbfBICAgwfP2CNp8y1lJYd4waxnTeYxClL7N8tdx1vyud+OkNC3BYKjOmzkz8I=; AWSALBTGCORS=Xx66ouqWMBuXDDec5OSvqFCQP/PUHcQk4/bdJiR94rGzG/V5WRcMBzuU3pJlVYu4HU/n7EHRJzBq62YNY3YiIq8OMg9muLvH/0Lvx0LTA1YmNk+cncExFCbfBICAgwfP2CNp8y1lJYd4waxnTeYxClL7N8tdx1vyud+OkNC3BYKjOmzkz8I=",
        }

        try:
            response = requests.request(method_type, url, headers=headers, data=payload)

        # Retry request if ssl error
        except requests.exceptions.SSLError:
            response = requests.request(method_type, url, headers=headers, data=payload)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print("API call was unsuccessful")
            logging.info("API call was unsuccessful")
            print(response.json()["titleMessage"])
            logging.info(response.json()["titleMessage"])
            print(response.json()["userMessage"])
            logging.info(response.json()["userMessage"])

            os._exit(1)

        print("API Call Status Code: " + str(response.status_code))
        logging.info("API Call Status Code: " + str(response.status_code))
        return response.json()
