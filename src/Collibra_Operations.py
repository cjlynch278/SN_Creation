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
            "Number": self.ci_number,
            "Description": self.description,
            "Application Status": self.install_status,
            "Business Criticality": self.business_criticality,
            "SN Regulatory & Compliance Standards": self.regulatory_and_compliance_standards,
            "URL": self.url,
            "Business Owner": self.owned_by,
            "IT Application Owner": self.it_owner,
            "Application Contact": self.supported_by,
            "Export Control": self.export_control,
            "Legal Hold": self.legal_hold,
            "Data Sensitivity": self.apm_data_sensitivity,
            "Disaster Recovery Gap": self.disaster_recovery_gap,
            "Records Retention": self.records_retention,
        }
        self.column_map = {
            "asset_name": self.asset_name,
            "CI_Number": self.ci_number,
            "Description": self.description,
            "Install_Status": self.install_status,
            "Business_Criticality": self.business_criticality,
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
        """
        :param dataframe: This is the dataframe that gets returned by the asset dataframe. It
            consists all of the assets that will need to be created/updated in Collibra to
            match SNOW
        :return: nothing
        """

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
        asset_create_response = self.collibra_api_call(
            "POST", self.bulk_assets_url, asset_list
        )

        if asset_create_response.status_code in [200, 201]:
            logging.info("Assets Created")
            for dict in asset_list:
                logging.info(
                    "Asset Created: "
                    + dict["displayName"]
                    + " with an SN System ID of "
                    + dict["name"]
                )
            self.create_attributes(dataframe, asset_create_response.json())
        else:
            logging.error("Error Creating Assets")
            print("Error Creating Assets")
            logging.info(asset_create_response.json()["titleMessage"])
            print(asset_create_response.json()["userMessage"])
            logging.info(asset_create_response.json()["userMessage"])
            print(asset_create_response.json()["userMessage"])

    def create_attributes(self, dataframe, json_response):
        """
        This method creates the attributes that will need to be created as part of the
        assets that are created. This method is a helper method for the create_assets
        method
        :param dataframe: dataframe consisting of SNOW objects with their respective
        attributes
        :return: nothing
        """
        # get all columns except asset name for attributes
        attribute_columns = dataframe.drop(columns=["asset_name", "SN_System_ID"])

        # Add asset_id column
        dataframe["asset_id"] = None
        for asset in json_response:
            # add asset_ids from json response to dataframe
            dataframe_row = dataframe.loc[
                dataframe["SN_System_ID"] == asset["name"], "asset_id"]
            if not dataframe_row.empty:
                dataframe.loc[dataframe["SN_System_ID"] == asset["name"], "asset_id"] = asset["id"]

        # Create new dataframe of attributes based on the created SNOW assets
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
        attribute_create_response = self.collibra_api_call("POST", self.bulk_attributes_url, attribute_list)

        if attribute_create_response.status_code in [200, 201]:
            logging.info("Attributes Created")
            for dict in attribute_list:
                logging.info(
                    dict["typeId"]
                    + " Attribute added to asset "
                    + dict["assetId"]
                    + " with value "
                    + dict["value"]
                )

        else:
            logging.error("Error Creating attributes")
            print("Error Creating attributes")
            logging.info(attribute_create_response.json()["titleMessage"])
            print(attribute_create_response.json()["userMessage"])
            logging.info(attribute_create_response.json()["userMessage"])
            print(attribute_create_response.json()["userMessage"])


    def update_attributes(self, dataframe):
        """
        :param dataframe: This is the dataframe that gets returned by the attribute dataframe. It
            consists all of the attributes that will need to be created/updated in Collibra to
            match SNOW
        :return: nothing
        """
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
                    "typeId": self.column_map[row["attribute_type"]],
                    "value": row["sn_value"],
                }
                create_list.append(current_attribute_dict)

        attribute_create_response = self.collibra_api_call(
            "POST", self.bulk_attributes_url, create_list
        )
        if attribute_create_response.status_code in [200, 201]:
            logging.info("Attributes Created")
            for dict in create_list:
                logging.info(
                    dict["typeId"]
                    + " Attribute added to asset "
                    + dict["assetId"]
                    + " with value "
                    + dict["value"]
                )

        else:
            logging.error("Error Creating attributes")
            print("Error Creating attributes")
            logging.info(attribute_create_response.json()["titleMessage"])
            print(attribute_create_response.json()["userMessage"])
            logging.info(attribute_create_response.json()["userMessage"])
            print(attribute_create_response.json()["userMessage"])

        attribute_update_response = self.collibra_api_call(
            "PATCH", self.bulk_attributes_url, update_list
        )
        if attribute_update_response.status_code in [200, 201]:
            logging.info("Attributes updated")
            for dict in create_list:
                logging.info(
                    dict["typeId"]
                    + " Attribute added to asset "
                    + dict["assetId"]
                    + " with value "
                    + dict["value"]
                )

        else:
            logging.error("Error updating attributes")
            print("Error updating attributes")
            logging.info(attribute_create_response.json()["titleMessage"])
            print(attribute_create_response.json()["userMessage"])
            logging.info(attribute_create_response.json()["userMessage"])
            print(attribute_create_response.json()["userMessage"])

    def collibra_api_call(self, method_type, url, item_list):
        """
        This method generically calls Collibra's api
        :param url: to specify which collibra needs to be called
        :param item_list: a list containing dicts of what should be in the
            body of the api call
        :param method_type: determines what type of api call is being sent e.g. patch,
            post, get etc..
        :return: the json response of the api call
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
        except requests.exceptions.HTTPError as e:
            print("API call was unsuccessful")
            logging.info("API call was unsuccessful")

        print("API Call Status Code: " + str(response.status_code))
        logging.info("API Call Status Code: " + str(response.status_code))
        return response
