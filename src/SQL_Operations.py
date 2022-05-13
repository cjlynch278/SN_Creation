from urllib.parse import quote_plus as url_quote
import pandas as pd
from sqlalchemy import create_engine
import json
import requests
from src.Access_Token import AccessToken
import numpy
import pyodbc


class SQL_Operations:
    def __init__(
        self,
        sql_user,
        sql_password,
        server_name,
        database_name,
        token_auth,
        sql_query,
        admin_only_id,
        environment,
    ):
        self.admin_only_id = admin_only_id
        self.environment = environment
        self.token_auth = token_auth
        self.sql_query = sql_query
        self.collibra_auth = "Bearer " + self.get_access_token()
        self.sql_user = sql_user
        self.sql_password = sql_password
        self.server_name = server_name
        self.database_name = database_name
        self.connection_string = (
            "mssql+pyodbc://"
            + self.sql_user
            + ":"
            + url_quote(self.sql_password)
            + "@"
            + self.server_name
            + "/"
            + self.database_name
            + "?driver=SQL+Server+Native+Client+11.0"
        )
        self.column_map = {
            "asset_name": "3b7023ae-9baf-4012-b6f2-aa52483d6c46",
            "CI_Number": "dda874d3-610e-4460-b4d1-7ef304605392",
            "Install_Status": "8eeeae37-e6cc-4cd9-a65d-c5b6c3a72c36",
            "Business_Criticality": "12d0118b-6d7b-4ef2-86aa-a44b5552dad9",
            "URL": "00000000-0000-0000-0000-000000000258",
            "Owned_By": "eff75d33-cba0-44d8-9c2a-f4c3fc589693",
            "IT_Owner": "23c90dd8-eba5-4a0b-8d2e-98ef071963c8",
            "Supported_By": "42203189-d926-4fb6-b809-829e9596bc28",
            "Regulatory_And_Compliance_Standards": "eb40afcd-313e-4cd5-b1c5-49b75863909a",
            "Export_Control": "5acb934c-e521-4858-8732-bffa326419ea",
            "Legal_Hold": "8100009a-2a8d-4dfa-9b83-77e65534318c",
            "APM_Data_Sensitivity": "270d4a3d-fd4e-45b8-b2bc-57b0bc31b12d",
            "Disaster_Recovery_Gap": "c6b1b435-956e-4cf7-b675-62287cb99f6e",
            "Records_Retention": "42024702-5654-4b3b-9bb9-2de6e806882e",
        }
        self.target_domain_id = self.admin_only_id

    def connect_to_sql(self):

        self.engine = create_engine(self.connection_string)

        self.conn = self.engine.connect()

    def get_access_token(self):
        access_token_class = AccessToken(self.token_auth)
        return access_token_class.get_bearer_token()

    def read_sql(self):

        SQL_Query = pd.read_sql_query(
            self.sql_query,
            self.conn,
        )

        df = pd.DataFrame(SQL_Query)
        return df

    def make_collibra_assets(self, asset_list):

        url = "https://" + self.environment + "/rest/2.0/assets/bulk"

        payload = json.dumps(
            asset_list
        )
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.collibra_auth,
            "Cookie": "AWSALBTG=Xx66ouqWMBuXDDec5OSvqFCQP/PUHcQk4/bdJiR94rGzG/V5WRcMBzuU3pJlVYu4HU/n7EHRJzBq62YNY3YiIq8OMg9muLvH/0Lvx0LTA1YmNk+cncExFCbfBICAgwfP2CNp8y1lJYd4waxnTeYxClL7N8tdx1vyud+OkNC3BYKjOmzkz8I=; AWSALBTGCORS=Xx66ouqWMBuXDDec5OSvqFCQP/PUHcQk4/bdJiR94rGzG/V5WRcMBzuU3pJlVYu4HU/n7EHRJzBq62YNY3YiIq8OMg9muLvH/0Lvx0LTA1YmNk+cncExFCbfBICAgwfP2CNp8y1lJYd4waxnTeYxClL7N8tdx1vyud+OkNC3BYKjOmzkz8I=",
        }


        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.status_code)
        return response.json()
    def add_collibra_attributes(self, attribute_dict):
        url = "https://" +self.environment + "/rest/2.0/attributes/bulk"

        payload = json.dumps(attribute_dict)
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.collibra_auth,
            'Cookie': 'AWSALBTG=hTSJN5tR+qrcy/5TGPlei2wxCvnVmYpT+BhxuML79+Jes7EGaoDtfmZScPgxcvqw7ZpbdUlu2cifoe/9ycd51Ni2OBPXr0MPn+KQGO+0bQoz775F3TtsUHjzrZJZ4Z9aKy9TWKjTPtlFeAF5JJCvJPkvYJTLp6aYx6TjsUX2z89U5dJy7Zo=; AWSALBTGCORS=hTSJN5tR+qrcy/5TGPlei2wxCvnVmYpT+BhxuML79+Jes7EGaoDtfmZScPgxcvqw7ZpbdUlu2cifoe/9ycd51Ni2OBPXr0MPn+KQGO+0bQoz775F3TtsUHjzrZJZ4Z9aKy9TWKjTPtlFeAF5JJCvJPkvYJTLp6aYx6TjsUX2z89U5dJy7Zo='
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)

    def add_asset_ids_to_df(self, dataframe, json_response):
        # Add asset_id column
        dataframe['asset_id'] = None
        for asset in json_response:
            # add asset_ids from json response to dataframe

            dataframe.loc[dataframe['asset_name'] == asset['name'], 'asset_id'] = asset['id']
        return dataframe

    def create_assets_and_attributes(self, dataframe):
        #get all columns except asset name for attributes
        attribute_columns = dataframe.loc[:, dataframe.columns != 'asset_name']

        asset_list = []
        asset_response = None
        for index, row in dataframe.iterrows():
            asset_name = row['asset_name']
            current_asset_dict = {
                "name": asset_name,
                "displayName": asset_name,
                "domainId": self.target_domain_id,
                "typeId": "00000000-0000-0000-0000-000000031302",
                "statusId": "a669d696-06c4-46cc-abac-cd95bc27d374"
                #"excludedFromAutoHyperlinking": "true",
            }
            asset_list.append(current_asset_dict)
        asset_response = self.make_collibra_assets(asset_list)
        dataframe = self.add_asset_ids_to_df(dataframe, asset_response)


        for index, row in dataframe.iterrows():
            attribute_list = []
            for attribute in attribute_columns:
                value = row[attribute]
                if not (value in ['Unknown', 'None', None]) and value == value:

                    current_attribute_dict = {
                    "assetId": row['asset_id'],
                    "typeId": self.column_map[attribute],
                    "value": value,
                    }
                    attribute_list.append(current_attribute_dict)

        return self.add_collibra_attributes(attribute_list)


