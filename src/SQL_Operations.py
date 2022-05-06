from urllib.parse import quote_plus as url_quote
import pandas as pd
from sqlalchemy import create_engine
import pyodbc


class SQL_Operations:
    def __init__(self, sql_user, sql_password, server_name, database_name):
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

    def connect_to_sql(self):

        self.engine = create_engine(self.connection_string)

        self.conn = self.engine.connect()
        self.cursor = self.conn.cursor()

    def read_sql(self):

        SQL_Query = pd.read_sql_query(
            """Select ltrim(rtrim(sn_assets.Name)) asset_name,[APM Data Sensitivity], [Business Criticality], 
            [CI Number] from Extract.servicenow_cmbd_ci_service_discovered_raw sn_assets left Join 
            (select * from collibra.collibra_assets where is_current=1 and 
            Domain_ID ='d248a4fd-255a-4a2a-83f9-8da0c7c2c19a') collibra_assets on sn_assets.Name = 
            collibra_assets.name where collibra_assets.Name is null
    """,
            self.conn,
        )

        df = pd.DataFrame(
            SQL_Query,
            columns=[
                "Asset_Name",
                "APM_DATA_SENSITIVITY",
                "Business_Criticality",
                "CI_NUMBER",
            ],
        )
        return df
