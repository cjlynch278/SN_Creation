from urllib.parse import quote_plus as url_quote
import pandas as pd
from sqlalchemy import create_engine
from src.Access_Token import AccessToken



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
        access_token_class = AccessToken(self.token_auth)
        self.collibra_auth = "Bearer " + access_token_class.get_bearer_token()

    def connect_to_sql(self):

        self.engine = create_engine(self.connection_string)

        self.conn = self.engine.connect()


    def read_sql(self):

        SQL_Query = pd.read_sql_query(
            self.sql_query,
            self.conn,
        )

        df = pd.DataFrame(SQL_Query)
        return df
