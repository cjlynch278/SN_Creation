from urllib.parse import quote_plus as url_quote
import pandas as pd
import sqlalchemy
from sqlalchemy import exc
import logging
from sqlalchemy import create_engine
from src.access_token import AccessToken
import pyodbc


class SQLOperations:
    """ This class handles the collection of data from SQL. It reaches out to SQL via a query specified, and returns a
    pandas dataframe containing a list of values"""
    def __init__(
        self,
        sql_user,
        sql_password,
        server_name,
        database_name,
        token_auth,
        admin_only_id,
        environment,
    ):
        self.admin_only_id = admin_only_id
        self.environment = environment
        self.token_auth = token_auth
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
        """ Connects to the SQL database"""
        self.engine = create_engine(self.connection_string)
        self.conn = self.engine.connect()

    def read_sql(self, string_sql_query):
        """
        :param string_sql_query: the sql query who's returned values will be transformed into a dataframe.
        :return: a pandas dataframe
        """
        try:
            sql_query = pd.read_sql_query(
                string_sql_query,
                self.conn,
            )

            df = pd.DataFrame(sql_query)
        except sqlalchemy.exc.ResourceClosedError as e:
            print("No SQL Read: " + str(e))
            logging.warning("No SQL Read: " + str(e))
            df = pd.DataFrame

        return df
