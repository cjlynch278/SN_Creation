from src.SQLOperations import SQLOperations
from src.Collibra_Operations import Collibra_Operations
import yaml
import os


class MainClass:
    def __init__(self, config_file):
        with open(config_file, "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        try:
            self.admin_only_domain_id = config["COLLIBRA_DETAILS"]["ADMIN_DOMAIN_ID"]
            self.sql_query = config["MYSQL_CONNECTION_DETAILS"]["SQL_QUERY"]
            self.token_auth = config["AUTH"]["token_auth_header"]
            self.database_name = str(
                config["MYSQL_CONNECTION_DETAILS"]["DATABASE_NAME"]
            )
            self.server_name = config["MYSQL_CONNECTION_DETAILS"]["SERVER_NAME"]
            self.sql_user = config["MYSQL_CONNECTION_DETAILS"]["LOGIN"]
            self.sql_password = config["MYSQL_CONNECTION_DETAILS"]["PASSWORD"]
            self.cookie = config["AUTH"]["cookie"]
            self.token_auth = config["AUTH"]["token_auth_header"]
            self.schema = "extract"
            self.environment = config["ENVIRONMENT"]["gore"]
            self.auth = config["AUTH"]["auth-header"]
        except KeyError:
            print("The config file is incorrectly setup")
            os._exit(1)

        self.sql_operations = SQLOperations(
            self.sql_user,
            self.sql_password,
            self.server_name,
            self.database_name,
            self.token_auth,
            self.sql_query,
            self.admin_only_domain_id,
            self.environment,
        )
        self.sql_operations.connect_to_sql()
        print(self.sql_operations.read_sql())

        self.collibra_operations = Collibra_Operations(
            self.admin_only_domain_id, self.environment, self.token_auth
        )

    def run(self):
        dataframe = self.sql_operations.read_sql()
        self.collibra_operations.create_assets_and_attributes(dataframe)


if __name__ == "__main__":
    # Run main class

    main = MainClass("config.yml")
    main.run()
    # mainClass = MainClass("")
