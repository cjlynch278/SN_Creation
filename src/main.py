from SQL_Operations import SQL_Operations
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
            self.token_auth = config["AUTH"]["token_auth_header"]
            self.database_name = str(config["MYSQL_CONNECTION_DETAILS"]["DATABASE_NAME"])
            self.server_name = config["MYSQL_CONNECTION_DETAILS"]["SERVER_NAME"]
            self.sql_user = config["MYSQL_CONNECTION_DETAILS"]["LOGIN"]
            self.sql_password = config["MYSQL_CONNECTION_DETAILS"]["PASSWORD"]
            self.cookie = config["AUTH"]["cookie"]

            self.schema = "extract"
            self.environment = config["ENVIRONMENT"]["gore"]
            self.auth = config["AUTH"]["auth-header"]
        except KeyError:
            print("The config file is incorrectly setup")
            os._exit(1)

        sql_operations = SQL_Operations(self.sql_user, self.sql_password,self.server_name,self.database_name)
        sql_operations.connect_to_sql()
        print(sql_operations.read_sql())

if __name__ == "__main__":
    # Run main class

    main = MainClass("config.yml")
    main.run()
    # mainClass = MainClass("")
