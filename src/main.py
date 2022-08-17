from src.SQLOperations import SQLOperations
from src.Collibra_Operations import Collibra_Operations
from src.Email import Email_Class
import yaml
import os
import logging
from datetime import datetime


class MainClass:
    def __init__(self, config_file):
        with open(config_file, "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        try:
            self.admin_only_domain_id = config["COLLIBRA_DETAILS"]["ADMIN_DOMAIN_ID"]
            self.systems_domain_id = config["COLLIBRA_DETAILS"]["Systems_Domain_ID"]
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
            self.logger_location = config["LOGGER"]["LOCATION"]
            self.status_attribute_id = config["COLLIBRA_DETAILS"]["Install_Status"]
            self.queries_location = config["ENVIRONMENT"]["Queries_Location"]
            self.debug_level = config["LOGGER"]["LEVEL"]
        except KeyError as e:
            print("The config file is incorrectly setup: " + str(e))
            os._exit(1)
        self.log_file_name = (
            self.logger_location + "_" + str(datetime.today().date()) + ".log"
        )
        level = logging.getLevelName(self.debug_level)
        logging.basicConfig(
            filename=self.log_file_name,
            filemode="a",
            format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
            level= level,
        )
        logging.info("Push To Collibra App started")
        logging.info("Config File Read")
        self.sql_operations = SQLOperations(
            self.sql_user,
            self.sql_password,
            self.server_name,
            self.database_name,
            self.token_auth,
            self.admin_only_domain_id,
            self.environment,
        )
        logging.debug("Sql operations setup")

        self.collibra_operations = Collibra_Operations(
            self.admin_only_domain_id, self.environment, self.token_auth, config_file
        )
        logging.debug("Collibra Operations setup")

        with open(self.queries_location + "/create_query.sql", "r") as create_sql_file:
            self.create_sql_query = create_sql_file.read().format(
                self.admin_only_domain_id, self.systems_domain_id
            )

        with open(self.queries_location + "/update_query.sql", "r") as update_sql_file:
            self.update_sql_query = update_sql_file.read().format(
                self.admin_only_domain_id, self.systems_domain_id
            )

        with open(self.queries_location + "/delete_query.sql", "r") as delete_sql_file:
            self.delete_sql_query = delete_sql_file.read().format(
                self.admin_only_domain_id, self.systems_domain_id
            )

    def prepare_and_send_email(self):
        log_file = open(self.log_file_name, "r")
        email_contents = log_file.read()
        email_class = Email_Class("smtp.wlgore.com", 25)
        email_class.send_mail(email_contents, "chlynch@wlgore.com")
        print("email sent!")

    def run(self):
        self.sql_operations.connect_to_sql()
        logging.debug("SQL connected")

        create_dataframe = self.sql_operations.read_sql(self.create_sql_query)
        logging.info("Create Sql read successfully")
        self.collibra_operations.create_assets(create_dataframe)

        update_dataframe = self.sql_operations.read_sql(self.update_sql_query)
        logging.info("Update Sql read successfully")
        self.collibra_operations.update_attributes(update_dataframe)

        delete_dataframe = self.sql_operations.read_sql(self.delete_sql_query)
        logging.info("Update Sql read successfully")
        self.collibra_operations.delete_assets(delete_dataframe)

        logging.info("================ SUMMARY ================")
        logging.info(
            "Assets Created Successfully: "
            + str(self.collibra_operations.create_assets_result)
        )
        logging.info(
            "Attributes Created Successfully: "
            + str(self.collibra_operations.create_attributes_result)
        )
        logging.info(
            "Assets Updated Successfully: "
            + str(self.collibra_operations.update_attributes_result)
        )
        logging.info(
            "Attributes Updated Successfully: "
            + str(self.collibra_operations.update_attributes_result)
        )
        try:
            self.prepare_and_send_email()
        except Exception as e:
            logging.error("Could not setup email: " + str(e))


if __name__ == "__main__":
    # Run main class
    main = MainClass("config.yml")
    main.run()
