from src.sql_operations import SQLOperations
from src.collibra_operations import Collibra_Operations
from src.email_operations import Email_Class
import yaml
import os
import logging
from datetime import datetime


class MainClass:
    """
    This main class is where the program starts. It calls the Collibra_Operations, Email, and SQL_Opearations classes
    """
    def __init__(self, config_file):
        """
        Initializes the program by gathering all required variables from the config file and setting up the logging
        file.
        :param config_file: The yaml file which contains many of the settings to run this program.
        """
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
            self.email_recepients = config["EMAIL_SETTINGS"]["Recipient"]
            self.environment_instance = config["ENVIRONMENT"]["instance"]
            self.driver = config["MYSQL_CONNECTION_DETAILS"]["DRIVER"]
        except KeyError as e:
            print("The config file is incorrectly setup: " + str(e))
            os._exit(1)
        # Configure Logger
        self.log_file_name = (
            self.logger_location + "_" + str(datetime.today().date()) + ".log"
        )
        # Set logging levels
        level = logging.getLevelName(self.debug_level)
        logging.basicConfig(
            filename=self.log_file_name,
            filemode="a",
            format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
            level=level,
        )
        logging.getLogger("requests").setLevel(level)
        logging.getLogger("urllib3").setLevel(level)

        logging.debug("Push To Collibra App started")
        logging.debug("Config File Read")

        # Set up SQLOperations class
        self.sql_operations = SQLOperations(
            self.sql_user,
            self.sql_password,
            self.server_name,
            self.database_name,
            self.token_auth,
            self.admin_only_domain_id,
            self.environment,
            self.driver
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

        with open(self.queries_location + "/update_display_name_query.sql", "r") as update_display_name_query_file:
            self.update_display_name_query = update_display_name_query_file.read().format(
                self.admin_only_domain_id, self.systems_domain_id
            )


    def prepare_and_send_email(self):
        """Call the email class and email the log file to the specified admin"""

        recepients = self.email_recepients.split(",")
        with open(self.log_file_name, "r") as log_file:
            try:
                contents = log_file.read()
            except Exception as e:
                print("Error Reading log file to email: " + str(e))
                contents = "Error Reading log file to email: " + str(e)
        email_class = Email_Class("smtp.wlgore.com", 25)
        # String to specify the subject and message of the email.
        message = 'Subject: {}\n\n{}'.format(self.environment_instance + ": Collibra and SNOW Pipeline", contents)
        email_class.send_mail(message, recepients)
        print("email sent!")

    def run(self):
        """
        This is one of the main methods for this program. It calls a majority of the necessary classes and
        methods to collect and modify data.
        """
        self.sql_operations.connect_to_sql()
        logging.debug("SQL connected")

        create_dataframe = self.sql_operations.read_sql(self.create_sql_query)
        logging.debug("Create Sql read successfully")
        self.collibra_operations.create_assets(create_dataframe)

        update_dataframe = self.sql_operations.read_sql(self.update_sql_query)
        logging.debug("Update Sql read successfully")
        self.collibra_operations.update_attributes(update_dataframe)

        delete_dataframe = self.sql_operations.read_sql(self.delete_sql_query)
        logging.debug("Delete Sql read successfully")
        self.collibra_operations.delete_assets(delete_dataframe)

        update_display_name_dataframe = self.sql_operations.read_sql(self.update_display_name_query)
        logging.debug("Update Display Name Sql read successfully")
        self.collibra_operations.update_display_name(update_display_name_dataframe)

        logging.debug(
            "Assets Created Successfully: "
            + str(self.collibra_operations.create_assets_result)
        )
        logging.debug(
            "Attributes Created Successfully: "
            + str(self.collibra_operations.create_attributes_result)
        )
        logging.debug(
            "Assets Updated Successfully: "
            + str(self.collibra_operations.update_assets_result)
        )
        logging.debug(
            "Attributes Updated Successfully: "
            + str(self.collibra_operations.update_attributes_result)
        )
        logging.debug(
            "Assets Deleted Successfully: "
            + str(self.collibra_operations.delete_asset_result)
        )
        logging.debug(
            "Display Name Update Successfully: "
            + str(self.collibra_operations.update_display_name_result)
        )
        if (
            self.collibra_operations.create_assets_result
            and self.collibra_operations.update_assets_result
            and self.collibra_operations.update_attributes_result
            and self.collibra_operations.create_attributes_result
            and self.collibra_operations.delete_asset_result
            and self.collibra_operations.update_display_name_result
        ):
            logging.info("SQL To Collibra Pipeline has run successfully")
        else:
            logging.critical("Error when running the SQL to Collibra Pipeline")

        # Shutdown Logger
        logging.shutdown()

        try:
            self.prepare_and_send_email()
        except Exception as e:
            logging.error("Could not setup email: " + str(e))


if __name__ == "__main__":
    # Run main class
    main = MainClass("config.yml")
    main.run()
