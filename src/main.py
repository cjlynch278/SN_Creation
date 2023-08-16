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
            self.inactive_services_id = config["COLLIBRA_DETAILS"]["INACTIVE_SERVICES_DOMAIN_ID"]
            self.active_services_id = config["COLLIBRA_DETAILS"]["ACTIVE_SERVICES_DOMAIN_ID"]
            self.inactive_applications_id = config["COLLIBRA_DETAILS"]["INACTIVE_BUSINESS_APPLICATIONS_DOMAIN_ID"]
            self.active_applications_id = config["COLLIBRA_DETAILS"]["ACTIVE_BUSINESS_APPLICATIONS_DOMAIN_ID"]
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
            self.inactive_services_id,
            self.environment,
            self.driver
        )
        logging.debug("Sql operations setup")

        self.services_collibra_operations = Collibra_Operations(
            self.inactive_services_id, self.environment, self.token_auth, config_file
        )
        self.applications_collibra_operations = Collibra_Operations(
            self.inactive_applications_id, self.environment, self.token_auth, config_file
        )

        logging.debug("Collibra Operations setup")

        with open(self.queries_location + "/create_query.sql", "r") as create_sql_file:
            self.create_services_sql_query = create_sql_file.read().format(
                self.inactive_services_id, self.active_services_id, "servicenow_cmbd_ci_service" , "sn_services"
            )
            self.create_applications_sql_query = create_sql_file.read().format(
                self.inactive_applications_id, self.active_services_id, "servicenow_cmbd_ci_business_app", "sn_business_apps"
            )

        with open(self.queries_location + "/update_query.sql", "r") as update_sql_file:
            self.update_services_sql_query = update_sql_file.read().format(
                self.inactive_services_id, self.active_services_id, "servicenow_cmbd_ci_service" , "sn_services"
            )
            self.update_applications_sql_query = update_sql_file.read().format(
                self.inactive_applications_id, self.active_services_id,"servicenow_cmbd_ci_business_app", "sn_business_apps"
            )

        with open(self.queries_location + "/delete_query.sql", "r") as delete_sql_file:
            self.delete_services_sql_query = delete_sql_file.read().format(
                self.inactive_services_id, self.active_services_id, "servicenow_cmbd_ci_service"
            )
            self.delete_applications_sql_query = delete_sql_file.read().format(
                self.inactive_applications_id, self.active_services_id, "servicenow_cmbd_ci_business_app"
            )

        with open(self.queries_location + "/update_display_name_query.sql", "r") as update_display_name_query_file:
            self.update_application_display_name_query = update_display_name_query_file.read().format(
                self.inactive_services_id, self.active_services_id, "servicenow_cmbd_ci_service"
            )
            self.update_services_display_name_query = update_display_name_query_file.read().format(
                self.inactive_applications_id, self.active_services_id, "servicenow_cmbd_ci_business_app"
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

        create_services_dataframe = self.sql_operations.read_sql(self.create_services_sql_query)
        logging.debug("Create services Sql read successfully")
        self.services_collibra_operations.create_assets(create_services_dataframe)

        create_applications_dataframe = self.sql_operations.read_sql(self.create_applications_sql_query)
        logging.debug("Create applications Sql read successfully")
        self.applications_collibra_operations.create_assets(create_applications_dataframe)

        update_services_dataframe = self.sql_operations.read_sql(self.update_services_sql_query)
        logging.debug("Update services Sql read successfully")
        self.services_collibra_operations.update_attributes(update_services_dataframe)

        update_applications_dataframe = self.sql_operations.read_sql(self.update_applications_sql_query)
        logging.debug("Update applications Sql read successfully")
        self.applications_collibra_operations.update_attributes(update_applications_dataframe)

        delete_services_dataframe = self.sql_operations.read_sql(self.delete_services_sql_query)
        logging.debug("Delete services Sql read successfully")
        self.services_collibra_operations.delete_assets(delete_services_dataframe)

        delete_applications_dataframe = self.sql_operations.read_sql(self.delete_applications_sql_query)
        logging.debug("Delete applications Sql read successfully")
        self.applications_collibra_operations.delete_assets(delete_applications_dataframe)

        update_services_display_name_dataframe = self.sql_operations.read_sql(self.update_services_display_name_query)
        logging.debug("Update services Display Name Sql read successfully")
        self.services_collibra_operations.update_display_name(update_services_display_name_dataframe)

        update_applications_display_name_dataframe = self.sql_operations.read_sql(self.update_application_display_name_query)
        logging.debug("Update application Display Name Sql read successfully")
        self.applications_collibra_operations.update_display_name(update_applications_display_name_dataframe)

        logging.debug(
            "Service Assets Created Successfully: "
            + str(self.services_collibra_operations.create_assets_result)
        )
        logging.debug(
            "Application Assets Created Successfully: "
            + str(self.applications_collibra_operations.create_assets_result)
        )
        logging.debug(
            "Service Attributes Created Successfully: "
            + str(self.services_collibra_operations.create_attributes_result)
        )
        logging.debug(
            "Application Attributes Created Successfully: "
            + str(self.applications_collibra_operations.create_attributes_result)
        )
        logging.debug(
            "Service Assets Updated Successfully: "
            + str(self.services_collibra_operations.update_assets_result)
        )
        logging.debug(
            "Application Assets Updated Successfully: "
            + str(self.applications_collibra_operations.update_assets_result)
        )
        logging.debug(
            "Service Attributes Updated Successfully: "
            + str(self.services_collibra_operations.update_attributes_result)
        )
        logging.debug(
            "Application Attributes Updated Successfully: "
            + str(self.applications_collibra_operations.update_attributes_result)
        )
        logging.debug(
            "Service Assets Deleted Successfully: "
            + str(self.services_collibra_operations.delete_asset_result)
        )
        logging.debug(
            "Application Assets Deleted Successfully: "
            + str(self.applications_collibra_operations.delete_asset_result)
        )
        logging.debug(
            "Service Display Names Update Successfully: "
            + str(self.services_collibra_operations.update_display_name_result)
        )
        logging.debug(
            "Application Display Names Update Successfully: "
            + str(self.applications_collibra_operations.update_display_name_result)
        )
        if (
            self.services_collibra_operations.create_assets_result
            and self.services_collibra_operations.update_assets_result
            and self.services_collibra_operations.update_attributes_result
            and self.services_collibra_operations.create_attributes_result
            and self.services_collibra_operations.delete_asset_result
            and self.services_collibra_operations.update_display_name_result
            and self.applications_collibra_operations.create_assets_result
            and self.applications_collibra_operations.update_assets_result
            and self.applications_collibra_operations.update_attributes_result
            and self.applications_collibra_operations.create_attributes_result
            and self.applications_collibra_operations.delete_asset_result
            and self.applications_collibra_operations.update_display_name_result
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
