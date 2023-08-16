import unittest
import pandas
import pytest
import yaml
from src.main import MainClass


class MainClassTest(unittest.TestCase):
    def setUp(self):
        self.main = MainClass("./src/tests/test_files/test_config.yml")
        print("Starting")

    def test_setup(self):
        print("Setup")

    def test_sql_format(self):
        with open("./src/tests/test_files/test_config.yml", "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        self.inactive_services_id = config["COLLIBRA_DETAILS"]["INACTIVE_SERVICES_DOMAIN_ID"]
        self.active_services_id = config["COLLIBRA_DETAILS"]["ACTIVE_SERVICES_DOMAIN_ID"]
        self.inactive_applications_id = config["COLLIBRA_DETAILS"]["INACTIVE_BUSINESS_APPLICATIONS_DOMAIN_ID"]
        self.active_applications_id = config["COLLIBRA_DETAILS"]["ACTIVE_BUSINESS_APPLICATIONS_DOMAIN_ID"]

        print(self.main.update_services_display_name_query)
