import unittest
import pandas
import pytest
import yaml
import os
import json
from src.main import MainClass

from src.SQLOperations import SQLOperations


class SqlOperationsTest(unittest.TestCase):
    def setUp(self):

        self.main = MainClass("src/tests/test_files/test_config.yml", "src/queries")

        self.test_dataframe = pandas.read_csv("./src/tests/test_files/test.csv")
        self.empty_test_df = pandas.DataFrame()

    def test_sql_format(self):
        print("setup")
        print(self.main.create_sql_query)

    def dtest_to_delete(self):
        self.test_dataframe = pandas.read_csv("src/tests/test_files/create_1.csv")
        self.test_dataframe = self.test_dataframe.drop("Unnamed: 0", axis=1)
        self.test_dataframe.to_csv("src/tests/test_files/create_1.csv", index=False)
        self.test_dataframe = pandas.read_csv(
            "src/tests/test_files/full_asset_test.csv"
        )
        self.test_dataframe = self.test_dataframe.drop("Unnamed: 0", axis=1)

        self.test_dataframe.to_csv(
            "src/tests/test_files/full_asset_test.csv", index=False
        )
        self.test_dataframe = pandas.read_csv("src/tests/test_files/six_test.csv")
        self.test_dataframe = self.test_dataframe.drop("Unnamed: 0", axis=1)

        self.test_dataframe.to_csv("src/tests/test_files/six_test.csv", index=False)
        self.test_dataframe = pandas.read_csv("src/tests/test_files/test.csv")
        self.test_dataframe = self.test_dataframe.drop("Unnamed: 0", axis=1)

        self.test_dataframe.to_csv("src/tests/test_files/test.csv", index=False)
        print("Done")
