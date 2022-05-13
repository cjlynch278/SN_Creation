import unittest
import pandas
import pytest

from src.main import MainClass


class MainClassTest(unittest.TestCase):
    def test_init(self):
        self.test_dataframe = pandas.read_csv("src/test.csv")
