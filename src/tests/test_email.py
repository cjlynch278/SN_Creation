import unittest
from src.main import MainClass
from src.Email import Email_Class

class EmailTest(unittest.TestCase):
    def setUp(self):
        self.main = MainClass("src/tests/test_files/test_config.yml")


    def test_email(self):
        email_contents = "Test 123"
        email_class = Email_Class("smtp.wlgore.com", 25)
        email_class.send_mail(email_contents, "chlynch@wlgore.com")
        print("email sent!")
