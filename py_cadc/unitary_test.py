import os
import unittest
from dotenv import load_dotenv

from cdc import CDC

load_dotenv()


class TestCDCMethods(unittest.TestCase):
    def test_create_cdc_object(self):
        self.host = os.getenv("HOST")
        self.passwd = os.getenv("PASSWORD")
        self.user = os.getenv("USER")
        self.db_name = os.getenv("DB")
        try:
            CDC(self.host, self.user, self.passwd, self.db_name)
        except Exception as error:
            self.assertNotEqual(error, "")

    def test_check_sqlconnection(self):
        self.host = os.getenv("HOST")
        self.passwd = os.getenv("PASSWORD")
        self.user = os.getenv("USER")
        self.db_name = os.getenv("DB")
        cdc = CDC(str(self.host), str(self.user), str(self.passwd), str(self.db_name))
        self.assertEqual(cdc.check_odbc_connection(), False)


if __name__ == "__main__":
    unittest.main()
