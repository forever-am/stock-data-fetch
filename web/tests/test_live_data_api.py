from os import path
from datetime import datetime
from unittest import TestCase
import mock
from .. import live_data_api

TEST_DIR = path.dirname(__file__)

def _read_quote(filename):
    """
    Read the file containing json response of Google finance API
    :param filename: the name of the file
    :return:
    """
    with open(filename, "r") as quote_file:
        return quote_file.read()

def fetch_quote_data(ticker):
    """
    Retrive quote under Google finance API from a file
    :param ticker: the instrument ticker
    :return:
    """
    filename = path.join(TEST_DIR, "mock-stock-data", ticker + ".json")
    return _read_quote(filename)

def fetch_quote_txt_data(ticker):
    """
    Retrive quote under Google finance API from a file
    :param ticker: the instrument ticker
    :return:
    """
    filename = path.join(TEST_DIR, "mock-stock-data", ticker + ".txt")
    return _read_quote(filename)

class LiveDataAPITest(TestCase):
    GoogleTicker = "GOOG"

    def setUp(self):
        """
        Prepare env before running unit tests
        :return:
        """
        patch_get_quote = mock.patch(live_data_api.__name__
                                     + ".fetch_quote_json_data",
                                   side_effect=fetch_quote_data)
        self.m_fetch_quote_data = patch_get_quote.start()
        self.addCleanup(patch_get_quote.stop)

        patch_get_quote_date = mock.patch(live_data_api.__name__
                                     + ".fetch_quote_txt_data",
                                     side_effect=fetch_quote_txt_data)
        self.m_fetch_quote_data_date = patch_get_quote_date.start()
        self.addCleanup(patch_get_quote_date.stop)

    def test_realtime_data_reader(self):
        """
        Test if the dataframe contains realtime price in "Adj close" for the
        current day when end=None
        """
        ticker = self.GoogleTicker
        quote = live_data_api.fetch_live_quote(ticker)
        self.assertEqual(datetime(2017, 11, 2, 20, 0), quote[0])
        self.assertEqual(1031.26, quote[1])