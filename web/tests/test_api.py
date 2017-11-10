from os import path
from shutil import rmtree
from unittest import TestCase
import mock
import pandas as pd
import pandas.util.testing as pdt
from datetime import datetime

from .. import api
from .. import live_data_api

TEST_DIR = path.dirname(__file__)


def _read_data_frame_csv(filename):
    return pd.read_csv(filename, parse_dates=True, index_col=0)

def web_reader(ticker, *args, **kwargs):
    filename = path.join(TEST_DIR, "mock-stock-data", ticker + ".csv")
    return _read_data_frame_csv(filename)

def _read_quote(filename):
    with open(filename, "r") as quote_file:
        return quote_file.read()

def fetch_quote_data(ticker):
    filename = path.join(TEST_DIR, "mock-stock-data", ticker + ".json")
    return _read_quote(filename)


@mock.patch(api.__name__ + ".mkdir")
def test_mkdir_if_not_exist(m_mkdir):
    dir_path = path.join(TEST_DIR, "dummy")
    api.mkdir_if_not_exist(dir_path)
    m_mkdir.assert_called_with(dir_path)


class DataReaderTest(TestCase):
    stock_data_dir = path.join(TEST_DIR, "stock-data")

    def setUp(self):
        api.HOME_DIR = TEST_DIR
        patch_data_reader = mock.patch(api.__name__ + ".web.DataReader",
                              side_effect=web_reader)
        self.m_web_reader = patch_data_reader.start()
        self.addCleanup(patch_data_reader.stop)
        self.reader = api.DataReader()

        patch_get_quote = mock.patch(live_data_api.__name__
                                     + ".fetch_quote_data",
                                   side_effect=fetch_quote_data)
        self.m_fetch_quote_data = patch_get_quote.start()
        self.addCleanup(patch_get_quote.stop)
        self.reader = api.DataReader()

    def test_cache_dir(self):
        self.assertEqual(self.reader.cache_dir, self.stock_data_dir)

    def test_data_reader(self):
        ticker = "GOOG"
        end = "2017-11-02"
        df = api.data_reader(ticker, end=end)
        self.m_web_reader.assert_called_with(
            ticker, "yahoo", start="1926-01-01", end=end
        )
        expected = web_reader(ticker)[:end]
        pdt.assert_frame_equal(df, expected)

    def test_realtime_data_reader(self):
        """
        Test if the dataframe contains realtime price in "Adj close" for the
        current day when end=None
        """
        ticker = "GOOG"
        pdt.assert_almost_equal(1031.26, live_data_api.fetch_live_quote(ticker))


    @classmethod
    def tearDownClass(cls):
        rmtree(cls.stock_data_dir)
