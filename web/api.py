from os import path, mkdir
from datetime import datetime
import pandas_datareader.data as web
from live_data_api import fetch_live_quote

HOME_DIR = path.expanduser("~")

def _today():
    return datetime.now().date()

def mkdir_if_not_exist(dir_path):
    if not path.exists(dir_path):
        mkdir(dir_path)

class DataReader(object):
    start = "1926-01-01"

    def __init__(self, cache_dir=None):
        self.cache_dir = cache_dir or path.join(HOME_DIR, "stock-data")
        mkdir_if_not_exist(self.cache_dir)

    def read(self, ticker, source="yahoo", end=None):
        filename = path.join(self.cache_dir, ticker + ".csv")
        quote = fetch_live_quote(ticker) if end is None else None
        end = end or _today()
        df = web.DataReader(ticker, source, start=self.start, end=end)
        if quote is not None:
            df.ix[end, "Adj Close"] = quote
        df.to_csv(filename, header=True)
        return df


def data_reader(ticker, source="yahoo", end=None):
    return DataReader().read(ticker, source, end)
