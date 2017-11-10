from os import path, mkdir
from datetime import datetime
import pandas as pd
import pandas_datareader.data as web
from live_data_api import fetch_live_quote

HOME_DIR = path.expanduser("~")

def _today():
    return datetime.now().date()

def _market_data_filename(source, ticker):
    return ticker + "." + source + ".csv"

def mkdir_if_not_exist(dir_path):
    if not path.exists(dir_path):
        mkdir(dir_path)

class DataReader(object):
    origin = "1926-01-01"

    def __init__(self, cache_dir=None):
        self.cache_dir = cache_dir or path.join(HOME_DIR, "stock-data")
        mkdir_if_not_exist(self.cache_dir)

    def _read_cache(self, ticker, source, end=None):
        """
        Read market data from a file identified by <ticker>.<source>.csv
        :param ticker: the instrument ticker
        :param source: the source vendor of market data
        :param end: then end date of the time series range
        :return: a dataframe containing the desired market data
        """
        filename = path.join(self.cache_dir, _market_data_filename(source,
                                                                   ticker))
        if not path.isfile(filename):
            return None
        df = pd.read_csv(filename, parse_dates=True, index_col=0)
        return df.ix[:end]

    def read(self, ticker, source="yahoo", end=None, enable_cache=True):
        filename = path.join(self.cache_dir, _market_data_filename(source,
                                                                   ticker))
        today = _today()
        end = end or today
        quote = fetch_live_quote(ticker) if end == today else None
        df = None
        # Retrieve market data from cache
        df_cached = self._read_cache(ticker, source, end=end) if enable_cache else None
        if df_cached is not None:
            # Get the day after the last day of the ranged retrived from the
            # cache
            start = (df_cached.index[-1] + pd.DateOffset(1)).date()
            # Perform a web request only if start < end
            if str(start) <= end:
                df = web.DataReader(ticker, source, start=str(start), end=end)
                df = df_cached.append(df)
            else:
                df = df_cached
        else:
            df = web.DataReader(ticker, source, start=self.origin, end=end)
        df.to_csv(filename, header=True)
        if quote is not None:
            df.ix[end, "Adj Close"] = quote
        return df

def data_reader(ticker, source="yahoo", end=None, enable_cache=True):
    return DataReader().read(ticker, source, end, enable_cache)
