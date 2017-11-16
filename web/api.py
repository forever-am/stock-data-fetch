from os import path, mkdir
from datetime import datetime
import math
import numpy as np
import pandas as pd
import pandas_datareader.data as web

from .live_data_api import fetch_live_quote
from .aggregation_strategies import average_strategy

HOME_DIR = path.expanduser("~")

def _today():
    return datetime.now().date()

def _market_data_filename(source, ticker):
    if source == "reference":
        return ticker + ".csv"
    else:
        return ticker + "." + source + ".csv"

def mkdir_if_not_exist(dir_path):
    if not path.exists(dir_path):
        mkdir(dir_path)

# def _merge_values(default_value, value):
#    return value if math.isnan(default_value) else default_value

# def _merge_series(serie_a, serie_b):
#    def merge_values(value):
#        return _merge_values(value, )
#    return serie_a.apply()

class DataReader(object):
    OpenCol = "Open"
    CloseCol = "Close"
    AdjCloseCol = "Adj Close"
    HighCol = "High"
    LowCol = "Low"
    VolumeCol = "Volume"

    origin = "1926-01-01"
    Sources = ["yahoo", "google-realtime", "quandl"]
    ReferenceSource = "reference"

    def __init__(self, cache_dir=None, enable_cache=True, use_reference=True):
        """

        :param cache_dir:
        :param enable_cache:
        :param use_reference:
        """
        self.cache_dir = cache_dir or path.join(HOME_DIR, "stock-data")
        self.enable_cache = enable_cache
        self.use_reference = use_reference
        mkdir_if_not_exist(self.cache_dir)

    def _read_cache(self, ticker, source):
        """
        Read market data from a file identified by <ticker>.<source>.csv
        :param ticker: the instrument ticker
        :param source: the source vendor of market data
        :return: a data frame containing the desired market data
        """
        filepath = path.join(self.cache_dir, _market_data_filename(source,
                                                                   ticker))
        if not path.isfile(filepath):
            return None
        return pd.read_csv(filepath, parse_dates=True, index_col=0)

    def _fetch_web_data(self, ticker, source, start, end):
        """
        Fetch data from the web using pandas web api and normalize data casting
        all columns as float64 and filling out Adj Close if the column doesn't
        exist
        :param ticker: the instrument ticker
        :param source: the source vendor of market data
        :param start: then begin date of the time series range
        :param end: then end date of the time series range
        :return: a data frame containing the web normalized market data
        """
        web_df = web.DataReader(ticker, source, start=start, end=end)
        web_df[self.OpenCol] = web_df[self.OpenCol].astype(np.float64)
        web_df[self.CloseCol] = web_df[self.CloseCol].astype(np.float64)
        web_df[self.HighCol] = web_df[self.HighCol].astype(np.float64)
        web_df[self.LowCol] = web_df[self.LowCol].astype(np.float64)
        web_df[self.VolumeCol] = web_df[self.VolumeCol].astype(np.float64)
        if self.AdjCloseCol in web_df.columns:
            web_df[self.AdjCloseCol] =\
                web_df[self.AdjCloseCol].astype(np.float64)
        else:
            web_df[self.AdjCloseCol] = web_df[self.CloseCol]
        return web_df[[self.OpenCol, self.HighCol, self.LowCol, self.CloseCol,
                       self.AdjCloseCol, self.VolumeCol]].loc[:end]

    def _read_raw_data(self, ticker, source, start, end):
        """
        Fetch data from the cache as much as possible and using the web api
        to retrieve the missing data
        :param ticker: the instrument ticker
        :param source: the source vendor of market data
        :param start: then begin date of the time series range
        :param end: then end date of the time series range
        :return: a data frame containing the market data
        """
        source = "google" if source == "google-realtime" else source

        if not self.enable_cache:
            return self._fetch_web_data(ticker, source, start=start, end=end)

        cache_df = self._read_cache(ticker, source)
        if cache_df is None or len(cache_df) == 0:
            return self._fetch_web_data(ticker, source, start=start, end=end)

        cache_end = str(cache_df.index[-1].date())
        if cache_end > end:
            return cache_df.loc[:end]

        web_df = self._fetch_web_data(ticker, source, start=str(cache_end),
                                      end=end)
        return web_df.combine_first(cache_df)

    def _read_raw_data_multi_sources(self, ticker, sources, start, end):
        """
        Read data from several sources
        :param ticker: the instrument ticker
        :param source: the source vendor of market data
        :param start: then begin date of the time series range
        :param end: then end date of the time series range
        :return: a dictionary under format source: dataframe
        """
        return {source: self._read_raw_data(ticker, source, start, end)
               for source in sources}

    def _save_raw_data(self, ticker, source, df):
        """
        Save the data frame under the cache folder with the following format:
        <ticker>.<source>.csv if the data come from one identified source
        <ticker>.csv is it's the reference data
        :param ticker: the instrument ticker
        :param source: the source vendor of market data
        :param df: the data frame to save
        :return:
        """
        filename = path.join(self.cache_dir, _market_data_filename(source,
                                                                   ticker))
        df.to_csv(filename, header=True)

    def _combine_ref_and_raw_data(self, ref_df, raw_df):
        """
        Combine the reference data frame and the raw data frame coming from an
        unique source. Here we just use the the raw data when the reference data
        are missing.
        In a further version we would apply a strategy to improve the reference
        with the raw data
        :param ref_df: the reference data frame
        :param raw_df: the raw data frame
        :return: the combined data frame
        """
        if ref_df is None:
            return raw_df

        # Update the reference data with the data of the source before the
        # current start of the reference
        ref_start = str(ref_df.index[0].date())
        ref_df = ref_df.combine_first(raw_df.loc[:ref_start])

        ref_end = str(ref_df.index[-1].date())
        # Backward propagate Adj Close: the priority is given to the raw data
        ref_df["Adj Close"] =\
            raw_df.combine_first(ref_df).loc[:ref_end]["Adj Close"]

        # Update the reference data with the data of the source after the
        # current end of the reference.
        return ref_df.combine_first(raw_df.loc[ref_end:])

    def _update_with_live_quote(self, ticker, df):
        """
        Update data frame with a live quote provided by google realtime api if
        the quote date (today) correspond to the last row of the dataframe
        :param ticker: the instrument ticker
        :param df: the data frame to update
        :return:
        """
        quote = fetch_live_quote(ticker)

        if quote is not None:
            quote_date = quote[0].date()
            quote_value = quote[1]
            if df.index[-1].date() == quote_date:
                df.loc[quote_date, self.AdjCloseCol] = quote_value

    def read(self, ticker, source="yahoo", end=None):
        """
        API to fetch source or reference data from the cache and/or the web when
        needed.
        :param ticker: the instrument ticker
        :param source: the source vendor of market data
        :param end: then end date of the time series range
        :return:
        """
        today = str(_today())
        end = end or today

        raw_df = self._read_raw_data(ticker, source, start=self.origin, end=end)
        if end == today and source == "google-realtime":
            self._update_with_live_quote(ticker, raw_df)
        self._save_raw_data(ticker, source, raw_df)
        df = raw_df

        if self.use_reference:
            ref_df = self._read_cache(ticker, self.ReferenceSource)
            df = self._combine_ref_and_raw_data(ref_df, raw_df)

        self._save_raw_data(ticker, self.ReferenceSource, df)

        return df

    def read_multi_sources(self, ticker, sources=Sources, end=None,
                          aggregation_strategy=average_strategy):
        """
        Read the market data from several sources and aggregate them into a
        reference following the given strategy
        :param ticker: the instrument ticker
        :param source: the source vendor of market data
        :param start: then begin date of the time series range
        :param end: then end date of the time series range
        :param aggregation_strategy: a function which aggregate a list of df into one df
        :return: the aggregated reference
        """
        today = str(_today())
        end = end or today

        dfs = self._read_raw_data_multi_sources(ticker, sources,
                                                self.origin, end)
        for source, raw_df in dfs.items():
            self._save_raw_data(ticker, source, raw_df)
        ref_df = aggregation_strategy(list(dfs.values()))
        self._save_raw_data(ticker, self.ReferenceSource, ref_df)
        return ref_df


def data_reader(ticker, source="yahoo", end=None,
                enable_cache=True, use_reference=True):
    reader = DataReader(enable_cache=enable_cache, use_reference=use_reference)
    if source == "all":
        return reader.read_multi_sources(ticker, end=end)
    else:
        return reader.read(ticker, source, end)
