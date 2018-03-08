from os import path, mkdir
from datetime import datetime
import pandas_datareader.data as web

HOME_DIR = path.expanduser("~")


def mkdir_if_not_exist(dir_path):
    if not path.exists(dir_path):
        mkdir(dir_path)


class Provider(object):

    start = "1926-01-01"

    @classmethod
    def from_source(cls, source):
        for sub_cls in cls.__subclasses__():
            sub_cls_source = sub_cls.__name__.split("Provider")[0]
            if source == sub_cls_source.lower():
                return sub_cls()

    def fetch_data(self, ticker, end):
        """Fetch a Pandas Dataframe from Internet"""
        pass


class YahooProvider(Provider):

    source = "yahoo"

    def fetch_data(self, ticker, end):
        return web.DataReader(ticker, self.source, start=self.start, end=end)


class DataReader(object):

    def __init__(self, cache_dir=None):
        self.cache_dir = cache_dir or path.join(HOME_DIR, "stock-data")
        mkdir_if_not_exist(self.cache_dir)

    def read(self, ticker, source="yahoo", end=None):
        end = end or datetime.now().date()
        filename = path.join(self.cache_dir, ticker + ".csv")
        provider = Provider.from_source(source)
        df = provider.fetch_data(ticker, end)
        df.to_csv(filename, header=True)
        return df


def data_reader(ticker, source="yahoo", end=None):
    return DataReader().read(ticker, source, end)
