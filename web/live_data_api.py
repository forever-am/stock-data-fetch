"""
This file contains the api to fetch realtime price of a given instrument
"""

#from googlefinance import getQuotes
import json
from datetime import datetime

try:
    from urllib.request import Request, urlopen
except ImportError:  # python 2
    from urllib2 import Request, urlopen


def fetch_live_quote(ticker):
    """
    Fetch "realtime" (15-minute-delayed) quote from the
    :param ticker: The instrument ticker
    :return: The quote and the date of this quote
    """
    #google realtime api is no more working since 2017-09-07
    #return getQuotes(ticker)
    quote_ts = int(get_google_quote_date(ticker))
    date = datetime.fromtimestamp(quote_ts)
    quote = float(get_google_delayed_quote(ticker))
    return (date, quote) if (date is not None) and (quote is not None) else None

def get_google_delayed_quote(ticker):
    """
    Retrieve 15-min-delayed quote from the Google API
    :param ticker: The instrument ticker
    :return: The quote formatted like XXX.xx
    """
    data = fetch_quote_json_data(ticker).decode('ascii', 'ignore').strip()
    content = json.loads(data[3:])
    return content[0]["l"].replace(',', '')

def fetch_quote_json_data(ticker):
    """
    Download data of the given ticker containing price, volume..
    :param ticker: the ticker of the instrument
    :return: the raw data
    """
    url = "https://finance.google.com/finance?q=%s&output=json" % ticker
    request = Request(url)
    resp = urlopen(request)
    return resp.read()

def get_google_quote_date(ticker):
    """
    Retrieve 15-min-delayed quote from the Google price API
    :param ticker: The instrument ticker
    :return: The quote formatted like XXX.xx
    """
    data = fetch_quote_txt_data(ticker).decode('ascii', 'ignore')
    return _parse_google_txt_quote_result(data)

def fetch_quote_txt_data(ticker):
    """
    Download data of the given ticker containing price, volume..
    :param ticker: the ticker of the instrument
    :return: the raw data
    """
    url = "https://finance.google.com/finance/getprices?q=%s&p=1d&f=d,o,h,l,c,v"\
          % ticker
    request = Request(url)
    resp = urlopen(request)
    return resp.read()

def _parse_google_txt_quote_result(data):
    lines = data.splitlines()
    quote_marker = False
    for line in lines:
        if quote_marker:
            return line[1:].split(",")[0]
        quote_marker = line.startswith("TIMEZONE_OFFSET")

    return None