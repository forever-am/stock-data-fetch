"""
This file contains the api to fetch realtime price of a given instrument
"""

#from googlefinance import getQuotes
import json
try:
    from urllib.request import Request, urlopen
except ImportError:  # python 2
    from urllib2 import Request, urlopen


def fetch_live_quote(ticker):
    """
    Fetch "realtime" (15-minute-delayed) quote from the
    :param ticker: The instrument ticker
    :return: The quote
    """
    #google realtime api is no more working since 2017-09-07
    #return getQuotes(ticker)
    return float(get_google_delayed_quote(ticker))

def get_google_delayed_quote(ticker):
    """
    Retrieve 15-min-delayed quote from the Google API
    :param ticker: The instrument ticker
    :return: The quote formatted like XXX.xx
    """
    data = fetch_quote_data(ticker).decode('ascii', 'ignore').strip()
    content = json.loads(data[3:])
    return content[0]["l"].replace(',', '')

def fetch_quote_data(ticker):
    """
    Download data of the given ticker containing price, volume..
    :param ticker: the ticker of the instrument
    :return: the raw data
    """
    url = "https://finance.google.com/finance?q=%s&output=json" % ticker
    request = Request(url)
    resp = urlopen(request)
    return resp.read()
