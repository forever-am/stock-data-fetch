# stock-data-fetch

[![Build Status](https://travis-ci.org/jdborowy/stock-data-fetch.svg?branch=master)](https://travis-ci.org/jdborowy/stock-data-fetch)
[![codecov](https://codecov.io/gh/jdborowy/stock-data-fetch/branch/master/graph/badge.svg)](https://codecov.io/gh/jdborowy/stock-data-fetch)
[![Maintainability](https://api.codeclimate.com/v1/badges/20c53f8fe1c6191e1a57/maintainability)](https://codeclimate.com/github/jdborowy/stock-data-fetch/maintainability)
[![Python27](https://img.shields.io/badge/python-2.7-blue.svg)](https://travis-ci.org/fcalice/stock-data-fetch)
[![Python35](https://img.shields.io/badge/python-3.5-blue.svg)](https://travis-ci.org/fcalice/stock-data-fetch)

This is an exercise to fetch stock data from different sources with cache and sanity check,
it supports both Python 2 and 3. Feel free to create an issue if you have any question or remark.

### Run PR checks on local machine
- Check if the code respects pep8 by running the command `pep8 web` in `.`
- Run the unit tests and coverage check with
```bash
nosetests . --with-coverage --cover-package=. --cover-html --cover-erase
```

### Features

- Implements pandas_datareader API enabling a cache by data source and for the reference 
- Build a reference source populated and improved by the raw sources
- Update google source with a live quote from google realtime API
- Build the reference source aggregating all the available sources following a given strategy. (average based strategy implemented as default) 

### API

A simple code to fetch the historical market data of Apple
```python
from web import data_reader

df_aapl = data_reader("AAPL")
```

To retrieve the same data from Quandl source
```python
from web import data_reader

df_aapl = data_reader("AAPL", source="quandl")
```

To get the same data as well but from google source and adding the live quote
```python
from web import data_reader

df_aapl = data_reader("AAPL", source="google-realtime")
```

To get the same data but until the 1st of September 2017
```python
from web import data_reader

df_aapl = data_reader("AAPL", end="2017-09-01")
```

To build the reference source taking all available sources
```python
from web import data_reader

df_aapl = data_reader("AAPL", source="all")
```
