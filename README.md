# stock-data-fetch

[![Build Status](https://travis-ci.org/fcalice/stock-data-fetch.svg?branch=master)](https://travis-ci.org/fcalice/stock-data-fetch)
[![codecov](https://codecov.io/gh/fcalice/stock-data-fetch/branch/master/graph/badge.svg)](https://codecov.io/gh/jdborowy/stock-data-fetch)
[![Maintainability](https://api.codeclimate.com/v1/badges/5b83db2c856ce54d95e4/maintainability)](https://codeclimate.com/github/jdborowy/stock-data-fetch/maintainability)
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
