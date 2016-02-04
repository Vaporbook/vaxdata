


# VaxData
## Vaccine Adverse Reaction Data
[Licensed under the MIT License]

### summary

A project to parse and investigate public data on adverse reactions to vaccinations.

This is an exercise for me in understanding Python data structures and ETL patterns. Hopefully it will turn into something more.

### requirements

	-python 2.7+
	-elasticsearch
	-VAERS data (see below for URL)
	-some pip installs - this is changing a lot, so for now you'll just have to feel your way through those

### testing

Note on the scripts: vax.py is an early version playing with pandas and pycurses and will probably be removed. vax2.py is the current working version and should be the one you test with.

Running vax2.py will look for the VAERS zipfile you specify, open it, and parse each CSV found within, building three things as it goes: 1. an internal model of the data, 2. a sqlite relational db, and 3. an elasticsearch document store.

To try it out for yourself ...

Obtain VAX data zips at:

	https://vaers.hhs.gov/data/data

Usage eg:

	./vax2.py --file ./2015VAERSData.zip



