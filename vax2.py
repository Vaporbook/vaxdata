import io, zipfile, csv, curses
import pylibs.pycurses_widgets


import fileinput
import sys
import argparse

argparser = argparse.ArgumentParser(description='Parse an html dump of an IA page for matching links')
argparser.add_argument('--file', dest='file',
                   default='',
                   help='file to analyze')

args = argparser.parse_args()

ns_set = set([])
data = {}
datasets = {}

def openZip(fn):

	rowcount = 0

	ns = 'UNKNOWN'

	z = zipfile.ZipFile(fn, 'r')

	for name in z.namelist():
		
		print 'Name {}'.format(name)

		if 'VAERSDATA' in name:
			print 'Found the VAERSDATA portion of zip'
			ns = 'DATA'			

		if 'VAERSVAX' in name:
			print 'Found the VAERSVAX portion of zip'
			ns = 'VAX'

		if 'VAERSSYMPTOMS' in name:
			print 'Found the VAERSSYMPTOMS portion of zip'
			ns = 'SYMP'			

		if ns not in data:
			data[ns] = {}
			rowcount = 0

		ns_set.add(ns)

		with z.open(name) as csvfile:
			deadparrot = csv.reader(csvfile)
			for row in deadparrot:

				# if we are initializing a new namespace,
				# grab the new schema and store it as metadata

				if rowcount == 0:
					data[ns]['schema'] = row
					datasets[ns] = {}
					print type(datasets[ns])

				# otherwise, use the data from the row
				# in a new dict and store it in the dataset

				else:
					rowdict = {}
					index = 0
					for label in data[ns]['schema']:
						rowdict[label] = row[index]
						index += 1
					hashid = hash(frozenset(rowdict.items()))
					datasets[ns][hashid] = rowdict

				# store the new count of rows stored

				rowcount += 1
				data[ns]['rows'] = rowcount


				# sys.stdout.write('.')
				#print row

	return data

z = openZip(args.file)

print z

# iterate on every data namespace
# and then every record in each namespace
# awaiting raw input to advance to next record or namespace

for item in ns_set:
	print item
	for key in datasets[item]:
		print key
		print datasets[item][key]
		raw_input()
