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

def saystuff(pandasData):
	"""say Stuff"""
	print pandasData.info()
	print pandasData.head()
	print pandasData.index
	print pandasData.columns
	print pandasData.values
	print pandasData.describe()
	print pandasData[0:6]
	#for pd in pandasData:
		#pp.pprint(pandasData[pd])



def mainstuff(stdscr):

	stdscr.erase()
	stdscr.box(0,0)

	stdscr.addstr(1,1, '++++++++++++++++++++++++++++/////////////////PROGRAM///////////////+++++++++++++++++++++')

	#stdscr.addstr(59,1, '++++++++++++++++++++++++++++++////////////////////////////////')

	curses.curs_set(0)
	zipfilename = './2015VAERSData.zip'


	def drawAndPoll(row):

		stdscr.addstr(1,1,', '.join(row), curses.A_REVERSE)
		stdscr.refresh()

		return stdscr.getch()


	def openZip(fn):

		data = {}
		z = zipfile.ZipFile(fn, 'r')
		for name in z.namelist():
			
			print 'Name {}'.format(name)

			with z.open(name) as csvfile:
				deadparrot = csv.reader(csvfile)
				for row in deadparrot:

					k = drawAndPoll(row)
					if k == ord('n'):
						continue
					else:
						break
			#opencsv(name)
			#data[name] = pandas.read_csv(z.open(name))

		return data


	z = openZip(zipfilename)
	



curses.wrapper(mainstuff)
