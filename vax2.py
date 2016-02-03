import io, zipfile, codecs, csv, curses, json

# pycurses not used at the moment
# ... but was before and may be again
import pylibs.pycurses_widgets

import fileinput
import sys
import argparse

import sqlite3

import elasticsearch


conn = sqlite3.connect('vaers.db')
conn.text_factory = str

argparser = argparse.ArgumentParser(description='Parse an html dump of an IA page for matching links')
argparser.add_argument('--file', dest='file',
                   default='',
                   help='file to analyze')

argparser.add_argument('--namespace', dest='ns',
                   default='SYMP',
                   help='limit output to one dataset (namespace)')


args = argparser.parse_args()

ns_set = set([])
data = {}
datasets = {}

es = elasticsearch.Elasticsearch()

def openZip(fn):

    c = conn.cursor()

    rowcount = 0

    ns = 'UNKNOWN'

    z = zipfile.ZipFile(fn, 'r')

    for name in z.namelist():

        print 'Name {}'.format(name)

        ns = ns_from_name(name) 

        if ns not in data:
            data[ns] = {}
            rowcount = 0

        ns_set.add(ns)


        # added this to handle non-json safe strings but it
        # did not fix the issue with inserting into elasticsearch!
        # leaving it for now in case we want to use for some other purpose

        def unicode_safe_row_generator(name):

            with z.open(name, 'rU') as csvfile:

             #   print csvfile.encoding
           
                for row in csv.reader(csvfile):

                    yield [e for e in row]


        for row in unicode_safe_row_generator(name):

                # if we are initializing a new namespace,
                # grab the new schema and store it as metadata

                if rowcount == 0:
                    data[ns]['schema'] = row
                    datasets[ns] = {}
                    db_create_if_not_exists( c, ns, data[ns]['schema'] )

                    es.index(index='schema_'+ns.lower(), doc_type='schema', body={
                            'schema':data[ns]['schema']
                    })
                    

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

                    # Insert data

                    names = rowdict.keys()
                    names = " ("+",".join(names)+") "

#                    rowdict_values = rowdict.values()
                    query = "INSERT INTO " + ns + names + " VALUES "

                    # print query

                    formatlist = ["?"]*len(rowdict.values())
    
                    # print formatlist

                    values = "("+",".join(formatlist)+")"

                    query += values;

                    print query

                    c.execute(query,tuple(rowdict.values()))
                    
                    # commit the changes
                    
                    conn.commit()


                    # this is a fix for using elasticsearch which requires
                    # valid JSON input - to keep the JSON dump from crapping
                    # out at either end, we have to use the unicode() method

                    for key in rowdict.keys():
                        rowdict[key] = unicode(rowdict[key], errors='replace')

                    es.index(index=ns.lower(), doc_type='textrow', body=rowdict)

                # store the new count of rows stored

                rowcount += 1
                data[ns]['rows'] = rowcount


                # sys.stdout.write('.')
                #print row

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()

    return data


def db_create_if_not_exists(cursor, ns, schema):

    c = cursor


    table_statement = [ ]

    for key in schema:
        table_statement.append( key+' text' )

    table_statement = '''CREATE TABLE IF NOT EXISTS ''' + ns  + ''' ('''+ ", ".join(table_statement) + ''')'''

    print table_statement

    # Create table with this schema
    c.execute(table_statement)
    conn.commit()


    return c.rowcount

# iterate on every data namespace
# and then every record in each namespace
# awaiting raw input to advance to next record or namespace

def browse_data(ns_set):

    for item in ns_set:
        if args.ns in item:
            print item
            for key in datasets[item]:
                print key
                print datasets[item][key]
                raw_input()


# get a db table name from the filename

def ns_from_name(name):
    
    ns = 'SYMP'

    if 'VAERSDATA' in name:
        print 'Found the VAERSDATA portion of zip'
        ns = 'DATA'         

    if 'VAERSVAX' in name:
        print 'Found the VAERSVAX portion of zip'
        ns = 'VAX'

    if 'VAERSSYMPTOMS' in name:
        print 'Found the VAERSSYMPTOMS portion of zip'
        ns = 'SYMP'
    
    return ns



z = openZip(args.file)

# dump the metadata

print z

# browse_data(ns_set)





