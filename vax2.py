import io, zipfile, csv, json
import fileinput
import sys
import argparse
import sqlite3
import elasticsearch
from dateutil.parser import parse as parsedate

# CLI args
argparser = argparse.ArgumentParser(description='Parse an html dump of an IA page for matching links')
argparser.add_argument('--file', dest='file',
                   default='',
                   help='file to analyze')
args = argparser.parse_args()

# use sqlite db for relational storage
# and get a connection to it
# TODO drop existing db first?
sqldbname = 'vaers.db'
conn = sqlite3.connect(sqldbname)

# unsure if this is needed, but added
# while debugging some issues with
# inserting text column data
conn.text_factory = str

# set of namespaces for extracted data
# each one will correspond to a SQL table
# or a type in elasticsearch
ns_set = set([])

# data about the data, such as rowcounts and schema
metadata = {}

# storage of the actual data, keyed by namespaces
datasets = {}

# elasticsearch, prefix and instance
es_prefix = 'vaers-'
es = elasticsearch.Elasticsearch()

# main function to open a zipped data file
# zipped vaers files seem to contain csv files
# but so far have only tested this with most recent
def openZip(fn):


    ns = 'UNKNOWN'
    z = zipfile.ZipFile(fn, 'r')
    for name in z.namelist():

        print 'Name {}'.format(name)

        # set up the namespace
        # make the table names sensible ones
        ns = ns_from_name(name)
        ns_set.add(ns)
        #if ns not in metadata:
        metadata[ns] = {}
        metadata[ns]['rows'] = 0

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

            if metadata[ns]['rows'] == 0:
                metadata[ns]['schema'] = row
                datasets[ns] = {}
                db_create_if_not_exists( conn, ns, metadata[ns]['schema'] )
                es.index(index=es_prefix+'docs', doc_type='schema', body={
                        'schema':metadata[ns]['schema']
                })
                

            # otherwise, use the data from the row
            # in a new dict and store it in the dataset

            else:
                rowdict = {}
                index = 0

                # first place that we iterate the csv data
                # just key it in the order we find it
                # TODO improve the type handling here
                for label in metadata[ns]['schema']:
                    rowdict[label] = row[index]
                    index += 1

                # generate a unique id for internal use only
                hashid = hash(frozenset(rowdict.items()))
                datasets[ns][hashid] = rowdict

                db_insert(conn, ns, rowdict)

                # this is a fix for using elasticsearch which requires
                # valid JSON input - to keep the JSON dump from crapping
                # out at either end, we have to use the unicode() method
                # this is after trying way too many other recommended fixes
                # using codec and other stuff!
                for key in rowdict.keys():
                    rowdict[key] = unicode(rowdict[key], errors='replace')

                # add the doc to elasticsearch
                es.index(index=es_prefix+'docs', doc_type=ns.lower(), body=rowdict)

            # store the new count of rows stored
            metadata[ns]['rows'] += 1

        # row iteration end
    # zip files iteration end
    # close db connection
    conn.close()

    return metadata


# create the table for storage in sqlite

def db_create_if_not_exists(conn, ns, schema):

    # Create table with this schema
    c = conn.cursor()
    c.execute(db_table_statement(ns,schema))
    conn.commit()
    return c.rowcount

# insert dict data into table

def db_insert(conn, ns, rowdict):
    c = conn.cursor()
    c.execute(
        db_build_insert(ns,rowdict),
        tuple(rowdict.values())
    )
    conn.commit()
    return 1

# build table creation sql

def db_table_statement(ns,schema):
    table_statement = [ ]
    for key in schema:
        table_statement.append( key+' text' )
    table_statement = '''CREATE TABLE IF NOT EXISTS ''' + ns  + ''' ('''+ ", ".join(table_statement) + ''')'''
    return table_statement


# the sqlite insert query builder

def db_build_insert(ns,rowdict):

    names = " (" + ",".join( rowdict.keys() ) + ") "
    query = "INSERT INTO " + ns + names + " VALUES "
    formatlist = ["?"]*len(rowdict.values())
    query += "("+",".join(formatlist)+")"
    return query

# iterate on every data namespace
# and then every record in each namespace
# awaiting raw input to advance to next record or namespace

def browse_data(ns_set):

    for item in ns_set:
        for key in datasets[item]:
            print key
            print datasets[item][key]
            raw_input()

# get a db table name from the filename

def ns_from_name(name):
    
    ns = 'SYMP'

    if 'VAERSDATA' in name:
        print 'Found the VAERSDATA portion of zip'
        ns = 'people'         

    if 'VAERSVAX' in name:
        print 'Found the VAERSVAX portion of zip'
        ns = 'vaccines'

    if 'VAERSSYMPTOMS' in name:
        print 'Found the VAERSSYMPTOMS portion of zip'
        ns = 'symptoms'
    
    return ns


# open it

z = openZip(args.file)

# dump the metadata

print z

# browse_data(ns_set)





