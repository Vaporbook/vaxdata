import io, zipfile, csv, json
import fileinput
import sys
import argparse
import sqlite3
import elasticsearch
import datetime
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


def openZip(fn):
    """
    main function to open a zipped data file
    zipped vaers files seem to contain csv files
    but so far have only tested this with most recent
    """
    
    es.indices.delete(index=es_prefix+'docs', ignore=[400, 404])

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


        # cast to the right type
        # we could define a VAERS schema, but
        # for now just have some baseline sanity on values

        def right_type(value):

            # just handle floats and strings for now
            # integers will convert to floats
            # dates will convert to strings

            try:

                v = float(value)

                # okay, can be cast as float, but:
                # this means numeric data ids will now be
                # floats, which could cause x-ref issues
                # TODO investigate?

                # infinity is a float that cannot be a JSON value
                if(float("inf") == v):
                    return value
                else:
                    return v
            except ValueError:
                return unicode(value, errors='replace')


        # added this to handle non-json safe strings but it
        # did not fix the issue with inserting into elasticsearch!
        # leaving it for now in case we want to use for some other purpose

        def unicode_safe_row_generator(name):

            with z.open(name, 'rU') as csvfile:

             #   print csvfile.encoding
           
                for row in csv.reader(csvfile):

                    try:

                        yield [right_type(value) for value in row]
                    
                    except ValueError as err:

                        print err.message
                        print e


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

                # do some transformation before sending to ES

                for key in rowdict.keys():

                    # follow ES dynamic mapping convention for dates

                    if 'DATE' in key:
                        try:
                            rowdict[key] = parsedate(rowdict[key]).strftime('%Y-%m-%d')
                        except ValueError as err:
                            rowdict[key] = None

                    elif key == 'VAERS_ID':
                        # convert known ID values back to strings
                        rowdict[key] = str(int(rowdict[key]))

                print "Doc to add to ES:"
                print rowdict

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





