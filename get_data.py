#!/usr/bin/env python3

# script to download and unpack imdb data files

import argparse
import gzip
import os
import psycopg2
import traceback
import urllib.request

parser = argparse.ArgumentParser(description='IMDB data updater script')
parser.add_argument('--do-get', dest='do_get', action='store_true')
parser.add_argument('--test-data', dest='test_data', action='store_true')
parser.add_argument('--drop-tables', dest='drop_tables', action='store_true')

args = parser.parse_args()

# size of test data files
head_size = 20

if args.do_get:
    print("checking if we need to fetch or unpack data")
    # get data
    file_names = ["title.principals.tsv.gz",
                  "title.basics.tsv.gz", "name.basics.tsv.gz"]
    for f_name in file_names:
        data_file_name = f_name.rstrip(".gz")
        test_data_file_name = data_file_name.replace(".tsv", ".test.tsv")

        # download data archive if it does not exist
        if not os.path.isfile(f_name):
            print("downloading " + f_name)
            urllib.request.urlretrieve(
                "https://datasets.imdbws.com/%s" % f_name, f_name)
        # extract archive if uncompressed data file does not exist
        if not os.path.isfile(data_file_name):
            with open(f_name), gzip.open(f_name, 'rb') as f_archive:
                data = f_archive.read()
                # remove first line of data so it can be properly imported
                # using COPY
                s = data.decode("utf-8").split("\n", 1)[1]
                with open(data_file_name, 'w') as f_data_file:
                    f_data_file.write(s)

                # TODO: head data files into .test.tsv data files
                with open(test_data_file_name, 'w') as f_test_data_file:
                    head_s = '\n'.join(s.split("\n")[0:head_size + 1])
                    f_test_data_file.write(head_s)

# do_get == false assumes unarchived data files exist


def db_title_principals(cursor, f_name):
    print("beginning title_principals db update")

    # TODO: add index on nconst
    cursor.execute("""CREATE TABLE "title_principals" (
    tconst text,
    ordering int,
    nconst text,
    category text,
    job text,
    characters text,
    PRIMARY KEY(tconst, ordering)
    );""")

    cursor.execute("""COPY title_principals FROM '%s'""" %
                   (os.path.join(os.getcwd(), f_name)))
    conn.commit()

    # as a test
    cursor.execute(
        """SELECT * FROM title_principals WHERE tconst = 'tt0000001';""")

    rows = cursor.fetchall()
    print(rows)
    print("completed title_principals db update")


def db_title_basics(cursor, f_name):
    print("beginning title_basics db update")

    cursor.execute("""CREATE TABLE "title_basics" (
    tconst                text primary key,
    titleType            text,
    primaryTitle        text,
    originalTitle        text,
    isAdult                int,
    startYear            int,
    endYear                int,
    runtimeMinutes        int,
    genres                text
    );""")

    cursor.execute("""COPY title_basics FROM '%s'""" %
                   (os.path.join(os.getcwd(), f_name)))
    conn.commit()

    # as a test
    cursor.execute(
        """SELECT * FROM title_basics WHERE tconst = 'tt0000009';""")
    rows = cursor.fetchall()
    print(rows)
    print("completed title_basics db update")

# parsing data into arrays is probably uneccessary
# TODO: refactor out some of the common tsv parsing code


def db_name_basics(cursor, f_name):
    print("beginning name_basics db update")

    cursor.execute("""CREATE TABLE "name_basics" (
    nconst text primary key,
    primaryName text,
    birthYear int,
    deathYear int,
    primaryProfession text,
    knownForTitles text
    );""")

    cursor.execute("""COPY name_basics FROM '%s'""" %
                   (os.path.join(os.getcwd(), f_name)))
    conn.commit()

    print("completed name_basics db update")


# first do:
# create_db aulty
try:
    connect_str = "dbname='aulty' user='aulty'"
    # use our connection values to establish a connection
    conn = psycopg2.connect(connect_str)
    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()
    # create a new table with a single column called "name"

    if args.drop_tables:
        print("dropping existing database tables")
        prefix = "DROP TABLE "
        suffix = ";"
        # TODO: stop duplicating these strings
        cmds = [prefix + "name_basics" + suffix, prefix +
                "title_basics" + suffix, prefix + "title_principals" + suffix]
        for cmd in cmds:
            cursor.execute(cmd)

    test_str = ""
    if args.test_data:
        test_str = ".test"

    db_title_principals(cursor, "title.principals" + test_str + ".tsv")
    db_title_basics(cursor, "title.basics" + test_str + ".tsv")
    db_name_basics(cursor, "name.basics" + test_str + ".tsv")

except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)
    traceback.print_exc()
