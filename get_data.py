#!/usr/bin/env python3

# script to download and unpack imdb data files

import argparse
import boto3
import gzip
import os
import psycopg2
import traceback

parser = argparse.ArgumentParser(description='IMDB data updater script')
parser.add_argument('--do-get', dest='do_get', action='store_true')
parser.add_argument('--test-data', dest='test_data', action='store_true')
args = parser.parse_args()

if args.do_get:
	print("checking if we need to fetch or unpack data")
	# get data
	file_names = ["title.principals.tsv.gz", "title.basics.tsv.gz", "name.basics.tsv.gz"]
	s3 = boto3.resource('s3')
	for f_name in file_names:
		data_file_name = f_name.rstrip(".gz")

		# download data archive if it does not exist
		if not os.path.isfile(f_name):
			print("downloading " + f_name)
			s3.Bucket("imdb-datasets").download_file("documents/v1/current/" + f_name, f_name, ExtraArgs={"RequestPayer": "requester"})

		# extract archive if uncompressed data file does not exist
		if not os.path.isfile(data_file_name):
			with open(f_name), gzip.open(f_name, 'rb') as f_archive:
				data = f_archive.read()
				with open(data_file_name, 'wb') as f_data_file:
					f_data_file.write(data)

# do_get == false assumes unarchived data files exist

def replace_null(s):
	return s.replace(r"\N", "0")

def gen_insert_string(table_name, col_names, data_str):
	d = replace_null(data_str)
	d = d.replace("'", "''")
	d = d.replace('"', '""')
	l = d.rstrip("\n").split("\t")
	num_data = len(l)
	d = str(l).lstrip("[").rstrip("]").replace('"', "'")
	d = d.replace("\\'", "")

	num_cols = len(col_names.split(","))
	if num_cols != num_data:
		raise ValueError("bad data does not have correct number of cols. Wants %d, has %d, %s" % (num_cols, num_data, data_str))
	s = """INSERT INTO "%s"(%s) VALUES (%s)""" % (table_name, col_names, d)
	#print(s)
	return s

def db_title_principals(cursor, f_name):
	cursor.execute("""CREATE TABLE "title_principals" (
	tconst text,
	nconst text,
	PRIMARY KEY(tconst, nconst)
	);""")

	with open(f_name) as f:
	# TODO as list comprehension for efficiency?
	# data = [line.rstrip("\n").split("\t") for line in f.readlines()]
		line_no = 0 # skip header
		for line in f:
			if line_no != 0:
					try:
						s = gen_insert_string("title_principals", "tconst, nconst", line)
						cursor.execute(s)
						#print(s)
					except ValueError as e:
						print(e)
						traceback.print_exc()
						continue
			if line_no%10000==0:
				print(line_no)
			line_no+=1
		conn.commit()

	# as a test
	cursor.execute("""SELECT * FROM title_principals WHERE tconst = 'tt0000001';""")
	rows = cursor.fetchall()
	print(rows)

def db_title_basics(cursor, f_name):
	cursor.execute("""CREATE TABLE "title_basics" (
	tconst				text primary key,
	titleType			text,
	primaryTitle		text,
	originalTitle		text,
	isAdult				int,
	startYear			int,
	endYear				int,
	runtimeMinutes		int,
	genres				text
	);""")

	with open(f_name) as f:
		line_no = 0 # skip header
		for line in f:
			if line_no != 0:
				try:
					s = gen_insert_string("title_basics", "tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres", line)
					#print(s)
					cursor.execute(s)
				except ValueError as e:
					print(e)
					traceback.print_exc()
					continue
			if line_no%10000==0:
				print(line_no)
			line_no+=1
	conn.commit()

	# as a test
	cursor.execute("""SELECT * FROM title_basics WHERE tconst = 'tt0000009';""")
	rows = cursor.fetchall()
	print(rows)

# parsing data into arrays is probably uneccessary
# TODO: refactor out some of the common tsv parsing code
def db_name_basics(cursor, f_name):
	cursor.execute("""CREATE TABLE "name_basics" (
	nconst text primary key,
	primaryName text,
	birthYear int,
	deathYear int,
	primaryProfession text,
	knownForTitles text
	);""")

	with open(f_name) as f:
		line_no = 0 # skip header
		for line in f:
			if line_no != 0:
				try:
					s = gen_insert_string("name_basics", "nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles", line)
					#print(s)
					cursor.execute(s)
				except ValueError as e:
					print(e)
					traceback.print_exc()
					continue
			if line_no%10000==0:
				print(line_no)
			line_no+=1
	conn.commit()


# first do:
# create_db aulty
try:
	connect_str = "dbname='aulty' user='aulty'"
	# use our connection values to establish a connection
	conn = psycopg2.connect(connect_str)
	# create a psycopg2 cursor that can execute queries
	cursor = conn.cursor()
	# create a new table with a single column called "name"

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
