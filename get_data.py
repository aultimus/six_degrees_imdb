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
			with open(f_name):
				with gzip.open(f_name, 'rb') as f_archive:
					data = f_archive.read()
					with open(data_file_name, 'wb') as f_data_file:
						f_data_file.write(data)

# do_get == false assumes unarchived data files exist

def replace_null(s):
	return s.replace(r"\N", "0")

def db_title_principals(cursor, f_name):
	cursor.execute("""CREATE TABLE "title_principals" (
	tconst          text primary key,
	principalcast   text[]
	);""")

	with open(f_name) as f:
	# TODO as list comprehension for efficiency?
	# data = [line.rstrip("\n").split("\t") for line in f.readlines()]
		line_no = 0 # skip header
		for line in f.readlines():
			if line_no != 0:
				tconst, nconsts = line.rstrip("\n").split("\t")
				s = """INSERT INTO "title_principals"(tconst, principalcast) VALUES ('%s', '{%s}');""" % (tconst, nconsts)
				cursor.execute(s)
				#print(s)
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
	genres				text[]
	);""")

	with open(f_name) as f:
		line_no = 0 # skip header
		for line in f.readlines():
			if line_no != 0:
				line = replace_null(line)
				others, genres = line.rsplit("\t", 1)
				genres = "{" + genres.rstrip("\n") + "}"
				others = others.split("\t")
				others = str(others).lstrip("[").rstrip("]")
				print(genres)
				print(others)
				
				s = """INSERT INTO "title_basics"(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (%s,'%s');""" % (others, genres)
				#print(s)
				cursor.execute(s)
			if line_no%10000==0:
				print(line_no)
			line_no+=1
	conn.commit()

	# as a test
	cursor.execute("""SELECT * FROM title_basics WHERE tconst = 'tt0000009';""")
	rows = cursor.fetchall()
	print(rows)

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

	#db_title_principals(cursor, "title.principals" + test_str + ".tsv")
	db_title_basics(cursor, "title.basics" + test_str + ".tsv")


except Exception as e:
	print("Uh oh, can't connect. Invalid dbname, user or password?")
	print(e)
	traceback.print_exc()
