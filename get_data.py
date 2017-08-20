#!/usr/bin/env python3

# script to download and unpack imdb data files

import argparse
import boto3
import gzip
import os
import psycopg2

parser = argparse.ArgumentParser(description='IMDB data updater script')
parser.add_argument('--do-get', dest='do_get', action='store_true')
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

# create tables

# first do:
# create_db aulty
try:
	connect_str = "dbname='aulty' user='aulty'"
	# use our connection values to establish a connection
	conn = psycopg2.connect(connect_str)
	# create a psycopg2 cursor that can execute queries
	cursor = conn.cursor()
	# create a new table with a single column called "name"


	# create tables
	cursor.execute("""CREATE TABLE "title_principals" (
	tconst          text primary key,
	principalcast   text[]
	);""")


	# insert data into tables

	# TODO as a generator?
	data = []
	with open("title.principals.tsv") as f:
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

except Exception as e:
	print("Uh oh, can't connect. Invalid dbname, user or password?")
	print(e)
