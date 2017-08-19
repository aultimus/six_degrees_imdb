#!/usr/bin/env python3

# script to download and unpack imdb data files

import boto3
import gzip
import os


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
