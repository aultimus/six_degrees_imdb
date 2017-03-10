#!/bin/bash
set -e

wget ftp://ftp.fu-berlin.de/pub/misc/movies/database/actors.list.gz
wget ftp://ftp.fu-berlin.de/pub/misc/movies/database/actresses.list.gz

dtrx actors.list.gz
dtrx actresses.list.gz

./parse_list.py --input-fname actors.list.gz --output-fname actors.db
./parse_list.py --input-fname actresses.list.gz --output-fname actresses.db