#!/usr/bin/env python3

import argparse
import codecs
import psycopg2
import re
import sys
from collections import namedtuple

def re_match(s):
    role_re = r"\t*(?P<name>[\w \"\'?!.\(\)*$\[\]#°+-:.,@®\%«»<>~ôûî=¢\$\£;\{\}\&]+)\ \((?P<year>[0-9?IVX\/]+)\)[ ]*(?:{(?P<ep>[\w#\(.0-9\)\-\ \']+)?})?[ ]*(?:\([\w ]+\))?[ ]*(?:\[(?P<char>[\w0-9 ]+)?\])?[ ]*(:?\<(?P<bill>[0-9]+)\>)?"
    #print(role_re)
    m = re.search(role_re, s)
    name, year, ep, char, bill = m.group("name"), m.group("year"), m.group("ep"), m.group("char"), m.group("bill")
    return Role(film_name=name, year=year, episode=ep, char_name=char, bill_pos=bill)

def escape_quote(s):
    return s.replace("'", "''")

def db_write(actor, role, cursor):
    s = "INSERT INTO roles(actor, film, year, episode, char_name, bill_pos) VALUES('%s', '%s', '%s', '%s', '%s', '%s')" % (escape_quote(actor), escape_quote(role.film_name), escape_quote(role.year or ""), escape_quote(role.episode or ""), escape_quote(role.char_name or ""), escape_quote(role.bill_pos or ""))
    print(s)
    # TODO: role may have NONE fields rather than empty string
    cursor.execute(s)

parser = argparse.ArgumentParser(
    description='Convert imdb list text file to DB')
parser.add_argument('--input-fname', type=str,
                    help='path to imdb list input file')
parser.add_argument('--output-fname', type=str,
                    help='path to db to output')
args = parser.parse_args()

if args.input_fname == None or args.output_fname == None:
    parser.print_help()
    sys.exit(1)

# wrt database we want to store roles (film, actor, year, episode, char_name, bill_pos)
# each line is a role
# we want to index by film, actor
# primary key is composite {film, actor}

# use case is:
# 1. search for an actor
# 2. retrieve all films by that actor
# 3. search for all actors with that film

#db_params = {}
#with db.connect(**server_params) as conn:
#    with conn.

# first do:
# create_db roles 
try:
    # todo use password and pass in via cmd line arg
    connect_str = "dbname='roles' user='aulty'"
    # use our connection values to establish a connection
    conn = psycopg2.connect(connect_str)
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)
    sys.exit(1)


# create a psycopg2 cursor that can execute queries
cursor = conn.cursor()
# create a new table with a single column called "name"
cursor.execute("CREATE TABLE roles(actor text NOT NULL CHECK (actor <> ''), film text NOT NULL CHECK (film <> ''), year text, episode text, char_name text, bill_pos text, PRIMARY KEY(film, actor, episode));")
cursor.execute("CREATE INDEX idx_actor ON roles(actor, film);")

Role = namedtuple("Role", ["film_name", "year", "episode", "char_name", "bill_pos"])
num_matched = 0
num_unmatched = 0

with codecs.open(args.input_fname, "r", "iso-8859-1") as f:
    # only read a line into memory at a time
    start_marker = False
    started = False
    for line in f:
        if not started:
            if line.startswith("Name") and line.find("Titles") != -1 and not start_marker:
                start_marker = True
            elif start_marker and line.startswith("----") and line.find("------") != -1:
                started = True
            elif start_marker:
                print("error finding start of actors")
                sys.exit(1)
        else:
            # print(line)
            if line == "\n":
                continue
            if not line.startswith("\t"):
                # new actor
                actor = line[:line.find("\t")]
                print(actor)
                role = re_match(line[line.rfind("\t"):])
                db_write(actor, role, cursor)
            else:
                # or use named optional capture groups?
                # \t+([\w \"]+)\ \(([0-9]+)\)[ ]+{[\w#\(.0-9\)]+}[ ]+\[([\w0-9 ]+)\]
                role = re_match(line)
                db_write(actor, role, cursor)

print("matched", num_matched)
print("failed to match", num_unmatched)

conn.close()