#!/usr/bin/env python3

import argparse
import codecs
import re
import sys
from collections import namedtuple

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

#Actor = namedtuple("Actor", ["films"])
# Actors = [] # list of namedtuples
Role = namedtuple("Role", ["film_name", "year", "char_name", "bill_pos"])

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
            if not line.startswith("\t"):
                # new actor
                actor = line[:line.find("\t")]
                # TODO: match first role of actor with regex
                roles = []
            else:
                # or use named optional capture groups?
                # \t+([\w \"]+)\ \(([0-9]+)\)[ ]+{[\w#\(.0-9\)]+}[ ]+\[([\w0-9 ]+)\]
                m = re.search(
                    "\t*(?P<name>[\w \"\'?!.\(\)*$\[\]#]+)\ \((?P<year>[0-9?IVX/]+)\)[ ]*(?:{(?P<ep>[\w#\(.0-9\)]+)?})?[ ]*(?:\[(?P<char>[\w0-9 ]+)?\])?[ ]*(:?\<(?P<bill>[0-9]+)\>)?", line)
                if m:
                    role = Role(film_name=m.group(0), year=m.group(1),
                                char_name=m.group(2), bill_pos=m.group(3))
                    num_matched += 1
                    # print(role)
                    #films = films.append(role)
                else:
                    num_unmatched += 1
                    #print("did not match:")
                    # print(line)

print("matched", num_matched)
print("failed to match", num_unmatched)
