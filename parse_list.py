#!/usr/bin/env python3

import argparse
import codecs
import re
import sys
from collections import namedtuple

def re_match(s):
    name_re = r"(?P<name>[\w \"\'?!.\(\)*$\[\]#°+-:.,@®\%«»<>~ôûî=¢\$\£;\{\}]+)"
    role_re = r"\t*" + name_re + \
    r"\ \((?P<year>[0-9?IVX\/]+)\)[ ]*(?:{(?P<ep>[\w#\(.0-9\)-]+)?})?[ ]*(?:\([\w ]+\))?[ ]*(?:\[(?P<char>[\w0-9 ]+)?\])?[ ]*(:?\<(?P<bill>[0-9]+)\>)?"
    #print(role_re)
    m = re.search(role_re, s)
    name, year, ep, char, bill = m.group("name"), m.group("year"), m.group("ep"), m.group("char"), m.group("bill")
    return Role(film_name=name, year=year, episode=ep, char_name=char, bill_pos=bill)

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

Role = namedtuple("Role", ["film_name", "year", "episode", "char_name", "bill_pos"])
roles = []
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
                # finished actor
                print("actor:", actor)
                for role in roles:
                    print(role)
                print("")
                roles = []
                continue
            if not line.startswith("\t"):
                # new actor
                actor = line[:line.find("\t")]
                role = re_match(line[line.rfind("\t"):])
                roles.append(role)
                # actor = line[:line.find("\t")]
                # TODO: match first role of actor with regex
            else:
                # or use named optional capture groups?
                # \t+([\w \"]+)\ \(([0-9]+)\)[ ]+{[\w#\(.0-9\)]+}[ ]+\[([\w0-9 ]+)\]
                role = re_match(line)
                roles.append(role)

print("matched", num_matched)
print("failed to match", num_unmatched)
