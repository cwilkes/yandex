#!/usr/bin/env python

import pdb
from generate_avro_bunches import process
import sys

SCHEMA_STR = """{
    "type": "record", 
    "name": "QueryGrade",
    "namespace": "com.ladro.yandex.avro",
    "fields": [
        {"name": "queryHash", "type": "long"}, 
        {"name": "regionId", "type": "int" },
        {"name": "good", "type": {"type": "array", "items": "long" }},
        {"name": "bad", "type": {"type": "array", "items": "long" }}
    ]
}"""


def create_record(e):
    rec = {}
    rec['queryHash'] = int(e[0])
    rec['regionId']  = int(e[1])
    rec['good']   = []
    rec['bad']    = []
    additional_row(rec, e)
    #pdb.set_trace()
    return rec


def additional_row(rec, e):
    url_type = 'good' if e[3] == '1' else 'bad'
    rec[url_type].append(int(e[2]))


def main(args):
    if not args:
        sys.exit("Usage: python ./generate_avro_query_grade.py  /tmp/release/Trainq.txt /tmp/e 2000")
    process(args[0], args[1], int(args[2]), SCHEMA_STR, create_record, additional_row)


if __name__ == '__main__':
    main(sys.argv[1:])
