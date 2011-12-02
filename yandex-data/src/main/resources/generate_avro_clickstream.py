#!/usr/bin/python

from generate_avro_bunches import split_input
import sys

SCHEMA_STR = """{
  "type": "record", 
  "name": "Clickstream",
  "namespace": "com.ladro.yandex.avro",
  "fields": [
  		{"name": "sessionId", "type": "long"}, 
		{"name": "regionId", "type": "int" },
	    {"name": "queries", "type": {
			"type": "array",
			"items": {
				"type": "record",
				"name": "Query",
				"fields": [
					{"name": "queryHash", "type": "long"},
					{"name": "time", "type": "int"},
					{"name": "urls", "type": {"type": "array", "items": "long" }}
					]
				}
			}
		}, 
	    {"name": "clicks", "type": {
			"type": "array",
			"items": {
				"type": "record",
				"name": "Click",
				"fields": [
					{"name": "time", "type": "int"},
					{"name": "url", "type": "long" }
					]
				}
			}
		}
	]
}"""


def create_record(e):
    rec = {}
    rec['sessionId'] = int(e[0])
    rec['regionId']  = int(e[4])
    rec['queries']   = []
    rec['clicks']    = []
    return rec


def additional_row(rec, e):
    if e[2] == 'Q':
        entry = {} 	
        entry['time']      = int(e[1])
        entry['queryHash'] = int(e[3])
        entry['urls']      = [ int(_) for _ in e[5:] ]
        rec['queries'].append(entry)
    else:
        entry = {} 	
        entry['time'] = int(e[1])
        entry['url']  = int(e[3])
        rec['clicks'].append(entry)

def main(args):
    if not args:
        sys.exit("Usage: python ./generate_avro_clickstream.py  /tmp/release/Clicklog.txt /tmp/d 1000")
    split_input(args[0], args[1], int(args[2]), SCHEMA_STR, create_record, additional_row)


if __name__ == '__main__':
    main(sys.argv[1:])
