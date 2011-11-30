#!/usr/bin/python

from avro import schema, datafile, io
import itertools
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


SCHEMA = schema.parse(SCHEMA_STR)


def avro_writer(file_name):
    rec_writer = io.DatumWriter(SCHEMA)
    df_writer = datafile.DataFileWriter(
        open(file_name, 'wb'),
        rec_writer,
        writers_schema = SCHEMA,
        codec = 'deflate'
    )
    return df_writer


def emit_writers(output_dir):
    file_number = 0
    while True:
        yield avro_writer("%s/%d.avro" % ( output_dir, file_number))
        file_number+=1


def emit_record(reader):
    session_id = -1
    rec = None
    for e in reader:
        my_session_id = int(e[0])
        if my_session_id != session_id:
            if rec:
                yield rec
            session_id = my_session_id
            rec = {}
            rec['sessionId'] = session_id
            rec['regionId']  = int(e[4])
            rec['queries']   = []
            rec['clicks']    = []
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
    if rec:
        yield rec


def emit_record_bunch(recgen, number_per_file):
    while recgen:
        yield itertools.islice(recgen, number_per_file)


def emit_records_and_writer(reader, output_dir, number_per_file):
    inputs  = emit_record_bunch( emit_record( reader ), number_per_file )
    writers = emit_writers(output_dir)
    return itertools.izip(inputs, writers)


def process(input_file, output_dir, number_per_file):
    reader = ( _.strip().split() for _ in open(input_file))
    for recs, writer in emit_records_and_writer(reader, output_dir, number_per_file):
        for rec in recs:
            writer.append(rec)
        writer.close()


def main(args):
    process(args[0], args[1], int(args[2]))


if __name__ == '__main__':
    main(sys.argv[1:])
