#!/usr/bin/python

from avro import schema, datafile, io
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


class MyWriter:
	def __init__(self, output_dir, max_in_file):
		self.file_count = 0
		self.rec_count = 0
		self.max_in_file = max_in_file
		self.output_dir = output_dir
		self.writer = None
	def add(self, rec):
		if not rec:
			return
		if not self.writer:
			self.writer = avro_writer("%s/%d.avro" % ( self.output_dir, self.file_count))
		self.writer.append(rec)
		self.rec_count += 1
		if self.rec_count > self.max_in_file:
			print >>sys.stderr,"Flushing out", self.rec_count
			self.writer.close()
			self.writer = None
			self.rec_count = 0
			self.file_count += 1
	def close(self):
		if self.writer:
			print >>sys.stderr,"Flushing out", self.rec_count
			self.writer.close()
			self.rec_count = 0
			self.file_count += 1

		
def read_and_write_data(reader, output_dir, number_per_file):
	session_id = -1
	rec = None
	output_writer = MyWriter(output_dir, number_per_file)
	for e in reader:
		my_session_id = int(e[0])
		if my_session_id != session_id:
			output_writer.add(rec)
			session_id = my_session_id
			rec = {}
			rec['sessionId'] = session_id
			rec['regionId'] = int(e[4])
			rec['queries'] = []
			rec['clicks'] = []
		if e[2] == 'Q':
			add_query(rec, e)
		else:
			add_click(rec, e)	
	output_writer.add(rec)
	output_writer.close()


def add_click(row, e):
	entry = {} 	
	entry['time']      = int(e[1])
	entry['url']      = int(e[3])
	row['clicks'].append(entry)


def add_query(row, e):
	entry = {} 	
	entry['time']      = int(e[1])
	entry['queryHash'] = int(e[3])
	entry['urls'] = [ int(_) for _ in e[5:] ]
	row['queries'].append(entry)
		

def main(args):
	reader = ( _.strip().split() for _ in open(args[0]))
	output_dir = args[1]
	number_per_file = int(args[2])
	read_and_write_data(reader, output_dir, number_per_file)


if __name__ == '__main__':
	main(sys.argv[1:])
