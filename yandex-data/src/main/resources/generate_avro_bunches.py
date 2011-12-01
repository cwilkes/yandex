#!/usr/bin/env python

import os
import pdb
from avro import schema, datafile, io
import itertools
import sys

"""Just call the process method"""

def avro_writer(schema, file_name):
    """Creates an avro writer with the given schema and writes to the file file_name"""
    rec_writer = io.DatumWriter(schema)
    df_writer = datafile.DataFileWriter(
        open(file_name, 'wb'),
        rec_writer,
        writers_schema = schema,
        codec = 'deflate'
    )
    return df_writer


def emit_writers(schema, output_dir):
    """Return a generator of avro writers, when asked for next() adds one to the previous file number.  ie 0.avro then 1.avro"""
    file_number = 0
    while True:
        yield avro_writer(schema, "%s/%d.avro" % ( output_dir, file_number))
        file_number+=1


def emit_record(reader, create_record, additional_row):
    """Reads individual lines from the reader and returns a completed record"""
    group_id = None
    rec = None
    for e in reader:
        if e[0] == group_id:
            additional_row(rec, e)
            continue
        # we're on to a new record, or possibly the first one
        if rec:
            yield rec
        group_id = e[0]
        rec = create_record(e)
    if rec:
        yield rec


def chunker(iterable1, size1, iterable2):
    in1 = iter(iterable1)
    in2 = iter(iterable2)
    for temp1 in in1:
        print "t1",temp1
        yield (itertools.islice(itertools.chain(temp1, in1), size1), in2.next())


def process(input_file, output_dir, number_per_file, schema_str, create_record, additional_row):
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    reader = ( _.strip().split() for _ in open(input_file))
    record_emitter = emit_record( reader, create_record, additional_row )
    writers = emit_writers(schema.parse(schema_str), output_dir)
    #pdb.set_trace()
    for recs, writer in chunker(record_emitter, number_per_file, writers):
        for rec in recs:
            writer.append(rec)
        writer.close()
