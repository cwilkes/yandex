import os
import pdb
from avro import schema, datafile, io
import itertools_utils as ut
import itertools
import sys

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


def _emit_writers(schema, output_dir):
    """Return a generator of avro writers, when asked for next() adds one to the previous file number.  ie 0.avro then 1.avro"""
    file_number = 0
    while True:
        yield avro_writer(schema, "%s/%d.avro" % ( output_dir, file_number))
        file_number+=1


def _emit_record(reader, create_record, additional_row):
    """Reads individual lines from the reader and returns a completed record"""
    group_id = None
    rec      = None
    for e in reader:
        if e[0] == group_id:
            # in the same record
            additional_row(rec, e)
            continue
        # we're on to a new record
        # first time through rec is None, don't emit that
        if rec:
            yield rec
        group_id = e[0]
        rec      = create_record(e)
    if rec:
        yield rec


def split_input(input_file, output_dir, number_per_file, schema_str, create_record, additional_row):
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    reader         = ( _.strip().split() for _ in open(input_file))
    writers        = _emit_writers(schema.parse(schema_str), output_dir)
    record_emitter = _emit_record(reader, create_record, additional_row)
    #pdb.set_trace()
    for writer,recs in ut.chunker(writers, record_emitter, number_per_file):
        has_record = False
        for rec in recs:
            has_record = True
            writer.append(rec)
        writer.close()
        if not has_record:
            # this is the problem, continiously loops with 0 entries in rec
            print >>sys.stderr,"Chunker should not have looped here.  Breaking"
            break


if __name__ == '__main__':
    """makes an input file for the query_grade"""
    out = open(sys.argv[1], 'w')
    out.write('%d\t%d\t%d\t%d' % (477974, 1, 731612,   1))
    out.write('%d\t%d\t%d\t%d' % (477974, 1, 38256263, 0))
    out.write('%d\t%d\t%d\t%d' % (145204, 0, 1093524,  0))
    out.close()
