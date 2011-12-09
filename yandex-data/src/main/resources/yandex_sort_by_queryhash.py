#!/usr/bin/env python

import sys


def dump_info(info, out):
    if not out:
        return
    for x,y in sorted(info.iteritems()):
        out.write("%d\t%s\n" % (x, '\t'.join(y)))
    out.close()
    info.clear()


def process_file(reader, max):
    in_parts = ( _.strip().split() for _ in sys.stdin )
    # session_id as a string as don't need to sort
    info = {}
    i = 0
    for session_id, query_hash in (( _[0], int(_[1])) for _ in in_parts):
        i+=1
        if i % max == 0:
            print >>sys.stderr, "Lines", i
            yield info
        if query_hash not in info:
            info[query_hash] = []
        info[query_hash].append(session_id)
    print >>sys.stderr, "Lines", i
    yield info


def main(args):
    out_dir = args[0]
    file_number = 0
    out = None
    max = 30000000
    for info in process_file(sys.stdin, max):
        if out:
            dump_info(info, out)
            info.clear()
            out = None
        if not out:
            out = open("%s/byid-%d.txt" % (out_dir, file_number), 'w')
            file_number += 1
    if out:
        dump_info(info, out)
        out = None

        
if __name__ == '__main__':
    main(sys.argv[1:])
