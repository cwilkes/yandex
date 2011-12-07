from avro import schema, datafile, io
import sys
import pdb
from operator import itemgetter
import yandex_utils as yu
from collections import Counter


def process_file(query_hashes, file):
    counter = Counter()
    c = 0
    s = 0
    o = 0
    for e in yu.avro_reader(file):
        s+=1
        if s == 100000:
            break
        for q in e['queries']:
            c+=1
            if c % 10000 == 0:
                print >>sys.stderr,"Queries:", c, "Sessions:", s, "Out", o
            if q['queryHash'] not in query_hashes:
                continue
            o+=1
            for pos, url in enumerate(q['urls']):
                pos_marker = 0 if pos < 5 else 1
                key = "%d\t%d\t%d\t%d" % (q['queryHash'], e['regionId'], url, pos_marker)
                counter[key]+=1
    print >>sys.stderr,"Queries:", c, "Sessions:", s, "Out", o
    dump_cache(counter)


def dump_cache(c):
    for key,count in c.most_common():
        print "%s\t%d" % (key, count)
    c.clear()


def main(args):
    query_hashes = yu.read_judged_queries(args[0]).keys()
    for f in args[1:]:
        process_file(query_hashes, f)


if __name__ == '__main__':
	main(sys.argv[1:])
