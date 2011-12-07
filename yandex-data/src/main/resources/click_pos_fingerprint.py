from avro import schema, datafile, io
import sys
import pdb
from operator import itemgetter
import yandex_utils as yu
from collections import Counter
import time

def process(recs, known_queries):
    count = 0
    start_time = time.time()
    for e in recs:
        count+=1
        if count % 10000 == 0:
            print >>sys.stderr,(time.time()-start_time), "Count", count
        cs = yu.crono_clickstream(e)
        in_query = False
        query_hash = -1
        urls = []
        finger_print = [0] * 10
        label = ''
        for c in cs:
            if c[0] == 'q':
                if in_query:
                    print '\t'.join(str(_) for _ in finger_print) + "\t" + label
                if c[1] in known_queries:
                    query_hash = c[1]
                    urls = c[3]
                    in_query = True
                    continue
                else:
                    in_query = False
                    continue
            if not in_query:
                continue
            # this is a click in a query that is known
            if label:
                label += ';'
            label += yu.get_click_type(known_queries, e['regionId'], c[2])
            for i, url in enumerate(urls):
                if c[2] == url:
                    finger_print[i] = 1
                    break
        if in_query:
            print '\t'.join(str(_) for _ in finger_print) + "\t" + label


def main(args):
    known_queries = yu.read_judged_queries(args[0])
    for f in args[1:]:
        process(yu.avro_reader(f), known_queries)

if __name__ == '__main__':
	main(sys.argv[1:])
