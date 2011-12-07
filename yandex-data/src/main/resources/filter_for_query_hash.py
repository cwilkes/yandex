import sys
import pdb
from operator import itemgetter
import yandex_utils as yu

def process_file(known_queries, file):
    count = 0
    out_count = 1
    out = "%d\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d"
    for e in yu.avro_reader(file):
        count+=1
        if count == 100001:
            break
        if count % 10000 == 0:
            print >>sys.stderr,"Count", count,"out", out_count
            sys.stderr.flush()
        for fp in yu.get_clickstream_fingerprint(known_queries, e):
            print out % (fp[0][0], fp[1][0], fp[2][0], fp[3][0], fp[1][1], fp[2][1], fp[3][1], fp[1][2], fp[2][2], fp[3][2])
            out_count+=1
    return count


def main(args):
    grades = yu.read_judged_queries(args[0])
    count = 0
    for f in args[1:]:
        count += process_file(grades, f)
    print >>sys.stderr,"Done", count


if __name__ == '__main__':
	main(sys.argv[1:])
