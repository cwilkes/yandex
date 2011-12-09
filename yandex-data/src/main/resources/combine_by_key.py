#!/usr/bin/env python

import os
import sys
import re
from operator import itemgetter


def parse_line(line):
    if not line:
        return (None, None)
    parts = line.split('\t', 1)
    return (int(parts[0]), parts[1])


def get_files(dir):
    """files in form /tmp/d/byid-0.txt"""
    regex = re.compile('byid-(\d+).txt')
    ret = []
    for f in os.listdir(dir):
        m = regex.search(f)
        if m:
            ret.append((int(m.group(1)), f))
    ret2 = []
    for pos, file_name in sorted(ret, key=itemgetter(0)):
        ret2.append(dir + '/' + file_name)
    return ret2


class MyReader:
    def __init__(self, reader):
        self._reader = reader
    def __iter__(self):
        return self
    def next(self):
        while True:
            line = self._reader.readline()
            if not line:
                # return none as the consumer can handle
                return None
            query_hash, times = parse_line(line.strip())
            if query_hash != None:
                return (query_hash, times)


class Readers:
    def __init__(self, readers):
        self._readers = readers
        self._vals = []
        for r in readers:
            self._vals.append(r.next())
    def __iter__(self):
        return self
    def next(self):
        ret = (sys.maxint, None)
        min_pos = -1
        for pos, e in enumerate(self._vals):
            if e != None and e[0] < ret[0]:
                ret = e
                min_pos = pos
        if ret[1] != None:
            self._vals[min_pos] = self._readers[min_pos].next()
            return ret
        else:
            raise StopIteration


def main(args):
    readers = Readers( [ MyReader(open(_)) for _ in get_files(args[0]) ] )
    writer = open(args[1], 'w')
    process(readers, writer)


def process(readers, writer):
    on_deck_key = -1
    on_deck_val = ''
    i = 0
    for query_hash, sessions in readers:
        i+=1
        if i % 100000 == 0:
            print >>sys.stderr,"Count", i
        if query_hash != on_deck_key:
            if on_deck_val:
                writer.write("%d\t%s\n" % (on_deck_key, on_deck_val))
            on_deck_key = query_hash
            on_deck_val = sessions
        else:
            on_deck_val += '\t' + sessions
    print >>sys.stderr,"Count", i
    if on_deck_val:
        writer.write("%d\t%s\n" % (on_deck_key, on_deck_val))
    writer.close()

        
if __name__ == '__main__':
    main(sys.argv[1:])
