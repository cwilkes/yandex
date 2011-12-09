#!/usr/bin/env python

import sys

def main(args):
    """looking for these lines: 0   0   Q   8   0   7   103 51  92  43  12  73  69  27  105
     ['0', '0', 'Q', '8', '0', '7', '103', '51', '92', '43', '12', '73', '69', '27', '105']"""
    out = open(args[0], 'w')
    i = 0
    o = 0
    for e in (_.strip().split() for _ in sys.stdin):
        i+=1
        if i % 100000 == 0:
            print >>sys.stderr, "Lines", i, "Out", o
            sys.stderr.flush()
            out.flush()
        if e[1] == '0' and e[2] == 'Q' and e[4] == '0':
            o += 1
            out.write(e[0] + '\t' + e[3] + '\n')
    out.close()
    print >>sys.stderr, "Lines", i, "Out", o
        
if __name__ == '__main__':
    main(sys.argv[1:])
