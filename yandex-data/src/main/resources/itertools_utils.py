import pdb
import itertools
import sys

def chunker(outer_iterable, inner_iterable, size):
    in1 = iter(outer_iterable)
    in2 = iter(inner_iterable)
    while in2:
        yield (in1.next(), itertools.islice(in2, size))


def _test_iter(c):
    while c > 0:
        yield c
        c-=1


if __name__ == '__main__':
    q=chunker('abcdefg', _test_iter(10), 3)
    prefix = ''
    for a,b in q:
        for c in b:
            print "%s%s\t%d" % (prefix, a, c)
        prefix += ' '
