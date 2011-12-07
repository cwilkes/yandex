from avro import schema, datafile, io
import sys
import pdb
from operator import itemgetter

def avro_reader(file_name):
    rec_reader = io.DatumReader()
    return datafile.DataFileReader(
        open(file_name),
        rec_reader
    )


def crono_clickstream(rec):
    ret = []
    for q in rec['queries']:
        ret.append(('q', q['queryHash'], q['time'], q['urls']))
    for c in rec['clicks']:
        ret.append(('c', c['time'], c['url']))
    return sorted(ret, key=itemgetter(2))


def read_judged_queries(file):
    #477974  1       731612  1
    #477974  1       38256263        0
    grades = {}
    reader = ( _.strip().split() for _ in open(file))
    # improve
    for qh, region, url, status in ((int(_[0]), int(_[1]), int(_[2]), int(_[3])) for _ in reader):
        if qh not in grades:
            grades[qh] = {}
        if url not in grades[qh]:
            grades[qh][url] = {}
        grades[qh][url][region] = status
    return grades


def get_click_type(grades_for_query, region, url):
    """U=unknown, B=bad url, G=good url, RG=region good, RB=region bad, RU=region undecided"""
    if url not in grades_for_query:
        return 'U'
    if region not in grades_for_query[url]:
        good = False
        bad  = False
        for r in grades_for_query[url]:
            if grades_for_query[url][r] == 0:
                bad = True
            else:
                good = True
        if good and not bad:
            return 'RG'
        if not good and bad:
            return 'RB'
        return 'RU'
    if grades_for_query[url][region] == 0:
        return 'B'
    return 'G'


def get_recognized_queries(known_queries, cs):
    for pos, e in enumerate(cs):
        if e[0] == 'q' and e[1] in known_queries:
            yield (pos, e[1], e[2], e[3])


def get_clickstream_fingerprint(known_queries, rec):
    cs = crono_clickstream(rec)
    for pos, query_hash, start_time, urls  in get_recognized_queries(known_queries, cs):
        ret=[]
        ret.append((query_hash, ))
        in_diff_query = False
        pos+=1
        end = pos+3
        for r in cs[pos:end]:
            if r[0] == 'q':
                ret.append(('Q', r[2] - start_time, -1))
                in_diff_query = True
                continue
            if in_diff_query:
                ret.append(('C', r[1] - start_time, -1))
                continue
            # in same query and a click
            click_pos_in_results = get_click_pos(urls, r[2])
            click_type = get_click_type(known_queries, rec['regionId'], query_hash)
            ret.append((click_type, r[1] - start_time, click_pos_in_results))
        while len(ret) < 4:
            ret.append(('E', 0, -1))
        yield ret


def get_click_pos(urls, url):
    for pos, u in enumerate(urls):
        if u == url:
            return pos
    return -1
