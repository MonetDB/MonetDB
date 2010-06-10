#!/usr/bin/env python

# Tool to compare the results of two different Mtest runs.
#
# This tool compares the "times.lst" files in the two given
# directories and reports all tests whose results differ.  If the
# standard output differs, differences in the error output are not
# reported.  The tool is meant to find tests whose results differ, not
# to list all differences.
#
# Optionally, significant (more than 10%) slow down of succeeding
# tests can also be reported.  (If tests fail, the speed is useless
# and thus not reported.)  Note that comparing times is only useful if
# the tests were run under comparable circumstances (optimization,
# machine load, etc).

import os

def cmptests(dir1, dir2, timing = True):
    lst1 = os.path.join(dir1, 'times.lst')
    lst2 = os.path.join(dir2, 'times.lst')
    res1 = {}
    for line in open(lst1):
        line = line.strip().split('\t')
        if res1.has_key(line[0]):
            print >> sys.stderr, '%s: duplicate key %s' % (lst1, line[0])
            sys.exit(1)
        if len(line) != 4:
            continue
        if line[0][-2:] == '/:':
            continue
        res1[line[0]] = tuple(line[1:])
    slowdown = []
    for line in open(lst2):
        line = line.strip().split('\t')
        if len(line) != 4:
            continue
        if line[0][-2:] == '/:':
            continue
        if not res1.has_key(line[0]):
            print 'New test in %s: %s' % (lst2, line[0])
            continue
        tm1, out1, err1 = res1[line[0]]
        tm2, out2, err2 = tuple(line[1:])
        if out1 != out2:
            print '%s output differs: %s %s' % (line[0], out1, out2)
        elif err1 != err2:
            print '%s errout differs: %s %s' % (line[0], err1, err2)
        if timing and out1 == 'F_OK' and out2 == 'F_OK' and err1 == 'F_OK' and err2 == 'F_OK':
            ftm1 = float(tm1)
            ftm2 = float(tm2)
            if ftm1 < ftm2:
                if ftm2 - ftm1 > 0.1 * ftm1 or ftm2 - ftm1 > 0.1 * ftm2:
                    slowdown.append((line[0], tm1, tm2))
        del res1[line[0]]
    if res1:
        print '\nRemoved tests in %s:' % lst1
        for tst in res1:
            print tst
    if slowdown:
        print '\nSignificant slowdown in tests:'
        for tst, tm1, tm2 in slowdown:
            print '%s %s %s' % (tst, tm1, tm2)

if __name__ == '__main__':
    import getopt, sys

    timing = False

    def usage(ext):
        print >> sys.stderr, 'Usage: %s [-t] dir1 dir2' % sys.argv[0]
        print >> sys.stderr, 'Compare test outputs in dir1 and dir2.'
        print >> sys.stderr, 'If -t option given, report significant slow down.'
        sys.exit(ext)

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'ht')
    except getopt.GetoptError:
        usage(1)

    for o, a in opts:
        if o == '-h':
            usage(0)
        elif o == '-t':
            timing = True

    if len(args) != 2:
        usage(1)

    cmptests(args[0], args[1], timing)
