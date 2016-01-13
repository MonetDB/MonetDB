#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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

def cmptests(dir1, dir2, timing = True, regressions = False):
    lst1 = os.path.join(dir1, 'times.lst')
    lst2 = os.path.join(dir2, 'times.lst')
    res1 = {}
    new2 = []
    for line in open(lst1):
        line = line.strip().split('\t')
        if line[0] in res1:
            sys.stderr.write('%s: duplicate key %s\n' % (lst1, line[0]))
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
        if line[0] not in res1:
            new2.append(line[0])
            continue
        tm1, out1, err1 = res1[line[0]]
        tm2, out2, err2 = tuple(line[1:])
        if (out1 != out2 and out2 != 'F_OK') or \
               (err1 != err2 and err2 != 'F_OK') or \
               not regressions:
            if out1 != out2:
                sys.stdout.write('%s output differs: %s %s\n' % (line[0], out1, out2))
            elif err1 != err2:
                sys.stdout.write('%s errout differs: %s %s\n' % (line[0], err1, err2))
        if timing and out1 == 'F_OK' and out2 == 'F_OK' and err1 == 'F_OK' and err2 == 'F_OK':
            ftm1 = float(tm1)
            ftm2 = float(tm2)
            if ftm1 < ftm2:
                if ftm2 - ftm1 > 0.1 * ftm1 or ftm2 - ftm1 > 0.1 * ftm2:
                    slowdown.append((line[0], tm1, tm2))
        del res1[line[0]]
    if res1:
        sys.stdout.write('\nRemoved tests in %s:\n' % lst1)
        for tst in sorted(res1.keys()):
            sys.stdout.write(tst.rstrip(':') + '\n')
    if new2:
        sys.stdout.write('\nNew tests in %s:\n' % lst2)
        for tst in sorted(new2):
            sys.stdout.write(tst.rstrip(':') + '\n')
    if slowdown:
        sys.stdout.write('\nSignificant slowdown in tests:\n')
        for tst, tm1, tm2 in sorted(slowdown):
            sys.stdout.write('%s %s %s\n' % (tst, tm1, tm2))

if __name__ == '__main__':
    import getopt, sys

    timing = False
    regressions = False

    def usage(ext):
        sys.stderr.write('Usage: %s [-t] [-r] dir1 dir2\n' % sys.argv[0])
        sys.stderr.write('Compare test outputs in dir1 and dir2.\n')
        sys.stderr.write('If -t option given, report significant slow down.\n')
        sys.stderr.write('If -r option given, report regressions only.\n')
        sys.exit(ext)

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'htr')
    except getopt.GetoptError:
        usage(1)

    for o, a in opts:
        if o == '-h':
            usage(0)
        elif o == '-t':
            timing = True
        elif o == '-r':
            regressions = True

    if len(args) != 2:
        usage(1)

    cmptests(args[0], args[1], timing, regressions)
