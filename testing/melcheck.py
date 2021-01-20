# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

import re, sys

try:
    import exportutils
except ImportError:
    from MonetDBtesting import exportutils

# MEL pattern
argreg = r'\s*,\s*(?:bat)?(?:var)?arg(?:any)?\s*\(\s*(?P<argname>"[^"]*")\s*,\s*(?P<argval>\w*)\s*\)'
patreg = r'^\s*(?P<cmdpat>pattern|command)\s*\(\s*"(?P<mod>[^"]*)"\s*,\s*"(?P<fcn>[^"]*)"\s*,\s*(?P<imp>\w+)\s*,[^,]*,\s*"[^\"]*(?:\\.[^\"]*)*"\s*,\s*args\s*\(\s*(?P<retc>\d+)\s*,\s*(?P<argc>\d+)(?P<args>(?:'+argreg+')*)\s*\)\s*\)'

argre = re.compile(argreg)
patre = re.compile(patreg, re.MULTILINE)

fcnreg = r'(?:static\s+)?(?:str|char\s*\*)\s+(?P<name>\w+)\s*\(\s*(?:(?P<pattern>Client\s+\w+\s*,\s*MalBlkPtr\s+\w+\s*,\s*MalStkPtr\s+\w+\s*,\s*InstrPtr\s+\w+)|(?P<command>(?:\w+\s*\*+|ptr\s)\s*\w+(?:\s*,\s*(?:const\s+)?(?:\w+\s*\*(?:\s*(?:const\s*)?\*)*|ptr\s)\s*\w+)*))\s*\)\s*{'

fcnre = re.compile(fcnreg)

gpats = {}
gcmds = {}

mel = []

def process1(f):
    data = exportutils.preprocess(f)
    pats = {}
    cmds = {}
    res = fcnre.search(data)
    while res is not None:
        if res.group('command'):
            cmds[res.group('name')] = res.group('command')
            if not res.group(0).startswith('static'):
                gcmds[res.group('name')] = res.group('command')
        else:
            pats[res.group('name')] = res.group('pattern')
            if not res.group(0).startswith('static'):
                gpats[res.group('name')] = res.group('pattern')
        res = fcnre.search(data, pos=res.end(0))

    res = patre.search(data)
    while res is not None:
        imp = res.group('imp')
        if res.group('cmdpat') == 'pattern':
            if imp not in pats and imp not in gpats:
                if imp in cmds or imp in gcmds:
                    print('command implementation {} for pattern {}.{}'.format(imp, res.group('mod'), res.group('fcn')))
                else:
                    mel.append(('pattern', imp, res.group('mod'), res.group('fcn')))
        else:
            if imp not in cmds and imp not in gcmds:
                if imp in pats or imp in gpats:
                    print('pattern implementation {} for command {}.{}'.format(imp, res.group('mod'), res.group('fcn')))
                else:
                    mel.append(('command ', imp, res.group('mod'), res.group('fcn')))
        res = patre.search(data, pos=res.end(0))

def process2():
    for (cmdpat, imp, mod, fcn) in mel:
        if cmdpat == 'pattern':
            if imp not in gpats:
                if imp in gcmds:
                    print('command implementation {} for pattern {}.{}'.format(imp, res.group('mod'), res.group('fcn')))
                else:
                    print('pattern implementation {} for {}.{} is missing'.format(imp, mod, fcn))
        else:
            if imp not in gcmds:
                if imp in gpats:
                    print('pattern implementation {} for command {}.{}'.format(imp, res.group('mod'), res.group('fcn')))
                else:
                    print('command implementation {} for {}.{} is missing'.format(imp, mod, fcn))

if len(sys.argv) > 1:
    files = sys.argv[1:]
else:
    files = map(lambda x: x.strip(), sys.stdin.readlines())
for f in files:
    process1(f)
process2()
