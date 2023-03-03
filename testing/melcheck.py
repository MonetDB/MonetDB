# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

import re, sys

try:
    import exportutils
except ImportError:
    from MonetDBtesting import exportutils

# MEL pattern
argreg = r'\s*,\s*(?P<bat>bat)?(?P<var>var)?arg(?P<any>any)?\s*\(\s*(?P<argname>"[^"]*")\s*,\s*(?P<argval>\w*)\s*\)'
patreg = r'^\s*(?P<cmdpat>pattern|command)\s*\(\s*"(?P<mod>[^"]*)"\s*,\s*"(?P<fcn>[^"]*)"\s*,\s*(?P<imp>\w+)\s*,[^,]*,\s*"[^\"]*(?:\\.[^\"]*)*"\s*,\s*args\s*\(\s*(?P<retc>\d+)\s*,\s*(?P<argc>\d+)(?P<args>(?:'+argreg+')*)\s*\)\s*\)'

argre = re.compile(argreg)
patre = re.compile(patreg, re.MULTILINE)

fcnargreg = r'\s*,\s*(?:const\s+)?(?:(?P<type>\w+)\s*\*(?:\s*(?:const\s*)?\*)*(?:\s*restrict\s)?|ptr\s)\s*(?P<argname>\w+)'
fcnreg = r'(?:static\s+)?(?:str|char\s*\*)\s+(?P<name>\w+)\s*\(\s*(?:(?P<pattern>Client\s+\w+\s*,\s*MalBlkPtr\s+\w+\s*,\s*MalStkPtr\s+\w+\s*,\s*InstrPtr\s+\w+)|(?P<command>(?:\w+\s*\*+|ptr\s)\s*\w+(?:'+ fcnargreg + r')*))\s*\)\s*{'

fcnre = re.compile(fcnreg)
fcnargre = re.compile(fcnargreg)

gpats = {}
gcmds = {}

mel = []
maldefs = {}

mappings = {
    'streams': 'Stream',
    'bstream': 'Bstream',
}

def malcheck(imp, mod, fcn, retc, argc, args):
    malfunc = f'{mod}.{fcn}'
    if retc == 0:
        retc = 1
        argc += 1
        args = ',arg("",void)' + args
    returns = []
    arguments = []
    pos = 0
    for i in range(argc):
        res = argre.match(args, pos)
        if res is None:
            print(f'not enough arguments in command {mod}.{fcn} with implementation {imp}')
            return
        normarg = res.group('bat','any','argval')
        if i < retc:
            returns.append(normarg)
        else:
            arguments.append(normarg)
        pos = res.end(0)
    if malfunc not in maldefs:
        maldefs[malfunc] = []
    for mf in maldefs[malfunc]:
        if mf[0] == retc and mf[1] == argc and mf[2] == returns and mf[3] == arguments:
            print(f'duplicate MAL definition for {mod}.{fcn} with implementations {mf[4]} and {imp}')
            return
    maldefs[malfunc].append((retc, argc, returns, arguments, imp))

def checkcommand(imp, mod, fcn, decl, retc, argc, args):
    if argc < retc:
        print('bad argc < retc for command {}.{} with implementation {}'.format(mod, fcn, imp))
        return
    decl = ',' + decl           # makes processing easier
    pos = 0
    cpos = 0
    if retc == 0:
        retc = 1
        argc += 1
        args = ',arg("",void)' + args
    for i in range(argc):
        res = argre.match(args, pos)
        if res is None:
            print('not enough arguments in command {}.{} with implementation {}'.format(mod, fcn, imp))
            return
        if res.group('var'):
            print('cannot have variable number of arguments in command {}.{} with implementation {}'.format(mod, fcn, imp))
            return
        if res.group('bat'):
            cmaltype = 'bat'
        elif res.group('any'):
            cmaltype = 'void'
        else:
            cmaltype = res.group('argval')
            cmaltype = mappings.get(cmaltype, cmaltype)
        cres = fcnargre.match(decl, cpos)
        if cres is None:
            print('not enough arguments in implementation {} for command {}.{}'.format(imp, mod, fcn))
            return
        ctype = cres.group('type')
        if not ctype:
            ctype = 'void'      # declared as "ptr val", so type is void *
        if i < retc and 'const' in cres.group(0):
            print('const return pointer in implementation {} for command {}.{} (arg {})'.format(imp, mod, fcn, i))
        if ctype != cmaltype:
            if cmaltype != 'str' or ctype != 'char' or cres.group(0).count('*') != 2:
                print('type mismatch for arg {} in implementation {} for command {}.{}'.format(i, imp, mod, fcn))
        pos = res.end(0)
        cpos = cres.end(0)

def process1(f):
    data = exportutils.preprocess(f, include=True)
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
        mod = res.group('mod')
        fcn = res.group('fcn')
        retc = res.group('retc')
        argc = res.group('argc')
        args = res.group('args')
        if res.group('cmdpat') == 'pattern':
            if imp not in pats and imp not in gpats:
                if imp in cmds or imp in gcmds:
                    print('command implementation {} for pattern {}.{}'.format(imp, mod, fcn))
                else:
                    mel.append(('pattern', imp, mod, fcn, retc, argc, args))
        else:
            if imp not in cmds and imp not in gcmds:
                if imp in pats or imp in gpats:
                    print('pattern implementation {} for command {}.{}'.format(imp, mod, fcn))
                else:
                    mel.append(('command', imp, mod, fcn, retc, argc, args))
            else:
                checkcommand(imp, mod, fcn, cmds.get(imp, gcmds.get(imp)), int(retc), int(argc), args)
        malcheck(imp, mod, fcn, int(retc), int(argc), args)
        res = patre.search(data, pos=res.end(0))

def process2():
    for (cmdpat, imp, mod, fcn, retc, argc, args) in mel:
        if cmdpat == 'pattern':
            if imp not in gpats:
                if imp in gcmds:
                    print('command implementation {} for pattern {}.{}'.format(imp, mod, fcn))
                else:
                    print('pattern implementation {} for {}.{} is missing'.format(imp, mod, fcn))
        else:
            if imp not in gcmds:
                if imp in gpats:
                    print('pattern implementation {} for command {}.{}'.format(imp, mod, fcn))
                else:
                    print('command implementation {} for {}.{} is missing'.format(imp, mod, fcn))
            else:
                checkcommand(imp, mod, fcn, gcmds[imp], int(retc), int(argc), args)

if len(sys.argv) > 1:
    files = sys.argv[1:]
else:
    files = map(lambda x: x.strip(), sys.stdin.readlines())
for f in files:
    process1(f)
process2()
