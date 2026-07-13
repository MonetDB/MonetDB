# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.

import re, sys

try:
    import exportutils
except ImportError:
    from MonetDBtesting import exportutils

# MEL pattern
argreg = r'\s*,\s*(?P<bat>(?:opt)?bat)?(?P<var>var)?arg(?P<any>any)?' \
    r'\s*\(\s*(?P<argname>"[^"]*")\s*,\s*(?P<argval>\w*)\s*\)'
patreg = r'^\s*(?P<cmdpat>pattern|command)\s*' \
    r'\(\s*"(?P<mod>[^"]*)"\s*,\s*"(?P<fcn>[^"]*)"\s*,' \
    r'\s*(?P<imp>\w+)\s*,[^,]*,\s*"[^\"]*(?:\\.[^\"]*)*"\s*,'\
    r'\s*args\s*\(\s*(?P<retc>\d+)\s*,\s*(?P<argc>\d+)' \
    r'(?P<args>(?:'+argreg+r')*)\s*\)\s*\)'

argre = re.compile(argreg)
patre = re.compile(patreg, re.MULTILINE)

fcnargreg = r'\s*,\s*(?:const\s+)?(?:(?P<type>\w+)\s*' \
    r'\*(?:\s*(?:const\s*)?\*)*(?:\s*restrict\s)?|ptr\s)\s*(?P<argname>\w+)'
fcnreg = r'(?:static\s+)?(?:str|char\s*\*)\s+(?P<name>\w+)\s*' \
    r'\(\s*(?:(?P<pattern>Client\s+\w+\s*,\s*MalBlkPtr\s+\w+\s*,' \
    r'\s*MalStkPtr\s+\w+\s*,\s*InstrPtr\s+\w+)|' \
    fr'(?P<command>Client\s+\w+(?:{fcnargreg})+))\s*\)\s*{{'

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
            print(f'not enough arguments in command {mod}.{fcn} with'
                  f' implementation {imp}')
            return
        normarg = res.group('bat', 'any', 'argval')
        if i < retc:
            returns.append(normarg)
        else:
            arguments.append(normarg)
        pos = res.end(0)
    if malfunc not in maldefs:
        maldefs[malfunc] = []
    for mf in maldefs[malfunc]:
        if mf[0] != retc or mf[1] != argc:
            continue
        if mf[2] == returns and mf[3] == arguments:
            print(f'duplicate MAL definition for {mod}.{fcn} with'
                  f' implementations {mf[4]} and {imp}')
            return
    maldefs[malfunc].append((retc, argc, returns, arguments, imp))


def checkcommand(imp, mod, fcn, decl, retc, argc, args):
    if argc < retc:
        print(f'bad argc < retc for command {mod}.{fcn} with'
              f' implementation {imp}')
        return
    decl = decl[decl.index(','):]  # skip over Client arg
    pos = 0
    cpos = 0
    if retc == 0:
        retc = 1
        argc += 1
        args = ',arg("",void)' + args
    for i in range(argc):
        res = argre.match(args, pos)
        if res is None:
            print(f'not enough arguments in command {mod}.{fcn} with'
                  f' implementation {imp}')
            return
        if res.group('var'):
            print('cannot have variable number of arguments in command'
                  f' {mod}.{fcn} with implementation {imp}')
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
            print(f'not enough arguments in implementation {imp} for'
                  f' command {mod}.{fcn}')
            return
        ctype = cres.group('type')
        if not ctype:
            ctype = 'void'      # declared as "ptr val", so type is void *
        if i < retc and 'const' in cres.group(0):
            print(f'const return pointer in implementation {imp} for'
                  f' command {mod}.{fcn} (arg {i})')
        if ctype != cmaltype:
            if cmaltype != 'str' or \
               ctype != 'char' or \
                   cres.group(0).count('*') != 2:
                print(f'type mismatch for arg {i} in'
                      f' implementation {imp} for command {mod}.{fcn}')
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
                    print(f'command implementation {imp} for'
                          f' pattern {mod}.{fcn}')
                else:
                    mel.append(('pattern', imp, mod, fcn, retc, argc, args))
        else:
            if imp not in cmds and imp not in gcmds:
                if imp in pats or imp in gpats:
                    print(f'pattern implementation {imp} for'
                          f' command {mod}.{fcn}')
                else:
                    mel.append(('command', imp, mod, fcn, retc, argc, args))
            else:
                checkcommand(imp, mod, fcn, cmds.get(imp, gcmds.get(imp)),
                             int(retc), int(argc), args)
        malcheck(imp, mod, fcn, int(retc), int(argc), args)
        res = patre.search(data, pos=res.end(0))


def process2():
    for (cmdpat, imp, mod, fcn, retc, argc, args) in mel:
        if cmdpat == 'pattern':
            if imp not in gpats:
                if imp in gcmds:
                    print(f'command implementation {imp} for'
                          f' pattern {mod}.{fcn}')
                else:
                    print(f'pattern implementation {imp} for'
                          f' {mod}.{fcn} is missing')
        else:
            if imp not in gcmds:
                if imp in gpats:
                    print(f'pattern implementation {imp} for'
                          f' command {mod}.{fcn}')
                else:
                    print(f'command implementation {imp} for'
                          f' {mod}.{fcn} is missing')
            else:
                checkcommand(imp, mod, fcn, gcmds[imp],
                             int(retc), int(argc), args)


if len(sys.argv) > 1:
    files = sys.argv[1:]
else:
    files = map(lambda x: x.strip(), sys.stdin.readlines())
for f in files:
    process1(f)
process2()
