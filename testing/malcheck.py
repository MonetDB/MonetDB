import re, sys

import exportutils

# MAL function: optional module with function name
malfre = r'(?P<malf>(?:[a-zA-Z_][a-zA-Z_0-9]*\.)?(?:[a-zA-Z_][a-zA-Z_0-9]*|[-+/*<>%=!]+))\s*(?:{[^}]*}\s*)?'
# MAL address declaration
addrre = r'address\s+(?P<func>[a-zA-Z_][a-zA-Z_0-9]*)'

# recognize MAL "command" declarations
comreg = re.compile(r'\bcommand\s+' + malfre + r'\(\s*(?P<args>[^()]*)\)\s*(?P<rets>\([^()]*\)|:\s*bat\[[^]]*\]|:\s*[a-zA-Z_][a-zA-Z_0-9]*|)\s+' + addrre + r'\b')

# recognize MAL "pattern" declarations
patreg = re.compile(r'\bpattern\s+' + malfre + r'\(\s*(?P<args>[^()]*)\)\s*(?P<rets>\([^()]*\)|:\s*bat\[[^]]*\](?:\.\.\.)?|:\s*[a-zA-Z_][a-zA-Z_0-9]*(?:\.\.\.)?|)\s+' + addrre + r'\b')

atmreg = re.compile(r'\batom\s+(?P<atom>[a-zA-Z_][a-zA-Z0-9_]*)(?:\s*[:=]\s*(?P<base>[a-zA-Z_][a-zA-Z0-9_]*))?\s*;')

treg = re.compile(r':\s*(bat\[[^]]*\]|[a-zA-Z_][a-zA-Z_0-9]*)')

expre = re.compile(r'\b[a-zA-Z_0-9]+export\s+(?P<decl>[^;]*;)', re.MULTILINE)
nmere = re.compile(r'\b(?P<name>[a-zA-Z_][a-zA-Z_0-9]*)\s*[[(;]')

freg = re.compile(r'(?P<rtype>\w+(?:\s*\*)*)\s*\b(?P<name>\w+)\((?P<args>[^()]*)\)')
creg = re.compile(r'\bconst\b')
sreg = re.compile(r'\bchar\s*\*')
areg = re.compile(r'\w+')
argreg = re.compile(r'\s*\w+$')

mappings = {
    'zrule': 'rule',
    'timezone': 'tzone',
    'streams': 'Stream',
    'bstream': 'Bstream',
    'any_1': 'void',
    'any_2': 'void',
    'any_3': 'void',
    'any_4': 'void',
    'sqlblob': 'blob',
}
cmappings = {
    'sqlblob': 'blob',
}
atomfunctypes = {
    # MAL name: (return type, (argument...))
    # where each argument is (type, const)
    'cmp': ('int', (('void *', True), ('void *', True))),
    'del': ('void', (('Heap *', False), ('var_t *', False))),
    'fix': ('int', (('void *', True),)),
    'fromstr': ('int', (('char *', True), ('int *', False), ('ptr *', False))),
    'hash': ('BUN', (('void *', True),)),
    'heap': ('void', (('Heap *', False), ('size_t', False))),
    'length': ('int', (('void *', False),)),
    'nequal': ('int', (('void *', True), ('void *', True))),
    'null': ('void *', (('void', False),)),
    'put': ('var_t', (('Heap *', False), ('var_t *', False), ('void *', True))),
    'read': ('void *', (('void *', False), ('stream *', False), ('size_t', False))),
    'storage': ('long', (('void', False),)),
    'tostr': ('int', (('str *', False), ('int *', False), ('void *', True))),
    'unfix': ('int', (('void *', True),)),
    'write': ('gdk_return', (('void *', True), ('stream *', False), ('size_t', False))),
    }

defre = re.compile(r'^[ \t]*#[ \t]*define[ \t]+(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\((?P<args>[a-zA-Z0-9_, \t]*)\)[ \t]*(?P<def>.*)$', re.MULTILINE)

cldef = re.compile(r'^[ \t]*#', re.MULTILINE)

malfuncs = []
malpats = []
atomfuncs = []
decls = {}
odecls = {}
pdecls = {}

def process(f):
    data = open(f).read()
    if f.endswith('.mal'):
        data = re.sub(r'[ \t]*#.*', '', data) # remove comments
        for res in comreg.finditer(data):
            malf, args, rets, func = res.groups()
            if not atomfunctypes.has_key(malf) or args.strip():
                rtypes = []
                atypes = []
                if not rets:
                    rets = ':void'
                for tres in treg.finditer(rets):
                    typ = tres.group(1)
                    if typ.startswith('bat['):
                        typ = 'bat'
                    rtypes.append(mappings.get(typ, typ))
                for tres in treg.finditer(args):
                    typ = tres.group(1)
                    if typ.startswith('bat['):
                        typ = 'bat'
                    atypes.append(mappings.get(typ, typ))
                malfuncs.append((tuple(rtypes), tuple(atypes), malf, func, f))
            elif args.strip():
                print 'atom function %s should be declared without arguments in %s' % (malf, f)
            else:
                if rets:
                    print 'atom function %s should be declared without return type in %s' % (malf, f)
                atom = None
                base = None
                for ares in atmreg.finditer(data, 0, res.start(0)):
                    atom = ares.group('atom')
                    base = ares.group('base')
                if not atom:
                    print 'atom function %s declared without known atom name in %s' % (malf, f)
                    continue
                atomfuncs.append((malf, atom, base, func, f))
        for res in patreg.finditer(data):
            malf, args, rets, func = res.groups()
            malpats.append((malf, func, f))
    elif f.endswith('.h') or f.endswith('.c'):
        data = exportutils.preprocess(data)

        for res in expre.finditer(data):
            pos = res.end(0)
            decl = exportutils.normalize(res.group('decl'))
            res = nmere.search(decl)
            if decl.startswith('char *'):
                decl = 'str ' + decl[6:]
            if '(' in decl:
                res = freg.match(decl)
                if res is not None:
                    rtype, name, args = res.groups()
                    args = map(lambda x: x.strip(), args.split(','))
                    if len(args) == 4 and \
                       args[0].startswith('Client ') and \
                       args[1].startswith('MalBlkPtr ') and \
                       args[2].startswith('MalStkPtr ') and \
                       args[3].startswith('InstrPtr ') and \
                       rtype == 'str':
                        pdecls[name] = f
                    elif rtype == 'str':
                        a = []
                        for arg in args:
                            if '(' in arg:
                                # complicated (function pointer) argument
                                break
                            if creg.search(arg) is not None:
                                rdonly = True
                                arg = creg.sub('', arg)
                            else:
                                rdonly = False
                            arg = arg.strip()
                            if arg.startswith('ptr ') and not '*' in arg:
                                arg = 'void *' + arg[4:]
                            # normalize "char *" to "str"
                            if arg.startswith('char **'):
                                arg = 'str ' + arg[6:]
                            elif arg.startswith('char *'):
                                arg = 'str' + arg[6:]
                            if '*' in arg or ' ' in arg:
                                # remove argument name (just keeping type)
                                arg = argreg.sub('', arg)
                            if '*' not in arg:
                                break
                            typ = areg.match(arg).group(0)
                            a.append((cmappings.get(typ, typ), rdonly))
                        else:
                            decls[name] = (tuple(a), f)
                    else:
                        if rtype == 'ptr':
                            rtype = 'void *'
                        a = []
                        for arg in args:
                            if '(' in arg:
                                # complicated (function pointer) argument
                                break
                            if creg.search(arg) is not None:
                                rdonly = True
                                arg = creg.sub('', arg)
                            else:
                                rdonly = False
                            arg = arg.strip()
                            if '*' in arg or ' ' in arg:
                                # remove argument name (just keeping type)
                                arg = argreg.sub('', arg)
                            if arg == 'str':
                                arg = 'char *'
                            a.append((arg, rdonly))
                        else:
                            odecls[name] = (rtype, tuple(a), f)

report_const = False
coverage = False
if len(sys.argv) > 1 and sys.argv[1] == '-c':
    del sys.argv[1]
    report_const = True
elif len(sys.argv) > 1 and sys.argv[1] == '-f':
    del sys.argv[1]
    coverage = True

if len(sys.argv) > 1:
    files = sys.argv[1:]
else:
    files = map(lambda x: x.strip(), sys.stdin.readlines())
for f in files:
    f = f.strip()
    process(f)

if coverage:
    for rtypes, atypes, malf, func, f in malfuncs:
        if decls.has_key(func):
            del decls[func]
    for malf, func, f in malpats:
        if pdecls.has_key(func):
            del pdecls[func]
    print 'commands:'
    for func in sorted(decls.keys()):
        print func
    print
    print 'patterns:'
    for func in sorted(pdecls.keys()):
        print func
else:
    for rtypes, atypes, malf, func, f in malfuncs:
        if not decls.has_key(func):
            print '%s: missing for MAL command %s in %s' % (func, malf, f)
        else:
            args, funcf = decls[func]
            if len(args) != len(rtypes) + len(atypes):
                print '%s in %s: argument count mismatch for %s %s' % (func, funcf, malf, f)
            else:
                args = list(args)
                i = 0
                for t in rtypes:
                    i = i + 1
                    if t != args[0][0] or args[0][1]:
                        print '%s in %s: return %d type mismatch for %s %s (%s vs %s)' % (func, funcf, i, malf, f, t, args[0][0])
                    del args[0]
                i = 0
                for t in atypes:
                    i = i + 1
                    # special dispensation for these functions: they
                    # handle str and json arguments both correctly
                    if func in ('JSONstr2json', 'JSONisvalid', 'JSONisobject', 'JSONisarray') and t in ('str', 'json'):
                        t = args[0][0]
                    if t != args[0][0]:
                        print '%s in %s: argument %d type mismatch for %s %s (%s vs %s)' % (func, funcf, i, malf, f, t, args[0][0])
                    elif report_const and not args[0][1]:
                        print '%s in %s: argument %d not const for %s %s (%s vs %s)' % (func, funcf, i, malf, f, t, args[0][0])
                    del args[0]

    for malf, func, f in malpats:
        if not pdecls.has_key(func):
            print '%s: missing for MAL pattern %s in %s' % (func, malf, f)

    for malf, atom, base, func, f in atomfuncs:
        if not odecls.has_key(func):
            print '%s: missing for MAL atom command %s in %s' % (func, malf, f)
        else:
            atm = mappings.get(atom, atom)
            rtype, args = atomfunctypes[malf]
            crtype, cargs, funcf = odecls[func]
            if len(args) != len(cargs):
                print '%s in %s: argument count mismatch for command %s for atom %s in %s' % (func, funcf, malf, atom, f)
            elif rtype != crtype and rtype == 'void *' and crtype != atm + ' *' and (base != 'str' or (crtype != atm and crtype != 'char *')):
                print '%s in %s: return type mismatch for command %s for atom %s in %s (%s vs %s)' % (func, funcf, malf, atom, f, rtype, crtype)
            else:
                for i in range(len(args)):
                    a1, r1 = args[i]
                    a2, r2 = cargs[i]
                    if r2 and not r1:
                        print 'argument %d of %s in %s incorrectly declared const for atom command %s in %s' % (i+1, func, funcf, malf, f)
                    if a1 != a2 and a1 == 'void *' and a2 != atm + ' *' and (base != 'str' or (a2 != atm and a2 != 'char *')):
                        print (a1,a2,atom,base)
                        print '%s in %s: argument %d mismatch for command %s for atom %s in %s (%s vs %s)' % (func, funcf, i+1, malf, atom, f, a1, a2)
