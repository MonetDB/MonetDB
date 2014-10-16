import re, sys

import exportutils

# recognize MAL "command" declarations
comreg = re.compile(r'\bcommand\s+(?P<malf>[a-zA-Z_][a-zA-Z_0-9.]*)\s*(?:{[^}]*}\s*)?\(\s*(?P<args>[^()]*)\)\s*(?P<rets>\([^()]*\)|:bat\[[^]]*\]|:[a-zA-Z_][a-zA-Z_0-9]*|)\s+address\s+(?P<func>[a-zA-Z_][a-zA-Z_0-9]*)\b')

# recognize MAL "pattern" declarations
patreg = re.compile(r'\bpattern\s+(?P<malf>[a-zA-Z_][a-zA-Z_0-9.]*)\s*(?:{[^}]*}\s*)?\(\s*(?P<args>[^()]*)\)\s*(?P<rets>\([^()]*\)|:bat\[[^]]*\]|:[a-zA-Z_][a-zA-Z_0-9]*|)\s+address\s+(?P<func>[a-zA-Z_][a-zA-Z_0-9]*)\b')

treg = re.compile(r':(bat\[[^]]*\]|[a-zA-Z_][a-zA-Z_0-9]*)')

expre = re.compile(r'\b[a-zA-Z_0-9]+export\s+(?P<decl>[^;]*;)', re.MULTILINE)
nmere = re.compile(r'\b(?P<name>[a-zA-Z_][a-zA-Z_0-9]*)\s*[[(;]')

freg = re.compile(r'str (?P<name>\w+)\((?P<args>[^()]*)\)')
creg = re.compile(r'\bconst\b')
sreg = re.compile(r'\bchar\s*\*')
areg = re.compile(r'\w+')

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

defre = re.compile(r'^[ \t]*#[ \t]*define[ \t]+(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\((?P<args>[a-zA-Z0-9_, \t]*)\)[ \t]*(?P<def>.*)$', re.MULTILINE)

cldef = re.compile(r'^[ \t]*#', re.MULTILINE)

malfuncs = []
malpats = []
decls = {}
pdecls = {}

def process(f):
    data = open(f).read()
    if f.endswith('.mal'):
        res = comreg.search(data)
        while res is not None:
            malf, args, rets, func = res.groups()
            if malf not in ('del', 'cmp', 'fromstr', 'fix', 'heap', 'hash', 'length', 'null', 'nequal', 'put', 'storage', 'tostr', 'unfix', 'read', 'write', 'epilogue'):
                rtypes = []
                atypes = []
                if not rets:
                    rets = ':void'
                tres = treg.search(rets)
                while tres is not None:
                    typ = tres.group(1)
                    if typ.startswith('bat['):
                        typ = 'bat'
                    rtypes.append(mappings.get(typ, typ))
                    tres = treg.search(rets, tres.end(0))
                tres = treg.search(args)
                while tres is not None:
                    typ = tres.group(1)
                    if typ.startswith('bat['):
                        typ = 'bat'
                    atypes.append(mappings.get(typ, typ))
                    tres = treg.search(args, tres.end(0))
                malfuncs.append((tuple(rtypes), tuple(atypes), malf, func, f))
            res = comreg.search(data, res.end(0))
        res = patreg.search(data)
        while res is not None:
            malf, args, rets, func = res.groups()
            malpats.append((malf, func, f))
            res = patreg.search(data, res.end(0))
    elif f.endswith('.h') or f.endswith('.c'):
        data = exportutils.preprocess(data)

        res = expre.search(data)
        while res is not None:
            pos = res.end(0)
            decl = exportutils.normalize(res.group('decl'))
            res = nmere.search(decl)
            if decl.startswith('char *'):
                decl = 'str ' + decl[6:]
            if '(' in decl and decl.startswith('str '):
                res = freg.match(decl)
                if res is not None:
                    name, args = res.groups()
                    args = map(lambda x: x.strip(), args.split(','))
                    if len(args) == 4 and \
                       args[0].startswith('Client ') and \
                       args[1].startswith('MalBlkPtr ') and \
                       args[2].startswith('MalStkPtr ') and \
                       args[3].startswith('InstrPtr '):
                        pdecls[name] = f
                    else:
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
                            if '*' not in arg:
                                break
                            arg = sreg.sub('str', arg)
                            typ = areg.match(arg).group(0)
                            a.append((cmappings.get(typ, typ), rdonly))
                        else:
                            decls[name] = (tuple(a), f)
            res = expre.search(data, pos)

report_const = False
if len(sys.argv) > 1 and sys.argv[1] == '-c':
    del sys.argv[1]
    report_const = True

if len(sys.argv) > 1:
    files = sys.argv[1:]
else:
    files = map(lambda x: x.strip(), sys.stdin.readlines())
for f in files:
    f = f.strip()
    process(f)

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
