import re

# a function-like #define that we expand to also find exports hidden
# in preprocessor macros
defre = re.compile(r'^[ \t]*#[ \t]*define[ \t]+'            # #define
                   r'(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)'      # name being defined
                   r'\((?P<args>[a-zA-Z0-9_, \t]*)\)[ \t]*' # arguments
                   r'(?P<def>.*)$',                         # macro replacement
                   re.MULTILINE)
# line starting with a "#"
cldef = re.compile(r'^[ \t]*#', re.MULTILINE)

# white space
spcre = re.compile(r'\s+')

# some regexps helping to normalize a declaration
strre = re.compile(r'([^ *])\*')
comre = re.compile(r',\s*')

# comments (/* ... */ where ... is as short as possible)
cmtre = re.compile(r'/\*.*?\*/|//[^\n]*', re.DOTALL)

deepnesting = False

# do something a bit like the C preprocessor
#
# we expand function-like macros and remove all ## sequences from the
# replacement (even when there are no adjacent parameters that were
# replaced), but this is good enough for our purpose of finding
# exports that are hidden away in several levels of macro definitions
#
# we assume that there are no continuation lines in the input
def preprocess(data):
    # remove C comments
    res = cmtre.search(data)
    while res is not None:
        data = data[:res.start(0)] + ' ' + data[res.end(0):]
        res = cmtre.search(data, res.start(0))
    # remove \ <newline> combo's
    data = data.replace('\\\n', '')

    defines = {}
    ndata = []
    for line in data.split('\n'):
        res = defre.match(line)
        if res is not None:
            name, args, body = res.groups()
            args = tuple([x.strip() for x in args.split(',')])
            if len(args) == 1 and args[0] == '':
                args = ()       # empty argument list
            if name not in ('__attribute__', '__format__', '__alloc_size__') and \
               (name not in defines or not defines[name][1].strip()):
                defines[name] = (args, body)
        else:
            changed = True
            while changed:
                line, changed = replace(line, defines, [])
            if not cldef.match(line):
                ndata.append(line)
    return '\n'.join(ndata)

def replace(line, defines, tried):
    changed = False
    # match argument to macro with optionally several levels
    # of parentheses
    if deepnesting:     # optionally deeply nested parentheses
        nested = r'(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*(?:\([^()]*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*\)[^()]*)*'
    else:
        nested = ''
    for name, (args, body) in defines.items():
        if name in tried:
            continue
        pat = r'\b%s\b' % name
        sep = r'\('
        for arg in args:
            pat = pat + sep + r'([^,()]*(?:\([^()]*' + nested + r'\)[^,()]*)*)'
            sep = ','
        pat += r'\)'
        repl = {}
        r = re.compile(pat)
        res = r.search(line)
        while res is not None:
            bd = body
            changed = True
            if len(args) > 0:
                pars = [x.strip() for x in res.groups()]
                pat = r'\b(?:'
                sep = ''
                for arg, par in zip(args, pars):
                    repl[arg] = par
                    pat += sep + arg
                    sep = '|'
                pat += r')\b'
                r2 = re.compile(pat)
                res2 = r2.search(bd)
                while res2 is not None:
                    arg = res2.group(0)
                    if bd[res2.start(0)-1:res2.start(0)] == '#' and \
                       bd[res2.start(0)-2:res2.start(0)] != '##':
                        # replace #ARG by stringified replacement
                        pos = res2.start(0) + len(repl[arg]) + 1
                        bd = bd[:res2.start(0)-1] + '"' + repl[arg] + '"' + bd[res2.end(0):]
                    else:
                        pos = res2.start(0) + len(repl[arg])
                        bd = bd[:res2.start(0)] + repl[arg] + bd[res2.end(0):]
                    res2 = r2.search(bd, pos)
            bd = bd.replace('##', '')
            bd, changed = replace(bd, defines, tried + [name])
            line = line[:res.start(0)] + bd + line[res.end(0):]
            res = r.search(line, res.start(0) + len(bd))
    return line, changed

def normalize(decl):
    decl = spcre.sub(' ', decl) \
                .replace(' ;', ';') \
                .replace(' (', '(') \
                .replace('( ', '(') \
                .replace(' )', ')') \
                .replace(') ', ')') \
                .replace('* ', '*') \
                .replace(' ,', ',') \
                .replace(')__attribute__', ') __attribute__')
    decl = strre.sub(r'\1 *', decl)
    decl = comre.sub(', ', decl)
    return decl
