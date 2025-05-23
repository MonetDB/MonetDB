# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

import os
import sys
import re

from . import exportutils

# sets of directories/files that end up in the same shared object
dirlist = {
    'gdk': ['gdk',
            os.path.join('common', 'options')],
    'mapi': [os.path.join('clients', 'mapilib'),
             os.path.join('common', 'options')],
    'monetdb5': ['monetdb5'],
    'stream': [os.path.join('common', 'stream')],
    'monetdbe': [os.path.join('tools', 'monetdbe', 'monetdbe.h')],
    'sql': ['sql'],
    'mutils': [os.path.join('common', 'utils')],
    }
libs = sorted(dirlist.keys())

# directories we skip
skipdirs = ['extras']

# individual files we skip
skipfiles = []

# where the files are
srcdir = os.environ.get('TSTSRCBASE')
if not srcdir:
    print('cannot find source directory (TSTSRCBASE environment variable)',
          file=sys.stderr)
    sys.exit(1)

# the export command; note the keyword we look for is a word that ends
# in "export"
expre = re.compile(r'\b[a-zA-Z_0-9]+export\s+(?P<decl>[^;]*;)', re.MULTILINE)

# the function or variable name
nmere = re.compile(r'\b(?P<name>[a-zA-Z_][a-zA-Z_0-9]*)\s*[([;]')

def extract(f):
    decls = []

    data = exportutils.preprocess(f, include=False)
    data = re.sub(r'"[^"\\]*(\\.[^"\\]*)*"', '""', data)

    res = expre.search(data)
    while res is not None:
        pos = res.end(0)
        decl = res.group('decl')
        if '{' in decl:
            print(f'export on definition:\n{res.group(0)}', file=sys.stderr)
        elif '"hidden"' in decl:
            print(f'cannot export hidden function:\n{res.group(0)}', file=sys.stderr)
        else:
            decl = exportutils.normalize(decl)
            res = nmere.search(decl)
            if res is not None:
                decls.append((res.group('name'), decl))
            else:
                decls.append(('', decl))
        res = expre.search(data, pos)
    return decls

def mywalk(d):
    if os.path.isfile(d):
        root, file = os.path.split(d)
        return [(root, [], [file])]
    return os.walk(d)

def findfiles(dirlist, skipfiles=[], skipdirs=[], fileset=None):
    decls = []
    done = {}
    for d in dirlist:
        for root, dirs, files in mywalk(d):
            for d in skipdirs:
                if d in dirs:
                    dirs.remove(d)
            for f in files:
                if f not in done and \
                        (f.endswith('.c') or f.endswith('.h')) and \
                        not f.startswith('.') and \
                        f not in skipfiles:
                    ff = os.path.join(root, f)
                    if os.path.isfile(ff) and (fileset is None or ff in fileset):
                        decls.extend(extract(ff))
                    done[f] = True
    decls.sort()
    return [decl for name, decl in decls]

def getrepofiles():
    curdir = os.getcwd()
    os.chdir(srcdir)
    if os.path.exists(os.path.join('.hg', 'store')):
        import subprocess
        with subprocess.Popen(['hg', '--config', 'ui.verbose=False', 'files', '-I', '**.[ch]'],
                              stdout=subprocess.PIPE,
                              universal_newlines=True) as p:
            out, err = p.communicate()
        fileset = set([os.path.join(srcdir, f) for f in filter(None, out.split('\n'))])
    else:
        fileset = None
    os.chdir(curdir)
    return fileset

def listexports():
    output = []
    fileset = getrepofiles()
    for lib in libs:
        dirs = dirlist[lib]
        dl = [os.path.join(srcdir, d) for d in dirs]
        decls = findfiles(dl, skipfiles=skipfiles, skipdirs=skipdirs, fileset=fileset)
        output.append(f'# {lib}\n')
        for d in decls:
            output.append(d + '\n')
        output.append('\n')
    return output

def main():
    print(*listexports(), sep='', end='')

if __name__ == '__main__':
    main()
