# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

import sys
import os
import subprocess
import string

# This script just calls the compiler (first argument) with the given
# arguments (following arguments), except that it replaces references
# to .LIB files that do not refer to shared libraries with a list of
# references to the .OBJ files inside those .LIB files.  .LIB files
# are recognized as not referring to shared libraries (.DLL files) if
# 1. the path to the file is not absolute; 2. the .LIB file exists
# (i.e. the compiler wouldn't need some search path to find it); and
# 3. there is no .DLL file in the same location as the .LIB file.
# This trick makes that the NOINST libraries that are specified in
# various Makefile.ag files are included completely, instead of just
# the files from those libraries that contain functions that happen to
# be referenced somewhere.

verbose = os.getenv('WINCOMPILEVERBOSE') is not None

# the splitcommand function is a straight copy of the same function in
# ../../testing/process.py.
def splitcommand(cmd):
    '''Like string.split, except take quotes into account.'''
    q = None
    w = []
    command = []
    for c in cmd:
        if q:
            if c == q:
                q = None
            else:
                w.append(c)
        elif c in string.whitespace:
            if w:
                command.append(''.join(w))
            w = []
        elif c == '"' or c == "'":
            q = c
        else:
            w.append(c)
    if w:
        command.append(''.join(w))
    if len(command) > 1 and command[0] == 'call':
        del command[0]
    return command

def process(args, recursive = False):
    argv = []
    for arg in args:
        if not recursive and arg[:1] == '@':
            argv.extend(process(splitcommand(open(arg[1:]).read()), True))
        elif arg[:1] in ('-', '/'):
            argv.append(arg)
        elif arg.endswith('.lib'):
            if os.path.isabs(arg) or not os.path.exists(arg) or os.path.exists(arg[:-4] + '.dll') or '\\' not in arg:
                argv.append(arg)
            else:
                dirname = os.path.dirname(arg)
                p = subprocess.Popen(['lib', '/nologo', '/list', arg],
                                     shell = False,
                                     universal_newlines = True,
                                     stdout = subprocess.PIPE)
                for f in p.stdout:
                    argv.append(os.path.join(dirname, f.strip()))
                p.wait()
        else:
            argv.append(arg)
    return argv

argv = process(sys.argv[1:])

if verbose:
    sys.stdout.write('EXECUTE: %s\n' % ' '.join(argv))
    sys.stdout.flush()
p = subprocess.Popen(argv, shell = False, universal_newlines = True,
                     stdout = subprocess.PIPE, stderr = subprocess.PIPE)
out, err = p.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
if p.returncode and not verbose:
    sys.stderr.write('failed invocation: %s\n' % ' '.join(argv))
    sys.stderr.flush()
sys.exit(p.returncode)
