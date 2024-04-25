#!/usr/bin/env python3

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

import os, sys, argparse, stat

license = [
    'SPDX-License-Identifier: MPL-2.0',
    '',
    'This Source Code Form is subject to the terms of the Mozilla Public',
    'License, v. 2.0.  If a copy of the MPL was not distributed with this',
    'file, You can obtain one at http://mozilla.org/MPL/2.0/.',
    '',
    'Copyright 2024 MonetDB Foundation;',
    'Copyright August 2008 - 2023 MonetDB B.V.;',
    'Copyright 1997 - July 2008 CWI.',
    ]

def main():
    func = addlicense
    pre = post = start = end = None
    verbose = False
    parser = argparse.ArgumentParser(description='Update license texts')
    parser.add_argument('--pre', action='store', default=None,
                        help='line before license text')
    parser.add_argument('--post', action='store', default=None,
                        help='line after license text')
    parser.add_argument('--start', action='store', default=None,
                        help='text at start of license text line')
    parser.add_argument('--end', action='store', default=None,
                        help='text at end of license text line')
    parser.add_argument('--nl', action='store_false',
                        help='whether a blank line is added after license text')
    parser.add_argument('--add', '-a', action='store_true',
                        help='add license file (default)')
    parser.add_argument('--remove', '-r', action='store_true',
                        help='remove license file')
    parser.add_argument('--list', '-s', action='store_true',
                        help='list files that already contain the license')
    parser.add_argument('--license', '-l', action='store',
                        type=argparse.FileType('r'),
                        help='file with license text')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='be a bit more verbose')
    parser.add_argument('files', nargs='*',
                        help='files to work on (default read from stdin)')
    opts = parser.parse_args()
    if opts.add + opts.remove + opts.list > 1:
        print('--add, --remove, and --list are mutually exclusive',
              file=sys.stderr)
        sys.exit(1)
    if opts.remove:
        func = dellicense
    elif opts.list:
        func = listfile
    else:
        func = addlicense
    if opts.license is not None:
        del license[:]
        license.extend([l.rstrip('\n') for l in opts.license.readlines()])

    if opts.files:
        for a in opts.files:
            func(a, pre=opts.pre, post=opts.post, start=opts.start, end=opts.end, verbose=opts.verbose)
    else:
        while True:
            filename = sys.stdin.readline()
            if not filename:
                break
            func(filename[:-1], pre=opts.pre, post=opts.post, start=opts.start, end=opts.end, verbose=opts.verbose)

suffixrules = {
    # suffix: (pre,     post,  start,  end, nl)
    # pre: line before license text
    # post: line after license text
    # start: at start of each line of license text
    # end: at end of each line of license text
    # nl: whether a blank line should be added after license text
    '.1':     ('',      '.\\"','.\\" ','', False), # manual page source
    '.bash':  ('',      '',    '# ',   '',  True),  # shell script
    '.bat':   ('',      '',    '@REM ','',  True),  # Windows cmd batch script
    '.c':     ('/*',    ' */', ' * ',  '',  True),  # C source
    '.cc':    ('',      '',    '// ',  '',  True),  # C++ source
    '.cmake': ('#[[',   '#]]', '# ',   '',  True),  # CMake source
    '.cpp':   ('',      '',    '// ',  '',  True),  # C++ source
    '.el':    ('',      '',    '; ',   '',  True),  # Emacs Lisp
    '.fc':    ('',      '',    '# ',   '',  True),  # SELinux file context
    '.h':     ('/*',    ' */', ' * ',  '',  True),  # C header file
    '.hs':    ('',      '',    '-- ',  '',  True),  # Haskell source
    '.html':  ('<!--',  '-->', '',     '',  True),  # HTML source
    '.java':  ('/*',    ' */', ' * ',  '',  True),  # Java source
    '.l':     ('/*',    ' */', ' * ',  '',  True),  # (f)lex source
    '.mal':   ('',      '',    '# ',   '',  True),  # MonetDB Assembly Language
    '.pc':    ('',      '',    '# ',   '',  True),  # Package config source
    '.php':   ('<?php', '?>',  '# ',   '',  True),  # PHP source
    '.pl':    ('',      '',    '# ',   '',  True),  # Perl source
    '.pm':    ('',      '',    '# ',   '',  True),  # Perl module source
    '.py':    ('',      '',    '# ',   '',  True),  # Python source
    '.R':     ('',      '',    '# ',   '',  True),  # R source
    '.rb':    ('',      '',    '# ',   '',  True),  # Ruby source
    '.rc':    ('',      '',    '// ',  '',  True),  # Windows resource file
    '.rst':   ('',      '',    '.. ',  '',  True),  # reStructured Text
    '.sh':    ('',      '',    '# ',   '',  True),  # shell script
    '.spec':  ('',      '',    '# ',   '',  True),  # RPM specification file
    '.sql':   ('',      '',    '-- ',  '',  True),  # SQL source
    '.t':     ('',      '',    '# ',   '',  True),  # Perl test
    '.te':    ('',      '',    '# ',   '',  True),  # SELinux
    '.xml':   ('<!--',  '-->', '',     '',  True),  # XML source
    '.y':     ('/*',    ' */', ' * ',  '',  True),  # yacc (bison) source
    # we also match some complete filenames
    'CMakeLists.txt': ('#[[', '#]]', '# ', '', True),
    'Makefile': ('', '', '# ', '', True),
    '.merovingian_properties': ('', '', '# ', '', True),
    'copyright': ('', '', '', '', True),
    'license.txt': ('', '', '', '', True),
    }

def getcomments(file, pre=None, post=None, start=None, end=None, nl=True):
    ext = ''
    if pre is None and post is None and start is None and end is None:
        if file.endswith('.in') and os.path.basename(file[:-3]) in suffixrules:
            ext = os.path.basename(file[:-3])
        elif os.path.basename(file) in suffixrules:
            ext = os.path.basename(file)
        else:
            root, ext = os.path.splitext(file)
            if ext == '.in':
                # special case: .in suffix doesn't count
                root, ext = os.path.splitext(root)
            if ext not in suffixrules:
                # no known suffix
                # see if file starts with #! (i.e. shell script)
                f = open(file)      # can raise IOError
                line = f.readline()
                f.close()
                if line[:2] == '#!':
                    if 'bash' in line or '/sh' in line:
                        ext = '.sh'
                    elif 'python' in line or 'PYTHON' in line:
                        ext = '.py'
                    elif 'perl' in line:
                        ext = '.pl'
                    elif 'make' in line:
                        ext = 'Makefile'
                else:
                    return '', '', '', '', '', True
        pre, post, start, end, nl = suffixrules[ext]
    if pre is None:
        pre = ''
    if post is None:
        post = ''
    if start is None:
        start = ''
    if end is None:
        end = ''
    return ext, pre, post, start, end, nl

PERL_COPYRIGHT = 'COPYRIGHT AND LICENCE\n\n'
COPYRIGHT_NOTICE = 'Copyright Notice\n================\n\n'

def addlicense(file, pre=None, post=None, start=None, end=None, verbose=False):
    try:
        f = open(file)
    except IOError:
        print(f'Cannot open file {file}', file=sys.stderr)
        return
    try:
        g = open(file + '.new', 'w')
    except IOError:
        print(f'Cannot create temp file {file}.new', file=sys.stderr)
        return
    try:
        data = f.read()
    except UnicodeError:
        print(f'UnicodeError in file {file}', file=sys.stderr)
        return
    if PERL_COPYRIGHT in data:
        notice = PERL_COPYRIGHT
    elif COPYRIGHT_NOTICE in data:
        notice = COPYRIGHT_NOTICE
    else:
        notice = ''
    if notice:
        pos = data.find(notice) + len(notice)
        g.write(data[:pos])
        for l in license:
            g.write(l)
            g.write('\n')
        g.write(data[pos:])
    else:
        try:
            ext, pre, post, start, end, nl = getcomments(file, pre, post, start, end)
            if not ext:
                return
        except IOError:
            return
        f = open(file)
        line = f.readline()
        addblank = False
        if line.startswith('#!'):
            # if file starts with #! command interpreter, keep the line there
            g.write(line)
            # add a blank line
            addblank = True
            line = f.readline()
        if '-*-' in line:
            # if file starts with an Emacs mode specification, keep
            # the line there
            g.write(line)
            # add a blank line
            addblank = True
            line = f.readline()
        if 'vim:' in line:
            # if file starts with a vim mode specification, keep
            # the line there
            g.write(line)
            # add a blank line
            addblank = True
            line = f.readline()
        if line.startswith('<?xml'):
            # if line starts with an XML declaration, keep the line there
            g.write(line)
            # add a blank line
            addblank = True
            line = f.readline()
        if addblank:
            g.write('\n')
        if pre:
            g.write(pre + '\n')
        for l in license:
            if l[:1] == '\t' or (not l and (not end or end[:1] == '\t')):
                # if text after start begins with tab, remove spaces from start
                g.write(start.rstrip() + l + end + '\n')
            else:
                g.write(start + l + end + '\n')
        if post:
            g.write(post + '\n')
        # add empty line after license
        if line and nl:
            g.write('\n')
        # but only one, so skip empty line from file, if any
        if line and line != '\n':
            g.write(line)
        # copy rest of file
        g.write(f.read())
    f.close()
    g.close()
    try:
        st = os.stat(file)
        os.chmod(file + '.new', stat.S_IMODE(st.st_mode))
    except OSError:
        pass
    try:
        os.rename(file, file + '~')     # make backup
    except OSError:
        print(f'Cannot make backup for {file}', file=sys.stderr)
        return
    try:
        os.rename(file + '.new', file)
        if verbose:
            print(file)
    except OSError:
        print(f'Cannot move file {file} into position', file=sys.stderr)

def normalize(s):
    # normalize white space: remove leading and trailing white space,
    # and replace multiple white space characters by a single space
    return ' '.join(s.split())

def dellicense(file, pre=None, post=None, start=None, end=None, verbose=False):
    try:
        f = open(file)
    except IOError:
        print(f'Cannot open file {file}', file=sys.stderr)
        return
    try:
        g = open(file + '.new', 'w')
    except IOError:
        print(f'Cannot create temp file {file}.new', file=sys.stderr)
        return
    try:
        data = f.read()
    except UnicodeError:
        print(f'UnicodeError in file {file}', file=sys.stderr)
        return
    if PERL_COPYRIGHT in data:
        notice = PERL_COPYRIGHT
    elif COPYRIGHT_NOTICE in data:
        notice = COPYRIGHT_NOTICE
    else:
        notice = ''
    if notice:
        pos = data.find(notice) + len(notice)
        g.write(data[:pos])
        for l in license:
            while data[pos] == ' ':
                pos += 1
            if data[pos:pos+len(l)+1] != l + '\n':
                print(f'line doesn\'t match in file {file}', file=sys.stderr)
                print(f'file:    "{data[pos:pos+len(l)]}"', file=sys.stderr)
                print(f'license: "{l}"', file=sys.stderr)
                f.close()
                g.close()
                try:
                    os.unlink(file + '.new')
                except OSError:
                    pass
                return
            pos += len(l) + 1
        g.write(data[pos:])
    else:
        try:
            ext, pre, post, start, end, nl = getcomments(file, pre, post, start, end)
            if not ext:
                return
        except IOError:
            return
        pre = normalize(pre)
        post = normalize(post)
        start = normalize(start)
        end = normalize(end)
        f = open(file)
        line = f.readline()
        if line.startswith('#!'):
            g.write(line)
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if '-*-' in line:
            g.write(line)
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if 'vim:' in line:
            g.write(line)
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if line.startswith('<?xml'):
            g.write(line)
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        nline = normalize(line)
        if pre:
            if nline == pre:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print(f'PRE doesn\'t match in file {file}', file=sys.stderr)
                f.close()
                g.close()
                try:
                    os.unlink(file + '.new')
                except OSError:
                    pass
                return
        for l in license:
            if normalize(l) in nline:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print(f'line doesn\'t match in file {file}', file=sys.stderr)
                print(f'file:    "{line}"', file=sys.stderr)
                print(f'license: "{l}"', file=sys.stderr)
                f.close()
                g.close()
                try:
                    os.unlink(file + '.new')
                except OSError:
                    pass
                return
        if post:
            if nline == post:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print(f'POST doesn\'t match in file {file}', file=sys.stderr)
                f.close()
                g.close()
                try:
                    os.unlink(file + '.new')
                except OSError:
                    pass
                return
        if line and line != '\n':
            g.write(line)
        g.write(f.read())
    f.close()
    g.close()
    try:
        st = os.stat(file)
        os.chmod(file + '.new', stat.S_IMODE(st.st_mode))
    except OSError:
        pass
    try:
        os.rename(file, file + '~')     # make backup
    except OSError:
        print(f'Cannot make backup for {file}', file=sys.stderr)
        return
    try:
        os.rename(file + '.new', file)
        if verbose:
            print(file)
    except OSError:
        print(f'Cannot move file {file} into position', file=sys.stderr)

def listfile(file, pre=None, post=None, start=None, end=None, verbose=False):
    try:
        f = open(file)
    except IOError:
        print(f'Cannot open file {file}', file=sys.stderr)
        return
    try:
        data = f.read()
    except UnicodeDecodeError:
        print(f'File {file} not Unicode', file=sys.stderr)
        return
    if PERL_COPYRIGHT in data:
        notice = PERL_COPYRIGHT
    elif COPYRIGHT_NOTICE in data:
        notice = COPYRIGHT_NOTICE
    else:
        notice = ''
    if notice:
        pos = data.find(notice) + len(notice)
        for l in license:
            while data[pos] == ' ':
                pos += 1
            if data[pos:pos+len(l)+1] != l + '\n':
                print(f'line doesn\'t match in file {file}', file=sys.stderr)
                print(f'file:    "{data[pos:pos+len(l)]}"', file=sys.stderr)
                print(f'license: "{l}"', file=sys.stderr)
                f.close()
                return
            pos += len(l) + 1
    else:
        try:
            ext, pre, post, start, end, nl = getcomments(file, pre, post, start, end)
            if not ext:
                return
        except IOError:
            return
        pre = normalize(pre)
        post = normalize(post)
        start = normalize(start)
        end = normalize(end)
        f = open(file)
        line = f.readline()
        if line.startswith('#!'):
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if '-*-' in line:
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if 'vim:' in line:
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if line.startswith('<?xml'):
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        nline = normalize(line)
        if pre:
            if nline == pre:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print(f'PRE doesn\'t match in file {file}', file=sys.stderr)
                f.close()
                return
        for l in license:
            if normalize(l) in nline:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print(f'line doesn\'t match in file {file}', file=sys.stderr)
                print(f'file:    "{line}"', file=sys.stderr)
                print(f'license: "{l}"', file=sys.stderr)
                f.close()
                return
        if post:
            if nline == post:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print(f'POST doesn\'t match in file {file}', file=sys.stderr)
                f.close()
                return
    f.close()
    print(file)

if __name__ == '__main__' or sys.argv[0] == __name__:
    main()
