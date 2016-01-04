#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import os, sys, getopt, stat

usage = '''\
%(prog)s [-ar] [-l licensefile] [file...]

Options:
-a\tadd license text (default)
-r\tremove license text
-l licensefile
\tprovide alternative license text
\t(handy for removing incorrect license text)
-v\treport changed files on standard output

If no file arguments, %(prog)s will read file names from standard input.

%(prog)s makes backups of all modified files.
The backup is the file with a tilde (~) appended.\
'''

license = [
    'This Source Code Form is subject to the terms of the Mozilla Public',
    'License, v. 2.0.  If a copy of the MPL was not distributed with this',
    'file, You can obtain one at http://mozilla.org/MPL/2.0/.',
    '',
    'Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.',
    ]

def main():
    func = addlicense
    pre = post = start = end = None
    verbose = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'arl:sv',
                                   ['pre=', 'post=', 'start=', 'end='])
    except getopt.GetoptError:
        print >> sys.stderr, usage % {'prog': sys.argv[0]}
        sys.exit(1)
    for o, a in opts:
        if o == '-a':
            func = addlicense
        elif o == '-r':
            func = dellicense
        elif o == '-s':
            func = listfile
        elif o == '-l':
            try:
                f = open(a)
            except IOError:
                print >> sys.stderr, 'Cannot open file %s' % a
                sys.exit(1)
            del license[:]
            while True:
                line = f.readline()
                if not line:
                    break
                license.append(line[:-1])
            f.close()
        elif o == '--pre':
            pre = a
        elif o == '--post':
            post = a
        elif o == '--start':
            start = a
        elif o == '--end':
            end = a
        elif o == '-v':
            verbose = True

    if args:
        for a in args:
            func(a, pre=pre, post=post, start=start, end=end, verbose=verbose)
    else:
        while True:
            filename = sys.stdin.readline()
            if not filename:
                break
            func(filename[:-1], pre=pre, post=post, start=start, end=end, verbose=verbose)

suffixrules = {
    # suffix:(pre,     post,  start,  end)
    '.ac':   ('',      '',    'dnl ', ''),
    '.ag':   ('',      '',    '# ',   ''),
    '.am':   ('',      '',    '# ',   ''),
    '.bash': ('',      '',    '# ',   ''),
    '.bat':  ('',      '',    '@REM ',''),
    '.brg':  ('/*',    ' */', ' * ',  ''),
    '.c':    ('/*',    ' */', ' * ',  ''),
    '.cc':   ('',      '',    '// ',  ''),
    '.cf':   ('',      '',    '# ',   ''),
    '.cpp':  ('',      '',    '// ',  ''),
    '.el':   ('',      '',    '; ',   ''),
    '.h':    ('/*',    ' */', ' * ',  ''),
    '.hs':   ('',      '',    '-- ',  ''),
    '.html': ('<!--',  '-->', '',     ''),
    '.i':    ('',      '',    '// ',  ''),
    '.java': ('/*',    ' */', ' * ',  ''),
    '.l':    ('/*',    ' */', ' * ',  ''),
    '.m4':   ('',      '',    'dnl ', ''),
    '.mal':  ('',      '',    '# ',   ''),
    '.mil':  ('',      '',    '# ',   ''),
    '.mk':   ('',      '',    '# ',   ''),
    '.msc':  ('',      '',    '# ',   ''),
    '.php':  ('<?php', '?>',  '# ',   ''),
    '.pc':   ('',      '',    '# ',   ''),
    '.pl':   ('',      '',    '# ',   ''),
    '.pm':   ('',      '',    '# ',   ''),
    '.py':   ('',      '',    '# ',   ''),
    '.R':    ('',      '',    '# ',   ''),
    '.rb':   ('',      '',    '# ',   ''),
    '.rc':   ('',      '',    '// ',  ''),
    '.rst':  ('',      '',    '.. ',  ''),
    '.sh':   ('',      '',    '# ',   ''),
    '.sql':  ('',      '',    '-- ',  ''),
    '.t':    ('',      '',    '# ',   ''),
    '.xml':  ('<!--',  '-->', '',     ''),
    '.xq':   ('(:',    ':)',  '',     ''),
    '.xs':   ('/*',    ' */', ' * ',  ''),
    '.y':    ('/*',    ' */', ' * ',  ''),
    # we also match some complete filenames
    'Makefile': ('', '', '# ', ''),
    '.merovingian_properties.in': ('', '', '# ', ''),
    'configure.ag': ('', '', 'dnl ', ''),
    'copyright': ('', '', '', ''),
    'license.txt': ('', '', '', ''),
    }

def getcomments(file, pre = None, post = None, start = None, end = None):
    ext = ''
    if pre is None and post is None and start is None and end is None:
        if suffixrules.has_key(os.path.basename(file)):
            ext = os.path.basename(file)
        else:
            root, ext = os.path.splitext(file)
            if ext == '.in':
                # special case: .in suffix doesn't count
                root, ext = os.path.splitext(root)
            if not suffixrules.has_key(ext):
                # no known suffix
                # see if file starts with #! (i.e. shell script)
                f = open(file)      # can raise IOError
                line = f.readline()
                f.close()
                if line[:2] == '#!':
                    ext = '.sh'
                else:
                    return '', '', '', '', ''
        pre, post, start, end = suffixrules[ext]
    if not pre:
        pre = ''
    if not post:
        post = ''
    if not start:
        start = ''
    if not end:
        end = ''
    return ext, pre, post, start, end

PERL_COPYRIGHT = 'COPYRIGHT AND LICENCE\n\n'
COPYRIGHT_NOTICE = 'Copyright Notice\n================\n\n'

def addlicense(file, pre = None, post = None, start = None, end = None, verbose = False):
    try:
        f = open(file)
    except IOError:
        print >> sys.stderr, 'Cannot open file %s' % file
        return
    try:
        g = open(file + '.new', 'w')
    except IOError:
        print >> sys.stderr, 'Cannot create temp file %s.new' % file
        return
    data = f.read()
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
            if file.endswith('README'):
                g.write('  ')
            g.write(l)
            g.write('\n')
        g.write(data[pos:])
    else:
        try:
            ext, pre, post, start, end = getcomments(file, pre, post, start, end)
            if not ext:
                return
        except IOError:
            return
        f = open(file)
        line = f.readline()
        addblank = False
        if line[:2] == '#!':
            # if file starts with #! command interpreter, keep the line there
            g.write(line)
            # add a blank line
            addblank = True
            line = f.readline()
        if line.find('-*-') >= 0:
            # if file starts with an Emacs mode specification, keep
            # the line there
            g.write(line)
            # add a blank line
            addblank = True
            line = f.readline()
        if line.find('vim:') >= 0:
            # if file starts with a vim mode specification, keep
            # the line there
            g.write(line)
            # add a blank line
            addblank = True
            line = f.readline()
        if line[:5] == '<?xml':
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
        if line:
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
        print >> sys.stderr, 'Cannot make backup for %s' % file
        return
    try:
        os.rename(file + '.new', file)
        if verbose:
            print file
    except OSError:
        print >> sys.stderr, 'Cannot move file %s into position' % file

def normalize(s):
    # normalize white space: remove leading and trailing white space,
    # and replace multiple white space characters by a single space
    return ' '.join(s.split())

def dellicense(file, pre = None, post = None, start = None, end = None, verbose = False):
    try:
        f = open(file)
    except IOError:
        print >> sys.stderr, 'Cannot open file %s' % file
        return
    try:
        g = open(file + '.new', 'w')
    except IOError:
        print >> sys.stderr, 'Cannot create temp file %s.new' % file
        return
    data = f.read()
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
                print >> sys.stderr, 'line doesn\'t match in file %s' % file
                print >> sys.stderr, 'file:    "%s"' % data[pos:pos+len(l)]
                print >> sys.stderr, 'license: "%s"' % l
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
            ext, pre, post, start, end = getcomments(file, pre, post, start, end)
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
        if line[:2] == '#!':
            g.write(line)
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if line.find('-*-') >= 0:
            g.write(line)
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if line.find('vim:') >= 0:
            g.write(line)
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if line[:5] == '<?xml':
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
                print >> sys.stderr, 'PRE doesn\'t match in file %s' % file
                f.close()
                g.close()
                try:
                    os.unlink(file + '.new')
                except OSError:
                    pass
                return
        for l in license:
            if nline.find(normalize(l)) >= 0:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print >> sys.stderr, 'line doesn\'t match in file %s' % file
                print >> sys.stderr, 'file:    "%s"' % line
                print >> sys.stderr, 'license: "%s"' % l
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
                print >> sys.stderr, 'POST doesn\'t match in file %s' % file
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
        print >> sys.stderr, 'Cannot make backup for %s' % file
        return
    try:
        os.rename(file + '.new', file)
        if verbose:
            print file
    except OSError:
        print >> sys.stderr, 'Cannot move file %s into position' % file

def listfile(file, pre = None, post = None, start = None, end = None, verbose = False):
    try:
        f = open(file)
    except IOError:
        print >> sys.stderr, 'Cannot open file %s' % file
        return
    data = f.read()
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
                print >> sys.stderr, 'line doesn\'t match in file %s' % file
                print >> sys.stderr, 'file:    "%s"' % data[pos:pos+len(l)]
                print >> sys.stderr, 'license: "%s"' % l
                f.close()
                return
            pos += len(l) + 1
    else:
        try:
            ext, pre, post, start, end = getcomments(file, pre, post, start, end)
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
        if line[:2] == '#!':
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if line.find('-*-') >= 0:
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if line.find('vim:') >= 0:
            line = f.readline()
            if line and line == '\n':
                line = f.readline()
        if line[:5] == '<?xml':
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
                print >> sys.stderr, 'PRE doesn\'t match in file %s' % file
                f.close()
                return
        for l in license:
            if nline.find(normalize(l)) >= 0:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print >> sys.stderr, 'line doesn\'t match in file %s' % file
                print >> sys.stderr, 'file:    "%s"' % line
                print >> sys.stderr, 'license: "%s"' % l
                f.close()
                return
        if post:
            if nline == post:
                line = f.readline()
                nline = normalize(line)
            else:
                # doesn't match
                print >> sys.stderr, 'POST doesn\'t match in file %s' % file
                f.close()
                return
    f.close()
    print file

if __name__ == '__main__' or sys.argv[0] == __name__:
    main()
