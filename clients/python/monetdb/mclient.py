#!/usr/bin/env python

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

#

import sys
import getopt

# a ugly hack to support python 2 and 3 at the same time
(major, minor, micro, level, serial)  = sys.version_info
if (major == 3):
    from monetdb import mapi3 as mapi
else:
    from monetdb import mapi


def main(argv) :
    hostname = 'localhost'
    port = '50000'
    username = 'monetdb'
    password = 'monetdb'
    language = 'sql'
    database = ''
    encoding = None

    opts, args = getopt.getopt(argv[1:], '',
                               ['host=', 'port=', 'user=', 'passwd=',
                                'language=', 'database=', 'encoding='])
    for o, a in opts:
        if o == '--host':
            hostname = a
        elif o == '--port':
            port = a
        elif o == '--user':
            username = a
        elif o == '--passwd':
            password = a
        elif o == '--language':
            language = a
        elif o == '--database':
            database = a
        elif o == '--encoding':
            encoding = a

    if encoding is None:
        import locale
        encoding = locale.getlocale()[1]
        if encoding is None:
            encoding = locale.getdefaultlocale()[1]

    s = mapi.Server()

    s.connect(hostname = hostname,
              port = int(port),
              username = username,
              password = password,
              language = language,
              database = database)
    print "#mclient (python) connected to %s:%d as %s" % (hostname, int(port), username)

    #fi = fileinput.FileInput()
    fi = sys.stdin

    prompt = '%s>' % language

    sys.stdout.write(prompt.encode('utf-8'))
    line = fi.readline()
    if encoding != 'utf-8':
        prompt = unicode(prompt, 'utf-8').encode(encoding, 'replace')
    while line and line != "\q\n":
        if encoding != 'utf-8':
            line = unicode(line, encoding).encode('utf-8')
        res = s.cmd('s' + line)
        if encoding != 'utf-8':
            res = unicode(res, 'utf-8').encode(encoding, 'replace')
        print res
        sys.stdout.write(prompt)
        line = fi.readline()

    s.disconnect()

### main(argv) #


if __name__ == "__main__":
    main(sys.argv)
