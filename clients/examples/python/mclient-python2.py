#!/usr/bin/env python2

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import sys
import getopt

from monetdb import mapi

def main() :
    hostname = 'localhost'
    port = '50000'
    username = 'monetdb'
    password = 'monetdb'
    language = 'sql'
    database = ''
    encoding = None

    opts, args = getopt.getopt(sys.argv[1:], '',
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

if __name__ == "__main__":
    main()
