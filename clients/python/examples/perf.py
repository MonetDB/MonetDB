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


import time

#configure the logger, so we can see what is happening
#import logging
#logging.basicConfig(level=logging.DEBUG)
#logger = logging.getLogger('monetdb')


try:
    import monetdb.sql
except ImportError:
    # running examples from development tree
    import sys
    import os
    parent = os.path.join(sys.path[0], os.pardir)
    sys.path.append(parent)
    import monetdb.sql

for i in (10, 100, 1000, 10000):
    t = time.time()
    x = monetdb.sql.connect(database="demo")
    c = x.cursor()
    c.arraysize=i
    c.execute('select * from tables, tables, tables')
    results = c.fetchall()
    print i, time.time() - t
