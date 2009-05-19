# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
# Copyright August 2008-2009 MonetDB B.V.
# All Rights Reserved.

import logging
import time

#configure the logger, so we can see what is happening
logging.getLogger().setLevel(logging.DEBUG)

try:
    import monetdb.sql
except ImportError:
    # running examples from development tree
    import sys
    import os
    parent = os.path.join(sys.path[0], '..')
    sys.path.append(parent)
    import monetdb.sql


x = monetdb.sql.connect(database='python')
c = x.cursor()

# some basic query
#c.arraysize=1
c.execute('select * from tables')
results = c.fetchall()
x.commit()
print results
#c.arraysize=3
#results = c.fetchmany()


#for arraysize in (100,1000,10000, 100000):
#    t = time.time()
#    c.arraysize = arraysize
#    c.execute('select * from tables, tables, tables, tables')
#    results = c.fetchall()
#    print(repr((len(results), (arraysize, time.time() -t))))

