# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import logging

#configure the logger, so we can see what is happening
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('monetdb')

try:
    import monetdb.sql
except ImportError:
    # running examples from development tree
    import sys
    import os
    parent = os.path.join(sys.path[0], os.pardir)
    sys.path.append(parent)
    import monetdb.sql


x = monetdb.sql.connect(username="monetdb", password="monetdb", hostname="localhost", database="demo")
c = x.cursor()

# some basic query
c.arraysize=100
c.execute('select * from tables')
results = c.fetchall()
x.commit()
print(results)
