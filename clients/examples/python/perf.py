# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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

t = time.time()
x = monetdb.sql.connect(database="demo")
c = x.cursor()
c.arraysize=10000
c.execute('select * from tables, tables')
results = c.fetchall()
