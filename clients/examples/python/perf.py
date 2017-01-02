# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

import time

#configure the logger, so we can see what is happening
#import logging
#logging.basicConfig(level=logging.DEBUG)
#logger = logging.getLogger('monetdb')


import pymonetdb

t = time.time()
x = pymonetdb.connect(database="demo")
c = x.cursor()
c.arraysize=10000
c.execute('select * from tables, tables')
results = c.fetchall()
