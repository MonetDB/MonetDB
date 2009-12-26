#!/usr/bin/env python

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

import unittest
import warnings
import sys
import os

#import logging
#logging.basicConfig(level=logging.DEBUG)
#logger = logging.getLogger('monetdb')

import capabilities
from dbapi20_monetdb import TextTestRunnerNoTime

try:
    import monetdb.sql
except ImportError:
    parent = os.path.join(sys.path[0], '..')
    sys.path.append(parent)
    import monetdb.sql


warnings.filterwarnings('error')

class Test_Monetdb_Sql(capabilities.DatabaseTest):
    MAPIPORT = int(os.environ.get('MAPIPORT', 50000))
    TSTDB = os.environ.get('TSTDB', 'demo')
    db_module = monetdb.sql
    connect_args = ()
    connect_kwargs = dict(database=TSTDB, port=MAPIPORT, autocommit=False)
    leak_test = False


if __name__ == '__main__':
    if Test_Monetdb_Sql.leak_test:
        import gc
        gc.enable()
        gc.set_debug(gc.DEBUG_LEAK)
    #unittest.main()
    suite = unittest.TestLoader().loadTestsFromTestCase(Test_Monetdb_Sql)
    #suite = unittest.TestLoader().loadTestsFromName('test_REAL', Test_Monetdb_Sql)
    TextTestRunnerNoTime(verbosity=3).run(suite)

