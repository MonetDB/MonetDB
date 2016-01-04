#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import unittest
import warnings
import sys
import os
import logging

try:
    import monetdb.sql
except ImportError:
    logging.warning("monetdb python API not found, using local monetdb python API")
    import sys
    import os
    here = os.path.dirname(__file__)
    parent = os.path.join(here, os.pardir)
    sys.path.append(parent)
    import monetdb.sql

import capabilities
import dbapi20
import test_pythonize
import test_monetize

warnings.filterwarnings('error')

MAPIPORT = int(os.environ.get('MAPIPORT', 50000))
TSTDB = os.environ.get('TSTDB', 'demo')
TSTHOSTNAME = os.environ.get('TSTHOSTNAME', 'localhost')
TSTUSERNAME = os.environ.get('TSTUSERNAME', 'monetdb')
TSTPASSWORD = os.environ.get('TSTPASSWORD', 'monetdb')
#TSTHOSTNAME = os.environ.get('MAPIHOST')  # set to this if testing a socket


if os.environ.get("TSTDEBUG", "no") == "yes":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('monetdb')


class TextTestRunnerNoTime(unittest.TextTestRunner):
    """A test runner class that displays results in textual form, but without time """
    def run(self, test):
        "Run the given test case or test suite."
        result = self._makeResult()
        test(result)
        result.printErrors()
        self.stream.writeln(result.separator2)
        run = result.testsRun
        self.stream.writeln("Ran %d test%s" % (run, run != 1 and "s" or ""))
        self.stream.writeln()
        if not result.wasSuccessful():
            self.stream.write("FAILED (")
            failed, errored = map(len, (result.failures, result.errors))
            if failed:
                self.stream.write("failures=%d" % failed)
            if errored:
                if failed: self.stream.write(", ")
                self.stream.write("errors=%d" % errored)
            self.stream.writeln(")")
        else:
            self.stream.writeln("OK")
        return result


class Test_Capabilities(capabilities.DatabaseTest):
    db_module = monetdb.sql
    connect_args = ()
    connect_kwargs = dict(database=TSTDB, port=MAPIPORT, hostname=TSTHOSTNAME,
            username=TSTUSERNAME, password=TSTPASSWORD, autocommit=False)
    leak_test = False


class Test_DBAPI20(dbapi20.DatabaseAPI20Test):
    driver = monetdb.sql
    connect_args = ()
    connect_kwargs = dict(database=TSTDB, port=MAPIPORT, hostname=TSTHOSTNAME,
            username=TSTUSERNAME, password=TSTPASSWORD, autocommit=False)

if __name__ == '__main__':
    suites = [
        Test_Capabilities,
        Test_DBAPI20,
        test_pythonize.TestPythonize,
        test_monetize.TestMonetize,
    ]

    for suite in suites:
        tests = unittest.TestLoader().loadTestsFromTestCase(suite)
        TextTestRunnerNoTime(verbosity=3).run(tests)
