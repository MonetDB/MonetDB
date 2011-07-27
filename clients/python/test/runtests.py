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

import unittest
import warnings
import sys
import os
import logging

import capabilities
import dbapi20

warnings.filterwarnings('error')

MAPIPORT = int(os.environ.get('MAPIPORT', 50000))
TSTDB = os.environ.get('TSTDB', 'demo')
TSTHOSTNAME = os.environ.get('TSTHOSTNAME', 'localhost')
TSTUSERNAME = os.environ.get('TSTUSERNAME', 'monetdb')
TSTPASSWORD = os.environ.get('TSTPASSWORD', 'monetdb')

if os.environ.get("TSTDEBUG", "no") == "yes":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('monetdb')

try:
    import monetdb.sql
except ImportError:
    logging.warning("monetdb python API not found, using local monetdb python API")
    import sys
    parent = os.path.join(sys.path[0], os.pardir)
    sys.path.append(parent)
    import monetdb.sql


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

    def test_bigresult(self):
        self.cursor.execute('select count(*) from tables')
        r = self.cursor.fetchone()
        n = r[0]
        self.cursor.arraysize=1000
        self.cursor.execute('select * from tables, tables')
        r = self.cursor.fetchall()
        self.assertEquals(len(r), n**2)





class Test_DBAPI20(dbapi20.DatabaseAPI20Test):
    driver = monetdb.sql
    connect_args = ()
    connect_kwargs = dict(database=TSTDB, port=MAPIPORT, hostname=TSTHOSTNAME,
            username=TSTUSERNAME, password=TSTPASSWORD, autocommit=False)

    lower_func = 'lower' # For stored procedure test

    def executeDDL1(self,cursor):
        """ We need a commit after a query, since otherwise transactions mix up """
        cursor.execute(self.ddl1)
        #cursor.connection.commit()

    def executeDDL2(self,cursor):
        """ We need a commit after a query, since otherwise transactions mix up """
        cursor.execute(self.ddl2)
        #cursor.connection.commit()

    def setUp(self):
        # Call superclass setUp In case this does something in the
        # future
        dbapi20.DatabaseAPI20Test.setUp(self)
        con = self._connect()
        con.close()


    def _connect(self):
        try:
            con = self.driver.connect( *self.connect_args, **self.connect_kwargs)
        except AttributeError:
            self.fail("No connect method found in self.driver module")
        finally:
            return con


    def tearDown(self):
        con = self._connect()
        try:
            cur = con.cursor()
            for ddl in (self.xddl1,self.xddl2):
                try:
                    cur.execute(ddl)
                except self.driver.Error:
                    pass
        finally:
            con.close()

    def test_nextset(self):
        pass

    def test_setoutputsize(self):
        pass

    def test_utf8(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            args = {'beer': '\xc4\xa5'}
            cur.execute( 'insert into %sbooze values (%%(beer)s)' % self.table_prefix, args )
            cur.execute('select name from %sbooze' % self.table_prefix)
            res = cur.fetchall()
            beer = res[0][0]
            self.assertEqual(beer,args['beer'],'incorrect data retrieved')
        finally:
            con.close()


    def test_unicode(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)

            # in python 3 everything is unicode
            import sys
            major = sys.version_info[0]
            if major == 3:
                args = {'beer': '\N{latin small letter a with acute}'}
                encoded = args['beer']
            else:
                args = {'beer': unicode('\N{latin small letter a with acute}', 'unicode-escape')}
                encoded = args['beer'].encode('utf-8')

            cur.execute( 'insert into %sbooze values (%%(beer)s)' % self.table_prefix, args )
            cur.execute('select name from %sbooze' % self.table_prefix)
            res = cur.fetchall()
            beer = res[0][0]
            self.assertEqual(beer,encoded,'incorrect data retrieved')
        finally:
            con.close()


    def test_substring(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            args = {'beer': '"" \"\'\",\\"\\"\"\'\"'}
            cur.execute( 'insert into %sbooze values (%%(beer)s)' % self.table_prefix, args )
            cur.execute('select name from %sbooze' % self.table_prefix)
            res = cur.fetchall()
            beer = res[0][0]
            self.assertEqual(beer,args['beer'],'incorrect data retrieved, got %s, should be %s' % (beer, args['beer']))
        finally:
            con.close()


    def test_newline(self):
        teststrings = [
            'abc\ndef',
            'abc\\ndef',
            'abc\\\ndef',
            'abc"def',
            'abc""def',
            'abc\'def',
            'abc\'\'def',
            "abc\"def",
            "abc\"\"def",
            "abc'def",
            "abc''def",
            ]

        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            for i in teststrings:
                args = {'beer': i}
                cur.execute( 'insert into %sbooze values (%%(beer)s)' % self.table_prefix, args )
                cur.execute('select * from %sbooze' % self.table_prefix)
                row = cur.fetchone()
                cur.execute('delete from %sbooze where name=%%s' % self.table_prefix, i)
                self.assertEqual(i, row[0], 'newline not properly converted, got %s, should be %s' % (row[0], i))
        finally:
            con.close()


    def test_Exceptions(self):
        # we override this since StandardError is depricated in python 3
        self.assertTrue(issubclass(self.driver.Warning,Exception))
        self.assertTrue(issubclass(self.driver.Error,Exception))
        self.assertTrue(issubclass(self.driver.InterfaceError, self.driver.Error))
        self.assertTrue(issubclass(self.driver.DatabaseError, self.driver.Error))
        self.assertTrue(issubclass(self.driver.OperationalError, self.driver.Error))
        self.assertTrue(issubclass(self.driver.IntegrityError, self.driver.Error))
        self.assertTrue(issubclass(self.driver.InternalError, self.driver.Error))
        self.assertTrue(issubclass(self.driver.ProgrammingError, self.driver.Error))
        self.assertTrue(issubclass(self.driver.NotSupportedError, self.driver.Error))



if __name__ == '__main__':

    if Test_Capabilities.leak_test:
        import gc
        gc.enable()
        gc.set_debug(gc.DEBUG_LEAK)

    suite1 = unittest.TestLoader().loadTestsFromTestCase(Test_Capabilities)
    # if you want to run a single test:
    #suite = unittest.TestLoader().loadTestsFromName('test_newline', Test_Monetdb_Sql)

    suite2 = unittest.TestLoader().loadTestsFromTestCase(Test_DBAPI20)
    #suite = unittest.TestLoader().loadTestsFromName('test_REAL', Test_Monetdb_Sql)

    TextTestRunnerNoTime(verbosity=3).run(suite1)
    TextTestRunnerNoTime(verbosity=3).run(suite2)


