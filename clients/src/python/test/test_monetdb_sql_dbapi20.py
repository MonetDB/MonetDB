#!/usr/bin/env python

import unittest
#import subprocess

#import logging
#logging.getLogger().setLevel(logging.DEBUG)

try:
    import monetdb.sql
except ImportError:
    import sys, os
    parent = os.path.join(sys.path[0], '..')
    sys.path.append(parent)
    import monetdb.sql


import dbapi20
class Test_Monetdb_Sql(dbapi20.DatabaseAPI20Test):
    driver = monetdb.sql
    connect_args = ()
    connect_kw_args = {'database': 'demo'}

    lower_func = 'lower' # For stored procedure test

    def setUp(self):
        # Call superclass setUp In case this does something in the
        # future
        dbapi20.DatabaseAPI20Test.setUp(self)
        con = self._connect()
        con.close()


    def _connect(self):
        try:
            con = self.driver.connect(
                *self.connect_args,**self.connect_kw_args
                )
            return con
        except AttributeError:
            self.fail("No connect method found in self.driver module")


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


    def test_nextset(self): pass
    def test_setoutputsize(self): pass

    def test_Exceptions(self):
        # we override this since StandardError is depricated in python 3
        self.failUnless(issubclass(self.driver.Warning,Exception))
        self.failUnless(issubclass(self.driver.Error,Exception))
        self.failUnless(
            issubclass(self.driver.InterfaceError,self.driver.Error)
            )
        self.failUnless(
            issubclass(self.driver.DatabaseError,self.driver.Error)
            )
        self.failUnless(
            issubclass(self.driver.OperationalError,self.driver.Error)
            )
        self.failUnless(
            issubclass(self.driver.IntegrityError,self.driver.Error)
            )
        self.failUnless(
            issubclass(self.driver.InternalError,self.driver.Error)
            )
        self.failUnless(
            issubclass(self.driver.ProgrammingError,self.driver.Error)
            )
        self.failUnless(
            issubclass(self.driver.NotSupportedError,self.driver.Error)
            )


if __name__ == '__main__':
    unittest.main()
