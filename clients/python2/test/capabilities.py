#!/usr/bin/env python -O

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

""" Script to test database capabilities and the DB-API interface
    for functionality and memory leaks.

    Adapted from a script by M-A Lemburg and taken from the MySQL python driver.

"""
from time import time
import unittest
from monetdb.exceptions import ProgrammingError

class DatabaseTest(unittest.TestCase):

    db_module = None
    connect_args = ()
    connect_kwargs = dict()
    create_table_extra = ''
    rows = 10

    def setUp(self):
        db = self.db_module.connect(*self.connect_args, **self.connect_kwargs)
        self.connection = db
        self.cursor = db.cursor()
        self.BLOBText = ''.join([chr(i) for i in range(33,127)] * 100);
        self.BLOBBinary = self.db_module.Binary(''.join([chr(i) for i in range(256)] * 16))
        self.BLOBUText = unicode(''.join([unichr(i) for i in range(1,16384)]))

    def tearDown(self):
        self.connection.close()

    def table_exists(self, name):
        try:
            self.cursor.execute('select * from %s where 1=0' % name)
        except:
            self.connection.rollback()
            return False
        else:
            return True

    def quote_identifier(self, ident):
        return '"%s"' % ident

    def new_table_name(self):
        i = id(self.cursor)
        while True:
            name = self.quote_identifier('tb%08x' % i)
            if not self.table_exists(name):
                return name
            i = i + 1

    def create_table(self, columndefs):
        """ Create a table using a list of column definitions given in
            columndefs.

            generator must be a function taking arguments (row_number,
            col_number) returning a suitable data object for insertion
            into the table.

        """
        self.table = self.new_table_name()
        self.cursor.execute('CREATE TABLE %s (%s) %s' %
                            (self.table,
                             ',\n'.join(columndefs),
                             self.create_table_extra))

    def check_data_integrity(self, columndefs, generator):
        self.create_table(columndefs)
        insert_statement = ('INSERT INTO %s VALUES (%s)' %
                            (self.table,
                             ','.join(['%s'] * len(columndefs))))
        data = [ [ generator(i,j) for j in range(len(columndefs)) ]
                 for i in range(self.rows) ]
        self.cursor.executemany(insert_statement, data)
        self.connection.commit()
        # verify
        self.cursor.execute('select * from %s' % self.table)
        l = self.cursor.fetchall()
        self.assertEqual(len(l), self.rows)
        try:
            for i in range(self.rows):
                for j in range(len(columndefs)):
                    self.assertEqual(l[i][j], generator(i,j))
        finally:
            self.cursor.execute('drop table %s' % (self.table))

    def test_transactions(self):
        columndefs = ( 'col1 INT', 'col2 VARCHAR(255)')
        def generator(row, col):
            if col == 0: return row
            else: return ('%i' % (row%10))*255
        self.create_table(columndefs)
        insert_statement = ('INSERT INTO %s VALUES (%s)' %
                            (self.table,
                             ','.join(['%s'] * len(columndefs))))
        data = [ [ generator(i,j) for j in range(len(columndefs)) ]
                 for i in range(self.rows) ]
        self.cursor.executemany(insert_statement, data)
        # verify
        self.connection.commit()
        self.cursor.execute('select * from %s' % self.table)
        l = self.cursor.fetchall()
        self.assertEqual(len(l), self.rows)
        for i in range(self.rows):
            for j in range(len(columndefs)):
                self.assertEqual(l[i][j], generator(i,j))
        delete_statement = 'delete from %s where col1=%%s' % self.table
        self.cursor.execute(delete_statement, (0,))
        self.cursor.execute('select col1 from %s where col1=%s' % \
                            (self.table, 0))
        l = self.cursor.fetchall()
        self.assertFalse(l, "DELETE didn't work")
        self.connection.rollback()
        self.cursor.execute('select col1 from %s where col1=%s' % \
                            (self.table, 0))
        l = self.cursor.fetchall()
        self.assertTrue(len(l) == 1, "ROLLBACK didn't work")
        self.cursor.execute('drop table %s' % (self.table))

    def test_truncation(self):
        columndefs = ( 'col1 INT', 'col2 VARCHAR(255)')
        def generator(row, col):
            if col == 0:
                return row
            else:
                return ('%i' % (row%10))*(int(255-self.rows/2)+row)
        self.create_table(columndefs)
        insert_statement = ('INSERT INTO %s VALUES (%s)' %
                            (self.table,
                             ','.join(['%s'] * len(columndefs))))
        try:
            self.cursor.execute(insert_statement, (0, '0'*256))
        except Warning:
            pass
        except self.connection.Error:
            pass
        else:
            self.fail("Over-long column did not generate warnings/exception with single insert")

        self.connection.rollback()
        self.create_table(columndefs)

        try:
            for i in range(self.rows):
                data = []
                for j in range(len(columndefs)):
                    data.append(generator(i,j))
                self.cursor.execute(insert_statement,tuple(data))
        except Warning:
            pass
        except self.connection.Error:
            pass
        else:
            self.fail("Over-long columns did not generate warnings/exception with execute()")

        self.connection.rollback()
        self.create_table(columndefs)

        try:
            data = [ [ generator(i,j) for j in range(len(columndefs)) ]
                     for i in range(self.rows) ]
            self.cursor.executemany(insert_statement, data)
        except Warning:
            pass
        except self.connection.Error:
            pass
        else:
            self.fail("Over-long columns did not generate warnings/exception with executemany()")

        self.connection.rollback()

    def test_CHAR(self):
        # Character data
        def generator(row,col):
            return ('%i' % ((row+col) % 10)) * 255
        self.check_data_integrity(
            ('col1 char(255)','col2 char(255)'),
            generator)

    def test_INT(self):
        # Number data
        def generator(row,col):
            return row*row
        self.check_data_integrity(
            ('col1 INT',),
            generator)

    def test_DECIMAL(self):
        # DECIMAL
        def generator(row,col):
            from decimal import Decimal
            return Decimal("%d.%02d" % (row, col))
        self.check_data_integrity(
            ('col1 DECIMAL(5,2)',),
            generator)

    def test_REAL(self):
        def generator(row,col):
            return row*1000.0
        self.check_data_integrity(
            ('col1 REAL',),
            generator)

    def test_DOUBLE(self):
        def generator(row,col):
            return row/1e-99
        self.check_data_integrity(
            ('col1 DOUBLE',),
            generator)

    def test_DATE(self):
        ticks = time()
        def generator(row,col):
            return self.db_module.DateFromTicks(ticks+row*86400-col*1313)
        self.check_data_integrity(
                 ('col1 DATE',),
                 generator)

    def test_TIME(self):
        ticks = time()
        def generator(row,col):
            return self.db_module.TimeFromTicks(ticks+row*86400-col*1313)
        self.check_data_integrity(
                 ('col1 TIME',),
                 generator)

    def test_TIMETZ(self):
        ticks = time()
        def generator(row,col):
            return self.db_module.TimeFromTicks(ticks+row*86400-col*1313)
        self.check_data_integrity(
                 ('col1 TIMETZ',),
                 generator)

    def test_DATETIME(self):
        ticks = time()
        def generator(row,col):
            return self.db_module.TimestampFromTicks(ticks+row*86400-col*1313)
        self.check_data_integrity(
                 ('col1 TIMESTAMP',),
                 generator)

    def test_TIMESTAMP(self):
        ticks = time()
        def generator(row,col):
            return self.db_module.TimestampFromTicks(ticks+row*86400-col*1313)
        self.check_data_integrity(
                 ('col1 TIMESTAMP',),
                 generator)

    def test_TIMESTAMPTZ(self):
        ticks = time()
        def generator(row,col):
            return self.db_module.TimestampFromTicks(ticks+row*86400-col*1313)
        self.check_data_integrity(
                 ('col1 TIMESTAMPTZ',),
                  generator)

    def test_fractional_TIMESTAMP(self):
        ticks = time()
        def generator(row,col):
            return self.db_module.TimestampFromTicks(ticks+row*86400-col*1313+row*0.7*col/3.0)
        self.check_data_integrity(
                 ('col1 TIMESTAMP',),
                 generator)

    def test_TEXT(self):
        def generator(row,col):
            return self.BLOBText # 'BLOB Text ' * 1024
        self.check_data_integrity(
                 ('col2 TEXT',),
                 generator)

    def test_BLOB(self):
        def generator(row,col):
            if col == 0:
                return row
            else:
                return self.BLOBBinary # 'BLOB\000Binary ' * 1024
        self.check_data_integrity(
                 ('col1 INT','col2 BLOB'),
                 generator)

    def test_TINYINT(self):
        # Number data
        def generator(row,col):
            v = (row*row) % 256
            if v > 127:
                v = v-256
            return v
        self.check_data_integrity(
            ('col1 TINYINT',),
            generator)

    def test_small_CHAR(self):
        # Character data
        def generator(row,col):
            i = (row*col+62)%256
            if i == 62: return ''
            if i == 63: return None
            return chr(i)
        self.check_data_integrity(
            ('col1 char(1)','col2 char(1)'),
            generator)

    def test_BOOL(self):
        def generator(row,col):
            return bool(row%2)
        self.check_data_integrity(
            ('col1 BOOL',),
            generator)

    def test_URL(self):
        def generator(row,col):
            return "http://example.org/something"
        self.check_data_integrity(
                 ('col1 URL',),
                 generator)

    def test_INET(self):
        def generator(row,col):
            return "192.168.254.101"
        self.check_data_integrity(
                 ('col1 INET',),
                 generator)

    def test_description(self):
        self.table = self.new_table_name()
        shouldbe = [
            ('c', 'varchar', None, 1024, None, None, None),
            ('d', 'decimal', None, 9, 9, 4, None),
            ('n', 'varchar', None, 1, None, None, None),
        ]
        try:
            self.cursor.execute("create table %s (c VARCHAR(1024), d DECIMAL(9,4), n VARCHAR(1) NOT NULL)" % self.table);
            self.cursor.execute("insert into %s VALUES ('test', 12345.1234, 'x')" % self.table)
            self.cursor.execute('select * from %s' % self.table)
            self.assertEqual(self.cursor.description, shouldbe, "cursor.description is incorrect")
        finally:
            self.cursor.execute('drop table %s' % (self.table))

    def test_bigresult(self):
        self.cursor.execute('select count(*) from tables')
        r = self.cursor.fetchone()
        n = r[0]
        self.cursor.arraysize=100000
        self.cursor.execute('select * from tables, tables t')
        r = self.cursor.fetchall()
        self.assertEqual(len(r), n**2)

    def test_closecur(self):
        self.cursor.close()
        self.assertRaises(ProgrammingError, self.cursor.execute, "select * from tables")
        self.cursor = self.connection.cursor()

    def test_customtype(self):
        t = ["list", "test"]
        self.assertRaises(ProgrammingError, self.db_module.monetize.convert, t)
        self.db_module.monetize.mapping_dict[list] = str
        self.assertEqual(self.db_module.monetize.convert(t), "['list', 'test']")

    def test_multiple_queries(self):
        table1 = self.new_table_name()
        table2 = table1[:-1] + 'bla"'
        self.cursor.execute("create table %s (a int)" % table1)
        self.cursor.execute("create table %s (a int, b int)" % table2)
        self.cursor.execute("insert into %s VALUES (100)" % table1)
        self.cursor.execute("insert into %s VALUES (50, 50)" % table2)
        self.cursor.execute('select * from %s; select * from %s;' %
                            (table1, table2))
        result = self.cursor.fetchall()
        self.assertEqual(result, [(50, 50)])
