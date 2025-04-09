#!/usr/bin/env python3

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

# skipif <system>
# onlyif <system>

# The skipif/onlyif mechanism has been slightly extended.  Recognized
# "system"s are:
# MonetDB, arch=<architecture>, system=<system>, bits=<bits>,
# threads=<threads>, has-hugeint, knownfail
# where <architecture> is generally what the Python call
# platform.machine() returns (i.e. x86_64, i686, aarch64, ppc64,
# ppc64le, note 'AMD64' is translated to 'x86_64' and 'arm64' to
# 'aarch64'); <system> is whatever platform.system() returns
# (i.e. Linux, Darwin, Windows); <bits> is either 32bit or 64bit;
# <threads> is the number of threads.

# statement (ok|ok rowcount|error) [arg]
# query (I|T|R)+ (nosort|rowsort|valuesort|python)? [arg]
#       I: integer; T: text (string); R: real (decimal)
#       nosort: do not sort
#       rowsort: sort rows
#       valuesort: sort individual values
#       python some.python.function: run data through function (MonetDB extension)
# hash-threshold number
# halt

# The python function that can be used instead of the various sort
# options should be a simple function that gets a list of lists as
# input and should produce a list of lists as output.  If the name
# contains a period, the last part is the name of the function and
# everything up to the last period is the module.  If the module
# starts with a period, it is searched in the MonetDBtesting module
# (where this file is also).

import pymonetdb
from MonetDBtesting.mapicursor import MapiCursor
import MonetDBtesting.malmapi as malmapi
import hashlib
import re
import sys
import platform
import importlib
import time
import MonetDBtesting.utils as utils
from pathlib import Path
from typing import Optional
import difflib

# this stuff is for geos pre 3.12: 3.12 introduced an extra set of
# parentheses in MULTIPOINT values
geosre = re.compile(r'MULTIPOINT *\((?P<points>[^()]*)\)')
ptsre = re.compile(r'-?\d+(?:\.\d+)? -?\d+(?:\.\d+)?')
geoszre = re.compile(r'MULTIPOINT *Z *\((?P<points>[^()]*)\)')
ptszre = re.compile(r'-?\d+(?:\.\d+)? -?\d+(?:\.\d+)? -?\d+(?:\.\d+)?')
# geos 3.13 introduced parentheses around EMPTY in MULTIPOLYGON (but not
# in all cases)
geosere = re.compile(r'MULTIPOLYGON \(EMPTY\)')

architecture = platform.machine()
if architecture == 'AMD64':     # Windows :-(
    architecture = 'x86_64'
if architecture == 'arm64':     # MacOS :-(
    architecture = 'aarch64'
bits = platform.architecture()[0]
if bits == '32bit' and architecture == 'x86_64':
    architecture = 'i686'
elif architecture == 'x86':     # Windows
    architecture = 'i686'
system = platform.system()
hashge = False                  # may get updated at start of testing

skipidx = re.compile(r'create index .* \b(asc|desc)\b', re.I)

class UnsafeDirectoryHandler(pymonetdb.SafeDirectoryHandler):
    def secure_resolve(self, filename: str) -> Optional[Path]:
        return (self.dir / filename).resolve()

class SQLLogicSyntaxError(Exception):
    pass

class SQLLogicConnection(object):
    def __init__(self, conn_id, dbh, crs=None, language='sql'):
        self.conn_id = conn_id
        self.dbh = dbh
        self.crs = crs
        self.language = language
        self.lastprepareid = None

    def cursor(self):
        if self.crs:
            return self.crs
        if self.language == 'sql':
            return self.dbh.cursor()
        return MapiCursor(self.dbh)


def is_copyfrom_stmt(stmt:[str]=[]):
    return '<COPY_INTO_DATA>' in stmt

def prepare_copyfrom_stmt(stmt:[str]=[]):
    index = stmt.index('<COPY_INTO_DATA>')
    head = stmt[:index]
    if stmt[index-1].endswith(';'):
        stmt[index-1] = stmt[index-1][:-1]
    # check for escape character (single period)
    tail = []
    for l in stmt[index+1:]:
        if l.strip() == '.':
            tail.append('')
        else:
            tail.append(l)
    head = '\n'.join(head)
    if not head.endswith(';'):
        head += ';'
    tail='\n'.join(tail)
    return head + '\n' + tail, head, stmt

def dq(s):
    return s.replace('"', '""')

class SQLLogic:
    def __init__(self, srcdir='.', report=None, out=sys.stdout):
        self.dbh = None
        self.crs = None
        self.out = out
        self.res = None
        self.rpt = report
        self.language = 'sql'
        self.conn_map = dict()
        self.database = None
        self.hostname = None
        self.port = None
        self.approve = None
        self.threshold = 100
        self.seenerr = False    # there was an error before timeout
        self.timedout = False   # there was a timeout
        self.__last = ''
        self.srcdir = srcdir
        self.lastprepareid = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _remainingtime(self):
        if self.timeout > 0:
            t = time.time()
            if self.starttime + self.timeout > t:
                return int(self.starttime + self.timeout - t)
            return 0
        return -1

    def connect(self, username='monetdb', password='monetdb',
                hostname='localhost', port=None, database='demo',
                language='sql', timeout: Optional[int]=0, alltests=False):
        self.starttime = time.time()
        self.language = language
        self.hostname = hostname
        self.port = port
        self.database = database
        self.timeout = timeout
        self.alltests = alltests
        if language == 'sql':
            transfer_handler = UnsafeDirectoryHandler(self.srcdir)
            dbh = pymonetdb.connect(username=username,
                                    password=password,
                                    hostname=hostname,
                                    port=port,
                                    database=database,
                                    autocommit=True,
                                    connect_timeout=timeout if timeout > 0 else -1)
            self.dbh = dbh
            dbh.set_uploader(transfer_handler)
            dbh.set_downloader(transfer_handler)
            self.crs = dbh.cursor()
        else:
            dbh = malmapi.Connection()
            dbh.connect(database=database,
                        username=username,
                        password=password,
                        language=language,
                        hostname=hostname,
                        port=port,
                        connect_timeout=timeout if timeout > 0 else -1)
            self.crs = MapiCursor(dbh)
        if timeout > 0:
            dbh.settimeout(timeout)

    def add_connection(self, conn_id, username='monetdb', password='monetdb'):
        if self.conn_map.get(conn_id, None) is None:
            hostname = self.hostname
            port = self.port
            database = self.database
            language = self.language
            t = self._remainingtime()
            if t == 0:
                raise TimeoutError('timed out')
            if language == 'sql':
                dbh  = pymonetdb.connect(username=username,
                                         password=password,
                                         hostname=hostname,
                                         port=port,
                                         database=database,
                                         autocommit=True,
                                         connect_timeout=t)
                crs = dbh.cursor()
                if t > 0:
                    dbh.settimeout(t)
                    crs.execute(f'call sys.setsessiontimeout({t})')
            else:
                dbh = malmapi.Connection()
                dbh.connect(database=database,
                            username=username,
                            password=password,
                            language=language,
                            hostname=hostname,
                            port=port,
                            connect_timeout=t)
                crs = MapiCursor(dbh)
                if t > 0:
                    dbh.settimeout(t)
                    crs.execute(f'clients.setsessiontimeout({t}:int)')
            conn = SQLLogicConnection(conn_id, dbh=dbh, crs=crs, language=language)
            self.conn_map[conn_id] = conn
            return conn

    def get_connection(self, conn_id):
        return self.conn_map.get(conn_id)

    def close(self):
        for k in self.conn_map:
            conn = self.conn_map[k]
            conn.dbh.close()
        self.conn_map.clear()
        if self.crs:
            try:
                self.crs.close()
            except BrokenPipeError:
                pass
            self.crs = None
        if self.dbh:
            self.dbh.close()
            self.dbh = None


    def drop(self):
        if self.language != 'sql':
            return
        self.crs.execute('select s.name, t.name, case when t.type in (select table_type_id from sys.table_types where table_type_name like \'%VIEW%\') then \'VIEW\' else \'TABLE\' end from sys.tables t, sys.schemas s where not t.system and t.schema_id = s.id')
        for row in self.crs.fetchall():
            try:
                self.crs.execute(f'drop {row[2]} "{dq(row[0])}"."{dq(row[1])}" cascade')
            except pymonetdb.Error:
                # perhaps already dropped because of the cascade
                pass
        self.crs.execute('select s.name, f.name, ft.function_type_keyword from functions f, schemas s, function_types ft where not f.system and f.schema_id = s.id and f.type = ft.function_type_id')
        for row in self.crs.fetchall():
            try:
                self.crs.execute(f'drop all {row[2]} "{dq(row[0])}"."{dq(row[1])}"')
            except pymonetdb.Error:
                # perhaps already dropped
                pass
        self.crs.execute('select s.name, q.name from sys.sequences q, schemas s where q.schema_id = s.id')
        for row in self.crs.fetchall():
            try:
                self.crs.execute(f'drop sequence "{dq(row[0])}"."{dq(row[1])}"')
            except pymonetdb.Error:
                # perhaps already dropped
                pass
        self.crs.execute("select name from sys.users where name not in ('monetdb', '.snapshot')")
        for row in self.crs.fetchall():
            try:
                self.crs.execute(f'alter user "{dq(row[0])}" SET SCHEMA "sys"')
            except pymonetdb.Error:
                pass
        self.crs.execute('select name from sys.schemas where not system')
        for row in self.crs.fetchall():
            try:
                self.crs.execute(f'drop schema "{dq(row[0])}" cascade')
            except pymonetdb.Error:
                pass
        self.crs.execute("select name from sys.users where name not in ('monetdb', '.snapshot')")
        for row in self.crs.fetchall():
            try:
                self.crs.execute(f'drop user "{dq(row[0])}"')
            except pymonetdb.Error:
                pass
        # drop custom types created in test
        self.crs.execute("select sqlname from sys.types where systemname is null order by id")
        for row in self.crs.fetchall():
            try:
                self.crs.execute('drop type "{}"'.format(row[0]))
            except pymonetdb.Error:
                pass

    def exec_statement(self, statement, expectok,
                       err_stmt=None,
                       expected_err_code=None,
                       expected_err_msg=None,
                       expected_rowcount=None,
                       conn=None,
                       verbose=False):
        crs = conn.cursor() if conn else self.crs
        crs.description = None
        if skipidx.search(statement) is not None:
            # skip creation of ascending or descending index
            return ['statement', 'ok']
        if '<LAST_PREPARE_ID>' in statement:
            id = conn.lastprepareid if conn else self.lastprepareid
            if id is not None:
                statement = statement.replace('<LAST_PREPARE_ID>', f'{id}')
        try:
            if verbose:
                print(f'Executing:\n{err_stmt or statement}')
            affected_rowcount = crs.execute(statement)
        except (pymonetdb.Error, ValueError) as e:
            msg = e.args[0]
            if not expectok:
                result = ['statement', 'error']
                if expected_err_code or expected_err_msg:
                    # check whether failed as expected
                    err_code_received, err_msg_received = utils.parse_mapi_err_msg(msg)
                    if expected_err_code and expected_err_msg and err_code_received and err_msg_received:
                        if expected_err_msg.startswith('/') and expected_err_msg.endswith('/'):
                            res = re.search(expected_err_msg[1:-1], err_msg_received)
                            if expected_err_code == err_code_received and res is not None:
                                result.append(err_code_received + '!' + expected_err_msg)
                                return result
                            result.append(err_code_received + '!' + err_msg_received)
                        else:
                            if expected_err_msg.endswith('...') and expected_err_code == err_code_received and err_msg_received.lower().startswith(expected_err_msg[:expected_err_msg.find('...')].lower()):
                                result.append(err_code_received + '!' + expected_err_msg)
                                return result
                            result.append(err_code_received + '!' + err_msg_received)
                            if expected_err_code == err_code_received and expected_err_msg.lower() == err_msg_received.lower():
                                return result
                    else:
                        if expected_err_code and err_code_received:
                            result.append(err_code_received + '!')
                            if expected_err_code == err_code_received:
                                return result
                        elif expected_err_msg and err_msg_received:
                            if expected_err_msg.startswith('/') and expected_err_msg.endswith('/'):
                                res = re.search(expected_err_msg[1:-1], err_msg_received)
                                if res is not None:
                                    result.append(expected_err_msg)
                                    return result
                                result.append(err_msg_received)
                            else:
                                if expected_err_msg.endswith('...') and err_msg_received.lower().startswith(expected_err_msg[:expected_err_msg.find('...')].lower()):
                                    result.append(expected_err_msg)
                                    return result
                                result.append(err_msg_received)
                                if expected_err_msg.lower() == err_msg_received.lower():
                                    return result
                    msg = "statement was expected to fail with" \
                            + (f" error code {expected_err_code}," if expected_err_code else '') \
                            + (f" error message {repr(expected_err_msg)}," if expected_err_msg else '') \
                            + f" received code {err_code_received}, message {repr(err_msg_received)}"
                    self.query_error(err_stmt or statement, str(msg), str(e))
                return result
        except TimeoutError as e:
            self.query_error(err_stmt or statement, 'Timeout', str(e))
            return ['statement', 'crash'] # should never be approved
        except ConnectionError as e:
            self.query_error(err_stmt or statement, 'Timeout or server may have crashed', str(e))
            return ['statement', 'crash'] # should never be approved
        except KeyboardInterrupt:
            raise
        except:
            type, value, traceback = sys.exc_info()
            self.query_error(statement, 'unexpected error from pymonetdb', str(value))
            return ['statement', 'error']
        else:
            result = ['statement', 'ok']
            if crs.description is not None and crs.lastrowid is not None:
                # it was a PREPARE query
                if conn:
                    conn.lastprepareid = crs.lastrowid
                else:
                    self.lastprepareid = crs.lastrowid
            if expectok:
                if expected_rowcount is not None:
                    result.append('rowcount')
                    result.append(f'{affected_rowcount}')
                    if expected_rowcount != affected_rowcount:
                        self.query_error(err_stmt or statement, f"statement was expecting to succeed with {expected_rowcount} rows but received {affected_rowcount} rows!")
                return result
            msg = None
        self.query_error(err_stmt or statement, expectok and "statement was expected to succeed but didn't" or "statement was expected to fail but didn't", msg)
        return ['statement', 'error']

    def convertresult(self, query, columns, data):
        ndata = []
        for row in data:
            if len(row) != len(columns):
                self.query_error(query, 'wrong number of columns received')
                return None
            nrow = []
            for i in range(len(columns)):
                try:
                    if row[i] is None:
                        nrow.append('NULL')
                    elif self.language == 'sql' and row[i] == 'NULL':
                        nrow.append('NULL')
                    elif self.language == 'mal' and row[i] == 'nil':
                        nrow.append('NULL')
                    elif columns[i] == 'I':
                        if row[i] in ('true', 'True'):
                            nrow.append('1')
                        elif row[i] in ('false', 'False'):
                            nrow.append('0')
                        else:
                            nrow.append('%d' % row[i])
                    elif columns[i] == 'T':
                        if row[i] == '' or row[i] == b'':
                            nrow.append('(empty)')
                        else:
                            nval = []
                            if isinstance(row[i], bytes):
                                for c in row[i]:
                                    c = '%02X' % c
                                    nval.append(c)
                            else:
                                for c in str(row[i]):
# sqllogictest original:
                                    # if ' ' <= c <= '~':
                                    #     nval.append(c)
                                    # else:
                                    #     nval.append('@')
# our variant, don't map printables:
                                    if c < ' ' or '\u007f' <= c <= '\u009f':
                                        # control characters
                                        nval.append('@')
                                    else:
                                        nval.append(c)
                            nrow.append(''.join(nval))
                    elif columns[i] == 'R':
                        nrow.append('%.3f' % row[i])
                    else:
                        self.raise_error('incorrect column type indicator')
                except TypeError:
                    self.query_error(query, f'bad column type {columns[i]} at {i}')
                    return None
            ndata.append(tuple(nrow))
        return ndata

    def raise_error(self, message):
        print(f'Syntax error in test file, line {self.qline}:', file=self.out)
        print(message, file=self.out)
        raise SQLLogicSyntaxError(message)

    def query_error(self, query, message, exception=None, data=None):
        if message == 'Timeout':
            self.timedout = True
        elif not self.timedout:
            self.seenerr = True
        if self.rpt:
            print(self.rpt, file=self.out)
        print(message, file=self.out)
        if exception:
            print(exception.rstrip('\n'), file=self.out)
        print("query started on line %d of file %s" % (self.qline, self.name),
              file=self.out)
        print("query text:", file=self.out)
        print(query, file=self.out)

    def exec_query(self, query, columns, sorting, pyscript, hashlabel, nresult, hash, expected, conn=None, verbose=False) -> bool:
        err = False
        crs = conn.cursor() if conn else self.crs
        if '<LAST_PREPARE_ID>' in query:
            id = conn.lastprepareid if conn else self.lastprepareid
            if id is not None:
                query = query.replace('<LAST_PREPARE_ID>', f'{id}')
        crs.description = None
        try:
            if verbose:
                print(f'Executing:\n{query}')
            crs.execute(query)
        except (pymonetdb.Error, ValueError) as e:
            self.query_error(query, 'query failed', e.args[0])
            return ['statement', 'error'], []
        except KeyboardInterrupt:
            raise
        except TimeoutError as e:
            self.query_error(query, 'Timeout', str(e))
            return ['statement', 'crash'] # should never be approved
        except ConnectionError as e:
            self.query_error(query, 'Timeout or server may have crashed', str(e))
            return ['statement', 'crash'] # should never be approved
        except:
            tpe, value, traceback = sys.exc_info()
            self.query_error(query, 'unexpected error from pymonetdb', str(value))
            return ['statement', 'error'], []
        if crs.description is None:
            # it's not a query, it's a statement
            self.query_error(query, 'query without results')
            return ['statement', 'ok'], []
        try:
            data = crs.fetchall()
        except KeyboardInterrupt:
            raise
        except:
            tpe, value, traceback = sys.exc_info()
            self.query_error(query, 'unexpected error from pymonetdb', str(value))
            return ['statement', 'error'], []
        if crs.lastrowid is not None:
            # it was a PREPARE query
            if conn:
                conn.lastprepareid = crs.lastrowid
            else:
                self.lastprepareid = crs.lastrowid
        ndata = []
        for row in data:
            nrow = []
            for col in row:
                if isinstance(col, str):
                    res = geosre.search(col)
                    if res is not None:
                        points = ptsre.sub(r'(\g<0>)', res.group('points'))
                        col = col[:res.start('points')] + points + col[res.end('points'):]
                    res = geoszre.search(col)
                    if res is not None:
                        points = ptszre.sub(r'(\g<0>)', res.group('points'))
                        col = col[:res.start('points')] + points + col[res.end('points'):]
                    res = geosere.search(col)
                    if res is not None:
                        col = col[:res.start(0)] + 'MULTIPOLYGON EMPTY' + col[res.end(0):]
                nrow.append(col)
            ndata.append(nrow)
        data = ndata
        if crs.description:
            rescols = []
            for desc in crs.description:
                if desc.type_code in ('boolean', 'tinyint', 'smallint', 'int', 'bigint', 'hugeint', 'bit', 'sht', 'lng', 'hge', 'oid', 'void'):
                    rescols.append('I')
                elif desc.type_code in ('decimal', 'double', 'real', 'flt', 'dbl'):
                    rescols.append('R')
                else:
                    rescols.append('T')
            rescols = ''.join(rescols)
            if len(crs.description) != len(columns):
                self.query_error(query, f'received {len(crs.description)} columns, expected {len(columns)} columns', data=data)
                columns = rescols
                err = True
        else:
            # how can this be?
            #self.query_error(query, 'no crs.description')
            rescols = 'T'
        if sorting != 'python' and crs.rowcount * len(columns) != nresult:
            if not err:
                self.query_error(query, f'received {crs.rowcount} rows, expected {nresult // len(columns)} rows', data=data)
                err = True
        if self.res is not None:
            for row in data:
                sep=''
                for col in row:
                    if col is None:
                        print(sep, 'NULL', sep='', end='', file=self.res)
                    else:
                        print(sep, col, sep='', end='', file=self.res)
                    sep = '|'
                print('', file=self.res)
        if columns != rescols and self.approve:
            resdata = self.convertresult(query, rescols, data)
        else:
            resdata = None
        data = self.convertresult(query, columns, data)
        if data is None:
            return ['statement', 'error'], []
        m = hashlib.md5()
        resm = hashlib.md5()
        i = 0
        result = []
        if sorting == 'valuesort':
            ndata = []
            for row in data:
                for col in row:
                    ndata.append(col)
            ndata.sort()
            for col in ndata:
                if expected is not None:
                    if i < len(expected) and col != expected[i]:
                        self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]))
                        err = True
                    i += 1
                m.update(bytes(col, encoding='utf-8'))
                m.update(b'\n')
                result.append(col)
            if err and expected is not None:
                print('Differences:', file=self.out)
                self.out.writelines(list(difflib.ndiff([x + '\n' for x in expected], [x + '\n' for x in ndata])))
            if resdata is not None:
                result = []
                ndata = []
                for row in resdata:
                    for col in row:
                        ndata.append(col)
                ndata.sort()
                for col in ndata:
                    resm.update(bytes(col, encoding='utf-8'))
                    resm.update(b'\n')
                    result.append(col)
        elif sorting == 'python':
            if '.' in pyscript:
                [mod, fnc] = pyscript.rsplit('.', 1)
                try:
                    if mod.startswith('.'):
                        pymod = importlib.import_module(mod, 'MonetDBtesting')
                    else:
                        pymod = importlib.import_module(mod)
                except ModuleNotFoundError:
                    self.query_error(query, 'cannot import filter function module')
                    err = True
                else:
                    try:
                        pyfnc = getattr(pymod, fnc)
                    except AttributeError:
                        self.query_error(query, 'cannot find filter function')
                        err = True
            elif re.match(r'[_a-zA-Z][_a-zA-Z0-9]*$', pyscript) is None:
                self.query_error(query, 'filter function is not an identifier')
                err = True
            else:
                try:
                    pyfnc = eval(pyscript)
                except NameError:
                    self.query_error(query, 'cannot find filter function')
                    err = True
            ndata = data
            if not err:
                try:
                    ndata = pyfnc(data)
                except:
                    self.query_error(query, 'filter function failed')
                    err = True
                if resdata is not None:
                    try:
                        resdata = pyfnc(resdata)
                    except:
                        resdata = None
            ncols = 1
            if (len(ndata)):
                ncols = len(ndata[0])
            if len(ndata)*ncols != nresult:
                self.query_error(query, f'received {len(ndata)*ncols} rows, expected {nresult} rows', data=data)
                err = True
            for row in ndata:
                for col in row:
                    if expected is not None:
                        if i < len(expected) and col != expected[i]:
                            self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]))
                            err = True
                        i += 1
                    m.update(bytes(col, encoding='utf-8'))
                    m.update(b'\n')
                    result.append(col)
            if err and expected is not None:
                recv = []
                for row in ndata:
                    for col in row:
                        recv.append(col)
                print('Differences:', file=self.out)
                self.out.writelines(list(difflib.ndiff([x + '\n' for x in expected], [x + '\n' for x in recv])))
            if resdata is not None:
                result = []
                for row in resdata:
                    for col in row:
                        resm.update(bytes(col, encoding='utf-8'))
                        resm.update(b'\n')
                        result.append(col)
        else:
            ndata = data
            if sorting == 'rowsort':
                ndata = sorted(data)
            err_msg_buff = []
            for row in ndata:
                for col in row:
                    if expected is not None:
                        if i < len(expected) and col != expected[i]:
                            err_msg_buff.append('unexpected value;\nreceived "%s"\nexpected "%s"' % (col, expected[i]))
                            #self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]), data=data)
                            err = True
                        i += 1
                    m.update(bytes(col, encoding='utf-8'))
                    m.update(b'\n')
                    result.append(col)
            if err and expected is not None:
                self.query_error(query, '\n'.join(err_msg_buff))
                recv = []
                for row in ndata:
                    for col in row:
                        recv.append(col + '\n')
                print('Differences:', file=self.out)
                self.out.writelines(list(difflib.ndiff([x + '\n' for x in expected], recv)))
            if resdata is not None:
                if sorting == 'rowsort':
                    resdata.sort()
                result = []
                for row in resdata:
                    for col in row:
                        resm.update(bytes(col, encoding='utf-8'))
                        resm.update(b'\n')
                        result.append(col)
        if err:
            if data is not None:
                if len(data) < 100:
                    print('Query result:', file=self.out)
                else:
                    print('Truncated query result:', file=self.out)
                for row in data[:100]:
                    sep=''
                    for col in row:
                        if col is None:
                            print(sep, 'NULL', sep='', end='', file=self.out)
                        else:
                            print(sep, col, sep='', end='', file=self.out)
                        sep = '|'
                    print(file=self.out)
            print(file=self.out)
        h = m.hexdigest()
        if resdata is not None:
            resh = resm.hexdigest()
        if not err:
            if hashlabel is not None and hashlabel in self.hashes and self.hashes[hashlabel][0] != h:
                self.query_error(query, 'query hash differs from previous query at line %d' % self.hashes[hashlabel][1], data=data)
                err = True
            elif hash is not None and h != hash:
                self.query_error(query, 'hash mismatch; received: "%s", expected: "%s"' % (h, hash), data=data)
                err = True
        if hashlabel is not None and hashlabel not in self.hashes:
            if hash is not None:
                self.hashes[hashlabel] = (hash, self.qline)
            elif not err:
                self.hashes[hashlabel] = (h, self.qline)
        result1 = ['query', rescols, sorting]
        if sorting == 'python':
            result1.append(pyscript)
        if hashlabel:
            result1.append(hashlabel)
        if len(result) > self.threshold:
            result2 = [f'{len(result)} values hashing to {h if resdata is None else resh}']
        else:
            result2 = result
        return result1, result2

    def initfile(self, f, defines, run_until=None):
        self.name = f
        self.file = open(f, 'r', encoding='utf-8', errors='replace')
        self.line = 0
        self.run_until = run_until
        self.hashes = {}
        defs = []
        if defines:
            for define in defines:
                key, val = define.split('=', 1)
                key = key.strip()
                val = val.strip()
                defs.append((re.compile(r'\$(' + key + r'\b|{' + key + '})'),
                             val, key))
                defs.append((re.compile(r'\$(Q' + key + r'\b|{Q' + key + '})'),
                             val.replace('\\', '\\\\'), 'Q'+key))
        self.defines = sorted(defs, key=lambda x: (-len(x[1]), x[1], x[2]))
        self.lines = []

    def readline(self):
        self.line += 1
        if self.run_until and self.line >= self.run_until:
            return ''
        origline = line = self.file.readline()
        for reg, val, key in self.defines:
            line = reg.sub(val.replace('\\', r'\\'), line)
        if self.approve:
            self.lines.append((origline, line))
        return line

    def writeline(self, line=None, replace=False):
        if self.approve:
            if line is None:
                line = ''
            elif line == '':
                return
            if not line and self.__last == '':
                return
            self.__last = line
            if not line.endswith('\n'):
                line = line + '\n'
            if replace:
                for reg, val, key in self.defines:
                    # line = line.replace('\''+val.replace('\\', '\\\\'),
                    #                     '\'${Q'+key+'}')
                    # line = line.replace(val, '${'+key+'}')
                    line = line.replace("r'"+val, "r'$"+key)
                    line = line.replace("R'"+val, "R'$"+key)
                    line = line.replace("'"+val.replace('\\', '\\\\'),
                                        "'$Q"+key)
                    line = line.replace(val, '$'+key)
            i = 0
            while i < len(self.lines):
                if self.lines[i][1] == line:
                    line = self.lines[i][0]
                    del self.lines[:i+1]
                    break
                i = i + 1
            self.approve.write(line)

    def parse_connection_string(self, s: str) -> dict:
        '''parse strings like @connection(id=con1, ...)
        '''
        res = dict()
        s = s.strip()
        if not (s.startswith('@connection(') and s.endswith(')')):
            self.raise_error(f'ERROR: invalid connection string {s}!')
        params = s[12:-1].split(',')
        for p in params:
            p = p.strip()
            try:
                k, v = p.split('=')
                if k == 'id':
                    k = 'conn_id'
                assert k in ['conn_id', 'username', 'password']
                assert res.get(k) is None
                res[k] = v
            except (ValueError, AssertionError) as e:
                self.raise_error('invalid connection parameters definition!')
        if len(res.keys()) > 1:
            try:
                assert res.get('username')
                assert res.get('password')
            except AssertionError as e:
                self.raise_error('invalid connection parameters definition, username or password missing!')
        return res

    def parse(self, f, approve=None, verbose=False, defines=None, run_until=None):
        self.approve = approve
        self.initfile(f, defines, run_until=run_until)
        nthreads = None
        if self.timeout:
            timeout = int((time.time() - self.starttime) + self.timeout)
        else:
            timeout = 0
        if self.language == 'sql':
            self.crs.execute(f'call sys.setsessiontimeout({timeout})')
            global hashge
            hashge = self.crs.execute("select * from sys.types where sqlname = 'hugeint'") == 1
        else:
            self.crs.execute(f'clients.setsessiontimeout({timeout}:int)')
        skiprest = False
        while True:
            skipping = skiprest
            line = self.readline()
            if not line:
                break
            if line.startswith('#'): # skip mal comments
                self.writeline(line.rstrip())
                continue
            if self.language == 'sql' and line.startswith('--'):
                self.writeline(line.rstrip())
                continue
            if line == '\n':
                self.writeline()
                continue
            self.qline = self.line
            conn = None
            # look for connection string
            if line.startswith('@connection'):
                conn_params = self.parse_connection_string(line)
                try:
                    conn = self.get_connection(conn_params.get('conn_id')) or self.add_connection(**conn_params)
                except TimeoutError as e:
                    self.query_error(line, 'Timeout', str(e))
                    conn = None
                    skiprest = True
                self.writeline(line.rstrip())
                line = self.readline()
            words = line.split(maxsplit=2)
            if not words:
                continue
            if not skiprest:
                while words[0] == 'skipif' or words[0] == 'onlyif':
                    if words[0] == 'skipif':
                        if words[1] in ('MonetDB', f'arch={architecture}', f'system={system}', f'bits={bits}'):
                            skipping = True
                        elif words[1].startswith('threads='):
                            if nthreads is None:
                                self.crs.execute("select value from env() where name = 'gdk_nr_threads'")
                                nthreads = self.crs.fetchall()[0][0]
                            if words[1] == f'threads={nthreads}':
                                skipping = True
                        elif words[1] == 'has-hugeint':
                            if hashge:
                                skipping = True
                        elif words[1] == 'knownfail':
                            if not self.alltests:
                                skipping = True
                    elif words[0] == 'onlyif':
                        skipping = True
                        if words[1] in ('MonetDB', f'arch={architecture}', f'system={system}', f'bits={bits}'):
                            skipping = False
                        elif words[1].startswith('threads='):
                            if nthreads is None:
                                self.crs.execute("select value from env() where name = 'gdk_nr_threads'")
                                nthreads = self.crs.fetchall()[0][0]
                            if words[1] == f'threads={nthreads}':
                                skipping = False
                        elif words[1] == 'has-hugeint':
                            if hashge:
                                skipping = False
                        elif words[1] == 'knownfail':
                            if self.alltests:
                                skipping = False
                    self.writeline(line.rstrip())
                    line = self.readline()
                    words = line.split(maxsplit=2)
            hashlabel = None
            if words[0] == 'hash-threshold':
                self.threshold = int(words[1])
                self.writeline(line.rstrip())
                self.writeline()
            elif words[0] == 'statement':
                expected_err_code = None
                expected_err_msg = None
                expected_rowcount = None
                expectok = words[1] == 'ok'
                if len(words) > 2:
                    if expectok:
                        rwords = words[2].split()
                        if rwords[0] == 'rowcount':
                            expected_rowcount = int(rwords[1])
                    else:
                        err_str = words[2]
                        expected_err_code, expected_err_msg = utils.parse_mapi_err_msg(err_str)
                statement = []
                self.qline = self.line + 1
                stline = line
                while True:
                    line = self.readline()
                    if not line or line == '\n':
                        break
                    statement.append(line.rstrip('\n'))
                if not skipping:
                    if is_copyfrom_stmt(statement):
                        stmt, stmt_less_data, statement = prepare_copyfrom_stmt(statement)
                        result = self.exec_statement(stmt, expectok, err_stmt=stmt_less_data, expected_err_code=expected_err_code, expected_err_msg=expected_err_msg, expected_rowcount=expected_rowcount, conn=conn, verbose=verbose)
                    else:
                        if self.language == 'sql' and statement[-1].endswith(';'):
                            statement[-1] = statement[-1][:-1]
                        result = self.exec_statement('\n'.join(statement), expectok, expected_err_code=expected_err_code, expected_err_msg=expected_err_msg, expected_rowcount=expected_rowcount, conn=conn, verbose=verbose)
                    if result[1] == 'crash':
                        skiprest = True
                        skipping = True
                    else:
                        self.writeline(' '.join(result))
                if skipping:
                    self.writeline(stline)
                dostrip = True
                for line in statement:
                    if dostrip:
                        line = line.rstrip()
                    self.writeline(line, replace=True)
                    if dostrip and '<COPY_INTO_DATA>' in line:
                        dostrip = False
                self.writeline()
            elif words[0] == 'query':
                columns = words[1]
                pyscript = None
                if len(words) > 2:
                    rwords = words[2].split()
                    sorting = rwords[0]  # nosort,rowsort,valuesort
                    if sorting == 'python':
                        pyscript = rwords[1]
                        if len(rwords) > 2:
                            hashlabel = rwords[2]
                    elif len(rwords) > 1:
                        hashlabel = rwords[1]
                else:
                    sorting = 'nosort'
                query = []
                self.qline = self.line + 1
                qrline = line
                while True:
                    line = self.readline()
                    if not line or line == '\n' or line.startswith('----'):
                        break
                    query.append(line.rstrip('\n'))
                if self.language == 'sql' and query[-1].endswith(';'):
                    query[-1] = query[-1][:-1]
                if not line.startswith('----'):
                    self.raise_error('---- expected')
                line = self.readline()
                if not line:
                    line = '\n'
                if 'values hashing to' in line:
                    words = line.split()
                    hash = words[4]
                    expected = None
                    nresult = int(words[0])
                else:
                    hash = None
                    expected = []
                    while line and line != '\n':
                        expected.append(line.rstrip('\n'))
                        line = self.readline()
                    nresult = len(expected)
                if not skipping:
                    result1, result2 = self.exec_query('\n'.join(query), columns, sorting, pyscript, hashlabel, nresult, hash, expected, conn=conn, verbose=verbose)
                    if result1[1] == 'crash':
                        skiprest = True
                        skipping = True
                    else:
                        self.writeline(' '.join(result1))
                        for line in query:
                            self.writeline(line.rstrip(), replace=True)
                        if result1[0] == 'query':
                            self.writeline('----')
                            for line in result2:
                                self.writeline(line)
                if skipping:
                    self.writeline(qrline.rstrip())
                    for line in query:
                        self.writeline(line.rstrip())
                    self.writeline('----')
                    if hash:
                        self.writeline(f'{nresult} values hashing to {hash}')
                    else:
                        for line in expected:
                            self.writeline(line)
                self.writeline()
            else:
                self.raise_error(f'unrecognized command {words[0]}')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run a Sqllogictest')
    parser.add_argument('--host', action='store', default='localhost',
                        help='hostname where the server runs')
    parser.add_argument('--port', action='store', type=int, default=50000,
                        help='port the server listens on')
    parser.add_argument('--database', action='store', default='demo',
                        help='name of the database')
    parser.add_argument('--user', action='store', default='monetdb',
                        help='user name to login to the database with')
    parser.add_argument('--password', action='store', default='monetdb',
                        help='password to use to login to the database with')
    parser.add_argument('--language', action='store', default='sql',
                        help='language to use for testing')
    parser.add_argument('--nodrop', action='store_true',
                        help='do not drop tables at start of test')
    parser.add_argument('--timeout', action='store', type=int, default=0,
                        help='timeout in seconds (<= 0 is no timeout) after which test is terminated')
    parser.add_argument('--verbose', action='store_true',
                        help='be a bit more verbose')
    parser.add_argument('--results', action='store',
                        type=argparse.FileType('w'),
                        help='file to store results of queries')
    parser.add_argument('--report', action='store', default='',
                        help='information to add to any error messages')
    parser.add_argument('--approve', action='store',
                        type=argparse.FileType('w'),
                        help='file in which to produce a new .test file '
                        'with updated results')
    parser.add_argument('--define', action='append',
                        help='define substitution for $var as var=replacement'
                        ' (can be repeated)')
    parser.add_argument('--alltests', action='store_true',
                        help='also executed "knownfail" tests')
    parser.add_argument('--run-until', action='store', type=int,
                        help='run tests until specified line')
    parser.add_argument('tests', nargs='*', help='tests to be run')
    opts = parser.parse_args()
    args = opts.tests
    sql = SQLLogic(report=opts.report)
    sql.res = opts.results
    sql.connect(hostname=opts.host, port=opts.port, database=opts.database,
                language=opts.language, username=opts.user,
                password=opts.password, alltests=opts.alltests,
                timeout=opts.timeout if opts.timeout > 0 else 0)
    for test in args:
        try:
            if not opts.nodrop:
                sql.drop()
            if opts.verbose:
                print(f'now testing {test}')
            try:
                sql.parse(test, approve=opts.approve, verbose=opts.verbose,
                          defines=opts.define, run_until=opts.run_until)
            except SQLLogicSyntaxError:
                pass
        except BrokenPipeError:
            break
    sql.close()
    if sql.seenerr:
        sys.exit(2)
    if sql.timedout:
        sys.exit(1)
