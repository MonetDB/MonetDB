#!/usr/bin/env python3

import copy
import inspect
import os
import sys
import tempfile
import textwrap
from io import StringIO
from typing import Any, Optional, Tuple

import pymonetdb

######
# Infrastructure
######


class TestCase:
    lineno: int
    sub: Optional[str]
    fieldspec: str
    raw_testdata: Optional[bytes]
    testdata: list[str]
    escape: Optional[bool]
    null: Optional[str]
    besteffort: bool
    quote: Optional[str]
    offsetk: Optional[int]
    nrecords: Optional[int]
    affected: Optional[int]
    expectations: list[Tuple[int, int, Any]]
    rejects: list[Tuple[int, int, str]]
    error: str

    DEFAULT_BLOCKSIZE = 1000

    def __init__(self, spec, data, raw=False, quote=None, null=None, escape=None, besteffort=False):
        self.fieldspec = spec
        if raw:
            self.raw_testdata = data
            self.testdata = None
        else:
            self.raw_testdata = None
            self.testdata = data.splitlines()
        self.quote = quote
        self.escape = escape
        self.null = null
        self.besteffort = besteffort
        self.offsetk = None
        self.nrecords = None
        self.affected = None
        self.error = None
        self.expectations = []
        self.rejects = []

    def replace(self, rowno, replacement) -> "TestCase":
        t = copy.deepcopy(self)
        t.testdata[rowno] = replacement
        t.expectations = [(r, c, v)
                          for r, c, v in t.expectations if r != rowno]
        return t

    def set_quote(self, q) -> "TestCase":
        t = copy.deepcopy(self)
        t.quote = q
        return t

    def set_backslashes(self, b) -> "TestCase":
        t = copy.deepcopy(self)
        t.backslashes = b
        return t

    def set_escape(self, e) -> "TestCase":
        t = copy.deepcopy(self)
        t.escape = e
        return t

    def offset(self, off) -> "TestCase":
        t = copy.deepcopy(self)
        t.offsetk = off
        return t

    def records(self, nrec) -> "TestCase":
        t = copy.deepcopy(self)
        t.nrecords = nrec
        return t

    def besteffort(self, best: bool) -> "TestCase":
        t = copy.deepcopy(self)
        t.besteffort = best
        return t

    def expect_affected(self, n) -> "TestCase":
        t = copy.deepcopy(self)
        t.affected = n
        t.expectations = []
        return t

    def expect_error(self, substring) -> "TestCase":
        t = copy.deepcopy(self)
        t.error = substring
        t.expectations = []
        return t

    def expect_value(self, row, col, val) -> "TestCase":
        assert not self.error
        t = copy.deepcopy(self)
        t.expectations.append((row, col, val))
        return t

    def expect_first(self, val) -> "TestCase":
        return self.expect_value(0, 0, val)

    def expect_reject(self, row, col, msg) -> "TestCase":
        t = copy.deepcopy(self)
        t.rejects.append((row, col, msg))
        return t

    def run(self, conn: pymonetdb.Connection, cursor, out, prefix):
        msg = f"RUNNING TEST DEFINED AT LINE {self.lineno}"
        if self.sub:
            msg += " WITH " + self.sub
        print(f"\n**** {msg}: **********************\n", file=out)
        conn.rollback()
        testdata, block_size = self.prepare_testdata()

        print(
            f'----- testdata -----\n{testdata}--------------------\n', file=out)

        f = tempfile.NamedTemporaryFile(
            'w', encoding='utf-8', delete=False, prefix="copyerrors", suffix=".txt")
        filename = f.name
        qfilename = self.escape_text(filename)
        f.write(testdata)
        f.close()

        prefix = "CALL sys.clearrejects();\n" + prefix
        option_parts = []
        nrec_offset_parts = []
        if self.quote:
            option_parts.append(f"USING DELIMITERS '|', E'\\n', {self.escape_text(self.quote)}")
        if self.escape is not None:
            if self.escape:
                option_parts.append("ESCAPE")
            else:
                option_parts.append("NO ESCAPE")
        if self.null is not None:
            option_parts.append(f" NULL AS {self.escape_text(self.null)}")
        if self.besteffort:
            option_parts.append("BEST EFFORT")
        if self.nrecords is not None:
            nrec_offset_parts.append(f"{self.nrecords} RECORDS")
        if self.offsetk is not None:
            nrec_offset_parts.append(f"OFFSET {self.offsetk}")
        options = ' '.join(option_parts)
        nrec_offset = ' '.join(nrec_offset_parts)
        if nrec_offset:
            nrec_offset = ' ' + nrec_offset
        query = prefix + textwrap.dedent(f"""\
            CALL sys.copy_blocksize({block_size});
            DROP TABLE IF EXISTS foo;
            CREATE TABLE foo({self.fieldspec});
            COPY{nrec_offset} INTO foo FROM {qfilename} {options};
        """)

        print(
            f'----- sql code -----\n{query}--------------------\n', file=out)

        if self.error is None:
            cursor.execute(query)
            rowcount = cursor.rowcount
            if self.affected is not None:
                expected = self.affected
            elif self.testdata is not None:
                expected = len(self.testdata)
            else:
                expected = None
            if expected is not None:
                print(f'Expected {expected} affected rows, got {rowcount}', file=out)
                print(file=out)
                assert rowcount == expected
            if self.expectations:
                cursor.execute('SELECT * FROM foo')
                results = cursor.fetchall()
                for r, c, expected in self.expectations:
                    if self.testdata and '%' in self.testdata[r]:
                        continue
                    value = results[r][c]
                    print(f'Row {r} col {c} expected {expected!r}, got {value!r}', file=out)
                    assert value == expected
            print(file=out)
            cursor.execute("SELECT rowid, fldid, message FROM sys.rejects")
            rejects = set(cursor.fetchall())
            seen = set()
            for row, col, msg in self.rejects:
                cands = [(r,c,m) for r,c,m in rejects if r == row and c == col and msg in m]
                if len(cands) == 1:
                    print(f"Found reject row={row} col={col} msg={msg!r}:\n\t{cands[0][2]!r}", file=out)
                    seen.add(cands[0])
                elif len(cands) == 0:
                    print(f"Did not find reject row={row} col={col} msg={msg!r}", file=out)
                    assert False
                else:
                    print(f"Found {len(cands)} candidates for reject row={row} col={col} msg={msg!r}", file=out)
                    assert False
            unseen = rejects - seen
            if unseen:
                print(f"Found {len(unseen)} unexpected rejects:", file=out)
                for r, c, m in sorted(unseen):
                    print(f"        row={r} col={c} msg={m}", file=out)
        else:
            try:
                cursor.execute(query)
                assert "should have thrown an exception" and False
            except pymonetdb.exceptions.Error as e:
                exc = e
            print(f'Got exception:      {exc}', file=out)
            print(f'Expected exception: {self.error}', file=out)
            print(file=out)
            assert self.error in str(exc)
        print(f'OK', file=out)

        os.unlink(filename)

    def prepare_testdata(self):
        if self.raw_testdata is not None:
            return (self.raw_testdata, self.DEFAULT_BLOCKSIZE)
        first_group = []
        second_group = self.testdata[:]
        block_size = self.DEFAULT_BLOCKSIZE
        for i, line in enumerate(second_group):
            if '%' in line:
                first_group = second_group[:i + 1]
                second_group = second_group[i + 1:]
                break
        first_block = "\n".join(first_group)
        second_block = "\n".join(second_group)
        if first_block:
            difference = len(second_block) - len(first_block)
            if difference > 0:
                idx = first_block.find('%')
                first_block = first_block[:idx] + \
                    difference * '%' + first_block[idx+1:]
                first_block = first_block[:idx] + \
                    difference * '%' + first_block[idx+1:]
            block_size = len(first_block) + 1
            first_block += "\n"
        second_block += "\n"
        testdata = first_block + second_block
        return (testdata, block_size)

    def escape_text(self, text):
        if "\\" not in text and "'" not in text and '"' not in text:
            return f"'{text}'"
        else:
            return "E'" + text.replace("\\", "\\\\").replace("\'", "\\\'") + "'"


class TestSuite:
    conn: pymonetdb.Connection
    verbose: bool
    level: int
    have_hge: bool
    filter = None

    def __init__(self, conn, filter, verbose, level):
        self.conn = conn
        self.verbose = verbose
        self.level = level
        self.have_hge = True
        self.filter = filter
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT CAST(1 AS HUGEINT)')
        except pymonetdb.ProgrammingError:
            self.have_hge = False
            self.conn.rollback()
        finally:
            cursor.close()

    def run_test(self, t, sub=None):
        prefix = ""
        if self.level is not None:
            prefix += f"CALL sys.copy_parallel({self.level});\n"
        t = copy.deepcopy(t)
        t.lineno = inspect.getframeinfo(inspect.stack()[1][0]).lineno
        t.sub = sub
        if self.filter and not self.filter(t):
            return
        if self.verbose:
            out = sys.stderr
        else:
            out = StringIO()
        try:
            c = self.conn.cursor()
            t.run(self.conn, c, out, prefix)
        except Exception:
            if not self.verbose:
                sys.stderr.write(out.getvalue())
            raise
        finally:
            c.close()


def setup_suite(level=None):
    verbose = os.getenv('VERBOSE') is not None
    linenos = set(int(a) for a in os.getenv('ONLY', '').split())
    env_level = os.getenv('LEVEL', None)
    if env_level is not None:
        level = int(env_level)
    if linenos:
        filter = lambda t: t.lineno in linenos
        verbose = True
    else:
        filter = None
    dbname = os.getenv("TSTDB", "demo")
    port = int(os.getenv("MAPIPORT", '50000'))
    conn = pymonetdb.connect(dbname, port=port)

    return TestSuite(conn, filter=filter, verbose=verbose, level=level)
