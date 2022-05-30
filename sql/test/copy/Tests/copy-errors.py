#!/usr/bin/env python3

import copy
import inspect
import os
import sys
import tempfile
import textwrap
from io import StringIO
from typing import Optional

import pymonetdb

dbname = os.getenv("TSTDB", "demo")
port = int(os.getenv("MAPIPORT", '50000'))
CONN = pymonetdb.connect(dbname, port=port)
CURSOR = CONN.cursor()

######
# Infrastructure
######

VERBOSE = os.getenv('VERBOSE') is not None
TESTS = []


def add_test(t):
    global TESTS
    t = copy.deepcopy(t)
    t.lineno = inspect.getframeinfo(inspect.stack()[1][0]).lineno
    TESTS.append(t)


class TestCase:
    lineno: int
    fieldspec: str
    raw_testdata: Optional[bytes]
    testdata: list[str]
    quote: Optional[str]
    error: str

    DEFAULT_BLOCKSIZE = 1000

    def __init__(self, spec, data, raw=False, quote=None):
        self.fieldspec = spec
        if raw:
            self.raw_testdata = data
            self.testdata = None
        else:
            self.raw_testdata = None
            self.testdata = data.splitlines()
        self.quote = quote
        self.error = None

    def replace(self, lineno, replacement):
        t = copy.deepcopy(self)
        t.testdata[lineno] = replacement
        return t

    def expect_error(self, substring):
        t = copy.deepcopy(self)
        t.error = substring
        return t

    def run(self):
        global VERBOSE, CONN, CURSOR

        if VERBOSE:
            out = sys.stderr
        else:
            out = StringIO()
        print(
            f"\n**** RUNNING TEST DEFINED AT LINE {self.lineno}: **********************\n", file=out)
        try:
            CONN.rollback()
            testdata, block_size = self.prepare_testdata()

            print(
                f'----- testdata -----\n{testdata}--------------------\n', file=out)

            f = tempfile.NamedTemporaryFile(
                'w', encoding='utf-8', delete=False, prefix="copyerrors", suffix=".txt")
            filename = f.name
            qfilename = self.escape(filename)
            f.write(testdata)
            f.close()
            using = f" USING DELIMITERS '|', E'\\n', {self.escape(self.quote)}" if self.quote else ''
            query = textwrap.dedent(f"""\
                CALL sys.copy_blocksize({block_size});
                DROP TABLE foo;
                CREATE TABLE foo({self.fieldspec});
                COPY INTO foo FROM {qfilename}{using};
            """)

            print(
                f'----- sql code -----\n{query}--------------------\n', file=out)

            if self.error is None:
                CURSOR.execute(query)
                rowcount = CURSOR.rowcount
                expected = len(self.testdata)
                print(f'Expected {expected} affected rows, got {rowcount}', file=out)
                print(file=out)
                assert rowcount == expected
            else:
                try:
                    CURSOR.execute(query)
                    assert "should have thrown" and False
                except pymonetdb.exceptions.Error as e:
                    exc = e
                print(f'Got exception:      {exc}', file=out)
                print(f'Expected exception: {self.error}', file=out)
                print(file=out)
                assert self.error in str(exc)
            print(f'OK', file=out)

            os.unlink(filename)
        except Exception:
            if not VERBOSE:
                sys.stderr.write(out.getvalue())
            raise

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

    def escape(self, text):
        if "\\" not in text and "'" not in text and '"' not in text:
            return f"'{text}'"
        else:
            return "E'" + text.replace("\\", "\\\\").replace("\'", "\\\'") + "'"


######
# Tests
######

testdata = """\
11|"12x"|13
21|"22x%"|23
31|"32x"|33
41|"42x"|43
51|"52x"|53
"""

basecase = TestCase("i INT, t TEXT, j INT", testdata, quote='"')

# Should succeed
add_test(basecase)

# Has doubled quote. Should still succeed
add_test(basecase
         .replace(2, '31|"32""x"|33'))

# Bad first column, should fail
add_test(basecase
         .replace(2, '31a|"32x"|33')
         .expect_error("Row 3 column 1 'i': unexpected characters while parsing integer: a"))

# NUL character, unquoted
add_test(basecase
         .replace(2, '31a\x00|"32x"|33')
         .expect_error("Row 3 column 1: invalid NUL character"))
add_test(basecase
         .replace(2, '3\x001a|"32x"|33')
         .expect_error("Row 3 column 1: invalid NUL character"))

# NUL character, quoted
add_test(basecase
         .replace(2, '31a|"32x\x00"|33')
         .expect_error("Row 3 column 2: invalid NUL character"))

######
# Run the tests
######

if __name__ == "__main__":
    args = sys.argv[1:]
    ran = set()
    for t in TESTS:
        line = str(t.lineno)
        if not args or line in args:
            t.run()
            ran.add(line)

    for arg in args:
        if not arg in ran:
            print(f"Warning: no test defined at line {arg}", file=sys.stderr)
