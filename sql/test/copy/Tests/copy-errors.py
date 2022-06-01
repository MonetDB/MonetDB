#!/usr/bin/env python3

import copy
import decimal
from decimal import Decimal
import inspect
import os
import sys
import tempfile
import textwrap
from io import StringIO
from typing import Any, Optional, Tuple

import pymonetdb

dbname = os.getenv("TSTDB", "demo")
port = int(os.getenv("MAPIPORT", '50000'))
CONN = pymonetdb.connect(dbname, port=port)
CURSOR = CONN.cursor()

HAVE_HGE = True
try:
    CURSOR.execute('SELECT CAST(1 AS HUGEINT)')
except pymonetdb.ProgrammingError:
    HAVE_HGE = False
    CONN.rollback()

######
# Infrastructure
######

VERBOSE = os.getenv('VERBOSE') is not None
TESTS = []


def add_test(t, sub=None):
    global TESTS
    t = copy.deepcopy(t)
    t.lineno = inspect.getframeinfo(inspect.stack()[1][0]).lineno
    t.sub = sub
    TESTS.append(t)


class TestCase:
    lineno: int
    sub: Optional[str]
    fieldspec: str
    raw_testdata: Optional[bytes]
    testdata: list[str]
    quote: Optional[str]
    expectations: list[Tuple[int,int,Any]]
    error: str

    DEFAULT_BLOCKSIZE = 1000

    def __init__(self, spec, data, raw=False, quote=None, null=None):
        self.fieldspec = spec
        if raw:
            self.raw_testdata = data
            self.testdata = None
        else:
            self.raw_testdata = None
            self.testdata = data.splitlines()
        self.quote = quote
        self.null = null
        self.error = None
        self.expectations = []

    def replace(self, rowno, replacement):
        t = copy.deepcopy(self)
        t.testdata[rowno] = replacement
        t.expectations = [ (r, c, v) for r, c, v in t.expectations if r != rowno ]
        return t

    def replace_schema(self, new_schema):
        t = copy.deepcopy(self)
        t.fieldspec = new_schema
        return t

    def expect_error(self, substring):
        t = copy.deepcopy(self)
        t.error = substring
        t.expectations = []
        return t

    def expect_value(self, row, col, val):
        assert not self.error
        t = copy.deepcopy(self)
        t.expectations.append((row, col, val))
        return t

    def expect_first(self, val):
        return self.expect_value(0, 0, val)

    def run(self):
        global VERBOSE, CONN, CURSOR

        if VERBOSE:
            out = sys.stderr
        else:
            out = StringIO()
        msg = f"RUNNING TEST DEFINED AT LINE {self.lineno}"
        if self.sub:
            msg += " WITH " + self.sub
        print(f"\n**** {msg}: **********************\n", file=out)
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
            null = f" NULL AS {self.escape(self.null)}" if self.null is not None else ''
            query = textwrap.dedent(f"""\
                CALL sys.copy_blocksize({block_size});
                DROP TABLE foo;
                CREATE TABLE foo({self.fieldspec});
                COPY INTO foo FROM {qfilename}{using}{null};
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
                if self.expectations:
                    CURSOR.execute('SELECT * FROM foo')
                    results = CURSOR.fetchall()
                    for r, c, expected in self.expectations:
                        if self.testdata and '%' in self.testdata[r]:
                            continue
                        value = results[r][c]
                        print(f'Row {r} col {c} expected {expected!r}, got {value!r}', file=out)
                        assert value == expected
            else:
                try:
                    CURSOR.execute(query)
                    assert "should have thrown an exception" and False
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

basecase = (TestCase("i INT, t TEXT, j INT", testdata, quote='"')
    .expect_value(0, 0, 11)
    .expect_value(0, 1, "12x")
    .expect_value(0, 2, 13)
    #
    .expect_value(1, 0, 21)
    .expect_value(1, 1, "22x")
    .expect_value(1, 2, 23)
    #
    .expect_value(2, 0, 31)
    .expect_value(2, 1, "32x")
    .expect_value(2, 2, 33)
    #
    .expect_value(3, 0, 41)
    .expect_value(3, 1, "42x")
    .expect_value(3, 2, 43)
    #
    .expect_value(4, 0, 51)
    .expect_value(4, 1, "52x")
    .expect_value(4, 2, 53)
)

# Should succeed
add_test(basecase)

# Has doubled quote. Should still succeed
add_test(basecase
         .replace(2, '31|"32""x"|33')
         .expect_value(2, 1, '32"x'))

# Bad first column, should fail
add_test(basecase
         .replace(2, '31a|"32x"|33')
         .expect_error("Row 3 column 1 'i': unexpected character"))

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

# Unterminated string
add_test(TestCase("i INT", '"42', quote='"')
         .expect_error("unterminated quoted string"))

# Unterminated final line
add_test(TestCase("i INT", '42', raw=True)
         .expect_error("unterminated line"))

# NULL tests
add_test(TestCase("i INT", "\n", null='').expect_first(None))
add_test(TestCase("i INT", "null", null="null").expect_first(None))
add_test(TestCase("i INT", "NULL", null="null").expect_first(None))
add_test(TestCase("i INT", "null", null="NULL").expect_first(None))

# Location reporting for integer parsing failures
add_test(basecase.replace(3, '41x|"42x"|43').expect_error("Row 4 column 1 'i':"))
add_test(basecase.replace(3, '41|"42x"|43x').expect_error("Row 4 column 3 'j':"))

# Integer overflow tests
add_test(TestCase("i TINYINT", "127").expect_first(127))
add_test(TestCase("i TINYINT", "+127").expect_first(+127))
add_test(TestCase("i TINYINT", "-127").expect_first(-127))
add_test(TestCase("i TINYINT", "128").expect_error("overflow"))
add_test(TestCase("i TINYINT", "-128").expect_error("overflow"))
#
add_test(TestCase("i SMALLINT", "32767").expect_first(32767))
add_test(TestCase("i SMALLINT", "+32767").expect_first(+32767))
add_test(TestCase("i SMALLINT", "-32767").expect_first(-32767))
add_test(TestCase("i SMALLINT", "32768").expect_error("overflow"))
add_test(TestCase("i SMALLINT", "-32768").expect_error("overflow"))
#
add_test(TestCase("i INT", "2147483647").expect_first(2147483647))
add_test(TestCase("i INT", "+2147483647").expect_first(+2147483647))
add_test(TestCase("i INT", "-2147483647").expect_first(-2147483647))
add_test(TestCase("i INT", "2147483648").expect_error("overflow"))
add_test(TestCase("i INT", "-2147483648").expect_error("overflow"))
#
add_test(TestCase("i BIGINT", "9223372036854775807").expect_first(9223372036854775807))
add_test(TestCase("i BIGINT", "+9223372036854775807").expect_first(+9223372036854775807))
add_test(TestCase("i BIGINT", "-9223372036854775807").expect_first(-9223372036854775807))
add_test(TestCase("i BIGINT", "9223372036854775808").expect_error("overflow"))
add_test(TestCase("i BIGINT", "-9223372036854775808").expect_error("overflow"))
#
if HAVE_HGE:
    add_test(TestCase("i HUGEINT", "170141183460469231731687303715884105727").expect_first(170141183460469231731687303715884105727))
    add_test(TestCase("i HUGEINT", "+170141183460469231731687303715884105727").expect_first(+170141183460469231731687303715884105727))
    add_test(TestCase("i HUGEINT", "-170141183460469231731687303715884105727").expect_first(-170141183460469231731687303715884105727))
    add_test(TestCase("i HUGEINT", "170141183460469231731687303715884105728").expect_error("overflow"))
    add_test(TestCase("i HUGEINT", "-170141183460469231731687303715884105728").expect_error("overflow"))

# Integer trailing whitespace and other tails
add_test(TestCase("i INT", "", null="null").expect_error("missing integer"))
add_test(TestCase("i INT", "10").expect_first(10))
add_test(TestCase("i INT", "10  ").expect_first(10))
add_test(TestCase("i INT", "10\t").expect_first(10))
add_test(TestCase("i INT", "10.").expect_first(10))
add_test(TestCase("i INT", "10.  ").expect_first(10))
add_test(TestCase("i INT", "10.\t").expect_first(10))
add_test(TestCase("i INT", "10.0").expect_first(10))
add_test(TestCase("i INT", "10.0  ").expect_first(10))
add_test(TestCase("i INT", "10.0\t").expect_first(10))
add_test(TestCase("i INT", "10.0000000000000000000000000000000000000000000000000000000"))
add_test(TestCase("i INT", "10.0000000000000000000000000000000000000000000000000000000  "))
add_test(TestCase("i INT", "10.0000000000000000000000000000000000000000000000000000000\t"))
add_test(TestCase("i INT", "10.01").expect_error("unexpected decimal digit '1'"))
add_test(TestCase("i INT", "10.01  ").expect_error("unexpected decimal digit '1'"))
add_test(TestCase("i INT", "10.01\t").expect_error("unexpected decimal digit '1'"))
add_test(TestCase("i INT", "10x").expect_error("unexpected character 'x'"))

# Decimal parsing, the basics
add_test(TestCase("d DECIMAL(5,0)", "0").expect_first(0))
add_test(TestCase("d DECIMAL(5,0)", "-0").expect_first(0))
add_test(TestCase("d DECIMAL(5,0)", "00").expect_first(0))
add_test(TestCase("d DECIMAL(5,0)", "-00").expect_first(0))
add_test(TestCase("d DECIMAL(5,0)", "0.").expect_first(0))
add_test(TestCase("d DECIMAL(5,0)", "0.0").expect_error("too many decimal digits"))
add_test(TestCase("d DECIMAL(5,0)", ".0").expect_error("too many decimal digits"))
#
add_test(TestCase("d DECIMAL(5,0)", "10").expect_first(10))
add_test(TestCase("d DECIMAL(5,0)", "10.").expect_first(10))
add_test(TestCase("d DECIMAL(5,0)", "010").expect_first(10))
add_test(TestCase("d DECIMAL(5,0)", "-10").expect_first(-10))
add_test(TestCase("d DECIMAL(5,0)", "-10.").expect_first(-10))
add_test(TestCase("d DECIMAL(5,0)", "-010").expect_first(-10))
add_test(TestCase("d DECIMAL(5,0)", "+10").expect_first(10))
add_test(TestCase("d DECIMAL(5,0)", "+10.").expect_first(10))
add_test(TestCase("d DECIMAL(5,0)", "+010").expect_first(10))
#
add_test(TestCase("d DECIMAL(5,0)", "99999").expect_first(99999))
add_test(TestCase("d DECIMAL(5,0)", "-99999").expect_first(-99999))
add_test(TestCase("d DECIMAL(5,0)", "099999").expect_first(99999))
add_test(TestCase("d DECIMAL(5,0)", "-099999").expect_first(-99999))
#
add_test(TestCase("d DECIMAL(5,0)", "100000").expect_error("too many decimal digits"))
add_test(TestCase("d DECIMAL(5,0)", "-100000").expect_error("too many decimal digits"))
add_test(TestCase("d DECIMAL(5,0)", "999999").expect_error("too many decimal digits"))
add_test(TestCase("d DECIMAL(5,0)", "-999999").expect_error("too many decimal digits"))
#
add_test(TestCase("d DECIMAL(5, 2)", "0").expect_first(0))
add_test(TestCase("d DECIMAL(5, 2)", "00").expect_first(0))
add_test(TestCase("d DECIMAL(5, 2)", "000").expect_first(0))
add_test(TestCase("d DECIMAL(5, 2)", "0000").expect_first(0))
add_test(TestCase("d DECIMAL(5, 2)", "0.").expect_first(0))
add_test(TestCase("d DECIMAL(5, 2)", "0.0").expect_first(0))
add_test(TestCase("d DECIMAL(5, 2)", "0.00").expect_first(0))
add_test(TestCase("d DECIMAL(5, 2)", "0.000").expect_error("too many decimal digits"))
#
add_test(TestCase("d DECIMAL(5, 2)", "123").expect_first(123))
add_test(TestCase("d DECIMAL(5, 2)", "1234").expect_error("too many decimal digits"))
add_test(TestCase("d DECIMAL(5, 2)", "0123").expect_first(123))
add_test(TestCase("d DECIMAL(5, 2)", "00123").expect_first(123))
add_test(TestCase("d DECIMAL(5, 2)", "000123").expect_first(123))
add_test(TestCase("d DECIMAL(5, 2)", "123.").expect_first(123))
add_test(TestCase("d DECIMAL(5, 2)", "123.0").expect_first(123))
add_test(TestCase("d DECIMAL(5, 2)", "123.00").expect_first(123))
add_test(TestCase("d DECIMAL(5, 2)", "123.000").expect_error("too many decimal digits"))
add_test(TestCase("d DECIMAL(5, 2)", "123.").expect_first(123))
add_test(TestCase("d DECIMAL(5, 2)", "123.4").expect_first(Decimal('123.4')))
add_test(TestCase("d DECIMAL(5, 2)", "123.45").expect_first(Decimal('123.45')))
add_test(TestCase("d DECIMAL(5, 2)", "23.45").expect_first(Decimal('23.45')))
add_test(TestCase("d DECIMAL(5, 2)", "3.45").expect_first(Decimal('3.45')))
add_test(TestCase("d DECIMAL(5, 2)", "0.45").expect_first(Decimal('0.45')))
add_test(TestCase("d DECIMAL(5, 2)", ".45").expect_first(Decimal('0.45')))
add_test(TestCase("d DECIMAL(5, 2)", "-.45").expect_first(-Decimal('0.45')))
#
add_test(TestCase("d DECIMAL(5, 2)", "x").expect_error("unexpected characters"))
add_test(TestCase("d DECIMAL(5, 2)", "0x").expect_error("unexpected characters"))
add_test(TestCase("d DECIMAL(5, 2)", "1x").expect_error("unexpected characters"))
add_test(TestCase("d DECIMAL(5, 2)", ".x").expect_error("unexpected characters"))
add_test(TestCase("d DECIMAL(5, 2)", "1.x").expect_error("unexpected characters"))
add_test(TestCase("d DECIMAL(5, 2)", "1.0x").expect_error("unexpected characters"))

# Location reporting for decimal parsing failures
decimalcase = basecase.replace_schema("i DECIMAL(5,2), t TEXT, j DECIMAL(5,2)")
add_test(decimalcase.replace(3, '41x|"42x"|43').expect_error("Row 4 column 1 'i':"))
add_test(decimalcase.replace(3, '41|"42x"|43x').expect_error("Row 4 column 3 'j':"))


# Decimal overflow
max_digits = 38 if HAVE_HGE else 19
decprec = decimal.getcontext().prec
decimal.getcontext().prec = max_digits + 2
for d in range(1, max_digits + 1):
    candidate_scales = set([0, 1,  d//2, (d+1)//2, (d//2) + 1, d - 1 , d])
    scales = sorted(s for s in candidate_scales if 0 <= s <= d)
    for s in scales:
        #
        n = ('9' * (d-s)) + '.' + ('9' * s)
        dn = Decimal(n)
        input = f'{n}\n+{n}\n-{n}\n'
        answers = [dn, dn, -dn]
        tc = TestCase(f'd DECIMAL({d}, {s})', input)
        for i, a in enumerate(answers):
            tc = tc.expect_value(i, 0, a)
        add_test(tc, sub=f'd={d}, s={s}')
        #
        n = '1' + n.replace('9', '0')
        add_test(TestCase(f'd DECIMAL({d}, {s})', f'{n}').expect_error('too many decimal digits'))
        add_test(TestCase(f'd DECIMAL({d}, {s})', f'+{n}').expect_error('too many decimal digits'))
        add_test(TestCase(f'd DECIMAL({d}, {s})', f'-{n}').expect_error('too many decimal digits'))
decimal.getcontext().prec = decprec


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
