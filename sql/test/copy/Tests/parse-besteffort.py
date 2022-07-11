#!/usr/bin/env python3

import decimal
from decimal import Decimal
import sys

from parsetest_support import setup_suite, TestCase

dec = Decimal
suite = setup_suite(level=2)
run_test = suite.run_test

##############################################################################
#
# NOTE NOTE NOTE
#
# For the time being our BEST EFFORT implementation doesn't remove the rows,
# it just replaces the values with NULLs.
#
# This means all of these tests will have to change when we change this behaviour.
#
##############################################################################


# When the parse functions encounter an error we expect them to insert a nil
# instead and log an error.
#
# NOTE: Later on the rows with the nils will be removed but for the time being
# we still see them.

testdata = """\
1|1.0|hello
2x|2.0|hi%
3|33333333.0|good morning
4|4.0|aa\\xC3Ab
5|5.0|fine, thank you
"""
tc = TestCase("i INT, d DECIMAL(5,2), t TEXT", testdata, besteffort=True)
tc = (tc
    .expect_affected(5)   # will be 2: row 1 and 5
    .expect_value(0, 0, 1)
    .expect_value(0, 1, 1)
    .expect_value(0, 2, 'hello')
    #
    .expect_value(1, 0, None)   # besteffort for copy.parse_integer
    .expect_value(1, 1, 2)
    # .expect_value(1, 2, 'hi%%%%%')
    #
    .expect_value(2, 0, 3)
    .expect_value(2, 1, None)    # besteffort for copy.parse_decimal
    .expect_value(2, 2, 'good morning')
    #
    .expect_value(3, 0, 4)
    .expect_value(3, 1, 4)
    .expect_value(3, 2, None)    # besteffort for copy.parse_generic
    #
    .expect_value(4, 0, 5)
    .expect_value(4, 1, 5)
    .expect_value(4, 2, 'fine, thank you')
    #
    .expect_reject(2, 1, 'unexpected character')
    .expect_reject(3, 2, 'too many decimal digits')
    .expect_reject(4, 3, 'incorrectly encoded')
)
run_test(tc)

# There are two errors copy.fixlines can run into:
# 1. a too long line, currently we don't detect that but just become slower and
# slower.
# 2. an unterminated final line.
# The first remains a hard error even in BEST EFFORT mode.
# The second, on encountering an unterminated final line it should log the
# error with line number. Then it can pretend the line never existed.
testdata = """\
1|one test
2|two tests
3x|this line has an error
4|four tests
5|five tests
6|and an unterminated line\
"""
tc = (TestCase("I INT, t TEXT", testdata, besteffort=True, raw=True, blocksize=13)
    .expect_reject(3, 1, "unexpected character")
    .expect_reject(6, None, "unterminated")
)
run_test(tc)


# Scan failures result in nils for the whole row
testdata = """\
a|b|c
d|e|f
g|h|i
"""
tc = (TestCase("s TEXT, t TEXT, u TEXT", testdata, besteffort=True)
    .expect_value(0, 0, 'a')
    .expect_value(1, 0, None)
    .expect_value(1, 1, None)
    .expect_value(1, 2, None)
    # skipped line 2
    .expect_value(2, 0, 'g')
)
run_test(tc.replace(1, r"d\x|e|f").expect_reject(2, 1, "incomplete hex"))
run_test(tc.replace(1, r"d|e\x|f").expect_reject(2, 2, "incomplete hex"))
run_test(tc.replace(1, r"d|e|f\x").expect_reject(2, 3, "incomplete hex"))
# Only lists the first one:
run_test(tc.replace(1, r"d\x|e\x|f\x")
    .expect_reject(2, 1, "incomplete hex")
)
#
run_test(tc.replace(1, r"d|e")
    .expect_reject(2, None, "too few fields")
)
run_test(tc.replace(1, r"d|e|f|g")
    .expect_reject(2, None, "too many fields")
)

# Same as above, but with quoting enabled
testdata = """\
a|b|c
d|e|f
g|h|i
"""
tc = (TestCase("s TEXT, t TEXT, u TEXT", testdata, besteffort=True, quote='^')
    .expect_value(0, 0, 'a')
    .expect_value(1, 0, None)
    .expect_value(1, 1, None)
    .expect_value(1, 2, None)
    # skipped line 2
    .expect_value(2, 0, 'g')
)
run_test(tc.replace(1, r"d\x|e|f").expect_reject(2, 1, "incomplete hex"))
run_test(tc.replace(1, r"d|e\x|f").expect_reject(2, 2, "incomplete hex"))
run_test(tc.replace(1, r"d|e|f\x").expect_reject(2, 3, "incomplete hex"))
# Only lists the first one:
run_test(tc.replace(1, r"d\x|e\x|f\x")
    .expect_reject(2, 1, "incomplete hex")
)
#
run_test(tc.replace(1, r"d|e")
    .expect_reject(2, None, "too few fields")
)
run_test(tc.replace(1, r"d|e|f|g")
    .expect_reject(2, None, "too many fields")
)

# What if the error occur in quoted fields?
testdata = """\
a|^b^|c
d|^e^|f
g|^^^^|i
"""
tc = (TestCase("s TEXT, t TEXT, u TEXT", testdata, besteffort=True, quote='^')
    .expect_value(0, 0, 'a')
    .expect_value(1, 0, None)
    .expect_value(1, 1, None)
    .expect_value(1, 2, None)
    # skipped line 2
    .expect_value(2, 0, 'g')
)
run_test(tc.replace(1, r"d\x|^e^|f").expect_reject(2, 1, "incomplete hex"))
run_test(tc.replace(1, r"d|^e\x^|f").expect_reject(2, 2, "incomplete hex"))
run_test(tc.replace(1, r"d|^e^BANANA|f").expect_reject(2, 2, "end quote"))
run_test(tc.replace(1, r"d|^e^|f\x").expect_reject(2, 3, "incomplete hex"))
# Only lists the first one:
run_test(tc.replace(1, r"d\x|^e\x^|f\x")
    .expect_reject(2, 1, "incomplete hex")
)
#
run_test(tc.replace(1, r"d|^e^")
    .expect_reject(2, None, "too few fields")
)
run_test(tc.replace(1, r"d|^e^|f|g")
    .expect_reject(2, None, "too many fields")
)
#
