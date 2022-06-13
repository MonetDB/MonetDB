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
    .expect_value(1, 0, None)   # besteffort
    .expect_value(1, 1, 2)
    # .expect_value(1, 2, 'hi%%%%%')
    #
    .expect_value(2, 0, 3)
    .expect_value(2, 1, None)    # besteffort
    .expect_value(2, 2, 'good morning')
    #
    .expect_value(3, 0, 4)
    .expect_value(3, 1, 4)
    .expect_value(3, 2, None)    # besteffort
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
