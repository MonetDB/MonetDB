#!/usr/bin/env python3

import decimal
from decimal import Decimal
import sys

from parsetest_support import setup_suite, TestCase

suite = setup_suite()
run_test = suite.run_test


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
run_test(basecase)

# Has doubled quote. Should still succeed
run_test(basecase
         .replace(2, '31|"32""x"|33')
         .expect_value(2, 1, '32"x'))

# NUL character, unquoted
run_test(basecase
         .replace(2, '31a\x00|"32x"|33')
         .expect_error("Row 3 column 1: invalid NUL character"))
run_test(basecase
         .replace(2, '3\x001a|"32x"|33')
         .expect_error("Row 3 column 1: invalid NUL character"))

# NUL character, quoted
run_test(basecase
         .replace(2, '31a|"32x\x00"|33')
         .expect_error("Row 3 column 2: invalid NUL character"))

# Unterminated string
run_test(TestCase("i INT", '"42', quote='"')
         .expect_error("unterminated quoted string"))

# Unterminated final line
run_test(TestCase("i INT", '42', raw=True)
         .expect_error("unterminated line"))

# NULL tests
run_test(TestCase("i INT", "\n", null='').expect_first(None))
run_test(TestCase("i INT", "null", null="null").expect_first(None))
run_test(TestCase("i INT", "NULL", null="null").expect_first(None))
run_test(TestCase("i INT", "null", null="NULL").expect_first(None))
