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


# UUID tests.
# UUID's are parsed through copy.parse_generic.
# These tests show that that operator reports its errors correctly.
uuiddata = """\
1|x|3e208c79-ff18-4615-8ae7-ceda0da7ffb8
2|%|408dcb98-bd0a-44d5-ad93-04c9680f9400
3|x|c79c7180-7ac3-4545-9f5f-3610d383fbd7
4|x|a3f13f6b-da3f-4827-9c09-b630e0acfb4f
5|x|395687bf-0a0a-4838-81d4-43e0fc8c6931
"""

uuidcase = TestCase("id INT, t TEXT, u UUID", uuiddata)

run_test(uuidcase)
run_test(uuidcase.replace(3, '4|x|banana')
         .expect_error("Row 4 column 3 'u': invalid uuid"))
