#!/usr/bin/env python3

import decimal
from decimal import Decimal
import sys

from parsetest_support import setup_suite, TestCase

suite = setup_suite()
run_test = suite.run_test


testdata = """\
11|13
21|23
31|33
41|43
51|53
"""

basecase = (TestCase("i INT, j INT", testdata, quote='"')
            .expect_value(0, 0, 11)
            .expect_value(0, 2, 13)
            #
            .expect_value(1, 0, 21)
            .expect_value(1, 2, 23)
            #
            .expect_value(2, 0, 31)
            .expect_value(2, 2, 33)
            #
            .expect_value(3, 0, 41)
            .expect_value(3, 2, 43)
            #
            .expect_value(4, 0, 51)
            .expect_value(4, 2, 53)
            )

# Should succeed
run_test(basecase)


# Location reporting for integer parsing failures
run_test(basecase.replace(3, '41x|43').expect_error("Row 4 column 1 'i':"))
run_test(basecase.replace(3, '41|43x').expect_error("Row 4 column 2 'j':"))

# Integer overflow tests
run_test(TestCase("i TINYINT", "127").expect_first(127))
run_test(TestCase("i TINYINT", "+127").expect_first(+127))
run_test(TestCase("i TINYINT", "-127").expect_first(-127))
run_test(TestCase("i TINYINT", "128").expect_error("overflow"))
run_test(TestCase("i TINYINT", "-128").expect_error("overflow"))
#
run_test(TestCase("i SMALLINT", "32767").expect_first(32767))
run_test(TestCase("i SMALLINT", "+32767").expect_first(+32767))
run_test(TestCase("i SMALLINT", "-32767").expect_first(-32767))
run_test(TestCase("i SMALLINT", "32768").expect_error("overflow"))
run_test(TestCase("i SMALLINT", "-32768").expect_error("overflow"))
#
run_test(TestCase("i INT", "2147483647").expect_first(2147483647))
run_test(TestCase("i INT", "+2147483647").expect_first(+2147483647))
run_test(TestCase("i INT", "-2147483647").expect_first(-2147483647))
run_test(TestCase("i INT", "2147483648").expect_error("overflow"))
run_test(TestCase("i INT", "-2147483648").expect_error("overflow"))
#
run_test(TestCase("i BIGINT", "9223372036854775807")
         .expect_first(9223372036854775807))
run_test(TestCase("i BIGINT", "+9223372036854775807").expect_first(+9223372036854775807))
run_test(TestCase("i BIGINT", "-9223372036854775807").expect_first(-9223372036854775807))
run_test(TestCase("i BIGINT", "9223372036854775808").expect_error("overflow"))
run_test(TestCase("i BIGINT", "-9223372036854775808").expect_error("overflow"))
#
if suite.have_hge:
    run_test(TestCase("i HUGEINT", "170141183460469231731687303715884105727")
             .expect_first(170141183460469231731687303715884105727))
    run_test(TestCase("i HUGEINT", "+170141183460469231731687303715884105727")
             .expect_first(+170141183460469231731687303715884105727))
    run_test(TestCase("i HUGEINT", "-170141183460469231731687303715884105727")
             .expect_first(-170141183460469231731687303715884105727))
    run_test(TestCase("i HUGEINT", "170141183460469231731687303715884105728")
             .expect_error("overflow"))
    run_test(TestCase("i HUGEINT", "-170141183460469231731687303715884105728")
             .expect_error("overflow"))

# Integer trailing whitespace and other tails
run_test(TestCase("i INT", "", null="null").expect_error("missing integer"))
run_test(TestCase("i INT", "10").expect_first(10))
run_test(TestCase("i INT", "10  ").expect_first(10))
run_test(TestCase("i INT", "10\t").expect_first(10))
run_test(TestCase("i INT", "10.").expect_first(10))
run_test(TestCase("i INT", "10.  ").expect_first(10))
run_test(TestCase("i INT", "10.\t").expect_first(10))
run_test(TestCase("i INT", "10.0").expect_first(10))
run_test(TestCase("i INT", "10.0  ").expect_first(10))
run_test(TestCase("i INT", "10.0\t").expect_first(10))
run_test(TestCase("i INT", "10.00000000000000000000000000000000000000000000"))
run_test(TestCase("i INT", "10.00000000000000000000000000000000000000000000  "))
run_test(TestCase("i INT", "10.00000000000000000000000000000000000000000000\t"))
run_test(TestCase("i INT", "10.01")
         .expect_error("unexpected decimal digit '1'"))
run_test(TestCase("i INT", "10.01  ")
         .expect_error("unexpected decimal digit '1'"))
run_test(TestCase("i INT", "10.01\t")
         .expect_error("unexpected decimal digit '1'"))
run_test(TestCase("i INT", "10x").expect_error("unexpected character 'x'"))
