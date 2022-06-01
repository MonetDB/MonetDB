#!/usr/bin/env python3

import decimal
from decimal import Decimal
import sys

from parsetest_support import setup_suite, TestCase

suite = setup_suite()
run_test = suite.run_test


# Decimal parsing, the basics
run_test(TestCase("d DECIMAL(5,0)", "0").expect_first(0))
run_test(TestCase("d DECIMAL(5,0)", "-0").expect_first(0))
run_test(TestCase("d DECIMAL(5,0)", "00").expect_first(0))
run_test(TestCase("d DECIMAL(5,0)", "-00").expect_first(0))
run_test(TestCase("d DECIMAL(5,0)", "0.").expect_first(0))
run_test(TestCase("d DECIMAL(5,0)", "0.0")
         .expect_error("too many decimal digits"))
run_test(TestCase("d DECIMAL(5,0)", ".0")
         .expect_error("too many decimal digits"))
#
run_test(TestCase("d DECIMAL(5,0)", "10").expect_first(10))
run_test(TestCase("d DECIMAL(5,0)", "10.").expect_first(10))
run_test(TestCase("d DECIMAL(5,0)", "010").expect_first(10))
run_test(TestCase("d DECIMAL(5,0)", "-10").expect_first(-10))
run_test(TestCase("d DECIMAL(5,0)", "-10.").expect_first(-10))
run_test(TestCase("d DECIMAL(5,0)", "-010").expect_first(-10))
run_test(TestCase("d DECIMAL(5,0)", "+10").expect_first(10))
run_test(TestCase("d DECIMAL(5,0)", "+10.").expect_first(10))
run_test(TestCase("d DECIMAL(5,0)", "+010").expect_first(10))
#
run_test(TestCase("d DECIMAL(5,0)", "99999").expect_first(99999))
run_test(TestCase("d DECIMAL(5,0)", "-99999").expect_first(-99999))
run_test(TestCase("d DECIMAL(5,0)", "099999").expect_first(99999))
run_test(TestCase("d DECIMAL(5,0)", "-099999").expect_first(-99999))
#
run_test(TestCase("d DECIMAL(5,0)", "100000")
         .expect_error("too many decimal digits"))
run_test(TestCase("d DECIMAL(5,0)", "-100000")
         .expect_error("too many decimal digits"))
run_test(TestCase("d DECIMAL(5,0)", "999999")
         .expect_error("too many decimal digits"))
run_test(TestCase("d DECIMAL(5,0)", "-999999")
         .expect_error("too many decimal digits"))
#
run_test(TestCase("d DECIMAL(5, 2)", "0").expect_first(0))
run_test(TestCase("d DECIMAL(5, 2)", "00").expect_first(0))
run_test(TestCase("d DECIMAL(5, 2)", "000").expect_first(0))
run_test(TestCase("d DECIMAL(5, 2)", "0000").expect_first(0))
run_test(TestCase("d DECIMAL(5, 2)", "0.").expect_first(0))
run_test(TestCase("d DECIMAL(5, 2)", "0.0").expect_first(0))
run_test(TestCase("d DECIMAL(5, 2)", "0.00").expect_first(0))
run_test(TestCase("d DECIMAL(5, 2)", "0.000")
         .expect_error("too many decimal digits"))
#
run_test(TestCase("d DECIMAL(5, 2)", "123").expect_first(123))
run_test(TestCase("d DECIMAL(5, 2)", "1234")
         .expect_error("too many decimal digits"))
run_test(TestCase("d DECIMAL(5, 2)", "0123").expect_first(123))
run_test(TestCase("d DECIMAL(5, 2)", "00123").expect_first(123))
run_test(TestCase("d DECIMAL(5, 2)", "000123").expect_first(123))
run_test(TestCase("d DECIMAL(5, 2)", "123.").expect_first(123))
run_test(TestCase("d DECIMAL(5, 2)", "123.0").expect_first(123))
run_test(TestCase("d DECIMAL(5, 2)", "123.00").expect_first(123))
run_test(TestCase("d DECIMAL(5, 2)", "123.000")
         .expect_error("too many decimal digits"))
run_test(TestCase("d DECIMAL(5, 2)", "123.").expect_first(123))
run_test(TestCase("d DECIMAL(5, 2)", "123.4").expect_first(Decimal('123.4')))
run_test(TestCase("d DECIMAL(5, 2)", "123.45").expect_first(Decimal('123.45')))
run_test(TestCase("d DECIMAL(5, 2)", "23.45").expect_first(Decimal('23.45')))
run_test(TestCase("d DECIMAL(5, 2)", "3.45").expect_first(Decimal('3.45')))
run_test(TestCase("d DECIMAL(5, 2)", "0.45").expect_first(Decimal('0.45')))
run_test(TestCase("d DECIMAL(5, 2)", ".45").expect_first(Decimal('0.45')))
run_test(TestCase("d DECIMAL(5, 2)", "-.45").expect_first(-Decimal('0.45')))
#
run_test(TestCase("d DECIMAL(5, 2)", "x").expect_error("unexpected characters"))
run_test(TestCase("d DECIMAL(5, 2)", "0x").expect_error("unexpected characters"))
run_test(TestCase("d DECIMAL(5, 2)", "1x").expect_error("unexpected characters"))
run_test(TestCase("d DECIMAL(5, 2)", ".x").expect_error("unexpected characters"))
run_test(TestCase("d DECIMAL(5, 2)", "1.x").expect_error("unexpected characters"))
run_test(TestCase("d DECIMAL(5, 2)", "1.0x")
         .expect_error("unexpected characters"))

# Location reporting for decimal parsing failures
testdata = """\
1.1|1.2
2.1|2.2
3.1|3.2
4.1|4.2
5.1|5.2
"""
testcase = TestCase("d2 DECIMAL(5,2), d3 DECIMAL(17,3)", testdata, quote='"')
run_test(testcase.expect_value(3, 1, Decimal('4.2')))
run_test(testcase.replace(3, '41x|43').expect_error("Row 4 column 1 'd2':"))
run_test(testcase.replace(3, '41|43x').expect_error("Row 4 column 2 'd3':"))


# Decimal overflow
max_digits = 38 if suite.have_hge else 19
decprec = decimal.getcontext().prec
decimal.getcontext().prec = max_digits + 2
for d in range(1, max_digits + 1):
    candidate_scales = set([0, 1,  d//2, (d+1)//2, (d//2) + 1, d - 1, d])
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
        run_test(tc, sub=f'd={d}, s={s}')
        #
        n = '1' + n.replace('9', '0')
        run_test(TestCase(f'd DECIMAL({d}, {s})', f'{n}')
                 .expect_error('too many decimal digits'))
        run_test(TestCase(f'd DECIMAL({d}, {s})', f'+{n}')
                 .expect_error('too many decimal digits'))
        run_test(TestCase(f'd DECIMAL({d}, {s})', f'-{n}')
                 .expect_error('too many decimal digits'))
decimal.getcontext().prec = decprec
