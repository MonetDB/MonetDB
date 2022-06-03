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

# Not fully enclosed
run_test(basecase
    .replace(2, '31|"32"x|33')
    .expect_error("end quote must be followed by separator"))

# NUL character, unquoted and quoted
for escape in [True, False]:
    for quote in ['"', None]:
        mycase = (basecase
            .set_escape(escape)
            .set_quote(quote)
        )
        sub = f'escape={escape!r} quote={quote!r}'
        msg = "NUL character not allowed in textual data"
        run_test(mycase
                .replace(2, '31a\x00|"32x"|33')
                .expect_error(f"Row 3 column 1: {msg}"),
                sub=sub)
        run_test(mycase
                .replace(2, '3\x001a|"32x"|33')
                .expect_error(f"Row 3 column 1: {msg}"),
                sub=sub)
        run_test(mycase
                .replace(2, '31a|"32x\x00"|33')
                .expect_error(f"Row 3 column 2: {msg}"),
                sub=sub)

# Unterminated string
run_test(TestCase("i INT, t TEXT", '1\n2\n3\n"42', quote='"')
         .expect_error("Row 4: unterminated quoted string"))

# Unterminated final line
run_test(TestCase("i INT, t TEXT", '1\n2\n3\n42', raw=True)
         .expect_error("Row 4: unterminated line"))


# Test various escape sequences
def good_escape(text, value):
    return TestCase("t TEXT", text).set_backslashes(True).expect_first(value)

def bad_escape(text, errmsg):
    return TestCase("t TEXT", text).set_backslashes(True).expect_error(errmsg)

run_test(good_escape(r"a\|", "a|"))


run_test(bad_escape(r"a\000b", "not a valid octal escape"))
run_test(bad_escape(r"a\00b", "not a valid octal escape"))
run_test(bad_escape(r"a\0b", "not a valid octal escape"))
run_test(bad_escape(r"a\0", "not a valid octal escape"))
# 8 is a decimal digit but not an octal digit:
run_test(good_escape(r"a\0018", "a\0018"))
run_test(good_escape(r"a\018", "a\0018"))
run_test(good_escape(r"a\18", "a\0018"))
run_test(bad_escape(r"a\400b", "octal escape out of range"))
run_test(bad_escape(r"a\777b", "octal escape out of range"))
run_test(good_escape(r"a\303\270b", "a\u00F8b"))
run_test(bad_escape(r"aa\303Ab", "incorrectly encoded UTF-8"))


run_test(bad_escape(r"a\x00b", "not a valid hex escape"))
run_test(good_escape(r"a\x01b", "a\001b"))
run_test(good_escape(r"a\xc3\xb8b", "a\u00F8b"))
run_test(bad_escape(r"aa\xc3Ab", "incorrectly encoded UTF-8"))
run_test(bad_escape(r"a\xG3b", "incomplete hex"))
run_test(bad_escape(r"a\x\3b", "incomplete hex"))
run_test(good_escape(r"a\xC3\xB8b", "a\u00F8b"))
run_test(bad_escape(r"aa\xC3Ab", "incorrectly encoded UTF-8"))

run_test(good_escape(r"a\u0021b", "a!b"))
run_test(good_escape(r"a\u25a1b", "a\u25a1b"))
run_test(good_escape(r"a\u0021", "a!"))
run_test(bad_escape(r"a\u002", "incomplete hex"))
run_test(bad_escape(r"a\u0000b", "\\u0000 is not a valid unicode escape"))
#
run_test(bad_escape(r"a\ud800b", "surrogate"))
run_test(bad_escape(r"a\udbffb", "surrogate"))
run_test(bad_escape(r"a\udc00b", "surrogate"))
run_test(bad_escape(r"a\udfffb", "surrogate"))
#
run_test(good_escape(r"a\U00000021b", "a!b"))
run_test(good_escape(r"a\U0001F440b", "a\U0001F440b"))
run_test(good_escape(r"a\U00000021", "a!"))
run_test(bad_escape(r"a\U0000002", "incomplete hex"))
run_test(bad_escape(r"a\U00000000b", "\\U00000000 is not a valid unicode escape"))
#
run_test(bad_escape(r"a\U0000d800b", "surrogate"))
run_test(bad_escape(r"a\U0000dbffb", "surrogate"))
run_test(bad_escape(r"a\U0000dc00b", "surrogate"))
run_test(bad_escape(r"a\U0000dfffb", "surrogate"))


# upper case also works
run_test(good_escape(r"a\xC3\xB8b", "a\u00F8b"))
run_test(good_escape(r"a\u25A1b", "a\u25a1b"))

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


# Test field and record termination
run_test(TestCase("i INT, t TEXT, j INT", """\
    11|x|12
    21|%|22
    31|x|32
    41|x|42
"""))
run_test(TestCase("i INT, t TEXT, j INT", """\
    11|x|12|
    21|%|22
    31|x|32|
    41|x|42
"""))
run_test(TestCase("i INT, t TEXT, j INT", """\
    11|x|12|
    21|%|22
    31|x|32|
    41|x|42|
"""))
run_test(TestCase("i INT, t TEXT, j INT", """\
    11|x|12
    21|%|22
    31|x|32|banana
    41|x|42
""").expect_error("Row 3: too many fields"))
run_test(TestCase("i INT, t TEXT, j INT", """\
    11|x|12
    21|%|22
    31|
    41|x|42
""").expect_error("Row 3: too few fields"))
run_test(TestCase("i INT", "1\n2\n3", raw=True).expect_error("unterminated line at end"))



# OFFSET and RECORDS
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .offset(0)  # same as 1
         .expect_value(2, 0, 31))
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .offset(1)  # same as 0
         .expect_value(2, 0, 31))
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .offset(2)
         .expect_affected(4)
         .expect_value(2, 0, 41))
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .offset(5)
         .expect_affected(1)
         .expect_first(51))
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .offset(6)
         .expect_affected(0))
#
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .records(0)
         .expect_affected(0))
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .records(3)
         .expect_affected(3)
         .expect_first(11)
         .expect_value(2, 0, 31)
         .expect_value(2, 2, 33))
run_test(basecase     # use basecase so we get the result set checks
         .records(10))


# Does OFFSET affect error reporting?
#
# This test exercises copy.splitlines
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .replace(3, '41|')
         .offset(3)
         .expect_error("Row 2: too few fields")  # note row 2, not 4!
         )
# This test exercises copy.parse_integer
run_test(TestCase("i INT, t TEXT, j INT", testdata)
         .replace(3, '4x1|bla|43')
         .offset(3)
         .expect_error("Row 2 column 1")  # note row 2, not 4!
         )
# These tests exercise copy.fixlines
run_test(TestCase("i INT, t TEXT", '1\n2\n3\n"4', quote='"')
         .offset(3)
         .expect_error("Row 2: unterminated quoted string"))
run_test(TestCase("i INT, t TEXT", '1\n2\n3\n4', raw=True)
         .offset(3)
         .expect_error("Row 2: unterminated line"))
