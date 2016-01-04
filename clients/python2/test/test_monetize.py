# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import unittest
from monetdb.sql.monetize import convert, monet_escape
from monetdb.exceptions import ProgrammingError


class TestMonetize(unittest.TestCase):
    def test_str_subclass(self):
        class StrSubClass(str):
            pass
        x = StrSubClass('test')
        csub = convert(x)
        cstr = convert('test')
        self.assertEqual(csub, cstr)

    def test_unknown_type(self):
        class Unknown:
            pass
        x = Unknown()
        self.assertRaises(ProgrammingError, convert, x)
