# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2015 MonetDB B.V.
# All Rights Reserved.

import unittest
import monetdb.sql.pythonize

class TestPythonize(unittest.TestCase):
    def test_Binary(self):
        input1 = ''.join([chr(i) for i in range(256)])
        output1 = ''.join(["%02X" % i for i in range(256)])
        result1 = monetdb.sql.pythonize.Binary(input1)
        self.assertEqual(output1, result1)

        input2 = '\tdharma'
        output2 = '09646861726D61'
        result2 = monetdb.sql.pythonize.Binary(input2)
        self.assertEqual(output2, result2)

