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

