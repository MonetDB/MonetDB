import unittest
from monetdb.sql.monetize import convert, monet_escape
from monetdb.exceptions import ProgrammingError


class TestMonetize(unittest.TestCase):
    def test_str_subclass(self):
        class StrSubClass(str):
            pass
        x = StrSubClass('test')
        func = convert(x)
        self.assertEqual(func, monet_escape)

    def test_unknown_type(self):
        class Unknown:
            pass
        x = Unknown()
        self.assertRaises(ProgrammingError, convert, x)
