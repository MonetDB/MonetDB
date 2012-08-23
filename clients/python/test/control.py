import unittest
from monetdb.control import Control
from monetdb.exceptions import OperationalError
import logging
logging.basicConfig(level=logging.DEBUG)

database_prefix = 'controltest_'
database_name = database_prefix + 'other'
passphrase = 'testdb'

def do_without_fail(function):
    try:
        function()
    except OperationalError:
        pass

class TestManage(unittest.TestCase):
    def setUp(self):
        self.control = Control('localhost', 50000, passphrase)
        do_without_fail(lambda: self.control.stop(database_name))
        do_without_fail(lambda: self.control.destroy(database_name))
        self.control.create(database_name)

    def tearDown(self):
        do_without_fail(lambda: self.control.stop(database_name))
        do_without_fail(lambda: self.control.destroy(database_name))

    def testCreate(self):
        create_name = database_prefix + "create"
        do_without_fail(lambda: self.control.destroy(create_name))

        self.control.create(create_name)
        # can't create it again
        self.assertRaises(OperationalError, self.control.create, create_name)

        # cleanup
        do_without_fail(lambda: self.control.destroy(create_name))

    def testDestroy(self):
        destroy_name = database_prefix + "destroy"
        self.control.create(destroy_name)
        self.control.destroy(destroy_name)
        self.assertRaises(OperationalError, self.control.destroy, destroy_name)

    def testLock(self):
        do_without_fail(lambda: self.control.release(database_name))
        self.control.lock(database_name)
        self.assertRaises(OperationalError, self.control.lock, database_name)

    def testRelease(self):
        do_without_fail(lambda: self.control.release(database_name))
        do_without_fail(lambda: self.control.lock(database_name))
        self.assertTrue(self.control.release(database_name))
        self.assertRaises(OperationalError, self.control.release, database_name)

    def testStatus(self):
        self.control.status(database_name)

    def testStart(self):
        do_without_fail(lambda: self.control.stop(database_name))
        self.assertTrue(self.control.start(database_name))

    def testStop(self):
        do_without_fail(lambda: self.control.start(database_name))
        self.assertTrue(self.control.stop(database_name))

    def testKill(self):
        do_without_fail(lambda: self.control.start(database_name))
        self.assertTrue(self.control.kill(database_name))

    def testSet(self):
        property_ = "readonly"
        value = "yes"
        self.control.set(database_name, property_, value)
        properties = self.control.get(database_name)
        self.assertEqual(properties[property_], value)

    def testGet(self):
        properties = self.control.get(database_name)

    def testInherit(self):
        self.control.set(database_name, "readonly", "yes")
        self.assertTrue(self.control.inherit(database_name, "readonly"))
        self.assertFalse(self.control.get(database_name).has_key("readonly"))

    def testVersion(self):
        self.control.version(database_name)

if __name__ == '__main__':
    unittest.main()