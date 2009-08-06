#!/usr/bin/env python

import unittest
import test_mapi

class TextTestRunnerNoTime(unittest.TextTestRunner):
    """A test runner class that displays results in textual form, but without time """
    def run(self, test):
        "Run the given test case or test suite."
        result = self._makeResult()
        test(result)
        result.printErrors()
        self.stream.writeln(result.separator2)
        run = result.testsRun
        self.stream.writeln("Ran %d test%s" % (run, run != 1 and "s" or ""))
        self.stream.writeln()
        if not result.wasSuccessful():
            self.stream.write("FAILED (")
            failed, errored = map(len, (result.failures, result.errors))
            if failed:
                self.stream.write("failures=%d" % failed)
            if errored:
                if failed: self.stream.write(", ")
                self.stream.write("errors=%d" % errored)
            self.stream.writeln(")")
        else:
            self.stream.writeln("OK")
        return result

if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(test_mapi.MapiConnectTests)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(test_mapi.MapiFunctionsTests)
    alltests = unittest.TestSuite([suite1, suite2])
    TextTestRunnerNoTime(verbosity=3).run(alltests)

