#!/usr/bin/env python3

import testdata
from testdata import Doc, TestFile

import hashlib
import json
import os
import subprocess
import sys


BOM = b'\xEF\xBB\xBF'


class TestCase:
    def __init__(self, name, doc, compression, openers, expected):
        self.tf = TestFile(name, compression)
        self.name = self.tf.name
        self.doc = doc
        self.compression = compression
        self.openers = openers
        self.expected = expected

    def run(self):
        doc = self.doc
        openers = self.openers
        filename = self.tf.path()

        test = f"write {openers} {self.name}"

        if not isinstance(openers, list):
            openers = [openers]

        if os.path.exists(filename):
            os.remove(filename)

        cmd = ['streamcat', 'write', filename, *openers]
        results = subprocess.run(cmd, input=doc.content, stderr=subprocess.PIPE)
        if results.returncode != 0 or results.stderr:
            print(f"TEST {test} FAILED: streamcat {cmd} returned with exit code {results.returncode}:\n{results.stderr or ''}")
            return False

        if not os.path.exists(filename):
            print(f"TEST {test} FAILED: test failed to create file '{filename}'")
            return False

        # Trial run to rule out i/o errors
        open(filename, 'rb').read()

        try:
            output = self.tf.read()  # should decompress it
        except Exception as e:
            print(f"TEST {test} FAILED reading on file {filename}: {e}")
            return False

        complaint = self.expected.verify(output)

        if complaint:
            print(f"TEST {test} FAILED: {complaint}")
            return False
        else:
            os.remove(filename)
            return True



def gen_docs():
    text = testdata.SHERLOCK
    assert Doc(text).is_unix()

    # Whole file
    yield 'sherlock.txt', Doc(text)

    # Empty file
    yield 'empty.txt', Doc(b'')

    # First 16 lines
    head = b'\n'.join(text.split(b'\n')[:16]) + b'\n'
    yield 'small.txt', Doc(head)

    # Buffer size boundary cases
    for base_size in [1024, 2048, 4096, 8192, 16384]:
        for delta in [-1, 0, 1]:
            size = base_size + delta
            yield f'block{size}.txt', Doc(text, truncate=size)


def gen_tests():
    sherlock = Doc(testdata.SHERLOCK)
    for compr in testdata.COMPRESSIONS:
        for name, doc in gen_docs():
            yield TestCase(name, doc, compr, "wstream", doc)
            yield TestCase(name, doc, compr, "wastream", doc.to_platform())
        for name, doc in gen_docs():
            if not name.startswith('sherlock') or name.startswith('empty'):
                continue
            yield TestCase(name, doc, compr, ["wstream", "blocksize:2"], doc)
            yield TestCase(name, doc, compr, ["wastream", "blocksize:2"], doc.to_platform())
            yield TestCase(name, doc, compr, ["wstream", "blocksize:1000000"], doc)
            yield TestCase(name, doc, compr, ["wastream", "blocksize:1000000"], doc.to_platform())
        for level in ['data', 'all']:
            yield TestCase(f'flush{level}.txt', sherlock, compr, ["wstream", "blocksize:1024", f"flush:{level}"], sherlock)


def all_tests(filename_filter):
    failures = 0
    for t in gen_tests():
        if not filename_filter(t.name):
            continue
        good = t.run()
        failures += not good

    return failures



if __name__ == "__main__":
    # generate test data for manual testing
    if len(sys.argv) == 1:
        for d in gen_docs():
            print(d.name)
    elif len(sys.argv) == 2:
        for d in gen_docs():
            if d.name == sys.argv[1]:
                d.write(sys.stdout.buffer)
    else:
        print("Usage: python3 read_tests.py [TESTDATANAME]", file=sys.stderr)
        sys.exit(1)
