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
        filename = self.tf.write(doc.content)

        test = f"read {openers} {self.name}"

        if not isinstance(openers, list):
            openers = [openers]

        cmd = ['streamcat', 'read', filename, *openers]
        results = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if results.returncode != 0 or results.stderr:
            print(f"TEST {test} FAILED: streamcat {cmd} returned with exit code {results.returncode}:\n{results.stderr or ''}")
            return False

        output = results.stdout or b""
        complaint = self.expected.verify(output)

        if complaint:
            print(f"TEST {test} FAILED: {complaint}")
            return False
        else:
            os.remove(filename)
            return True


def gen_docs():
    # We use a document with DOS line endings.
    # This way we can verify that rastream replaces them with \n
    # and rstream leaves them alone.
    text = Doc(testdata.SHERLOCK).to_dos().content

    # Whole file
    yield 'sherlock.txt', Doc(text)
    yield 'sherlock_bom.txt', Doc(text, prepend_bom=True)
    yield 'sherlock_cr.txt', Doc(text.replace(b'\n', b'\r'))

    # Empty file
    yield 'empty.txt', Doc(b'')
    yield 'empty_bom.txt', Doc(b'', prepend_bom=True)

    # First few lines
    small = b'\n'.join(text.split(b'\n')[:16]) + b'\n'
    yield 'small.txt', Doc(small)
    yield 'small_bom.txt', Doc(small, prepend_bom=True)

    # Buffer size boundary cases
    for base_size in [1024, 2048, 4096, 8192, 16384]:
        for delta in [-1, 0, 1]:
            size = base_size + delta
            yield f'block{size}.txt', Doc(text, truncate=size)
            yield f'block{size}_bom.txt', Doc(text, prepend_bom=True, truncate=size)

    # \r at end of first block, \n at start of next
    abc = (1023 * b'a') + b'\r\n' + (20 * b'b') + b'\r\n' + (20 * b'c')
    # word of wisdom: you have to test your tests
    assert abc[:1024].endswith(b'\r')
    assert abc[1024:].startswith(b'\n')
    yield 'crlf1024.txt', Doc(abc)

    yield 'brokenbom1.txt', Doc(BOM[:1] + small)
    yield 'brokenbom2.txt', Doc(BOM[:2] + small)


def gen_tests():
    for compr in testdata.COMPRESSIONS:
        for name, doc in gen_docs():
            yield TestCase(name, doc, compr, "rstream", doc)
            yield TestCase(name, doc, compr, "rastream", doc.without_bom().to_unix())
        for name, doc in gen_docs():
            if not name.startswith('sherlock') or name.startswith('empty'):
                continue
            yield TestCase(name, doc, compr, ["rstream", "blocksize:2"], doc)
            yield TestCase(name, doc, compr, ["rastream", "blocksize:2"], doc.without_bom().to_unix())
            yield TestCase(name, doc, compr, ["rstream", "blocksize:1"], doc)
            yield TestCase(name, doc, compr, ["rastream", "blocksize:1"], doc.without_bom().to_unix())
            yield TestCase(name, doc, compr, ["rstream", "blocksize:1000000"], doc)
            yield TestCase(name, doc, compr, ["rastream", "blocksize:1000000"], doc.without_bom().to_unix())


def all_tests(filename_filter):
    failures = 0
    for t in gen_tests():
        if not filename_filter(t.name):
            continue
        good = t.run()
        failures += not good

    return failures


def read_concatenated(ext):
    tf1 = TestFile("concatenated1", ext)
    tf1.write(b"hi")
    compressed_content = open(tf1.path(), "rb").read()

    tf2 = TestFile("concatenated2", ext)
    tf2.write_raw(compressed_content + compressed_content)

    cmd = ['streamcat', 'read', tf2.path(), "rstream"]
    results = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if results.returncode != 0 or results.stderr:
        raise SystemExit(f"Command {cmd} returned with exit code {results.returncode}:\n{results.stderr or ''}")

    output = results.stdout

    expected = b'hihi'
    if output != expected:
        raise SystemExit(f"Expected {expected!r}, got {output!r}")


if __name__ == "__main__":
    # generate test data for manual testing
    if len(sys.argv) == 1:
        for name, d in gen_docs():
            print(name)
    elif len(sys.argv) == 2:
        for name, d in gen_docs():
            if name == sys.argv[1]:
                sys.stdout.buffer.write(d.content)
    else:
        print("Usage: python3 read_tests.py [TESTDATANAME]", file=sys.stderr)
        sys.exit(1)

