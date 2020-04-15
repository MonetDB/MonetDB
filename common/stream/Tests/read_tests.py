#!/usr/bin/env python3

import testdata

import hashlib
import json
import os
import subprocess
import sys


BOM = b'\xEF\xBB\xBF'


def gen_compr_variants(name, content, limit):
    yield testdata.Doc(name, content, limit, None)
    yield testdata.Doc(name + ".gz", content, limit, "gz")
    yield testdata.Doc(name + ".bz2", content, limit, "bz2")
    yield testdata.Doc(name + ".xz", content, limit, "xz")
    yield testdata.Doc(name + ".lz4", content, limit, "lz4")


def gen_bom_compr_variants(name, content, limit):
    yield from gen_compr_variants(name + ".txt", content, limit)
    yield from gen_compr_variants(name + "_bom.txt", BOM + content, limit)


def gen_docs():
    input = testdata.SHERLOCK

    # Whole file
    yield from gen_bom_compr_variants('sherlock', input, None)

    # Empty file
    yield from gen_bom_compr_variants('empty', b'', None)

    # First 16 lines
    head = b'\n'.join(input.split(b'\n')[:16]) + b'\n'
    yield from gen_bom_compr_variants('small', head, None)

    # Buffer size boundary cases
    for base_size in [1024, 2048, 4096, 8192, 16384]:
        for delta in [-1, 0, 1]:
            size = base_size + delta
            yield from gen_bom_compr_variants(f'block{size}', input, size)

    # \r at end of first block, \n at start of next
    head = (1023 * b'a') + b'\r\n' + (20 * b'b') + b'\r\n' + (20 * b'c')
    # word of wisdom: you have to test your tests
    assert head[:1024].endswith(b'\r')
    assert head[1024:].startswith(b'\n')
    yield from gen_compr_variants('crlf1024.txt', head, None)

def test_read(opener, text_mode, doc):
    filename = doc.write_tmp()
    test = f"read {opener} {doc.name}"

    print()

    cmd = ['streamcat', 'read', filename, opener]
    results = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if results.returncode != 0 or results.stderr:
        print(
            f"Test {test}: streamcat returned with exit code {results.returncode}:\n{results.stderr or ''}")
        return False

    output = results.stdout or b""
    complaint = doc.verify(output, text_mode)

    if complaint:
        print(f"Test {test} failed: {complaint}")
        return False
    else:
        print(f"Test {test} OK")
        os.remove(filename)
        return True


def test_reads(doc):
    failures = 0

    # rstream does not strip BOM
    failures += not test_read('rstream', False, doc)

    # rastream does strip the BOM
    failures += not test_read('rastream', True, doc)

    return failures


def all_tests(filename_filter):
    failures = 0
    for d in gen_docs():
        if not filename_filter(d.name):
            continue
        failures += test_reads(d)

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

