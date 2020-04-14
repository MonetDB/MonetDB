#!/usr/bin/env python3

import read_tests
import sys


def filter(f):
    return f.endswith('.txt.gz')


if read_tests.all_tests(filter) != 0:
    sys.exit(1)
