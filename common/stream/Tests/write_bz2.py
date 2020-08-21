#!/usr/bin/env python3

import write_tests
import sys


def filter(f):
    return f.endswith('.txt.bz2')


if write_tests.all_tests(filter) != 0:
    sys.exit(1)
