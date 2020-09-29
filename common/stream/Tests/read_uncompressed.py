#!/usr/bin/env python3

import sys, os
sys.path.append(os.environ.get('TSTSRCDIR','.'))
import read_tests


def filter(f):
    return f.endswith('.txt')


if read_tests.all_tests(filter) != 0:
    sys.exit(1)
