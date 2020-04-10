#!/usr/bin/env python3

import write_tests
import sys

filter = lambda f: f.endswith('.txt.xz')

if write_tests.all_tests(filter) != 0:
	sys.exit(1)
