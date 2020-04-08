#!/usr/bin/env python3

import streamtests
import sys

filter = lambda f: f.endswith('.txt')

if streamtests.all_read_tests(filter) != 0:
	sys.exit(1)
