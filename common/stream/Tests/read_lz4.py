#!/usr/bin/env python3

import streamtests
import sys

filter = lambda f: f.endswith('.txt.lz4')

if not streamtests.all_read_tests(filter):
	sys.exit(1)
