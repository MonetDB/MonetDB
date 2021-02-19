#!/usr/bin/env python3

import sys, os
sys.path.append(os.environ.get('TSTSRCDIR','.'))
import read_tests

read_tests.read_concatenated('gz')
