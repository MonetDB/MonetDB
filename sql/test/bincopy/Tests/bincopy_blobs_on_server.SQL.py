#!/usr/bin/env python3

import sys

import os
sys.path.append(os.getenv('TSTSRCDIR'))
from bincopy_support import run_test
from bincopy_support import NRECS, NULL_BLOBS, NULL_BLOBS_LE, NULL_BLOBS_BE

run_test('server', NULL_BLOBS, NRECS // 10)

run_test('server', NULL_BLOBS_LE, NRECS // 10)

run_test('server', NULL_BLOBS_BE, NRECS // 10)
