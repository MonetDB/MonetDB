#!/usr/bin/env python3

import sys
import os
sys.path.append(os.getenv('TSTSRCDIR'))
from bincopy_support import run_test
from bincopy_support import NULL_BLOBS, NULL_BLOBS_LE, NULL_BLOBS_BE

run_test('client', NULL_BLOBS)

run_test('client', NULL_BLOBS_LE)

run_test('client', NULL_BLOBS_BE)
