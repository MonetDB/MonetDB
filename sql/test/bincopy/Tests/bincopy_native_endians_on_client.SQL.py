#!/usr/bin/env python3

import sys
import os
sys.path.append(os.getenv('TSTSRCDIR'))
from bincopy_support import run_test
from bincopy_support import NATIVE_ENDIANS as testcode

run_test('client', testcode)
