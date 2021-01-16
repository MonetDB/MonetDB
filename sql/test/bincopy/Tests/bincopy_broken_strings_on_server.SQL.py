#!/usr/bin/env python3

from bincopy_support import run_test
from bincopy_support import BROKEN_STRINGS as testcode

run_test('server', testcode)
