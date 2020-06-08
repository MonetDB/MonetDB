#!/usr/bin/env python3

# Test if the snapshot data is actually compressed.

import sys
import os
sys.path.append(os.getenv('TSTSRCDIR'))
from hot_snapshot import test_snapshot

test_snapshot('.gz', b'\x1F\x8B')
