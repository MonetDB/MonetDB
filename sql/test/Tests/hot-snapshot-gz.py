#!/usr/bin/env python3

# Test if the snapshot data is actually compressed.

import sys
import os
sys.path.append(os.getenv('TSTSRCDIR'))
from hot_snapshot_compression import check_compression

complaint = check_compression('gz', b'\x1F\x8B')
if complaint:
    print(complaint)
    sys.exit(1)
