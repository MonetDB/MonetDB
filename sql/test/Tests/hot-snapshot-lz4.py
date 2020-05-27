#!/usr/bin/env python3

# Test if the snapshot data is actually compressed.

import sys
import os
sys.path.append(os.getenv('TSTSRCDIR'))
from hot_snapshot_compression import check_compression

complaint = check_compression('lz4', b'\x04\x22\x4D\x18')
if complaint:
    print(complaint)
    sys.exit(1)
