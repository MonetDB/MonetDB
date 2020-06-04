#!/usr/bin/env python3

# Test if the snapshot data is actually compressed.

import sys
import os
sys.path.append(os.getenv('TSTSRCDIR'))
from hot_snapshot_compression import check_compression

complaint = check_compression('bz2', b'BZh')
if complaint:
    print(complaint)
    sys.exit(1)
