#!/usr/bin/env python3

# Test if the snapshot data is actually compressed.

from hot_snapshot_compression import check_compression

import sys

complaint = check_compression('bz2', b'BZh')
if complaint:
    print(complaint)
    sys.exit(1)
