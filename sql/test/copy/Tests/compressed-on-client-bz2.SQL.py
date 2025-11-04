#!/usr/bin/env python3

import sys
import os
sys.path.append(os.getenv('TSTSRCDIR'))
from io import BytesIO
from compressed_on_client import test_compressed_onclient

from bz2 import BZ2File

def compress(data):
    bio = BytesIO()
    gz = BZ2File(bio, 'wb')
    gz.write(data)
    gz.close()
    return bio.getbuffer().tobytes()

def decompress(data):
    bio = BytesIO(data)
    return BZ2File(bio, 'rb').read(1_000_000)

test_compressed_onclient('bz2', compress, decompress)
