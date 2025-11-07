#!/usr/bin/env python3

import sys
import os
sys.path.append(os.getenv('TSTSRCDIR'))
from io import BytesIO
from compressed_on_client import test_compressed_onclient

from lzma import LZMAFile

def compress(data):
    bio = BytesIO()
    gz = LZMAFile(bio, 'wb')
    gz.write(data)
    gz.close()
    return bio.getbuffer().tobytes()

def decompress(data):
    bio = BytesIO(data)
    return LZMAFile(bio, 'rb').read(1_000_000)

test_compressed_onclient('xz', compress, decompress)
