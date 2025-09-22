#!/usr/bin/env python3

from io import BytesIO
from compressed_on_client import test_compressed_onclient

from gzip import GzipFile

def compress(data):
    bio = BytesIO()
    gz = GzipFile(mode='wb', fileobj=bio)
    gz.write(data)
    gz.close()
    return bio.getbuffer().tobytes()

def decompress(data):
    bio = BytesIO(data)
    return GzipFile(mode='rb', fileobj=bio).read(1_000_000)

test_compressed_onclient('gz', compress, decompress)
