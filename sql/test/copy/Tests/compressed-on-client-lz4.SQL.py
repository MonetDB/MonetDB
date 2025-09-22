#!/usr/bin/env python3

from io import BytesIO
from compressed_on_client import test_compressed_onclient

from lz4.frame import LZ4FrameFile

def compress(data):
    bio = BytesIO()
    gz = LZ4FrameFile(bio, 'wb')
    gz.write(data)
    gz.close()
    return bio.getbuffer().tobytes()

def decompress(data):
    bio = BytesIO(data)
    return LZ4FrameFile(bio, 'rb').read(1_000_000)

test_compressed_onclient('lz4', compress, decompress)
