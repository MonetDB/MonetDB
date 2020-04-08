#!/usr/bin/env python3

import bz2
import gzip
import hashlib
import json
import lz4.frame
import lzma
import os

import sys

BOM = b'\xEF\xBB\xBF'

# Note: this is a byte string, not a text string.
INPUT = gzip.open('1661-0.txt.gz', 'rb').read()

# Note: it uses DOS line endings.
# This is important because the streams library leaves those alone,
# even in text mode.
assert INPUT.find(b'\x0d\x0a') >= 0

# Contents of the catalog file.
DESCRIPTORS = []

def write(name, compression, content):
    filename = 'data/' + name + '.txt'
    if not compression:
        f = open(filename, 'wb')
    elif compression == 'gz':
        filename += '.gz'
        f = gzip.GzipFile(filename, 'wb', mtime=131875200, compresslevel=9)
    elif compression == 'bz2':
        filename += '.bz2'
        f = bz2.BZ2File(filename, 'wb', compresslevel=9)
    elif compression == 'xz':
        filename += '.xz'
        f = lzma.LZMAFile(filename, 'wb', preset=9)
    elif compression == 'lz4':
        filename += '.lz4'
        f = lz4.frame.LZ4FrameFile(filename, 'wb', compression_level=9)
    else:
        raise Exception("Unknown compression scheme: " + compression)

    f.write(content)
    f.close()

    # DIRTY HACK
    # For the time being the test script normalizes everything to unix
    # line endings before doing any comparisons.
    #
    # Here we make sure the reference data matches that
    content = content.replace(b'\r\n', b'\n')

    has_bom = content.startswith(BOM)
    without_bom = content[3:] if has_bom else content

    n = 5
    descr = dict(
        filename=os.path.basename(filename),
        has_bom = has_bom,
        size_without_bom = len(without_bom),
        md5 = hashlib.md5(content).hexdigest(),
        md5_without_bom = hashlib.md5(without_bom).hexdigest(),
        start_without_bom = str(without_bom[:n], 'ascii'),
        end_without_bom = str(without_bom[-n:], 'ascii'),
    )

    DESCRIPTORS.append(descr)


def write_all_compressions(name, content, limit):
    if limit:
        content = content[:limit]
    write(name, None, content)
    write(name, 'gz', content)
    write(name, 'bz2', content)
    write(name, 'xz', content)
    write(name, 'lz4', content)

def write_all(name, content, limit=None):
    write_all_compressions(name, content, limit)
    write_all_compressions(name + '_bom', BOM + content, limit)


# Whole file
write_all('sherlock', INPUT)

# Empty file
write_all('empty', b'')

# First 16 lines
head = b'\n'.join(INPUT.split(b'\n')[:16]) + b'\n'
write_all('small', head)

# Buffer size boundary cases
for base_size in [1024, 2048, 4096, 8192, 16384]:
    for delta in [-1, 0, 1]:
        size = base_size + delta
        write_all(f'block{size}', INPUT, size)

# Write the descriptor
json.dump(DESCRIPTORS, open('data/testcases.json', 'w'), indent=4)
