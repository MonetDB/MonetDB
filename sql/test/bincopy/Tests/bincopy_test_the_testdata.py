#!/usr/bin/env python3

import math
import subprocess
import struct
import sys

def check_testdata(args, expected):
    cmd = [ 'bincopydata', *(str(a) for a in args)]
    output = subprocess.check_output(cmd, input=b"")

    if expected == output:
        return

    def show(bs):
        return " ".join(f"{c:02X}{' ' if i % 4 == 3 else ''}" for i, c in enumerate(bs))

    print(f'WHEN RUNNING: {cmd}')
    print(f"EXPECTED: {show(expected)}")
    print(f"GOT:      {show(output)}")

    sys.exit(1)

# The first few tests generate 12 shorts so the byte \x0A is included and
# we can see bincopydata does not accidentally perform line ending conversion.

# native endian by default
check_testdata(
    ['smallints', 12, '-'],
    struct.pack('=12h', *range(12))
)

# native endian, explicitly
check_testdata(
    ['--native-endian', 'smallints', 12, '-'],
    struct.pack('=12h', *range(12))
)

# big endian
check_testdata(
    ['--big-endian', 'smallints', 12, '-'],
    struct.pack('>12h', *range(12))
)

# little endian
check_testdata(
    ['--little-endian', 'smallints', 12, '-'],
    struct.pack('<12h', *range(12))
)



# Also try some floating point data.
# Should be IEEE 754 format.
# Admittedly very limited tests but better than nothing.

# native endian by default
check_testdata(
    ['floats', 12, '-'],
    struct.pack('=12f', *(0.5 + f for f in range(12)))
)
check_testdata(
    ['doubles', 12, '-'],
    struct.pack('=12d', *(0.5 + f for f in range(12)))
)

# native endian, explicitly
check_testdata(
    ['--native-endian', 'floats', 12, '-'],
    struct.pack('=12f', *(0.5 + f for f in range(12)))
)
check_testdata(
    ['--native-endian', 'doubles', 12, '-'],
    struct.pack('=12d', *(0.5 + f for f in range(12)))
)

# big endian
check_testdata(
    ['--big-endian', 'floats', 12, '-'],
    struct.pack('>12f', *(0.5 + f for f in range(12)))
)
check_testdata(
    ['--big-endian', 'doubles', 12, '-'],
    struct.pack('>12d', *(0.5 + f for f in range(12)))
)

# little endian
check_testdata(
    ['--little-endian', 'floats', 12, '-'],
    struct.pack('<12f', *(0.5 + f for f in range(12)))
)
check_testdata(
    ['--little-endian', 'doubles', 12, '-'],
    struct.pack('<12d', *(0.5 + f for f in range(12)))
)
