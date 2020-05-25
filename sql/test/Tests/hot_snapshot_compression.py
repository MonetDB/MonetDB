#!/usr/bin/env python3

# Helper module for hot_snapshot_{gz,bz2,xz,lz4}.py

import tempfile
import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process


TSTTRGBASE = os.environ['TSTTRGBASE']


def check_compression(extension, expected_first_bytes):
    if not extension.startswith('.'):
        extension = '.' + extension
    # destfile = os.path.join(TSTTRGBASE, 'hot-snapshot.tar' + extension)
    h, destfile = tempfile.mkstemp('hot_snapshot.tar' + extension)
    os.close(h)
    with process.client('sql', stdin = process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
        c.stdin.write(f"CALL sys.hot_snapshot(r'{destfile}');")
        out, err = c.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
        if c.returncode:
            return f"Client exited with status {c.returncode}"

    contents = open(destfile, "rb").read()
    first_bytes = contents[:len(expected_first_bytes)]
    if first_bytes != expected_first_bytes:
        return f"Content validation failed.\nFile: {destfile}\nFirst bytes: {repr(first_bytes)},\nExpected:    {repr(expected_first_bytes)}"

    os.remove(destfile)
    return None
