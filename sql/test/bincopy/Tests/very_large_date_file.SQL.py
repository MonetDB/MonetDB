# -*- python -*-

import contextlib
import os
import sys
import tempfile
import time
import typing
import pymonetdb
import shutil

sys.stdout = sys.stderr

SCRATCH_DIR = os.getenv('TSTTRGDIR')
TSTDB = database = os.getenv("TSTDB")
MAPIPORT = int(os.getenv("MAPIPORT"))


def write_ones(count: int, f: typing.BinaryIO):
    block_size = 1024 * 1024
    block = block_size * b'\x01'
    while count > 0:
        chunk_size = min(count, len(block))
        written = f.write(block[:chunk_size])
        assert written > 0
        count -= written


class MyUploader(pymonetdb.Uploader):
    nbytes: int

    def __init__(self, nbytes: int):
        self.nbytes = nbytes

    def handle_upload(self, upload, filename, text_mode, skip_amount):
        assert not text_mode
        wr = upload.binary_writer()
        write_ones(self.nbytes, wr)


def test_large_upload(size: int, on_clause: str):
    print(f'#\n# TESTING with {size:_} bytes {on_clause}')

    with pymonetdb.connect(TSTDB, port=MAPIPORT) as conn, conn.cursor() as c:
        temp_file = None
        try:
            if on_clause == 'ON CLIENT':
                conn.set_uploader(MyUploader(size))
                from_clause = f"FROM 'virtual' ON CLIENT"
            elif on_clause == 'ON SERVER':
                temp_file = tempfile.NamedTemporaryFile(
                    mode='wb', dir=SCRATCH_DIR, delete=False)
                write_ones(size, temp_file)
                temp_file.close()
                quoted = "R'" + temp_file.name.replace("'", "''") + "'"
                from_clause = f'FROM {quoted} ON SERVER'
            else:
                raise Exception('invalid on_clause: ' + on_clause)

            def execute(s: str):
                print(f'# ' + s.replace('\n', ' '))
                t0 = time.time()
                c.execute(s)
                t1 = time.time()
                print(f'# -> took {t1-t0:.3f}s, {c.rowcount} affected rows')

            execute('DROP TABLE IF EXISTS foo')
            execute('CREATE TABLE foo(i DATE)')
            execute(f'COPY BINARY INTO foo {from_clause}')
            execute('SELECT COUNT(*) FROM foo')
            rowcount = c.fetchone()[0]
            expected = size // 4
            print(f'# expecting {expected:_} rows, found {rowcount:_}')
            assert rowcount == expected
        finally:
            if temp_file:
                os.unlink(temp_file.name)


GiB = 1024 * 1024 * 1024
assert shutil.disk_usage(SCRATCH_DIR).free >= 11 * GiB
test_large_upload(3 * GiB, 'ON CLIENT')
test_large_upload(3 * GiB, 'ON SERVER')
test_large_upload(5 * GiB, 'ON CLIENT')
test_large_upload(5 * GiB, 'ON SERVER')
