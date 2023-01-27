#!/usr/bin/env python3

import array
import datetime
from io import StringIO
import os
import sys
from typing import Any, List, Optional
import pymonetdb


class BitsUploader(pymonetdb.Uploader):
    def __init__(self, bits: bytes):
        self.bits = bits

    def handle_upload(self, upload: pymonetdb.Upload, filename: str, text_mode: bool, skip_amount: int):
        w = upload.binary_writer()
        print(f"upload  {self.bits}")
        w.write(self.bits)


class TestCase:
    sqltype: str
    little_endian_bits: bytes
    big_endian_bits: bytes
    expected: List[Any]

    def __init__(self,
                 sqltype: str, byteswap_size: int, expected: List[Any],
                 little_endian_bits: bytes,
                 big_endian_bits: Optional[bytes] = None):
        self.sqltype = sqltype
        self.little_endian_bits = little_endian_bits
        self.expected = expected
        if big_endian_bits:
            self.big_endian_bits = big_endian_bits
        elif byteswap_size > 0:
            self.big_endian_bits = byteswap(little_endian_bits, byteswap_size)
        else:
            self.big_endian_bits = None

    def run(self, conn: pymonetdb.Connection, big_endian=False):
        if self.sqltype not in SUPPORTED_TYPES:
            print(f"SKIP    {self.sqltype}")
            print()
            return
        c = conn.cursor()
        bits = self.big_endian_bits if big_endian else self.little_endian_bits
        uploader = BitsUploader(bits)
        conn.set_uploader(uploader)
        self.execute(c, f"DROP TABLE IF EXISTS foo")
        self.execute(c, f"CREATE TABLE foo(x {self.sqltype})")
        endian = 'BIG ENDIAN' if big_endian else 'LITTLE ENDIAN'
        copy_stmt = f"COPY {endian} BINARY INTO foo FROM '{self.sqltype}' ON CLIENT"
        self.execute(c, copy_stmt)
        self.execute(c, "SELECT * FROM foo")
        received = [row[0] for row in c.fetchall()]
        print(f"expect  {self.expected!r}")
        print(f"receive {received!r}")
        c.close()
        print()
        assert received == self.expected

    def execute(self, c: pymonetdb.sql.cursors.Cursor, query: str):
        print(f"execute {query}")
        return c.execute(query)


def byteswap(bits: bytes, size: int) -> bytes:
    if size == 16:
        return byteswap_huge(bits)
    for code in 'bhilq':
        arr = array.array(code)
        if arr.itemsize == size:
            break
    else:
        raise Exception(f"No array type code for size {size}")
    arr.frombytes(bits)
    arr.byteswap()
    return arr.tobytes()


def byteswap_huge(bits: bytes):
    partially_swapped = byteswap(bits, 8)
    result = b''
    for i in range(0, len(partially_swapped), 16):
        result += partially_swapped[i + 8:i + 16]
        result += partially_swapped[i + 0:i + 8]
    return result


def f(n):
    """Convert 8 to 127, 16 to 32767, etc."""
    return (1 << (n - 1)) - 1


SUPPORTED_TYPES = None    # will be set below

TEST_CASES = [
    TestCase('text', 0,
             ['A', None],
             b'A\x00\x80\x00'),
    TestCase('clob', 0,
             ['A', None],
             b'A\x00\x80\x00'),
    TestCase('varchar(7)', 0,
             ['A', None],
             b'A\x00\x80\x00'),

    TestCase('tinyint', 1,
             [0, 1, -1, 127, -127, None],
             b'\x00\x01\xFF\x7F\x81\x80'),
    TestCase('smallint', 2,
             [0, 1, -1, f(16), -f(16), None],
             b'\x00\x00'
             b'\x01\x00'
             b'\xFF\xFF'
             b'\xFF\x7F'
             b'\x01\x80'
             b'\x00\x80'),
    TestCase('int', 4,
             [0, 1, -1, f(32), -f(32), None],
             b'\x00\x00\x00\x00'
             b'\x01\x00\x00\x00'
             b'\xFF\xFF\xFF\xFF'
             b'\xFF\xFF\xFF\x7F'
             b'\x01\x00\x00\x80'
             b'\x00\x00\x00\x80'),
    TestCase('bigint', 8,
             [0, 1, -1, f(64), -f(64), None],
             b'\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x01\x00\x00\x00\x00\x00\x00\x00'
             b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'
             b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F'
             b'\x01\x00\x00\x00\x00\x00\x00\x80'
             b'\x00\x00\x00\x00\x00\x00\x00\x80'),
    TestCase('hugeint', 16,
             [0, 1, -1, f(128), -f(128), None],
             b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'
             b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F'
             b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80'
             b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80'),

    TestCase('boolean', 1,
             [False, True, None],
             b'\x00\x01\x80'),

    TestCase('real', 4,
             [0.0, 1.0, None],
             b'\x00\x00\x00\x00'
             b'\x00\x00\x80\x3F'
             b'\xFF\xFF\xFF\x7F'),
    TestCase('double', 8,
             [0.0, 1.0, None],
             b'\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00\x00\x00\xF0\x3F'
             b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F'),
    TestCase('float(24)', 4,
             [0.0, 1.0, None],
             b'\x00\x00\x00\x00'
             b'\x00\x00\x80\x3F'
             b'\xFF\xFF\xFF\x7F'),
    TestCase('float(53)', 8,
             [0.0, 1.0, None],
             b'\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00\x00\x00\xF0\x3F'
             b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F'),

    TestCase('date', 0,
             [datetime.date(2015, 2, 14), None],
             # 2015 = 0x07DF, 14 = 0xE
             little_endian_bits=(
                 b'\x0E\x02\xDF\x07'
                 b'\x00\x00\x00\x00'
             ),
             big_endian_bits=(
                 b'\x0E\x02\x07\xDF'
                 b'\x00\x00\x00\x00'
             )),

    TestCase('time(3)', 0,
             [datetime.time(12, 34, 56, 789000), None],
             # 12 = 0xC, 34 = 0x22, 56 = 0x38, 789000 = 0x0C0A08
             little_endian_bits=(
                 b'\x08\x0A\x0C\x00\x38\x22\x0C\x00'
                 b'\x00\x00\x00\x00\x40\x00\x00\x00'
             ),
             big_endian_bits=(
                 b'\x00\x0C\x0A\x08\x38\x22\x0C\x00'
                 b'\x00\x00\x00\x00\x40\x00\x00\x00'
             )),

    TestCase('timestamp', 0,
             [datetime.datetime(2015, 2, 14, 12, 34, 56, 789000), None],
             little_endian_bits=(
                 b'\x08\x0A\x0C\x00\x38\x22\x0C\x00'
                 b'\x0E\x02\xDF\x07'
                 b'\x00\x00\x00\x00\x40\x00\x00\x00'
                 b'\x00\x00\x00\x00'
             ),
             big_endian_bits=(
                 b'\x00\x0C\x0A\x08\x38\x22\x0C\x00'
                 b'\x0E\x02\x07\xDF'
                 b'\x00\x00\x00\x00\x40\x00\x00\x00'
                 b'\x00\x00\x00\x00'
             )),


    # uuid
]


if __name__ == "__main__":
    sys.stdout = StringIO()
    try:
        conn = pymonetdb.connect(
            database=os.getenv("TSTDB"),
            port=int(os.getenv("MAPIPORT")))
        c = conn.cursor()
        c.execute("SELECT sqlname FROM sys.types")
        SUPPORTED_TYPES = set(row[0] for row in c.fetchall())
        for case in TEST_CASES:
            case.run(conn, False)
            if case.big_endian_bits is not None:
                try:
                    case.run(conn, True)
                except:
                    print(f"BIG endian '{case.sqltype}' fails while LITTLE endian succeeds!")
                    raise
    except:
        if hasattr(sys.stdout, 'getvalue'):
            output = sys.stdout.getvalue()
            print('----- STDOUT: -----', file=sys.stderr)
            print(output, file=sys.stderr)
            print('----- END STDOUT -----', file=sys.stderr)
        raise
