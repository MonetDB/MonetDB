#!/usr/bin/env python3

import os
import struct
from MonetDBtesting import tpymonetdb as pymonetdb


class MyDownloader(pymonetdb.Downloader):
    def __init__(self):
        self.data = {}

    def handle_download(self, download, filename, text_mode):
        assert text_mode == False
        br = download.binary_reader()
        self.data[filename] = br.read(1_000_000)


NROWS = 10

conn = pymonetdb.connect(
    database=os.getenv("TSTDB", 'demo'),
    port=int(os.getenv("MAPIPORT", '44001')))
downloader = MyDownloader()
conn.set_downloader(downloader)
conn.set_autocommit(True)

c = conn.cursor()
c.execute("DROP TABLE IF EXISTS foo")
c.execute("CREATE TABLE foo(i TINYINT, j TINYINT)")
c.execute("INSERT INTO foo SELECT value AS i, 10 * value AS j FROM sys.generate_series(%s, %s)", [1, NROWS + 1])

# This should work
downloader.data.clear()
c.execute("COPY SELECT i, j FROM foo INTO BINARY 'i', 'j' ON CLIENT")
expected_i = bytes([1,2,3,4,5,6,7,8,9,10])
actual_i = downloader.data['i']
assert actual_i == expected_i, "expected {expected_i!r}, found {value_i!r}"
expected_j = bytes([10,20,30,40,50,60,70,80,90,100])
actual_j = downloader.data['j']
assert actual_j == expected_j, "expected {expected_j!r}, found {value_j!r}"

# Too many file names
try:
    c.execute("COPY SELECT i, j FROM foo INTO BINARY 'i', 'j', 'x' ON CLIENT")
    assert False, "should have complained about the extra file name"
except pymonetdb.Error as e:
    if 'need 2 file names, got 3' not in str(e):
        raise

# Too few file names
try:
    c.execute("COPY SELECT i, j FROM foo INTO BINARY 'i' ON CLIENT")
    assert False, "should have complained about the missing file name"
except pymonetdb.Error as e:
    if 'need 2 file names, got 1' not in str(e):
        raise

conn.execute("DROP TABLE foo")
