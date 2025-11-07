# -*- python -*-


import os
import struct
import sys
import pymonetdb

TSTDB = database = os.getenv("TSTDB", 'tmpdb')
MAPIPORT = int(os.getenv("MAPIPORT", '50000'))


class MyUploader(pymonetdb.Uploader):
    def __init__(self, **data):
        self.data = data

    def handle_upload(self, upload, filename, text_mode, skip_amount):
        data = self.data[filename]
        w = upload.binary_writer()
        print(f'# sent    {filename}={data!r}')
        w.write(data)


class MyDownloader(pymonetdb.Downloader):
    def __init__(self):
        self.data = {}

    def handle_download(self, download, filename, text_mode):
        r = download.binary_reader()
        self.data[filename] = data = r.read(1000_000)
        print(f'# received {filename}={data!r}')

def test_compressed_onclient(algo_name, compress, decompress):
    old_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
        return run_the_tests(algo_name, compress, decompress)
    finally:
        sys.stdout = old_stdout




def run_the_tests(algo_name, compress, decompress):
    qalgo = f"'{algo_name}'" if algo_name else ''
    print(f'# testing ON {qalgo} CLIENT')
    print('#')
    print('# self-test')
    x = b'foo'
    print(f'# x={x}')
    y = compress(x)
    print(f'# y={y}')
    z = decompress(y)
    print(f'# z={z}')
    print('#')

    test_data = [(1, 'one'), (2, 'two'), (3, 'three')]
    csv_data = ''.join(f'{i}|{s}{os.linesep}' for i, s in test_data)
    bin_data = bytes(csv_data, 'utf-8')
    compressed_data = compress(bin_data)

    bin_col0 = struct.pack(f'<{len(test_data)}i', *(i for i, _ in test_data))
    bin_col1 =  b''.join(bytes(t, 'utf-8') + b'\x00' for _, t in test_data)
    compressed_col0 = compress(bin_col0)
    compressed_col1 = compress(bin_col1)

    with pymonetdb.connect(TSTDB, port=MAPIPORT) as conn, conn.cursor() as c:
        c.execute("DROP TABLE IF EXISTS foo")
        c.execute("CREATE TABLE foo(i INT, t TEXT)")

        uploader = MyUploader(
            csv=compressed_data,
            col0 = compressed_col0,
            col1 = compressed_col1,
        )
        downloader = MyDownloader()
        conn.set_uploader(uploader)
        conn.set_downloader(downloader)

        sql = f"COPY INTO foo FROM 'csv' ON {qalgo} CLIENT"
        print('#')
        print('#', sql)
        c.execute(sql)
        c.execute('SELECT * FROM foo')
        inserted = c.fetchall()
        print('# expect ', test_data)
        print('# got    ', inserted)
        assert inserted == test_data

        sql = f"COPY (SELECT * FROM foo) INTO 'csv' ON {qalgo} CLIENT USING DELIMITERS '|', E'\\n', ''"
        print('#')
        print('#', sql)
        c.execute(sql)
        retrieved = downloader.data['csv']
        decompressed = decompress(retrieved)
        print(f'# expect   {bin_data!r}')
        print(f'# got      {decompressed!r}')
        assert decompressed == bin_data

        c.execute('TRUNCATE foo')

        sql = f"COPY LITTLE ENDIAN BINARY INTO foo FROM 'col0', 'col1' ON {qalgo} CLIENT"
        print('#')
        print('#', sql)
        c.execute(sql)
        c.execute('SELECT * FROM foo')
        inserted = c.fetchall()
        print('# expect ', test_data)
        print('# got    ', inserted)
        assert inserted == test_data

        sql = f"COPY (SELECT * FROM foo) INTO LITTLE ENDIAN BINARY 'col0', 'col1' ON {qalgo} CLIENT"
        print('#')
        print('#', sql)
        c.execute(sql)
        raw0 = downloader.data['col0']
        raw1 = downloader.data['col1']
        decompressed0 = decompress(raw0)
        print(f'# expect   col0={bin_col0!r}')
        print(f'# got           {decompressed0!r}')
        assert decompressed0 == bin_col0
        decompressed1 = decompress(raw1)
        print(f'# expect   col1={bin_col1!r}')
        print(f'# got           {decompressed1!r}')
        assert decompressed1 == bin_col1
