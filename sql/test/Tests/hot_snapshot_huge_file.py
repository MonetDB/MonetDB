# Check that hot snapshot deals correctly with very large member files, for
# example huge BATs or a huge WAL.
#
# The classical tar format stores the size of the member files as an 11 digit
# octal number so it cannot represent members >=8GiB.
#
# To represent larger files, hot snapshot switches to a gnu extension
# recognizable by the first byte of the size field being >=0x80.

from dataclasses import dataclass
import logging
import os
import struct
import sys
import tempfile

import lz4.frame
import pymonetdb

log_level = logging.DEBUG
log_format = '%(levelname)s:t=%(relativeCreated)d:func=%(funcName)s:line=%(lineno)d:%(message)s'
if '-v' in sys.argv:
    log_level = logging.INFO
logging.basicConfig(level=log_level, format=log_format)
logging.getLogger('pymonetdb').setLevel(logging.WARNING)

logging.addLevelName(logging.DEBUG, '#DEBUG')
logging.addLevelName(logging.INFO, '#INFO')

SCRATCH_PREFIX = os.getenv('TSTTRGDIR', None)


def human_size(size):
    unit = ''
    if size > 1500:
        unit = 'k'
        size /= 1024
    if size > 1500:
        unit = 'M'
        size /= 1024
    if size > 1500:
        unit = 'G'
        size /= 1024
    return f'{size:.1f}{unit}'


def open_compressed_tar(filename):
    if filename.endswith('.tar'):
        return open(filename, 'rb')
    if filename.endswith('.tar.lz4'):
        return lz4.frame.LZ4FrameFile(filename, 'rb')
    raise Exception(f"Don't know how to uncompress {filename}")


def generate_huge_table(conn, size_in_gib=8.1):
    size_in_bytes = int(size_in_gib * 1024 * 1024 * 1024)
    size_in_rows = int(size_in_bytes / 8)
    with conn.cursor() as c:
        c.execute("DROP TABLE IF EXISTS foo")
        conn.commit()
        logging.info(f'creating table with {size_in_rows} bigint rows ({human_size(8 * size_in_rows)} bytes)')

        c.execute("CREATE TABLE foo(i BIGINT)")
        c.execute("INSERT INTO foo VALUES (0)")
        n = 1

        while n < size_in_rows:
            todo = size_in_rows - n
            i = c.execute("INSERT INTO foo SELECT i FROM foo LIMIT %s", (todo,))
            logging.debug(f'inserted {i} rows')
            n += i

        c.execute("SELECT COUNT(*) FROM foo")
        row_count = c.fetchone()[0]
        assert row_count == size_in_rows, f"row count is {row_count}, expected {size_in_rows}"
        logging.info('inserted all rows, committing')
        conn.commit()
        logging.info('committed')


def create_tar_file(conn, path):
    with conn.cursor() as c:
        logging.info('starting hot_snapshot')
        c.execute('CALL sys.hot_snapshot(%s, 1)', [path])
        logging.info('finished hot_snapshot')
        size = os.stat(path).st_size
        logging.info(f'compressed tar file {path} has size {size} ({human_size(size)})')


@dataclass
class Member:
    name: str
    raw_size: bytes

    @property
    def size(self):
        if self.raw_size[0] < 0x80:
            field = self.raw_size.rstrip(b'\x00').rstrip(b' ')
            return int(field, 8)
        else:
            num = struct.unpack('>Q', m.raw_size[-8:])[0]
            return num ^ 0x8000_0000_0000_0000  # strip high bit


def parse_tar_file(f):
    block_size = 512
    chunk_size = 64 * 1024
    assert chunk_size >= block_size
    assert (chunk_size % block_size) == 0
    chunk = b''

    def extract(block, offset, size):
        base = block * block_size + offset
        return chunk[base:base + size]

    while True:
        chunk = f.read(chunk_size)
        if not chunk:
            break
        assert len(chunk) % block_size == 0
        for i in range(len(chunk) // block_size):
            ustar = extract(i, 257, 5)
            if ustar != b'ustar':
                continue
            raw_name = extract(i, 0, 100)
            name = str(raw_name.rstrip(b'\x00').rstrip(b' '), 'utf-8')
            size = extract(i, 124, 12)
            member = Member(name, size)
            yield member


if __name__ == "__main__":
    dbname = os.getenv('TSTDB', 'demo')
    port = int(os.getenv('MAPIPORT', '50000'))
    with pymonetdb.connect(dbname, port=port) as conn, tempfile.TemporaryDirectory(dir=SCRATCH_PREFIX) as dir:

        tar_path = os.path.join(dir, 'dump.tar')
        tar_path += '.lz4'
        logging.info(f'tar_path = {tar_path}')

        generate_huge_table(conn, 8.1)    # > 8GiB
        create_tar_file(conn, tar_path)

        member_count = 0
        huge_member_count = 0
        with open_compressed_tar(tar_path) as f:
            for m in parse_tar_file(f):
                member_count += 1
                if m.raw_size[0] >= 0x80:
                    size = m.size
                    logging.info(f'found huge member size={size}: {m}')
                    huge_member_count += 1
        logging.info(f'found {member_count} members')
        logging.info(f'found {huge_member_count} huge members')

        with conn.cursor() as c:
            logging.info('dropping the table')
            c.execute("DROP TABLE foo")
        conn.commit()

        assert huge_member_count == 1, f"expected 1 huge member file, not {huge_member_count}"

    logging.info('goodbye')

