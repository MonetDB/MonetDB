#

# Generating test files and verifying them after transmission.

import bz2
import gzip
import lz4.frame
import lzma

import os
import sys
import tempfile


BOM = b'\xEF\xBB\xBF'
SRCDIR = os.environ.get(
    'TSTSRCDIR',
    os.path.dirname(os.path.abspath(sys.argv[0]))
)
TMPDIR = os.environ.get('TSTTRGDIR')

# Often used for testfile contents. Uses DOS line endings, which is
# important because we can check whether they are transformed or left alone.
TESTDATA = os.path.join(SRCDIR, '1661-0.txt.gz')
SHERLOCK = gzip.open(TESTDATA, 'rb').read()
assert SHERLOCK.find(b'\x0d\x0a') >= 0


class Doc:
    def __init__(self, name, content, length_limit=None, compression=None):
        if length_limit:
            content = content[:length_limit]

        has_bom = content.startswith(BOM)
        if has_bom:
            normalized = content[3:]
        else:
            normalized = content

        # TEMPORARY: UNTIL WE GET OUR LINE ENDING STORY STRAIGHT.
        # (Method .verify has a corresponding hack)
        normalized = normalized.replace(b'\r\n', b'\n')

        self.name = name
        self.has_bom = has_bom
        self.compression = compression
        self.content = content  # for output
        self.normalized = normalized  # for checking

        if length_limit != None:
            self.content = self.content[:length_limit]

    # not sure if this is the right API
    def write(self, filename):
        if not self.compression:
            f = open(filename, 'wb')
        elif self.compression == 'gz':
            f = gzip.GzipFile(filename, 'wb', mtime=131875200, compresslevel=1)
        elif self.compression == 'bz2':
            f = bz2.BZ2File(filename, 'wb', compresslevel=1)
        elif self.compression == 'xz':
            f = lzma.LZMAFile(filename, 'wb', preset=1)
        elif self.compression == 'lz4':
            f = lz4.frame.LZ4FrameFile(filename, 'wb', compression_level=1)
        else:
            raise Exception("Unknown compression scheme: " + self.compression)
        f.write(self.content)
        f.close()
        return filename

    def write_tmp(self, dir=None):
        prefix = "_streamtest_"
        suffix = "_" + self.name
        dir = dir or TMPDIR or None
        h, p = tempfile.mkstemp(suffix, prefix, dir)
        os.close(h)
        return self.write(p)

    def verify(self, text, text_mode):
        # TEMPORARY: UNTIL WE GET OUR LINE ENDING STORY STRAIGHT.
        # See also the corresponding line in .__init__()
        text = text.replace(b'\r\n', b'\n')

        bom_found = text.startswith(BOM)

        # | HAS_BOM | TEXT_MODE | BOM_FOUND | RESULT                               |
        # |---------+-----------+-----------+--------------------------------------|
        # | False   | *         | False     | OK                                   |
        # | False   | *         | True      | Somehow, a BOM was inserted!         |
        # | True    | False     | False     | The BOM should not have been removed |
        # | True    | False     | True      | OK                                   |
        # | True    | True      | False     | OK                                   |
        # | True    | True      | True      | The BOM should have been removed     |
        if not self.has_bom:
            if bom_found:
                return "Somehow, a BOM was inserted!"
        if self.has_bom:
            if text_mode and bom_found:
                return "In text mode, the BOM should have been removed"
            if not text_mode and not bom_found:
                return "In binary mode, the BOM should not have been removed"

        if bom_found:
            text = text[len(BOM):]

        n = 8
        text_start = text[:n]
        text_end = text[-n:]
        ref_start = self.normalized[:n]
        ref_end = self.normalized[-n:]

        if text_start != ref_start:
            return f"Expected text to start with {repr(ref_start)}, found {repr(text_start)}"
        if text_end != ref_end:
            return f"Expected text to end with {repr(ref_end)}, found {repr(text_end)}"

        if len(text) != len(self.normalized):
            return f"Size mismatch: found {len(text)} bytes, expected {len(self.normalized)}"

        if text != self.normalized:
            idx = 0
            line = 1
            col = 1
            for (actual, ref) in zip(text, self.normalized):
                if actual != ref:
                    n = 8
                    expected = self.normalized[idx:][:8]
                    got = text[idx:][:8]
                    return f"Difference at byte {idx} line {line} col {col}: expected {repr(expected)}, got {repr(got)}"
                assert False and "unreachable"

        return None


# test code
if __name__ == "__main__":
    d = Doc("banana")
    p = d.write_tmp()
    print(f"path to doc {d.name} is {p}")