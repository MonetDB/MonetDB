#

# Generating test files and verifying them after transmission.

import gzip

import os
import sys
import tempfile


LF = b'\n'
CRLF = b'\r\n'
BOM = b'\xEF\xBB\xBF'

SRCDIR = os.environ.get(
    'TSTSRCDIR',
    os.path.dirname(os.path.abspath(sys.argv[0]))
)
# The functions we pass this to will pick their own default if None:
TMPDIR = os.environ.get('TSTTRGDIR')

SHERLOCK = gzip.open(os.path.join(SRCDIR, '1661-0.txt.gz'), 'rb').read().replace(CRLF, LF)

COMPRESSIONS = [None, "gz", "bz2", "xz", "lz4"]

class Doc:
    """Contents to be read or written. The constructor has several options
    to make it easy to construct certain variants. The verify method tries
    to give a human friendly description of the differences found.
    """

    def __init__(self, content, prepend_bom=False, dos_line_endings=False, truncate=None):
        assert isinstance(content, bytes)
        if prepend_bom:
            content = BOM + content
        if dos_line_endings:
            content = content.replace(LF, CRLF)
        if truncate != None:
            assert truncate >= 0
            content = content[:truncate]
        self.content = content

    def with_bom(self):
        assert not self.content.startswith(BOM)
        new_content = BOM + self.content
        return Doc(new_content)

    def without_bom(self):
        if self.content.startswith(BOM):
            new_content = self.content[3:]
        else:
            new_content = self.content
        return Doc(new_content)

    def is_unix(self):
        return self.content.count(CRLF) == 0

    def is_dos(self):
        # every LF is part of a CRLF
        return self.content.count(LF) == self.content.count(CRLF)

    def to_unix(self):
        if self.is_dos():
            new_content = self.content.replace(CRLF, LF)
        else:
            new_content = self.content
        return Doc(new_content)

    def to_dos(self):
        if self.is_unix():
            new_content = self.content.replace(LF, CRLF)
        else:
            new_content = self.content
        return Doc(new_content)

    if os.linesep == '\r\n':
        to_platform = to_dos
    else:
        to_platform = to_unix

    def verify(self, text):
        """Return a textual description of the difference between our content
        and the given text, or None if identical"""

        content = self.content

        self_bom = content.startswith(BOM)
        text_bom = text.startswith(BOM)
        if text_bom and not self_bom:
            return "Unexpected BOM found!"
        elif self_bom and not text_bom:
            return "Expected BOM not found"

        if self_bom:
            text = text[3:]
            content = content[3:]
            skipped = 3
        else:
            skipped = 0

        n = 8

        self_start = content[:n]
        text_start = text[:n]
        if text_start != self_start:
            return f"Expected text to start with {repr(self_start)}, found {repr(text_start)}"

        for idx, (c1, c2) in enumerate(zip(content, text)):
            if c1 != c2:
                lines = content[:idx].split(LF)
                line = len(lines)
                col = len(lines[-1]) + 1
                ours = content[idx:][:n]
                theirs = text[idx:][:n]
                return f"Difference at byte={idx+skipped} line={line} col={col}: expected {repr(ours)}, found {repr(theirs)}"

        len_content = len(content)
        len_text = len(text)
        pos = min(len_content, len_text)
        if len_content < len_text:
            data = text[pos:][:n]
            return f"Size mismatch: unexpected {data} after {pos + skipped} bytes"
        elif len_content > len_text:
            data = content[pos:][:n]
            return f"Size mismatch: missing {data} after {pos + skipped} bytes"

        return None


# assert Doc(b"monetdb").content == b"monetdb"
# assert Doc(b"monetdb", prepend_bom=True).content == BOM + b"monetdb"
# assert Doc(b"monet\ndb", dos_line_endings=True).content == b"monet\r\ndb"
# assert Doc(b"monet\ndb", prepend_bom=True, dos_line_endings=True, truncate=9).content == BOM + b"monet\r"

# assert Doc(b"monetdb").with_bom().content == BOM + b"monetdb"
# assert Doc(b"monet\ndb").to_dos().content == b"monet\r\ndb"
# assert Doc(b"monet\r\ndb").to_unix().content == b"monet\ndb"
# assert Doc(b"").to_dos().content == b""
# assert Doc(b"").to_unix().content == b""

# def _expect_assert(fn):
#     try:
#         fn()
#     except AssertionError:
#         return
#     assert False and "expected assertion to fail"
# _expect_assert(lambda: Doc(b"monetdb", prepend_bom=True).with_bom())
# _expect_assert(lambda: Doc(b"monet\r\ndb").to_dos())
# _expect_assert(lambda: Doc(b"monet\ndb").to_unix())

# def _verify_verify(content, text, message_part):
#     if isinstance(content, Doc):
#         d = content
#     else:
#         d = Doc(content)
#     result = d.verify(text)
#     if message_part == None:
#         assert result == None, result
#     else:
#         assert result != None and message_part in result, result

# _verify_verify(b"monet", b"monet", None)
# _verify_verify(b"monet", BOM + b"db", "inserted")
# _verify_verify(BOM + b"monet", b"db", "disappeared")

# _verify_verify(BOM + b"monet", BOM + b"db", "to start with b'mo")

# _verify_verify(b"aap\nnoot\nmies", b"aap\nnootmies", "Difference at byte=8 line=2 col=5")
# _verify_verify(BOM + b"aap\nnoot\nmies", BOM + b"aap\nnootmies", "Difference at byte=11 line=2 col=5")

# _verify_verify(b"sherlock holmes", b"sherlock holmes2", "Size mismatch: unexpected b'2' after 15 bytes")
# _verify_verify(b"sherlock holmes2", b"sherlock holmes", "Size mismatch: missing b'2' after 15 bytes")



class TestFile:
    """A file on the file system to read/write test data
    """

    def __init__(self, name, compression):
        if compression and not name.endswith('.' + compression):
            name += '.' + compression
        self.name = name
        self.compression = compression
        self._path = None

    def path(self):
        if not self._path:
            prefix = "_streamtest_"
            suffix = "_" + self.name
            dir = TMPDIR or None
            h, p = tempfile.mkstemp(suffix, prefix, dir)
            os.close(h)
            os.remove(p)
            self._path = p
        return self._path

    def write(self, content):
        filename = self.path()
        fileobj = open(filename, 'wb')

        if not self.compression:
            f = fileobj
        elif self.compression == 'gz':
            f = gzip.GzipFile(filename, 'wb', fileobj=fileobj, mtime=131875200, compresslevel=1)
        elif self.compression == 'bz2':
            import bz2
            f = bz2.BZ2File(fileobj, 'wb', compresslevel=1)
        elif self.compression == 'xz':
            import lzma
            f = lzma.LZMAFile(fileobj, 'wb', preset=1)
        elif self.compression == 'lz4': # ok
            import lz4.frame
            f = lz4.frame.LZ4FrameFile(fileobj, 'wb', compression_level=1)
        else:
            raise Exception("Unknown compression scheme: " + self.compression)
        f.write(content)
        return filename


    def write_raw(self, content):
        filename = self.path()
        f = open(filename, 'wb')
        f.write(content)
        return filename

    def read(self):
        filename = self.path()
        if not self.compression:
            f = open(filename, 'rb')
        elif self.compression == 'gz':
            f = gzip.GzipFile(filename, 'rb', mtime=131875200)
        elif self.compression == 'bz2':
            import bz2
            f = bz2.BZ2File(filename, 'rb')
        elif self.compression == 'xz':
            import lzma
            f = lzma.LZMAFile(filename, 'rb')
        elif self.compression == 'lz4':
            import lz4.frame
            f = lz4.frame.LZ4FrameFile(filename, 'rb')
        else:
            raise Exception("Unknown compression scheme: " + self.compression)

        return f.read()





# test code
if __name__ == "__main__":
    d = TestFile("banana", "gz")
    p = d.path()
    print(f"path to doc {d.name} is {p}")
    p = d.path()
    print(f"path to doc {d.name} is {p}")
