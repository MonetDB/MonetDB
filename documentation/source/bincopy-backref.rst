*******************************************
Backref encoding for strings in COPY BINARY
*******************************************

With COPY BINARY, a string column is encoded as the concatenation of the
NUL-terminated strings. Nil is encoded as ``80 00``, that is,
byte value 0x80=128 followed by byte value 0. This is safe because
valid UTF-8 encodings never start with bytes in the ranges
0x80..0xBF or 0xF8..0xFF.

We have now extended the file format with a way to reuse earlier
entries in the file. This is useful for two reasons

1. It saves space because the backreference is almost always shorter
   than the string would have been

2. It saves time because inserting a string value into a BAT takes quite
   some effort compared to inserting for example an integer, but reusing
   a string that has been inserted before is about as cheap as inserting
   an integer.

The backreferences are always relative to the the current end of the
BAT, so the most recently inserted entry is referred to as 1, the one
before as 2, etc. We'll write this number as N. Note that N is never 0.

If N<=63, the backreference is encoded as the single byte 0x80+N.
It is not followed by a 0x00.

If N >= 64, N is written as N = N0 + 128*N1 + 128^2*N2 + .. +128^i*Ni .
The backreference is then encoded as::

    0x80 0x80+N0 .. 0x80+N(i-1) + Ni

For example, N = 500 = 116 + 3 * 128 is encoded as::

    0x80 0xF4 0x03

Again there is no trailing 00.

Sample Python code::

    def backref_encode(strings_or_nones):
        memory = {}
        prev_nil = -64
        for i, s in enumerate(strings_or_nones):
            if s is not None:
                prev = memory.get(s)
                memory[s] = i  # always prefer latest
                if prev is None:
                    # nul-terminated string
                    yield bytes(s, 'utf-8') + b'\x00'
                else:
                    delta = i - prev
                    if delta < 64:
                        # short backref
                        yield bytes([0x80 + delta])
                    else:
                        # long backref
                        buf = bytearray([0x80])
                        while delta > 0:
                            chunk = delta % 128
                            if chunk < delta:
                                chunk += 0x80  # not the last
                            buf.append(chunk)
                            delta = delta // 128
                        yield buf
            else:
                delta = i - prev_nil
                prev_nil = i
                if delta < 64:
                    # short backref to earlier nil
                    yield bytes([0x80 + delta])
                else:
                    # no space advantage, just encode the nil
                    yield b'\x80\x00'


    input = ['foo', None, None, 'foo']
    expected = b'foo\x00' + b'\x80\x00' + b'\x81' + b'\x83'
    actual = b''.join(backref_encode(input))
    assert actual == expected

    input = ['foo', None] + 500 * ['bar'] + ['foo', None]
    expected = (
        (b'foo\x00' + b'\x80\x00')
        + (b'bar\x00' + 499 * b'\x81')
        # 502 == 118 + 3 * 128; 118 + 128 == 0xF6
        + (b'\x80\xF6\x03' + b'\x80\x00')
    )
    actual = b''.join(backref_encode(input))
    assert actual == expected
