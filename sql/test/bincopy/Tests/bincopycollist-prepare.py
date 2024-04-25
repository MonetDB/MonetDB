import struct
import os

target_dir = os.getenv('TSTTRGDIR')


MAXINT32 = 0x7FFF_FFFF


def write_data(name, format, *items):
    if name.endswith('.be'):
        assert '>' in format
    if name.endswith('.le'):
        assert '<' in format
    if format:
        data = struct.pack(format, *items)
    else:
        data = b''
        for item in items:
            data += item
            data += b'\0'
    filename = os.path.join(target_dir, name)
    with open(filename, 'wb') as f:
        f.write(data)


write_data('123.be', '>3i', 1, 2, 3)
write_data('123.le', '<3i', 1, 2, 3)

write_data('456.be', '>3i', 4, 5, 6)
write_data('bincopyint.bin', '=10i', *range(10))

write_data('maxint.be', '>3i', MAXINT32, MAXINT32, MAXINT32)
write_data('maxint.le', '<3i', MAXINT32, MAXINT32, MAXINT32)

write_data('strings.bin', '', b'one', b'two', b'three')

