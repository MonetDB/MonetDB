import struct
import os

target_dir = os.getenv('TSTTRGDIR')

testdata = dict(
    maxint=b'\x7f\xff\xff\xff\x7f\xff\xff\xff\x7f\xff\xff\xff',
    strings=b'one\x00two\x00three\x00',
    be456=b'\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\x06',
    be123=b'\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x03',
    bincopyint=struct.pack('10i', *range(10)),
)

for name, content in testdata.items():
    filename = os.path.join(target_dir, name + '.bin')
    with open(filename, 'wb') as f:
        f.write(content)
