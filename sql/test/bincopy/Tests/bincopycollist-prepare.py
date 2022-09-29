import struct
import os

f = open(os.path.join(os.getenv('TSTTRGDIR'), 'bincopyint.bin'), 'wb')

for i in range(10):
    f.write(struct.pack('@i', i))

f.close()
