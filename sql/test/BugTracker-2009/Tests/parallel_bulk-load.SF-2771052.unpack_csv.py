from __future__ import print_function

import zipfile, os

archive = 'parallel_bulk-load.SF-2771052.unpack_csv.zip'

def mkpardir(path):
    i = path.find('/')
    d = ''
    while i >= 0:
        d = os.path.join(d, path[:i])
        if not os.path.exists(d):
            os.mkdir(d)
        path = path[i + 1:]
        i = path.find('/')

z = zipfile.ZipFile(archive)
print('Archive:  %s' % archive)
for name in z.namelist():
    print('  inflating: %s' % name)
    mkpardir(name)
    data = z.read(name)
    f = open(name, 'wb')
    f.write(data)
    f.close()
