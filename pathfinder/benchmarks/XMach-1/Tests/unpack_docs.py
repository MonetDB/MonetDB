import zipfile

archive = 'unpack_docs.zip'

z = zipfile.ZipFile(archive)
print 'Archive:  %s' % archive
for name in z.namelist():
    print '  inflating: %s' % name
    data = z.read(name)
    f = open(name, 'wb')
    f.write(data)
    f.close()
