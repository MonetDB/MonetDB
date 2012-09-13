import os, sys, zipfile

dest = os.environ['TSTSRCDIR']
archive = os.path.join(dest, 'example_mseed_files.zip')
if not os.path.exists(archive):
        sys.exit(1)

z = zipfile.ZipFile(archive)
z.extractall(dest)
fl = open(os.path.join(dest, 'example_mseed_file_list.txt'), 'w')
for f in z.namelist():
        fl.write(str(os.path.join(dest, f)) + '\n')
z.close()
