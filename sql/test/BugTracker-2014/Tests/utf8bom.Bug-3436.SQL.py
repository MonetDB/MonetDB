from __future__ import print_function

import os, sys, zipfile
try:
    from MonetDBtesting import process
except ImportError:
    import process

TSTTRGDIR = os.environ['TSTTRGDIR']

archive = 'utf8bom.Bug-3436.zip'

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

query = '''\
start transaction;
create table utf8bom (
    city string,
    id integer
);
copy into utf8bom from '%s' using delimiters ',','\\n','"';
select * from utf8bom order by id;
rollback;
'''

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write(query % os.path.join(TSTTRGDIR, 'utf8bom.csv').replace('\\', r'\\'));
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
