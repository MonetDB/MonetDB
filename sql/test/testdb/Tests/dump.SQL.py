import sys
import os

try:
    from MonetDBtesting import process
except ImportError:
    import process

# this stuff is for geos pre 3.12
import re
geosre = re.compile(r'MULTIPOINT *\((?P<points>[^()]*)\)')
ptsre = re.compile(r'-?\d+ -?\d+')

with process.client('sqldump',
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE) as clt:
    out, err = clt.communicate()

if len(sys.argv) == 2 and sys.argv[1] in ('dump', 'dump-nogeom'):
    output = ''.join(out).splitlines(keepends=True)
    while len(output) > 0 and output[0].startswith('--'):
        del output[0]
    # this for loop is for geos pre 3.12
    for i in range(len(output)):
        line = output[i]
        res = geosre.search(line)
        if res is not None:
            points = ptsre.sub(r'(\g<0>)', res.group('points'))
            output[i] = line[:res.start('points')] + points + line[res.end('points'):]
    stableout = f'{sys.argv[1]}.stable.out'
    with open(stableout, encoding='utf-8') as fil:
        stable = fil.readlines()
    import difflib
    for line in difflib.unified_diff(stable, output,
                                     fromfile='expected', tofile='received'):
        sys.stderr.write(line)
    approve = os.getenv('MTEST_APPROVE')
    if approve:
        if approve == 'REPLACE':
            fn = os.path.join(os.getenv('TSTSRCDIR'),
                              f'{sys.argv[1]}.stable.out')
        else:
            fn = os.path.join(os.getenv('TSTTRGDIR'),
                              f'{sys.argv[1]}.stable.out.new')
        with open(fn, 'w') as f:
            f.writelines(out)
else:
    sys.stdout.writelines(out)

if err:
    sys.stderr.write(err)
