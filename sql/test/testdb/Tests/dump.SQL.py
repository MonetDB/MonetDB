import sys

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
        else:
            # this is independent of geos, but use the same loop to sort the list of AS PARTITION IN
            txt = 'AS PARTITION IN ('
            idx = line.find(txt)
            if idx >= 0:
                strt = idx + len(txt)
                end = line.find(')', strt)
                vals = ', '.join(sorted(line[strt:end].split(', ')))
                output[i] = line[:strt] + vals + line[end:]

    stableout = '{}.stable.out'.format(sys.argv[1])
    with open(stableout, encoding='utf-8') as fil:
        stable = fil.readlines()
    import difflib
    for line in difflib.unified_diff(stable, output,
                                     fromfile='expected', tofile='received'):
        sys.stderr.write(line)
else:
    sys.stdout.writelines(out)
