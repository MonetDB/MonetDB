import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sqldump',
                    stdin=process.PIPE,
                    stdout=process.PIPE,
                    stderr=process.PIPE) as clt:
    out, err = clt.communicate()

if len(sys.argv) == 2 and sys.argv[1] in ('dump', 'dump-nogeom'):
    output = ''.join(out).splitlines(keepends=True)
    while len(output) > 0 and output[0].startswith('--'):
        del output[0]
    stableout = '{}.stable.out'.format(sys.argv[1])
    stable = open(stableout, encoding='utf-8').readlines()
    import difflib
    for line in difflib.unified_diff(stable, output, fromfile='test', tofile=stableout):
        sys.stderr.write(line)
else:
    sys.stdout.writelines(out)
