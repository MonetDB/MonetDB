import gzip
import os
import tempfile
import subprocess


# This SQL script redirects the output to a file (the %s).
# We will check that all output arrives there, even if it's a gzipped file.
SCRIPT = r"""
\>%s
SELECT 'Donald Knuth';
"""


with tempfile.TemporaryDirectory('mtest') as dir:
    outputfile = os.path.join(dir, 'output.txt.gz')
    inputfile = os.path.join(dir, 'input.sql')

    with open(inputfile, 'w') as f:
        f.write(SCRIPT % outputfile)

    subprocess.check_call([
        'mclient', '-i',
        '-p', os.environ['MAPIPORT'],
        inputfile,
    ])

    with gzip.open(outputfile, 'rt', encoding='utf-8') as f:
        content = f.read()

    assert 'Donald Knuth' in content
