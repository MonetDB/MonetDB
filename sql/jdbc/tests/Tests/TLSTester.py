import os, sys
from subprocess import run, PIPE, CalledProcessError

PORT=os.environ['TST_TLSTESTERPORT']

cmd = [
    'java', 'TLSTester',
    # '-v',
    f'localhost:{PORT}',
    '-a', 'localhost.localdomain'
]
try:
    p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
    sys.stderr.write(p.stdout)
    sys.stderr.write(p.stderr)
except CalledProcessError as e:
    raise SystemExit(e.stderr)

