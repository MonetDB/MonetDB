#!/usr/bin/env python3

import locale
import os
import re
import subprocess
import sys
import time

from monetdbd import MonetDBD

# MonetDBD.VERBOSE = True

gdk_farmdir = os.environ.get('GDK_FARMDIR') or '/tmp'
farmdir = os.path.join(gdk_farmdir, 'monetdbd-test')

def header(*args, **opts):
    if 'file' in opts:
        del opts['file']
    print(*['--', *args], flush=True, **opts)
    print(file=sys.stderr, flush=True, *['-  ', *args], **opts)

header('CREATE FARM')

# test that .napshotdir is not set by default
with MonetDBD(farmdir, set_snapdir=False) as m:

    header('CHECK SNAPDIR NOT SET')
    output = m.run_monetdbd('get', 'snapshotdir', output=True)
    assert('<unknown>' in output)

    header('CREATE')
    try:
        m.run_monetdb('snapshot', 'create', 'foo1')
    except subprocess.CalledProcessError as e:
        pass

    header('SET SNAPDIR')
    m.run_monetdbd('set', f'snapshotdir={m.qsnapdir}')

    header('CREATE')
    m.run_monetdb('snapshot', 'create', 'foo1')

    header('LIST')
    out = m.run_monetdb('snapshot', 'list', output=True)
    lines = out.rstrip().split('\n')
    assert(len(lines) == 2 and lines[1].startswith('foo1@1 '))

    header('RESTORE')
    m.run_monetdb('snapshot', 'restore', 'foo1@1', 'foo_restored', output=True)
    out = m.run_mclient('-s', 'select * from t', '-fcsv', output=True, db='foo_restored')
    assert(out.strip() == 'foo1')

    header('DESTROY')
    m.run_monetdb('snapshot', 'destroy', '-f', 'foo1@1', output=True)
    out = m.run_monetdb('snapshot', 'list', output=True)
    lines = out.rstrip().split('\n')
    assert(len(lines) == 1)
    m.run_monetdb('destroy', '-f', 'foo_restored')

    header('CHECK')
    m.run_monetdb('status', output=True)
    lines = out.rstrip().split('\n')
    assert(len(lines) == 1)

    header('SNAPSHOT MULTI')
    m.run_monetdb('snapshot', 'create', 'foo*')
    out = m.run_monetdb('snapshot', 'list', output=True)
    lines = out.rstrip().split('\n')
    first_words = [ line.split(' ')[0] for line in lines ]
    print(first_words)
    assert first_words == ['name', 'foo1@1', 'foo2@1']
    #
    time.sleep(1.5) # ensure different snapshot names
    m.run_monetdb('snapshot', 'create', '*')
    out = m.run_monetdb('snapshot', 'list', output=True)
    lines = out.rstrip().split('\n')
    first_words = [ line.strip().split(' ')[0] for line in lines ]
    print(first_words)
    assert first_words == ['name', 'bar@1', 'foo1@1', '@2', 'foo2@1', '@2']

    header('RESTORE OVER')
    m.run_monetdb('snapshot', 'restore', '-f', 'foo2@1', 'bar', output=True)
    out = m.run_mclient('-s', 'select * from t', '-fcsv', output=True, db='bar')
    assert(out.strip() == 'foo2')
