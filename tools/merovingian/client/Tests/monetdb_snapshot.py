#!/usr/bin/env python3

import locale
import os
import pipes
import re
import subprocess
import sys
import time

from monetdbd import MonetDBD, Runner

# MonetDBD.VERBOSE = True

gdk_farmdir = os.environ.get('TSTTRGDIR') or '/tmp/'
farmdir = os.path.join(gdk_farmdir, 'monetdbd-test')

with Runner(False) as run:

    def header(text):
        run.print()
        run.print('-- ', text)

    def note(*args, **kwargs):
        run.print('NOTE ', sep='', end='')
        run.print(*args, **kwargs)

    header('CREATE FARM')
    with MonetDBD(run, farmdir, set_snapdir=False) as m:

        header('CHECK SNAPDIR NOT SET')
        output = m.run_monetdbd('get', 'snapshotdir', output=True)
        assert '<unknown>' in output
        note("""'<unknown>' in output""")

        header('TRY TO CREATE')
        try:
            m.run_monetdb('snapshot', 'create', 'foo1')
            note("should have failed")
            assert False
        except subprocess.CalledProcessError as e:
            note("failed as expected")

        header('SET SNAPDIR')
        m.run_monetdbd('set', f'snapshotdir={m.snapdir}')

        header('TRY TO CREATE AGAIN')
        m.run_monetdb('snapshot', 'create', 'foo1')

        header('LIST')
        out = m.run_monetdb('snapshot', 'list', output=True)
        lines = out.rstrip().split('\n')
        assert len(lines) == 2 and lines[1].startswith('foo1@1 ')
        note("""len(lines) == 2 and lines[1].startswith('foo1@1 ')""")

        header('RESTORE')
        m.run_monetdb('snapshot', 'restore', 'foo1@1', 'foo_restored', output=True)
        out = m.run_mclient('-s', 'select * from t', '-fcsv', output=True, db='foo_restored')
        assert out.strip() == 'foo1'
        note("""output == 'foo1'""")

        header('DESTROY')
        m.run_monetdb('snapshot', 'destroy', '-f', 'foo1@1', output=True)
        out = m.run_monetdb('snapshot', 'list', output=True)
        lines = out.rstrip().split('\n')
        assert len(lines) == 1
        note("""len(lines) == 1""")
        m.run_monetdb('destroy', '-f', 'foo_restored')

        header('CHECK')
        m.run_monetdb('status', output=True)
        lines = out.rstrip().split('\n')
        assert len(lines) == 1
        note("""len(lines) == 1""")

        header('SNAPSHOT MULTI')
        m.run_monetdb('snapshot', 'create', 'foo*')
        out = m.run_monetdb('snapshot', 'list', output=True)
        lines = out.rstrip().split('\n')
        first_words = [ line.split(' ')[0] for line in lines ]
        run.print(first_words)
        assert first_words == ['name', 'foo1@1', 'foo2@1']
        #
        time.sleep(1.5) # ensure different snapshot names
        m.run_monetdb('snapshot', 'create', '*')
        out = m.run_monetdb('snapshot', 'list', output=True)
        lines = out.rstrip().split('\n')
        first_words = [ line.strip().split(' ')[0] for line in lines ]
        run.print(first_words)
        assert first_words == ['name', 'bar@1', 'foo1@1', '@2', 'foo2@1', '@2']

        header('RESTORE OVER EXISTING')
        m.run_monetdb('snapshot', 'restore', '-f', 'foo2@1', 'bar', output=True)
        out = m.run_mclient('-s', 'select * from t', '-fcsv', output=True, db='bar')
        assert out.strip() == 'foo2'
        note("""output == 'foo2'""")

        header('CUSTOM FILENAME')
        custom_name = os.path.join(m.snapdir, 'snap.tar')
        qcustom_name = pipes.quote(custom_name)
        m.run_monetdb('snapshot', 'create', '-t', qcustom_name, 'foo1')
        assert os.path.exists(custom_name)
        note("""os.path.exists(custom_name)""")
        m.run_monetdb('snapshot', 'restore', qcustom_name, 'foo99', output=True)
        out = m.run_mclient('-s', 'select * from t', '-fcsv', output=True, db='foo99')
        assert out.strip() == 'foo1'
        note("""output == 'foo1'""")

        header('DONE')
