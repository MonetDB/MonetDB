#!/usr/bin/env python3

import sys, os
sys.path.append(os.environ.get('TSTSRCDIR','.'))
from testdata import Doc, TestFile

import subprocess
import tempfile


def run_streamcat(text, enc):
    content = bytes(text, 'utf-8')
    name = f'write_iconv_{enc}.txt'

    tf = TestFile(name, None)
    filename = tf.path()

    cmd = ['streamcat', 'write', filename, 'wstream', f'iconv:{enc}']

    print(f"Input UTF-8 encoded is {repr(content)}")
    # print(cmd)
    proc = subprocess.run(cmd, input=content)
    if proc.returncode != 0:
        print(f"{cmd} exited with status {proc.returncode}", file=sys.stderr)
        sys.exit(1)
    output = tf.read()
    os.remove(filename)
    print(f"Output is {repr(output)}")
    print()


text = "MøNëTDB"
run_streamcat(text, 'utf-8')
run_streamcat(text, 'latin1')
