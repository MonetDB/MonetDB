#!/usr/bin/env python3

import sys, os
sys.path.append(os.environ.get('TSTSRCDIR','.'))
from testdata import Doc, TestFile

import subprocess
import tempfile


def run_streamcat(text, enc):
    utf8_text = bytes(text, 'utf-8')
    expected = bytes(text, enc)

    name = f'write_iconv_{enc}.txt'
    tf = TestFile(name, None)

    cmd = ['streamcat', 'write', tf.path(), 'wstream', f'iconv:{enc}']
    proc = subprocess.run(cmd, input=utf8_text)
    if proc.returncode != 0:
        print(f"command {cmd} exited with status {proc.returncode} for input {utf8_text!r} ({text!r})", file=sys.stderr)
        sys.exit(1)
    output = tf.read()
    os.remove(tf.path())

    if output != expected:
        raise Exception(f"command {cmd} with input {utf8_text!r} yielded {output!r}, expected {expected!r}")


text = "MøNëTDB"
run_streamcat(text, 'utf-8')
run_streamcat(text, 'latin1')
