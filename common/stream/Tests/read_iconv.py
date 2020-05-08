#!/usr/bin/env python3

from testdata import Doc, TestFile

import os
import subprocess
import sys


def run_streamcat(text, enc):
    content = bytes(text, enc)
    name = f'read_iconv_{enc}.txt'

    tf = TestFile(name, None)
    filename = tf.write(content)

    cmd = ['streamcat', 'read', filename, 'rstream', f'iconv:{enc}']
    print(f"Input with encoding '{enc}' is {repr(content)}")
    # print(cmd)
    proc = subprocess.run(cmd, stdout=subprocess.PIPE)
    if proc.returncode != 0:
        print(f"{cmd} exited with status {proc.returncode}", file=sys.stderr)
        sys.exit(1)
    os.remove(filename)
    print(f"Output is {repr(proc.stdout)}")
    print()
    return proc.stdout


text = "MøNëTDB"
run_streamcat(text, 'utf-8')
run_streamcat(text, 'latin1')
