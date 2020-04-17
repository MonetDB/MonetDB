#!/usr/bin/env python3

from testdata import Doc, pick_tmp_name

import os
import subprocess
import sys
import tempfile


def run_streamcat(text, enc):
    input = bytes(text, 'utf-8')
    filename = pick_tmp_name('_streamtest_write_iconv', '.txt')

    cmd = ['streamcat', 'write', filename, 'wstream', f'iconv:{enc}']

    print(f"Input UTF-8 encoded is {repr(input)}")
    # print(cmd)
    proc = subprocess.run(cmd, input=input)
    if proc.returncode != 0:
        print(f"{cmd} exited with status {proc.returncode}", file=sys.stderr)
        sys.exit(1)
    output = open(filename, 'rb').read()
    os.remove(filename)
    print(f"Output is {repr(output)}")
    print()


text = "MøNëTDB"
run_streamcat(text, 'utf-8')
run_streamcat(text, 'latin1')
