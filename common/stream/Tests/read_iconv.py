#!/usr/bin/env python3

import sys, os
sys.path.append(os.environ.get('TSTSRCDIR','.'))
from testdata import Doc, TestFile

import subprocess


def run_streamcat(text, enc, expected_error = None):
    if isinstance(text, bytes):
        enc_text = text
        expected = None
    else:
        enc_text = bytes(text, enc)
        expected = bytes(text, 'utf-8')

    name = f'read_iconv_{enc}.txt'
    tf = TestFile(name, None)
    filename = tf.write(enc_text)

    cmd = ['streamcat', 'read', filename, 'rstream', f'iconv:{enc}']
    descr = f"command {cmd} with input {enc_text!r}"
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = proc.stdout
    os.remove(filename)

    if expected_error:
        if proc.returncode == 0:
            print(f"{descr} exited without expected error", file=sys.stderr)
            sys.exit(1)
        elif expected_error not in proc.stderr:
            print(f"{descr} failed as expected but stderr does not contain {expected_error!r}", file=sys.stderr)
            sys.exit(1)
        else:
            return

    if proc.returncode != 0:
        print(f"{descr} exited with status {proc.returncode}", file=sys.stderr)
        sys.exit(1)
    if output != expected:
        print(f"{descr} yielded {output!r}, expected {expected!r}")
        sys.exit(1)


text = "MøNëTDB"
run_streamcat(text, 'utf-8')
run_streamcat(text, 'latin1')

# invalid utf-8, expect an error
run_streamcat(b"M\xc3\xc3NETDB", 'utf-8', b'multibyte sequence')
