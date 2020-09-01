#!/usr/bin/env python3

import sys, os
sys.path.append(os.environ.get('TSTSRCDIR','.'))
from testdata import Doc, TestFile

import subprocess


def run_streamcat(text, enc, expected_error = None):
    if isinstance(text, bytes):
        content = text
    else:
        content = bytes(text, enc)
    name = f'read_iconv_{enc}.txt'

    tf = TestFile(name, None)
    filename = tf.write(content)

    cmd = ['streamcat', 'read', filename, 'rstream', f'iconv:{enc}']
    print(f"Input is {repr(content)}")
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.stdout:
        proc.stdout = proc.stdout.replace(b'\r', b'')
    if proc.stderr:
        proc.stderr = proc.stderr.replace(b'\r', b'')
        sys.stderr.buffer.write(proc.stderr)
        sys.stderr.flush()
    if expected_error == None:
        if proc.returncode != 0:
            print(f"{cmd} exited with status {proc.returncode}", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"Output after decoding as '{enc}' is {repr(proc.stdout)}")
    else:
        if proc.returncode == 0:
            print(f"{cmd} exited without expected error", file=sys.stderr)
            sys.exit(1)
        elif expected_error not in proc.stderr:
            print(f"{cmd} failed as expected but stderr does not contain {repr(expected_error)}", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"Decoding failed as expected: {repr(proc.stderr)}")
    os.remove(filename)
    print()
    return proc.stdout


text = "MøNëTDB"
run_streamcat(text, 'utf-8')
run_streamcat(text, 'latin1')

# invalid utf-8, expect an error
run_streamcat(b"M\xc3\xc3NETDB", 'utf-8', b'multibyte sequence')
