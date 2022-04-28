#!/usr/bin/env python3

import subprocess
import sys

try:
    subprocess.check_output("testcondvar", stderr=subprocess.STDOUT)
except subprocess.CalledProcessError as e:
    output = str(e.stdout, 'utf-8')
    if not output.endswith('\n'):
        output += '\n'
    print(f"Test program failed with the following output:\n------", file=sys.stderr)
    print(f"{output}-----", file=sys.stderr)
    sys.exit('TEST FAILED')
