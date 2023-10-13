import os
import subprocess
import sys

test_dir = os.environ.get('TSTSRCDIR', '.')
default_tests_file = os.path.join(test_dir, 'tests.md')
tests_file = os.environ.get('TST_TESTS_MD', default_tests_file)


cmd = [ 'murltest', tests_file ]
try:
    subprocess.run(cmd, check=True)
except subprocess.CalledProcessError as e:
    print(f"Command {cmd} failed.", file=sys.stderr)
    print(f"Add -v, -vv or -vvv to get more context", file=sys.stderr)
    exit(1)
