# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

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
