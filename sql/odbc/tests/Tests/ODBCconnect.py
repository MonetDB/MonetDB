# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.


# For each target, odbcconnect first prints a status line,
# either 'OK' or 'Error FUNCNAME', followed by zero or more
# sqlstate lines of the form '    - STATE: MESSAGE'.

import atexit
import subprocess
import sys


class Execution:
    def __init__(self, *odbcconnect_args):
        cmd = self.cmd = ['odbcconnect', *odbcconnect_args]
        proc = self.proc = subprocess.run(
            cmd,
            stderr=subprocess.PIPE, stdout=subprocess.PIPE,
            encoding='utf-8')
        self.expected_exitcode = 0
        self.remaining = proc.stdout.splitlines()
        self.checks = []

    def report(self):
        parts = [f'COMMAND: {self.cmd}',
                 f'EXIT CODE: {self.proc.returncode}', '']
        if self.proc.stdout:
            parts += [
                '--- stdout: ---',
                self.proc.stdout.rstrip(),
                '--- end ---',
                ''
            ]
        if self.proc.stderr:
            parts += [
                '--- stderr: ---',
                self.proc.stderr.rstrip(),
                '--- end ---',
                ''
            ]
        if self.checks:
            parts.append('--- test history: ---')
            for wanted, found in self.checks:
                parts.append(f'wanted {wanted!r}, found {found!r}')
            parts.append('--- end ---')
        if self.remaining:
            parts += [
                '--- remaining output: ---',
                *self.remaining,
                '--- end ---'
            ]
        return '\n'.join(parts)

    def expect(self, pattern):
        line = self.next_line()
        self.checks.append((pattern, line))
        if pattern not in line:
            raise Exception(f'Wanted {pattern!r}, found {line!r}')

    def next_line(self):
        if not self.remaining:
            raise Exception(f"Unexpected end of output")
        line = self.remaining[0]
        del self.remaining[0]
        return line

    def expect_fail(self, exitcode=1):
        self.expected_exitcode = exitcode

    def end(self):
        if self.remaining:
            raise Exception(f'Unexpected output remaining: {self.remaining}')
        code = self.proc.returncode
        expected = self.expected_exitcode
        if code != expected:
            raise Exception(
                f'Process exited with code {code!r}, expected {expected!r}')


ex = None


@atexit.register
def show_context():
    global ex
    if ex:
        # ex.end()
        print(file=sys.stderr)
        print(ex.report(), file=sys.stderr)


dbname = 'MonetDB-Test'


ex = Execution(dbname)
ex.expect('OK')
ex.end()

ex = Execution(dbname + '-nonexistent')
ex.expect_fail()
ex.expect('Error')
ex.expect('IM002:')  # IM002 not found
ex.end()

ex = Execution(dbname, '-p', 'wrongpassword')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')  # 28000 bad credentials
ex.end()

ex = Execution(dbname, '-u', 'wronguser')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')  # 28000 bad credentials
ex.end()

ex = Execution(dbname, '-p', '')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')  # 28000 bad credentials
ex.end()

ex = Execution(dbname, '-u', '')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')  # 28000 bad credentials
ex.end()

ex = None
