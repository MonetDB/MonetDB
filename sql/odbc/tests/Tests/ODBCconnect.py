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
import os
import shlex
import subprocess
import sys

dsn = 'MonetDB-Test'
dbname = os.environ.get('TSTDB', 'demo')
user = 'monetdb'
password = 'monetdb'
port = os.environ.get('MAPIPORT', 50000)


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
        parts = [f'COMMAND: {shlex.join(self.cmd)}',
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
                if len(wanted) == 1:
                    parts.append(f'wanted {wanted[0]!r}, found {found!r}')
                else:
                    parts.append(f'wanted all of {wanted!r}, found {found!r}')
            parts.append('--- end ---')
        if self.remaining:
            parts += [
                '--- remaining output: ---',
                *self.remaining,
                '--- end ---'
            ]
        return '\n'.join(parts)

    def expect(self, pattern, *more_patterns):
        patterns = [pattern, *more_patterns]
        line = self.next_line()
        self.checks.append((patterns, line))
        for p in patterns:
            if p not in line:
                raise Exception(f'Wanted {p!r}, found {line!r}')

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


ex = Execution(dsn)
ex.expect('OK')
ex.end()

ex = Execution(dsn + '-nonexistent')
ex.expect_fail()
ex.expect('Error')
ex.expect('IM002:')  # IM002 not found
ex.end()

ex = Execution(dsn, '-p', 'wrongpassword')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')  # 28000 bad credentials
ex.end()

ex = Execution(dsn, '-u', 'wronguser')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')  # 28000 bad credentials
ex.end()

ex = Execution(dsn, '-p', '')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')  # 28000 bad credentials
ex.end()

ex = Execution(dsn, '-u', '')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')  # 28000 bad credentials
ex.end()

# parameters passed directly to SQLConnect override those from the dsn
ex = Execution(dsn + '-Wrong')
ex.expect_fail()
ex.expect('Error')
ex.expect('28000:')    # this dsn uses the wrong user name and password
ex.end()
ex = Execution(dsn + '-Wrong', '-u', user, '-p', password)
ex.expect('OK')    # but those passes as arguments take precedence
ex.end()

# test non-NUL terminated strings

ex = Execution(dsn, '-0')
ex.expect('OK')
ex.end()

ex = Execution(dsn, '-0', '-u', user, '-p', password)
ex.expect('OK')
ex.end()

# test connection strings

ex = Execution('-d', f'DSN={dsn}')
ex.expect('OK')
ex.end()

# override things that are already set in the dsn
ex = Execution('-d', f'DSN={dsn}-Wrong;Database={dbname};Uid={user};Pwd={password}')
ex.expect('OK')
ex.end()

# .. even if the DSN= comes last
ex = Execution('-d', f'Database={dbname};Uid={user};Pwd={password};DSN={dsn}-Wrong')
ex.expect('OK')
ex.end()

# test without DSN= clause
ex = Execution('-d', f'DRIVER={{MonetDB}};Database={dbname};Uid={user};Pwd={password};Port={port}')
ex.expect('OK')
ex.end()

# non-NUL terminated connection string
ex = Execution('-0', '-d', f'DSN={dsn}-Wrong;Database={dbname};Uid={user};Pwd={password}')
ex.expect('OK')
ex.end()

# test autocommit, default should be On
ex = Execution('-d', f'DSN={dsn}', '-q', 'ROLLBACK')
ex.expect('OK')         # connect succeeds
ex.expect('Error:')     # rollback fails
ex.expect('2DM30:')     # because 2DM30: not allowed in autocommit mode
ex.expect_fail()
ex.end()

# test autocommit, force On
ex = Execution('-d', f'DSN={dsn};Autocommit=On', '-q', 'ROLLBACK')
ex.expect('OK')         # connect succeeds
ex.expect('Error:')     # rollback fails
ex.expect('2DM30:')     # because 2DM30: not allowed in autocommit mode
ex.expect_fail()
ex.end()

# test autocommit, force Off
ex = Execution('-d', f'DSN={dsn};Autocommit=Off', '-q', 'ROLLBACK')
ex.expect('OK')         # connect succeeds
ex.expect('RESULT')     # rollback succeeds
ex.end()

# test that configuration does not leak to next connection when handle is reused
ex = Execution('-d',
    '-q', 'select remark from sys.sessions where sessionid = current_sessionid()',
    f'DSN={dsn};Client Remark=banana',
    f'DSN={dsn}'
)
ex.expect('OK')
ex.expect('RESULT')
ex.expect('- banana;')   # as set by Client Remark property
ex.expect('OK')
ex.expect('RESULT')
ex.expect('- ;')   # the second connection does not have a Client Remark
ex.end()


# Test browsing

ex = Execution('-b', 'Driver={MonetDB}')
ex.expect('Info')
ex.expect('08001')
# browse not complete, uid and pwd mandatory, database optional
ex.expect('BROWSE', 'UID:User=?', 'PWD:Password=?', '*DATABASE')
ex.end()

# same as above, but with another iteration of browsing
ex = Execution('-b', 'Driver={MonetDB}', f'Driver={{MonetDB}};UID={user};PWD={password};Port={port}')
# first iteration
ex.expect('Info')
ex.expect('08001')
ex.expect('BROWSE', 'UID:User=?', 'PWD:Password=?', '*DATABASE')
# second iteration
ex.expect('OK', ';', 'HOST=')
ex.end()

# similar to above, but not NUL-terminated
ex = Execution('-0', '-b', 'Driver={MonetDB}')
ex.expect('Info')
ex.expect('08001')
# browse not complete, uid and pwd mandatory, database optional
ex.expect('BROWSE', 'UID:User=?', 'PWD:Password=?', '*DATABASE')
ex.end()

# it should also work when the user and password are in the dsn
ex = Execution('-b', f'DSN={dsn}')
ex.expect('OK', ';')
ex.end()

# clear 'ex', otherwise the atexit handler will write things
# to stderr
ex = None
