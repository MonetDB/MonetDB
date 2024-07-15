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


# UnixODBC turns out to be broken when it comes to Unicode in connection
# strings. In SQLBrowseConnectW it basically converts the 16-bit connection
# string to an 8-bit connection string by dropping the upper bytes and keeping
# only the lower bytes. The character sequence below has been chosen with this
# in mind.

# \u{E1} is LATIN SMALL LETTER A WITH ACUTE.
basic_unicode_text = 'R\u00E1inbow'
# \u{1F308} is RAINBOW EMOJI
full_unicode_text = basic_unicode_text + '\U0001F308'

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

# Grab the output of 'odbcconnect -l' so we can show it if a test fails
list_output = None
ex = Execution('-l')
list_output = ex.proc.stdout

ex = None


@atexit.register
def show_context():
    global ex
    if ex:
        # ex.end()
        print(file=sys.stderr)
        print(ex.report(), file=sys.stderr)
        if list_output:
            print(f'\n--- output of odbcconnect -l ---\n{list_output}--- end ---', file=sys.stderr)
        odbcini = os.getenv('ODBCINI', 'odbc.ini')
        sysini = os.getenv('ODBCSYSINI', os.getenv('TSTTRGDIR'))
        fullpath = os.path.join(sysini, odbcini)
        try:
            with open(fullpath) as f:
                content = f.read()
                print(f'\n--- content of {fullpath} ---\n{content}\n--- end ---', file=sys.stderr)
        except FileNotFoundError:
            pass


#######################################################################
# Test SQLConnect
#######################################################################

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

# test wide characters
ex = Execution('-w', dsn + '-Wrong', '-u', user, '-p', password)
ex.expect('OK')
ex.end()

# test wide characters in combination with non-NUL
ex = Execution('-0', '-w', dsn + '-Wrong', '-u', user, '-p', password)
ex.expect('OK')
ex.end()



#######################################################################
# Test SQLDriverConnect
#######################################################################

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

# test error handling when -w is given

# first without the -w to demonstrate the expected behavior
ex = Execution('-d', f'Driver={{MonetDB}};User={user};Password={password};Database={dbname + "-Wrong"}')
ex.expect_fail()
ex.expect('Error')
ex.expect('08001')  # something wrong with the database
ex.end()
# then with the -w
ex = Execution('-w', '-d', f'Driver={{MonetDB}};User={user};Password={password};Database={dbname + "-Wrong"}')
ex.expect_fail()
ex.expect('Error')
ex.expect('08001')  # something wrong with the database
ex.end()

# test wide characters
# we can use the full character set because UnixODBC's SQLDriverConnect
# passes the connection string on without looking at it
ex = Execution('-w', '-d', f'DSN={dsn};Client Remark={full_unicode_text}')
# expect OK followed by connection string containing the rainbow
ex.expect('OK', f'CLIENTREMARK={full_unicode_text}')
ex.end()

# test maptolongvarchar
ex = Execution(
    '-d', f'DSN={dsn}',
    '-q', "SELECT 'xxx' AS a, 'xxxyyyzzz' AS b"
)
ex.expect('OK')
ex.expect('RESULT', 'a:varchar', 'b:varchar')
ex.expect('-')
ex.end()

ex = Execution(
    '-d', f'DSN={dsn};mapToLongVarchar=5',    # enable maptolong
    '-q', "SELECT 'xxx' AS a, 'xxxyyyzzz' AS b"
)
ex.expect('OK')
ex.expect('RESULT', 'a:varchar', 'b:*varchar')    # second must now be longvarchar
ex.expect('-')
ex.end()



#######################################################################
# Test SQLBrowseConnect
#######################################################################

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

# test wide characters
# we use the limited character set because UnixODBC's SQLBrowserConnect
# messes up code points > 255
ex = Execution('-w', '-b', f'DSN={dsn}')
ex.expect('OK', ';')
ex.end()

ex = Execution('-w', '-b', f'DSN={dsn};Client Remark={basic_unicode_text}')
ex.expect('OK', f';CLIENTREMARK={basic_unicode_text}')
ex.expect
ex.end()

# also with non-NUL terminated strings
ex = Execution('-0', '-w', '-b', f'DSN={dsn}')
ex.expect('OK', ';')
ex.end()

ex = Execution('-0', '-w', '-b', f'DSN={dsn};Client Remark={basic_unicode_text}')
ex.expect('OK', f';CLIENTREMARK={basic_unicode_text}')
ex.expect
ex.end()

# Also test that queries return unicode ok
ex = Execution(
    '-w',
    '-b', f'DSN={dsn};Client Remark={basic_unicode_text}',
    '-q', 'select remark from sys.sessions where sessionid = current_sessionid()',
)
ex.expect('OK', f';CLIENTREMARK={basic_unicode_text}')
ex.expect('RESULT')
ex.expect(basic_unicode_text)
ex.end()


#######################################################################
#
# clear 'ex', otherwise the atexit handler will write things to stderr
#
#######################################################################
ex = None
