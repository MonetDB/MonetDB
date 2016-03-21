# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import subprocess
import os
import sys
import time
import string
import tempfile
import copy
import atexit
import threading
if sys.version[:1] == '2':
    import Queue as queue
else:
    import queue

from subprocess import PIPE

__all__ = ['PIPE', 'Popen', 'client', 'server']

verbose = False

def splitcommand(cmd):
    '''Like string.split, except take quotes into account.'''
    q = None
    w = []
    command = []
    for c in cmd:
        if q:
            if c == q:
                q = None
            else:
                w.append(c)
        elif c in string.whitespace:
            if w:
                command.append(''.join(w))
            w = []
        elif c == '"' or c == "'":
            q = c
        else:
            w.append(c)
    if w:
        command.append(''.join(w))
    if len(command) > 1 and command[0] == 'call':
        del command[0]
    return command

_mal_client = splitcommand(os.getenv('MAL_CLIENT', 'mclient -lmal'))
_sql_client = splitcommand(os.getenv('SQL_CLIENT', 'mclient -lsql'))
_sql_dump = splitcommand(os.getenv('SQL_DUMP', 'msqldump -q'))
_server = splitcommand(os.getenv('MSERVER', ''))
_dbfarm = os.getenv('GDK_DBFARM', None)

_dotmonetdbfile = []

def _delfiles():
    for f in _dotmonetdbfile:
        try:
            os.unlink(f)
        except OSError:
            pass

atexit.register(_delfiles)

class _BufferedPipe:
    def __init__(self, fd, waitfor = None, skip = None):
        self._pipe = fd
        self._queue = queue.Queue()
        self._eof = False
        self._empty = ''
        if waitfor is not None:
            self._wfq = queue.Queue()
        else:
            self._wfq = None
        self._thread = threading.Thread(target = self._readerthread,
                                        args = (fd, self._queue, waitfor, self._wfq, skip))
        self._thread.setDaemon(True)
        self._thread.start()

    def _readerthread(self, fh, queue, waitfor, wfq, skip):
        # If `skip' has a value, don't pass it through the first time
        # we encounter it.
        # If `waitfor' has a value, put something into the wfq queue
        # when we've seen it.
        s = 0
        w = 0
        skipqueue = []
        first = True
        while True:
            if skipqueue:
                c = skipqueue[0]
                del skipqueue[0]
            else:
                c = fh.read(1)
                if first:
                    if type(c) is type(b''):
                        self._empty = b''
                    first = False
                if skip and c:
                    if c == skip[s]:
                        s += 1
                        if s == len(skip):
                            skip = None
                    else:
                        j = 0
                        while j < s:
                            if skip[j:s] + c != skip[:s-j+1]:
                                skipqueue.append(skip[j])
                                j += 1
                            else:
                                s = s-j+1
                                break
                        else:
                            if c == skip[0]:
                                s = 1
                            else:
                                skipqueue.append(c)
                                s = 0
                    continue
            if waitfor and c:
                if c == waitfor[w]:
                    w += 1
                    if w == len(waitfor):
                        waitfor = None
                        wfq.put('ready')
                        wfq = None
                else:
                    j = 0
                    while j < w:
                        if waitfor[j:w] + c != waitfor[:w-j+1]:
                            queue.put(waitfor[j])
                            j += 1
                        else:
                            w = w-j+1
                            break
                    else:
                        if c == waitfor[0]:
                            w = 1
                        else:
                            queue.put(c)
                            w = 0
                continue
            queue.put(c)                # put '' if at EOF
            if not c:
                if waitfor is not None:
                    # if at EOF and still waiting for string, signal EOF
                    wfq.put('eof')
                    waitfor = None
                    wfq = None
                break

    def _waitfor(self):
        rdy = self._wfq.get()
        self._wfq = None

    def close(self):
        if self._thread:
            self._thread.join()
        self._thread = None

    def read(self, size = -1):
        if self._eof:
            return self._empty
        if size < 0:
            self.close()
        ret = []
        while size != 0:
            c = self._queue.get()
            if c == '\r':
                c = self._queue.get()   # just ignore \r
            ret.append(c)
            if size > 0:
                size -= 1
            try:
                # only available as of Python 2.5
                self._queue.task_done()
            except AttributeError:
                # not essential, if not available
                pass
            if not c:
                self._eof = True
                break                   # EOF
        return self._empty.join(ret)

    def readline(self, size = -1):
        ret = []
        while size != 0:
            c = self.read(1)
            ret.append(c)
            if size > 0:
                size -= 1
            if c == '\n' or c == self._empty:
                break
        return self._empty.join(ret)

class Popen(subprocess.Popen):
    def __init__(self, *args, **kwargs):
        self.dotmonetdbfile = None
        subprocess.Popen.__init__(self, *args, **kwargs)

    def wait(self):
        ret = subprocess.Popen.wait(self)
        if self.dotmonetdbfile is not None:
            os.unlink(self.dotmonetdbfile)
            try:
                _dotmonetdbfile.remove(self.dotmonetdbfile)
            except ValueError:
                pass
        return ret

    def communicate(self, input = None):
        # since we always use threads for stdout/stderr, we can just read()
        stdout = None
        stderr = None
        if self.stdin:
            if input:
                try:
                    self.stdin.write(input)
                except IOError:
                    pass
            self.stdin.close()
        if self.stdout:
            stdout = self.stdout.read()
            self.stdout.close()
        if self.stderr:
            stderr = self.stderr.read()
            self.stderr.close()
        self.wait()
        return stdout, stderr

def client(lang, args = [], stdin = None, stdout = None, stderr = None,
           port = os.getenv('MAPIPORT'), dbname = os.getenv('TSTDB'), host = None,
           user = 'monetdb', passwd = 'monetdb', log = False,
           interactive = None, echo = None, format = None,
           input = None, communicate = False, universal_newlines = True):
    '''Start a client process.'''
    if lang == 'mal':
        cmd = _mal_client[:]
    elif lang == 'sql':
        cmd = _sql_client[:]
    elif lang == 'sqldump':
        cmd = _sql_dump[:]

    # no -i if input from -s or /dev/null
    if '-i' in cmd and ('-s' in args or stdin is None):
        cmd.remove('-i')
    if interactive is not None:
        if '-i' in cmd and not interactive:
            cmd.remove('-i')
        elif '-i' not in cmd and interactive:
            cmd.append('-i')
    if echo is not None:
        if '-e' in cmd and not echo:
            cmd.remove('-e')
        elif '-e' not in cmd and echo:
            cmd.append('-e')
    if format is not None:
        for c in cmd:
            if c.startswith('-f'):
                cmd.remove(c)
                break
        cmd.append('-f' + format)

    env = None

    if port is not None:
        for i in range(len(cmd)):
            if cmd[i][:7] == '--port=':
                del cmd[i]
                break
        cmd.append('--port=%d' % int(port))
    if dbname is not None:
        cmd.append('--database=%s' % dbname)
    fnam = None
    if user is not None or passwd is not None:
        env = copy.deepcopy(os.environ)
        fd, fnam = tempfile.mkstemp(text = True)
        _dotmonetdbfile.append(fnam)
        if user is not None:
            os.write(fd, ('user=%s\n' % user).encode('utf-8'))
        if passwd is not None:
            os.write(fd, ('password=%s\n' % passwd).encode('utf-8'))
        os.close(fd)
        env['DOTMONETDBFILE'] = fnam
    if host is not None:
        for i in range(len(cmd)):
            if cmd[i][:7] == '--host=':
                del cmd[i]
                break
        cmd.append('--host=%s' % host)
    if verbose:
        sys.stdout.write('Executing: ' + ' '.join(cmd +  args) + '\n')
        sys.stdout.flush()
    if log:
        prompt = time.strftime('# %H:%M:%S >  ')
        cmdstr = ' '.join(cmd +  args)
        if hasattr(stdin, 'name'):
            cmdstr += ' < "%s"' % stdin.name
        sys.stdout.write('\n')
        sys.stdout.write(prompt + '\n')
        sys.stdout.write('%s%s\n' % (prompt, cmdstr))
        sys.stdout.write(prompt + '\n')
        sys.stdout.write('\n')
        sys.stdout.flush()
        sys.stderr.write('\n')
        sys.stderr.write(prompt + '\n')
        sys.stderr.write('%s%s\n' % (prompt, cmdstr))
        sys.stderr.write(prompt + '\n')
        sys.stderr.write('\n')
        sys.stderr.flush()
    if stdin is None:
        # if no input provided, use /dev/null as input
        stdin = open(os.devnull)
    if stdout == 'PIPE':
        out = PIPE
    else:
        out = stdout
    p = Popen(cmd + args,
              stdin = stdin,
              stdout = out,
              stderr = stderr,
              shell = False,
              env = env,
              universal_newlines = universal_newlines)
    p.dotmonetdbfile = fnam
    if stdout == PIPE:
        p.stdout = _BufferedPipe(p.stdout)
    if stderr == PIPE:
        p.stderr = _BufferedPipe(p.stderr)
    if input is not None:
        p.stdin.write(input)
    if communicate:
        out, err = p.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    return p

def server(args = [], stdin = None, stdout = None, stderr = None,
           mapiport = None, dbname = os.getenv('TSTDB'), dbfarm = None,
           dbinit = None, bufsize = 0, log = False, notrace = False,
           notimeout = False):
    '''Start a server process.'''
    cmd = _server[:]
    if not cmd:
        cmd = ['mserver5',
               '--set', 'mapi_open=true',
               '--set', 'gdk_nr_threads=1',
               '--set', 'monet_prompt=']
    if notimeout and 'Mtimeout' in cmd[0]:
        del cmd[0:3]            # Mtimeout -timeout 60
    if notrace and '--trace' in cmd:
        cmd.remove('--trace')
    if dbinit is not None:
        cmd.append('--dbinit')
        cmd.append(dbinit)
    if mapiport is not None:
        for i in range(len(cmd)):
            if cmd[i][:10] == 'mapi_port=':
                del cmd[i]
                del cmd[i - 1]
                break
        cmd.append('--set')
        cmd.append('mapi_port=%d' % int(mapiport))
    for i in range(len(cmd)):
        if cmd[i][:9] == '--dbpath=':
            dbpath = cmd[i][9:]
            del cmd[i]
            break
        elif cmd[i] == '--dbpath':
            dbpath = cmd[i+1]
            del cmd[i:i+2]
            break
    else:
        dbpath = None
    if dbname is None and dbfarm is not None:
        dbname = 'demo'
    if dbname is not None:
        if dbfarm is None:
            if _dbfarm is None:
                raise RuntimeError('no dbfarm known')
            dbfarm = _dbfarm
        dbpath = os.path.join(dbfarm, dbname)
    if dbpath is not None:
        cmd.append('--dbpath=%s' % dbpath)
    if verbose:
        sys.stdout.write('Executing: ' + ' '.join(cmd +  args) + '\n')
        sys.stdout.flush()
    for i in range(len(args)):
        if args[i] == '--set' and i+1 < len(args):
            s = args[i+1].partition('=')[0]
            for j in range(len(cmd)):
                if cmd[j] == '--set' and j+1 < len(cmd) and cmd[j+1].startswith(s + '='):
                    del cmd[j:j+2]
                    break
    if log:
        prompt = time.strftime('# %H:%M:%S >  ')
        cmdstr = ' '.join(cmd +  args)
        if hasattr(stdin, 'name'):
            cmdstr += ' < "%s"' % stdin.name
        sys.stdout.write('\n')
        sys.stdout.write(prompt + '\n')
        sys.stdout.write('%s%s\n' % (prompt, cmdstr))
        sys.stdout.write(prompt + '\n')
        sys.stdout.write('\n')
        sys.stdout.flush()
        sys.stderr.write('\n')
        sys.stderr.write(prompt + '\n')
        sys.stderr.write('%s%s\n' % (prompt, cmdstr))
        sys.stderr.write(prompt + '\n')
        sys.stderr.write('\n')
        sys.stderr.flush()
    p = Popen(cmd + args,
              stdin = stdin,
              stdout = stdout,
              stderr = stderr,
              shell = False,
              universal_newlines = True,
              bufsize = bufsize)
    if stderr == PIPE:
        p.stderr = _BufferedPipe(p.stderr)
    if stdout == PIPE:
        if stdin == PIPE:
            # If both stdin and stdout are pipes, we wait until the
            # server is ready.  This is done by sending a print
            # command and waiting for the result to appear.
            rdy = '\nServer Ready.\n'
            cmd = 'io.printf("%s");\n' % rdy.replace('\n', '\\n')
            p.stdout = _BufferedPipe(p.stdout, rdy, cmd)
            p.stdin.write(cmd)
            p.stdin.flush()
            p.stdout._waitfor()
        else:
            p.stdout = _BufferedPipe(p.stdout)
    return p
