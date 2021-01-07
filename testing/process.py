# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

import subprocess
import os
import sys
import time
import string
import tempfile
import copy
import atexit
import threading
import signal
import queue

from subprocess import PIPE
try:
    from subprocess import DEVNULL
except ImportError:
    DEVNULL = os.open(os.devnull, os.O_RDWR)
__all__ = ['PIPE', 'DEVNULL', 'Popen', 'client', 'server']

try:
    # on Windows, also make this available
    from subprocess import CREATE_NEW_PROCESS_GROUP
except ImportError:
    pass
else:
    __all__.append('CREATE_NEW_PROCESS_GROUP')

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
    def __init__(self, fd):
        self._pipe = fd
        self._queue = queue.Queue()
        self._cur = None
        self._curidx = 0
        self._eof = False
        self._empty = ''
        self._nl = '\n'
        self._cr = '\r'
        self._thread = threading.Thread(target=self._readerthread,
                                        args=(fd, self._queue))
        self._thread.setDaemon(True)
        self._thread.start()

    def _readerthread(self, fh, queue):
        s = 0
        w = 0
        first = True
        while True:
            c = fh.read(1024)
            if first:
                if type(c) is type(b''):
                    self._empty = b''
                    self._nl = b'\n'
                    self._cr = b'\r'
                    first = False
            if not c:
                queue.put(c)    # put '' if at EOF
                break
            c = c.replace(self._cr, self._empty)
            if c:
                queue.put(c)

    def close(self):
        if self._thread:
            self._thread.join()
        self._thread = None

    def read(self, size=-1):
        ret = []
        if self._cur:
            if size < 0:
                ret.append(self._cur)
                self._cur = None
            else:
                ret.append(self._cur[self._curidx:self._curidx+size])
                if self._curidx + size >= len(self._cur):
                    self._cur = None
                    self._curidx = 0
                else:
                    self._curidx += len(ret[-1])
                size -= len(ret[-1])
                if size == 0:
                    return self._empty.join(ret)
        if self._eof:
            if ret:
                return self._empty.join(ret)
            return self._empty
        if size < 0:
            self.close()
        while size != 0:
            c = self._queue.get()
            if len(c) > size > 0:
                ret.append(c[:size])
                self._cur = c[size:]
                size = 0
            else:
                ret.append(c)
                if size > 0:
                    size -= len(c)
            self._queue.task_done()
            if not c:
                self._eof = True
                break                   # EOF
        return self._empty.join(ret)

    def readline(self, size=-1):
        ret = []
        while size != 0:
            c = self.read(1)
            ret.append(c)
            if size > 0:
                size -= 1
            if c == self._nl or c == self._empty:
                break
        return self._empty.join(ret)

class Popen(subprocess.Popen):
    def __init__(self, *args, **kwargs):
        self.dotmonetdbfile = None
        self.isserver = False
        if sys.version[:3] < '3.7':
            kw = kwargs.copy()
            if 'text' in kw:
                kw['universal_newlines'] = kw['text']
                del kw['text']
            kwargs = kw
        super().__init__(*args, **kwargs)

    def __exit__(self, exc_type, value, traceback):
        self.terminate()
        self._clean_dotmonetdbfile()
        super().__exit__(exc_type, value, traceback)

    def __del__(self):
        if self._child_created:
            self.terminate()
        self._clean_dotmonetdbfile()
        super().__del__()

    def _clean_dotmonetdbfile(self):
        if self.dotmonetdbfile is not None:
            try:
                os.unlink(self.dotmonetdbfile)
            except FileNotFoundError:
                pass
            try:
                _dotmonetdbfile.remove(self.dotmonetdbfile)
            except ValueError:
                pass
            self.dotmonetdbfile = None

    def wait(self):
        ret = super().wait()
        self._clean_dotmonetdbfile()
        return ret

    def communicate(self, input=None):
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
        if self.isserver:
            try:
                if os.name == 'nt':
                    self.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    self.terminate()
            except OSError:
                pass
        if self.stdout:
            stdout = self.stdout.read()
            self.stdout.close()
        if self.stderr:
            stderr = self.stderr.read()
            self.stderr.close()
        self.wait()
        return stdout, stderr

class client(Popen):
    def __init__(self, lang, args=[], stdin=None, stdout=None, stderr=None,
                 server=None, port=None, dbname=None, host=None,
                 user='monetdb', passwd='monetdb',
                 interactive=None, echo=None, format=None,
                 input=None, communicate=False, text=None, encoding=None):
        '''Start a client process.'''
        if lang == 'mal':
            cmd = _mal_client[:]
        elif lang == 'sql':
            cmd = _sql_client[:]
        elif lang == 'sqldump':
            cmd = _sql_dump[:]
        if verbose:
            sys.stdout.write('Default client: ' + ' '.join(cmd +  args) + '\n')

        if (encoding is None or encoding.lower() == 'utf-8') and text is None:
            text = True

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
            for i in range(len(cmd)):
                if cmd[i] == '-f' or cmd[i] == '--format':
                    del cmd[i:i+2]
                    break
                if cmd[i].startswith('-f') or cmd[i].startswith('--format='):
                    del cmd[i]
                    break
            if format:
                cmd.append('-f' + format)
        if encoding is not None:
            if text and encoding.lower() != 'utf-8':
                raise RuntimeError('text cannot be combined with encoding')
            for i in range(len(cmd)):
                if cmd[i] == '-E' or cmd[i] == '--encoding':
                    del cmd[i:i+2]
                    break
                if cmd[i].startswith('-E') or cmd[i].startswith('--encoding='):
                    del cmd[i]
                    break
            if encoding:
                cmd.append('-E' + encoding)

        env = None

        # if server instance is specified, it provides defaults for
        # database name and port
        if server is not None:
            if port is None:
                port = server.dbport
            if dbname is None:
                dbname = server.dbname

        if port is not None:
            for i in range(len(cmd)):
                if cmd[i].startswith('--port='):
                    del cmd[i]
                    break
            try:
                cmd.append('--port=%d' % int(port))
            except ValueError:
                if port:
                    raise
        if dbname is None:
            dbname = os.getenv('TSTDB')
        if dbname is not None and dbname:
            cmd.append('--database=%s' % dbname)
        if user is not None or passwd is not None:
            env = copy.deepcopy(os.environ)
            fd, fnam = tempfile.mkstemp(text=True)
            self.dotmonetdbfile = fnam
            _dotmonetdbfile.append(fnam)
            if user is not None:
                os.write(fd, ('user=%s\n' % user).encode('utf-8'))
            if passwd is not None:
                os.write(fd, ('password=%s\n' % passwd).encode('utf-8'))
            os.close(fd)
            env['DOTMONETDBFILE'] = fnam
        if host is not None:
            for i in range(len(cmd)):
                if cmd[i].startswith('--host='):
                    del cmd[i]
                    break
            if host:
                cmd.append('--host=%s' % host)
        if verbose:
            sys.stdout.write('Executing: ' + ' '.join(cmd +  args) + '\n')
            sys.stdout.flush()
        if stdin is None:
            # if no input provided, use /dev/null as input
            stdin = open(os.devnull)
        if stdout == 'PIPE':
            out = PIPE
        else:
            out = stdout
        if text and not encoding:
            encoding = 'utf-8'
        super().__init__(cmd + args,
                         stdin=stdin,
                         stdout=out,
                         stderr=stderr,
                         shell=False,
                         env=env,
                         encoding=encoding,
                         text=text)
        if stdout == PIPE:
            self.stdout = _BufferedPipe(self.stdout)
        if stderr == PIPE:
            self.stderr = _BufferedPipe(self.stderr)
        if input is not None:
            self.stdin.write(input)
        if communicate:
            out, err = self.communicate()
            sys.stdout.write(out)
            sys.stderr.write(err)

class server(Popen):
    def __init__(self, args=[], stdin=None, stdout=None, stderr=None,
                 mapiport=None, dbname=os.getenv('TSTDB'), dbfarm=None,
                 dbextra=None, bufsize=0,
                 notrace=False, notimeout=False, ipv6=False):
        '''Start a server process.'''
        cmd = _server[:]
        if not cmd:
            cmd = ['mserver5',
                   '--set', ipv6 and 'mapi_listenaddr=all' or 'mapi_listenaddr=0.0.0.0',
                   '--set', 'gdk_nr_threads=1']
        if verbose:
            sys.stdout.write('Default server: ' + ' '.join(cmd +  args) + '\n')
        if notrace and '--trace' in cmd:
            cmd.remove('--trace')
        if mapiport is not None:
            # make sure it's a string
            mapiport = str(int(mapiport))
            for i in range(len(cmd)):
                if cmd[i].startswith('mapi_port='):
                    del cmd[i]
                    del cmd[i - 1]
                    break
            usock = None
            for i in range(len(cmd)):
                if cmd[i].startswith('mapi_usock='):
                    usock = cmd[i][11:cmd[i].rfind('.')]
                    del cmd[i]
                    del cmd[i - 1]
                    break
            cmd.append('--set')
            cmd.append('mapi_port=%s' % mapiport)
            if usock is not None:
                cmd.append('--set')
                if mapiport == '0':
                    cmd.append('mapi_usock=%s.${PORT}' % usock)
                else:
                    cmd.append('mapi_usock=%s.%s' % (usock, mapiport))
        for i in range(len(cmd)):
            if cmd[i].startswith('--dbpath='):
                dbpath = cmd[i][9:]
                del cmd[i]
                break
            elif cmd[i] == '--dbpath':
                dbpath = cmd[i+1]
                del cmd[i:i+2]
                break
        else:
            dbpath = None
        if dbpath is not None:
            if dbfarm is None:
                dbfarm = os.path.dirname(dbpath)
            if dbname is None:
                dbname = os.path.basename(dbpath)
        if dbname is None:
            dbname = 'demo'
        if dbfarm is None:
            if _dbfarm is None:
                raise RuntimeError('no dbfarm known')
            dbfarm = _dbfarm
        dbpath = os.path.join(dbfarm, dbname)
        cmd.append('--dbpath=%s' % dbpath)
        for i in range(len(cmd)):
            if cmd[i].startswith('--dbextra='):
                dbextra_path = cmd[i][10:]
                del cmd[i]
                break
            elif cmd[i] == '--dbextra':
                dbextra_path = cmd[i+1]
                del cmd[i:i+2]
                break
        else:
            dbextra_path = None
        if dbextra is not None:
            dbextra_path = dbextra
        if dbextra_path is not None:
            cmd.append('--dbextra=%s' % dbextra_path)

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
        started = os.path.join(dbpath, '.started')
        try:
            os.unlink(started)
        except OSError:
            pass
        if os.name == 'nt':
            kw = {'creationflags': CREATE_NEW_PROCESS_GROUP}
        else:
            kw = {}
        super().__init__(cmd + args,
                         stdin=stdin,
                         stdout=stdout,
                         stderr=stderr,
                         shell=False,
                         text=True,
                         bufsize=bufsize,
                         encoding='utf-8',
                         **kw)
        self.isserver = True
        if stderr == PIPE:
            self.stderr = _BufferedPipe(self.stderr)
        if stdout == PIPE:
            self.stdout = _BufferedPipe(self.stdout)
        # store database name and port in the returned instance for the
        # client to pick up
        self.dbname = dbname
        self.dbport = mapiport
        while True:
            self.poll()
            if self.returncode is not None:
                # process exited already
                break
            if os.path.exists(started):
                # server is ready
                try:
                    conn = open(os.path.join(dbpath, '.conn')).read()
                except:
                    if verbose:
                        print('failed to open {}'.format(os.path.join(dbpath, '.conn')))
                    pass
                else:
                    # retrieve mapi port if available
                    for c in conn.splitlines():
                        c = c.rstrip('/')
                        c = c.rsplit(':', maxsplit=1)
                        if len(c) == 2:
                            try:
                                port = int(c[1])
                            except ValueError:
                                pass
                            else:
                                if verbose:
                                    print('mapi port: {}'.format(c[1]))
                                self.dbport = c[1]
                                break
                break
            time.sleep(0.001)
