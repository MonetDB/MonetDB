# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

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

from subprocess import PIPE, TimeoutExpired
try:
    from subprocess import DEVNULL
except ImportError:
    DEVNULL = os.open(os.devnull, os.O_RDWR)
__all__ = ['PIPE', 'DEVNULL', 'Popen', 'client', 'server', 'TimeoutExpired']

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

def _remainingtime(endtime):
    if endtime is None:
        return None
    curtime = time.time()
    if curtime >= endtime:
        raise queue.Empty
    return endtime - curtime

class _BufferedPipe:
    def __init__(self, fd):
        self._queue = queue.Queue()
        self._cur = None
        self._curidx = 0
        self._eof = False
        self._empty = ''
        self._nl = '\n'
        self._cr = '\r'
        self._thread = threading.Thread(target=self._readerthread,
                                        args=(fd, self._queue))
        # self._thread.daemon = True
        self._continue = True
        self._thread.start()

    def _readerthread(self, fh, q):
        s = 0
        w = 0
        first = True
        while self._continue:
            if verbose:
                print('fh.readline', flush=True)
            c = fh.readline()
            if verbose:
                print(f'read {len(c)} bytes', flush=True)
            if first:
                if type(c) is type(b''):
                    self._empty = b''
                    self._nl = b'\n'
                    self._cr = b'\r'
                    first = False
            if not self._continue:
                break
            if not c:
                q.put(c)    # put '' if at EOF
                break
            c = c.replace(self._cr, self._empty)
            if c:
                q.put(c)

    def close(self):
        if verbose:
            print('close _BufferedPipe', flush=True)
        self._continue = False
        if self._thread:
            if verbose:
                print('close: joining', flush=True)
            self._thread.join()
            if verbose:
                print('close: joined', flush=True)
        self._thread = None

    def _read(self, size=-1, endtime=None):
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
        while size != 0:
            if verbose:
                print('queue.get', flush=True)
            try:
                c = self._queue.get(timeout=_remainingtime(endtime))
            except queue.Empty:
                print('queue.empty', flush=True)
                break
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

    def read(self, size=-1, timeout=None):
        if timeout is None:
            endtime = None
        else:
            endtime = time.time() + timeout
        return self._read(size=size, endtime=endtime)

    def readline(self, size=-1, timeout=None):
        if timeout is None:
            endtime = None
        else:
            endtime = time.time() + timeout
        ret = []
        while size != 0:
            c = self._read(1, endtime=endtime)
            ret.append(c)
            if size > 0:
                size -= 1
            if c == self._nl or c == self._empty:
                break
        return self._empty.join(ret)

# signals that by default produce a core dump, i.e. bad
# this of course doesn't work on Windows
_coresigs = set()
try:
    _coresigs.add(signal.SIGABRT)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGBUS)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGFPE)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGILL)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGIOT)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGQUIT)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGSEGV)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGSYS)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGTRAP)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGXCPU)
except AttributeError:
    pass
try:
    _coresigs.add(signal.SIGXFSZ)
except AttributeError:
    pass

class Popen(subprocess.Popen):
    def __init__(self, *args, **kwargs):
        self.dotmonetdbfile = None
        self.isserver = False
        if sys.hexversion < 0x03070000 and 'text' in kwargs:
            kwargs = kwargs.copy()
            kwargs['universal_newlines'] = kwargs.pop('text')
        super().__init__(*args, **kwargs)

    def __exit__(self, exc_type, value, traceback):
        self.terminate()
        try:
            self.wait(timeout=10)
        except TimeoutExpired:
            self.kill()
            self.wait()
        self._clean_dotmonetdbfile()
        super().__exit__(exc_type, value, traceback)
        if self.returncode and self.returncode < 0 and -self.returncode in _coresigs:
            raise RuntimeError('process exited with coredump generating signal %r' % signal.Signals(-self.returncode))

    def __del__(self):
        if self._child_created and self.returncode is None:
            # this may well fail in Python 3.13.2 ("TypeError:
            # 'NoneType' object is not callable" in import signal), but
            # it is very unlikely we actually get here since the above
            # __exit__ will normally have been executed first and so
            # returncode will have been set
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

    def wait(self, timeout=None):
        ret = super().wait(timeout=timeout)
        self._clean_dotmonetdbfile()
        return ret

    def communicate(self, input=None, timeout=None):
        # since we always use threads for stdout/stderr, we can just read()
        if not isinstance(self.stdout, _BufferedPipe) and not isinstance(self.stderr, _BufferedPipe):
            if verbose:
                print('relegating communicate to super()', flush=True)
            return super().communicate(input=input, timeout=timeout)
        stdout = None
        stderr = None
        if self.stdin:
            if input:
                try:
                    if verbose:
                        print('communicate: writing input', flush=True)
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
            if verbose:
                print('communicate: reading stdout', flush=True)
            stdout = self.stdout.read()
            self.stdout.close()
        if self.stderr:
            if verbose:
                print('communicate: reading stderr', flush=True)
            stderr = self.stderr.read()
            self.stderr.close()
        if verbose:
            print('communicate: waiting', flush=True)
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
            print('Default client: ' + ' '.join(cmd +  args))

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
            print('Executing: ' + ' '.join(cmd +  args), flush=True)
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
            if verbose:
                print('create _BufferedPipe for stdout', flush=True)
            self.stdout = _BufferedPipe(self.stdout)
        if stderr == PIPE:
            if verbose:
                print('create _BufferedPipe for stderr', flush=True)
            self.stderr = _BufferedPipe(self.stderr)
        if input is not None:
            self.stdin.write(input)
        if communicate:
            out, err = self.communicate()
            sys.stdout.write(out)
            sys.stderr.write(err)
        if verbose:
            print('client created', flush=True)

class server(Popen):
    def __init__(self, args=[], stdin=None, stdout=None, stderr=None,
                 mapiport=None, dbname=os.getenv('TSTDB'), dbfarm=None,
                 dbextra=None, bufsize=0,
                 notrace=False, ipv6=False):
        '''Start a server process.'''
        cmd = _server[:]
        if not cmd:
            cmd = ['mserver5',
                   '--set', 'mapi_listenaddr=all' if ipv6 else 'mapi_listenaddr=0.0.0.0',
                   '--set', 'gdk_nr_threads=1']
        if os.getenv('NOWAL'):
            cmd.extend(['--set', 'sql_debug=128'])
        if verbose:
            print('Default server: ' + ' '.join(cmd +  args))
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
        if os.path.exists(os.path.join(dbpath, '.vaultkey')):
            cmd.extend(['--set',
                        'monet_vault_key={}'.format(os.path.join(dbpath, '.vaultkey'))])
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
            print('Executing: ' + ' '.join(cmd +  args), flush=True)
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
        starttime = time.time()
        super().__init__(cmd + args,
                         stdin=stdin,
                         stdout=stdout,
                         stderr=stderr,
                         shell=False,
                         text=True,
                         bufsize=bufsize,
                         encoding='utf-8')
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
            # wait at most 30 seconds for the server to start
            if time.time() > starttime + 30:
                self.kill()
                self.wait()
                raise TimeoutExpired(cmd, 30)
            time.sleep(0.1)
