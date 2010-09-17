import subprocess
import os
import sys
import time
import string
import tempfile
import copy
import atexit
import threading
import Queue

from subprocess import PIPE

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

_mil_client = splitcommand(os.getenv('MIL_CLIENT', 'mclient -lmil'))
_mal_client = splitcommand(os.getenv('MAL_CLIENT', 'mclient -lmal'))
_sql_client = splitcommand(os.getenv('SQL_CLIENT', 'mclient -lsql'))
_xquery_client = splitcommand(os.getenv('XQUERY_CLIENT', 'mclient -lxquery -fxml'))
_sql_dump = splitcommand(os.getenv('SQL_DUMP', 'msqldump -q'))
_server = splitcommand(os.getenv('MSERVER', ''))

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
        self._queue = Queue.Queue()
        self._eof = False
        self._thread = threading.Thread(target = self._readerthread,
                                        args = (fd, self._queue))
        self._thread.setDaemon(True)
        self._thread.start()

    def _readerthread(self, fh, queue):
        while True:
            c = fh.read(1)
            queue.put(c)                # put '' if at EOF
            if not c:
                break

    def close(self):
        if self._thread:
            self._thread.join()
        self._thread = None

    def read(self, size = -1):
        if self._eof:
            return ''
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
        return ''.join(ret)

    def readline(self, size = -1):
        ret = []
        while size != 0:
            c = self.read(1)
            ret.append(c)
            if size > 0:
                size -= 1
            if c == '\n' or c == '':
                break
        return ''.join(ret)

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
                self.stdin.write(input)
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
           port = os.getenv('MAPIPORT'), host = None,
           user = 'monetdb', passwd = 'monetdb', log = False):
    '''Start a client process.'''
    if lang == 'mil':
        cmd = _mil_client[:]
    elif lang == 'mal':
        cmd = _mal_client[:]
    elif lang == 'sql':
        cmd = _sql_client[:]
    elif lang == 'xquery':
        cmd = _xquery_client[:]
    elif lang == 'sqldump':
        cmd = _sql_dump[:]

    # no -i if input from -s or /dev/null
    if '-i' in cmd and ('-s' in args or stdin is None):
        cmd.remove('-i')

    env = None

    if port is not None:
        for i in range(len(cmd)):
            if cmd[i][:7] == '--port=':
                del cmd[i]
                break
        cmd.append('--port=%d' % int(port))
    fnam = None
    if user is not None or passwd is not None:
        env = copy.deepcopy(os.environ)
        fd, fnam = tempfile.mkstemp(text = True)
        _dotmonetdbfile.append(fnam)
        if user is not None:
            os.write(fd, 'user=%s\n' % user)
        if passwd is not None:
            os.write(fd, 'password=%s\n' % passwd)
        os.close(fd)
        env['DOTMONETDBFILE'] = fnam
    if host is not None:
        for i in range(len(cmd)):
            if cmd[i][:7] == '--host=':
                del cmd[i]
                break
        cmd.append('--host=%s' % host)
    if verbose:
        print 'Executing', ' '.join(cmd +  args)
        sys.stdout.flush()
    if log:
        prompt = time.strftime('# %H:%M:%S >  ')
        cmdstr = ' '.join(cmd +  args)
        if hasattr(stdin, 'name'):
            cmdstr += ' < "%s"' % stdin.name
        print
        print prompt
        print '%s%s' % (prompt, cmdstr)
        print prompt
        print
        sys.stdout.flush()
        print >> sys.stderr
        print >> sys.stderr, prompt
        print >> sys.stderr, '%s%s' % (prompt, cmdstr)
        print >> sys.stderr, prompt
        print >> sys.stderr
        sys.stderr.flush()
    if stdin is None:
        # if no input provided, use /dev/null as input
        stdin = open(os.devnull)
    p = Popen(cmd + args,
              stdin = stdin,
              stdout = stdout,
              stderr = stderr,
              shell = False,
              env = env,
              universal_newlines = True)
    p.dotmonetdbfile = fnam
    if stdout == PIPE:
        p.stdout = _BufferedPipe(p.stdout)
    if stderr == PIPE:
        p.stderr = _BufferedPipe(p.stderr)
    return p

def server(lang, args = [], stdin = None, stdout = None, stderr = None,
           mapiport = None, xrpcport = None, dbname = os.getenv('TSTDB'),
           dbfarm = None, dbinit = None, bufsize = 0, log = False):
    '''Start a server process.'''
    cmd = _server[:]
    if not cmd:
        if lang in ('mil', 'xquery'):
            cmd = ['Mserver']
        else:
            cmd = ['mserver5']
        cmd.extend(['--set', 'mapi_open=true', '--set', 'gdk_nr_threads=1',
                    '--set', 'xrpc_open=true', '--set', 'monet_prompt=',
                    '--trace'])
    if dbinit is None:
        if lang == 'xquery':
            dbinit = 'module(pathfinder);'
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
    if xrpcport is not None:
        for i in range(len(cmd)):
            if cmd[i][:10] == 'xrpc_port=':
                del cmd[i]
                del cmd[i - 1]
                break
        cmd.append('--set')
        cmd.append('xrpc_port=%d' % int(xrpcport))
    if dbname is not None:
        cmd.append('--set')
        cmd.append('gdk_dbname=%s' % dbname)
    if dbfarm is not None:
        for i in range(len(cmd)):
            if cmd[i][:11] == 'gdk_dbfarm=':
                del cmd[i]
                del cmd[i - 1]
                break
        cmd.append('--set')
        cmd.append('gdk_dbfarm=%s' % dbfarm)
    if verbose:
        print 'Executing', ' '.join(cmd +  args)
        sys.stdout.flush()
    if log:
        prompt = time.strftime('# %H:%M:%S >  ')
        cmdstr = ' '.join(cmd +  args)
        if hasattr(stdin, 'name'):
            cmdstr += ' < "%s"' % stdin.name
        print
        print prompt
        print '%s%s' % (prompt, cmdstr)
        print prompt
        print
        sys.stdout.flush()
        print >> sys.stderr
        print >> sys.stderr, prompt
        print >> sys.stderr, '%s%s' % (prompt, cmdstr)
        print >> sys.stderr, prompt
        print >> sys.stderr
        sys.stderr.flush()
    p = Popen(cmd + args,
              stdin = stdin,
              stdout = stdout,
              stderr = stderr,
              shell = False,
              universal_newlines = True,
              bufsize = bufsize)
    if stdout == PIPE:
        p.stdout = _BufferedPipe(p.stdout)
    if stderr == PIPE:
        p.stderr = _BufferedPipe(p.stderr)
    return p
