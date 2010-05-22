import subprocess
import os
import sys
import string
import tempfile
import copy
import atexit

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

_mil_client = splitcommand(os.getenv('MIL_CLIENT', 'mclient -lmil -i'))
_mal_client = splitcommand(os.getenv('MAL_CLIENT', 'mclient -lmal -i'))
_sql_client = splitcommand(os.getenv('SQL_CLIENT', 'mclient -lsql -i'))
_xquery_client = splitcommand(os.getenv('XQUERY_CLIENT', 'mclient -lxquery -fxml'))
_sql_dump = splitcommand(os.getenv('SQL_DUMP', 'msqldump'))
_server = splitcommand(os.getenv('MSERVER', ''))

_dotmonetdbfile = []

def _delfiles():
    for f in _dotmonetdbfile:
        try:
            os.unlink(f)
        except OSError:
            pass

atexit.register(_delfiles)

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

def client(lang, args = [], stdin = None, stdout = None, stderr = None,
           port = os.getenv('MAPIPORT'), host = None,
           user = 'monetdb', passwd = 'monetdb'):
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
    p = Popen(cmd + args,
              stdin = stdin,
              stdout = stdout,
              stderr = stderr,
              shell = False,
              env = env,
              universal_newlines = True)
    p.dotmonetdbfile = fnam
    return p

def server(lang, args = [], stdin = None, stdout = None, stderr = None,
           mapiport = None, xrpcport = None, dbname = os.getenv('TSTDB'),
           dbfarm = None, dbinit = None, bufsize = 0):
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
    return Popen(cmd + args,
                 stdin = stdin,
                 stdout = stdout,
                 stderr = stderr,
                 shell = False,
                 universal_newlines = True,
                 bufsize = bufsize)
