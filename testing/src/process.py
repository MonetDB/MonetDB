import subprocess
import os
import sys
import string

from subprocess import PIPE, Popen

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
    return command

_mil_client = splitcommand(os.getenv('MIL_CLIENT', 'mclient -lmil -i'))
_mal_client = splitcommand(os.getenv('MAL_CLIENT', 'mclient -lmal -i'))
_sql_client = splitcommand(os.getenv('SQL_CLIENT', 'mclient -lsql -i'))
_xquery_client = splitcommand(os.getenv('XQUERY_CLIENT', 'mclient -lxquery -fxml'))
_sql_dump = splitcommand(os.getenv('SQL_DUMP', 'msqldump'))
_server = splitcommand(os.getenv('MSERVER', ''))

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

    if port is not None:
        for i in range(len(cmd)):
            if cmd[i][:7] == '--port=':
                del cmd[i]
                break
        cmd.append('--port=%d' % int(port))
    if user is not None:
        for i in range(len(cmd)):
            if cmd[i][:2] == '-u':
                del cmd[i]
                break
        cmd.append('-u%s' % user)
    if passwd is not None:
        for i in range(len(cmd)):
            if cmd[i][:2] == '-P':
                del cmd[i]
                break
        cmd.append('-P%s' % passwd)
    if host is not None:
        for i in range(len(cmd)):
            if cmd[i][:7] == '--host=':
                del cmd[i]
                break
        cmd.append('--host=%s' % host)
    if verbose:
        print 'Executing', ' '.join(cmd +  args)
        sys.stdout.flush()
    return Popen(cmd + args,
                 stdin = stdin,
                 stdout = stdout,
                 stderr = stderr,
                 shell = False,
                 universal_newlines = True)

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
        if lang == 'sql':
            dbinit = 'include sql;'
        elif lang == 'xquery':
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
