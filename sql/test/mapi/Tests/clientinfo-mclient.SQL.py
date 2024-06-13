import os
import subprocess
import urllib.parse

TSTDB = os.environ['TSTDB']
MAPIPORT = os.environ['MAPIPORT']

QUERY = """\
SELECT *
FROM sys.sessions
WHERE sessionid = current_sessionid()
"""

def run_mclient(**extra_args):
    url = f'monetdb://localhost:{MAPIPORT}/{TSTDB}'
    if extra_args:
        url += '?' + urllib.parse.urlencode(extra_args)
    cmd = [ 'mclient', '-d', url, '-fexpanded', '-s', QUERY ]
    out = subprocess.check_output(cmd, encoding='latin1')
    fields = dict()
    for line in out.splitlines()[1:]:
        line = line.strip()
        if line:
            k, v = line.split('|', 1)
            k = k.strip()
            v = v.strip()
            fields[k] = v
    return fields


#######################################################################
# By default, most fields get filled in

fields = run_mclient()

assert fields['language'] == 'sql',\
    f'Found {fields["language"]!r}'

assert fields['peer'] == '<UNIX SOCKET>' or ']:' in fields['peer'],\
    f'Found {fields["peer"]!r}'

assert fields['hostname'] != 'null',\
    f'Found {fields["hostname"]!r}'

# could be mclient-11.51.0, mclient.exe, or whatever
assert fields['application'].startswith('mclient'),\
    f'Found {fields["application"]!r}'

assert fields['client'].startswith('libmapi '),\
    f'Found {fields["client"]!r}'

assert fields['clientpid'] != 'null' and int(fields['clientpid']) > 0,\
    f'Found {fields["clientpid"]!r}'

assert fields['remark'] == 'null',\
    f'Found {fields["remark"]!r}'


#######################################################################
# client_info=off suppresses everything sent by the client.
# Server still fills in language and peer

fields = run_mclient(client_info='off')

assert fields['language'] == 'sql',\
    f'Found {fields["language"]!r}'

assert fields['peer'] == '<UNIX SOCKET>' or ']:' in fields['peer'],\
    f'Found {fields["peer"]!r}'

assert fields['hostname'] == 'null',\
    f'Found {fields["hostname"]!r}'

# could be mclient-11.51.0, mclient.exe, or whatever
assert fields['application'] == 'null',\
    f'Found {fields["application"]!r}'

assert fields['client'] == 'null',\
    f'Found {fields["client"]!r}'

assert fields['clientpid'] == 'null',\
    f'Found {fields["clientpid"]!r}'

assert fields['remark'] == 'null',\
    f'Found {fields["remark"]!r}'

#######################################################################
# We can override application and remark

fields = run_mclient(client_application='app', client_remark='mark')

# could be mclient-11.51.0, mclient.exe, or whatever
assert fields['application'] == 'app',\
    f'Found {fields["application"]!r}'

assert fields['remark'] == 'mark',\
    f'Found {fields["remark"]!r}'


#######################################################################
# We can override application and remark, but client_info=off
# suppresses that.

fields = run_mclient(client_application='app', client_remark='mark', client_info='off')

# could be mclient-11.51.0, mclient.exe, or whatever
assert fields['application'] == 'null',\
    f'Found {fields["application"]!r}'

assert fields['remark'] == 'null',\
    f'Found {fields["remark"]!r}'

