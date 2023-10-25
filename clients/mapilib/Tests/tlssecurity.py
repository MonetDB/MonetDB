

import logging
import os
import subprocess
import sys
import threading

sys.path.append(os.environ.get('TSTSRCDIR','.'))
import tlstester

level = logging.WARNING
if sys.platform == 'win32':
    level=logging.DEBUG
#level = logging.DEBUG
logging.basicConfig(level=level)

tgtdir = os.environ['TSTTRGDIR']
assert os.path.isdir(tgtdir)

hostnames = ['localhost']
# Generate certificates and write them to the scratch dir
# Write them to the scratch dir for inspection by the user.
certs = tlstester.Certs(hostnames)
certsdir = os.path.join(tgtdir, "certs")
try:
    os.mkdir(certsdir)
except FileExistsError:
    pass
count = 0
for name, content in certs.all().items():
    with open(os.path.join(certsdir, name), "wb") as a:
        a.write(content)
        count += 1
logging.debug(f"Wrote {count} files to {certsdir}")

def certpath(name):
    return os.path.join(certsdir, name)
def certbytes(name):
    filename = certpath(name)
    with open(filename, 'rb') as f:
        return f.read()

# Start the worker threads

server = tlstester.TLSTester(
    certs=certs,
    listen_addr='127.0.0.1',
    preassigned=dict(),
    sequential=False,
    hostnames=hostnames)
server_thread = threading.Thread(target=server.serve_forever, daemon=True)
server_thread.start()

def attempt(portname: str, expected_error: str, tls=True, **params):
    port = server.get_port(portname)
    scheme = 'monetdbs' if tls else 'monetdb'
    url = f"{scheme}://localhost:{port}/demo"
    if params:
        # should be percent-escaped
        url += '?' + '&'.join(f"{k}={v}" for k, v in params.items())
    logging.debug(f"Connecting to {url}, expected_error={expected_error}")
    cmd = ['mclient', '-d', url]
    logging.debug(f"cmd={cmd}")
    proc = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    logging.debug(f"mclient exited with code {proc.returncode}, err={proc.stderr}")
    assert proc.returncode == 2, f"mclient is supposed to exit with status 2, not {proc.returncode}"
    output = str(proc.stderr, 'utf-8').rstrip()
    actual_error = None if 'Sorry, this is not' in output else output

    if expected_error is None and actual_error is None:
        logging.debug("Test succeeded")
        return
    if expected_error is not None and actual_error is not None and expected_error in actual_error:
        logging.debug("Test succeeded")
        return
    logging.error(f"Unexpected result when connecting to port {port} ('{portname}')")
    logging.error(f"Using URL {url}")
    message = f"expected_error={expected_error} but actual_error={actual_error}"
    logging.error(message)
    raise Exception(message)


# Follow the test cases laid out in the README of the tlstester
# https://github.com/MonetDB/monetdb-tlstester#suggested-test-cases


# connect_plain
#
# Connect to port 'plain', without using TLS. Have a succesful MAPI exchange.

attempt('plain', None, tls=False)

# connect_tls
#
# Connect to port 'server1' over TLS, verifying the connection using ca1.crt.
# Have a succesful MAPI exchange.

attempt('server1', None, cert=certpath('ca1.crt'))

# refuse_no_cert
#
# Connect to port 'server1' over TLS, without passing a certificate. The
# connection should fail because ca1.crt is not in the system trust root store.

attempt('server1', "verify failed")

# refuse_wrong_cert
#
# Connect to port 'server1' over TLS, verifying the connection using ca2.crt.
# The client should refuse to let the connection proceed.

attempt('server1', 'verify failed', cert=certpath('ca2.crt'))

# refuse_tlsv12
#
# Connect to port 'tls12' over TLS, verifying the connection using ca1.crt. The
# client should refuse to let the connection proceed because it should require
# at least TLSv1.3.

attempt('tls12', 'protocol version', cert=certpath('ca1.crt'))

# refuse_expired
#
# Connect to port 'expiredcert' over TLS, verifying the connection using
# ca1.crt. The client should refuse to let the connection proceed.

attempt('expiredcert', 'verify failed', cert=certpath('ca1.crt'))

# connect_client_auth
#
# Connect to port 'clientauth' over TLS, verifying the connection using ca1.crt.
# Authenticate using client2.key and client2.crt. Have a succesful MAPI
# exchange.

# TODO
#attempt('clientauth', None, cert=certpath('ca1.crt'),clientcert=certpath('client2.crt'), clientkey=certpath('client2.key'))

# fail_plain_to_tls
#
# Connect to port 'plain' over TLS. This should fail, not hang.

attempt('plain', 'wrong version number', tls=True)

# fail_tls_to_plain
#
# Make a plain MAPI connection to port 'server1'. This should fail.

attempt('server1', 'terminated', tls=False)

# connect_trusted
#
# Only when running in a throwaway environment such as a Docker container:
# Install ca3.crt in the system root certificate store. This is highly
# system-specific. Connect to port 'server3' over TLS without passing a
# certificate to check. The implementation should pick it up from the system
# store. Have a succesful MAPI exchange.

# TODO
#attempt('server3', None)


# Uncomment to keep the server running so you
# can run some experiments from the command line

# logging.warning("sleeping"); import time; time.sleep(86400)
