

import subprocess
import sys

# Test that certificates from the system trust store are used when no explicit
# certificate is given.
#
# # If all goes well, this will give a protocol error (MAPI != HTTP),
# not a TLS error

# Ideally we'd use www.monetdb.org but, but at the time of writing www.monetdb.org
# only returns an error after a 20 second time out.
#
# python.org on the other hand uses a CDN which kicks us out instantly.
HOST = 'python.org'

# Run mclient
cmd = ['mclient', '-L-', '-d', f"monetdbs://{HOST}:443/demo"]
proc = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
assert proc.returncode == 2, f"mclient is supposed to exit with status 2, not {proc.returncode}"

# We expect the server to send something like 'HTTP/1.1 400 Bad Request'
# libmapi will interpret the first two bytes as a block header.
#
# In ASCII, 'H' + 256 * 'T' is 21576.
if b'21576' not in proc.stderr:
    msg = str(proc.stderr, 'utf-8')
    print(f"Expected mclient to print an error message containing the number 21576, got:\n--- stderr ---\n{msg}\n---end stderr ---", file=sys.stderr)
    exit(1)

