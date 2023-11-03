# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.


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

# After the TLS handshake succeeds we expect the server to send something like
# 'HTTP/1.1 400 Bad Request' because we're sending \x00\x00 instead of an HTTP
# request. libmapi will interpret the first two bytes 'H' and 'T' as an invalid
# block header.
#
# In ASCII,  'H' + 256 * 'T'  ==  72 + 256 * 84  ==  21576.
tls_works_but_mapi_fails = b'21576' in proc.stderr

if not tls_works_but_mapi_fails:
    msg = str(proc.stderr, 'utf-8')
    print(f"Expected mclient to print an error message containing the number 21576, got:\n--- stderr ---\n{msg}\n---end stderr ---", file=sys.stderr)
    exit(1)

