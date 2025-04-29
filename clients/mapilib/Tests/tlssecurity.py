# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

from hashlib import sha256
import logging
import os
import re
import subprocess
import sys

from MonetDBtesting.tlstester import TLSTesterClient

log_level = logging.WARNING
log_format = '%(levelname)s:t=%(relativeCreated)d:%(name)s:%(message)s'# if sys.platform == 'win32':

# Be verbose but make Mtest ignore it
log_level = logging.DEBUG
logging.addLevelName(logging.DEBUG, '#DEBUG')
logging.addLevelName(logging.INFO, '#INFO')

logging.basicConfig(level=log_level,format=log_format)

# A tmpdir to write certificates to
tgtdir = os.environ['TSTTRGDIR']
assert os.path.isdir(tgtdir)
scratchdir = os.path.join(tgtdir, "scratch")
logging.debug(f"scratchdir={scratchdir}")

tlstester = TLSTesterClient(scratchdir)


def certpath(name):
    return tlstester.download(name)

def attempt(experiment: str, portname: str, expected_error_regex: str, tls=True, host='localhost', **params):
    port = tlstester.get_port(portname)
    scheme = 'monetdbs' if tls else 'monetdb'
    url = f"{scheme}://{host}:{port}/demo"
    if params:
        # should be percent-escaped
        url += '?' + '&'.join(f"{k}={v}" for k, v in params.items())
    logging.info(f"**** START TEST {experiment}")
    logging.info(f"Connecting to {url}, expected_error={expected_error_regex}")
    test_log_file = os.path.join(scratchdir, portname + '.log')
    cmd = ['mclient', '-d', url, '-L', test_log_file]
    logging.info(f"cmd={cmd}")
    proc = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    logging.info(f"mclient exited with code {proc.returncode}, err={proc.stderr}")
    with open(test_log_file, 'r') as f:
        for line in f:
            logging.debug(f'mclient log: {line.rstrip()}')
    if proc.returncode != 2:
        msg = str(proc.stderr, 'utf-8')
        print(f"mclient is supposed to exit with status 2, not {proc.returncode}.\n--- stderr ---\n{msg}\n---end stderr ---", file=sys.stderr)
        assert proc.returncode == 2, f"mclient is supposed to exit with status 2, not {proc.returncode}"
    output = str(proc.stderr, 'utf-8').rstrip()
    actual_error = None if 'Sorry, this is not' in output else output

    ok = False
    if expected_error_regex is None and actual_error is None:
        ok = True
    elif  expected_error_regex is not None and actual_error is not None and re.search(expected_error_regex, actual_error):
        ok = True
    if ok:
        logging.info(f"**** END SUCCESSFUL TEST {experiment} ")
        return
    logging.error(f"Unexpected result for test {experiment}")
    logging.error(f"When connecting to port '{portname}' using URL {url}")
    logging.error(f"Expected error message matching {expected_error_regex!r}")
    logging.error(f"Found error message: {actual_error}")
    message = f"expected_error_regex={expected_error_regex!r} but actual_error={actual_error!r}"
    raise Exception(message)


# Follow the test cases laid out in the README of the tlstester
# https://github.com/MonetDB/monetdb-tlstester#suggested-test-cases


# connect_plain
#
# Connect to port 'plain', without using TLS. Have a successful MAPI exchange.

attempt('connect_plain', 'plain', None, tls=False)

# connect_tls
#
# Connect to port 'server1' over TLS, verifying the connection using ca1.crt.
# Have a successful MAPI exchange.

attempt('connect_tls', 'server1', None, cert=certpath('ca1.crt'))

# refuse_no_cert
#
# Connect to port 'server1' over TLS, without passing a certificate. The
# connection should fail because ca1.crt is not in the system trust root store.

attempt('refuse_no_cert', 'server1', "") # we expect "verify failed" but Mac gives "No such file or directory", i.e. still an error so we take it

# refuse_wrong_cert
#
# Connect to port 'server1' over TLS, verifying the connection using ca2.crt.
# The client should refuse to let the connection proceed.

attempt('refuse_wrong_cert', 'server1', 'verify failed', cert=certpath('ca2.crt'))

# refuse_wrong_host
#
# Connect to port 'server1' over TLS, but using an alternative host name.
# For example, `localhost.localdomain` instead of `localhost`.
# The client should refuse to let the connection proceed.

attempt('refuse_wrong_host', 'server1', 'verify failed', host='localhost.localdomain', cert=certpath('ca1.crt'))

# refuse_tlsv12
#
# Connect to port 'tls12' over TLS, verifying the connection using ca1.crt. The
# client should refuse to let the connection proceed because it should require
# at least TLSv1.3.

attempt('refuse_tlsv12', 'tls12', 'protocol version', cert=certpath('ca1.crt'))

# refuse_expired
#
# Connect to port 'expiredcert' over TLS, verifying the connection using
# ca1.crt. The client should refuse to let the connection proceed.

attempt('refuse_expired', 'expiredcert', 'verify failed', cert=certpath('ca1.crt'))

# connect_client_auth
#
# Connect to port 'clientauth' over TLS, verifying the connection using ca1.crt.
# Authenticate using client2.key and client2.crt. Have a successful MAPI
# exchange.

attempt('connect_client_auth', 'clientauth', None, cert=certpath('ca1.crt'), clientcert=certpath('client2.crt'), clientkey=certpath('client2.key'))

# fail_tls_to_plain
#
# Connect to port 'plain' over TLS. This should fail, not hang.

attempt('fail_tls_to_plain', 'plain', 'wrong version number', tls=True)

# fail_plain_to_tls
#
# Make a plain MAPI connection to port 'server1'. This should fail.

attempt('fail_plain_to_tls', 'server1', 'Connection terminated while starting handshake|Challenge string is not valid', tls=False)

# connect_server_name
#
# Connect to port 'sni' over TLS. Have a successful MAPI exchange. This indicates
# that the implementation sent a correct Server Name Indication.

attempt('connect_server_name', 'sni', None, cert=certpath('ca1.crt'))

# connect_alpn_mapi9
#
# Connect to port 'alpn_mapi9' over TLS. Have a successful MAPI exchange. This
# indicates that the implementation successfully negotiated ALPN protocol
# "mapi/9".

attempt('connect_alpn_mapi9', 'alpn_mapi9', None, cert=certpath('ca1.crt'))

# connect_right_hash
#
# Connect to port 'server1' over TLS, with certhash set to a prefix of the hash
# of the server certificate in DER form. Have a successful MAPI exchange.

server1hash = sha256(tlstester.fetch('server1.der')).hexdigest()
attempt('connect_right_hash', 'server1', None, certhash='sha256:' + server1hash[:6])

# connect_wrong_hash
#
# Connect to port 'server1' over TLS, with certhash set to a syntactically valid
# hash that is not a prefix of the hash of the server certificate in DER form.
# This should fail.

first_digit = server1hash[0]
other_digit = f"{8 ^ int(first_digit, 16):x}"
wronghash = other_digit + server1hash[1:]

attempt('connect_wrong_hash', 'server1', "does not match certhash", certhash='sha256:' + wronghash[:6])


# connect_ca_hash
#
# Connect to port 'server1' over TLS, with certhash set to a prefix of the hash
# of the CA1 certificate in DER form. This should fail.

ca1hash = sha256(tlstester.fetch('ca1.der')).hexdigest()
attempt('connect_ca_hash', 'server1', "does not match certhash", certhash='sha256:' + ca1hash[:6])



# connect_trusted
#
# Only when running in a throwaway environment such as a Docker container:
# Install ca3.crt in the system root certificate store. This is highly
# system-specific. Connect to port 'server3' over TLS without passing a
# certificate to check. The implementation should pick it up from the system
# store. Have a successful MAPI exchange.
#
# Tested elsewhere, in test systemcertificates

# TODO
#attempt('connect_trusted', 'server3', None)


# Uncomment to keep the server running so you
# can run some experiments from the command line

# logging.warning("sleeping"); import time; time.sleep(86400)
