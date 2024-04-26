# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

# Test that certificates from the system trust store are used when no explicit
# certificate is given.
#
# This test tries to connect to an existing TLS+MAPI server that has a publicly
# trusted certificate. This needs to be configured externally through
# the environment variables TLSTEST_URL and TLSTEST_ALTURL.
#
# These must point to the same server so they receive the same certificate.
# However, the host name in TLSTEST_URL must match the certificate while the
# hostname in TLSTEST_ALT doesn't match.


import logging
import os
import shlex
import socket
import subprocess
import ssl
import sys
import urllib
from urllib.parse import urlparse

level = logging.WARNING
# if sys.platform == 'win32':
#     level=logging.DEBUG
if '-v' in sys.argv:
    level = logging.DEBUG
# level = logging.DEBUG
logging.basicConfig(level=level)

logger = logging.root

#####################################################################
# Look for the URL configuration variables, exit if not present

def getvar(name):
    value = os.environ.get(name)
    if value:
        logger.debug(f"Environment variable {name}={value}")
        return value
    else:
        logger.debug(f"Environment variable {name} is not set, exiting early")
        exit(0)

url = getvar('TLSTEST_URL')
alturl = getvar('TLSTEST_ALTURL')


#####################################################################
# Before trying with mclient, connect directly to see if the server is
# up and behaves the way we need for this test

def try_connect(url, check_hostname, expected_error):
    logger.info(f"Attempting to connect to {url}, check_hostname={check_hostname}")
    parsed = urlparse(url)
    assert parsed.scheme == 'monetdbs', f'Expected scheme monetdbs, not {parsed.scheme}'

    host = parsed.hostname
    port = parsed.port or 50000
    logger.debug(f"Connecting to host {host!r} port {port!r}")

    with socket.create_connection((host, port)) as sock:
        logger.debug(f"Connection established")

        logger.debug(f"Verifying TLS")
        ctx = ssl.create_default_context()
        ctx.check_hostname = check_hostname
        try:
            ssl_connection = ctx.wrap_socket(sock, server_hostname=host)
            # if we get here it succeeded
            if expected_error:
                raise Exception(f"Verification succeeded unexpectedly")
            else:
                logger.debug(f"Verification succeeded as expected")
            logger.debug("Closing")
            ssl_connection.close()
        except ssl.SSLError as e:
            if expected_error and expected_error in str(e):
                logger.debug(f"Verification failed as expected: {e}")
            elif expected_error:
                logger.error(f"Verification failed but the error did not match {expected_error!r}: {e}")
            else:
                logger.error(f"Verification failed unexpectedly: {e}")
                raise e


# both urls should be reachable and present a TLS certificate
try_connect(url, check_hostname=False, expected_error=None)
try_connect(alturl, check_hostname=False, expected_error=None)

# url should have the expected hostname so this should succeed
try_connect(url, check_hostname=True, expected_error=None)

# alturl should have a hostname that makes the verification fail
try_connect(alturl, check_hostname=True, expected_error='Hostname mismatch')


#####################################################################
# The URLs and the server are configured correctly.
# Check if mclient notices the wrong host name in alturl.

def run_mclient(url, should_pass):
    cmd = ['mclient', '-L-', '-s', 'SELECT 42', '-d', url]
    display_cmd = ' '.join(shlex.quote(w) for w in cmd)
    logger.info(f"Running {display_cmd}")
    proc = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    status = proc.returncode
    logger.debug(f"Mclient exited with status {status}")

    err = proc.stderr
    logger.debug('--- stderr ---')
    try:
        for line in str(err, 'utf-8').splitlines():
            logger.debug(line)
    except UnicodeDecodeError:
        logger.debug(err)
    logger.debug('--- end of stderr ---')

    passed_verification = (status == 0 or b'Sorry, this is not a real' in err)
    msg = f"passed_verification={passed_verification}, should_pass={should_pass}"

    if passed_verification == should_pass:
        logger.debug(f"{msg}, as expected")
    else:
        logger.error(f"{msg}, unexpected!")
        raise Exception(msg)

run_mclient(url, should_pass=True)

run_mclient(alturl, should_pass=False)
