# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.

from pymonetdb import *
from pymonetdb import __version__
from pymonetdb import __all__

import os
import time
_timeout = int(os.getenv('TIMEOUT', '0'))
_starttime = time.time() if _timeout > 0 else 0
_endtime = _starttime + _timeout

connect_orig = connect


def connect(*args, **kwargs):
    if _timeout > 0:
        curtime = time.time()
        if curtime >= _endtime:
            raise TimeoutError('Timed out')
        kwargs = kwargs.copy()
        kwargs['connect_timeout'] = 20
        timeout = _endtime - curtime  # time remaining
    else:
        timeout = 0
    h = connect_orig(*args, **kwargs)
    if _timeout > 0:
        h.settimeout(timeout)
        with h.cursor() as c:
            c.execute(f'call sys.setsessiontimeout({timeout})')
    return h
