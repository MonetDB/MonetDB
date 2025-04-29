# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

from pymonetdb import *
connect_orig = connect
def connect(*args, **kwargs):
    import os
    timeout = int(os.environ.get('TIMEOUT', '0'))
    if timeout > 0:
        kwargs = kwargs.copy()
        kwargs['connect_timeout'] = 20
    h = connect_orig(*args, **kwargs)
    if timeout > 0:
        h.settimeout(timeout)
        with h.cursor() as c:
            c.execute(f'call sys.setsessiontimeout({timeout})')
    return h
