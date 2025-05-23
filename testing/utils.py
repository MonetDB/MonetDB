# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

def parse_mapi_err_msg(error:str=''):
    """Parse error string and returns (err_code, err_msg) tuple
    """
    err_code = None
    err_msg = None
    tmp = error.split('!', 1)
    if len(tmp) > 1:
        try:
            err_code = tmp[0].strip()
        except (ValueError, TypeError):
            pass
        err_msg = tmp[1].strip()
    elif len(tmp) == 1:
        if tmp[0]:
            err_msg = tmp[0].strip()
    return err_code, err_msg
