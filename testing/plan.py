# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

import re

def filter(tab):
    res = []
    for row in tab:
        if row[0].find('usec') < 0:
            s = re.sub(r'hugeint\(64\)', r'bigint(63)', row[0])
            res.append([s])
    return res
