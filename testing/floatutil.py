# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

from decimal import Decimal
from re import sub

def do_round(res):
    return str(round(Decimal(res.group(0)), 3))

def filter(tab):
    res = []
    for row in tab:
        res.append([sub(r'\b([0-9]+\.[0-9]{4,})\b', do_round, row[0])])
    return sorted(res)
