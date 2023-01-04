/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _FOR_H
#define _FOR_H

#include "sql.h"

//extern BAT *FORdecompress_(BAT *o, lng minval, int type, role_t role);
extern str FORcompress_col(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str FORdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _FOR_H */

