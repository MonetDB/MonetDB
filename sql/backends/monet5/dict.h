/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _DICT_H
#define _DICT_H

#include "sql.h"

extern str FORcompress_col(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str FORdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

extern str DICTcompress_col(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

extern str DICTcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTconvert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTthetaselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTrenumber(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _DICT_H */

