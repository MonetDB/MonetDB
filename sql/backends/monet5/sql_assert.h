/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _SQL_ASSERT_H_
#define _SQL_ASSERT_H_
#include "sql.h"

extern str SQLassert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLassertInt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLassertLng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#ifdef HAVE_HGE
extern str SQLassertHge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

#endif /* _SQL_ASSERT_H_ */
