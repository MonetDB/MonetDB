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

#ifndef _SQL_EXECUTE_H_
#define _SQL_EXECUTE_H_
#include "sql.h"

sql5_export str SQLstatementIntern(Client c, const char *expr, const char *nme, bit execute, bit output, res_table **result);
sql5_export str RAstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str RAstatement2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str RAstatementEnd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export void SQLdestroyResult(res_table *destroy);
char *SQLrun(Client c, mvc *m);

#endif /* _SQL_EXECUTE_H_ */
