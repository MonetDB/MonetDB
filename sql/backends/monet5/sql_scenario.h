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

#ifndef _SQL_SCENARIO_H_
#define _SQL_SCENARIO_H_
#include "sql.h"

extern int SQLdebug;
extern str SQLprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLepilogue(void *ret);

sql5_export str SQLautocommit(mvc *m);
sql5_export str SQLtrans(mvc *m);

sql5_export str SQLexitClient(Client c);
sql5_export str SQLresetClient(Client c);
sql5_export str SQLinitClient(Client c, const char *passwd, const char *challenge, const char *algo);
sql5_export str SQLinitClientFromMAL(Client c, const char *passwd, const char *challenge, const char *algo);
sql5_export str SQLengine_(Client c);
sql5_export void SQLengine(Client c);
extern str handle_error(mvc *m, int pstatus, str msg);

extern str SQLstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLinclude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLescapeString(str s);

extern str SYSupdate_tables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SYSupdate_schemas(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_SCENARIO_H_ */
