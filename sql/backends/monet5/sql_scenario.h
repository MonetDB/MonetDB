/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _SQL_SCENARIO_H_
#define _SQL_SCENARIO_H_
#include "sql.h"

extern int SQLdebug;
extern str SQLprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLepilogue(void *ret);

extern str SQLautocommit(mvc *m);
extern str SQLtrans(mvc *m);

sql5_export str SQLexit(Client c);
sql5_export str SQLexitClient(Client c);
sql5_export str SQLresetClient(Client c);
sql5_export str SQLinitClient(Client c);
sql5_export str SQLinitClientFromMAL(Client c);
sql5_export str SQLreader(Client c);
sql5_export str SQLparser(Client c);
sql5_export str SQLengine(Client c);
sql5_export str SQLcallback(Client c, str msg);
extern str handle_error(mvc *m, int pstatus, str msg);

extern str SQLstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLcompile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLinclude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLCacheRemove(Client c, const char *nme);
sql5_export str SQLescapeString(str s);

extern str SYSupdate_tables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SYSupdate_schemas(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_SCENARIO_H_ */
