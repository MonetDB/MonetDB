/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _SQL_SCENARIO_H_
#define _SQL_SCENARIO_H_
#include "sql.h"

/* #define _SQL_SCENARIO_DEBUG */
/* #define _SQL_READER_DEBUG */
/* #define _SQL_PARSER_DEBUG */

sql5_export int SQLdebug;
sql5_export str SQLsession(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLsession2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLprelude(void *ret);
sql5_export str SQLepilogue(void *ret);

sql5_export int SQLautocommit(Client c, mvc *m);
sql5_export void SQLtrans(mvc *m);

sql5_export str SQLexit(Client c);
sql5_export str SQLexitClient(Client c);
sql5_export str SQLinitClient(Client c);
sql5_export str SQLreader(Client c);
sql5_export str SQLparser(Client c);
sql5_export str SQLengine(Client c);
sql5_export str SQLassert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLassertInt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLassertLng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export int handle_error(mvc *m, stream *out, int pstatus);
#ifdef HAVE_HGE
sql5_export str SQLassertHge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

sql5_export str SQLinitEnvironment(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLstatementIntern(Client c, str *expr, str nme, bit execute, bit output, res_table **result);
sql5_export str SQLcompile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLinclude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLCacheRemove(Client c, str nme);
sql5_export str SQLescapeString(str s);

sql5_export MT_Lock sql_contextLock;
#endif /* _SQL_SCENARIO_H_ */
