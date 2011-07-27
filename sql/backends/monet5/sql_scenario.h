/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _SQL_SCENARIO_H_
#define _SQL_SCENARIO_H_
#include "sql.h"

/* #define _SQL_SCENARIO_DEBUG */
/* #define _SQL_READER_DEBUG */
/* #define _SQL_PARSER_DEBUG */

sql5_export str SQLsession(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLsession2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLprelude(void);
sql5_export str SQLepilogue(void);

sql5_export str SQLexit(Client c);
sql5_export str SQLexitClient(Client c);
sql5_export str SQLinitClient(Client c);
sql5_export str SQLreader(Client c);
sql5_export str SQLparser(Client c);
sql5_export str SQLengine(Client c);
sql5_export str SQLassert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLassertInt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLassertWrd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLassertLng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str SQLinitEnvironment(Client cntxt);
sql5_export str SQLstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLstatementIntern(Client c, str *expr, str nme, int execute, bit output);
sql5_export str SQLcompile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLinclude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str RAstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLtrace(int *ret);
sql5_export str SQLCacheRemove(Client c, str nme);

sql5_export MT_Lock sql_contextLock;
#endif /* _SQL_SCENARIO_H_ */

