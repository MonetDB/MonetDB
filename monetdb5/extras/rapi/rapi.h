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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * H. Muehleisen, M. Kersten
 * The R interface
 */
#ifndef _RAPI_LIB_
#define _RAPI_LIB_

#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#ifdef WIN32
#ifndef LIBRAPI
#define rapi_export extern __declspec(dllimport)
#else
#define rapi_export extern __declspec(dllexport)
#endif
#else
#define rapi_export extern
#endif

#define RAPI_MAX_TUPLES 2147483647L

rapi_export str RAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
		InstrPtr pci, bool grouped);
rapi_export str RAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
		InstrPtr pci);
rapi_export str RAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
		InstrPtr pci);

rapi_export str RAPIprelude(void);

rapi_export void writeConsoleEx(const char * buf, int buflen, int foo);
rapi_export void writeConsole(const char * buf, int buflen);
rapi_export void clearRErrConsole(void);

char* rtypename(int rtypeid);
int RAPIEnabled(void);

#endif /* _RAPI_LIB_ */
