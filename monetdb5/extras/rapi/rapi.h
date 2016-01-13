/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
		InstrPtr pci, bit grouped);
rapi_export str RAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
		InstrPtr pci);
rapi_export str RAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
		InstrPtr pci);

rapi_export str RAPIprelude(void *ret);

rapi_export void writeConsoleEx(const char * buf, int buflen, int foo);
rapi_export void writeConsole(const char * buf, int buflen);
rapi_export void clearRErrConsole(void);

char* rtypename(int rtypeid);
int RAPIEnabled(void);

#endif /* _RAPI_LIB_ */
