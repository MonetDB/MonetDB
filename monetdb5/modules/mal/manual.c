/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * This module provides a wrapping of the help function in the .../mal/mal_modules.c
 * and the list of all MAL functions for analysis using SQL.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include <time.h>
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

static str
MANUALcreateOverview(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *mod, *fcn, *sig, *adr, *com;
	bat *mx = getArgReference_bat(stk,pci,0);
	bat *fx = getArgReference_bat(stk,pci,1);
	bat *sx = getArgReference_bat(stk,pci,2);
	bat *ax = getArgReference_bat(stk,pci,3);
	bat *cx = getArgReference_bat(stk,pci,4);
	Module s;
	Module* moduleList;
	int length;
	int j, k, top = 0;
	Symbol t;
	Module list[256];
	char buf[BUFSIZ], *tt;

	mod = COLnew(0, TYPE_str, 0, TRANSIENT);
	fcn = COLnew(0, TYPE_str, 0, TRANSIENT);
	sig = COLnew(0, TYPE_str, 0, TRANSIENT);
	adr = COLnew(0, TYPE_str, 0, TRANSIENT);
	com = COLnew(0, TYPE_str, 0, TRANSIENT);
	if (mod == NULL || fcn == NULL || sig == NULL || adr == NULL || com == NULL) {
		BBPreclaim(mod);
		BBPreclaim(fcn);
		BBPreclaim(sig);
		BBPreclaim(adr);
		BBPreclaim(com);
		throw(MAL, "manual.functions", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	list[top++] = cntxt->usermodule;
	getModuleList(&moduleList, &length);
	if (moduleList == NULL)
		goto bailout;
	while (top < 256 && top <= length) {
		list[top] = moduleList[top - 1];
		top++;
	}
	freeModuleList(moduleList);

	for (k = 0; k < top; k++) {
		s = list[k];
		if (s->space) {
			for (j = 0; j < MAXSCOPE; j++) {
				if (s->space[j]) {
					for (t = s->space[j]; t != NULL; t = t->peer) {
						if (t->def->stmt[0]->fcnname[0] == '#')
							continue;
						(void) fcnDefinition(t->def, getInstrPtr(t->def, 0), buf, TRUE, buf, sizeof(buf));
						tt = strstr(buf, "address ");
						if (tt) {
							*tt = 0;
							tt += 8;
						}
						if (BUNappend(mod, t->def->stmt[0]->modname, false) != GDK_SUCCEED ||
							BUNappend(fcn, t->def->stmt[0]->fcnname, false) != GDK_SUCCEED ||
							BUNappend(com, t->def->help ? t->def->help : "", false) != GDK_SUCCEED ||
							BUNappend(sig,buf,false) != GDK_SUCCEED ||
							BUNappend(adr, tt ? tt : "", false) != GDK_SUCCEED) {
							goto bailout;
						}
					}
				}
			}
		}
	}

	BBPkeepref( *mx = mod->batCacheid);
	BBPkeepref( *fx = fcn->batCacheid);
	BBPkeepref( *sx = sig->batCacheid);
	BBPkeepref( *ax = adr->batCacheid);
	BBPkeepref( *cx = com->batCacheid);
	(void)mb;
	return MAL_SUCCEED;

  bailout:
	BBPreclaim(mod);
	BBPreclaim(fcn);
	BBPreclaim(sig);
	BBPreclaim(adr);
	BBPreclaim(com);
	throw(MAL, "manual.functions", GDK_EXCEPTION);
}

#include "mel.h"
mel_func manual_init_funcs[] = {
 pattern("manual", "functions", MANUALcreateOverview, false, "Produces a table with all MAL functions known", args(5,5, batarg("mod",str),batarg("fcn",str),batarg("sig",str),batarg("adr",str),batarg("com",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_manual_mal)
{ mal_module("manual", NULL, manual_init_funcs); }
