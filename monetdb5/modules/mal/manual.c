/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * This module provides a wrapping of the help function in the .../mal/mal_modules.c
 * and the list of all MAL functions for analysis using SQL.
 */
#include "monetdb_config.h"
#include "manual.h"

str
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
		throw(MAL, "manual.functions", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
						(void) fcnDefinition(t->def, getInstrPtr(t->def, 0), buf, TRUE, buf, sizeof(buf));
						tt = strstr(buf, "address ");
						if (tt) {
							*tt = 0;
							tt += 8;
						}
						if (BUNappend(mod, t->def->stmt[0]->modname, FALSE) != GDK_SUCCEED ||
							BUNappend(fcn, t->def->stmt[0]->fcnname, FALSE) != GDK_SUCCEED ||
							BUNappend(com, t->def->help ? t->def->help : "", TRUE) != GDK_SUCCEED ||
							BUNappend(sig,buf,TRUE) != GDK_SUCCEED ||
							BUNappend(adr, tt ? tt : "", TRUE) != GDK_SUCCEED) {
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
