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
#include "mal_interpreter.h"
#include "mal_namespace.h"

static str
MANUALcreateOverview(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *mod, *fcn, *sig, *adr, *com;
	bat *mx = getArgReference_bat(stk, pci, 0);
	bat *fx = getArgReference_bat(stk, pci, 1);
	bat *sx = getArgReference_bat(stk, pci, 2);
	bat *ax = getArgReference_bat(stk, pci, 3);
	bat *cx = getArgReference_bat(stk, pci, 4);
	Module *moduleList;
	int length;

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

	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);
	getModuleList(ta, &moduleList, &length);
	if (moduleList == NULL)
		goto bailout;

	for (int k = 0; k <= length; k++) {
		Module s = k < length ? moduleList[k] : cntxt->usermodule;
		for (int j = 0; j < MAXSCOPE; j++) {
			if (s->space[j]) {
				for (Symbol t = s->space[j]; t != NULL; t = t->peer) {
					if (t->kind == FUNCTIONsymbol && t->def->stmt[0]->fcnname[0] == '#')
						continue;
					char buf[1024];
					const char *comment = NULL;
					const char *tt = NULL;
					if (t->kind == FUNCTIONsymbol) {
						comment = t->def->help;
						(void) fcnDefinition(t->def, getInstrPtr(t->def, 0), buf, LIST_MAL_NOCFUNC, buf, sizeof(buf));
						tt = t->def->binding;
					} else {
						assert(t->func);
						comment = t->func->comment;
						(void) cfcnDefinition(t, buf, sizeof(buf));
						tt = t->func->cname;
					}
					if (comment == NULL)
						comment = "";
					if (tt == NULL)
						tt = "";
					if (BUNappend(mod, s->name, false) != GDK_SUCCEED
						|| BUNappend(fcn, t->name, false) != GDK_SUCCEED
						|| BUNappend(com, comment, false) != GDK_SUCCEED
						|| BUNappend(sig, buf, false) != GDK_SUCCEED
						|| BUNappend(adr, tt, false) != GDK_SUCCEED) {
						goto bailout;
					}
				}
			}
		}
	}
	ma_close(&ta_state);

	*mx = mod->batCacheid;
	BBPkeepref(mod);
	*fx = fcn->batCacheid;
	BBPkeepref(fcn);
	*sx = sig->batCacheid;
	BBPkeepref(sig);
	*ax = adr->batCacheid;
	BBPkeepref(adr);
	*cx = com->batCacheid;
	BBPkeepref(com);
	(void) mb;
	return MAL_SUCCEED;

  bailout:
	ma_close(&ta_state);
	BBPreclaim(mod);
	BBPreclaim(fcn);
	BBPreclaim(sig);
	BBPreclaim(adr);
	BBPreclaim(com);
	throw(MAL, "manual.functions", GDK_EXCEPTION);
}

#include "mel.h"
static mel_func manual_init_funcs[] = {
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
