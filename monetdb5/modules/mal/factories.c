/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * The factory infrastructure can be inspected and steered with
 * the commands provided here. to-be-completed-when-needed
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

static str
FCTgetPlants(bat *ret, bat *ret2)
{
	(void) ret;
	(void) ret2;
	throw(MAL, "factories.getPlants", SQLSTATE(0A000) PROGRAM_NYI);
}

static str
FCTgetCaller(int *ret)
{
	(void) ret;
	throw(MAL, "factories.getCaller", SQLSTATE(0A000) PROGRAM_NYI);
}

static str
FCTgetOwners(bat *ret)
{
	(void) ret;
	throw(MAL, "factories.getOwner", SQLSTATE(0A000) PROGRAM_NYI);
}

static str
FCTgetArrival(bat *ret)
{
	(void) ret;
	throw(MAL, "factories.getArrival", SQLSTATE(0A000) PROGRAM_NYI);
}

static str
FCTgetDeparture(bat *ret)
{
	(void) ret;
	throw(MAL, "factories.getDeparture", SQLSTATE(0A000) PROGRAM_NYI);
}

static str
FCTshutdown(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str mod = *getArgReference_str(stk, pci, 1);
	str fcn = *getArgReference_str(stk, pci, 2);
	Symbol s;
	(void) mb;

	s = findSymbol(cntxt->usermodule, putName(mod), putName(fcn));
	if (s == NULL)
		throw(MAL, "factories.shutdown", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	shutdownFactory(cntxt,s->def);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func factories_init_funcs[] = {
 command("factories", "getPlants", FCTgetPlants, false, "Retrieve the names for all active factories.", args(2,2, batarg("mod",str),batarg("fcn",str))),
 command("factories", "getCaller", FCTgetCaller, false, "Retrieve the unique identity of the factory caller.", args(1,1, arg("",int))),
 command("factories", "getOwners", FCTgetOwners, false, "Retrieve the factory owners table.", args(1,1, batarg("",str))),
 command("factories", "getArrival", FCTgetArrival, false, "Retrieve the time stamp the last call was made.", args(1,1, batarg("",timestamp))),
 command("factories", "getDeparture", FCTgetDeparture, false, "Retrieve the time stamp the last answer was returned.", args(1,1, batarg("",timestamp))),
 pattern("factories", "shutdown", FCTshutdown, false, "Close a factory.", args(1,3, arg("",void),arg("m",str),arg("f",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_factories_mal)
{ mal_module("factories", NULL, factories_init_funcs); }
