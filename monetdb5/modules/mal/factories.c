/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * The factory infrastructure can be inspected and steered with
 * the commands provided here. to-be-completed-when-needed
 */
#include "monetdb_config.h"
#include "factories.h"

str
FCTgetPlants(bat *ret, bat *ret2)
{
	(void) ret;
	(void) ret2;
	throw(MAL, "factories.getPlants", PROGRAM_NYI);
}

str
FCTgetCaller(int *ret)
{
	(void) ret;
	throw(MAL, "factories.getCaller", PROGRAM_NYI);
}

str
FCTgetOwners(bat *ret)
{
	(void) ret;
	throw(MAL, "factories.getOwner", PROGRAM_NYI);
}

str
FCTgetArrival(bat *ret)
{
	(void) ret;
	throw(MAL, "factories.getArrival", PROGRAM_NYI);
}

str
FCTgetDeparture(bat *ret)
{
	(void) ret;
	throw(MAL, "factories.getDeparture", PROGRAM_NYI);
}

str
FCTshutdown(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str mod = *getArgReference_str(stk, pci, 1);
	str fcn = *getArgReference_str(stk, pci, 2);
	Symbol s;
	(void) mb;

	s = findSymbol(cntxt->usermodule, putName(mod), putName(fcn));
	if (s == NULL)
		throw(MAL, "factories.shutdown", RUNTIME_OBJECT_MISSING);
	shutdownFactory(cntxt,s->def);
	return MAL_SUCCEED;
}
