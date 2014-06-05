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
 * (c) Martin Kersten
 * The factory infrastructure can be inspected and steered with
 * the commands provided here. to-be-completed-when-needed
 */
#include "monetdb_config.h"
#include "factories.h"

str
FCTgetPlants(int *ret, int *ret2)
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
FCTgetOwners(int *ret)
{
	(void) ret;
	throw(MAL, "factories.getOwner", PROGRAM_NYI);
}

str
FCTgetArrival(int *ret)
{
	(void) ret;
	throw(MAL, "factories.getArrival", PROGRAM_NYI);
}

str
FCTgetDeparture(int *ret)
{
	(void) ret;
	throw(MAL, "factories.getDeparture", PROGRAM_NYI);
}

str
FCTsetLocation(int *ret, str *loc)
{
	(void) ret;
	(void) loc;
	throw(MAL, "factories.setLocation", PROGRAM_NYI);
}

str
FCTgetLocations(int *ret)
{
	(void) ret;
	throw(MAL, "factories.getLocations", PROGRAM_NYI);
}

str
FCTshutdown(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str mod = *(str*) getArgReference(stk, pci, 1);
	str fcn = *(str*) getArgReference(stk, pci, 2);
	Symbol s;
	(void) mb;

	s = findSymbol(cntxt->nspace, putName(mod,strlen(mod)), putName(fcn, strlen(fcn)));
	if (s == NULL)
		throw(MAL, "factories.shutdown", RUNTIME_OBJECT_MISSING);
	shutdownFactory(cntxt,s->def);
	return MAL_SUCCEED;
}
