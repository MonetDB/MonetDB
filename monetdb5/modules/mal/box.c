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
 * author Martin Kersten
 * Box definitions
 * This module shows the behavior of a simple box of objects.
 * Objects are stored into the box using deposit() and taken
 * out with take(). Once you are done, elements can be
 * removed by name or reference using discard().
 *
 * A box should be opened before being used. It is typically used
 * to set-up the list of current users and to perform authorization.
 */
#include "monetdb_config.h"
#include "mal_exception.h"
#include "mal_box.h"
#include "mal_interpreter.h"
#include "box.h"

/*
 * Access to a box calls for resolving the first parameter
 * to a named box.
 */
#define OpenBox(X)\
	name = *(str *) getArgReference(stk,pci,1);\
	box= findBox(name);\
	if( box ==0) \
	throw(MAL, "box."X,BOX_CLOSED);

str
BOXopen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;

	(void) cntxt;
	(void) mb;		/*fool compiler */
	name = *(str *) getArgReference(stk, pci, 1);
	if (openBox(name) != 0)
		return MAL_SUCCEED;
	throw(MAL, "box.open", BOX_CLOSED);
}

str
BOXclose(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;

	(void) cntxt;
	(void) mb;		/*fool compiler */
	name = *(str *) getArgReference(stk, pci, 1);
	if (closeBox(name, FALSE) == 0)
		return MAL_SUCCEED;
	throw(MAL, "box.close", BOX_CLOSED);
}

str
BOXdestroy(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;
	str name;

	(void) cntxt;
	(void) mb;
	(void) stk;		/*fool compiler */
	OpenBox("destroy");
	destroyBox(name);
	return MAL_SUCCEED;
}

 
str
BOXdeposit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	ValPtr v;
	Box box;

	(void) cntxt;
	(void) mb;
	OpenBox("deposit");
	name = *(str *) getArgReference(stk, pci, 2);
	v = getArgReference(stk,pci,3); 
	if (depositBox(box, name, getArgType(mb,pci,2), v))
		throw(MAL, "box.deposit", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
BOXtake(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;
	ValPtr v;

	(void) cntxt;
	OpenBox("take");
	name = *(str *) getArgReference(stk, pci, 2);
	v = getArgReference(stk,pci,0); 
	if (takeBox(box, name, v, (int) getArgType(mb, pci, 0)))
		throw(MAL, "box.take", OPERATION_FAILED);
	(void) mb;
	return MAL_SUCCEED;
}

str
BOXrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;

	(void) cntxt;
	(void) mb;		/*fool compiler */
	OpenBox("release");
	name = *(str *) getArgReference(stk, pci, 2);
	releaseBox(box, name);
	return MAL_SUCCEED;
}

str
BOXreleaseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;

	(void) cntxt;
	(void) mb;		/*fool compiler */
	OpenBox("releaseAll");
	releaseAllBox(box);
	return MAL_SUCCEED;
}

str
BOXdiscard(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;

	(void) cntxt;
	(void) mb;		/*fool compiler */
	OpenBox("discard");
	name = *(str *) getArgReference(stk, pci, 2);
	if (discardBox(box, name))
		throw(MAL, "box.discard", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
BOXtoString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;

	(void) cntxt;
	(void) mb;
	(void) stk;		/*fool compiler */
	OpenBox("toString");
	return MAL_SUCCEED;
}

str
BOXgetBoxNames(int *bid)
{
	return getBoxNames(bid);
}

str
BOXiterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;
	oid *cursor;
	ValPtr v;

	(void) cntxt;
	(void) mb;		/*fool compiler */
	OpenBox("iterator");
	cursor = (oid *) getArgReference(stk, pci, 0);
	v = getArgReference(stk,pci,2); 
	(void) nextBoxElement(box, cursor, v);
	return MAL_SUCCEED;
}

