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
 * Constants
 * The const module provides a box abstraction store for global constants.
 * Between sessions, the value of the constants is saved on disk
 * in the form of a simple MAL program, which is scanned and made
 * available by opening the box.  A future implementation should
 * provide transaction support over the box, which would permit
 * multiple clients to exchange (scalar) information easily.
 *
 * The default constant box is initialized with session variables,
 * such as 'user' and 'dbpath'.
 * These actions are encapsulated in the prelude routine called.
 *
 * A box should be opened before being used. It is typically used
 * to set-up the list of current users and to perform authorization.
 * The constant box is protected with a simple authorization scheme,
 * prohibiting all updates unless issued by the system administrator.
 */
#include "monetdb_config.h"
#include "const.h"

#define authorize(X1) \
	{ str tmp = NULL; rethrow("const."X1, tmp, AUTHrequireAdmin(&cntxt)); }

#define insertBox(X1,X2)\
	msg = X2;\
	insertToBox(box,X1,msg);\
	GDKfree(msg);

/*
 * Access to a box calls for resolving the first parameter
 * to a named box.
 */
#define OpenBox(X1)\
	authorize(X1);\
	box= findBox("const");\
	if( box ==0) \
	throw(MAL, "const."X1, BOX_CLOSED);
 
str
CSTprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;
	char u[24];

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	authorize("prelude");
	box = openBox("const");
	if (box == 0)
		throw(MAL, "const.prelude", BOX_CLOSED);
	/* if the box was already filled we can skip initialization */
	if (box->sym && box->sym->vtop == 0) {
		insertToBox(box, "dbpath", GDKgetenv("gdk_dbpath"));
		insertToBox(box, "version", VERSION);
		snprintf(u, 24, "%s", GDKversion());
		insertToBox(box, "gdk_version", u);
	}
	return MAL_SUCCEED;
}

str
CSTepilogue(int *ret)
{
	(void)ret;
	closeBox("const", 0);
	return MAL_SUCCEED;
}

/*
 * Operator implementation
 */
str
CSTopen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	authorize("open");
	if (openBox("const") != 0)
		return MAL_SUCCEED;
	throw(MAL, "const.open", BOX_CLOSED);
}

str
CSTclose(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	authorize("close");
	if (closeBox("const", TRUE) == 0)
		return MAL_SUCCEED;
	throw(MAL, "const.close", BOX_CLOSED);
}

str
CSTdestroy(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	OpenBox("destroy");
	destroyBox("const");
	return MAL_SUCCEED;
}

str
CSTdeposit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	ValPtr v;
	Box box;

	(void) cntxt;
	OpenBox("deposit");
	name = *(str*) getArgReference(stk, pci, 1);
	v = getArgReference(stk,pci,2);
	if (depositBox(box, name, getArgType(mb,pci,1), v))
		throw(MAL, "const.deposit", OPERATION_FAILED);
	(void) mb;
	return MAL_SUCCEED;
}

str
CSTtake(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;
	ValPtr v;

	(void) cntxt;
	OpenBox("take");
	name = *(str*) getArgReference(stk, pci, 1);
	v = getArgReference(stk,pci,0);
	if (takeBox(box, name, v, (int) getArgType(mb, pci, 0)))
		throw(MAL, "const.take", OPERATION_FAILED);
	(void) mb;
	return MAL_SUCCEED;
}

str
CSTrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;

	(void) cntxt;
	(void) mb;		/* fool compiler */

	OpenBox("release");
	name = *(str*) getArgReference(stk, pci, 1);
	if (releaseBox(box, name))
		throw(MAL, "const.release", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
CSTreleaseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	OpenBox("release");
	releaseAllBox(box);
	return MAL_SUCCEED;
}

str
CSTdiscard(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("discard");
	name = *(str*) getArgReference(stk, pci, 1);
	if (discardBox(box, name) == 0)
		throw(MAL, "const.discard", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
CSTtoString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;
	int i, len = 0;
	ValPtr v;
	str nme, s = 0;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("toString");
	nme = *(str*) getArgReference(stk, pci, 1);
	i = findVariable(box->sym, nme);
	if (i < 0)
		throw(MAL, "const.toString", OPERATION_FAILED);

	v = &box->val->stk[i];
	if (v->vtype == TYPE_str)
		s = v->val.sval;
	else
		(*BATatoms[v->vtype].atomToStr) (&s, &len, v);
	if (s == NULL)
		throw(MAL, "const.toString", ILLEGAL_ARGUMENT " Attempt to coerce NULL string" );
	VALset( getArgReference(stk,pci,0), TYPE_str, s);
	return MAL_SUCCEED;
}

str
CSTnewIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;
	oid *cursor;
	ValPtr v;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("iterator");
	cursor = (oid *) getArgReference(stk, pci, 0);
	v = getArgReference(stk,pci,1);
	if ( nextBoxElement(box, cursor, v)  == oid_nil)
		throw(MAL, "const.iterator", OPERATION_FAILED);
	return MAL_SUCCEED;
}

str
CSThasMoreElements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;
	oid *cursor;
	ValPtr v;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("iterator");
	cursor = (oid *) getArgReference(stk, pci, 0);
	v = getArgReference(stk,pci,1);
	if ( nextBoxElement(box, cursor, v) == oid_nil)
		throw(MAL, "const.iterator", OPERATION_FAILED);
	return MAL_SUCCEED;
}

