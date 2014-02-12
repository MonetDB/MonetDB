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
 * @a M. Ivanova, M. Kersten
 * @f recycle
 * The Recycler
 * Just the interface to the recycler.
 *
 * The Recycler is a variation of the interpreter
 * which inspects the variable table for alternative results.
 */
#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_recycle.h"
#include "recycle.h"
#include "algebra.h"

/*
 * The recycler is started when the first function is called for its support using recycler.prologue().
 * Upon exit of the last function, the content of the recycle cache is destroyed using recycler.epilogue().
 */
str
RECYCLEdumpWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	RECYCLEdump(cntxt->fdout);
	return MAL_SUCCEED;
}


/*
 * Called to collect statistics at the end of each query.
 */

str
RECYCLEsetCache(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) mb;
	recycleCacheLimit = * (int*) getArgReference(stk, p, 1);
	return MAL_SUCCEED;
}

/*
 * At the end of the session we have to cleanup the recycle cache.
 */
str
RECYCLEdropWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){

	(void) mb;
	(void) stk;
	(void) p;
	RECYCLEdrop(cntxt);
	return MAL_SUCCEED;
}

str RECYCLEappendSQL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str sch = *(str*) getArgReference(stk, p, 2);
	str tbl = *(str*) getArgReference(stk, p, 3);
	str col = *(str*) getArgReference(stk, p, 4);
	(void) mb;
	return RECYCLEcolumn(cntxt,sch,tbl,col);
}

str RECYCLEdeleteSQL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str sch = *(str*) getArgReference(stk, p, 2);
	str tbl = *(str*) getArgReference(stk, p, 3);
	(void) mb;
	return RECYCLEcolumn(cntxt,sch,tbl,0);
}

str RECYCLEresetBATwrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) mb;
	return RECYCLEresetBAT(cntxt,*(int*) getArgReference(stk,p,1));
}
