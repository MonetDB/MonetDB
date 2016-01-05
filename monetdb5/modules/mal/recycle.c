/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
	recycleCacheLimit = * getArgReference_int(stk, p, 1);
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
	str sch = *getArgReference_str(stk, p, 2);
	str tbl = *getArgReference_str(stk, p, 3);
	str col = *getArgReference_str(stk, p, 4);
	(void) mb;
	return RECYCLEcolumn(cntxt,sch,tbl,col);
}

str RECYCLEdeleteSQL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str sch = *getArgReference_str(stk, p, 2);
	str tbl = *getArgReference_str(stk, p, 3);
	(void) mb;
	return RECYCLEcolumn(cntxt,sch,tbl,0);
}

str RECYCLEresetBATwrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) mb;
	return RECYCLEresetBAT(cntxt,*getArgReference_bat(stk,p,1));
}
