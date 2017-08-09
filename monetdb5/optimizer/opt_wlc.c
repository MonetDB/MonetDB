/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* author M.Kersten
 * This optimizer prepares the code for workload-capture-replay processing.
 */
#include "monetdb_config.h"
#include "opt_wlc.h"


str
OPTwlcImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int i, j, limit, slimit, updates=0, query=1;
	InstrPtr p, q, def = 0;
	InstrPtr *old;
	lng usec = GDKusec();
	char buf[256];

	(void) pci;
	(void) cntxt;
	(void) stk;		/* to fool compilers */

	if( ! WLCused() )
		goto wrapup;

	old= mb->stmt;
	limit= mb->stop;
	slimit = mb->ssize;

	/* check if we are dealing with an update  and move definition to front*/
	for (i = 0; i < limit; i++) {
		p = old[i];
		if( getModuleId(p) == querylogRef && getFunctionId(p) == defineRef){
			def = p;
			for(j = i; j>1; j--)
				old[j]= old[j-1];
			old[j]= def;
		}
		if( getModuleId(p) == sqlcatalogRef)
			query = 0;
		if( getModuleId(p) == sqlRef && 
			( getFunctionId(p) == appendRef  ||
			  getFunctionId(p) == updateRef  ||
			  getFunctionId(p) == deleteRef  ||
			  getFunctionId(p) == clear_tableRef ))
			query = 0;
	}
	def = 0;

	if(query) // nothing to log
		return MAL_SUCCEED;
	
	// We use a fake collection of objects to speed up the checking later.

	// Now optimize the code
	if (newMalBlkStmt(mb,mb->ssize + updates) < 0)
		return createException(MAL, "wlcr.optimizer", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	for (i = 0; i < limit; i++) {
		p = old[i];
		pushInstruction(mb,p);
		if( getModuleId(p) == querylogRef && getFunctionId(p) == defineRef){
			q= copyInstruction(p);
			setModuleId(q, wlcRef);
			setFunctionId(q,queryRef);
			getArg(q,0) = newTmpVariable(mb,TYPE_any);
			pushInstruction(mb,q);
			def = q;
			def->argc -=2;
			updates++;
		} else
		/* the catalog operations all need to be re-executed */
		if( def && getModuleId(p) == sqlcatalogRef &&
			strcmp( getVarConstant(mb,getArg(p,1)).val.sval, "tmp") != 0 ){
			assert( def);// should always be there
			setFunctionId(def,catalogRef);
			updates++;
		} else
		if( def && getModuleId(p) == sqlRef && getFunctionId(p) == clear_tableRef &&
			strcmp( getVarConstant(mb,getArg(p,1)).val.sval, "tmp") != 0 ){
			setFunctionId(def,actionRef);
				assert(def);
				q= copyInstruction(p);
				setModuleId(q, wlcRef);
				for( j=0; j< p->retc; j++)
					getArg(q,j) = newTmpVariable(mb,TYPE_any);
				pushInstruction(mb,q);
				updates++;
		} else
		if( def && getModuleId(p) == sqlRef && 
			( getFunctionId(p) == appendRef  ||
			  getFunctionId(p) == updateRef  ||
			  getFunctionId(p) == deleteRef  ||
			  getFunctionId(p) == clear_tableRef ) &&
			  strcmp( getVarConstant(mb,getArg(p,2)).val.sval, "tmp") != 0 ){
				assert( def);// should always be there, temporary tables are always ignored
				setFunctionId(def,actionRef);
				q= copyInstruction(p);
				delArgument(q, q->retc);
				setModuleId(q, wlcRef);
				for( j=0; j< p->retc; j++)
					getArg(q,j) = newTmpVariable(mb,TYPE_any);
				pushInstruction(mb,q);
				updates++;
		}
	} 
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	GDKfree(old);

    /* Defense line against incorrect plans */
	chkTypes(cntxt->usermodule, mb, FALSE);
	chkFlow(mb);
	//chkDeclarations(mb);
    /* keep all actions taken as a post block comment */
#ifdef _WLCR_DEBUG_
	printFunction(cntxt->fdout,mb, 0, LIST_MAL_ALL);
#endif

wrapup:
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","wlc",updates,GDKusec() - usec);
    newComment(mb,buf);
	return MAL_SUCCEED;
}
