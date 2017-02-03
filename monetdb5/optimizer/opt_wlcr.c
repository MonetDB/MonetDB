/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* author M.Kersten
 * This optimizer prepares a MAL block for delayed locking
 * The objects are mapped to a fixed hash table to speedup testing later.
 * We don't need the actual name of the objects
 */
#include "monetdb_config.h"
#include "opt_wlcr.h"


int
OPTwlcrImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int i, j, limit, slimit, updates=0;
	InstrPtr p,q;
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
	
	// We use a fake collection of objects to speed up the checking later.

	// Now optimize the code
	if ( newMalBlkStmt(mb,mb->ssize + updates) < 0)
		return 0;
	for (i = 0; i < limit; i++) {
		p = old[i];
		pushInstruction(mb,p);
		if( getModuleId(p) == querylogRef && getFunctionId(p) == defineRef){
			q= copyInstruction(p);
			setModuleId(q, wlcrRef);
			setFunctionId(q,queryRef);
			getArg(q,0) = newTmpVariable(mb,TYPE_any);
			q->argc--; // no need for the userid
			pushInstruction(mb,q);
		} else
/* CATALOG functions need not yet be reported explicitly.
 * They are already captured in the query define.
		if( getModuleId(p) == sqlcatalogRef &&
		 (
			getFunctionId(p) == create_seqRef ||
			getFunctionId(p) == alter_seqRef ||
			getFunctionId(p) == create_tableRef ||
			getFunctionId(p) == alter_tableRef ||
			getFunctionId(p) == create_viewRef ||
			getFunctionId(p) == create_functionRef
		 )){
			q= copyInstruction(p);
			setModuleId(q, wlcrRef);
			getArg(q,0) = newTmpVariable(mb,TYPE_any);
			delArgument(q, 3);
			pushInstruction(mb,q);
		} else
		if( getModuleId(p) == sqlcatalogRef){
			q= copyInstruction(p);
			setModuleId(q, wlcrRef);
			getArg(q,0) = newTmpVariable(mb,TYPE_any);
			pushInstruction(mb,q);
		} else
*/
		if( getModuleId(p) == sqlRef && 
			(getFunctionId(p) == clear_tableRef || 
			 getFunctionId(p) == sqlcatalogRef) ){
			q= copyInstruction(p);
			setModuleId(q, wlcrRef);
			getArg(q,0) = newTmpVariable(mb,TYPE_any);
			pushInstruction(mb,q);
		} else
		if( getModuleId(p) == sqlRef && 
			( getFunctionId(p) == appendRef  ||
			  getFunctionId(p) == updateRef  ||
			  getFunctionId(p) == deleteRef  )){
				q= copyInstruction(p);
				delArgument(q, q->retc);
				setModuleId(q, wlcrRef);
				for( j=0; j< p->retc; j++)
					getArg(q,j) = newTmpVariable(mb,TYPE_any);
				pushInstruction(mb,q);
		}
	} 
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	GDKfree(old);

    /* Defense line against incorrect plans */
	chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
	chkFlow(cntxt->fdout, mb);
	//chkDeclarations(cntxt->fdout, mb);
    /* keep all actions taken as a post block comment */
#ifdef _WLCR_DEBUG_
	printFunction(cntxt->fdout,mb, 0, LIST_MAL_ALL);
#endif

wrapup:
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","wlc",updates,GDKusec() - usec);
    newComment(mb,buf);
	return 1;
}
