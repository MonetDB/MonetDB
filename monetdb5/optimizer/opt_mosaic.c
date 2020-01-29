/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* (c) M. Kersten
 * Pass relevant algebra operations through the mosaic library.
 * It should only look at direct use of persistent readonly columns.
 * This can be recognized by observing the various components being accessed.
 */
#include "monetdb_config.h"
#include "opt_mosaic.h"
#include "mosaic.h"
#include "mtime.h"
#include "opt_prelude.h"

/* #define _DEBUG_MOSAIC_*/

static int OPTmosaicType(MalBlkPtr mb, InstrPtr pci, int idx)
{	int type;
	switch(type = getBatType( getArgType(mb,pci,idx))){
	case TYPE_bte:
	case TYPE_bit:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_oid:
	case TYPE_flt:
	case TYPE_dbl:
		return 1;
	default:
		if( type == TYPE_date)
			return 1;
		if( type == TYPE_daytime)
			return 1;
		if( type == TYPE_timestamp)
			return 1;
	}
	return 0;
}

static str
OPTmosaicImplementationInternal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bool coui)
{
	InstrPtr p,q, *old;
    int limit, slimit,i,j, k, actions =0;
	signed char *check;
	char buf[256];
    lng usec = GDKusec();

	if( optimizerIsApplied(mb,"mosaic") || optimizerIsApplied(mb,"mosaiccoui"))
		return MAL_SUCCEED;
	check = GDKzalloc(mb->vsize);
	if ( check == NULL)
		throw(MAL,"optimizer.mosaic",MAL_MALLOC_FAIL);

#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#mosaic implementation\n");
    printFunction(cntxt->fdout,mb,0,LIST_MAL_ALL);
#endif
	limit  = mb->stop;
	slimit = mb->ssize;
	old = mb->stmt;
	if ( newMalBlkStmt(mb, mb->ssize) < 0){
		GDKfree(check);
		throw(MAL,"optimizer.mosaic",MAL_MALLOC_FAIL);
	}

	(void) cntxt;
	(void) stk;
	(void) pci;

	// pre-scan to identify all potentially compressed columns
    for( i=1; i < limit; i++){
        p = old[i];
        if ( getModuleId(p) == sqlRef && getFunctionId(p) == bindRef && getVarConstant(mb,getArg(p,5)).val.ival == 0 && OPTmosaicType(mb,p,0)){
				check[getArg(p,0)] = 1;
		} else
        if ( getModuleId(p) == sqlRef && getFunctionId(p) == bindRef && getVarConstant(mb,getArg(p,5)).val.ival != 0){
			// locate the corresponding base column and turn it off
			for( k= i-1; k>0; k--){
				q = old[k];
				if ( getArg(q,2) == getArg(p,2) && getArg(q,3) == getArg(p,3) && getArg(q,4) == getArg(p,4))
					check[getArg(q,0)] = 0;
			}
		} else
		if ( getModuleId(p) == sqlRef &&  getFunctionId(p) == tidRef){
				check[getArg(p,0)] = 1;
		}
		else
		if ( p->token == ASSIGNsymbol)
			for( j=0; j < p->retc; j++)
				check[getArg(p,j)]= check[getArg(p,p->retc+j)];
		else
		// mark all that needs decompression
		for(j= p->retc; j<p->argc; j++)
			if( check[getArg(p,j)] )
				check[getArg(p,j)] = -1;
    }
	// actual conversion
    for( i=0; i < limit; i++){
        p = old[i];
        if ( getModuleId(p) == algebraRef && (getFunctionId(p) == selectRef || getFunctionId(p) == thetaselectRef) && OPTmosaicType(mb,p,1)){
			setModuleId(p, mosaicRef);
			actions++;
		} else
        if ( getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef && OPTmosaicType(mb,p,2)){
			setModuleId(p, mosaicRef);
			actions++;
		} else
        if ( getModuleId(p) == algebraRef && getFunctionId(p) == joinRef && p->argc ==8 && OPTmosaicType(mb,p,2)){
			setModuleId(p, mosaicRef);
			if (coui) {
				/*This will push and set the COUI flag true.*/
				p = pushBit(mb, p, 1);
			}
			actions++;
		}
		if( p )
			pushInstruction(mb,p);
    }
	for (; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	GDKfree(check);
	chkTypes(cntxt->usermodule, mb, FALSE);
	chkFlow(mb);
	chkDeclarations(mb);
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","mosaic",actions, GDKusec() - usec);
    newComment(mb,buf);
#ifdef _DEBUG_MOSAIC_
    printFunction(cntxt->fdout,mb,0,LIST_MAL_ALL);
#endif
	return MAL_SUCCEED;
}

str
OPTmosaicImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	bool coui = strcmp("mosaiccoui", getFunctionId(pci)) == 0;
	return OPTmosaicImplementationInternal(cntxt, mb, stk, pci, coui);
}
