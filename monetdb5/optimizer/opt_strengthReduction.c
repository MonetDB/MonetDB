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

#include "monetdb_config.h"
#include "opt_strengthReduction.h"
#include "mal_interpreter.h"	/* for showErrors() */

/*
 * Strength reduction implementation
 * Strength reduction of the code is defensive.
 * This first shot assumes a single loop, so we do not have to
 * maintain a complex administration. We simply split the code
 * into two sections. Those that should do Before and  Within the loop.
 *
 * A critical decision is to make sure that none of the arguments
 * are overwritten in a given context.
 */
static int
SRoverwritten(InstrPtr p, int varid)
{
	int i;

	for (i =0; i < p->retc;  i++)
		if (getArg(p, i) == varid)
			return 1;
	return 0;
}
static int
isNewSource(InstrPtr p) {
	str mp= getModuleId(p);
	if( mp == sqlRef && getFunctionId(p) == bindRef) return 1;
	if( mp == calcRef) return 1;
	if( mp == batcalcRef) return 1;
	if( mp == strRef) return 1;
	if( mp == batstrRef) return 1;
	if( mp == putName("array",5)) return 1;
	if( mp == putName("url",3)) return 1;
	if( mp == putName("daytime",7)) return 1;
	if( mp == putName("day",3)) return 1;
	if( mp == putName("date",4)) return 1;
	if( mp == putName("time",4)) return 1;
	if( mp == putName("tzone",5)) return 1;
	if( mp == putName("color",4)) return 1;
	if( mp == putName("batcolor",8)) return 1;
	if( mp == putName("blob",4)) return 1;
	return 0;
}
int
OPTstrengthReductionImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j = 0, k, se= FALSE;
	InstrPtr p;
	int bk, ik, blk, blkbegin, blkexit, actions = 0;
	InstrPtr *before, *within, *newstmt;
	Lifespan span;

	(void) cntxt;
	(void) pci;
	(void) stk;		/* to fool compilers */

	before = (InstrPtr *) GDKmalloc((mb->ssize + 1) * sizeof(InstrPtr));
	within = (InstrPtr *) GDKmalloc((mb->ssize + 1) * sizeof(InstrPtr));
	if (before== NULL || within == NULL){
		if(before) GDKfree(before);
		if(within) GDKfree(within);
		return 0;
	}
	bk = 0;
	ik = 0;
	blk = 0;
	blkexit= blkbegin = 0;
	for (i = 0; i < mb->stop; i++)
		before[i] = within[i] = 0;
	before[bk++] = getInstrPtr(mb, 0);

	span =setLifespan(mb);
	if( span == NULL){
		GDKfree(before);
		GDKfree(within);
		return 0;
	}

	for (i = 1; i < mb->stop - 1; i++) {
		p = getInstrPtr(mb, i);
		if (blockStart(p)) {
			if (blkbegin == 0){
				if( isLoopBarrier(mb,i) ){
					blkbegin = i;
					blkexit = getBlockExit(mb,i);
				}
				OPTDEBUGstrengthReduction
					mnstr_printf(cntxt->fdout, "#check block %d-%d\n", blkbegin, blkexit);
			}
			within[ik++] = p;
			blk++;
			continue;
		}
		if (blockExit(p)) {
			blk--;
			if (blk == 0)
				blkexit= blkbegin = 0;
			/* move the saved instruction into place */
			OPTDEBUGstrengthReduction
				mnstr_printf(cntxt->fdout, "#combine both %d %d\n", bk, ik);
			for (k = 0; k < ik; k++)
				before[bk++] = within[k];
			ik = 0;
			before[bk++] = p;
			continue;
		}
		/*
		 * Strength reduction is only relevant inside a block;
		 */
		if( blkexit == 0) {
			within[ik++] = p;
			continue;
		}
		/*
		 * Flow control statements may not be moved around
		 */
		if ( p->barrier != 0){
			within[ik++] = p;
			continue;
		}
		/*
		 * Limit strength reduction to the type modules and the batcalc, batstr, batcolor
		 * and sql.bind.
		 */
		if(getModuleId(p) && !isNewSource(p) ) {
			within[ik++] = p;
			continue;
		}
		/*
		 * Search the prospective new block and make sure that
		 * none of the arguments is assigned a value.
		 */
		for (j = ik-1; j > 0; j--) {
			InstrPtr q = within[j];
			for (k = 0; k < q->retc; k++)
				if (SRoverwritten(p, getArg(q, k))) {
					se = TRUE;
					OPTDEBUGstrengthReduction
						mnstr_printf(cntxt->fdout, "variable is set in loop %d\n", getArg(p, k));
					goto noreduction;
				}
		}
		/*
		 * Make sure the variables are not declared before the loop and used
		 * after the loop, because then you may not simple move an expression.
		 */
		for (k = 0; k < p->retc; k++)
			if ( getBeginLifespan(span, getArg(p, k))<= blkbegin ||
				 getEndLifespan(span, getArg(p, k))> blkexit) {
				se = TRUE;
				OPTDEBUGstrengthReduction
					mnstr_printf(cntxt->fdout, "variable %d may not be moved %d-%d\n",
					getArg(p, k),getBeginLifespan(span, getArg(p, k)), getEndLifespan(span, getArg(p, k)));
				goto noreduction;
			}

  noreduction:
		OPTDEBUGstrengthReduction{
			mnstr_printf(cntxt->fdout,"move %d to stack %s\n", i, (se ?"within":"before"));
			printInstruction(cntxt->fdout, mb, 0, p, LIST_MAL_ALL);
		}
		if (blkexit && se == FALSE && !hasSideEffects(p, TRUE) && !isUpdateInstruction(p) )
			before[bk++] = p;
		else
			within[ik++] = p;
	}
	actions += ik;
	for (k = 0; k < ik; k++)
		before[bk++] = within[k];
	before[bk++] = getInstrPtr(mb, i);
	newstmt = (InstrPtr *) GDKzalloc((mb->ssize) * sizeof(InstrPtr));
	if ( newstmt == NULL){
		GDKfree(span);
		GDKfree(before);
		GDKfree(within);
		return 0;
	} else {
		GDKfree(mb->stmt);
		mb->stmt = newstmt;
	}
	mb->stop = 0;

	OPTDEBUGstrengthReduction
		mnstr_printf(cntxt->fdout,"stop= %d bk=%d\n",mb->stop,bk);

	for (i = 0; i < bk; i++)
	if( before[i])
		pushInstruction(mb, before[i]);
	GDKfree(span);
	GDKfree(before);
	GDKfree(within);
	return actions;
}
