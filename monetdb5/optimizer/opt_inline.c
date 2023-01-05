/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_inline.h"

static bool
isCorrectInline(MalBlkPtr mb){
	/* make sure we have a simple inline function with a singe return */
	InstrPtr p;
	int i, retseen=0;

	for( i= 1; i < mb->stop; i++){
		p= getInstrPtr(mb,i);
		if ( p->token == RETURNsymbol || p->token == YIELDsymbol ||
			 p->barrier == RETURNsymbol || p->barrier == YIELDsymbol)
			retseen++;
	}
	return retseen <= 1;
}


static bool
OPTinlineMultiplex(MalBlkPtr mb, InstrPtr p)
{
	Symbol s;
	str mod,fcn;

	int plus_one = getArgType(mb, p, p->retc) == TYPE_lng ? 1 : 0;
	mod = VALget(&getVar(mb, getArg(p, p->retc+0+plus_one))->value);
	fcn = VALget(&getVar(mb, getArg(p, p->retc+1+plus_one))->value);
	if ((s = findSymbolInModule(getModule(putName(mod)), putName(fcn))) == 0)
		return false;
	return s->def->inlineProp;
}


str
OPTinlineImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	InstrPtr q, sig;
	int actions = 0;
	str msg = MAL_SUCCEED;

	(void)stk;

	for (i = 1; i < mb->stop; i++) {
		q = getInstrPtr(mb, i);
		if( q->blk ){
			sig = getInstrPtr(q->blk,0);
			/*
			 * Time for inlining functions that are used in multiplex operations.
			 * They are produced by SQL compiler.
			 */
			if (isMultiplex(q)) {
				 OPTinlineMultiplex(mb,q);
			} else
			/*
			 * Check if the function definition is tagged as being inlined.
			 */
			if (sig->token == FUNCTIONsymbol && q->blk->inlineProp &&
				isCorrectInline(q->blk) ) {
				(void) inlineMALblock(mb,i,q->blk);
				i--;
				actions++;
			}
		}
	}

	//mnstr_printf(cntxt->fdout,"inline limit %d ssize %d vtop %d vsize %d\n", mb->stop, (int)(mb->ssize), mb->vtop, (int)(mb->vsize));
	/* Defense line against incorrect plans */
	if( actions > 0){
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);
	return msg;
}
