/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_inline.h"

static int
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


static int OPTinlineMultiplex(Client cntxt, MalBlkPtr mb, InstrPtr p){
	Symbol s;
	str mod,fcn;

	mod = VALget(&getVar(mb, getArg(p, 1))->value);
	fcn = VALget(&getVar(mb, getArg(p, 2))->value);
	if( (s= findSymbol(cntxt->usermodule, mod,fcn)) ==0 )
		return FALSE;
	/*
	 * Before we decide to propagate the inline request
	 * to the multiplex operation, we check some basic properties
	 * of the target function. Moreover, we apply the inline optimizer
	 * to the target function as well.
	 * This code should be protected against overflow due to recursive calls.
	 * In general, this is a hard problem. For now, we just expand.
	 */
	(void) OPTinlineImplementation(cntxt, s->def, NULL, p);
	return s->def->inlineProp;
}


str
OPTinlineImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i;
	InstrPtr q,sig;
	int actions = 0;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	(void) p;
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
				if (OPTinlineMultiplex(cntxt,mb,q)) {
#ifdef DEBUG_OPT_INLINE
					fprintf(stderr,"#multiplex inline function\n");
					fprintInstruction(stderr,mb,0,q,LIST_MAL_ALL);
#endif
				}
			} else
			/*
			 * Check if the function definition is tagged as being inlined.
			 */
			if (sig->token == FUNCTIONsymbol && q->blk->inlineProp &&
				isCorrectInline(q->blk) ) {
				(void) inlineMALblock(mb,i,q->blk);
				i--;
				actions++;
#ifdef DEBUG_OPT_INLINE
				fprintf(stderr,"#inline function at %d\n",i);
				fprintFunction(stderr, mb, 0, LIST_MAL_ALL);
				fprintInstruction(stderr,q->blk,0,sig,LIST_MAL_ALL);
#endif
			}
		}
	}

    /* Defense line against incorrect plans */
    if( actions > 0){
        chkTypes(cntxt->usermodule, mb, FALSE);
        chkFlow(mb);
        chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","inline",actions, usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);

	return msg;
}
