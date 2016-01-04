/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @f run_adder
 * @a The ROX Team
 * @+ Dynamic Statement Generation Example
 *
 * we have a function: adder.generate(target, batch)
 * that is recognized by this scheduler.
 * @example
 *         x := 0;
 * 	x := adder.generate(10,2)
 * 	io.print(x);
 * @end example
 * should produce on first iteration
 * @example
 * 	x := 0;
 * 	x := calc.+(x,1);
 * 	x := calc.+(x,1);
 * 	x := adder.generate(8,2)
 * 	io.print(x);
 * @end example
 * next when generate() is found:
 * @example
 * 	x := calc.+(x,1);
 * 	x := calc.+(x,1);
 * 	x := adder.generate(6,2)
 * 	io.print(x);
 * @end example
 * etc, until x = base
 *
 */
/*
 * @+ Adder implementation
 * The code below is a mixture of generic routines and
 * sample implementations to run the tests.
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "opt_prelude.h"
#include "run_adder.h"

/*
 * @-
 * THe choice operator first searches the next one to identify
 * the fragment to be optimized and to gain access to the variables
 * without the need to declare them upfront.
 */
/* helper routine that at runtime propagates values to the stack */
static void adder_addval(MalBlkPtr mb, MalStkPtr stk, int i) {
	ValPtr rhs, lhs = &stk->stk[i];
	if (isVarConstant(mb,i) > 0 ){
		rhs = &getVarConstant(mb,i);
		VALcopy(lhs,rhs);
	} else {
		lhs->vtype = getVarGDKType(mb,i);
		lhs->val.pval = 0;
		lhs->len = 0;
	}
}

str
RUNadder(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int total;
	int batch;
	int size;
	int i,pc;
	InstrPtr q, *old;
	int oldtop;

	(void) cntxt;
	pc = getPC(mb,p);
	total = *getArgReference_int(stk,p,1);
	batch = *getArgReference_int(stk,p,2);
	if (total == 0) return MAL_SUCCEED;

	old = mb->stmt;
	oldtop= mb->stop;
	size = ((mb->stop+batch) < mb->ssize)? mb->ssize:(mb->stop+batch);
	mb->stmt = (InstrPtr *) GDKzalloc(size * sizeof(InstrPtr));
	mb->ssize = size;
	memcpy( mb->stmt, old, sizeof(InstrPtr)*(pc+1));
	mb->stop = pc+1;

	if (batch > total) total = batch;
	for (i=0; i<batch; i++) {
		/* tmp := calc.+(x,1) */
		q = newStmt(mb, calcRef, plusRef);
		getArg(q,0) = getArg(p,0);
		q = pushArgument(mb, q, getArg(p, 0));
		q = pushInt(mb, q, 1);
		adder_addval(mb, stk, getArg(q,2));
	}
	total -= batch;
	*getArgReference_int(stk,p,1) = total;
	mb->var[getArg(p,1)]->value.val.ival = total; /* also set in symbol table */
	if (total > 0) {
		q = copyInstruction(p);
		pushInstruction(mb, q);
	}
	memcpy(mb->stmt+mb->stop, old+pc+1, sizeof(InstrPtr) * (oldtop-pc)-1);
	mb->stop += (oldtop-pc)-1;

	/* check new statments for sanity */
	chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
	chkFlow(cntxt->fdout, mb);
	chkDeclarations(cntxt->fdout, mb);

	GDKfree(old);
	return MAL_SUCCEED;
}
