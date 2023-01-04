/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_linker.h"
#include "mal_client.h"

/*
 * @-
 * THe choice operator first searches the next one to identify
 * the fragment to be optimized and to gain access to the variables
 * without the need to declare them upfront.
 */
/* helper routine that at runtime propagates values to the stack */
static void
adder_addval(MalBlkPtr mb, MalStkPtr stk, int i)
{
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

static str
RUNadder(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int total;
	int batch;
	int size;
	int i,pc;
	InstrPtr q =  NULL, *old;
	int oldtop;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	pc = getPC(mb,p);
	total = *getArgReference_int(stk,p,1);
	batch = *getArgReference_int(stk,p,2);
	if (total == 0) return MAL_SUCCEED;

	old = mb->stmt;
	oldtop= mb->stop;
	size = ((mb->stop+batch) < mb->ssize)? mb->ssize:(mb->stop+batch);
	mb->stmt = (InstrPtr *) GDKzalloc(size * sizeof(InstrPtr));
	if (mb->stmt == NULL) {
		mb->stmt = old;
		throw(MAL, "adder.generate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
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
	getVar(mb,getArg(p,1))->value.val.ival = total; /* also set in symbol table */
	if (total > 0) {
		if ((q = copyInstruction(p)) == NULL) {
			for(i=0; i<mb->stop; i++)
				if( mb->stmt[i])
					freeInstruction(mb->stmt[i]);
			GDKfree(mb->stmt);
			mb->stmt = old;
			throw(MAL, "adder.generate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		pushInstruction(mb, q);
	}
	memcpy(mb->stmt+mb->stop, old+pc+1, sizeof(InstrPtr) * (oldtop-pc)-1);
	mb->stop += (oldtop-pc)-1;

	/* check new statments for sanity */
	msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if( msg == MAL_SUCCEED) msg = chkFlow(mb);
	if( msg == MAL_SUCCEED) msg = chkDeclarations(mb);

	GDKfree(old);
	return msg;
}

#include "mel.h"
mel_func run_adder_init_funcs[] = {
 pattern("run_adder", "generate", RUNadder, false, "Generate some statements to add more integers", args(1,3, arg("",int),arg("target",int),arg("batch",int))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_run_adder_mal)
{ mal_module("run_adder", NULL, run_adder_init_funcs); }
