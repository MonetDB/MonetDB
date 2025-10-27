/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/* author: M Kersten
 * Post-optimization of projection lists.
 */
#include "monetdb_config.h"
#include "opt_deadcode.h"
#include "opt_projectionpath.h"

str
OPTprojectionpathImplementation(Client ctx, MalBlkPtr mb, MalStkPtr stk,
								InstrPtr pci)
{
	int i, j, k, actions = 0, maxprefixlength = 0;
	int *pc = NULL;
	InstrPtr p, q, r;
	InstrPtr *old = 0;
	int *varcnt = NULL;			/* use count */
	int limit, slimit;
	str msg = MAL_SUCCEED;
	int vtop = mb->vtop;
	allocator *ta = mb->ta;

	(void) ctx;
	(void) stk;
	if (mb->inlineProp)
		goto wrapupall1;

	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == algebraRef
			&& ((getFunctionId(p) == projectionRef && p->argc == 3)
				|| getFunctionId(p) == projectionpathRef)) {
			break;
		}
	}
	if (i == mb->stop) {
		goto wrapupall1;
	}

	limit = mb->stop;
	slimit = mb->ssize;
	old = mb->stmt;
	if (newMalBlkStmt(mb, 2 * mb->stop) < 0)
		throw(MAL, "optimizer.projectionpath", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* beware, new variables and instructions are introduced */
	allocator_state ta_state = ma_open(ta);
	pc = (int *) ma_zalloc(ta, sizeof(int) * /*mb->*/vtop/* * 2*/);	/* to find last assignment */
	varcnt = (int *) ma_zalloc(ta, sizeof(int) * /*mb->*/vtop/* * 2*/);
	if (pc == NULL || varcnt == NULL) {
		ma_close(ta, &ta_state);
		throw(MAL, "optimizer.projectionpath", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/*
	 * Count the variable reuse  used as arguments first.
	 * A pass operation is not a real reuse
	 */
	for (i = 0; i < limit; i++) {
		p = old[i];
		for (j = p->retc; j < p->argc; j++)
			if (!(getModuleId(p) == languageRef && getFunctionId(p) == passRef))
				varcnt[getArg(p, j)]++;
	}

	/* assume a single pass over the plan, and only consider projection sequences
	 * beware, we are only able to deal with projections without candidate lists. (argc=3)
	 * We also should not change the type of the outcome, i.e. leaving the last argument untouched.
	 */
	for (i = 0; i < limit; i++) {
		p = old[i];
		if (getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef
			&& p->argc == 3) {
			/*
			 * Try to expand its argument list with what we have found so far.
			 */
			int args = p->retc;
			for (j = p->retc; j < p->argc; j++) {
				if (pc[getArg(p, j)]
					&& (r = getInstrPtr(mb, pc[getArg(p, j)])) != NULL
					&& varcnt[getArg(p, j)] <= 1 && getModuleId(r) == algebraRef
					&& (getFunctionId(r) == projectionRef
						|| getFunctionId(r) == projectionpathRef))
					args += r->argc - r->retc;
				else
					args++;
			}
			if (args == p->argc) {
				/* no change */
				goto wrapup;
			}
			if ((q = copyInstructionArgs(mb, p, args)) == NULL) {
				msg = createException(MAL, "optimizer.projectionpath",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto wrapupall;
			}

			q->argc = p->retc;
			for (j = p->retc; j < p->argc; j++) {
				if (pc[getArg(p, j)])
					r = getInstrPtr(mb, pc[getArg(p, j)]);
				else
					r = 0;
				if (r && varcnt[getArg(p, j)] > 1)
					r = 0;

				/* inject the complete sub-path */
				if (r && getModuleId(r) == algebraRef
						&& (getFunctionId(r) == projectionRef
							|| getFunctionId(r) == projectionpathRef)) {
					for (k = r->retc; k < r->argc; k++)
							q = pushArgument(mb, q, getArg(r, k));
				} else
					q = pushArgument(mb, q, getArg(p, j));
			}
			if (q->argc <= p->argc) {
				assert(0);
				/* no change */
				freeInstruction(mb, q);
				goto wrapup;
			}
			/*
			 * Final type check and hardwire the result type, because that  can not be inferred directly from the signature
			 * We already know that all heads are void. Only the last element may have a non-oid type.
			 */
			for (j = 1; j < q->argc - 1; j++)
				if (getBatType(getArgType(mb, q, j)) != TYPE_oid
					&& getBatType(getArgType(mb, q, j)) != TYPE_void) {
					/* don't use the candidate list */
					freeInstruction(mb, q);
					goto wrapup;
				}

			/* fix the type */
			setVarType(mb, getArg(q, 0),
					   newBatType(getBatType(getArgType(mb, q, q->argc - 1))));
			if (getFunctionId(q) == projectionRef)
				setFunctionId(q, projectionpathRef);
			q->typeresolved = false;

			freeInstruction(mb, p);
			p = q;
			/* keep track of the longest projection path */
			if (p->argc > maxprefixlength)
				maxprefixlength = p->argc;
			actions++;
		}
  wrapup:
		pushInstruction(mb, p);
		if (getModuleId(p) == algebraRef
				&& (getFunctionId(p) == projectionRef
					|| getFunctionId(p) == projectionpathRef)) {
			for (j = 0; j < p->retc; j++)
				pc[getArg(p, j)] = mb->stop - 1;
		}
	}

	for (; i < slimit; i++)
		if (old[i])
			pushInstruction(mb, old[i]);

	/* At this location there used to be commented out experimental code
	 * to reuse common prefixes, but this was counter productive.  This
	 * comment is all that is left. */

	/* Defense line against incorrect plans */
	if (actions > 0) {
		msg = chkTypes(ctx->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	assert(vtop == mb->vtop);
  wrapupall:
	ma_close(ta, &ta_state);
  wrapupall1:
	/* keep actions taken as a fake argument */
	(void) pushInt(mb, pci, actions);

	/*
	if (old)
		//GDKfree(old);
		*/

	return msg;
}
