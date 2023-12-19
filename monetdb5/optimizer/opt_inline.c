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

#define MAXEXPANSION 256

int
inlineMALblock(MalBlkPtr mb, int pc, MalBlkPtr mc)
{
	int i, k, l, n;
	InstrPtr *ns, p, q;
	int *nv;

	p = getInstrPtr(mb, pc);
	q = getInstrPtr(mc, 0);
	ns = GDKzalloc((l = (mb->ssize + mc->ssize + p->retc - 3)) * sizeof(InstrPtr));
	if (ns == NULL)
		return -1;
	nv = (int *) GDKmalloc(mc->vtop * sizeof(int));
	if (nv == 0) {
		GDKfree(ns);
		return -1;
	}

	/* add all variables of the new block to the target environment */
	for (n = 0; n < mc->vtop; n++) {
		if (isExceptionVariable(getVarName(mc, n))) {
			nv[n] = newVariable(mb, getVarName(mc, n),
								strlen(getVarName(mc, n)), TYPE_str);
		} else if (isVarTypedef(mc, n)) {
			nv[n] = newTypeVariable(mb, getVarType(mc, n));
		} else if (isVarConstant(mc, n)) {
			nv[n] = cpyConstant(mb, getVar(mc, n));
		} else {
			nv[n] = newTmpVariable(mb, getVarType(mc, n));
		}
		if (nv[n] < 0) {
			GDKfree(nv);
			GDKfree(ns);
			return -1;
		}
	}

	/* use an alias mapping to keep track of the actual arguments */
	for (n = p->retc; n < p->argc; n++)
		nv[getArg(q, n)] = getArg(p, n);

	k = 0;
	/* find the return statement of the inline function */
	for (i = 1; i < mc->stop - 1; i++) {
		q = mc->stmt[i];
		if (q->barrier == RETURNsymbol) {
			/* add the mapping of the return variables */
			for (n = 0; n < p->retc; n++)
				nv[getArg(q, n)] = getArg(p, n);
		}
	}

	/* copy the stable part */
	for (i = 0; i < pc; i++)
		ns[k++] = mb->stmt[i];

	for (i = 1; i < mc->stop - 1; i++) {
		q = mc->stmt[i];
		if (q->token == ENDsymbol)
			break;

		/* copy the instruction and fix variable references */
		ns[k] = copyInstruction(q);
		if (ns[k] == NULL) {
			GDKfree(nv);
			GDKfree(ns);
			return -1;
		}

		for (n = 0; n < q->argc; n++)
			getArg(ns[k], n) = nv[getArg(q, n)];

		if (q->barrier == RETURNsymbol) {
			for (n = 0; n < q->retc; n++)
				clrVarFixed(mb, getArg(ns[k], n));	/* for typing */
			setModuleId(ns[k], getModuleId(q));
			setFunctionId(ns[k], getFunctionId(q));
			ns[k]->typechk = TYPE_UNKNOWN;
			ns[k]->barrier = 0;
			ns[k]->token = ASSIGNsymbol;
		}
		k++;
	}

	/* copy the remainder of the stable part */
	freeInstruction(p);
	for (i = pc + 1; i < mb->stop; i++) {
		ns[k++] = mb->stmt[i];
	}
	/* remove any free instruction */
	for (; i < mb->ssize; i++)
		if (mb->stmt[i]) {
			freeInstruction(mb->stmt[i]);
			mb->stmt[i] = 0;
		}
	GDKfree(mb->stmt);
	mb->stmt = ns;

	mb->ssize = l;
	mb->stop = k;
	GDKfree(nv);
	return pc;
}

static bool
isCorrectInline(MalBlkPtr mb)
{
	/* make sure we have a simple inline function with a singe return */
	InstrPtr p;
	int i, retseen = 0;

	for (i = 1; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (p->token == RETURNsymbol || p->barrier == RETURNsymbol)
			retseen++;
	}
	return retseen <= 1;
}


static bool
OPTinlineMultiplex(MalBlkPtr mb, InstrPtr p)
{
	Symbol s;
	str mod, fcn;

	int plus_one = getArgType(mb, p, p->retc) == TYPE_lng ? 1 : 0;
	mod = VALget(&getVar(mb, getArg(p, p->retc + 0 + plus_one))->value);
	fcn = VALget(&getVar(mb, getArg(p, p->retc + 1 + plus_one))->value);
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

	(void) stk;

	for (i = 1; i < mb->stop; i++) {
		q = getInstrPtr(mb, i);
		if (q->blk) {
			sig = getInstrPtr(q->blk, 0);
			/*
			 * Time for inlining functions that are used in multiplex operations.
			 * They are produced by SQL compiler.
			 */
			if (isMultiplex(q)) {
				OPTinlineMultiplex(mb, q);
			} else
				/*
				 * Check if the function definition is tagged as being inlined.
				 */
			if (sig->token == FUNCTIONsymbol && q->blk->inlineProp
					&& isCorrectInline(q->blk)) {
				(void) inlineMALblock(mb, i, q->blk);
				i--;
				actions++;
			}
		}
	}

	//mnstr_printf(cntxt->fdout,"inline limit %d ssize %d vtop %d vsize %d\n", mb->stop, (int)(mb->ssize), mb->vtop, (int)(mb->vsize));
	/* Defense line against incorrect plans */
	if (actions > 0) {
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep actions taken as a fake argument */
	(void) pushInt(mb, pci, actions);
	return msg;
}
