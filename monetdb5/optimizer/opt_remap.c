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

/*
 * The first attempt of the multiplex optimizer is to locate
 * a properly typed multi-plexed implementation.
 * The policy is to search for bat<mod>.<fcn> before going
 * into the iterator code generation.
 */
#include "monetdb_config.h"
#include "opt_remap.h"
#include "opt_inline.h"
#include "opt_multiplex.h"

static InstrPtr
pushNilAt(MalBlkPtr mb, InstrPtr p, int pos)
{
    int i;

    p = pushNilBat(mb, p);   /* push at end */
    if (mb->errors == NULL) {
		int arg = getArg(p, p->argc - 1);
        for (i = p->argc - 1; i > pos; i--)
            getArg(p, i) = getArg(p, i - 1);
        getArg(p, pos) = arg;
    }
    return p;
}

static int
OPTremapDirect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int idx,
			   Module scope)
{
	str mod, fcn;
	char buf[1024];
	int i, retc = pci->retc;
	InstrPtr p;
	const char *bufName, *fcnName;

	(void) cntxt;
	(void) stk;
	int plus_one = getArgType(mb, pci, pci->retc) == TYPE_lng ? 1 : 0;
	mod = VALget(&getVar(mb, getArg(pci, retc + 0 + plus_one))->value);
	fcn = VALget(&getVar(mb, getArg(pci, retc + 1 + plus_one))->value);

	if (strncmp(mod, "bat", 3) == 0)
		mod += 3;


	snprintf(buf, 1024, "bat%s", mod);
	bufName = putName(buf);
	fcnName = putName(fcn);
	if (bufName == NULL || fcnName == NULL)
		return 0;

	p = newInstructionArgs(mb, bufName, fcnName, pci->argc + 2);
	if (p == NULL)
		return 0;

	for (i = 0; i < pci->retc; i++)
		if (i < 1)
			getArg(p, i) = getArg(pci, i);
		else
			p = pushReturn(mb, p, getArg(pci, i));
	p->retc = p->argc = pci->retc;


	if (plus_one) {
		p = pushArgument(mb, p, getArg(pci, pci->retc));	// cardinality argument
	}

	for (i = pci->retc + 2 + plus_one; i < pci->argc; i++)
		p = pushArgument(mb, p, getArg(pci, i));
	if (p->retc == 1 &&
		((bufName == batcalcRef
		  && (fcnName == mulRef
			  || fcnName == divRef
			  || fcnName == plusRef
			  || fcnName == minusRef
			  || fcnName == modRef))
		 || bufName == batmtimeRef
		 || bufName == batstrRef)) {
		if (p->argc == 3 &&
			/* these two filter out unary batcalc.- with a candidate list */
			getBatType(getArgType(mb, p, 1)) != TYPE_oid
			&& (getBatType(getArgType(mb, p, 2)) != TYPE_oid
				&& !(isVarConstant(mb, getArg(p, 2))
					 && isaBatType(getArgType(mb, p, 2) )))) {
			/* add candidate lists */
			if (isaBatType(getArgType(mb, p, 1)))
				p = pushNilBat(mb, p);
			if (isaBatType(getArgType(mb, p, 2)))
				p = pushNilBat(mb, p);
		}
	}

	/* now see if we can resolve the instruction */
	typeChecker(scope, mb, p, idx, TRUE);
	if (!p->typeresolved) {
		freeInstruction(p);
		return 0;
	}
	pushInstruction(mb, p);
	return 1;
}

/*
 * Multiplex inline functions should be done with care.
 * The approach taken is to make a temporary copy of the function to be inlined.
 * To change all the statements to reflect the new situation
 * and, if no error occurs, replaces the target instruction
 * with this new block.
 *
 * By the time we get here, we know that function is
 * side-effect free.
 *
 * The multiplex upgrade is targeted at all function
 * arguments whose actual is a BAT and its formal
 * is a scalar.
 * This seems sufficient for the SQL generated PSM code,
 * but does in general not hold.
 * For example,
 *
 * function foo(b:int,c:bat[:oid,:int])
 * 	... d:= batcalc.+(b,c)
 * and
 * multiplex("user","foo",ba:bat[:oid,:int],ca:bat[:oid,:int])
 * upgrades the first argument. The naive upgrade of
 * the statement that would fail. The code below catches
 * most of them by simple prepending "bat" to the MAL function
 * name and leave it to the type resolver to generate the
 * error.
 *
 * The process terminates as soon as we
 * find an instruction that does not have a multiplex
 * counterpart.
 */
static int
OPTmultiplexInline(Client cntxt, MalBlkPtr mb, InstrPtr p, int pc)
{
	MalBlkPtr mq;
	InstrPtr q = NULL, sig;
	char buf[1024];
	int i, j, k, m;
	int refbat = 0, retc = p->retc;
	bit *upgrade;
	str msg;


	str mod = VALget(&getVar(mb, getArg(p, retc + 0))->value);
	str fcn = VALget(&getVar(mb, getArg(p, retc + 1))->value);
	//Symbol s = findSymbol(cntxt->usermodule, mod,fcn);
	Symbol s = findSymbolInModule(getModule(putName(mod)), putName(fcn));

	if (s == NULL || !isSideEffectFree(s->def)
		|| getInstrPtr(s->def, 0)->retc != p->retc) {
		return 0;
	}
	/*
	 * Determine the variables to be upgraded and adjust their type
	 */
	if ((mq = copyMalBlk(s->def)) == NULL) {
		return 0;
	}
	sig = getInstrPtr(mq, 0);

	upgrade = (bit *) GDKzalloc(sizeof(bit) * mq->vtop);
	if (upgrade == NULL) {
		freeMalBlk(mq);
		return 0;
	}

	setVarType(mq, 0, newBatType(getArgType(mb, p, 0)));
	clrVarFixed(mq, getArg(getInstrPtr(mq, 0), 0));	/* for typing */
	upgrade[getArg(getInstrPtr(mq, 0), 0)] = TRUE;

	for (i = 3; i < p->argc; i++) {
		if (!isaBatType(getArgType(mq, sig, i - 2))
			&& isaBatType(getArgType(mb, p, i))) {

			if (getBatType(getArgType(mb, p, i)) != getArgType(mq, sig, i - 2)) {
				goto terminateMX;
			}

			setVarType(mq, i - 2, newBatType(getArgType(mb, p, i)));
			upgrade[getArg(sig, i - 2)] = TRUE;
			refbat = getArg(sig, i - 2);
		}
	}
	/*
	 * The next step is to check each instruction of the
	 * to-be-inlined function for arguments that require
	 * an upgrade and resolve it afterwards.
	 */
	for (i = 1; i < mq->stop; i++) {
		int fnd = 0;

		q = getInstrPtr(mq, i);
		if (q->token == ENDsymbol)
			break;
		for (j = 0; j < q->argc && !fnd; j++)
			if (upgrade[getArg(q, j)]) {
				for (k = 0; k < q->retc; k++) {
					setVarType(mq, getArg(q, j),
							   newBatType(getArgType(mq, q, j)));
					/* for typing */
					clrVarFixed(mq, getArg(q, k));
					if (!upgrade[getArg(q, k)]) {
						upgrade[getArg(q, k)] = TRUE;
						/* lets restart */
						i = 0;
					}
				}
				fnd = 1;
			}
		/* nil:type -> nil:bat[:oid,:type] */
		if (!getModuleId(q) && q->token == ASSIGNsymbol && q->argc == 2
			&& isVarConstant(mq, getArg(q, 1)) && upgrade[getArg(q, 0)]
			&& getArgType(mq, q, 0) == TYPE_void
			&& !isaBatType(getArgType(mq, q, 1))) {
			/* handle nil assignment */
			if (ATOMcmp(getArgGDKType(mq, q, 1),
						VALptr(&getVar(mq, getArg(q, 1))->value),
						ATOMnilptr(getArgType(mq, q, 1))) == 0) {
				ValRecord cst;
				int tpe = getArgType(mq, q, 1);

				cst.vtype = tpe;
				cst.bat = true;
				cst.val.bval = bat_nil;
				cst.len = 0;
				tpe = newBatType(tpe);
				setVarType(mq, getArg(q, 0), tpe);
				m = defConstant(mq, tpe, &cst);
				if (m >= 0) {
					getArg(q, 1) = m;
					setVarType(mq, getArg(q, 1), tpe);
				}
			} else {
				/* handle constant tail setting */
				int tpe = newBatType(getArgType(mq, q, 1));

				setVarType(mq, getArg(q, 0), tpe);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushArgument(mb, q, getArg(q, 1));
				mq->stmt[i] = q;
				getArg(q, 1) = refbat;
			}
		}
	}

	/* now upgrade the statements */
	for (i = 1; i < mq->stop; i++) {
		q = getInstrPtr(mq, i);
		if (q->token == ENDsymbol)
			break;
		for (j = 0; j < q->argc; j++)
			if (upgrade[getArg(q, j)]) {
				if (blockStart(q) || q->barrier == REDOsymbol
					|| q->barrier == LEAVEsymbol)
					goto terminateMX;
				if (getModuleId(q)) {
					snprintf(buf, 1024, "bat%s", getModuleId(q));
					setModuleId(q, putName(buf));
					q->typeresolved = false;
					if (q->retc == 1 &&
						((getModuleId(q) == batcalcRef
						  && (   getFunctionId(q) == mulRef
							   || getFunctionId(q) == divRef
							   || getFunctionId(q) == plusRef
							   || getFunctionId(q) == minusRef
							   || getFunctionId(q) == modRef
						       || (q->argc > 3 && (
							         getFunctionId(q) == intRef
							      || getFunctionId(q) == lngRef
							      || getFunctionId(q) == hgeRef))
							 ))
						 || getModuleId(q) == batmtimeRef
						 || getModuleId(q) == batstrRef)) {
						if (q->argc == 3 &&
							/* these two filter out unary batcalc.- with a candidate list */
							getBatType(getArgType(mq, q, 1)) != TYPE_oid
							&& getBatType(getArgType(mq, q, 2)) != TYPE_oid) {
							/* add candidate lists */
							if (isaBatType(getArgType(mq, q, 1)))
								q = pushNilBat(mq, q);
							if (isaBatType(getArgType(mq, q, 2)))
								q = pushNilBat(mq, q);
						} else if (q->argc == 4
								   && getBatType(getArgType(mq, q, 3)) == TYPE_bit
								   /* these two filter out unary
								    * batcalc.- with a candidate
								    * list */
								   && getBatType(getArgType(mq, q, 1)) != TYPE_oid
								   && getBatType(getArgType(mq, q, 2)) != TYPE_oid) {
							int a = getArg(q, 3);
							q->argc--;
							/* add candidate lists */
							if (isaBatType(getArgType(mq, q, 1)))
								q = pushNilBat(mq, q);
							if (isaBatType(getArgType(mq, q, 2)))
								q = pushNilBat(mq, q);
							q = pushArgument(mq, q, a);
						} else if (q->argc == 5 && getModuleId(q) == batcalcRef) { /* decimal casts */
							int pos = 3;
							if (isaBatType(getArgType(mq, q, 1)))
								q = pushNilAt(mq, q, pos++);
							if (isaBatType(getArgType(mq, q, 2)))
								q = pushNilAt(mq, q, pos);
						}
					}

					/* now see if we can resolve the instruction */
					typeChecker(cntxt->usermodule, mq, q, i, TRUE);
					if (!q->typeresolved)
						goto terminateMX;
					break;
				}
				/* handle simple upgraded assignments as well */
				if (q->token == ASSIGNsymbol && q->argc == 2
					&& !(isaBatType(getArgType(mq, q, 1)))) {
					setModuleId(q, algebraRef);
					setFunctionId(q, projectRef);
					q = pushArgument(mq, q, getArg(q, 1));
					mq->stmt[i] = q;
					getArg(q, 1) = refbat;

					q->typeresolved = false;
					typeChecker(cntxt->usermodule, mq, q, i, TRUE);
					if (!q->typeresolved)
						goto terminateMX;
					break;
				}
			}
	}


	if (mq->errors) {
  terminateMX:

		freeMalBlk(mq);
		GDKfree(upgrade);

		/* ugh ugh, fallback to non inline, but optimized code */
		msg = OPTmultiplexSimple(cntxt, s->def);
		if (msg)
			freeException(msg);
		if (s->kind == FUNCTIONsymbol)
			s->def->inlineProp = 0;
		return 0;
	}
	/*
	 * We have successfully constructed a variant
	 * of the to-be-inlined function. Put it in place
	 * of the original multiplex.
	 * But first, shift the arguments of the multiplex.
	 */
	delArgument(p, 2);
	delArgument(p, 1);
	inlineMALblock(mb, pc, mq);

	freeMalBlk(mq);
	GDKfree(upgrade);
	return 1;
}

/*
 * The comparison multiplex operations with a constant head may be supported
 * by reverse of the operation.
 */
static const struct {
	const char *src, *dst;
	const int len;
} OperatorMap[] = {
	{"<", ">", 1},
	{">", "<", 1},
	{">=", "<=", 2},
	{"<=", ">=", 2},
	{"==", "==", 2},
	{"!=", "!=", 2},
	{0, 0, 0}
};

static int
OPTremapSwitched(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
				 int idx, Module scope)
{
	char *fcn;
	int r, i;
	(void) stk;
	(void) scope;

	if (!isMultiplex(pci) && getArgType(mb, pci, pci->retc) != TYPE_lng
		&& !isVarConstant(mb, getArg(pci, 1))
		&& !isVarConstant(mb, getArg(pci, 2))
		&& !isVarConstant(mb, getArg(pci, 4)) && pci->argc != 5)
		return 0;
	fcn = VALget(&getVar(mb, getArg(pci, 2))->value);
	for (i = 0; OperatorMap[i].src; i++)
		if (strcmp(fcn, OperatorMap[i].src) == 0) {
			/* found a candidate for a switch */
			getVarConstant(mb, getArg(pci, 2)).val.sval = (char *) putNameLen(OperatorMap[i].dst, OperatorMap[i].len);
			getVarConstant(mb, getArg(pci, 2)).len = OperatorMap[i].len;
			r = getArg(pci, 3);
			getArg(pci, 3) = getArg(pci, 4);
			getArg(pci, 4) = r;
			r = OPTremapDirect(cntxt, mb, stk, pci, idx, scope);

			/* always restore the allocated function name */
			getVarConstant(mb, getArg(pci, 2)).val.sval = fcn;
			getVarConstant(mb, getArg(pci, 2)).len = strlen(fcn);

			if (r)
				return 1;

			/* restore the arguments */
			r = getArg(pci, 3);
			getArg(pci, 3) = getArg(pci, 4);
			getArg(pci, 4) = r;
		}
	return 0;
}

str
OPTremapImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr *old, p;
	int i, limit, slimit, actions = 0;
	Module scope = cntxt->usermodule;
	str msg = MAL_SUCCEED;

	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (isMultiplex(p)
			|| (p->argc == 4 && getModuleId(p) == aggrRef
				&& getFunctionId(p) == avgRef)) {
			break;
		}
	}
	if (i == mb->stop) {
		goto wrapup;
	}

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if (newMalBlkStmt(mb, mb->ssize) < 0)
		throw(MAL, "optimizer.remap", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < limit; i++) {
		p = old[i];
		if (isUnion(p)) {
			/*
			 * The next step considered is to handle inlined functions.
			 * It means we have already skipped the most obvious ones,
			 * such as the calculator functions. It is particularly
			 * geared at handling the PSM code.
			 */
			int plus_one = getArgType(mb, p, p->retc) == TYPE_lng ? 1 : 0;
			str mod = VALget(&getVar(mb, getArg(p, p->retc + 0 + plus_one))-> value);
			str fcn = VALget(&getVar(mb, getArg(p, p->retc + 1 + plus_one))-> value);
			//Symbol s = findSymbol(cntxt->usermodule, mod,fcn);
			Symbol s = findSymbolInModule(getModule(putName(mod)), putName(fcn));

			if (s && s->kind == FUNCTIONsymbol && s->def->inlineProp) {
				pushInstruction(mb, p);
				if (OPTmultiplexInline(cntxt, mb, p, mb->stop - 1)) {
					actions++;
				}
			} else if (OPTremapDirect(cntxt, mb, stk, p, i, scope)
					   || OPTremapSwitched(cntxt, mb, stk, p, i, scope)) {
				freeInstruction(p);
				actions++;
			} else {
				pushInstruction(mb, p);
			}
		} else if (p->argc == 4 && getModuleId(p) == aggrRef
				   && getFunctionId(p) == avgRef) {
			/* group aggr.avg -> aggr.sum/aggr.count */
			InstrPtr sum, avg, t, iszero;
			InstrPtr cnt;
			sum = copyInstruction(p);
			if (sum == NULL) {
				msg = createException(MAL, "optimizer.remap",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			cnt = copyInstruction(p);
			if (cnt == NULL) {
				freeInstruction(sum);
				msg = createException(MAL, "optimizer.remap",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			setFunctionId(sum, sumRef);
			setFunctionId(cnt, countRef);
			getArg(sum, 0) = newTmpVariable(mb, getArgType(mb, p, 1));
			getArg(cnt, 0) = newTmpVariable(mb, newBatType(TYPE_lng));
			pushInstruction(mb, sum);
			pushInstruction(mb, cnt);

			t = newInstruction(mb, batcalcRef, eqRef);
			if (t == NULL) {
				msg = createException(MAL, "optimizer.remap",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			getArg(t, 0) = newTmpVariable(mb, newBatType(TYPE_bit));
			t = pushArgument(mb, t, getDestVar(cnt));
			t = pushLng(mb, t, 0);
			pushInstruction(mb, t);
			iszero = t;

			t = newInstruction(mb, batcalcRef, dblRef);
			if (t == NULL) {
				msg = createException(MAL, "optimizer.remap",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			getArg(t, 0) = newTmpVariable(mb, getArgType(mb, p, 0));
			t = pushArgument(mb, t, getDestVar(sum));
			pushInstruction(mb, t);
			sum = t;

			t = newInstruction(mb, batcalcRef, ifthenelseRef);
			if (t == NULL) {
				msg = createException(MAL, "optimizer.remap",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			getArg(t, 0) = newTmpVariable(mb, getArgType(mb, p, 0));
			t = pushArgument(mb, t, getDestVar(iszero));
			t = pushNil(mb, t, TYPE_dbl);
			t = pushArgument(mb, t, getDestVar(sum));
			pushInstruction(mb, t);
			sum = t;

			t = newInstruction(mb, batcalcRef, dblRef);
			if (t == NULL) {
				msg = createException(MAL, "optimizer.remap",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			getArg(t, 0) = newTmpVariable(mb, getArgType(mb, p, 0));
			t = pushArgument(mb, t, getDestVar(cnt));
			pushInstruction(mb, t);
			cnt = t;

			avg = newInstruction(mb, batcalcRef, divRef);
			if (avg == NULL) {
				msg = createException(MAL, "optimizer.remap",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			getArg(avg, 0) = getArg(p, 0);
			avg = pushArgument(mb, avg, getDestVar(sum));
			avg = pushArgument(mb, avg, getDestVar(cnt));
			avg = pushNilBat(mb, avg);
			avg = pushNilBat(mb, avg);
			freeInstruction(p);
			pushInstruction(mb, avg);
		} else {
			pushInstruction(mb, p);
		}
	}
	for (; i < slimit; i++)
		if (old[i])
			pushInstruction(mb, old[i]);
	GDKfree(old);

	/* Defense line against incorrect plans */
	if (msg == MAL_SUCCEED && actions > 0) {
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
  wrapup:
	/* keep actions taken as a fake argument */
	(void) pushInt(mb, pci, actions);
	return msg;
}
