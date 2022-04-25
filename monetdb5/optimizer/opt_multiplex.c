/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_multiplex.h"
#include "manifold.h"
#include "mal_interpreter.h"

/*
 * The generic solution to the multiplex operators is to translate
 * them to a MAL loop.
 * The call optimizer.multiplex(MOD,FCN,A1,...An) introduces the following code
 * structure:
 *
 * 	resB:= bat.new(restype, A1);
 * barrier (h,t1):= iterator.new(A1);
 * 	t2:= algebra.fetch(A2,h)
 * 	...
 * 	cr:= MOD.FCN(t1,...,tn);
 * 	bat.append(resB,cr);
 * 	redo (h,t):= iterator.next(A1);
 * end h;
 *
 * The algorithm consists of two phases: phase one deals with
 * collecting the relevant information, phase two is the actual
 * code construction.
 */
static str
OPTexpandMultiplex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i = 2, iter = 0;
	int hvar, tvar;
	const char *mod, *fcn;
	int *alias, *resB;
	InstrPtr q;
	int tt;
	int bat = (getModuleId(pci) == batmalRef) ;

	(void) cntxt;
	(void) stk;
	for (i = 0; i < pci->retc; i++) {
		tt = getBatType(getArgType(mb, pci, i));
		if (tt== TYPE_any)
			throw(MAL, "optimizer.multiplex", SQLSTATE(HY002) "Target tail type is missing");
		if (isAnyExpression(getArgType(mb, pci, i)))
			throw(MAL, "optimizer.multiplex", SQLSTATE(HY002) "Target type is missing");
	}
	int plus_one = getArgType(mb, pci, pci->retc) == TYPE_lng ? 1 : 0;
	mod = VALget(&getVar(mb, getArg(pci, pci->retc + plus_one))->value);
	mod = putName(mod);
	fcn = VALget(&getVar(mb, getArg(pci, pci->retc+1 + plus_one))->value);
	fcn = putName(fcn);
	if(mod == NULL || fcn == NULL)
		throw(MAL, "optimizer.multiplex", SQLSTATE(HY013) MAL_MALLOC_FAIL);

#ifndef NDEBUG
	TRC_WARNING_IF(MAL_OPTIMIZER) {
		char *ps = instruction2str(mb, stk, pci, LIST_MAL_DEBUG);
		TRC_WARNING_ENDIF(MAL_OPTIMIZER, "To speedup %s.%s a bulk operator implementation is needed%s%s\n", mod, fcn, ps ? " for " : "", ps ? ps : "");
		GDKfree(ps);
	}
#endif

	if (plus_one) {
		q = newFcnCallArgs(mb, batRef, putName("densebat"), 2);
		q = pushArgument(mb, q, getArg(pci, pci->retc));
		iter = getArg(q,0);
	}
	else /* search the iterator bat */
	for (i = pci->retc+2; i < pci->argc; i++)
		if (isaBatType(getArgType(mb, pci, i))) {
			iter = getArg(pci, i);
			break;
		}
	if( i == pci->argc)
		throw(MAL, "optimizer.multiplex", SQLSTATE(HY002) "Iterator BAT type is missing");

	/*
	 * Beware, the operator constant (arg=1) is passed along as well,
	 * because in the end we issue a recursive function call that should
	 * find the actual arguments at the proper place of the callee.
	 */

	alias= (int*) GDKmalloc(sizeof(int) * pci->maxarg);
	resB = (int*) GDKmalloc(sizeof(int) * pci->retc);
	if (alias == NULL || resB == NULL)  {
		GDKfree(alias);
		GDKfree(resB);
		throw(MAL, "optimizer.multiplex", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* resB := new(refBat) */
	for (i = 0; i < pci->retc; i++) {
		q = newFcnCallArgs(mb, batRef, newRef, 3);
		resB[i] = getArg(q, 0);

		tt = getBatType(getArgType(mb, pci, i));

		setVarType(mb, getArg(q, 0), newBatType(tt));
		q = pushType(mb, q, tt);
		q = pushArgument(mb, q, iter);
		assert(q->argc==3);
	}

	/* barrier (h,r) := iterator.new(refBat); */
	q = newFcnCall(mb, iteratorRef, newRef);
	q->barrier = BARRIERsymbol;
	hvar = newTmpVariable(mb, TYPE_any);
	getArg(q,0) = hvar;
	tvar = newTmpVariable(mb, TYPE_any);
	q= pushReturn(mb, q, tvar);
	(void) pushArgument(mb,q,iter);

	/* $1:= algebra.fetch(Ai,h) or constant */
	for (i = pci->retc+2+plus_one; i < pci->argc; i++) {
		if (getArg(pci, i) != iter && isaBatType(getArgType(mb, pci, i))) {
			q = newFcnCall(mb, algebraRef, "fetch");
			alias[i] = newTmpVariable(mb, getBatType(getArgType(mb, pci, i)));
			getArg(q, 0) = alias[i];
			q= pushArgument(mb, q, getArg(pci, i));
			(void) pushArgument(mb, q, hvar);
		}
	}

	/* cr:= mod.CMD($1,...,$n); */
	q = newFcnCallArgs(mb, mod, fcn, pci->argc - 2 - plus_one);
	for (i = 0; i < pci->retc; i++) {
		int nvar = 0;
		if (bat) {
			tt = getBatType(getArgType(mb, pci, i));
			nvar = newTmpVariable(mb, newBatType(tt));
		} else {
			nvar = newTmpVariable(mb, TYPE_any);
		}
		if (i)
			q = pushReturn(mb, q, nvar);
		else
			getArg(q, 0) = nvar;
	}

	for (i = pci->retc+2+plus_one; i < pci->argc; i++) {
		if (getArg(pci, i) == iter) {
			q = pushArgument(mb, q, tvar);
		} else if (isaBatType(getArgType(mb, pci, i))) {
			q = pushArgument(mb, q, alias[i]);
		} else {
			q = pushArgument(mb, q, getArg(pci, i));
		}
	}

	for (i = 0; i < pci->retc; i++) {
		InstrPtr a = newFcnCall(mb, batRef, appendRef);
		a = pushArgument(mb, a, resB[i]);
		a = pushArgument(mb, a, getArg(q,i));
		getArg(a,0) = resB[i];
	}

/* redo (h,r):= iterator.next(refBat); */
	q = newFcnCall(mb, iteratorRef, nextRef);
	q->barrier = REDOsymbol;
	getArg(q,0) = hvar;
	q= pushReturn(mb, q, tvar);
	(void) pushArgument(mb,q,iter);

	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	getArg(q,0) = hvar;
	(void) pushReturn(mb, q, tvar);

	for (i = 0; i < pci->retc; i++) {
		q = newAssignment(mb);
		getArg(q, 0) = getArg(pci, i);
		(void) pushArgument(mb, q, resB[i]);
	}
	GDKfree(alias);
	GDKfree(resB);
	return MAL_SUCCEED;
}

/*
 * The multiplexSimple is called by the MAL scenario. It bypasses
 * the optimizer infrastructure, to avoid excessive space allocation
 * and interpretation overhead.
 */
str
OPTmultiplexSimple(Client cntxt, MalBlkPtr mb)
{
	int i, doit=0;
	InstrPtr p;
	str msg = MAL_SUCCEED;

	if(mb)
		for( i=0; i<mb->stop; i++){
			p= getInstrPtr(mb,i);
			if(isMultiplex(p)) {
				p->typechk = TYPE_UNKNOWN;
				doit++;
			}
		}
	if( doit) {
		msg = OPTmultiplexImplementation(cntxt, mb, 0, 0);
		if (!msg)
			msg = chkTypes(cntxt->usermodule, mb,TRUE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	return msg;
}

str
OPTmultiplexImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr *old = 0, p;
	int i, limit, slimit, actions= 0;
	str msg= MAL_SUCCEED;

	(void) stk;
	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb,i);
		if (isMultiplex(p)) {
			break;
		}
	}
	if( i == mb->stop){
		goto wrapup;
	}

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0 )
		throw(MAL,"optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < limit; i++) {
		p = old[i];
		if (msg == MAL_SUCCEED && isMultiplex(p)) {
			if ( MANIFOLDtypecheck(cntxt,mb,p,0) != NULL){
				setFunctionId(p, manifoldRef);
				p->typechk = TYPE_UNKNOWN;
				pushInstruction(mb, p);
				actions++;
				continue;
			}
			msg = OPTexpandMultiplex(cntxt, mb, stk, p);
			if( msg== MAL_SUCCEED){
				freeInstruction(p);
				old[i]=0;
				actions++;
				continue;
			}

			pushInstruction(mb, p);
			actions++;
		} else if( old[i])
			pushInstruction(mb, p);
	}
	for(;i<slimit; i++)
		if( old[i])
			pushInstruction(mb, old[i]);
	GDKfree(old);

	/* Defense line against incorrect plans */
	if( msg == MAL_SUCCEED && actions > 0){
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);

	return msg;
}
