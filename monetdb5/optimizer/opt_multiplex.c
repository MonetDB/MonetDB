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
#include "opt_multiplex.h"
#include "mal_interpreter.h"

/*
 * The generic solution to the multiplex operators is to translate
 * them to a MAL loop.
 * The call optimizer.multiplex(MOD,FCN,A1,...An) introduces the following code
 * structure:
 *
 * @verbatim
 *  A1rev:=bat.reverse(A1);
 * 	resB:= bat.new(A1);
 * barrier (h,t):= iterator.new(A1);
 * 	$1:= algebra.fetch(A1,h);
 * 	$2:= A2;	# in case of constant?
 * 	...
 * 	cr:= MOD.FCN($1,...,$n);
 *  y:=algebra.fetch(A1rev,h);
 * 	bat.insert(resB,y,cr);
 * 	redo (h,t):= iterator.next(A1);
 * end h;
 * @end verbatim
 *
 * The algorithm consists of two phases: phase one deals with
 * collecting the relevant information, phase two is the actual
 * code construction.
 */
static str
OPTexpandMultiplex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i = 2, resB, iter = 0, cr;
	int hvar, tvar;
	int x, y;
	str mod, fcn;
	int *alias;
	InstrPtr q;
	int ht, tt;

	(void) cntxt;
	(void) stk;

	ht = getHeadType(getArgType(mb, pci, 0));
	if (ht != TYPE_oid)
		throw(MAL, "optimizer.multiplex", "Target head type is missing");
	tt = getTailType(getArgType(mb, pci, 0));
	if (tt== TYPE_any)
		throw(MAL, "optimizer.multiplex", "Target tail type is missing");
	if (isAnyExpression(getArgType(mb, pci, 0)))
		throw(MAL, "optimizer.multiplex", "Target type is missing");

	mod = VALget(&getVar(mb, getArg(pci, pci->retc))->value);
	mod = putName(mod,strlen(mod));
	fcn = VALget(&getVar(mb, getArg(pci, pci->retc+1))->value);
	fcn = putName(fcn,strlen(fcn));
	mnstr_printf(GDKstdout,"#Bulk operator required for %s.%s\n", mod,fcn);

	/* search the iterator bat */
	for (i = pci->retc+2; i < pci->argc; i++)
		if (isaBatType(getArgType(mb, pci, i))) {
			iter = getArg(pci, i);
			if (getHeadType(getVarType(mb,iter)) != TYPE_oid)
				throw(MAL, "optimizer.multiplex", "Iterator BAT is not OID-headed");
			break;
		}
	if( i == pci->argc)
		throw(MAL, "optimizer.multiplex", "Iterator BAT type is missing");

	OPTDEBUGmultiplex {
		mnstr_printf(cntxt->fdout,"#calling the optimize multiplex script routine\n");
		printFunction(cntxt->fdout,mb, 0, LIST_MAL_ALL );
		mnstr_printf(cntxt->fdout,"#multiplex against operator %d %s\n",iter, getTypeName(getVarType(mb,iter)));
		printInstruction(cntxt->fdout,mb, 0, pci,LIST_MAL_ALL);
	}
	/*
	 * Beware, the operator constant (arg=1) is passed along as well,
	 * because in the end we issue a recursive function call that should
	 * find the actual arguments at the proper place of the callee.
	 */

	alias= (int*) GDKmalloc(sizeof(int) * pci->maxarg);
	if (alias == NULL)
		return NULL;

	/* x := bat.reverse(A1); */
	x = newTmpVariable(mb, newBatType(getTailType(getVarType(mb,iter)),
									  getHeadType(getVarType(mb,iter))));
	q = newFcnCall(mb, batRef, reverseRef);
	getArg(q, 0) = x;
	q = pushArgument(mb, q, iter);

	/* resB := new(refBat) */
	q = newFcnCall(mb, batRef, newRef);
	resB = getArg(q, 0);

	setVarType(mb, getArg(q, 0), newBatType(ht, tt));
	q = pushType(mb, q, ht);
	q = pushType(mb, q, tt);
	/* barrier (h,r) := iterator.new(refBat); */
	q = newFcnCall(mb, iteratorRef, newRef);
	q->barrier = BARRIERsymbol;
	hvar = newTmpVariable(mb, TYPE_any);
	getArg(q,0) = hvar;
	tvar = newTmpVariable(mb, TYPE_any);
	q= pushReturn(mb, q, tvar);
	(void) pushArgument(mb,q,iter);

	/* $1:= algebra.fetch(Ai,h) or constant */
	alias[i] = tvar;

	for (i++; i < pci->argc; i++)
		if (isaBatType(getArgType(mb, pci, i))) {
			q = newFcnCall(mb, algebraRef, "fetch");
			alias[i] = newTmpVariable(mb, getTailType(getArgType(mb, pci, i)));
			getArg(q, 0) = alias[i];
			q= pushArgument(mb, q, getArg(pci, i));
			(void) pushArgument(mb, q, hvar);
		}

	/* cr:= mod.CMD($1,...,$n); */
	q = newFcnCall(mb, mod, fcn);
	cr = getArg(q, 0) = newTmpVariable(mb, TYPE_any);

	for (i = pci->retc+2; i < pci->argc; i++)
		if (isaBatType(getArgType(mb, pci, i))) {
			q= pushArgument(mb, q, alias[i]);
		} else {
			q = pushArgument(mb, q, getArg(pci, i));
		}

	/* y := algebra.fetch(x,h); */
	y = newTmpVariable(mb, getHeadType(getVarType(mb,iter)));
	q = newFcnCall(mb, algebraRef, "fetch");
	getArg(q, 0) = y;
	q = pushArgument(mb, q, x);
	q = pushArgument(mb, q, hvar);

	/* insert(resB,h,cr);
	   not append(resB, cr); the head type (oid) may dynamically change */

	q = newFcnCall(mb, batRef, insertRef);
	q= pushArgument(mb, q, resB);
	q= pushArgument(mb, q, y);
	(void) pushArgument(mb, q, cr);

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

	q = newAssignment(mb);
	getArg(q, 0) = getArg(pci, 0);
	(void) pushArgument(mb, q, resB);
	GDKfree(alias);
	return MAL_SUCCEED;
}

/*
 * The multiplexSimple is called by the MAL scenario. It bypasses
 * the optimizer infrastructure, to avoid excessive space allocation
 * and interpretation overhead.
 */
str
OPTmultiplexSimple(Client cntxt)
{
	MalBlkPtr mb= cntxt->curprg->def;
	int i, doit=0;
	InstrPtr p;
	if(mb)
	for( i=0; i<mb->stop; i++){
		p= getInstrPtr(mb,i);
		if(getModuleId(p) == malRef && getFunctionId(p) == multiplexRef)
			doit++;
	}
	if( doit) {
		OPTmultiplexImplementation(cntxt, mb, 0, 0);
		chkTypes(cntxt->fdout, cntxt->nspace, mb,TRUE);
		if ( mb->errors == 0) {
			chkFlow(cntxt->fdout, mb);
			chkDeclarations(cntxt->fdout,mb);
		}
	}
	return 0;
}
int
OPTmultiplexImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr *old, p;
	int i, limit, slimit, actions= 0;
	str msg= MAL_SUCCEED;

	(void) stk;
	(void) pci;

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0 )
		return 0;

	for (i = 0; i < limit; i++) {
		p = old[i];
		if (msg == MAL_SUCCEED &&
                    getModuleId(p) == malRef &&
		    getFunctionId(p) == multiplexRef) {
			msg = OPTexpandMultiplex(cntxt, mb, stk, p);
			if( msg== MAL_SUCCEED){
				freeInstruction(p);
				old[i]=0;
			} else {
				pushInstruction(mb, p);
			}
			actions++;
		} else if( old[i])
			pushInstruction(mb, p);
	}
	for(;i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	if (mb->errors){
		/* rollback */
	}
	return mb->errors? 0: actions;
}
