/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_prelude.h"
#include "opt_macro.h"
#include "mal_interpreter.h"
#include "mal_instruction.h"

static int
malMatch(InstrPtr p1, InstrPtr p2)
{
	int i, j;

	if (getFunctionId(p1) == 0 && getFunctionId(p2) != 0)
		return 0;
	if (getModuleId(p1) == 0 && getModuleId(p2) != 0)
		return 0;
	if (getModuleId(p1) != getModuleId(p2))
		return 0;
	if (getFunctionId(p2) == 0)
		return 0;
	if (getFunctionId(p1) != getFunctionId(p2))
		return 0;
	if (p1->retc != p2->retc)
		return 0;
	if (p1->argc != p2->argc)
		return 0;
	if (p1->barrier != p2->barrier)
		return 0;
	for (i = 0; i < p1->argc; i++)
		for (j = i + 1; j < p1->argc; j++)
			if ((getArg(p1, i) == getArg(p1, j)) != (getArg(p2, i) == getArg(p2, j)))
				return 0;
	return 1;
}

/*
 * Matching a block calls for building two variable lists used.
 * The isomorphism can be determined after-wards using a single scan.
 * The candidate block is matched with mb starting at a given pc.
 * The candidate block is expected to defined as a function, including
 * a signature and end-statement. They are ignored in the comparison
 *
 * Beware, the variables in the block being removed, could be
 * used furtheron in the program. [tricky to detect, todo]
 */
static int
malFcnMatch(MalBlkPtr mc, MalBlkPtr mb, int pc)
{
	int i, j, k, lim;
	int *cvar, *mvar;
	int ctop = 0, mtop = 0;
	InstrPtr p, q;

	if (mb->stop - pc < mc->stop - 2)
		return 0;

	cvar = (int *) GDKmalloc(mc->vtop * mc->maxarg * sizeof(*cvar));
	if (cvar == NULL)
		return 0;
	mvar = (int *) GDKmalloc(mb->vtop * mc->maxarg * sizeof(*mvar));
	if (mvar == NULL){
		GDKfree(cvar);
		return 0;
	}
	/* also trim the return statement */
	lim = pc + mc->stop - 3;
	k = 1;
	for (i = pc; i < lim; i++, k++) {
		p = getInstrPtr(mb, i);
		q = getInstrPtr(mc, k);
		if (malMatch(p, q) == 0){
			GDKfree(cvar);
			GDKfree(mvar);
			return 0;
		}
		for (j = 0; j < p->argc; j++)
			cvar[ctop++] = getArg(p, j);

		for (j = 0; j < q->argc; j++)
			mvar[mtop++] = getArg(q, j);
	}
	assert(mtop == ctop);	/*shouldn't happen */

	for (i = 0; i < ctop; i++)
		for (j = i + 1; j < ctop; j++)
			if ((cvar[i] == cvar[j]) != (mvar[i] == mvar[j])) {
				GDKfree(cvar);
				GDKfree(mvar);
				return 0;
			}
	GDKfree(cvar);
	GDKfree(mvar);
	return 1;
}
/*
 * Macro expansions
 * The macro expansion routine walks through the MAL code block in search
 * for the function to be expanded.
 * The macro expansion process is restarted at the first new instruction.
 * A global is used to protect at (direct) recursive expansions
 */
#define MAXEXPANSION 256

int
inlineMALblock(MalBlkPtr mb, int pc, MalBlkPtr mc)
{
	int i, k, l, n;
	InstrPtr *ns, p,q;
	int *nv;

	p = getInstrPtr(mb, pc);
	q = getInstrPtr(mc, 0);
	ns = GDKzalloc((l = (mb->ssize + mc->ssize + p->retc - 3)) * sizeof(InstrPtr));
	if (ns == NULL)
		return -1;
	nv = (int*) GDKmalloc(mc->vtop * sizeof(int));
	if (nv == 0){
		GDKfree(ns);
		return -1;
	}

	/* add all variables of the new block to the target environment */
	for (n = 0; n < mc->vtop; n++) {
		if (isExceptionVariable(getVarName(mc,n))) {
			nv[n] = newVariable(mb, getVarName(mc,n), strlen(getVarName(mc,n)), TYPE_str);
			if (isVarUDFtype(mc,n))
				setVarUDFtype(mb,nv[n]);
		} else if (isVarTypedef(mc,n)) {
			nv[n] = newTypeVariable(mb,getVarType(mc,n));
		} else if (isVarConstant(mc,n)) {
			nv[n] = cpyConstant(mb,getVar(mc,n));
		} else {
			nv[n] = newTmpVariable(mb, getVarType(mc, n));
			if (isVarUDFtype(mc,n))
				setVarUDFtype(mb,nv[n]);
		}
	}

	/* use an alias mapping to keep track of the actual arguments */
	for (n = p->retc; n < p->argc; n++)
		nv[getArg(q,n)] = getArg(p, n);

	k = 0;
	/* find the return statement of the inline function */
	for (i = 1; i < mc->stop - 1; i++) {
		q = mc->stmt[i];
		if( q->barrier== RETURNsymbol || q->barrier== YIELDsymbol){
			/* add the mapping of the return variables */
			for(n=0; n<p->retc; n++)
				nv[getArg(q,n)] = getArg(p,n);
		}
	}

	/* copy the stable part */
	for (i = 0; i < pc; i++)
		ns[k++] = mb->stmt[i];

	for (i = 1; i < mc->stop - 1; i++) {
		q = mc->stmt[i];
		if( q->token == ENDsymbol)
			break;

		/* copy the instruction and fix variable references */
		ns[k] = copyInstruction(q);
		if( ns[k] == NULL)
			return -1;

		for (n = 0; n < q->argc; n++)
			getArg(ns[k], n) = nv[getArg(q, n)];

		if (q->barrier == RETURNsymbol || q->barrier == YIELDsymbol) {
			for(n=0; n<q->retc; n++)
				clrVarFixed(mb,getArg(ns[k],n)); /* for typing */
			setModuleId(ns[k],getModuleId(q));
			setFunctionId(ns[k],getFunctionId(q));
			ns[k]->typechk = TYPE_UNKNOWN;
			ns[k]->barrier = 0;
			ns[k]->token = ASSIGNsymbol;
		}
		k++;
	} 

	/* copy the remainder of the stable part */
	freeInstruction(p);
	for (i = pc + 1; i < mb->stop; i++){
		ns[k++] = mb->stmt[i];
	}
	/* remove any free instruction */
	for(; i<mb->ssize; i++)
	if( mb->stmt[i]){
		freeInstruction(mb->stmt[i]);
		mb->stmt[i]= 0;
	}
	GDKfree(mb->stmt);
	mb->stmt = ns;

	mb->ssize = l;
	mb->stop = k;
	GDKfree(nv);
	return pc;
}

/*
 * The macro processor should be careful in replacing the
 * instruction. In particular, any RETURN or YIELD statement
 * should be replaced by a jump. For the time being,
 * we only allow for a single return statement at the end
 * of the block.
 * The semantic test is encapsulated in a routines.
 */

static str
MACROvalidate(MalBlkPtr mb)
{
	int retseen = 0;
	int i;
	InstrPtr p = 0;

	if (getArgType(mb, getInstrPtr(mb, 0), 0) == TYPE_void)
		return MAL_SUCCEED;

	for (i = 1; retseen == 0 && i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		retseen = p->token == RETURNsymbol || p->token == YIELDsymbol || p->barrier == RETURNsymbol || p->barrier == YIELDsymbol;
	}
	if (retseen && i != mb->stop - 1)
		throw(MAL, "optimizer.MACROvalidate", SQLSTATE(HY002) MACRO_SYNTAX_ERROR);
	return MAL_SUCCEED;
}

str
MACROprocessor(Client cntxt, MalBlkPtr mb, Symbol t)
{
	InstrPtr q;
	int i, cnt = 0, last = -1;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	if (t == NULL)
		return msg;
	msg = MACROvalidate(t->def);
	if (msg)
		return msg;
	for (i = 0; i < mb->stop; i++) {
		q = getInstrPtr(mb, i);
		if (getFunctionId(q) && idcmp(getFunctionId(q), t->name) == 0 && 
			getSignature(t)->token == FUNCTIONsymbol) {
			if (i == last)
				throw(MAL, "optimizer.MACROoptimizer", SQLSTATE(HY002) MACRO_DUPLICATE);

			last = i;
			i = inlineMALblock(mb, i, t->def);
			if( i < 0)
				throw(MAL, "optimizer.MACROoptimizer", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				
			cnt++;
			if (cnt > MAXEXPANSION)
				throw(MAL, "optimizer.MACROoptimizer", SQLSTATE(HY002) MACRO_TOO_DEEP);
		}
	}
	return msg;
}

/*
 * Macro inversions map a consecutive sequences of MAL instructions
 * into a single call. Subsequent resolution will bind it with the proper
 * function. The pattern being replaced should be a self-standing
 * assignment. [could be improved]
 *
 * The function being replaced should assign the result to
 * the signature variables. Otherwise it will be difficult
 * to assess which result to retain.
 */
static int
replaceMALblock(MalBlkPtr mb, int pc, MalBlkPtr mc)
{
	int i, j, k, lim;
	InstrPtr p, q, rq;
	int *cvar, *mvar;
	int ctop = 0, mtop = 0;

	/* collect variable map */
	cvar = (int *) GDKmalloc(mc->vtop * mc->maxarg * sizeof(*cvar));
	if (cvar == NULL)
		return -1;
	mvar = (int *) GDKmalloc(mb->vtop * mc->maxarg * sizeof(*mvar));
	if (mvar == NULL){
		GDKfree(cvar);
		return -1;
	}
	lim = pc + mc->stop - 3;
	k = 1;
	for (i = pc; i < lim; i++, k++) {
		p = getInstrPtr(mb, i);
		q = getInstrPtr(mc, k);
		for (j = 0; j < q->argc; j++)
			cvar[ctop++] = getArg(q, j);
		assert(ctop < mc->vtop *mc->maxarg);

		for (j = 0; j < p->argc; j++)
			mvar[mtop++] = getArg(p, j);
	}
	assert(mtop == ctop);	/*shouldn't happen */

	p = getInstrPtr(mb, pc);
	q = copyInstruction(getInstrPtr(mc, 0));	/* the signature */
	if( q == NULL)
		return -1;
	q->token = ASSIGNsymbol;
	mb->stmt[pc] = q;

	for (i = q->retc; i < q->argc; i++)
		for (j = 0; j < ctop; j++)
			if (q->argv[i] == cvar[j]) {
				q->argv[i] = mvar[j];
				break;
			}
	/* take the return expression  and match the variables*/
	rq = getInstrPtr(mc, mc->stop - 2);
	for (i = 0; i < rq->retc; i++)
		for (j = 0; j < ctop; j++)
			if (rq->argv[i+rq->retc] == cvar[j]) {
				q->argv[i] = mvar[j];
				break;
			}
	freeInstruction(p);

	/* strip signature, return, and end statements */
	k = mc->stop - 3;
	j = pc + k;
	for (i = pc + 1; i < pc + k; i++)
		freeInstruction(mb->stmt[i]);

	for (i = pc + 1; i < mb->stop - k; i++)
		mb->stmt[i] = mb->stmt[j++];

	k = i;
	for (; i < mb->stop; i++)
		mb->stmt[i] = 0;

	mb->stop = k;
	GDKfree(cvar);
	GDKfree(mvar);
	return pc;
}

static str
ORCAMprocessor(Client cntxt, MalBlkPtr mb, Symbol t)
{
	MalBlkPtr mc;
	int i;
	str msg = MAL_SUCCEED;

	if (t == NULL )
		return msg;	/* ignore the call */
	mc = t->def;
	if ( mc->stop < 3)
		return msg;	/* ignore small call */

	/* strip signature, return, and end statements */
	for (i = 1; i < mb->stop - mc->stop + 3; i++)
		if (malFcnMatch(mc, mb, i)) {
			msg = MACROvalidate(mc);
			if (msg == MAL_SUCCEED){
				if( replaceMALblock(mb, i, mc) < 0)
					throw(MAL,"orcam", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			} else
				break;
		}
	chkTypes(cntxt->usermodule, mb, FALSE);
	chkFlow(mb);
	chkDeclarations(mb);
	return msg;
}

str
OPTmacroImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	MalBlkPtr target= mb;
	Module s;
	Symbol t;
	str mod,fcn;
	int j;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;

	if( p->argc == 3){
		mod= getArgDefault(mb,p,1);
		fcn= getArgDefault(mb,p,2);
	} else {
		mod= getArgDefault(mb,p,1);
		fcn= getArgDefault(mb,p,2);
		t= findSymbol(cntxt->usermodule, putName(mod), fcn);
		if( t == 0)
			return 0;
		target= t->def;
		mod= getArgDefault(mb,p,3);
		fcn= getArgDefault(mb,p,4);
	}
	s = findModule(cntxt->usermodule, putName(mod));
	if (s == 0)
		return 0;
	if (s->space) {
		j = getSymbolIndex(fcn);
		for (t = s->space[j]; t != NULL; t = t->peer)
			if (t->def->errors == 0) {
				if (getSignature(t)->token == FUNCTIONsymbol){
					msg = MACROprocessor(cntxt, target, t);
					// failures from the macro expansion are ignored
					// They leave the scene as is
					if ( msg)
						freeException(msg);
				}
			}
	}
	return MAL_SUCCEED;
}
/*
 * The optimizer call infrastructure is identical to the liners
 * function with the exception that here we inline all possible
 * functions, regardless their
 */

str
OPTorcamImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	MalBlkPtr target= mb;
	Module s;
	Symbol t;
	str mod,fcn;
	int j;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;

	if( p->argc == 3){
		mod= getArgDefault(mb,p,1);
		fcn= getArgDefault(mb,p,2);
	} else {
		mod= getArgDefault(mb,p,1);
		fcn= getArgDefault(mb,p,2);
		t= findSymbol(cntxt->usermodule, putName(mod), fcn);
		if( t == 0)
			return 0;
		target= t->def;
		mod= getArgDefault(mb,p,3);
		fcn= getArgDefault(mb,p,4);
	}
	s = findModule(cntxt->usermodule, putName(mod));
	if (s == 0)
		return 0;
	if (s->space) {
		j = getSymbolIndex(fcn);
		for (t = s->space[j]; t != NULL; t = t->peer)
			if (t->def->errors == 0) {
				if (getSignature(t)->token == FUNCTIONsymbol) {
					freeException(msg);
					msg =ORCAMprocessor(cntxt, target, t);
				}
			}
	}
	return msg;
}

str OPTmacro(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	Symbol t;
	str msg,mod,fcn;
	lng clk= GDKusec();
	char buf[256];
	lng usec = GDKusec();

	if( p ==NULL )
		return 0;
	removeInstruction(mb, p);
	if( p->argc == 3){
		mod= getArgDefault(mb,p,1);
		fcn= getArgDefault(mb,p,2);
	} else {
		mod= getArgDefault(mb,p,3);
		fcn= getArgDefault(mb,p,4);
	}
	t= findSymbol(cntxt->usermodule, putName(mod), fcn);
	if( t == 0)
		return 0;

	msg = MACROvalidate(t->def);
	if( msg) 
		return msg;
	if( mb->errors == 0)
		msg= OPTmacroImplementation(cntxt,mb,stk,p);
	// similar to OPTmacro
	if( msg) {
		GDKfree(msg);
		msg= MAL_SUCCEED;
	}

    /* Defense line against incorrect plans */
	chkTypes(cntxt->usermodule, mb, FALSE);
	chkFlow(mb);
	chkDeclarations(mb);
	usec += GDKusec() - clk;
	/* keep all actions taken as a post block comment */
	snprintf(buf,256,"%-20s actions=1 time=" LLFMT " usec","macro",usec);
	newComment(mb,buf);
	addtoMalBlkHistory(mb);
	if (mb->errors)
		throw(MAL, "optimizer.macro", SQLSTATE(42000) PROGRAM_GENERAL);
	return msg;
}

str OPTorcam(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	Symbol t;
	str mod,fcn;
	lng clk= GDKusec();
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	if( p ==NULL )
		return 0;
	removeInstruction(mb, p);
	if( p->argc == 3){
		mod= getArgDefault(mb,p,1);
		fcn= getArgDefault(mb,p,2);
	} else {
		mod= getArgDefault(mb,p,3);
		fcn= getArgDefault(mb,p,4);
	}
	t= findSymbol(cntxt->usermodule, putName(mod), fcn);
	if( t == 0)
		return 0;

	msg = MACROvalidate(t->def);
	if( msg) 
		return msg;
	if( mb->errors == 0)
		msg= OPTorcamImplementation(cntxt,mb,stk,p);
	if( msg) 
		return msg;
	chkTypes(cntxt->usermodule, mb, FALSE);
	chkFlow(mb);
	chkDeclarations(mb);
	usec += GDKusec() - clk;
	/* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions=1 time=" LLFMT " usec","orcam",usec);
	newComment(mb,buf);
	addtoMalBlkHistory(mb);
	if (mb->errors)
		throw(MAL, "optimizer.orcam", SQLSTATE(42000) PROGRAM_GENERAL);
	return msg;
}
