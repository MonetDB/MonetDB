/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

 /* (c) M. Kersten
 */
#include "monetdb_config.h"
#include "opt_prelude.h"
#include "opt_support.h"
#include "mal_interpreter.h"
#include "mal_listing.h"
#include "mal_debugger.h"
#include "opt_multiplex.h"
#include "optimizer_private.h"
#include "manifold.h"

/*
 * Optimizer catalog with runtime statistics;
 */
struct OPTcatalog {
	char *name;
	int enabled;
	int calls;
	int actions;
} optcatalog[]= {
{"aliases",		0,	0,	0},
{"coercions",	0,	0,	0},
{"commonTerms",	0,	0,	0},
{"constants",	0,	0,	0},
{"costModel",	0,	0,	0},
{"crack",		0,	0,	0},
{"datacyclotron",0,	0,	0},
{"dataflow",	0,	0,	0},
{"deadcode",	0,	0,	0},
{"evaluate",	0,	0,	0},
{"factorize",	0,	0,	0},
{"garbage",		0,	0,	0},
{"generator",	0,	0,	0},
{"history",		0,	0,	0},
{"inline",		0,	0,	0},
{"projectionpath",	0,	0,	0},
{"json",		0,	0,	0},
{"macro",		0,	0,	0},
{"matpack",		0,	0,	0},
{"mergetable",	0,	0,	0},
{"mitosis",		0,	0,	0},
{"multiplex",	0,	0,	0},
{"origin",		0,	0,	0},
{"peephole",	0,	0,	0},
{"reduce",		0,	0,	0},
{"remap",		0,	0,	0},
{"remote",		0,	0,	0},
{"reorder",		0,	0,	0},
{"replication",	0,	0,	0},
{"selcrack",	0,	0,	0},
{"sidcrack",	0,	0,	0},
{"strengthreduction",	0,	0,	0},
{"pushselect",	0,	0,	0},
{ 0,	0,	0,	0}
};

/* some optimizers can only be applied once.
 * The optimizer trace at the end of the MAL block
 * can be used to check for this.
 */
int
optimizerIsApplied(MalBlkPtr mb, str optname)
{
	InstrPtr p;
	int i;
	for( i = mb->stop; i < mb->ssize; i++){
		p = getInstrPtr(mb,i);
		if( p == NULL)
			return 0;
		if (getModuleId(p) == optimizerRef && p->token == REMsymbol && strcmp(getFunctionId(p),optname) == 0) 
			return 1;
	}
	return 0;
}
/*
 * All optimizers should pass the optimizerCheck for defense against
 * incomplete and malicious MAL code.
 */
str
optimizerCheck(Client cntxt, MalBlkPtr mb, str name, int actions, lng usec)
{
	char buf[256];
	lng clk = GDKusec();

	if (cntxt->mode == FINISHCLIENT)
		throw(MAL, name, "prematurely stopped client");
	if( actions > 0){
		chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
		chkFlow(cntxt->fdout, mb);
		chkDeclarations(cntxt->fdout, mb);
		usec += GDKusec() - clk;
	}
	/* keep all actions taken as a post block comment */
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec",name,actions,usec);
	newComment(mb,buf);
	if (mb->errors)
		throw(MAL, name, PROGRAM_GENERAL);
	return MAL_SUCCEED;
}

/*
 * Limit the loop count in the optimizer to guard against indefinite
 * recursion, provided the optimizer does not itself generate
 * a growing list.
 */
str
optimizeMALBlock(Client cntxt, MalBlkPtr mb)
{
	InstrPtr p;
	int pc;
	int qot = 0;
	str msg = MAL_SUCCEED;
	int cnt = 0;
	lng clk = GDKusec();
	char buf[256];

	/* assume the type and flow have been checked already */
	/* SQL functions intended to be inlined should not be optimized */
	if ( mb->inlineProp)
        	return 0;

	/* force at least once a complete type check by resetting the type check flag */
	resetMalBlk(mb,mb->stop);
	chkTypes(cntxt->fdout, cntxt->nspace, mb, TRUE);
	chkFlow(cntxt->fdout, mb);
	chkDeclarations(cntxt->fdout, mb);

	do {
		/* any errors should abort the optimizer */
		if (mb->errors)
			break;
		qot = 0;
		for (pc = 0; pc < mb->stop ; pc++) {
			p = getInstrPtr(mb, pc);
			if (getModuleId(p) == optimizerRef && p->fcn && p->token != REMsymbol) {
				/* all optimizers should behave like patterns */
				/* However, we don't have a stack now */
				qot++;
				msg = (str) (*p->fcn) (cntxt, mb, 0, p);
				if (msg) {
					str place = getExceptionPlace(msg);
					msg= createException(getExceptionType(msg), place, "%s", getExceptionMessage(msg));
					GDKfree(place);
					return msg;
				}
				pc= -1;
			}
		}
	} while (qot && cnt++ < mb->stop);
	mb->optimize= GDKusec() - clk;
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","total",1,mb->optimize);
	newComment(mb,buf);
	if (cnt >= mb->stop)
		throw(MAL, "optimizer.MALoptimizer", OPTIMIZER_CYCLE);
	return 0;
}

/*
 * The default MAL optimizer includes a final call to
 * the multiplex expander.
 * We should take care of functions marked as 'inline',
 * because they should be kept in raw form.
 * Their optimization takes place after inlining.
 */
str
MALoptimizer(Client c)
{
	str msg;

	if ( c->curprg->def->inlineProp)
		return MAL_SUCCEED;
	msg= optimizeMALBlock(c, c->curprg->def);
	if( msg == MAL_SUCCEED)
		OPTmultiplexSimple(c, c->curprg->def);
	return msg;
}

/* Only used by opt_commonTerms! */
int hasSameSignature(MalBlkPtr mb, InstrPtr p, InstrPtr q, int stop){
	int i;

	if ( getFunctionId(q) == getFunctionId(p) &&
		 getModuleId(q) == getModuleId(p) &&
		getFunctionId(q) != 0 &&
		getModuleId(q) != 0) {
		if( q->retc != p->retc || q->argc != p->argc) return FALSE;
		assert(stop <= p->argc);
		for( i=0; i<stop; i++)
			if (getArgType(mb,p,i) != getArgType(mb,q,i))
				return FALSE;
		return TRUE;
	}
	return FALSE;
}

/* Only used by opt_commonTerms! */
int hasSameArguments(MalBlkPtr mb, InstrPtr p, InstrPtr q)
{   int k;
	int (*cmp)(const void *, const void *);
	VarPtr w,u;

	(void) mb;
	if( p->retc != q->retc || p->argc != q->argc)
		return FALSE;
	/* heuristic, because instructions are linked using last constant argument */
	for(k=p->argc-1; k >= p->retc; k--)
		if( q->argv[k]!= p->argv[k]){
			if( isVarConstant(mb,getArg(p,k)) &&
				isVarConstant(mb,getArg(q,k)) ) {
					w= getVar(mb,getArg(p,k));
					u= getVar(mb,getArg(q,k));
					cmp = ATOMcompare(w->value.vtype);
					if ( w->value.vtype == u->value.vtype &&
						(*cmp)(VALptr(&w->value), VALptr(&u->value)) == 0)
						continue;
			}
			return FALSE;
		}
	return TRUE;
}

/*
 * If two instructions have elements in common in their target list,
 * it means a variable is re-initialized and should not be considered
 * an alias.
 */
int
hasCommonResults(InstrPtr p, InstrPtr q)
{
	int k, l;

	for (k = 0; k < p->retc; k++)
		for (l = 0; l < q->retc; l++)
			if (p->argv[k] == q->argv[l])
				return TRUE;
	return FALSE;
}
/*
 * Dependency between target variables and arguments can be
 * checked with isDependent().
 */
int
isDependent(InstrPtr p, InstrPtr q){
	int i,j;
	for(i= 0; i<q->retc; i++)
	for(j= p->retc; j<p->argc; j++)
		if( getArg(q,i)== getArg(p,j)) return TRUE;
	return FALSE;
}
/*
 * The safety property should be relatively easy to determine for
 * each MAL function. This calls for accessing the function MAL block
 * and to inspect the arguments of the signature.
 */
int
isUnsafeFunction(InstrPtr q)
{
	InstrPtr p;

	if (q->fcn == 0 || getFunctionId(q) == 0 || q->blk == NULL)
		return FALSE;
	p= getInstrPtr(q->blk,0);
	if( p->retc== 0)
		return TRUE;
	return q->blk->unsafeProp;
}

/*
 * Instructions are unsafe is one of the arguments is also mentioned
 * in the result list. Alternatively, the 'unsafe' property is set
 * for the function call itself.
 */
int
isUnsafeInstruction(InstrPtr q)
{
	int j, k;

	for (j = 0; j < q->retc; j++)
		for (k = q->retc; k < q->argc; k++)
			if (q->argv[k] == q->argv[j])
				return TRUE;
	return FALSE;
}

/*
 * The routine isInvariant determines if the variable V is not
 * changed in the instruction sequence identified by the range [pcf,pcl].
 */
int
isInvariant(MalBlkPtr mb, int pcf, int pcl, int varid)
{
	(void) mb;
	(void) pcf;
	(void) pcl;
	(void) varid;		/*fool compiler */
	return TRUE;
}

/*
 * Any instruction may block identification of a common
 * subexpression. It suffices to stumble upon an unsafe function
 * whose parameter lists has a non-empty intersection with the
 * targeted instruction.
 * To illustrate, consider the sequence
 * @example
 * L1 := f(A,B,C);
 * ...
 * G1 := g(D,E,F);
 * ...
 * l2:= f(A,B,C);
 * ...
 * L2:= h()
 * @end example
 *
 * The instruction G1:=g(D,E,F) is blocking if G1 is an alias
 * for @verb{ { }A,B,C@verb{ } }.
 * Alternatively, function g() may be unsafe and @verb{ { }D,E,F@verb{ } }
 * has a non-empty intersection with @verb{ { }A,B,C@verb{ } }.
 * An alias can only be used later on for readonly (and not be used for a function with side effects).
 */
int
safetyBarrier(InstrPtr p, InstrPtr q)
{
	int i,j;
	if( isDependent(q,p))
		return TRUE;
	if (isUnsafeFunction(q)) {
		for (i = p->retc; i < p->argc; i++)
			for (j = q->retc; j < q->argc; j++)
				if (p->argv[i] == q->argv[j]) {
					/* TODO check safety property of the argument */
					return TRUE;
				}
	}
	return FALSE;
}

/*
 * In many cases we should be assured that a variable is not used in
 * the instruction range identified. For, we may exchange some instructions that
 * might change its content.
 */
#if 0
int
isTouched(MalBlkPtr mb, int varid, int p1, int p2)
{
	int i, k;

	for (i = p1; i < p2; i++) {
		InstrPtr p = getInstrPtr(mb, i);

		for (k = 0; k < p->argc; k++)
			if (p->argv[k] == varid)
				return TRUE;
	}
	return FALSE;
}
#endif

int
isProcedure(MalBlkPtr mb, InstrPtr p)
{
	if (p->retc == 0 || (p->retc == 1 && getArgType(mb,p,0) == TYPE_void))
		return TRUE;
	//if( mb->unsafeProp) return TRUE;
	return FALSE;
}

int
isUpdateInstruction(InstrPtr p){
	if ( getModuleId(p) == sqlRef &&
	   ( getFunctionId(p) == inplaceRef ||
		getFunctionId(p) == appendRef ||
		getFunctionId(p) == updateRef ||
		getFunctionId(p) == replaceRef ))
			return TRUE;
	if ( getModuleId(p) == batRef &&
	   ( getFunctionId(p) == inplaceRef ||
		getFunctionId(p) == appendRef ||
		getFunctionId(p) == updateRef ||
		getFunctionId(p) == replaceRef ))
			return TRUE;
	return FALSE;
}
int
hasSideEffects(InstrPtr p, int strict)
{
	if( getFunctionId(p) == NULL) return FALSE;

	if ( (getModuleId(p) == batRef || getModuleId(p)==sqlRef) &&
	     (getFunctionId(p) == setAccessRef ||
	 	  getFunctionId(p) == setWriteModeRef ||
		  getFunctionId(p) == clear_tableRef))
		return TRUE;

	if (getFunctionId(p) == depositRef)
		return TRUE;

	if (getModuleId(p) == malRef && getFunctionId(p) == multiplexRef)
		return FALSE;

	if( getModuleId(p) == ioRef ||
		getModuleId(p) == streamsRef ||
		getModuleId(p) == bstreamRef ||
		getModuleId(p) == mdbRef ||
		getModuleId(p) == malRef ||
		getModuleId(p) == remapRef ||
		getModuleId(p) == constraintsRef ||
		getModuleId(p) == optimizerRef ||
		getModuleId(p) == lockRef ||
		getModuleId(p) == semaRef ||
		getModuleId(p) == alarmRef)
		return TRUE;
		
	if( getModuleId(p) == pyapiRef ||
		getModuleId(p) == pyapimapRef ||
		getModuleId(p) == rapiRef)
		return TRUE;

	if (getModuleId(p) == sqlRef){
		if (getFunctionId(p) == tidRef) return FALSE;
		if (getFunctionId(p) == deltaRef) return FALSE;
		if (getFunctionId(p) == subdeltaRef) return FALSE;
		if (getFunctionId(p) == projectdeltaRef) return FALSE;
		if (getFunctionId(p) == bindRef) return FALSE;
		if (getFunctionId(p) == bindidxRef) return FALSE;
		if (getFunctionId(p) == binddbatRef) return FALSE;
		if (getFunctionId(p) == columnBindRef) return FALSE;
		if (getFunctionId(p) == copy_fromRef) return FALSE;
		/* assertions are the end-point of a flow path */
		if (getFunctionId(p) == not_uniqueRef) return FALSE;
		if (getFunctionId(p) == zero_or_oneRef) return FALSE;
		if (getFunctionId(p) == mvcRef) return FALSE;
		if (getFunctionId(p) == singleRef) return FALSE;
		/* the update instructions for SQL has side effects.
		   whether this is relevant should be explicitly checked
		   in the environment of the call */
		if (isUpdateInstruction(p)) return TRUE;
		return TRUE;
	}
	if( getModuleId(p) == languageRef){
		if( getFunctionId(p) == assertRef) return TRUE;
		return FALSE;
	}
	if (getModuleId(p) == constraintsRef)
		return FALSE;
	if( getModuleId(p) == mapiRef){
		if( getFunctionId(p) == rpcRef)
			return TRUE;
		if( getFunctionId(p) == reconnectRef)
			return TRUE;
		if( getFunctionId(p) == disconnectRef)
			return TRUE;
	}
	if (strict &&  getFunctionId(p) == newRef &&
		getModuleId(p) != groupRef )
		return TRUE;

	if ( getModuleId(p) == remoteRef)
		return TRUE;
	return FALSE;
}

/* Void returning functions always have side-effects.
 */
int
mayhaveSideEffects(Client cntxt, MalBlkPtr mb, InstrPtr p, int strict)
{
	int tpe;
	tpe= getVarType(mb,getArg(p,0));
	if( tpe == TYPE_void)
		return TRUE;
	if (getModuleId(p) != malRef || getFunctionId(p) != multiplexRef) 
		return hasSideEffects( p, strict);
	if (MANIFOLDtypecheck(cntxt,mb,p) == NULL)
		return TRUE;
	return FALSE;
}

/*
 * Side-effect free functions are crucial for several operators.
 */
int
isSideEffectFree(MalBlkPtr mb){
	int i;
	for(i=1; i< mb->stop && getInstrPtr(mb,i)->token != ENDsymbol; i++){
		if( hasSideEffects(getInstrPtr(mb,i), TRUE))
			return FALSE;
	}
	return TRUE;
}
/*
 * Breaking up a MAL program into pieces for distributed requires
 * identification of (partial) blocking instructions. A conservative
 * definition can be used.
 */
int
isBlocking(InstrPtr p)
{
	if (blockStart(p) || blockExit(p) || blockCntrl(p))
		return TRUE;

	if ( getFunctionId(p) == sortRef )
		return TRUE;

	if( getModuleId(p) == aggrRef ||
		getModuleId(p) == groupRef ||
		getModuleId(p) == sqlRef )
			return TRUE;
	return FALSE;
}

int isAllScalar(MalBlkPtr mb, InstrPtr p)
{
	int i;
	for (i=p->retc; i<p->argc; i++)
	if (isaBatType(getArgType(mb,p,i)) || getArgType(mb,p,i)==TYPE_bat)
		return FALSE;
	return TRUE;
}

/*
 * Used in the merge table optimizer. It is built incrementally
 * and should be conservative.
 */

static int 
isOrderDepenent(InstrPtr p)
{
    if( getModuleId(p) != batsqlRef)
        return 0;
    if ( getFunctionId(p) == diffRef ||
        getFunctionId(p) == row_numberRef ||
        getFunctionId(p) == rankRef ||
        getFunctionId(p) == dense_rankRef)
        return 1;
    return 0;
}

int isMapOp(InstrPtr p){
	return	getModuleId(p) &&
		((getModuleId(p) == malRef && getFunctionId(p) == multiplexRef) ||
		 (getModuleId(p) == malRef && getFunctionId(p) == manifoldRef) ||
		 (getModuleId(p) == batcalcRef) ||
		 (getModuleId(p) != batcalcRef && getModuleId(p) != batRef && strncmp(getModuleId(p), "bat", 3) == 0) ||
		 (getModuleId(p) == mkeyRef)) && !isOrderDepenent(p) &&
		 getModuleId(p) != batrapiRef &&
		 getModuleId(p) != batpyapiRef;
}

int isLikeOp(InstrPtr p){
	return	(getModuleId(p) == batalgebraRef &&
		(getFunctionId(p) == likeRef || 
		 getFunctionId(p) == not_likeRef || 
		 getFunctionId(p) == ilikeRef ||
		 getFunctionId(p) == not_ilikeRef));
}

int 
isTopn(InstrPtr p)
{
	return ((getModuleId(p) == algebraRef && getFunctionId(p) == firstnRef) ||
			isSlice(p));
}

int 
isSlice(InstrPtr p)
{
	return (getModuleId(p) == algebraRef &&
	   (getFunctionId(p) == subsliceRef || getFunctionId(p) == sliceRef)); 
}

int 
isSample(InstrPtr p)
{
	return (getModuleId(p) == sampleRef && getFunctionId(p) == subuniformRef);
}

int isOrderby(InstrPtr p){
	return getModuleId(p) == algebraRef &&
		(getFunctionId(p) == sortRef ||
		 getFunctionId(p) == sortReverseRef);
}

int 
isMatJoinOp(InstrPtr p)
{
	return (isSubJoin(p) || (getModuleId(p) == algebraRef &&
                (getFunctionId(p) == crossRef ||
                 getFunctionId(p) == subjoinRef ||
                 getFunctionId(p) == subantijoinRef || /* is not mat save */
                 getFunctionId(p) == subthetajoinRef ||
                 getFunctionId(p) == subbandjoinRef ||
                 getFunctionId(p) == subrangejoinRef)
		));
}

int 
isMatLeftJoinOp(InstrPtr p)
{
	return (getModuleId(p) == algebraRef && 
		getFunctionId(p) == subleftjoinRef);
}

int isDelta(InstrPtr p){
	return
			(getModuleId(p)== sqlRef && (
				getFunctionId(p)== deltaRef ||
				getFunctionId(p)== projectdeltaRef ||
				getFunctionId(p)== subdeltaRef 
			) 
		);
}

int isFragmentGroup2(InstrPtr p){
	return
			(getModuleId(p)== algebraRef && (
				getFunctionId(p)== projectionRef
			)) ||
			(getModuleId(p)== batRef && (
				getFunctionId(p)== mergecandRef || 
				getFunctionId(p)== intersectcandRef 
			) 
		);
}

int isSubSelect(InstrPtr p)
{
	char *func = getFunctionId(p);
	size_t l = func?strlen(func):0;
	
	return (l >= 9 && strcmp(func+l-9,"subselect") == 0);
}

int isSubJoin(InstrPtr p)
{
	char *func = getFunctionId(p);
	size_t l = func?strlen(func):0;
	
	return (l >= 7 && strcmp(func+l-7,"subjoin") == 0);
}

int isMultiplex(InstrPtr p)
{
	return ((getModuleId(p) == malRef || getModuleId(p) == batmalRef) &&
		getFunctionId(p) == multiplexRef);
}

int isFragmentGroup(InstrPtr p){
	return
			(getModuleId(p)== algebraRef && (
				getFunctionId(p)== projectRef ||
				getFunctionId(p)== selectNotNilRef
			))  ||
			isSubSelect(p) ||
			(getModuleId(p)== batRef && (
				getFunctionId(p)== mirrorRef 
			));
}

/*
 * Some optimizers are interdependent (e.g. mitosis ), which
 * requires inspection of the pipeline attached to a MAL block.
 */
int
isOptimizerEnabled(MalBlkPtr mb, str opt)
{
	int i;
	InstrPtr q;

	for (i= mb->stop-1; i > 0; i--){
		q= getInstrPtr(mb,i);
		if ( q->token == ENDsymbol)
			break;
		if ( getModuleId(q) == optimizerRef &&
			 getFunctionId(q) == opt)
			return 1;
	}
	return 0;
}

