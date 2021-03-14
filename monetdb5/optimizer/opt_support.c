/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

/* Some optimizers can/need only be applied once.
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
		if (p && getModuleId(p) == optimizerRef && p->token == REMsymbol && strcmp(getFunctionId(p),optname) == 0)
			return 1;
	}
	return 0;
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
		if ( getModuleId(q) == optimizerRef && getFunctionId(q) == opt)
			return 1;
	}
	return 0;
}

/*
 * Optimizers leave behind information on the number of actions taken.
 * This information can be handy to avoid steps further down the pipeline.
 */
int
isOptimizerUsed(MalBlkPtr mb, str opt)
{
	int i, cnt;
	char *s, *haystack;
	InstrPtr q;

	for (i= mb->stop-1; i > 0; i--){
		q= getInstrPtr(mb,i);
		if ( q->token == ENDsymbol)
			break;
		if (q && q->token == REMsymbol && getModuleId(q) == 0 && q->argc > 0){
			//decompose the message
			haystack = getVarConstant(mb,getArg(q,0)).val.sval;
			if ( ! haystack)
				continue;
			s = strstr(haystack, opt);
			if( !s)
				continue;
			s = strstr(haystack, "actions=");
			if( s){
				cnt = atoi(s + 8);
				return cnt;
			}
		}
	}
	return 0;
}

/* Simple insertion statements do not require complex optimizer steps */
int
isSQLinsert(MalBlkPtr mb)
{
        int cnt = 0;
        int i;
        InstrPtr p;

        for(i = 0; i < mb->stop; i++) {
                p = getInstrPtr(mb,i);
                if (p &&  getModuleId(p) == sqlRef && getFunctionId(p) == appendRef ){
                        cnt ++;
                }
		if (p &&  getModuleId(p) == sqlRef && getFunctionId(p) == setVariableRef ){
                        cnt ++;
                }

        }
        return cnt > 0.63 * mb->stop;
}


/* Hypothetical, optimizers may massage the plan in such a way
 * that multiple passes are needed.
 * However, the current SQL driven approach only expects a single
 * non-repeating pipeline of optimizer steps stored at the end of the MAL block.
 * A single scan forward over the MAL plan is assumed.
 */
str
optimizeMALBlock(Client cntxt, MalBlkPtr mb)
{
	InstrPtr p;
	int pc, oldstop;
	str msg = MAL_SUCCEED;
	int cnt = 0;
	int actions = 0;
	lng clk = GDKusec();
	char buf[256];

	/* assume the type and flow have been checked already */
	/* SQL functions intended to be inlined should not be optimized */
	if ( mb->inlineProp)
		return 0;

	mb->optimize = 0;
	if (mb->errors)
		throw(MAL, "optimizer.MALoptimizer", SQLSTATE(42000) "Start with inconsistent MAL plan");

	// strong defense line, assure that MAL plan is initially correct
	if( mb->errors == 0 && mb->stop > 1){
		resetMalBlk(mb, mb->stop);
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
		if (msg)
			return msg;
		if (mb->errors != MAL_SUCCEED){
			msg = mb->errors;
			mb->errors = MAL_SUCCEED;
			return msg;
		}
	}

	oldstop = mb->stop;
	for (pc = 0; pc < mb->stop; pc++) {
		p = getInstrPtr(mb, pc);
		if (getModuleId(p) == optimizerRef && p->fcn && p->token != REMsymbol) {
			actions++;
			msg = (str) (*p->fcn) (cntxt, mb, 0, p);
			if (msg) {
				str place = getExceptionPlace(msg);
				str nmsg = NULL;
				if (place){
					nmsg = createException(getExceptionType(msg), place, "%s", getExceptionMessageAndState(msg));
					GDKfree(place);
				}
				if (nmsg ) {
					freeException(msg);
					msg = nmsg;
				}
				goto wrapup;
			}
			if (cntxt->mode == FINISHCLIENT){
				mb->optimize = GDKusec() - clk;
				throw(MAL, "optimizeMALBlock", SQLSTATE(42000) "prematurely stopped client");
			}
			/* the MAL block may have changed */
			pc += mb->stop - oldstop - 1;
			oldstop = mb->stop;
		}
	}

wrapup:
	/* Keep the total time spent on optimizing the plan for inspection */
	if(actions > 0 && msg == MAL_SUCCEED){
		mb->optimize = GDKusec() - clk;
		snprintf(buf, 256, "%-20s actions=%2d time=" LLFMT " usec", "total", actions, mb->optimize);
		newComment(mb, buf);
	}
	if (cnt >= mb->stop)
		throw(MAL, "optimizer.MALoptimizer", SQLSTATE(42000) OPTIMIZER_CYCLE);
	return msg;
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
	// only a signature statement can be skipped
	if (c ->curprg->def->stop == 1)
		return MAL_SUCCEED;
	msg= optimizeMALBlock(c, c->curprg->def);
	if( msg == MAL_SUCCEED)
		msg = OPTmultiplexSimple(c, c->curprg->def);
	return msg;
}

/* Only used by opt_commonTerms! */
int hasSameSignature(MalBlkPtr mb, InstrPtr p, InstrPtr q){
	int i;

	if( q->retc != p->retc || q->argc != p->argc)
		return FALSE;
	for( i=0; i < p->argc; i++)
		if (getArgType(mb,p,i) != getArgType(mb,q,i))
			return FALSE;
	return TRUE;
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
static int
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
inline int
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
 * Instructions are unsafe if one of the arguments is also mentioned
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

inline int
isUpdateInstruction(InstrPtr p){
	if ( getModuleId(p) == sqlRef &&
	   ( getFunctionId(p) == appendRef ||
		getFunctionId(p) == updateRef ||
		getFunctionId(p) == deleteRef ||
		getFunctionId(p) == claimRef ||
		getFunctionId(p) == growRef ||
		getFunctionId(p) == clear_tableRef ||
		getFunctionId(p) == setVariableRef))
			return TRUE;
	if ( getModuleId(p) == batRef &&
	   ( getFunctionId(p) == appendRef ||
		getFunctionId(p) == replaceRef ||
		getFunctionId(p) == deleteRef))
			return TRUE;
	return FALSE;
}

int
hasSideEffects(MalBlkPtr mb, InstrPtr p, int strict)
{
	if( getFunctionId(p) == NULL) return FALSE;

/*
 * Void-returning operations have side-effects and
 * should be considered as such
 */
	if (p->retc == 0 || (p->retc == 1 && getArgType(mb,p,0) == TYPE_void))
		return TRUE;

/*
 * Any function marked as unsafe can not be moved around without
 * affecting its behavior on the program. For example, because they
 * check for volatile resource levels.
 */
	if ( isUnsafeFunction(p))
		return TRUE;

	/* update instructions have side effects, they can be marked as unsafe */
	if (isUpdateInstruction(p))
		return TRUE;

	if ( (getModuleId(p) == batRef || getModuleId(p)==sqlRef) &&
	     (getFunctionId(p) == setAccessRef ||
	 	  getFunctionId(p) == setWriteModeRef ))
		return TRUE;

	if (getModuleId(p) == malRef && getFunctionId(p) == multiplexRef)
		return FALSE;

	if( getModuleId(p) == ioRef ||
		getModuleId(p) == streamsRef ||
		getModuleId(p) == bstreamRef ||
		getModuleId(p) == mdbRef ||
		getModuleId(p) == malRef ||
		getModuleId(p) == remapRef ||
		getModuleId(p) == optimizerRef ||
		getModuleId(p) == lockRef ||
		getModuleId(p) == semaRef ||
		getModuleId(p) == alarmRef)
		return TRUE;

	if( getModuleId(p) == pyapi3Ref ||
		getModuleId(p) == pyapi3mapRef ||
		getModuleId(p) == rapiRef ||
		getModuleId(p) == capiRef)
		return TRUE;

	if (getModuleId(p) == sqlcatalogRef)
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
		if (getFunctionId(p) == importColumnRef) return FALSE;
		return TRUE;
	}
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

	if ( getModuleId(p) == sqlcatalogRef)
		return TRUE;
	if ( getModuleId(p) == oltpRef)
		return TRUE;
	if ( getModuleId(p) == wlrRef)
		return TRUE;
	if ( getModuleId(p) == wlcRef)
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
		return hasSideEffects(mb, p, strict);
	//  a manifold instruction can also have side effects.
	//  for this to check we need the function signature, not its function address.
	//  The easy way out now is to consider all manifold instructions as potentially having side effects.
	if ( getModuleId(p) == malRef && getFunctionId(p) == manifoldRef)
		return TRUE;
	if (MANIFOLDtypecheck(cntxt,mb,p,1) == NULL)
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
		if( hasSideEffects(mb,getInstrPtr(mb,i), TRUE))
			return FALSE;
	}
	return TRUE;
}

/*
 * Breaking up a MAL program into pieces for distributed processing requires
 * identification of (partial) blocking instructions. A conservative
 * definition can be used.
 */
inline int
isBlocking(InstrPtr p)
{
	if (blockStart(p) || blockExit(p) || blockCntrl(p))
		return TRUE;

	if ( getFunctionId(p) == sortRef )
		return TRUE;

	if( getModuleId(p) == aggrRef ||
		getModuleId(p) == groupRef ||
		getModuleId(p) == sqlcatalogRef )
			return TRUE;
	return FALSE;
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
	if ( getFunctionId(p) == differenceRef ||
		getFunctionId(p) == window_boundRef ||
		getFunctionId(p) == row_numberRef ||
		getFunctionId(p) == rankRef ||
		getFunctionId(p) == dense_rankRef ||
		getFunctionId(p) == percent_rankRef ||
		getFunctionId(p) == cume_distRef ||
		getFunctionId(p) == ntileRef ||
		getFunctionId(p) == first_valueRef ||
		getFunctionId(p) == last_valueRef ||
		getFunctionId(p) == nth_valueRef ||
		getFunctionId(p) == lagRef ||
		getFunctionId(p) == leadRef)
		return 1;
	return 0;
}

inline int isMapOp(InstrPtr p){
	if (isUnsafeFunction(p))
		return 0;
	return	getModuleId(p) &&
		((getModuleId(p) == malRef && getFunctionId(p) == multiplexRef) ||
		 (getModuleId(p) == malRef && getFunctionId(p) == manifoldRef) ||
		 (getModuleId(p) == batcalcRef) ||
		 (getModuleId(p) != batcalcRef && getModuleId(p) != batRef && strncmp(getModuleId(p), "bat", 3) == 0) ||
		 (getModuleId(p) == mkeyRef)) && !isOrderDepenent(p) &&
		 getModuleId(p) != batrapiRef &&
		 getModuleId(p) != batpyapi3Ref &&
		 getModuleId(p) != batcapiRef;
}

inline int isMap2Op(InstrPtr p){
	if (isUnsafeFunction(p))
		return 0;
	return	getModuleId(p) &&
		((getModuleId(p) == malRef && getFunctionId(p) == multiplexRef) ||
		 (getModuleId(p) == malRef && getFunctionId(p) == manifoldRef) ||
		 (getModuleId(p) == batcalcRef) ||
		 (getModuleId(p) != batcalcRef && getModuleId(p) != batRef && strncmp(getModuleId(p), "bat", 3) == 0) ||
		 (getModuleId(p) == mkeyRef)) && !isOrderDepenent(p) &&
		 getModuleId(p) != batrapiRef &&
		 getModuleId(p) != batpyapi3Ref &&
		 getModuleId(p) != batcapiRef;
}

inline int isLikeOp(InstrPtr p){
	return	(getModuleId(p) == batalgebraRef &&
		(getFunctionId(p) == likeRef ||
		 getFunctionId(p) == not_likeRef ||
		 getFunctionId(p) == ilikeRef ||
		 getFunctionId(p) == not_ilikeRef));
}

inline int
isTopn(InstrPtr p)
{
	return ((getModuleId(p) == algebraRef && getFunctionId(p) == firstnRef) ||
			isSlice(p));
}

inline int
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

inline int isOrderby(InstrPtr p){
	return getModuleId(p) == algebraRef &&
		getFunctionId(p) == sortRef;
}

inline int
isMatJoinOp(InstrPtr p)
{
	return (isSubJoin(p) || (getModuleId(p) == algebraRef &&
				(getFunctionId(p) == crossRef ||
				 getFunctionId(p) == joinRef ||
				 getFunctionId(p) == thetajoinRef ||
				 getFunctionId(p) == bandjoinRef ||
				 getFunctionId(p) == rangejoinRef)
		));
}

inline int
isMatLeftJoinOp(InstrPtr p)
{
	return (getModuleId(p) == algebraRef &&
		getFunctionId(p) == leftjoinRef);
}

inline int isDelta(InstrPtr p){
	return
			(getModuleId(p)== sqlRef && (
				getFunctionId(p)== deltaRef ||
				getFunctionId(p)== projectdeltaRef ||
				getFunctionId(p)== subdeltaRef
			)
		);
}

int isFragmentGroup2(InstrPtr p){
	if (getModuleId(p) == batRef && getFunctionId(p) == replaceRef)
			return TRUE;
	return
			(getModuleId(p)== algebraRef && (
				getFunctionId(p)== projectionRef
			)) ||
			(getModuleId(p)== batRef && (
				getFunctionId(p)== mergecandRef ||
				getFunctionId(p)== intersectcandRef ||
				getFunctionId(p)== diffcandRef
			)
		);
}

inline int isSelect(InstrPtr p)
{
	const char *func = getFunctionId(p);
	size_t l = func?strlen(func):0;

	return (l >= 6 && strcmp(func+l-6,"select") == 0);
}

inline int isSubJoin(InstrPtr p)
{
	const char *func = getFunctionId(p);
	size_t l = func?strlen(func):0;

	return (l >= 7 && strcmp(func+l-7,"join") == 0);
}

inline int isMultiplex(InstrPtr p)
{
	return (malRef && (getModuleId(p) == malRef || getModuleId(p) == batmalRef) &&
		getFunctionId(p) == multiplexRef);
}

int isFragmentGroup(InstrPtr p){
	return
			(getModuleId(p)== algebraRef && (
				getFunctionId(p)== projectRef ||
				getFunctionId(p)== selectNotNilRef
			))  ||
			isSelect(p) ||
			(getModuleId(p)== batRef && (
				getFunctionId(p)== mirrorRef
			));
}

