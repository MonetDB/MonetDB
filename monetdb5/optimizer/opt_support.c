/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

 /* (c) M. Kersten
 * Building Your Own Optimizer
 * Implementation of your own MAL-MAL optimizer can best be started
 * from refinement of one of the examples included in the code base.
 * Beware that only those used in the critical path of SQL execution
 * are thorouhly tested. The others are developed up to the point that
 * the concept and approach can be demonstrated.
 *
 * The general structure of most optimizers is to actively copy
 * a MAL block into a new program structure. At each step we
 * determine the action taken, e.g. replace the instruction or
 * inject instructions to achieve the desired goal.
 *
 * A tally on major events should be retained, because it gives
 * valuable insight in the effectiveness of your optimizer.
 * The effects of all optimizers is collected in a system catalog.
 *
 * Each optimizer ends with a strong defense line, optimizerCheck() 
 * * It performs a complete type and data flow analysis before returning.
 * Moreover, if you are in debug mode, it will  keep a copy of the
 * plan produced for inspection. Studying the differences between
 * optimizer steps provide valuable information to improve your code.
 *
 * The functionality of the optimizer should be clearly delineated.
 * The guiding policy is that it is always safe to not apply an
 * optimizer step.
 * This helps to keep the optimizers as independent as possible.
 *
 * It really helps if you start with a few tiny examples to test
 * your optimizer. They should be added to the Tests directory
 * and administered in Tests/All.
 *
 * Breaking up the optimizer into different components and
 * grouping them together in arbitrary sequences calls for
 * careful programming.
 *
 * One of the major hurdles is to test interference of the
 * optimizer. The test set is a good starting point, but does
 * not garantee that all cases have been covered.
 *
 * In principle, any subset of optimizers should work flawlessly.
 * With a few tens of optimizers this amounts to potential millions
 * of runs. Adherence to a partial order reduces the problem, but
 * still is likely to be too resource consumptive to test continously.
 *
 * The optimizers defined here are registered to the optimizer module.
 */
/*
 * @node Framework, Lifespan Analysis, Building Blocks, The MAL Optimizer
 * @subsection Optimizer framework
 * The large number of query transformers calls for a flexible scheme for
 * the deploy them. The approach taken is to make all optimizers visible
 * at the language level as a signature
 * @code{optimizer.F()} and @code{optimizer.F(mod,fcn)}.
 * The latter designates a target function to be inspected by
 * the optimizer @code{F()}.
 * Then (semantic) optimizer merely
 * inspects a MAL block for their occurrences and activitates it.
 *
 * The optimizer routines have access to the client context, the MAL block,
 * and the program counter where the optimizer call was found. Each optimizer
 * should remove itself from the MAL block.
 *
 * The optimizer repeatedly runs through the program until
 * no optimizer call is found.
 *
 * Note, all optimizer instructions are executed only once. This means that the
 * instruction can be removed from further consideration. However, in the case
 * that a designated function is selected for optimization (e.g.,
 * commonTerms(user,qry)) the pc is assumed 0. The first instruction always
 * denotes the signature and can not be removed.
 *
 * To safeguard against incomplete optimizer implementations it
 * is advisable to perform an optimizerCheck at the end.
 * It takes as arguments the number of optimizer actions taken
 * and the total cpu time spent.
 * The body performs a full flow and type check and re-initializes
 * the lifespan administration. In debugging mode also a copy
 * of the new block is retained for inspection.
 *
 * @node Lifespan Analysis, Flow Analysis, Framework, The MAL Optimizer
 * @subsection Lifespan analysis
 * Optimizers may be interested in the characteristic of the
 * barrier blocks for making a decision.
 * The variables have a lifespan in the code blocks, denoted by properties
 * beginLifespan,endLifespan. The beginLifespan denotes the intruction where
 * it receives its first value, the endLifespan the last instruction in which
 * it was used as operand or target.
 *
 * If, however, the last use lies within a BARRIER block, we can not be sure
 * about its end of life status, because a block redo may implictly
 * revive it. For these situations we associate the endLifespan with
 * the block exit.
 *
 * In many cases, we have to determine if the lifespan interferes with
 * a optimization decision being prepared.
 * The lifespan is calculated once at the beginning of the optimizer sequence.
 * It should either be maintained to reflect the most accurate situation while
 * optimizing the code base. In particular, it means that any move/remove/addition
 * of a MAL instruction calls for either a recalculation or further propagation.
 * Unclear what will be the best strategy. For the time being we just recalc.
 * If one of the optimizers fails, we should not attempt others.
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
	int debug;
} optcatalog[]= {
{"aliases",		0,	0,	0,	DEBUG_OPT_ALIASES},
{"coercions",	0,	0,	0,	DEBUG_OPT_COERCION},
{"commonTerms",	0,	0,	0,	DEBUG_OPT_COMMONTERMS},
{"constants",	0,	0,	0,	DEBUG_OPT_CONSTANTS},
{"costModel",	0,	0,	0,	DEBUG_OPT_COSTMODEL},
{"crack",		0,	0,	0,	DEBUG_OPT_CRACK},
{"datacyclotron",0,	0,	0,	DEBUG_OPT_DATACYCLOTRON},
{"dataflow",	0,	0,	0,	DEBUG_OPT_DATAFLOW},
{"deadcode",	0,	0,	0,	DEBUG_OPT_DEADCODE},
{"evaluate",	0,	0,	0,	DEBUG_OPT_EVALUATE},
{"factorize",	0,	0,	0,	DEBUG_OPT_FACTORIZE},
{"garbage",		0,	0,	0,	DEBUG_OPT_GARBAGE},
{"generator",	0,	0,	0,	DEBUG_OPT_GENERATOR},
{"history",		0,	0,	0,	DEBUG_OPT_HISTORY},
{"inline",		0,	0,	0,	DEBUG_OPT_INLINE},
{"projectionpath",	0,	0,	0,	DEBUG_OPT_PROJECTIONPATH},
{"json",		0,	0,	0,	DEBUG_OPT_JSON},
{"macro",		0,	0,	0,	DEBUG_OPT_MACRO},
{"matpack",		0,	0,	0,	DEBUG_OPT_MATPACK},
{"mergetable",	0,	0,	0,	DEBUG_OPT_MERGETABLE},
{"mitosis",		0,	0,	0,	DEBUG_OPT_MITOSIS},
{"multiplex",	0,	0,	0,	DEBUG_OPT_MULTIPLEX},
{"origin",		0,	0,	0,	DEBUG_OPT_ORIGIN},
{"peephole",	0,	0,	0,	DEBUG_OPT_PEEPHOLE},
{"recycler",	0,	0,	0,	DEBUG_OPT_RECYCLE},
{"reduce",		0,	0,	0,	DEBUG_OPT_REDUCE},
{"remap",		0,	0,	0,	DEBUG_OPT_REMAP},
{"remote",		0,	0,	0,	DEBUG_OPT_REMOTE},
{"reorder",		0,	0,	0,	DEBUG_OPT_REORDER},
{"replication",	0,	0,	0,	DEBUG_OPT_REPLICATION},
{"selcrack",	0,	0,	0,	DEBUG_OPT_SELCRACK},
{"sidcrack",	0,	0,	0,	DEBUG_OPT_SIDCRACK},
{"strengthreduction",	0,	0,	0,	DEBUG_OPT_STRENGTHREDUCTION},
{"pushselect",	0,	0,	0,	DEBUG_OPT_PUSHSELECT},
{ 0,	0,	0,	0,	0}
};

lng optDebug;

/*
 * Front-ends can set a collection of optimizers by name or their pipe alias.
 */
str
OPTsetDebugStr(void *ret, str *nme)
{
	int i;
	str name= *nme, t, s, env = 0;

	(void) ret;
	optDebug = 0;
	if ( name == 0 || *name == 0)
		return MAL_SUCCEED;
	name = GDKstrdup(name);

	if ( strstr(name,"_pipe") ){
		env = GDKgetenv(name);
		if ( env ) {
			GDKfree(name);
			name = GDKstrdup(env);
		}
	}
	for ( t = s = name; t && *t ; t = s){
		s = strchr(s,',');
		if ( s ) *s++ = 0;
		for ( i=0; optcatalog[i].name; i++)
		if ( strcmp(t,optcatalog[i].name) == 0){
			optDebug |= DEBUG_OPT(optcatalog[i].debug);
			break;
		}
	}
	GDKfree(name);
	return MAL_SUCCEED;
}

/*
 * All optimizers should pass the optimizerCheck for defense against
 * incomplete and malicious MAL code.
 */
str
optimizerCheck(Client cntxt, MalBlkPtr mb, str name, int actions, lng usec)
{
	char buf[256];
	if (cntxt->mode == FINISHCLIENT)
		throw(MAL, name, "prematurely stopped client");
	if( actions > 0){
		chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
		chkFlow(cntxt->fdout, mb);
		chkDeclarations(cntxt->fdout, mb);
	}
	/* keep all actions taken as a post block comments */
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

	/* assume the type and flow have been checked already */
	/* SQL functions intended to be inlined should not be optimized */
	if ( mb->inlineProp)
        	return 0;


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
		getModuleId(p) == recycleRef ||
		getModuleId(p) == alarmRef)
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
	if ( getModuleId(p) == recycleRef)
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
		 getModuleId(p) != batrapiRef;
}

int isLikeOp(InstrPtr p){
	return	(getModuleId(p) == batalgebraRef &&
		(getFunctionId(p) == likeRef || 
		 getFunctionId(p) == not_likeRef || 
		 getFunctionId(p) == ilikeRef ||
		 getFunctionId(p) == not_ilikeRef));
}

int isTopn(InstrPtr p){
	return ((getModuleId(p) == algebraRef && getFunctionId(p) == firstnRef) ||
			isSlice(p));
}

int isSlice(InstrPtr p){
	return (getModuleId(p) == algebraRef &&
		getFunctionId(p) == subsliceRef); 
}

int isSample(InstrPtr p){
	return (getModuleId(p) == sampleRef &&
		getFunctionId(p) == subuniformRef);
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

