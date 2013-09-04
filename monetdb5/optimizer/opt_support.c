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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

 /*
 * @- Building Your Own Optimizer
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
 * Each optimizer ends with a strong defense line, @code{optimizerCheck()}
 * It performs a complete type and data flow analysis before returning.
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
 * @-
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

/*
 * @-
 * Optimizer catalog with runtime statistics;
 */
struct OPTcatalog {
	char *name;
	int enabled;
	int calls;
	int actions;
	int debug;
} optcatalog[]= {
{"accumulators",0,	0,	0,	DEBUG_OPT_ACCUMULATORS},
{"groups",		0,	0,	0,	DEBUG_OPT_GROUPS},
{"aliases",		0,	0,	0,	DEBUG_OPT_ALIASES},
{"cluster",		0,	0,	0,	DEBUG_OPT_CLUSTER},
{"coercions",	0,	0,	0,	DEBUG_OPT_COERCION},
{"commonTerms",	0,	0,	0,	DEBUG_OPT_COMMONTERMS},
{"compress",	0,	0,	0,	DEBUG_OPT_COMPRESS},
{"constants",	0,	0,	0,	DEBUG_OPT_CONSTANTS},
{"costModel",	0,	0,	0,	DEBUG_OPT_COSTMODEL},
{"crack",		0,	0,	0,	DEBUG_OPT_CRACK},
{"datacell",	0,	0,	0,	DEBUG_OPT_DATACELL},
{"datacyclotron",0,	0,	0,	DEBUG_OPT_DATACYCLOTRON},
{"dataflow",	0,	0,	0,	DEBUG_OPT_DATAFLOW},
{"deadcode",	0,	0,	0,	DEBUG_OPT_DEADCODE},
{"dictionary",	0,	0,	0,	DEBUG_OPT_DICTIONARY},
{"emptySet",	0,	0,	0,	DEBUG_OPT_EMPTYSET},
{"evaluate",	0,	0,	0,	DEBUG_OPT_EVALUATE},
{"factorize",	0,	0,	0,	DEBUG_OPT_FACTORIZE},
{"garbage",		0,	0,	0,	DEBUG_OPT_GARBAGE},
{"history",		0,	0,	0,	DEBUG_OPT_HISTORY},
{"inline",		0,	0,	0,	DEBUG_OPT_INLINE},
{"joinPath",	0,	0,	0,	DEBUG_OPT_JOINPATH},
{"json",		0,	0,	0,	DEBUG_OPT_JSON},
{"macro",		0,	0,	0,	DEBUG_OPT_MACRO},
{"mapreduce",	0,	0,	0,	DEBUG_OPT_MAPREDUCE},
{"matpack",		0,	0,	0,	DEBUG_OPT_MATPACK},
{"mergetable",	0,	0,	0,	DEBUG_OPT_MERGETABLE},
{"mitosis",		0,	0,	0,	DEBUG_OPT_MITOSIS},
{"multiplex",	0,	0,	0,	DEBUG_OPT_MULTIPLEX},
{"octopus",		0,	0,	0,	DEBUG_OPT_OCTOPUS},
{"origin",		0,	0,	0,	DEBUG_OPT_ORIGIN},
{"peephole",	0,	0,	0,	DEBUG_OPT_PEEPHOLE},
{"prejoin",		0,	0,	0,	DEBUG_OPT_PREJOIN},
{"pushranges",	0,	0,	0,	DEBUG_OPT_PUSHRANGES},
{"recycler",	0,	0,	0,	DEBUG_OPT_RECYCLE},
{"reduce",		0,	0,	0,	DEBUG_OPT_REDUCE},
{"remap",		0,	0,	0,	DEBUG_OPT_REMAP},
{"remote",		0,	0,	0,	DEBUG_OPT_REMOTE},
{"reorder",		0,	0,	0,	DEBUG_OPT_REORDER},
{"replication",	0,	0,	0,	DEBUG_OPT_REPLICATION},
{"selcrack",	0,	0,	0,	DEBUG_OPT_SELCRACK},
{"sidcrack",	0,	0,	0,	DEBUG_OPT_SIDCRACK},
{"strengthreduction",	0,	0,	0,	DEBUG_OPT_STRENGTHREDUCTION},
{"centipede",	0,	0,	0,	DEBUG_OPT_CENTIPEDE},
{"pushselect",	0,	0,	0,	DEBUG_OPT_PUSHSELECT},
{ 0,	0,	0,	0,	0}
};

lng optDebug;

/*
 * @-
 * Front-ends can set a collection of optimizers by name or their pipe alias.
 */
str
OPTsetDebugStr(int *ret, str *nme)
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

str
optimizerCheck(Client cntxt, MalBlkPtr mb, str name, int actions, lng usec, int flag)
{
	if( actions > 0){
		if( flag & OPT_CHECK_TYPES) chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
		if( flag & OPT_CHECK_FLOW) chkFlow(cntxt->fdout, mb);
		if( flag & OPT_CHECK_DECL) chkDeclarations(cntxt->fdout, mb);
	}
	if( cntxt->debugOptimizer){
		/* keep the actions take as post block comments */
		char buf[BUFSIZ];
		sprintf(buf,"%-20s actions=%2d time=" LLFMT " usec",name,actions,usec);
		newComment(mb,buf);
		if (mb->errors)
			throw(MAL, name, PROGRAM_GENERAL);
	}
	/* code to collect all last versions to study code coverage  in SQL
	{stream *fd;
	char nme[25];
	snprintf(nme,25,"/tmp/mal_%d",getpid());
	fd= open_wastream(nme);
	if( fd == NULL)
		printf("Error in %s\n",nme);
	printFunction(fd,mb,0,LIST_MAL_ALL);
	mnstr_close(fd);
	}
	*/
	return MAL_SUCCEED;
}
/*
 * @-
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

	optimizerInit();
	/* assume the type and flow have been checked already */
	/* SQL functions intended to be inlined should not be optimized */
	if ( varGetProp( mb, getArg(getInstrPtr(mb,0),0), inlineProp ) != NULL &&
		 varGetProp( mb, getArg(getInstrPtr(mb,0),0), sqlfunctionProp ) != NULL
	)
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
				/* However, we don;t have a stack now */
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
 * @-
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

	if ( varGetProp(c->curprg->def,0,inlineProp))
		return MAL_SUCCEED;
	msg= optimizeMALBlock(c, c->curprg->def);
	if( msg == MAL_SUCCEED)
		OPTmultiplexSimple(c);
	return msg;
}

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
					cmp = BATatoms[w->value.vtype].atomCmp;
					if ( w->value.vtype == u->value.vtype &&
						(*cmp)(VALptr(&w->value), VALptr(&u->value)) == 0)
						continue;
			}
			return FALSE;
		}
	return TRUE;
}
/*
 * @-
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
 * @-
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
 * @-
 * See is all arguments mentioned in the instruction at point pc
 * are still visible at instruction qc and have not been updated
 * in the mean time.
 * Take into account that variables may be declared inside
 * a block. This can be calculated using the BARRIER/CATCH
 * and EXIT pairs.
 */
static int
countBlocks(MalBlkPtr mb, int start, int stop){
	int i,cnt =0;
	InstrPtr p;
	for(i= start; i< stop; i++){
		p= getInstrPtr(mb,i);
		if ( p->barrier == BARRIERsymbol || p->token== CATCHsymbol)
			cnt++;
		if ( p->barrier == EXITsymbol )
			cnt--;
	}
	return cnt;
}
#if 0
int
allArgumentsVisible(MalBlkPtr mb, Lifespan span, int pc,int qc){
	int i;
	InstrPtr p;

	if( countBlocks(mb,pc,qc) )
		return FALSE;
	p= getInstrPtr(mb,pc);
	for(i=p->retc; i< p->argc; i++){
		if( getLastUpdate(span,getArg(p,i)) >  getBeginLifespan(span,getArg(p,i)) &&
			qc > getLastUpdate(span,getArg(p,i)) )
			return FALSE;
	}
	return TRUE;
}
#endif
int
allTargetsVisible(MalBlkPtr mb, Lifespan span, int pc,int qc){
	int i;
	InstrPtr p;

	if( countBlocks(mb,pc,qc) )
		return FALSE;
	p= getInstrPtr(mb,pc);
	for(i=0; i < p->retc; i++){
		if( getLastUpdate(span,getArg(p,i))> getBeginLifespan(span,getArg(p,i))  &&
			qc > getLastUpdate(span,getArg(p,i)) )
			return FALSE;
	}
	return TRUE;
}
/*
 * @-
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
	return (varGetProp( q->blk, getArg(p,0), unsafeProp ) != NULL);
	/* check also arguments for 'unsafe' property */
}

/*
 * @-
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
 * @-
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
 * @-
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

/*
 * @-
 * @node Flow Analysis, Optimizer Toolkit, Lifespan Analysis, The MAL Optimizer
 * @subsection Flow analysis
 * In many optimization rules, the data flow dependency between statements is
 * of crucial importance. The MAL language encodes a multi-source, multi-sink
 * dataflow network. Optimizers typically extract part of the workflow and use
 * the language properties to enumerate semantic equivalent solutions, which
 * under a given cost model turns out to result in better performance.
 *
 * The flow graph plays a crucial role in many optimization steps.
 * It is unclear as yet what primitives and what storage structure is
 * most adequate. For the time being we introduce the operations needed and
 * evaluate them directly against the program
 *
 * For each variable we should determine its scope of stability.
 * End-points in the flow graph are illustrative as dead-code,
 * that do not produce persistent data. It can be removed when
 * you know there are no side-effect.
 *
 * Side-effect free evaluation is a property that should be known upfront.
 * For the time being, we assume it for all operations known to the system.
 * The property "unsafe" is reserved to identify cases where this does not hold.
 * Typically, a bun-insert operation is unsafe, as it changes one of the parameters.
 * @
 * Summarization of the data flow dependencies can be modelled as a dependency graph.
 * It can be made explicit or kept implicit using the operators needed.
 * We start with the latter. The primary steps to deal with is dead code removal.
 * @- Basic Algebraic Blocks
 * Many code snippets produced by e.g. the SQL compiler is just
 * a linear representation of an algebra tree/graph. Its detection
 * makes a number of optimization decisions more easy, because
 * the operations are known to be side-effect free within the tree/graph.
 * This can be used to re-order the plan without concern on impact of the outcome.
 * It suffice to respect the flow graph.
 * [unclear as what we need]
 * @-
 * @node Optimizer Toolkit, Access Mode, Flow Analysis , The MAL Optimizer
 * @+ Optimizer Toolkit
 * In this section, we introduce the collection of MAL optimizers
 * included in the code base. The tool kit is incrementally built, triggered
 * by experimentation and curiousity. Several optimizers require
 * further development to cope with the many features making up the MonetDB system.
 * Such limitations on the implementation are indicated where appropriate.
 *
 * Experience shows that construction and debugging of a front-end specific optimizer
 * is simplified when you retain information on the origin of the MAL code
 * produced as long as possible. For example,
 * the snippet @code{ sql.insert(col, 12@@0, "hello")} can be the target
 * of simple SQL rewrites using the module name as the discriminator.
 *
 * Pipeline validation. The pipelines used to optimize MAL programs contain
 * dependencies. For example, it does not make much sense to call the deadcode
 * optimizer too early in the pipeline, although it is not an error.
 * Moreover, some optimizers are merely examples of the direction to take,
 * others are critical for proper functioning for e.g. SQL.
 *
 * @menu
 * * Access Mode::
 * * Accumulators::
 * * Alias Removal::
 * * Code Factorization::
 * * Coercions::
 * * Common Terms::
 * * Constant Expressions::
 * * Cost Models::
 * * Data Flow::
 * * Dead Code Removal::
 * * Empty Set Removal::
 * * Garbage Collector::
 * * Heuristic Rules::
 * * Inline Functions::
 * * Join Paths::
 * * Macro Processing::
 * * Memo Execution::
 * * Merge Tables ::
 * * Multiplex Compiler::
 * * Partitioned Tables::
 * * Peephole Optimization::
 * * Query Plans::
 * * Range Propagation::
 * * Recycler::
 * * Remote::
 * * Remote Queries::
 * * Singleton Sets ::
 * * Stack Reduction::
 * * Strength Reduction::
 * @end menu
 * @-
 * The dead code remover should not be used for testing,
 * because it will trim most programs to an empty list.
 * The side effect tests should become part of the signature
 * definitions.
 *
 * A side effect is either an action to leave data around
 * in a variable/resource outside the MALblock.
 * A variant encoded here as well is that the linear flow
 * of control can be broken.
 */

int
isProcedure(MalBlkPtr mb, InstrPtr p)
{
	if (p->retc == 0 || (p->retc == 1 && getArgType(mb,p,0) == TYPE_void))
		return TRUE;
/*	if (p->retc == 1 && (varGetProp( q->blk, getArg(p,0), unsafeProp ) != NULL))
		return TRUE; */
	return FALSE;
}

int
isUpdateInstruction(InstrPtr p){
	if ( (getModuleId(p) == batRef || getModuleId(p)==sqlRef) &&
	   (getFunctionId(p) == insertRef ||
		getFunctionId(p) == inplaceRef ||
		getFunctionId(p) == appendRef ||
		getFunctionId(p) == updateRef ||
		getFunctionId(p) == replaceRef ||
		getFunctionId(p) == deleteRef ))
			return TRUE;
	return FALSE;
}
int
hasSideEffects(InstrPtr p, int strict)
{
	if( getFunctionId(p) == NULL) return FALSE;

/*
	if ( getModuleId(p) == algebraRef &&
		 getFunctionId(p) == reuseRef)
			return TRUE;
*/
	if ( (getModuleId(p) == batRef || getModuleId(p)==sqlRef) &&
	     (getFunctionId(p) == setAccessRef ||
	 	  getFunctionId(p) == setWriteModeRef ||
		  getFunctionId(p) == clear_tableRef))
		return TRUE;

	if (getFunctionId(p) == depositRef)
		return TRUE;

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

	if ( getModuleId(p) == octopusRef){
		if (getFunctionId(p) == bindRef) return FALSE;
		if (getFunctionId(p) == bindidxRef) return FALSE;
		if (getFunctionId(p) == binddbatRef) return FALSE;
		return TRUE;
	}
	if ( getModuleId(p) == remoteRef)
		return TRUE;
	if ( getModuleId(p) == recycleRef)
		return TRUE;
	return FALSE;
}
/*
 * @-
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
 * @-
 * Breaking up a MAL program into pieces for distributed requires
 * identification of (partial) blocking instructions. A conservative
 * definition can be used.
 */
int
isBlocking(InstrPtr p)
{
	if (blockStart(p) || blockExit(p) || blockCntrl(p))
		return TRUE;

	if ( getFunctionId(p) == sortTailRef ||
		 getFunctionId(p) == sortHRef ||
		 getFunctionId(p) == sortHTRef ||
		 getFunctionId(p) == sortTHRef )
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
int isMapOp(InstrPtr p){
	return	getModuleId(p) &&
		((getModuleId(p) == malRef && getFunctionId(p) == multiplexRef) ||
		(getModuleId(p)== batcalcRef && getFunctionId(p) != mark_grpRef && getFunctionId(p) != rank_grpRef) ||
		(getModuleId(p)== batmtimeRef) ||
		(getModuleId(p)== batstrRef) ||
		(getModuleId(p)== batmmathRef) ||
		(getModuleId(p)== batxmlRef) ||
		(strcmp(getModuleId(p),"batsql") == 0) ||
		(getModuleId(p)== mkeyRef));
}

int isLikeOp(InstrPtr p){
	return	(getModuleId(p) == batstrRef &&
		(getFunctionId(p) == likeRef || 
		 getFunctionId(p) == not_likeRef || 
		 getFunctionId(p) == ilikeRef ||
		 getFunctionId(p) == not_ilikeRef));
}

int isTopn(InstrPtr p){
	return ((getModuleId(p) == pqueueRef &&
		(getFunctionId(p) == topn_minRef ||
		 getFunctionId(p) == topn_maxRef ||
		 getFunctionId(p) == utopn_minRef ||
		 getFunctionId(p) == utopn_maxRef)) || isSlice(p));
}

int isSlice(InstrPtr p){
	return (getModuleId(p) == algebraRef &&
		getFunctionId(p) == subsliceRef);
}

int isOrderby(InstrPtr p){
	return getModuleId(p) == algebraRef &&
		(getFunctionId(p) == sortTailRef ||
		 getFunctionId(p) == sortReverseTailRef);
}

int isDiffOp(InstrPtr p){
	return (getModuleId(p) == algebraRef &&
	    	(getFunctionId(p) == semijoinRef ||
 	     	 getFunctionId(p) == kdifferenceRef));
}

int isMatJoinOp(InstrPtr p){
	return (getModuleId(p) == algebraRef &&
                (getFunctionId(p) == joinRef ||
                 getFunctionId(p) == antijoinRef || /* is not mat save */
                 getFunctionId(p) == thetajoinRef ||
                 getFunctionId(p) == bandjoinRef)
		);
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
				getFunctionId(p)== leftfetchjoinRef
			)) ||
			(getModuleId(p)== batRef && (
				getFunctionId(p)== mergecandRef || 
				getFunctionId(p)== intersectcandRef 
			) 
		);
}

int isSubSelect(InstrPtr p)
{
	return (getModuleId(p)== algebraRef && (
			getFunctionId(p)== subselectRef ||
			getFunctionId(p)== thetasubselectRef ||
			getFunctionId(p)== likesubselectRef ||
			getFunctionId(p)== ilikesubselectRef));
}

int isFragmentGroup(InstrPtr p){
	return
			(getModuleId(p)== pcreRef && (
			getFunctionId(p)== likeselectRef ||
			getFunctionId(p)== likeuselectRef  ||
			getFunctionId(p)== ilikeselectRef  ||
			getFunctionId(p)== ilikeuselectRef 
			))  ||
			(getModuleId(p)== algebraRef && (
				getFunctionId(p)== projectRef ||
				getFunctionId(p)== selectRef ||
				getFunctionId(p)== selectNotNilRef ||
				getFunctionId(p)== uselectRef ||
				getFunctionId(p)== antiuselectRef ||
				getFunctionId(p)== thetauselectRef 
			))  ||
			isSubSelect(p) ||
			(getModuleId(p)== batRef && (
				getFunctionId(p)== mirrorRef 
			)
		);
}

/*
 * Some optimizers are interdependent (e.g. mitosis and octopus), which
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
wrd
getVarRows(MalBlkPtr mb, int v)
{
	VarPtr p = varGetProp(mb, v, rowsProp);

	if (!p)
		return -1;
	if (p->value.vtype == TYPE_wrd
#if SIZEOF_BUN <= SIZEOF_WRD
		    && p->value.val.wval <= (wrd) BUN_MAX
#endif
		)
		return p->value.val.wval;
	if (p->value.vtype == TYPE_lng
#if SIZEOF_BUN <= SIZEOF_LNG
		    && p->value.val.lval <= (lng) BUN_MAX
#endif
		)
		return (wrd)p->value.val.lval;
	if (p->value.vtype == TYPE_int
#if SIZEOF_BUN <= SIZEOF_INT
		    && p->value.val.ival <= (int) BUN_MAX
#endif
		)
		return p->value.val.ival;
	if (p->value.vtype == TYPE_sht)
		return p->value.val.shval;
	if (p->value.vtype == TYPE_bte)
		return p->value.val.btval;
	return -1;
}

