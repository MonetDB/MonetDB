/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * The statemens are all checked for being eligible for dataflow.
 */
#include "monetdb_config.h"
#include "opt_dataflow.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "manifold.h"

/*
 * Dataflow processing incurs overhead and is only
 * relevant if multiple tasks kan be handled at the same time.
 * Also simple expressions dont have to be executed in parallel.
 *
 * The garbagesink contains variables whose endoflife is within
 * a dataflow block and who are used concurrently.
 * They are garbage collected at the end of the parallel block.
 *
 * The dataflow analysis centers around the read/write use patterns of
 * the variables and the occurrence of side-effect bearing functions.
 * Any such function should break the dataflow block as it may rely
 * on the sequential order in the plan.
 *
 * The following state properties can be distinguished for all variables:
 * VARWRITE  - variable assigned a value in the dataflow block
 * VARREAD   - variable is used in an argument
 * VAR2READ  - variable is read in concurrent mode
 * VARBLOCK  - variable next use terminate the // block, set after encountering an update
 *
 * Only some combinations are allowed.
 */

#define VARFREE  0
#define VARWRITE 1
#define VARREAD  2
#define VARBLOCK 4
#define VAR2READ 8

typedef char *States;

#define setState(S,P,K,F)  ( assert(getArg(P,K) < vlimit), (S)[getArg(P,K)] |= F)
#define getState(S,P,K)  ((S)[getArg(P,K)])

typedef enum {
	no_region,
	singleton_region, // only ever a single statement
	dataflow_region,  // statements without side effects, in parallel
	existing_region,  // existing barrier..exit region, copied as-is
	sql_region,       // region of nonconflicting sql.append/sql.updates only
} region_type;

typedef struct {
	region_type type;
	union {
		struct {
			int level;  // level of nesting
		} existing_region;
	} st;
} region_state;

static int
simpleFlow(InstrPtr *old, int start, int last, region_state *state)
{
	int i, j, k, simple = TRUE;
	InstrPtr p = NULL, q;

	/* ignore trivial blocks */
	if ( last - start == 1)
		return TRUE;
	if ( state->type == existing_region )
		return TRUE;
	/* skip sequence of simple arithmetic first */
	for( ; simple && start < last; start++)
	if ( old[start] ) {
		p= old[start];
		simple = getModuleId(p) == calcRef || getModuleId(p) == mtimeRef || getModuleId(p) == strRef || getModuleId(p)== mmathRef;
	}
	for( i = start; i < last; i++)
	if ( old[i]) {
		q= old[i];
		simple = getModuleId(q) == calcRef || getModuleId(q) == mtimeRef || getModuleId(q) == strRef || getModuleId(q)== mmathRef;
		if( !simple)  {
			/* if not arithmetic than we should consume the previous result directly */
			for( j= q->retc; j < q->argc; j++)
				for( k =0; k < p->retc; k++)
					if( getArg(p,k) == getArg(q,j))
						simple= TRUE;
			if( !simple)
				return 0;
		}
		p = q;
	}
	return simple;
}

/* Updates are permitted if it is a unique update on
 * a BAT created in the context of this block
 * As far as we know, no SQL nor MAL test re-uses the
 * target BAT to insert again and subsequently calls dataflow.
 * In MAL scripts, they still can occur.
*/

/* a limited set of MAL instructions may appear in the dataflow block*/
static int
dataflowBreakpoint(Client cntxt, MalBlkPtr mb, InstrPtr p, States states)
{
	int j;

	if (p->token == ENDsymbol || p->barrier || isUnsafeFunction(p) ||
		(isMultiplex(p) && MANIFOLDtypecheck(cntxt,mb,p,0) == NULL) ){
			return TRUE;
		}

	/* flow blocks should be closed when we reach a point
	   where a variable is assigned  more then once or already
	   being read.
	*/
	for(j=0; j<p->retc; j++)
		if ( getState(states,p,j) & (VARWRITE | VARREAD | VARBLOCK)){
			return 1;
		}

	/* update instructions can be updated if the target variable
	 * has not been read in the block so far */
	if ( isUpdateInstruction(p) ){
		/* the SQL update functions change BATs that are not
		 * explicitly mentioned as arguments (and certainly not as the
		 * first argument), but that can still be available to the MAL
		 * program (see bugs.monetdb.org/6641) */
		if (getModuleId(p) == sqlRef)
			return 1;
		return getState(states,p,p->retc) & (VARREAD | VARBLOCK);
	}

	for(j=p->retc; j < p->argc; j++){
		if ( getState(states,p,j) & VARBLOCK){
			return 1;
		}
	}
	return hasSideEffects(mb,p,FALSE);
}

/* Collect the BATs that are used concurrently to ensure that
 * there is a single point where they can be released
 */
static int
dflowGarbagesink(Client cntxt, MalBlkPtr mb, int var, InstrPtr *sink, int top)
{
	InstrPtr r;
	int i;
	for( i =0; i<top; i++)
		if( getArg(sink[i],1) == var)
			return top;
	(void) cntxt;

	r = newInstruction(NULL,languageRef, passRef);
	getArg(r,0) = newTmpVariable(mb,TYPE_void);
	r= addArgument(mb,r, var);
	sink[top++] = r;
	return top;
}


static str
get_str_arg(MalBlkPtr mb, InstrPtr p, int argno)
{
	int var = getArg(p, argno);
	return getVarConstant(mb, var).val.sval;
}

static str
get_sql_sname(MalBlkPtr mb, InstrPtr p)
{
	return get_str_arg(mb, p, 2);
}

static str
get_sql_tname(MalBlkPtr mb, InstrPtr p)
{
	return get_str_arg(mb, p, 3);
}

static str
get_sql_cname(MalBlkPtr mb, InstrPtr p)
{
	return get_str_arg(mb, p, 4);
}


static bool
isSqlAppendUpdate(MalBlkPtr mb, InstrPtr p)
{
	if (p->modname != sqlRef)
		return false;
	if (p->fcnname != appendRef) //  && p->fcnname != updateRef)
		return false;

	// pattern("sql", "append", mvc_append_wrap, false, "...", args(1,7, arg("",int),
	//              arg("mvc",int
	//              arg("sname",str
	//              arg("tname",str
	//              arg("cname",str
	//              arg("offset",lng
	//              argany("ins",0))),

 	// pattern("sql", "update", mvc_update_wrap, false, "...", args(1,7, arg("",int
	//              arg("mvc",int),
	//              arg("sname",str
	//              arg("tname",str
	//              arg("cname",str
	//              argany("rids",0
	//              argany("upd",0))

	if (p->argc != 7)
		return false;

	int mvc_var = getArg(p, 1);
	if (getVarType(mb, mvc_var) != TYPE_int)
		return false;

	int sname_var = getArg(p, 2);
	if (getVarType(mb, sname_var) != TYPE_str || !isVarConstant(mb, sname_var))
		return false;

	int tname_var = getArg(p, 3);
	if (getVarType(mb, tname_var) != TYPE_str || !isVarConstant(mb, tname_var))
		return false;

	int cname_var = getArg(p, 4);
	if (getVarType(mb, cname_var) != TYPE_str || !isVarConstant(mb, cname_var))
		return false;

	return true;
}

static bool
sqlBreakpoint(MalBlkPtr mb, InstrPtr *first, InstrPtr *p)
{
	InstrPtr instr = *p;
	if (!isSqlAppendUpdate(mb, instr))
		return true;

	str my_sname = get_sql_sname(mb, instr);
	str my_tname = get_sql_tname(mb, instr);
	str my_cname = get_sql_cname(mb, instr);
	for (InstrPtr *q = first; q < p; q++) {
		str sname = get_sql_sname(mb, *q);
		if (strcmp(my_sname, sname) != 0) {
			// different sname, no conflict
			continue;
		}
		str tname = get_sql_tname(mb, *q);
		if (strcmp(my_tname, tname) != 0) {
			// different tname, no conflict
			continue;
		}
		str cname = get_sql_cname(mb, *q);
		if (strcmp(my_cname, cname) != 0) {
			// different cname, no conflict
			continue;
		}
		// Found a statement in the region that works on the same column so this is a breakpoint
		return true;
	}

	// None of the statements in the region works on this column so no breakpoint necessary
	return false;
}

static bool
checkBreakpoint(Client cntxt, MalBlkPtr mb, InstrPtr *first, InstrPtr *p, States states, region_state *state)
{
	InstrPtr instr = *p;
	switch (state->type) {
		case singleton_region:
			return true;
		case dataflow_region:
			return dataflowBreakpoint(cntxt, mb, instr, states);
		case existing_region:
			if (state->st.existing_region.level == 0) {
				// previous statement ended the region
				return true;
			}
			if (blockStart(instr)) {
				state->st.existing_region.level += 1;
			} else if (blockExit(instr)) {
				state->st.existing_region.level -= 1;
			}
			return false;
		case sql_region:
			return sqlBreakpoint(mb, first, p);
		default:
			// serious corruption has occurred.
			assert(0 && "corrupted region_type");
			abort();
	}
	assert(0 && "unreachable");
	return true;
}

static void
decideRegionType(Client cntxt, MalBlkPtr mb, InstrPtr p, region_state *state)
{
	(void) cntxt;

	state->type = no_region;
	if (blockStart(p)) {
		state->type = existing_region;
		state->st.existing_region.level = 1;
	} else if (p->token == ENDsymbol) {
		state->type = existing_region;
	} else if (isSqlAppendUpdate(mb,p)) {
		state->type = sql_region;
	} else if (p->barrier) {
		state->type = singleton_region;
	} else if (isUnsafeFunction(p)) {
		state->type = singleton_region;
	} else if (hasSideEffects(mb, p, false)) {
		state->type = singleton_region;
	} else if (isMultiplex(p)) {
		state->type = singleton_region;
	} else {
		state->type = dataflow_region;
	}
	assert(state->type != no_region);
}


/* dataflow blocks are transparent, because they are always
   executed, either sequentially or in parallel */

static str
OPTdataflowImplementation_wrapped(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	volatile int interesting_banana = 0;
	int i,j,k, start, slimit, breakpoint, actions=0, simple = TRUE;
	int flowblock= 0;
	InstrPtr *sink = NULL, *old = NULL, q;
	int limit, vlimit, top = 0;
	States states;
	char  buf[256];
	region_state state = { singleton_region };
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;


	do {
		if (mb->stop < 2)
			break;
		InstrPtr p = mb->stmt[1];

		if (strcmp("joeri", mb->stmt[0]->fcnname) == 0) {
			interesting_banana = 1;
		}

		if (p->argc < 2)
			break;

		if (p->modname != querylogRef || p->fcnname != defineRef)
			break;

		const char *txt = getVarConstant(mb, getArg(p,1)).val.sval;
		if (strstr(txt, "value / 2") != 0)
			interesting_banana = 1;
	} while (0);

	// if (!interesting_banana)
	// 	return MAL_SUCCEED;



	/* don't use dataflow on single processor systems */
	if (GDKnr_threads <= 1)
		return MAL_SUCCEED;

	if ( optimizerIsApplied(mb,"dataflow"))
		return MAL_SUCCEED;
	(void) stk;
	/* inlined functions will get their dataflow control later */
	if ( mb->inlineProp)
		return MAL_SUCCEED;

	vlimit = mb->vsize;
	states = (States) GDKzalloc(vlimit * sizeof(char));
	sink = (InstrPtr *) GDKzalloc(mb->stop * sizeof(InstrPtr));
	if (states == NULL || sink == NULL){
		msg= createException(MAL,"optimizer.dataflow", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup;
	}

	setVariableScope(mb);

	limit= mb->stop;
	slimit= mb->ssize;
	old = mb->stmt;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		msg= createException(MAL,"optimizer.dataflow", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		actions = -1;
		goto wrapup;
	}

	/* inject new dataflow barriers using a single pass through the program */
	start = 0;
	state.type = singleton_region;
	for (i = 1; i<limit; i++) {
		p = old[i];
		assert(p);
		breakpoint = checkBreakpoint(cntxt, mb, &old[start], &old[i], states, &state);
		if ( breakpoint ){
			/* close previous flow block */
			simple = simpleFlow(old,start,i, &state);

			if ( !simple){
				flowblock = newTmpVariable(mb,TYPE_bit);
				q= newFcnCall(mb,languageRef,dataflowRef);
				q->barrier= BARRIERsymbol;
				getArg(q,0)= flowblock;
				actions++;
			}
			// copyblock the collected statements
			for( j=start ; j<i; j++) {
				q= old[j];
				pushInstruction(mb,q);
				// collect BAT variables garbage collected within the block
				if( !simple)
					for( k=q->retc; k<q->argc; k++){
						if (getState(states,q,k) & VAR2READ &&  getEndScope(mb,getArg(q,k)) == j && isaBatType(getVarType(mb,getArg(q,k))) )
								top = dflowGarbagesink(cntxt, mb, getArg(q,k), sink, top);
					}
			}
			/* exit parallel block */
			if ( ! simple){
				// force the pending final garbage statements
				for( j=0; j<top; j++)
					pushInstruction(mb,sink[j]);
				q= newAssignment(mb);
				q->barrier= EXITsymbol;
				getArg(q,0) = flowblock;
			}
			if (p->token == ENDsymbol){
				for(; i < limit; i++)
					if( old[i])
						pushInstruction(mb,old[i]);
				break;
			}

			// Start a new region
			memset((char*) states, 0, vlimit * sizeof(char));
			top = 0;
			start = i;
			decideRegionType(cntxt, mb, p, &state);
		}

		// remember you assigned/read variables
		for ( k = 0; k < p->retc; k++)
			setState(states, p, k, VARWRITE);
		if( isUpdateInstruction(p) && (getState(states,p,1) == 0 || getState(states,p,1) & VARWRITE))
			setState(states, p,1, VARBLOCK);
		for ( k = p->retc; k< p->argc; k++)
		if( !isVarConstant(mb,getArg(p,k)) ){
			if( getState(states, p, k) & VARREAD)
				setState(states, p, k, VAR2READ);
			else
			if( getState(states, p, k) & VARWRITE)
				setState(states, p ,k, VARREAD);
		}
	}

	/* take the remainder as is */
	for (; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);
    /* Defense line against incorrect plans */
    if( actions > 0){
        msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if (!msg)
        	msg = chkFlow(mb);
	if (!msg)
        	msg = chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","dataflow",actions,usec);
    newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);

wrapup:
	if(states) GDKfree(states);
	if(sink)   GDKfree(sink);
	if(old)    GDKfree(old);
	(void) interesting_banana;
	return msg;
}



static stream *
open_trace_stream(void)
{
	char path[4096] = {0};
	stream *s = NULL;

	char *tst_trace_dir = getenv("JOERITRACE");
	if (tst_trace_dir) {
		long t = time(NULL);
		int p = getpid();
		sprintf(path, "%s/trace.%ld.%d.log", tst_trace_dir, t, p);
	} else {
		strcpy(path, "a");
	}

	s = open_wastream(path);
	return s;
}

str
OPTdataflowImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int list_flags = LIST_MAL_ALL | LIST_MAL_NOCOMMENTS;
	static stream *s = NULL;
	static MT_Lock lock = MT_LOCK_INITIALIZER(lock);

	MT_lock_set(&lock);

	if (s == NULL)
		s = open_trace_stream();

	if (s) {
		printFunction(s, mb, stk, list_flags);
		mnstr_printf(s, "\n\n--\n\n");
		mnstr_flush(s, MNSTR_FLUSH_ALL);
	}
	str msg = OPTdataflowImplementation_wrapped(cntxt, mb, stk, p);

	if (s) {
		if (msg != MAL_SUCCEED) {
			mnstr_printf(s, "ERROR: %s\n", msg);
		}
		printFunction(s, mb, stk, list_flags);
		mnstr_printf(s, "\n\n------\n\n");
		mnstr_flush(s, MNSTR_FLUSH_ALL);
	}

	MT_lock_unset(&lock);

	return msg;
}
