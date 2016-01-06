/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a S. Manegold
 * @- SQL append show-case optimizer
 *
 * This optimizer is mainly for demonstrating how to implement a MAL
 * optimizer in principle.
 *
 * Purely for that purpose, the optimizer's task is to recognize MAL code
 * patterns like
 *
 *  ...
 *  v3 := sql.append( ..., ..., ..., ..., v0 );
 *  ...
 *  v5 := ... v0 ...;
 *  ...
 *
 * i.e., an sql.append() statement that is eventually followed by some other
 * statement later on in the MAL program that uses the same v0 BAT as
 * argument as the sql.append() statement does,
 * Do you assume a single re-use of the variable v0?
 * Do you assume a non-nested  MAL block ?
 *
 * and transform them into
 *
 *  ...
 *  v1 := aggr.count( v0 );
 *  v2 := algebra.slice( v0, 0, v1 );
 *  v3 := sql.append( ..., ..., ..., ..., v2 );
 *  ...
 *  v5 := ... v0 ...;
 *  ...
 *
 * i.e., handing a BAT view v2 of BAT v0 as argument to the sql.append()
 * statement, rather than the original BAT v0.
 * My advice, always use new variable names, it may capture some easy
 * to make errors.
 *
 * As a refinement, patterns like
 *
 *  ...
 *  v3 := sql.append( ..., ..., ..., ..., v0 );
 *  v4 := aggr.count( v0 );
 *  ...
 *  v5 := ... v0 ...;
 *  ...
 *
 * are transformed into
 *
 *  ...
 *  v4 := aggr.count( v0 );
 *  v2 := algebra.slice( v0, 0, v4 );
 *  v3 := sql.append( ..., ..., ..., ..., v2 );
 *  ...
 *  v5 := ... v0 ...;
 *  ...
 *
 */

/*
 * It is mandatory to make optimizers part of the 'optimizer' module.
 * This allows the optimizer implementation to find them and react on them.
 */
#include "monetdb_config.h"
#include "opt_sql_append.h"
#include "mal_interpreter.h"
#include "opt_statistics.h"

/* focus initially on persistent tables. */

static int
OPTsql_appendImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr *old = NULL;
	int i, limit, slimit, actions = 0;

	(void) pci; /* Tell compilers that we know that we do not */
	(void) stk; /* use these function parameters, here.       */

	/* In general, a MAL optimizer transforms a given MAL program into a
	 * modified one by sequentially walking through the given program
	 * and concurrently creating a new one from scratch by
	 * (1) copying statements as is, modified, or in a different order,
	 * or (2) omitting statements or (3) introducing new statements.
	 */

	/* check for logical error: mb must never be NULL */
	assert (mb != NULL);

	/* save the old stage of the MAL block */
	old = mb->stmt;
	limit= mb->stop;
	slimit = mb->ssize;

	/* initialize the statement list. Notice, the symbol table remains intact */
	if (newMalBlkStmt(mb, mb->ssize) < 0)
		return 0;

	/* the plan signature can be copied safely */
	pushInstruction(mb, old[0]);

	/* iterate over the instructions of the input MAL program */
	for (i = 1; i < limit; i++) {
		InstrPtr p = old[i];

		/* check for
		 *  v3 := sql.append( ..., ..., ..., ..., v0 );
		 */
		if (getModuleId(p) == sqlRef &&
		    getFunctionId(p) == appendRef &&
		    p->argc > 5 &&
		    p->retc == 1 &&
		    isaBatType(getArgType(mb, p, 5))) {
			/* found
			 *  v3 := sql.append( ..., ..., ..., ..., v0 );
			 */
			int j = 0, k = 0;
			InstrPtr q1 = NULL, q2 = NULL;
			bit found = FALSE;

			/* check whether next is
			 *  v4 := aggr.count(v0);
			 */
			if (i+1 < limit) {
				InstrPtr q = old[i+1];
				if (getModuleId(q) == aggrRef &&
				    getFunctionId(q) == countRef &&
				    q->argc == 2 &&
				    q->retc == 1 &&
				    getArg(q, 1) == getArg(p, 5)) {
					/* found
					 *  v3 := sql.append( ..., ..., ..., ..., v0 );
					 *  v4 := aggr.count(v0);
					 */
					/* issue/execute
					 *  v4 := aggr.count(v0);
					 * before
					 *  v3 := sql.append( ..., ..., ..., ..., v0 );
					 */
					pushInstruction(mb, q);
					q1 = q;
					i++;
					actions++;	/* to keep track if anything has been done */
				}
			}

			/* look for
			 *  v5 := ... v0 ...;
			 */
			/* an expensive loop, better would be to remember that v0
			 * has a different role.  A typical method is to keep a
			 * map from variable -> instruction where it was
			 * detected. Then you can check each assignment for use of
			 * v0
			*/
			for (j = i+1; !found  && j < limit; j++)
				for (k = old[j]->retc; !found && k < old[j]->argc; k++)
					found = (getArg(old[j], k) == getArg(p, 5));
			if (found) {
				/* replace
				 *  v3 := sql.append( ..., ..., ..., ..., v0 );
				 * with
				 *  v1 := aggr.count( v0 );
				 *  v2 := algebra.slice( v0, 0, v1 );
				 *  v3 := sql.append( ..., ..., ..., ..., v2 );
				 */

				/* push new v1 := aggr.count( v0 ); unless already available */
				if (q1 == NULL) {
					/* use mal_builder.h primitives
					 * q1 = newStmt(mb, aggrRef,countRef);
					 * setArgType(mb,q1,TYPE_wrd) */
					/* it will be added to the block and even my
					 * re-use MAL instructions */
					q1 = newInstruction(mb,ASSIGNsymbol);
					getArg(q1,0) = newTmpVariable(mb, TYPE_wrd);
					setModuleId(q1, aggrRef);
					setFunctionId(q1, countRef);
					q1 = pushArgument(mb, q1, getArg(p, 5));
					pushInstruction(mb, q1);
				}

				/* push new v2 := algebra.slice( v0, 0, v1 ); */
				/* use mal_builder.h primitives
				 * q1 = newStmt(mb, algebraRef,sliceRef); */
				q2 = newInstruction(mb,ASSIGNsymbol);
				getArg(q2,0) = newTmpVariable(mb, TYPE_any);
				setModuleId(q2, algebraRef);
				setFunctionId(q2, sliceRef);
				q2 = pushArgument(mb, q2, getArg(p, 5));
				q2 = pushWrd(mb, q2, 0);
				q2 = pushArgument(mb, q2, getArg(q1, 0));
				pushInstruction(mb, q2);

				/* push modified v3 := sql.append( ..., ..., ..., ..., v2 ); */
				getArg(p, 5) = getArg(q2, 0);
				pushInstruction(mb, p);

				actions++;
				continue;
			}
		}

		pushInstruction(mb, p);
		if (p->token == ENDsymbol) break;
	}

	/* We would like to retain everything from the ENDsymbol
	 * up to the end of the plan, because after the ENDsymbol
	 * the remaining optimizer steps are stored.
	 */
	for(i++; i<limit; i++)
		if (old[i])
			pushInstruction(mb, old[i]);
	/* any remaining MAL instruction records are removed */
	for(; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);

	GDKfree(old);

	/* for statistics we return if/how many patches have been made */
	DEBUGoptimizers
		mnstr_printf(cntxt->fdout,"#opt_sql_append: %d statements added\n",
				actions);
	return actions;
}

/* optimizers have to be registered in the optcatalog in opt_support.c.
 * you have to path the file accordingly.
 */

/* Optimizer code wrapper
 * The optimizer wrapper code is the interface to the MAL optimizer calls.
 * It prepares the environment for the optimizers to do their work and removes
 * the call itself to avoid endless recursions.
 *
 * Before an optimizer is finished, it should leave a clean state behind.
 * Moreover, the information of the optimization step is saved for
 * debugging and analysis.
 *
 * The wrapper expects the optimizers to return the number of
 * actions taken, i.e. number of successful changes to the code.
 */

str OPTsql_append(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	str modnme;
	str fcnnme;
	str msg= MAL_SUCCEED;
	Symbol s= NULL;
	lng t,clk= GDKusec();
	int actions = 0;

	if( p )
		removeInstruction(mb, p);
	OPTDEBUGsql_append mnstr_printf(cntxt->fdout,"=APPLY OPTIMIZER sql_append\n");
	if( p && p->argc > 1 ){
		if( getArgType(mb,p,1) != TYPE_str ||
			getArgType(mb,p,2) != TYPE_str ||
			!isVarConstant(mb,getArg(p,1)) ||
			!isVarConstant(mb,getArg(p,2))
		) {
			throw(MAL, "optimizer.sql_append", ILLARG_CONSTANTS);
		}
		if( stk != 0){
			modnme= *getArgReference_str(stk,p,1);
			fcnnme= *getArgReference_str(stk,p,2);
		} else {
			modnme= getArgDefault(mb,p,1);
			fcnnme= getArgDefault(mb,p,2);
		}
		s= findSymbol(cntxt->nspace, putName(modnme,strlen(modnme)),putName(fcnnme,strlen(fcnnme)));

		if( s == NULL) {
			char buf[1024];
			snprintf(buf,1024, "%s.%s",modnme,fcnnme);
			throw(MAL, "optimizer.sql_append", RUNTIME_OBJECT_UNDEFINED ":%s", buf);
		}
		mb = s->def;
		stk= 0;
	}
	if( mb->errors ){
		/* when we have errors, we still want to see them */
		addtoMalBlkHistory(mb,"sql_append");
		return MAL_SUCCEED;
	}
	actions= OPTsql_appendImplementation(cntxt, mb,stk,p);
	msg= optimizerCheck(cntxt, mb, "optimizer.sql_append", actions, t=(GDKusec() - clk));
	OPTDEBUGsql_append {
		mnstr_printf(cntxt->fdout,"=FINISHED sql_append %d\n",actions);
		printFunction(cntxt->fdout,mb,0,LIST_MAL_ALL );
	}
	DEBUGoptimizers
		mnstr_printf(cntxt->fdout,"#opt_reduce: " LLFMT " ms\n",t);
	QOTupdateStatistics("sql_append",actions,t);
	addtoMalBlkHistory(mb,"sql_append");
	return msg;
}
