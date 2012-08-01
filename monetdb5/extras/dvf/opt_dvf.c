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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */


/*
 * @a Yagiz Kargin
 * @- Data Vault Framework Optimizer
 *
 * This optimizer is for "indirect" rewriting of MAL plans that access the 'data'
 * tables of a data vault in the framework.
 *
 * Its indirectness comes from the fact that this optimizer is to inject only
 * one MAL line (a pattern call) somewhere in the MAL plan, and that pattern call
 * will do the real rewriting in the execution time.
 *
 * So, the aim of this optimizer is to find the right place to inject the pattern
 * call.
 *
 * The right place for injection is right after we get to know about the files
 * which have to be mounted (or possibly brought). It should also be before any
 * 'data' table is bound.
 *
 * For that purpose, the optimizer's task is to recognize MAL code
 * patterns like
 *
 *  ...
 *  v3 := sql.bind(..., schema_name, meta_table_name, "file_location", 0);
 *  ...
 *  v4 := algebra.leftjoin(v2, v3);
 *  .
 *  .
 *  .
 *  v6 := algebra.leftjoin(v5, v4);
 *  ...
 *  v7 := sql.bind(..., schema_name, data_table_name, ..., ...);
 *  ...
 *
 * WHERE the same pattern does not occur between assignments of v6 and v7.
 * Namely, the subpattern of v3, v4, ..., v6 should not occur in '...' before v7.
 * (Note that from v4 to v6 always the similar leftjoin pattern occurs.)
 *
 * And transforms them into
 *
 *  ...
 *  v3 := sql.bind(..., schema_name, meta_table_name, "file_location", 0);
 *  ...
 *  v4 := algebra.leftjoin(v2, v3);
 *  .
 *  .
 *  .
 *  v6 := algebra.leftjoin(v5, v4);
 *  (t1, t2) := group.done(v6);
 *  t3 := bat.mirror(t1);
 *  t4 := algebra.leftjoin(t3, v6);
 *  dvf.plan_modifier(schema_name, t4);
 *  ...
 *  v7 := sql.bind(..., schema_name, data_table_name, ..., ...);
 *  ...
 *
 * The optimizer looks for this pattern because v2 is supposed to have the oids
 * of meta_table_name with all the selections on that meta_table_name are applied
 * (it is all selections because of the WHERE clause) and v5 is supposed to have
 * the oids of (an)other meta_table_name with all the selections on that
 * meta_table_name are applied. Then v6 is supposed to provide the required
 * file_locations. Line with v7 is there, because the required file_locations
 * should be collected before the first bind to a data_table happens.
 *
 * The first 3 lines directly after assignment to v6 are to get the distinct
 * file_locations.
 *
 *
 * WARNING: This optimizer should run without the dataflow optimizations!
*/



#include "monetdb_config.h"
#include "opt_dvf.h"
#include "mal_interpreter.h"
#include "opt_statistics.h"

static int
OPTdvfImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	//TODO: Replace these with a proper (global) constants
	str sys_schema_name = "sys";
	str data_table_identifier = "data";
	str file_location_identifier = "file_location";
	str dvfRef = putName("dvf", 3);
	str planmodifierRef = putName("plan_modifier", 13);

	//states of finding the pattern
	int state = 0; //0: start, 1:found v3, 2:found v5, 3:found v6;

	//state variables (instruction index) numbered with state
	int i1 = 0, i2 = 0;

	InstrPtr *old = NULL, q = NULL, r = NULL, s = NULL, t = NULL;
	int i, limit, actions = 0;

	stk = stk; //to escape 'unused' parameter error.
	pci = pci; //to escape 'unused' parameter error.
	cntxt = cntxt; //to escape 'unused' parameter error.

	/* check for logical error: mb must never be NULL */
	assert (mb != NULL);

	/* save the old stage of the MAL block */
	old = mb->stmt;
	limit= mb->stop;

	/* iterate over the instructions of the input MAL program */
	for (i = 1; i < limit; i++) /* the plan signature can be skipped safely */
	{
		InstrPtr p = old[i];

		/* check for
		 * v3 := sql.bind(..., schema_name, meta_table_name, "file_location", 0);
		 */
		if(getModuleId(p) == sqlRef &&
			getFunctionId(p) == bindRef &&
			p->argc == 6 &&
			p->retc == 1 &&
			strcmp(getVarConstant(mb, getArg(p, 2)).val.sval, sys_schema_name) != 0 &&
			strstr(getVarConstant(mb, getArg(p, 3)).val.sval, data_table_identifier) == NULL &&
			strcmp(getVarConstant(mb, getArg(p, 4)).val.sval, file_location_identifier) == 0)
		{
			i1 = i;
			state = 1;
		}
		/* check for
		 * v4 := algebra.leftjoin(v2, v3);
		 */
		else if((state == 1 || state == 2) &&
			getModuleId(p) == algebraRef &&
			getFunctionId(p) == leftjoinRef &&
			p->argc == 3 &&
			p->retc == 1 &&
			getArg(p, 2) == getArg(old[i1], 0))
		{
			i2 = i;
			state = 2;

			/* check for
			 * v6 := algebra.leftjoin(v5, v4);
			 * or series thereof.
			 */
			for(i = i+1; i < limit; i++)
			{
				p = old[i];

				if(getModuleId(p) == algebraRef &&
					getFunctionId(p) == leftjoinRef &&
					p->argc == 3 &&
					p->retc == 1 &&
					getArg(p, 2) == getArg(old[i2], 0))
				{
					i2 = i;
				}
				else
				{
					i = i-1;
					break;
				}
			}
		}
		/* check for
		 * v7 := sql.bind(..., schema_name, data_table_name, ..., ...);
		 */
		else if((state == 1 || state == 2) &&
			getModuleId(p) == sqlRef &&
			getFunctionId(p) == bindRef &&
			p->argc == 6 &&
			p->retc == 1 &&
			strcmp(getVarConstant(mb, getArg(p, 2)).val.sval, getVarConstant(mb, getArg(old[i1], 2)).val.sval) == 0 &&
			strstr(getVarConstant(mb, getArg(p, 3)).val.sval, data_table_identifier) != NULL)
		{

			switch(state)
			{
				case 1:
					/* Error! What to do */
					return -1;
					//throw(MAL,"optimizer.DVframework", "Schema of %s vault is not well-organized.\n", getVarConstant(mb, getArg(p, 2)).val.sval);
				case 2:
					/* pattern found! What to do */
					/* (t1, t2) := group.done(v6);
					 *  t3 := bat.mirror(t1);
					 *  t4 := algebra.leftjoin(t3, v6);
					 *  dvf.plan_modifier(schema_name, t4);
					 */

					/* create group.done instruction */
					r = newInstruction(mb, ASSIGNsymbol);
					setModuleId(r, groupRef);
					setFunctionId(r, doneRef);
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_bat));
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_bat));
					r = pushArgument(mb, r, getArg(old[i2], 0));


					/* create bat.mirror instruction */
					s = newInstruction(mb, ASSIGNsymbol);
					setModuleId(s, batRef);
					setFunctionId(s, mirrorRef);
					s = pushReturn(mb, s, newTmpVariable(mb, TYPE_bat));
					s = pushArgument(mb, s, getArg(r, 0));

					/* create algebra.leftjoin instruction */
					t = newInstruction(mb, ASSIGNsymbol);
					setModuleId(t, algebraRef);
					setFunctionId(t, leftjoinRef);
					t = pushReturn(mb, t, newTmpVariable(mb, TYPE_bat));
					t = pushArgument(mb, t, getArg(s, 0));
					t = pushArgument(mb, t, getArg(old[i2], 0));

					/* create dvf.plan_modifier instruction */
					q = newInstruction(mb, ASSIGNsymbol);
					q->argc = 3;
					q->retc = 1;
					setModuleId(q, dvfRef);
					setFunctionId(q, planmodifierRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_void));
					getArg(q, 1) = getArg(p, 2);
					getArg(q, 2) = getArg(t, 0);

					/* insert the new instruction in pc i2+1 */
					insertInstruction(mb, q, i2+1);
					insertInstruction(mb, t, i2+1);
					insertInstruction(mb, s, i2+1);
					insertInstruction(mb, r, i2+1);

					actions++;
					goto finish;
			}
		}
	}

finish:
	return actions;

}

str OPTdvf(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str msg= MAL_SUCCEED;
	lng t,clk= GDKusec();
	int actions = 0;
	
	optimizerInit();
	if( p )
		removeInstruction(mb, p);

	if( mb->errors ){
		/* when we have errors, we still want to see them */
		addtoMalBlkHistory(mb,"dvf");
		return MAL_SUCCEED;
	}
	actions= OPTdvfImplementation(cntxt, mb,stk,p);
	msg= optimizerCheck(cntxt, mb, "optimizer.DVframework", actions, t=(GDKusec() - clk),OPT_CHECK_ALL);
	OPTDEBUGdvf {
		mnstr_printf(cntxt->fdout,"=FINISHED opt_dvf %d\n",actions);
		printFunction(cntxt->fdout,mb,0,LIST_MAL_STMT | LIST_MAPI);
	}
	DEBUGoptimizers
	mnstr_printf(cntxt->fdout,"#opt_dvf: " LLFMT " ms\n",t);
	QOTupdateStatistics("dvf",actions,t);
	addtoMalBlkHistory(mb,"dvf");
	return msg;
}
