#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "monetdb_config.h"

#include "../../modules/atoms/mtime.h"
#include "dvf.h"
#include "mal_interpreter.h"

#define NUM_RET_MOUNT 4

int get_column_num(str schema_name, str table_name, str column_name);
int get_column_type(str schema_name, str table_name, int column_num);

int get_column_type(str schema_name, str table_name, int column_num)
{
	if(strcmp(schema_name, "mseed") != 0 || strcmp(table_name, "data") != 0)
		return -1;

	switch(column_num)
	{
		case 0:
			return TYPE_str;
		case 1:
			return TYPE_int;
		case 2:
			return TYPE_timestamp;
		case 3:
			return TYPE_int;
		default:
			return -1;
	}
}

int get_column_num(str schema_name, str table_name, str column_name)
{
	if(strcmp(schema_name, "mseed") != 0 || strcmp(table_name, "data") != 0)
		return -1;

	if(strcmp(column_name, "file_location") == 0)
		return 0;
	else if(strcmp(column_name, "seq_no") == 0)
		return 1;
	else if(strcmp(column_name, "sample_time") == 0)
		return 2;
	else if(strcmp(column_name, "sample_value") == 0)
		return 3;
	else
		return -1;
}

str plan_modifier(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str data_table_identifier = "data";
	str mountRef = putName("mount", 5);
	str miniseedRef = putName("miniseed", 8);
	str dvfRef = putName("dvf", 3);
	str planmodifierRef = putName("plan_modifier", 13);

	InstrPtr *old = NULL, *mounts = NULL, q = NULL, r = NULL, s = NULL, o = NULL;
	int i, j, k, limit, slimit, actions = 0;
	int num_fl = 0;
	BUN b1 = 0, b2 = 0;

	/* Declarations for copying of vars into stack and making a recursive runMALsequence call */
	str msg = MAL_SUCCEED;
	MalStkPtr stk_new;
	ValPtr lhs, rhs;
	int startpc = 1000000, old_vtop = 0;
	MalBlkPtr copy_old, copy_mb;
	bit is_stack_new = FALSE;

	BAT *BAT_fl = NULL; /* BAT for file_locations */

	bit after_first_data_bind = FALSE;

	str *schema_name = (str*) getArgReference(stk,pci,1); /* arg 1: schema_name */
	int bat_fl = *(int*) getArgReference(stk,pci,2); /* arg 2: bat of file_locations */

	BATiter fli;
	
	int run_dataflow_opt = 1;

	VarRecord low, high;

	/* prepare to set low and high oids of return vars of mounts */
	high.value.vtype= low.value.vtype= TYPE_oid;
	high.value.val.oval = 0;
	low.value.val.oval = 0;

	/* check for logical error: mb must never be NULL */
	assert (mb != NULL);

	cntxt = cntxt; /* to escape 'unused' parameter error. */
	stk = stk; /* to escape 'unused' parameter error. */
	pci = pci; /* to escape 'unused' parameter error. */

	if ((BAT_fl = BATdescriptor(bat_fl)) == NULL)
		throw(MAL, "dvf.plan_modifier", RUNTIME_OBJECT_MISSING);

	/* check tail type */
	if (BAT_fl->ttype != TYPE_str)
	{
		throw(MAL, "dvf.plan_modifier",
		      "tail-type of input BAT must be TYPE_str");
	}

	BBPincref(bat_fl, TRUE);

	num_fl = BAT_fl->U->count;

	/* when number of files to be mounted is 0. */
	if(num_fl == 0)
		goto finish;

	/* prepare to keep the potential mount instructions */
	mounts = (InstrPtr*)GDKmalloc(num_fl*sizeof(InstrPtr));
	if(mounts == NULL)
		throw(MAL, "dvf.plan_modifier", MAL_MALLOC_FAIL);

	/* save the old stage of the MAL block */
	copy_mb = copyMalBlk(mb);
	copy_old = mb;
	mb = copy_mb;

	old = mb->stmt;
	limit= mb->stop;
	slimit = mb->ssize;


	/* initialize the statement list. Notice, the symbol table remains intact */
	if (newMalBlkStmt(mb, slimit) < 0)
		return 0;

	/* iterate over the instructions of the input MAL program, skip the dvf.plan_modifier itself. */
	for (i = 0; i < limit; i++)
	{
		InstrPtr p = old[i];

		/* check for
		 * dvf.plan_modifier(...);
		 */
		if(getModuleId(p) == dvfRef &&
			getFunctionId(p) == planmodifierRef)
		{
			startpc = i;
			pushInstruction(mb, copyInstruction(old[i]));
		}
		/* check for
		 * v7 := sql.bind(..., schema_name, data_table_name, ..., ...);
		 */
		else if(getModuleId(p) == sqlRef &&
			getFunctionId(p) == bindRef &&
			p->argc == 6 &&
			p->retc == 1 &&
			strcmp(getVarConstant(mb, getArg(p, 2)).val.sval, *schema_name) == 0 &&
			strstr(getVarConstant(mb, getArg(p, 3)).val.sval, data_table_identifier) != NULL)
		{
			int which_mount = 0;
			int which_column = 0;
			if(!after_first_data_bind)
			{
				int which_fl = 0;
				after_first_data_bind = TRUE;
				/* push mount instructions */
				/* create BAT iterator */
				fli = bat_iterator(BAT_fl);

				/* loop over the file_locations */

				BATloop(BAT_fl, b1, b2)
				{
					int a = 0, type;
					/* get tail value */
					str t = (str) BUNtail(fli, b1);

					/* create mount instruction */
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, miniseedRef);
					setFunctionId(q, mountRef);
					for(; a < NUM_RET_MOUNT; a++)
					{
						type = get_column_type(*schema_name, getVarConstant(mb, getArg(p, 3)).val.sval, a);
						if(type < 0)
							throw(MAL, "dvf.get_column_num", "is not defined yet for schema: %s and table: %s and column: %s.", *schema_name, getVarConstant(mb, getArg(p, 3)).val.sval, getVarConstant(mb, getArg(p, 4)).val.sval);
						q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_oid, type)));
						varSetProp(mb, getArg(q, a), PropertyIndex("hlb"), op_gte, (ptr) &low.value);
						varSetProp(mb, getArg(q, a), PropertyIndex("hub"), op_lt, (ptr) &high.value);
					}

					q = pushStr(mb, q, t);

					low.value.val.oval += 1;
					high.value.val.oval += 1;

					mounts[which_fl] = q;
					which_fl++;
					/* push the new instruction */
					pushInstruction(mb, q);
					actions++;
				}

				/* check for logical error */
				assert(which_fl == num_fl);

			}

			/* new mat.new for the column being binded */
			r = newInstruction(mb, ASSIGNsymbol);
			setModuleId(r, matRef);
			setFunctionId(r, newRef);
			r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any)); /* push tmp var to pass to markH. */
			which_column = get_column_num(*schema_name, getVarConstant(mb, getArg(p, 3)).val.sval,
						      getVarConstant(mb, getArg(p, 4)).val.sval);
			if(which_column < 0)
				throw(MAL, "dvf.get_column_num", "is not defined yet for schema: %s and table: %s and column: %s.",
				      *schema_name, getVarConstant(mb, getArg(p, 3)).val.sval, getVarConstant(mb, getArg(p, 4)).val.sval);
			for(; which_mount < num_fl; which_mount++)
			{
				r = pushArgument(mb, r, getArg(mounts[which_mount], which_column));
			}

			setVarInit(mb, getArg(r, 0));

			/* push the new instruction */
			pushInstruction(mb, r);
			actions++;

			/* arrange oids of return val of mat.new */
			s = newInstruction(mb, ASSIGNsymbol);
			setModuleId(s, algebraRef);
			setFunctionId(s, markHRef);
			s = pushReturn(mb, s, getArg(p, 0)); /* push the ret of sql.bind as ret of (mat.new + algebra.markH) */
			s = pushArgument(mb, s, getArg(r, 0));
			s = pushOid(mb, s, 0);

			/* push the new instruction */
			pushInstruction(mb, s);
			actions++;
/* 			s = s; */

			/* comment out in the old for reusing the old */
			copy_old->stmt[i]->token = REMsymbol;

		}
		else
		{
			/* push instruction */
			pushInstruction(mb, copyInstruction(old[i]));

			if (p->token == ENDsymbol) break;

			/* comment out in the old for reusing the old */
			if(i > startpc)
			{
/* 				old[i]->token = REMsymbol; */
				copy_old->stmt[i]->token = REMsymbol;
			}
		}
	}
	/* We would like to retain everything from the ENDsymbol
	 * up to the end of the plan, because after the ENDsymbol
	 * the remaining optimizer steps are stored.
	 */
	for(i++; i<limit; i++)
		if (old[i])
			pushInstruction(mb, copyInstruction(old[i]));


	/* save the vtop before calling any optimizer. */
	old_vtop = mb->vtop;

	/* call necessary optimizers
	 * *optimizer.inline();optimizer.remap();optimizer.evaluate();optimizer.costModel();optimizer.coercions();optimizer.emptySet();optimizer.aliases(); optimizer.mergetable();optimizer.deadcode();optimizer.commonTerms();optimizer.groups();optimizer.joinPath();optimizer.reorder();optimizer.deadcode();optimizer.reduce();optimizer.history();optimizer.multiplex();optimizer.accumulators();optimizer.garbageCollector();
	 */

	o = newFcnCall(mb, "optimizer", "mergetable");
	typeChecker(cntxt->fdout, cntxt->nspace, mb, o, FALSE);
	
	if(run_dataflow_opt)
	{
		o = newFcnCall(mb, "optimizer", "dataflow");
		typeChecker(cntxt->fdout, cntxt->nspace, mb, o, FALSE);
	}

	optimizeMALBlock(cntxt, mb);

	/* New variables might have been created by the optimizers, so their values has to be copied into the stack. However, there might not be enough space in stack for them. We cannot reallocate the stack, but we may create our own enlarged stack, then run the rest of the plan with our own stack. */

	/* arrange the new stack without freeing the old one. */
	if (stk->stksize > mb->vsize)
		stk_new = stk;
	else
	{
		k = ((mb->vsize / STACKINCR) + 1) * STACKINCR;
		stk_new = newGlobalStack(k);
		memcpy(stk_new, stk, stackSize(stk->stksize));
		stk_new->stksize = k;
		is_stack_new = TRUE;
	}

	/* copy values into the new stack */
	for (j = 0; j < mb->vtop; j++) {
		lhs = &stk_new->stk[j];
		if (isVarConstant(mb, j) > 0) {
			if (!isVarDisabled(mb, j)) {
				rhs = &getVarConstant(mb, j);
				VALcopy(lhs, rhs);
			}
		}
		else
		{
			if (j > old_vtop)
			{
				lhs->vtype = getVarGDKType(mb, j);
				lhs->val.pval = 0;
				lhs->len = 0;
			}
			else if(j > stk->stksize)
			{
				rhs = getVarValue(mb, j);
				VALcopy(lhs, rhs);
			}
			else
			{
				rhs = &stk->stk[j];
				VALcopy(lhs, rhs);
			}
		}
	}
	stk_new->blk = mb;

	/* adjust variable lifetimes */
	malGarbageCollector(mb);

/* 	chkProgram(cntxt->fdout, cntxt->nspace, mb);
	printFunction(cntxt->fdout,mb, 0, LIST_MAL_EXPLAIN); */

	/* relocate the startpc, the instruction to proceed with the execution. Because it might be changed by the optimizers. */
	for (i = 0; i < limit; i++)
	{
		InstrPtr ip = mb->stmt[i];
		
		/* check for
		 * dvf.plan_modifier(...);
		 */
		if(getModuleId(ip) == dvfRef &&
			getFunctionId(ip) == planmodifierRef)
		{
			startpc = i;
		}
	}
	
	msg = msg;
	/* run rest of the plan */
	msg = runMALsequence(cntxt, mb, startpc+1, mb->stop, stk_new, stk_new, mb->stmt[startpc]);

	if(msg != MAL_SUCCEED)
	{
		throw(MAL, "dvf.plan_modifier", "From the recursive call: %s", msg);
	}

/* 	chkProgram(cntxt->fdout, cntxt->nspace, copy_old); */
/* 	printFunction(cntxt->fdout, copy_old, 0, LIST_MAL_EXPLAIN); */

	/* any remaining MAL instruction records are removed */
	for(i = 0; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);

	GDKfree(old);
	freeMalBlk(mb);
	if(is_stack_new)
		freeStack(stk_new);

finish:
	/* for statistics we print if/how many patches have been made */
	DEBUGoptimizers
	printf("#dvf.plan_modifier: %d actions\n", actions);

	return MAL_SUCCEED;
}

