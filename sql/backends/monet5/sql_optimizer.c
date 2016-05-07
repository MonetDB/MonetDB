/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * N. Nes, M.L. Kersten
 */
/*
 * The queries are stored in the user cache after they have been
 * type checked and optimized.
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "mal_debugger.h"
#include "opt_prelude.h"
#include "sql_mvc.h"
#include "sql_optimizer.h"
#include "sql_scenario.h"
#include "sql_gencode.h"
#include "opt_pipes.h"

static lng 
SQLgetSpace(mvc *m, MalBlkPtr mb)
{
	sql_trans *tr = m->session->tr;
	lng size,space = 0, i; 

	for (i = 0; i < mb->stop; i++) {
		InstrPtr p = mb->stmt[i];
		char *f = getFunctionId(p);

		if (getModuleId(p) == sqlRef && (f == bindRef || f == bindidxRef)) {
			int upd = (p->argc == 7 || p->argc == 9), mode = 0;
			char *sname = getVarConstant(mb, getArg(p, 2 + upd)).val.sval;
			char *tname = getVarConstant(mb, getArg(p, 3 + upd)).val.sval;
			char *cname = NULL;
			sql_schema *s = mvc_bind_schema(m, sname);

			if (!s || strcmp(s->base.name, dt_schema) == 0) 
				continue;
			cname = getVarConstant(mb, getArg(p, 4 + upd)).val.sval;
			mode = getVarConstant(mb, getArg(p, 5 + upd)).val.ival;
			if (mode != 0 || !cname || !s)
				continue;
			if (f == bindidxRef) {
				sql_idx *i = mvc_bind_idx(m, s, cname);

				if (i && (!isRemote(i->t) && !isMergeTable(i->t))) {
					BAT *b = store_funcs.bind_idx(tr, i, RDONLY);
					if (b) {
						space += (size =getBatSpace(b));
						if( 0 && size == 0){
							// replace with an empty dummy bat
							clrFunction(p);
							setModuleId(p, batRef);
							setFunctionId(p, newRef);
							p->argc =1;
							p =pushType(mb,p, b->ttype);
					
						}
						BBPunfix(b->batCacheid);
					}
				}
			} else if (f == bindRef) {
				sql_table *t = mvc_bind_table(m, s, tname);
				sql_column *c = mvc_bind_column(m, t, cname);

				if (c && (!isRemote(c->t) && !isMergeTable(c->t))) {
					BAT *b = store_funcs.bind_col(tr, c, RDONLY);
					if (b) {
						space += (size= getBatSpace(b));
						if( 0 && size == 0){
							// replace with an empty dummy bat
							clrFunction(p);
							setModuleId(p, batRef);
							setFunctionId(p, newRef);
							p->argc =1;
							p =pushType(mb,p, b->ttype);
						}
						BBPunfix(b->batCacheid);
					}
				}
			}
		}
	}
	return space;
}

str
getSQLoptimizer(mvc *m)
{
	ValRecord *val = stack_get_var(m, "optimizer");
	char *pipe = "default_pipe";

	if (val && val->val.sval)
		pipe = val->val.sval;
	return pipe;
}

static void
addOptimizers(Client c, MalBlkPtr mb, char *pipe)
{
	int i;
	InstrPtr q;
	backend *be;
	str msg;
	lng space;

	be = (backend *) c->sqlcontext;
	assert(be && be->mvc);	/* SQL clients should always have their state set */

	space = SQLgetSpace(be->mvc, mb);
	if(space && (pipe == NULL || strcmp(pipe,"default_pipe")== 0)){
		if( space > (lng)(0.8 * MT_npages() * MT_pagesize())  && GDKnr_threads > 1){
			pipe = "volcano_pipe";
			//mnstr_printf(GDKout, "#use volcano optimizer pipeline? "SZFMT"\n", space);
		}else
			pipe = "default_pipe";
	} else
		pipe = pipe? pipe: "default_pipe";
	msg = addOptimizerPipe(c, mb, pipe);
	if (msg)
		GDKfree(msg);	/* what to do with an error? */
	if (be->mvc->no_mitosis) {
		for (i = mb->stop - 1; i > 0; i--) {
			q = getInstrPtr(mb, i);
			if (q->token == ENDsymbol)
				break;
			if (getFunctionId(q) == mitosisRef || getFunctionId(q) == dataflowRef)
				q->token = REMsymbol;	/* they are ignored */
		}
	}
	if (be->mvc->emod & mod_debug)
		addtoMalBlkHistory(mb, "getStatistics");
}

str
SQLoptimizeFunction(Client c, MalBlkPtr mb, mvc *m)
{
	str msg;
	str pipe = getSQLoptimizer(m);

	addOptimizers(c, mb, pipe);
	msg = optimizeMALBlock(c, mb);
	if (msg)
		return msg;

	/* time to execute the optimizers */
	if (c->debug)
		optimizerCheck(c, mb, "sql.baseline", -1, 0);
#ifdef _SQL_OPTIMIZER_DEBUG
	mnstr_printf(GDKout, "End Optimize Query\n");
	printFunction(GDKout, mb, 0, LIST_MAL_ALL);
#endif
	return MAL_SUCCEED;
}

str
SQLoptimizeQuery(Client c, MalBlkPtr mb)
{
	backend *be;
	str msg = 0;

	be = (backend *) c->sqlcontext;
	assert(be && be->mvc);	/* SQL clients should always have their state set */

	trimMalBlk(c->curprg->def);
	c->blkmode = 0;
	chkProgram(c->fdout, c->nspace, mb);
#ifdef _SQL_OPTIMIZER_DEBUG
	mnstr_printf(GDKout, "Optimize query\n");
	printFunction(GDKout, mb, 0, LIST_MAL_ALL);
#endif
	/*
	 * An error in the compilation should be reported to the user.
	 * And if the debugging option is set, the debugger is called
	 * to allow inspection.
	 */
	if (mb->errors) {
		showErrors(c);

		if (c->listing)
			printFunction(c->fdout, mb, 0, c->listing);
		if (be->mvc->debug) {
			msg = runMALDebugger(c, c->curprg->def);
			if (msg != MAL_SUCCEED)
				GDKfree(msg); /* ignore error */
		}
		return NULL;
	}
	return SQLoptimizeFunction(c,mb,be->mvc);
}

void
addQueryToCache(Client c)
{
	//str msg = NULL;

	insertSymbol(c->nspace, c->curprg);
}

