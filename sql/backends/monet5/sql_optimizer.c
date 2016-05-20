/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @f sql_optimizer
 * @t SQL catalog management
 * @a N. Nes, M.L. Kersten
 */
/*
 * The queries are stored in the user cache after they have been
 * type checked and optimized.
 * The Factory optimizer encapsulates the query with a re-entrance
 * structure. However, this structure is only effective if
 * quite some (expensive) instructions can be safed.
 * The current heuristic is geared at avoiding trivial
 * factory structures.
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
	lng space = 0, i; 

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
						space += getBatSpace(b);
						BBPunfix(b->batCacheid);
					}
				}
			} else if (f == bindRef) {
				sql_table *t = mvc_bind_table(m, s, tname);
				sql_column *c = mvc_bind_column(m, t, cname);

				if (c && (!isRemote(c->t) && !isMergeTable(c->t))) {
					BAT *b = store_funcs.bind_col(tr, c, RDONLY);
					if (b) {
						space += getBatSpace(b);
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

void
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
	if (be->mvc->emod & mod_debug){
		addtoMalBlkHistory(mb);
		c->curprg->def->keephistory = TRUE;
	} else
		c->curprg->def->keephistory = FALSE;
}

static str
sqlJIToptimizer(Client c, MalBlkPtr mb, backend *be)
{
	str msg;
	str pipe = getSQLoptimizer(be->mvc);

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
optimizeQuery(Client c)
{
	MalBlkPtr mb;
	backend *be;
	str msg = 0;

	be = (backend *) c->sqlcontext;
	assert(be && be->mvc);	/* SQL clients should always have their state set */

	trimMalBlk(c->curprg->def);
	c->blkmode = 0;
	mb = c->curprg->def;
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
	return sqlJIToptimizer(c,mb,be);
}

void
addQueryToCache(Client c)
{
	str msg = NULL;

	insertSymbol(c->nspace, c->curprg);
	msg = optimizeQuery(c);
	if (msg != MAL_SUCCEED) {
		showScriptException(c->fdout, c->curprg->def, 0, MAL, "%s", msg);
		GDKfree(msg);
	}
}

/*
 * The default SQL optimizer performs a limited set of operations
 * that are known to be (reasonably) stable and effective.
 * Finegrained control over the optimizer steps is available thru
 * setting the corresponding SQL variable.
 *
 * This version simply runs through the MAL script and re-orders the instructions
 * into catalog operations, query graph, and result preparation.
 * This distinction is used to turn the function into a factory, which would
 * enable re-entry when used as a cache-optimized query.
 * The second optimization is move access mode changes on the base tables
 * to the front of the plan.
 *
 *
 */
str
SQLoptimizer(Client c)
{
	(void) c;
#ifdef _SQL_OPTIMIZER_DEBUG
	mnstr_printf(GDKout, "SQLoptimizer\n");
	printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_DEBUG);
	mnstr_printf(GDKout, "done\n");
#endif
	return MAL_SUCCEED;
}
