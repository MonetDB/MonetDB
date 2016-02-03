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

#define TOSMALL 10

/*
 * Cost-based optimization and semantic evaluations require statistics to work with.
 * They should come from the SQL catalog or the BATs themselves.
 * A better way is to mark all BATs used as a constant, because that permits
 * access to all properties. However, this creates unnecessary locking during stack
 * initialization. Therfore, we store the BAT id as a property for the optimizer
 * to work with. It can pick up the BAT if needed.
 *
 * Care should be taken in marking the delta bats as empty, because their
 * purpose is to fill them during the query. Therefore, we keep track
 * of all bound tables and mark them not-empty when a direct update
 * takes place using append().
 *
 * We also reduce the number of bind operations by keeping track
 * of those seen already.  This can not be handled by the
 * common term optimizer, because the first bind has a side-effect.
 */

static lng SQLgetStatistics(Client cntxt, mvc *m, MalBlkPtr mb)
{
	InstrPtr *old = NULL;
	int oldtop, i, actions = 0, size = 0;
	lng clk = GDKusec();
	sql_trans *tr = m->session->tr;
	str msg;
	lng space = 0; // sum the total amount of data potentially read

	old = mb->stmt;
	oldtop = mb->stop;
	size = (mb->stop * 1.2 < mb->ssize) ? mb->ssize : (int) (mb->stop * 1.2);
	mb->stmt = (InstrPtr *) GDKzalloc(size * sizeof(InstrPtr));
	mb->ssize = size;
	mb->stop = 0;

	for (i = 0; i < oldtop; i++) {
		InstrPtr p = old[i];
		char *f = getFunctionId(p);

		if (getModuleId(p) == sqlRef && f == tidRef) {
			char *sname = getVarConstant(mb, getArg(p, 2)).val.sval;
			char *tname = getVarConstant(mb, getArg(p, 3)).val.sval;
			sql_schema *s = mvc_bind_schema(m, sname);
			sql_table *t;

			if (!s || strcmp(s->base.name, dt_schema) == 0) {
				pushInstruction(mb, p);
				continue;
			}

		       	t = mvc_bind_table(m, s, tname);

			if (t && (!isRemote(t) && !isMergeTable(t)) && t->p) {
				int mt_member = t->p->base.id;
				setMitosisPartition(p,mt_member);
			}
		}
		if (getModuleId(p) == sqlRef && (f == bindRef || f == bindidxRef)) {
			int upd = (p->argc == 7 || p->argc == 9);
			char *sname = getVarConstant(mb, getArg(p, 2 + upd)).val.sval;
			char *tname = getVarConstant(mb, getArg(p, 3 + upd)).val.sval;
			char *cname = NULL;

			int mt_member = 0;
			BUN rows = 1;	/* default to cope with delta bats */
			int mode = 0;
			int k = getArg(p, 0);
			sql_schema *s = mvc_bind_schema(m, sname);
			BAT *b;

			if (!s || strcmp(s->base.name, dt_schema) == 0) {
				pushInstruction(mb, p);
				continue;
			}
			cname = getVarConstant(mb, getArg(p, 4 + upd)).val.sval;
			mode = getVarConstant(mb, getArg(p, 5 + upd)).val.ival;

			if (s && f == bindidxRef && cname) {
				size_t cnt;
				sql_idx *i = mvc_bind_idx(m, s, cname);

				if (i && (!isRemote(i->t) && !isMergeTable(i->t))) {
					cnt = store_funcs.count_idx(tr, i, 1);
					assert(cnt <= (size_t) GDK_oid_max);
					b = store_funcs.bind_idx(m->session->tr, i, RDONLY);
					if (b) {
						cnt = BATcount(b);
						if( mode == 0) {
							space += getBatSpace(b);
						}
						BBPunfix(b->batCacheid);
					}
					rows = (BUN) cnt;
					if (i->t->p) 
						mt_member = i->t->p->base.id;
				}
			} else if (s && f == bindRef && cname) {
				size_t cnt;
				sql_table *t = mvc_bind_table(m, s, tname);
				sql_column *c = mvc_bind_column(m, t, cname);

				if (c && (!isRemote(c->t) && !isMergeTable(c->t))) {
					cnt = store_funcs.count_col(tr, c, 1);
					assert(cnt <= (size_t) GDK_oid_max);
					b = store_funcs.bind_col(m->session->tr, c, RDONLY);
					if (b) {
						cnt = BATcount(b);
						if( mode == 0) {
							space += getBatSpace(b);
						}
						BBPunfix(b->batCacheid);
					}
					rows = (BUN) cnt;
					if (c->t->p) 
						mt_member = c->t->p->base.id;
				}
			}
			//mnstr_printf(GDKerr, "#space estimate after %s.%s.%s mode %d "LLFMT"\n",sname,tname,cname, mode, space);
			if (rows > 1 && mode != RD_INS)
				setRowCnt(mb,k,rows);
			if (mt_member && mode != RD_INS)
				setMitosisPartition(p,mt_member);

			pushInstruction(mb, p);
		} else {
			pushInstruction(mb, p);
		}
	}
	GDKfree(old);
	msg = optimizerCheck(cntxt, mb, "optimizer.SQLgetstatistics", actions, GDKusec() - clk);
	if (msg)		/* what to do with an error? */
		GDKfree(msg);
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

	space = SQLgetStatistics(c, be->mvc, mb);
	if(space && (pipe == NULL || strcmp(pipe,"default_pipe")== 0)){
		if( space > (lng)(0.8 * MT_npages() * MT_pagesize()) ){
			pipe = "volcano_pipe";
			//mnstr_printf(GDKout, "#use volcano optimizer pipeline? "SZFMT"\n", space);
		}else
			pipe = "default_pipe";
	} else
		pipe = pipe? pipe: "default_pipe";
	msg = addOptimizerPipe(c, mb, pipe);
	if (msg)
		GDKfree(msg);	/* what to do with an error? */
	/* point queries do not require mitosis and dataflow */
	if (be->mvc->point_query) {
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
optimizeQuery(Client c)
{
	MalBlkPtr mb;
	backend *be;
	str msg = 0, pipe;

	be = (backend *) c->sqlcontext;
	assert(be && be->mvc);	/* SQL clients should always have their state set */
	pipe = getSQLoptimizer(be->mvc);

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
			msg = runMALDebugger(c, c->curprg);
			if (msg != MAL_SUCCEED)
				GDKfree(msg); /* ignore error */
		}
		return NULL;
	}
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
	return NULL;
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
