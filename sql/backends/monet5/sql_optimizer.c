/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * N. Nes, M.L. Kersten
 * The queries are stored in the user cache after they have been
 * type checked and optimized.
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "mal_debugger.h"
#include "mal_runtime.h"
#include "opt_prelude.h"
#include "sql_mvc.h"
#include "sql_optimizer.h"
#include "sql_scenario.h"
#include "sql_gencode.h"
#include "opt_pipes.h"

/* calculate the footprint for optimizer pipe line choices
 * and identify empty columns upfront for just in time optimizers.
 */
static lng
SQLgetColumnSize(sql_trans *tr, sql_column *c, int access)
{
	lng size = 0;
	BAT *b;
	switch(access){
	case 0:
		b= store_funcs.bind_col(tr, c, RDONLY);
		if (b) {
			size += getBatSpace(b);
			BBPunfix(b->batCacheid);
		}
		break;
	case 1:
		b = store_funcs.bind_col(tr, c, RD_INS);
		if (b) {
			size+= getBatSpace(b);
			BBPunfix(b->batCacheid);
		}
		break;
	case 2:
		b = store_funcs.bind_col(tr, c, RD_UPD_VAL);
		if (b) {
			size += getBatSpace(b);
			BBPunfix(b->batCacheid);
		}
		b = store_funcs.bind_col(tr, c, RD_UPD_ID);
		if (b) {
			size+= getBatSpace(b);
			BBPunfix(b->batCacheid);
		}
	}
	return size;
}

static lng
SQLgetSpace(mvc *m, MalBlkPtr mb, int prepare)
{
	sql_trans *tr = m->session->tr;
	lng size,space = 0, i;

	for (i = 0; i < mb->stop; i++) {
		InstrPtr p = mb->stmt[i];

		/* now deal with the update binds, it is only necessary to identify that there are updats
		 * The actual size is not that important */
		if (getModuleId(p) == sqlRef && getFunctionId(p) == bindRef  && p->retc <= 2){
			char *sname = getVarConstant(mb, getArg(p, 1 + p->retc)).val.sval;
			char *tname = getVarConstant(mb, getArg(p, 2 + p->retc)).val.sval;
			char *cname = getVarConstant(mb, getArg(p, 3 + p->retc)).val.sval;
			int access = getVarConstant(mb, getArg(p, 4 + p->retc)).val.ival;
			sql_schema *s = mvc_bind_schema(m, sname);
			sql_table *t = 0;
			sql_column *c = 0;

			if (!s || strcmp(s->base.name, dt_schema) == 0)
				continue;
			t = mvc_bind_table(m, s, tname);
			if (!t)
				continue;
			c = mvc_bind_column(m, t, cname);
			if (!s)
				continue;

			/* we have to sum the cost of all three components of a BAT */
			if (c && (!isRemote(c->t) && !isMergeTable(c->t))) {
				size = SQLgetColumnSize(tr, c, access);
				space += size;	// accumulate once
				if( !prepare && size == 0  && ! t->system){
					//mnstr_printf(GDKout,"found empty column %s.%s.%s prepare %d size "LLFMT"\n",sname,tname,cname,prepare,size);
					setFunctionId(p, emptybindRef);
				}
			}
		}
		if (getModuleId(p) == sqlRef && (getFunctionId(p) == bindidxRef)) {
			char *sname = getVarConstant(mb, getArg(p, 1 + p->retc)).val.sval;
			//char *tname = getVarConstant(mb, getArg(p, 2 + p->retc)).val.sval;
			char *idxname = getVarConstant(mb, getArg(p, 3 + p->retc)).val.sval;
			int access = getVarConstant(mb, getArg(p, 4 + p->retc)).val.ival;
			sql_schema *s = mvc_bind_schema(m, sname);
			BAT *b;

			if (getFunctionId(p) == bindidxRef) {
				sql_idx *i = mvc_bind_idx(m, s, idxname);

				if (i && (!isRemote(i->t) && !isMergeTable(i->t))) {
					b = store_funcs.bind_idx(tr, i, RDONLY);
					if (b) {
						space += (size =getBatSpace(b));
						if (!size) {
							sql_column *c = i->t->columns.set->h->data;
							size = SQLgetColumnSize(tr, c, access);
						}

						if( !prepare && size == 0 && ! i->t->system){
							setFunctionId(p, emptybindidxRef);
							//mnstr_printf(GDKout,"found empty column %s.%s.%s prepare %d size "LLFMT"\n",sname,tname,idxname,prepare,size);
						}
						BBPunfix(b->batCacheid);
					}
				}
			}
		}
	}
	return space;
}

/* gather the optimizer pipeline defined in the current session */
str
getSQLoptimizer(mvc *m)
{
	char *opt = stack_get_string(m, "optimizer");
	char *pipe = "default_pipe";

	if (opt)
		pipe = opt;
	return pipe;
}

static str
addOptimizers(Client c, MalBlkPtr mb, char *pipe, int prepare)
{
	int i;
	InstrPtr q;
	backend *be;
	str msg= MAL_SUCCEED;
	lng space;

	be = (backend *) c->sqlcontext;
	assert(be && be->mvc);	/* SQL clients should always have their state set */

	space = SQLgetSpace(be->mvc, mb, prepare);
	if(space && (pipe == NULL || strcmp(pipe,"default_pipe")== 0)){
		/* for queries with a potential large footprint and running the default pipe line,
		 * we can switch to a better one. */
		if( space > (lng)(0.8 * MT_npages() * MT_pagesize())  && GDKnr_threads > 1){
			pipe = "volcano_pipe";
			//mnstr_printf(GDKout, "#use volcano optimizer pipeline? "SZFMT"\n", space);
		}else
			pipe = "default_pipe";
	} else
		pipe = pipe? pipe: "default_pipe";
	msg = addOptimizerPipe(c, mb, pipe);
	if (msg){
		return msg;
	}
	mb->keephistory |= be->mvc->emod & mod_debug;
	if (be->mvc->no_mitosis) {
		for (i = mb->stop - 1; i > 0; i--) {
			q = getInstrPtr(mb, i);
			if (q->token == ENDsymbol)
				break;
			if (getFunctionId(q) == mitosisRef || getFunctionId(q) == dataflowRef)
				q->token = REMsymbol;	/* they are ignored */
		}
	}
	addtoMalBlkHistory(mb);
	return msg;
}

/* Queries that should rely on the latest consolidated state
 * are not allowed to remove sql.binds operations.
 */

str
SQLoptimizeFunction(Client c, MalBlkPtr mb)
{
	str msg;
	str pipe;
	backend *be = (backend *) c->sqlcontext;
	assert(be && be->mvc);	/* SQL clients should always have their state set */

	pipe = getSQLoptimizer(be->mvc);
	msg = addOptimizers(c, mb, pipe, TRUE);
	if (msg)
		return msg;
	mb->keephistory |= be->mvc->emod & mod_debug;
	msg = optimizeMALBlock(c, mb);
	mb->keephistory = FALSE;
	return msg;
}

str
SQLoptimizeQuery(Client c, MalBlkPtr mb)
{
	backend *be;
	str msg = 0;
	str pipe;

	if (mb->stop > 0 &&
	    mb->stmt[mb->stop-1]->token == REMsymbol &&
	    mb->stmt[mb->stop-1]->argc > 0 &&
	    mb->var[mb->stmt[mb->stop-1]->argv[0]].value.vtype == TYPE_str &&
	    mb->var[mb->stmt[mb->stop-1]->argv[0]].value.val.sval &&
	    strncmp(mb->var[mb->stmt[mb->stop-1]->argv[0]].value.val.sval, "total", 5) == 0)
		return MAL_SUCCEED; /* already optimized */

	be = (backend *) c->sqlcontext;
	assert(be && be->mvc);	/* SQL clients should always have their state set */

	c->blkmode = 0;
	chkProgram(c->fdout, c->nspace, mb);

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

	pipe = getSQLoptimizer(be->mvc);
	msg = addOptimizers(c, mb, pipe, FALSE);
	if (msg)
		return msg;
	mb->keephistory |= be->mvc->emod & mod_debug;
	msg = optimizeMALBlock(c, mb);
	return msg;
}

/* queries are added to the MAL catalog  under the client session namespace */
void
SQLaddQueryToCache(Client c)
{
	insertSymbol(c->nspace, c->curprg);
}

