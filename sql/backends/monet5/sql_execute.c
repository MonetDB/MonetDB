/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * SQL execution
 * N. Nes, M.L. Kersten
 */
/*
 * Execution of SQL instructions.
 * Before we are can process SQL statements the global catalog should be initialized.
 */
#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_scenario.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include "sql_assert.h"
#include "sql_execute.h"
#include "sql_env.h"
#include "sql_mvc.h"
#include "sql_user.h"
#include "sql_optimizer.h"
#include "sql_datetime.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_dump.h"
#include "gdk_time.h"
#include "optimizer.h"
#include "opt_inline.h"
#include <unistd.h>

/* #define _SQL_COMPILE */

/*
* BEWARE: SQLstatementIntern only commits after all statements found
* in expr are executed, when autocommit mode is enabled.
*
* The tricky part for this statement is to ensure that the SQL statement
* is executed within the client context specified. This leads to context juggling.
*/

/*
 * The trace operation collects the events in the BATs
 * and creates a secondary result set upon termination
 * of the query.
 *
 * SQLsetTrace extends the MAL plan with code to collect the events.
 * from the profile cache and returns it as a secondary resultset.
 */
static str
SQLsetTrace(Client cntxt, MalBlkPtr mb)
{
	InstrPtr q, resultset;
	InstrPtr tbls, cols, types, clen, scale;
	str msg = MAL_SUCCEED;
	int k;

	if((msg = startTrace(cntxt)) != MAL_SUCCEED)
		return msg;
	clearTrace(cntxt);

	for(k= mb->stop-1; k>0; k--)
		if( getInstrPtr(mb,k)->token ==ENDsymbol)
			break;
	mb->stop=k;

	q= newStmt(mb, profilerRef, stoptraceRef);
	if (q == NULL) {
		throw(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	pushInstruction(mb, q);

	/* cook a new resultSet instruction */
	resultset = newInstruction(mb,sqlRef, resultSetRef);
	if (resultset == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	getArg(resultset,0) = newTmpVariable(mb, TYPE_int);

	/* build table defs */
	tbls = newStmt(mb,batRef, newRef);
	if (tbls == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	setVarType(mb, getArg(tbls,0), newBatType(TYPE_str));
	tbls = pushType(mb, tbls, TYPE_str);
	pushInstruction(mb, tbls);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q,getArg(tbls,0));
	q= pushStr(mb,q,".trace");
	k= getArg(q,0);
	pushInstruction(mb, q);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q,k);
	q= pushStr(mb,q,".trace");
	pushInstruction(mb, q);

	resultset= pushArgument(mb,resultset, getArg(q,0));

	/* build colum defs */
	cols = newStmt(mb,batRef, newRef);
	if (cols == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	setVarType(mb, getArg(cols,0), newBatType(TYPE_str));
	cols = pushType(mb, cols, TYPE_str);
	pushInstruction(mb, cols);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q,getArg(cols,0));
	q= pushStr(mb,q,"usec");
	k= getArg(q,0);
	pushInstruction(mb, q);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q, k);
	q= pushStr(mb,q,"statement");
	pushInstruction(mb, q);

	resultset= pushArgument(mb,resultset, getArg(q,0));

	/* build type defs */
	types = newStmt(mb,batRef, newRef);
	if (types == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	setVarType(mb, getArg(types,0), newBatType(TYPE_str));
	types = pushType(mb, types, TYPE_str);
	pushInstruction(mb, types);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q, getArg(types,0));
	q= pushStr(mb,q,"bigint");
	k= getArg(q,0);
	pushInstruction(mb, q);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q, k);
	q= pushStr(mb,q,"clob");
	pushInstruction(mb, q);

	resultset= pushArgument(mb,resultset, getArg(q,0));

	/* build scale defs */
	clen = newStmt(mb,batRef, newRef);
	if (clen == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	setVarType(mb, getArg(clen,0), newBatType(TYPE_int));
	clen = pushType(mb, clen, TYPE_int);
	pushInstruction(mb, clen);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q, getArg(clen,0));
	q= pushInt(mb,q,64);
	k= getArg(q,0);
	pushInstruction(mb, q);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q, k);
	q= pushInt(mb,q,0);
	pushInstruction(mb, q);

	resultset= pushArgument(mb,resultset, getArg(q,0));

	/* build scale defs */
	scale = newStmt(mb,batRef, newRef);
	if (scale == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	setVarType(mb, getArg(scale,0), newBatType(TYPE_int));
	scale = pushType(mb, scale, TYPE_int);
	pushInstruction(mb, scale);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb,q, getArg(scale,0));
	q= pushInt(mb,q,0);
	k= getArg(q,0);
	pushInstruction(mb, q);

	q= newStmt(mb,batRef,appendRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q= pushArgument(mb, q, k);
	q= pushInt(mb,q,0);
	pushInstruction(mb, q);

	resultset= pushArgument(mb,resultset, getArg(q,0));

	/* add the ticks column */

	q = newStmt(mb, profilerRef, getTraceRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q = pushStr(mb, q, putName("usec"));
	resultset= pushArgument(mb,resultset, getArg(q,0));
	pushInstruction(mb, q);

	/* add the stmt column */
	q = newStmt(mb, profilerRef, getTraceRef);
	if (q == NULL) {
		msg = createException(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	q = pushStr(mb, q, putName("stmt"));
	resultset= pushArgument(mb,resultset, getArg(q,0));
	pushInstruction(mb, q);

	pushInstruction(mb,resultset);
	pushEndInstruction(mb);
	if( msg == MAL_SUCCEED)
		msg = chkTypes(cntxt->usermodule, mb, TRUE);
	renameVariables(mb);
	return msg;
}

str
SQLrun(Client c, mvc *m)
{
	str msg = MAL_SUCCEED;
	MalBlkPtr mb = c->curprg->def;

	assert(!*m->errstr);

	TRC_INFO(SQL_EXECUTION, "Executing: %s", c->query);
	MT_thread_setworking(c->query);

	if (m->emod & mod_explain) {
		if (c->curprg->def)
			printFunction(c->fdout, mb, 0, LIST_MAL_NAME | LIST_MAL_VALUE  | LIST_MAL_TYPE |  LIST_MAL_MAPI);
	} else {
		if (m->emod & mod_trace){
			if((msg = SQLsetTrace(c,mb)) == MAL_SUCCEED) {
				setVariableScope(mb);
				MT_lock_set(&mal_contextLock);
				c->idle = 0;
				c->lastcmd = time(0);
				MT_lock_unset(&mal_contextLock);
				msg = runMAL(c, mb, 0, 0);
				stopTrace(c);
			}
		} else {
			setVariableScope(mb);
			MT_lock_set(&mal_contextLock);
			c->idle = 0;
			c->lastcmd = time(0);
			MT_lock_unset(&mal_contextLock);
			msg = runMAL(c, mb, 0, 0);
		}
		resetMalBlk(mb);
	}
	/* after the query has been finished we enter the idle state */
	MT_lock_set(&mal_contextLock);
	c->idle = time(0);
	c->lastcmd = 0;
	MT_lock_unset(&mal_contextLock);
	MT_thread_setworking(NULL);
	return msg;
}

/*
 * Escape single quotes and backslashes. This is important to do before calling
 * SQLstatementIntern, if we are pasting user provided strings into queries
 * passed to the SQLstatementIntern. Otherwise we open ourselves to SQL
 * injection attacks.
 *
 * It returns the input string with all the single quotes(') and the backslashes
 * (\) doubled, or NULL, if it could not allocate enough space.
 *
 * The caller is responsible to free the returned value.
 */
str
SQLescapeString(str s)
{
	str ret = NULL;
	char *p, *q;
	size_t len = 0;

	if(!s) {
		return NULL;
	}

	/* At most we will need 2*strlen(s) + 1 characters */
	len = strlen(s);
	ret = (str)GDKmalloc(2*len + 1);
	if (!ret) {
		return NULL;
	}

	for (p = s, q = ret; *p != '\0'; p++, q++) {
		*q = *p;
		if (*p == '\'') {
			*(++q) = '\'';
		}
		else if (*p == '\\') {
			*(++q) = '\\';
		}
	}

	*q = '\0';
	return ret;
}

str
SQLstatementIntern(Client c, const char *expr, const char *nme, bit execute, bit output, res_table **result)
{
	int status = 0, err = 0, oldvtop, oldstop = 1, oldvid, inited = 0, ac, sizeframes, topframes;
	unsigned int label;
	mvc *o = NULL, *m = NULL;
	sql_frame **frames;
	list *global_vars;
	buffer *b = NULL;
	char *n = NULL;
	bstream *bs = NULL;
	stream *buf = NULL;
	str msg = MAL_SUCCEED;
	backend *be = NULL, *sql = (backend *) c->sqlcontext;
	Symbol backup = NULL;
	size_t len = strlen(expr);

	if (!sql) {
		inited = 1;
		msg = SQLinitClient(c, NULL, NULL, NULL);
		sql = (backend *) c->sqlcontext;
	}
	if (msg){
		freeException(msg);
		throw(SQL, "sql.statement", SQLSTATE(HY002) "Catalogue not available");
	}

	m = sql->mvc;
	ac = m->session->auto_commit;
	o = MNEW(mvc);
	if (!o) {
		if (inited) {
			msg = SQLresetClient(c);
			freeException(msg);
		}
		throw(SQL, "sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	*o = *m;

	/* create private allocator */
	m->sa = NULL;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED) {
		be = sql;
		sql = NULL;
		goto endofcompile;
	}
	status = m->session->status;

	m->type = Q_PARSE;
	be = sql;
	sql = backend_create(m, c);
	if (sql == NULL) {
		msg = createException(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto endofcompile;
	}
	sql->output_format = be->output_format;
	if (!output) {
		sql->output_format = OFMT_NONE;
	}
	sql->depth++;

	m->user_id = m->role_id = USER_MONETDB;
	if (result)
		m->reply_size = -2; /* do not clean up result tables */

	/* mimic a client channel on which the query text is received */
	b = malloc(sizeof(buffer));
	if (b == NULL) {
		msg = createException(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto endofcompile;
	}
	n = malloc(len + 1 + 1);
	if (n == NULL) {
		msg = createException(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto endofcompile;
	}
	strcpy_len(n, expr, len + 1);
	n[len] = '\n';
	n[len + 1] = 0;
	len++;
	buffer_init(b, n, len);
	buf = buffer_rastream(b, "sqlstatement");
	if (buf == NULL) {
		buffer_destroy(b); /* n and b will be freed by the buffer */
		b = NULL;
		msg = createException(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto endofcompile;
	}
	bs = bstream_create(buf, b->len);
	if (bs == NULL) {
		mnstr_destroy(buf);
		buffer_destroy(b);
		b = NULL;
		msg = createException(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto endofcompile;
	}
	scanner_init(&m->scanner, bs, NULL);
	m->scanner.mode = LINE_N;
	bstream_next(m->scanner.rs);

	m->params = NULL;
	m->session->auto_commit = 0;
	if (!m->sa && !(m->sa = sa_create(m->pa)) ) {
		msg = createException(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto endofcompile;
	}

	/*
	 * System has been prepared to parse it and generate code.
	 * Scan the complete string for SQL statements, stop at the first error.
	 */
	c->sqlcontext = sql;
	if (c->curprg) {
		backup = c->curprg;
		c->curprg = NULL;
	}

	while (msg == MAL_SUCCEED && m->scanner.rs->pos < m->scanner.rs->len) {
		sql_rel *r;

		m->sym = NULL;
		if ((err = sqlparse(m)) ||
		    /* Only forget old errors on transaction boundaries */
		    (mvc_status(m) && m->type != Q_TRANS) || !m->sym) {
			if (!err)
				err = mvc_status(m);
			if (*m->errstr){
				if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
					msg = createException(PARSE, "SQLparser", "%s", m->errstr);
				else
					msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
				*m->errstr = 0;
			}
			sqlcleanup(sql, err);
			execute = 0;
			if (!err)
				continue;
			goto endofcompile;
		}

		/*
		 * We have dealt with the first parsing step and advanced the input reader
		 * to the next statement (if any).
		 * Now is the time to also perform the semantic analysis,
		 * optimize and produce code.
		 * We don't search the cache for a previous incarnation yet.
		 */
		if((msg = MSinitClientPrg(c, sql_private_module_name, nme)) != MAL_SUCCEED) {
			goto endofcompile;
		}
		oldvtop = c->curprg->def->vtop;
		oldstop = c->curprg->def->stop;
		oldvid = c->curprg->def->vid;
		r = sql_symbol2relation(sql, m->sym);

		assert(m->emode != m_prepare);
		scanner_query_processed(&(m->scanner));
		if ((err = mvc_status(m)) ) {
			if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
				msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			else
				msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
			*m->errstr=0;
			msg = handle_error(m, status, msg);
			sqlcleanup(sql, err);
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, c->glb, oldvtop, oldvid);
			c->curprg->def->errors = 0;
			goto endofcompile;
		}
		/* generate MAL code */
		be->depth++;
		setVarType(c->curprg->def, 0, 0);
		if (backend_dumpstmt(be, c->curprg->def, r, 1, 1, NULL) < 0)
			err = 1;
		be->depth--;

		if (err == 0) {
			if (msg == MAL_SUCCEED)
				msg = SQLoptimizeQuery(c, c->curprg->def);
			if (msg)
				err = 1;
		}

		if (err) {
			status = -10;
			if (msg)
				msg = handle_error(m, status, msg);
			sqlcleanup(sql, err);
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, c->glb, oldvtop, oldvid);
			c->curprg->def->errors = 0;
			goto endofcompile;
		}

		if (execute) {
			if (!output)
				sql->out = NULL;	/* no output stream */
			be->depth++;
			msg = SQLrun(c,m);
			be->depth--;
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, NULL, oldvtop, oldvid);
			sqlcleanup(sql, 0);
			if (!execute)
				goto endofcompile;
		}
		if (sql->results) {
			if (result) { /* return all results sets */
				*result = sql->results;
			} else {
				if (sql->results == be->results)
					be->results = NULL;
				res_tables_destroy(sql->results);
			}
			sql->results = NULL;
		}
	}
/*
 * We are done; a MAL procedure resides in the cache.
 */
endofcompile:
	if (execute)
		MSresetInstructions(c->curprg->def, 1);

	if (backup)
		c->curprg = backup;

	c->sqlcontext = be;
	backend_destroy(sql);
	buffer_destroy(b);
	bstream_destroy(m->scanner.rs);
	if (m->sa)
		sa_destroy(m->sa);
	m->sa = NULL;
	m->sym = NULL;
	m->runs = NULL;
	/* variable stack maybe resized, ie we need to keep the new stack */
	label = m->label;
	status = m->session->status;
	global_vars = m->global_vars;
	sizeframes = m->sizeframes;
	topframes = m->topframes;
	frames = m->frames;
	*m = *o;
	_DELETE(o);
	m->label = label;
	m->global_vars = global_vars;
	m->sizeframes = sizeframes;
	m->topframes = topframes;
	m->frames = frames;
	m->session->status = status;
	m->session->auto_commit = ac;
	if (inited) {
		str other = SQLresetClient(c);
		freeException(other);
	}
	return msg;
}

str
SQLengineIntern(Client c, backend *be)
{
	str msg = MAL_SUCCEED;
	//char oldlang = be->language;
	mvc *m = be->mvc;

	assert (m->emode != m_deallocate && m->emode != m_prepare);
	assert (c->curprg->def->stop > 2);

	//be->language = 'D';
	if (MALcommentsOnly(c->curprg->def))
		msg = MAL_SUCCEED;
	else
		msg = SQLrun(c,m);

	if (m->type == Q_SCHEMA && m->qc != NULL)
		qc_clean(m->qc);
	be->q = NULL;
	if (msg)
		m->session->status = -10;
	sqlcleanup(be, (!msg) ? 0 : -1);
	MSresetInstructions(c->curprg->def, 1);
	freeVariables(c, c->curprg->def, NULL, be->vtop, be->vid);
	//be->language = oldlang;
	/*
	 * Any error encountered during execution should block further processing
	 * unless auto_commit has been set.
	 */
	return msg;
}

void
SQLdestroyResult(res_table *destroy)
{
	res_table_destroy(destroy);
}

static str
RAcommit_statement(backend *be, str msg)
{
	mvc *m = be->mvc;
	/* if an error already exists set the session status to dirty */
	if (msg != MAL_SUCCEED && m->session->tr->active && !m->session->status)
		m->session->status = -1;
	return msg;
}

/* a hook is provided to execute relational algebra expressions */
str
RAstatement(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pos = 0;
	str *expr = getArgReference_str(stk, pci, 1);
	bit *opt = getArgReference_bit(stk, pci, 2);
	backend *be = NULL;
	mvc *m = NULL;
	str msg = MAL_SUCCEED;
	sql_rel *rel;
	list *refs;

	if ((msg = getSQLContext(c, mb, &m, &be)) != NULL)
		return msg;
	if ((msg = checkSQLContext(c)) != NULL)
		return msg;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		return msg;
	if (!m->sa)
		m->sa = sa_create(m->pa);
	if (!m->sa)
		return RAcommit_statement(be, createException(SQL,"RAstatement",SQLSTATE(HY013) MAL_MALLOC_FAIL));
	refs = sa_list(m->sa);
	rel = rel_read(m, *expr, &pos, refs);
	if (*opt && rel)
		rel = sql_processrelation(m, rel, 0, 0, 0, 0);
	if (!rel) {
		if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
			msg = createException(SQL, "RAstatement", "%s", m->errstr);
		else
			msg = createException(SQL, "RAstatement", SQLSTATE(42000) "%s", m->errstr);
	} else {
		if ((msg = MSinitClientPrg(c, sql_private_module_name, "test")) != MAL_SUCCEED)
			return RAcommit_statement(be, msg);

		/* generate MAL code, ignoring any code generation error */
		setVarType(c->curprg->def, 0, 0);
		if (backend_dumpstmt(be, c->curprg->def, rel, 0, 1, NULL) < 0) {
			msg = createException(SQL,"RAstatement","Program contains errors"); // TODO: use macro definition.
		} else {
			SQLaddQueryToCache(c);
			msg = SQLoptimizeFunction(c,c->curprg->def);
			if( msg == MAL_SUCCEED)
				msg = SQLrun(c,m);
			resetMalBlk(c->curprg->def);
			SQLremoveQueryFromCache(c);
		}
		rel_destroy(rel);
	}
	return RAcommit_statement(be, msg);
}

static char *
parseIdent(char *in, char *out)
{
	while (*in && *in != '"') {
		if (*in == '\\' && (*(in + 1) == '\\' || *(in + 1) == '"')) {
			*out++ = *(in + 1);
			in+=2;
		} else {
			*out++ = *in++;
		}
	}
	*out++ = '\0';
	return in;
}

struct local_var_entry {
	char *vname;
	sql_subtype tpe;
} local_var_entry;

struct global_var_entry {
	char *vname;
	sql_schema *s;
} global_var_entry;

static str
RAstatement2_return(backend *be, mvc *m, int nlevels, struct global_var_entry *gvars, int gentries, str msg)
{
	while (nlevels) { /* clean added frames */
		stack_pop_frame(m);
		nlevels--;
	}
	for (int i = 0 ; i < gentries ; i++) { /* clean any added global variables */
		struct global_var_entry gv = gvars[i];
		(void) remove_global_var(m, gv.s, gv.vname);
	}
	sa_reset(m->ta);
	return RAcommit_statement(be, msg);
}

str
RAstatement2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pos = 0, nlevels = 0, *lkeys = NULL, lcap = 0, lentries = 0, gcap = 0, gentries = 0;
	str mod = *getArgReference_str(stk, pci, 1);
	str nme = *getArgReference_str(stk, pci, 2);
	str expr = *getArgReference_str(stk, pci, 3);
	str sig = *getArgReference_str(stk, pci, 4);
	str types = pci->argc == 6 ? *getArgReference_str(stk, pci, 5) : NULL;
	backend *be = NULL;
	mvc *m = NULL;
	str msg = MAL_SUCCEED;
	sql_rel *rel;
	list *refs, *ops;
	struct local_var_entry *lvars = NULL;
	struct global_var_entry *gvars = NULL;

	if ((msg = getSQLContext(cntxt, mb, &m, &be)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		return msg;
	if (!m->sa)
		m->sa = sa_create(m->pa);
	if (!m->sa)
		return RAstatement2_return(be, m, nlevels, gvars, gentries, createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL));

	ops = sa_list(m->sa);
	while (sig && *sig) {
		sql_schema *sh = NULL;
		sql_type *t = NULL;
		sql_subtype tpe;
		char *p, *vtype = NULL, *sch = NULL, *var = NULL;
		int d, s, level = strtol(sig, &p, 10);

		var = p+1;
		assert(*p == '"');
		p = parseIdent(p+1, var);
		p++;
		if (*p == '"') { /* global variable, parse schema and name */
			sch = var;
			var = p+1;
			p = parseIdent(p+1, var);
			p++;
		}

		assert(*p == ' ');
		p++; /* skip space and get type */
		vtype = p;
		p = strchr(p, '(');
		*p++ = '\0';

		/* get digits and scale */
		d = strtol(p, &p, 10);
		p++; /* skip , */
		s = strtol(p, &p, 10);
		p+=2; /* skip ) and , or ' ' */
		sig = p;

		if (!sql_find_subtype(&tpe, vtype, d, s)) {
			if (!(t = mvc_bind_type(m, vtype))) /* try an external type */
				return RAstatement2_return(be, m, nlevels, gvars, gentries, createException(SQL,"RAstatement2",SQLSTATE(42000) "SQL type %s(%d, %d) not found\n", vtype, d, s));
			sql_init_subtype(&tpe, t, d, s);
		}

		if (sch) {
			assert(level == 0);
			if (!(sh = mvc_bind_schema(m, sch)))
				return RAstatement2_return(be, m, nlevels, gvars, gentries, createException(SQL,"RAstatement2",SQLSTATE(3F000) "No such schema '%s'", sch));
			if (!find_global_var(m, sh, var)) { /* don't add the same global variable again */
				if (!push_global_var(m, sch, var, &tpe)) /* if doesn't exist, add it, then remove it before returning */
					return RAstatement2_return(be, m, nlevels, gvars, gentries, createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL));
				if (gentries == gcap) {
					if (gcap == 0) {
						gcap = 8;
						gvars = SA_NEW_ARRAY(m->ta, struct global_var_entry, gcap);
					} else {
						int ngcap = gcap * 4;
						gvars = SA_RENEW_ARRAY(m->ta, struct global_var_entry, gvars, ngcap, gcap);
						gcap = ngcap;
					}
					gvars[gentries++] = (struct global_var_entry) {.s = sh, .vname = var,};
				}
			}
			list_append(ops, exp_var(m->sa, sa_strdup(m->sa, sch), sa_strdup(m->sa, var), &tpe, 0));
		} else {
			char opname[BUFSIZ];

			if (lentries == lcap) {
				if (lcap == 0) {
					lcap = 8;
					lkeys = SA_NEW_ARRAY(m->ta, int, lcap);
					lvars = SA_NEW_ARRAY(m->ta, struct local_var_entry, lcap);
				} else {
					int nlcap = lcap * 4;
					lkeys = SA_RENEW_ARRAY(m->ta, int, lkeys, nlcap, lcap);
					lvars = SA_RENEW_ARRAY(m->ta, struct local_var_entry, lvars, nlcap, lcap);
					lcap = nlcap;
				}
			}
			lkeys[lentries] = level;
			lvars[lentries] = (struct local_var_entry) {.tpe = tpe, .vname = var,};
			lentries++;

			snprintf(opname, BUFSIZ, "%d%%%s", level, var); /* engineering trick */
			list_append(ops, exp_var(m->sa, NULL, sa_strdup(m->sa, opname), &tpe, level));
		}
	}
	if (lentries) {
		GDKqsort(lkeys, lvars, NULL, lentries, sizeof(int), sizeof(struct local_var_entry), TYPE_int, false, false);

		for (int i = 0 ; i < lentries ; i++) {
			int next_level = lkeys[i];
			struct local_var_entry next_val = lvars[i];

			assert(next_level != 0); /* no global variables here */
			while (nlevels < next_level) { /* add gap levels */
				if (!stack_push_frame(m, NULL))
					return RAstatement2_return(be, m, nlevels, gvars, gentries, createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL));
				nlevels++;
			}
			if (!frame_push_var(m, next_val.vname, &next_val.tpe))
				return RAstatement2_return(be, m, nlevels, gvars, gentries, createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL));
		}
	}

	refs = sa_list(m->sa);
	rel = rel_read(m, expr, &pos, refs);
	if (rel)
		rel = sql_processrelation(m, rel, 0, 0, 0, 0);
	if (!rel) {
		if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
			msg = createException(SQL, "RAstatement2", "%s", m->errstr);
		else
			msg = createException(SQL, "RAstatement2", SQLSTATE(42000) "%s", m->errstr);
	} else if (rel && types && is_simple_project(rel->op)) { /* Test if types match */
		list *types_list = sa_list(m->sa);
		str token, rest;

		for (token = strtok_r(types, "%", &rest); token; token = strtok_r(NULL, "%", &rest))
			list_append(types_list, token);

		if (list_length(types_list) != list_length(rel->exps))
			msg = createException(SQL, "RAstatement2", SQLSTATE(42000) "The number of projections don't match between the generated plan and the expected one: %d != %d",
								  list_length(types_list), list_length(rel->exps));
		else {
			int i = 1;
			for (node *n = rel->exps->h, *m = types_list->h ; n && m && !msg ; n = n->next, m = m->next) {
				sql_exp *e = (sql_exp *) n->data;
				sql_subtype *t = exp_subtype(e);
				str got = sql_subtype_string(be->mvc->ta, t), expected = (str) m->data;

				if (!got)
					msg = createException(SQL, "RAstatement2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				else if (strcmp(expected, got) != 0)
					msg = createException(SQL, "RAstatement2", SQLSTATE(42000) "Parameter %d has wrong SQL type, expected %s, but got %s instead", i, expected, got);
				i++;
			}
		}
	}
	if (!msg && monet5_create_relational_function(m, mod, nme, rel, NULL, ops, 0) < 0)
		msg = createException(SQL, "RAstatement2", "%s", m->errstr);
	rel_destroy(rel);
	return RAstatement2_return(be, m, nlevels, gvars, gentries, msg);
}

str
RAstatementEnd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be = NULL;
	mvc *m = NULL;
	str msg = MAL_SUCCEED;

	(void) stk;
	(void) pci;
	if ((msg = getSQLContext(cntxt, mb, &m, &be)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sqlcleanup(be, 0);
	return SQLautocommit(m);
}
