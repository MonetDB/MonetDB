/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "rel_partition.h"
#include "rel_distribute.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_dump.h"
#include "mal_debugger.h"
#include "gdk_time.h"
#include "optimizer.h"
#include "opt_inline.h"
#include <unistd.h>

/*
 * The SQLcompile operation can be used by separate
 * front-ends to benefit from the SQL functionality.
 * It expects a string and returns the name of the
 * corresponding MAL block as it is known in the
 * SQL_cache, where it can be picked up.
 * The SQLstatement operation also executes the instruction upon request.
 *
 * In both cases the SQL string is handled like an ordinary
 * user query, following the same optimization paths and
 * caching.
 */

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

	/* cook a new resultSet instruction */
	resultset = newInstruction(mb,sqlRef, resultSetRef);
	getArg(resultset,0) = newTmpVariable(mb, TYPE_int);

	/* build table defs */
	tbls = newStmt(mb,batRef, newRef);
	setVarType(mb, getArg(tbls,0), newBatType(TYPE_str));
	tbls = pushType(mb, tbls, TYPE_str);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q,getArg(tbls,0));
	q= pushStr(mb,q,".trace");
	k= getArg(q,0);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q,k);
	q= pushStr(mb,q,".trace");

	resultset= addArgument(mb,resultset, getArg(q,0));

	/* build colum defs */
	cols = newStmt(mb,batRef, newRef);
	setVarType(mb, getArg(cols,0), newBatType(TYPE_str));
	cols = pushType(mb, cols, TYPE_str);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q,getArg(cols,0));
	q= pushStr(mb,q,"usec");
	k= getArg(q,0);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q, k);
	q= pushStr(mb,q,"statement");

	resultset= addArgument(mb,resultset, getArg(q,0));

	/* build type defs */
	types = newStmt(mb,batRef, newRef);
	setVarType(mb, getArg(types,0), newBatType(TYPE_str));
	types = pushType(mb, types, TYPE_str);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q, getArg(types,0));
	q= pushStr(mb,q,"bigint");
	k= getArg(q,0);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q, k);
	q= pushStr(mb,q,"clob");

	resultset= addArgument(mb,resultset, getArg(q,0));

	/* build scale defs */
	clen = newStmt(mb,batRef, newRef);
	setVarType(mb, getArg(clen,0), newBatType(TYPE_int));
	clen = pushType(mb, clen, TYPE_int);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q, getArg(clen,0));
	q= pushInt(mb,q,64);
	k= getArg(q,0);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q, k);
	q= pushInt(mb,q,0);

	resultset= addArgument(mb,resultset, getArg(q,0));

	/* build scale defs */
	scale = newStmt(mb,batRef, newRef);
	setVarType(mb, getArg(scale,0), newBatType(TYPE_int));
	scale = pushType(mb, scale, TYPE_int);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb,q, getArg(scale,0));
	q= pushInt(mb,q,0);
	k= getArg(q,0);

	q= newStmt(mb,batRef,appendRef);
	q= pushArgument(mb, q, k);
	q= pushInt(mb,q,0);

	resultset= addArgument(mb,resultset, getArg(q,0));

	/* add the ticks column */

	q = newStmt(mb, profilerRef, getTraceRef);
	q = pushStr(mb, q, putName("usec"));
	resultset= addArgument(mb,resultset, getArg(q,0));

	/* add the stmt column */
	q = newStmt(mb, profilerRef, getTraceRef);
	q = pushStr(mb, q, putName("stmt"));
	resultset= addArgument(mb,resultset, getArg(q,0));

	pushInstruction(mb,resultset);
	pushEndInstruction(mb);
	if( msg == MAL_SUCCEED)
		msg = chkTypes(cntxt->usermodule, mb, TRUE);
	renameVariables(mb);
	return msg;
}

static str
SQLrun(Client c, mvc *m)
{
	str msg= MAL_SUCCEED;
	MalBlkPtr mb=c->curprg->def;

	if (*m->errstr){
		if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
		else
			msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
		*m->errstr=0;
		return msg;
	}
	TRC_INFO(SQL_EXECUTION, "Executing: %s", c->query);
	MT_thread_setworking(c->query);
	// JIT optimize the SQL query using all current information
	// This include template constants, BAT sizes.
	if( m->emod & mod_debug)
		mb->keephistory = TRUE;
	msg = SQLoptimizeQuery(c, mb);
	if( msg != MAL_SUCCEED){
		MT_thread_setworking(NULL);
		return msg;
	}
	mb->keephistory = FALSE;

	if (mb->errors){
		msg = mb->errors;
		mb->errors = 0;
		MT_thread_setworking(NULL);
		return msg;
	}

	if (m->emod & mod_explain) {
		if (c->curprg->def)
			printFunction(c->fdout, mb, 0, LIST_MAL_NAME | LIST_MAL_VALUE  | LIST_MAL_TYPE |  LIST_MAL_MAPI);
	} else if( m->emod & mod_debug) {
		c->idle = 0;
		c->lastcmd = time(0);
		msg = runMALDebugger(c, mb);
	} else {
		if( m->emod & mod_trace){
			if((msg = SQLsetTrace(c,mb)) == MAL_SUCCEED) {
				c->idle = 0;
				c->lastcmd = time(0);
				msg = runMAL(c, mb, 0, 0);
				stopTrace(c);
			}
		} else {
			c->idle = 0;
			c->lastcmd = time(0);
			msg = runMAL(c, mb, 0, 0);
		}
	}
	/* after the query has been finished we enter the idle state */
	c->idle = time(0);
	c->lastcmd = 0;
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

#ifdef _SQL_COMPILE
	mnstr_printf(c->fdout, "#SQLstatement:%s\n", expr);
#endif
	if (!sql) {
		inited = 1;
		msg = SQLinitClient(c);
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
		if (inited)
			SQLresetClient(c);
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
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#SQLstatement:\n");
#endif
		if (m->emode != m_prepare) {

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
#ifdef _SQL_COMPILE
			mnstr_printf(c->fdout, "#SQLstatement:pre-compile\n");
			printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_NAME | LIST_MAL_VALUE  |  LIST_MAL_MAPI);
#endif
			be->depth++;
			setVarType(c->curprg->def, 0, 0);
			if (backend_dumpstmt(be, c->curprg->def, r, 1, 1, NULL) < 0)
				err = 1;
			be->depth--;
#ifdef _SQL_COMPILE
			mnstr_printf(c->fdout, "#SQLstatement:post-compile\n");
			printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_NAME | LIST_MAL_VALUE  |  LIST_MAL_MAPI);
#endif
		} else {
			// Do not directly execute prepared statements.
			execute = 0;

			if ((c->query = query_cleaned(m->sa, QUERY(m->scanner))) == NULL) {
				err = 1;
				msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}

			char *q_copy = sa_strdup(m->sa, c->query);

			be->q = NULL;
			if (!q_copy) {
				err = 1;
				msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			} else {
				be->q = qc_insert(m->qc, m->sa,	/* the allocator */
						  r,	/* keep relational query */
						  m->sym,	/* the sql symbol tree */
						  m->params,	/* the argument list */
						  m->type,	/* the type of the statement */
						  q_copy,
						  be->no_mitosis);
			}
			if (!be->q) {
				err = 1;
				msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			scanner_query_processed(&(m->scanner));
			if (be->q && backend_dumpproc(be, c, be->q, r) < 0)
				err = 1;

			/* passed over to query cache, used during dumpproc */
			m->sa = NULL;
			m->sym = NULL;
			m->params = NULL;
			/* register name in the namespace */
			if (be->q) {
				be->q->name = putName(be->q->name);
				if (!be->q->name) {
					err = 1;
					msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}

				if (!err)
					err = mvc_export_prepare(be, c->fdout, "");
			}
		}

		if (err) {
			status = -10;
			if (msg) {
				msg = handle_error(m, status, msg);
			}
			sqlcleanup(sql, err);
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, c->glb, oldvtop, oldvid);
			c->curprg->def->errors = 0;
			goto endofcompile;
		}

		if (execute) {

			/*

			msg = SQLoptimizeFunction(c, c->curprg->def);

			if (c->curprg->def->errors || msg) {
				// restore the state
				char *error = NULL;
				MSresetInstructions(c->curprg->def, oldstop);
				freeVariables(c, c->curprg->def, c->glb, oldvtop, oldvid);
				c->curprg->def->errors = 0;
				if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
					error = createException(PARSE, "SQLparser", "%s", m->errstr);
				else if (*m->errstr)
					error = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
				else
					error = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", msg);
				if (msg)
					freeException(msg);
				msg = error;
				*m->errstr = 0;
				goto endofcompile;
			}
			*/
#ifdef _SQL_COMPILE
			mnstr_printf(c->fdout, "#result of sql.eval()\n");
			printFunction(c->fdout, c->curprg->def, 0, c->listing);
#endif

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
#ifdef _SQL_COMPILE
			mnstr_printf(c->fdout, "#parse/execute result %d\n", err);
#endif
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
	if (inited)
		SQLresetClient(c);
	return msg;
}

str
SQLengineIntern(Client c, backend *be)
{
	str msg = MAL_SUCCEED;
	char oldlang = be->language;
	mvc *m = be->mvc;

	if (oldlang == 'X') {	/* return directly from X-commands */
		sqlcleanup(be, 0);
		c->query = NULL;
		return MAL_SUCCEED;
	}

	if (c->curprg->def->stop == 1) {
		if (mvc_status(m)) {
			if (*m->errstr){
				if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
					msg = createException(PARSE, "SQLparser", "%s", m->errstr);
				else
					msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
				*m->errstr = 0;
			}
			goto cleanup_engine;
		}
		sqlcleanup(be, 0);
		c->query = NULL;
		return MAL_SUCCEED;
	}

	if (m->emode == m_deallocate || m->emode == m_prepare)
		goto cleanup_engine;

	be->language = 'D';
	/*
	 * The code below is copied from MALengine, which handles execution
	 * in the context of a user global environment. We have a private
	 * environment.
	 */
	if (MALcommentsOnly(c->curprg->def))
		msg = MAL_SUCCEED;
	else
		msg = SQLrun(c,m);

cleanup_engine:
	if (m->emode != m_deallocate && m->emode != m_prepare && m->type == Q_SCHEMA && m->qc != NULL)
		qc_clean(m->qc);
	if (msg) {
		/* don't print exception decoration, just the message */
/*
		char *n = NULL;
		char *o = msg;
		while ((n = strchr(o, '\n')) != NULL) {
			*n = '\0';
			mnstr_printf(c->fdout, "!%s\n", getExceptionMessage(o));
			*n++ = '\n';
			o = n;
		}
		if (*o != 0)
			mnstr_printf(c->fdout, "!%s\n", getExceptionMessage(o));
*/
		m->session->status = -10;
	}

	if (m->type != Q_SCHEMA && be->q && msg) {
		qc_delete(m->qc, be->q);
	}
	be->q = NULL;
	sqlcleanup(be, (!msg) ? 0 : -1);
	MSresetInstructions(c->curprg->def, 1);
	freeVariables(c, c->curprg->def, NULL, be->vtop, be->vid);
	be->language = oldlang;
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
	if (!rel) {
		if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
			msg = createException(SQL, "RAstatement", "%s", m->errstr);
		else
			msg = createException(SQL, "RAstatement", SQLSTATE(42000) "%s", m->errstr);
	} else {
		int oldvtop = c->curprg->def->vtop, oldstop = c->curprg->def->stop, oldvid = c->curprg->def->vid;

		if (*opt && rel)
			rel = sql_processrelation(m, rel, 1, 1);

		if ((msg = MSinitClientPrg(c, sql_private_module_name, "test")) != MAL_SUCCEED) {
			rel_destroy(rel);
			return RAcommit_statement(be, msg);
		}

		/* generate MAL code, ignoring any code generation error */
		setVarType(c->curprg->def, 0, 0);
		if (backend_dumpstmt(be, c->curprg->def, rel, 0, 1, NULL) < 0) {
			msg = createException(SQL,"RAstatement","Program contains errors"); // TODO: use macro definition.
		} else {
			SQLaddQueryToCache(c);
			msg = SQLoptimizeFunction(c,c->curprg->def);
		}
		rel_destroy(rel);
		if( msg == MAL_SUCCEED)
			msg = SQLrun(c,m);
		if (!msg) {
			resetMalBlk(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, NULL, oldvtop, oldvid);
		}
	}
	return RAcommit_statement(be, msg);
}

static int
is_a_number(char *v)
{
	while(*v) {
		if (!isdigit((unsigned char) *v))
			return 0;
		v++;
	}
	return 1;
}

str
RAstatement2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pos = 0;
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
	char buf[BUFSIZ];

	if ((msg = getSQLContext(cntxt, mb, &m, &be)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		return msg;
	if (!m->sa)
		m->sa = sa_create(m->pa);
	if (!m->sa)
		return RAcommit_statement(be, createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL));

	/* keep copy of signature and relational expression */
	snprintf(buf, BUFSIZ, "%s %s", sig, expr);

	if (!stack_push_frame(m, NULL))
		return RAcommit_statement(be, createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL));
	ops = sa_list(m->sa);
	while (sig && *sig && !isspace((unsigned char) *sig)) {
		char *vnme = sig, *tnme;
		char *p = strchr(++sig, (int)' ');
		int d,s,nr = -1;
		sql_subtype t;
		//atom *a;

		assert(0);

		*p++ = 0;
		/* vnme can be name or number */
		if (is_a_number(vnme+1))
			nr = strtol(vnme+1, NULL, 10);
		tnme = p;
		p = strchr(p, (int)'(');
		*p++ = 0;
		tnme = sa_strdup(m->sa, tnme);
		if (!tnme)
			return RAcommit_statement(be, createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL));
		d = strtol(p, &p, 10);
		p++; /* skip , */
		s = strtol(p, &p, 10);

		sql_find_subtype(&t, tnme, d, s);
		//a = atom_general(m->sa, &t, NULL);
		//a->isnull = 0; // disable NULL value optimizations ugh
		/* the argument list may have holes and maybe out of order, ie
		 * don't use sql_add_arg, but special numbered version
		 * sql_set_arg(m, a, nr);
		 * */
		if (nr >= 0) {
			append(ops, exp_atom_ref(m->sa, nr, &t));
			//if (!sql_set_arg(m, nr, a)) {
			//	sqlcleanup(be, 0);
			//	return createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			//}
		} else {
			if (!push_global_var(m, "sys", vnme+1, &t))
				return RAcommit_statement(be, createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL));
			append(ops, exp_var(m->sa, NULL, sa_strdup(m->sa, vnme+1), &t, 0));
		}
		sig = strchr(p, (int)',');
		if (sig)
			sig++;
	}
	refs = sa_list(m->sa);
	rel = rel_read(m, expr, &pos, refs);
	stack_pop_frame(m);
	if (rel)
		rel = sql_processrelation(m, rel, 1, 1);
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
	return RAcommit_statement(be, msg);
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
