/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
#include <sql_optimizer.h>
#include <sql_datetime.h>
#include <rel_optimizer.h>
#include <rel_partition.h>
#include <rel_distribute.h>
#include <rel_select.h>
#include <rel_rel.h>
#include <rel_exp.h>
#include <rel_dump.h>
#include "mal_debugger.h"
#include <mtime.h>
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
 */
static void
SQLsetTrace(Client cntxt, MalBlkPtr mb)
{
	InstrPtr q, resultset;
	InstrPtr tbls, cols, types, clen, scale;
	int k;

	startTrace("sql_traces");
	clearTrace();

	for(k= mb->stop-1; k>0; k--)
		if( getInstrPtr(mb,k)->token ==ENDsymbol)
			break;
	mb->stop=k;

	q= newStmt(mb, profilerRef, stoptraceRef);
	q= pushStr(mb,q,"sql_traces");

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

	resultset= pushArgument(mb,resultset, getArg(q,0));

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

	resultset= pushArgument(mb,resultset, getArg(q,0));

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

	resultset= pushArgument(mb,resultset, getArg(q,0));

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

	resultset= pushArgument(mb,resultset, getArg(q,0));

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

	resultset= pushArgument(mb,resultset, getArg(q,0));

	/* add the ticks column */

	q = newStmt(mb, profilerRef, "getTrace");
	q = pushStr(mb, q, putName("usec"));
	resultset= pushArgument(mb,resultset, getArg(q,0));

	/* add the stmt column */
	q = newStmt(mb, profilerRef, "getTrace");
	q = pushStr(mb, q, putName("stmt"));
	resultset= pushArgument(mb,resultset, getArg(q,0));

	pushInstruction(mb,resultset);
	pushEndInstruction(mb);
	chkTypes(cntxt->usermodule, mb, TRUE);
}

/*
 * Execution of the SQL program is delegated to the MALengine.
 * Different cases should be distinguished. The default is to
 * hand over the MAL block derived by the parser for execution.
 * However, when we received an Execute call, we make a shortcut
 * and prepare the stack for immediate execution
 */
static str
SQLexecutePrepared(Client c, backend *be, MalBlkPtr mb)
{
	mvc *m = be->mvc;
	int argc, parc;
	ValPtr *argv, argvbuffer[MAXARG], v;
	ValRecord *argrec, argrecbuffer[MAXARG];
	MalStkPtr glb;
	InstrPtr pci;
	int i;
	str ret;
	cq *q= be->q;

	pci = getInstrPtr(mb, 0);
	if (pci->argc >= MAXARG){
		argv = (ValPtr *) GDKmalloc(sizeof(ValPtr) * pci->argc);
		if( argv == NULL)
			throw(SQL,"sql.prepare",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	} else
		argv = argvbuffer;

	if (pci->retc >= MAXARG){
		argrec = (ValRecord *) GDKmalloc(sizeof(ValRecord) * pci->retc);
		if( argrec == NULL){
			if( argv != argvbuffer)
				GDKfree(argv);
			throw(SQL,"sql.prepare",SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	} else
		argrec = argrecbuffer;

	/* prepare the target variables */
	for (i = 0; i < pci->retc; i++) {
		argv[i] = argrec + i;
		argv[i]->vtype = getVarGDKType(mb, i);
	}

	argc = m->argc;
	parc = q->paramlen;

	if (argc != parc) {
		if (pci->argc >= MAXARG && argv != argvbuffer)
			GDKfree(argv);
		if (pci->retc >= MAXARG && argrec != argrecbuffer)
			GDKfree(argrec);
		throw(SQL, "sql.prepare", SQLSTATE(07001) "EXEC: wrong number of arguments for prepared statement: %d, expected %d", argc, parc);
	} else {
		for (i = 0; i < m->argc; i++) {
			atom *arg = m->args[i];
			sql_subtype *pt = q->params + i;

			if (!atom_cast(m->sa, arg, pt)) {
				/*sql_error(c, 003, buf); */
				if (pci->argc >= MAXARG && argv != argvbuffer)
					GDKfree(argv);
				if (pci->retc >= MAXARG && argrec != argrecbuffer)
					GDKfree(argrec);
				throw(SQL, "sql.prepare", SQLSTATE(07001) "EXEC: wrong type for argument %d of " "prepared statement: %s, expected %s", i + 1, atom_type(arg)->type->sqlname, pt->type->sqlname);
			}
			argv[pci->retc + i] = &arg->data;
		}
	}
	glb = (MalStkPtr) (q->stk);
	ret = callMAL(c, mb, &glb, argv, (m->emod & mod_debug ? 'n' : 0));
	/* cleanup the arguments */
	for (i = pci->retc; i < pci->argc; i++) {
		garbageElement(c, v = &glb->stk[pci->argv[i]]);
		v->vtype = TYPE_int;
		v->val.ival = int_nil;
	}
	q->stk = (backend_stack) glb; /* save garbageCollected stack */
	if (glb && SQLdebug & 1)
		printStack(GDKstdout, mb, glb);
	if (pci->argc >= MAXARG && argv != argvbuffer)
		GDKfree(argv);
	if (pci->retc >= MAXARG && argrec != argrecbuffer)
		GDKfree(argrec);
	return ret;
}

static str
SQLrun(Client c, backend *be, mvc *m)
{
	str msg= MAL_SUCCEED;
	MalBlkPtr mc = 0, mb=c->curprg->def;
	InstrPtr p=0;
	int i,j, retc;
	ValPtr val;
			
	if (*m->errstr){
		if( strstr(m->errstr,"SQLSTATE"))
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
		else 
			msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
		*m->errstr=0;
		return msg;
	}
	// locate and inline the query template instruction
	mb = copyMalBlk(c->curprg->def);
	if (!mb) {
		throw(SQL, "sql.prepare", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	mb->history = c->curprg->def->history;
	c->curprg->def->history = 0;

	/* only consider a re-optimization when we are dealing with query templates */
	for ( i= 1; i < mb->stop;i++){
		p = getInstrPtr(mb,i);
		if( getFunctionId(p) &&  qc_isapreparedquerytemplate(getFunctionId(p) ) ){
			msg = SQLexecutePrepared(c, be, p->blk);
			freeMalBlk(mb);
			return msg;
		}
		if( getFunctionId(p) &&  p->blk && qc_isaquerytemplate(getFunctionId(p)) ) {
			mc = copyMalBlk(p->blk);
			if (!mc) {
				throw(SQL, "sql.prepare", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			retc = p->retc;
			freeMalBlk(mb);
			mb = mc;
			// declare the argument values as a constant
			// We use the knowledge that the arguments are first on the stack
			for (j = 0; j < m->argc; j++) {
				sql_subtype *pt = be->q->params + j;
				atom *arg = m->args[j];
				
				if (!atom_cast(m->sa, arg, pt)) {
					throw(SQL, "sql.prepare", SQLSTATE(07001) "EXEC: wrong type for argument %d of " "query template : %s, expected %s", i + 1, atom_type(arg)->type->sqlname, pt->type->sqlname);
				}
				val= (ValPtr) &arg->data;
				if (VALcopy(&mb->var[j+retc].value, val) == NULL)
					throw(MAL, "sql.prepare", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				setVarConstant(mb, j+retc);
				setVarFixed(mb, j+retc);
			}
			mb->stmt[0]->argc = 1;
			break;
		}
	}
	// JIT optimize the SQL query using all current information
	// This include template constants, BAT sizes.
	if( m->emod & mod_debug)
		mb->keephistory = TRUE;
	msg = SQLoptimizeQuery(c, mb);
	mb->keephistory = FALSE;

	if (mb->errors){
		//freeMalBlk(mb);
		// mal block might be so broken free causes segfault
		return msg;
	}

	if (m->emod & mod_explain) {
		if (c->curprg->def)
			printFunction(c->fdout, mb, 0, LIST_MAL_NAME | LIST_MAL_VALUE  |  LIST_MAL_MAPI);
	} else if( m->emod & mod_debug) {
		msg = runMALDebugger(c, mb);
	} else {
		if( m->emod & mod_trace){
			SQLsetTrace(c,mb);
			msg = runMAL(c, mb, 0, 0);
			stopTrace(0);
		} else {
			msg = runMAL(c, mb, 0, 0);
		}
	}

	// release the resources
	freeMalBlk(mb);
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
SQLstatementIntern(Client c, str *expr, str nme, bit execute, bit output, res_table **result)
{
	int status = 0;
	int err = 0;
	mvc *o, *m;
	int ac, sizevars, topvars;
	sql_var *vars;
	int oldvtop, oldstop = 1;
	buffer *b;
	char *n;
	stream *buf;
	str msg = MAL_SUCCEED;
	backend *be, *sql = (backend *) c->sqlcontext;
	size_t len = strlen(*expr);
	int inited = 0;

#ifdef _SQL_COMPILE
	mnstr_printf(c->fdout, "#SQLstatement:%s\n", *expr);
#endif
	if (!sql) {
		inited = 1;
		msg = SQLinitClient(c);
		sql = (backend *) c->sqlcontext;
	}
	if (msg){
		freeException(msg);
		throw(SQL, "SQLstatement", SQLSTATE(HY002) "Catalogue not available");
	}

	initSQLreferences();
	m = sql->mvc;
	ac = m->session->auto_commit;
	o = MNEW(mvc);
	if (!o) {
		if (inited)
			SQLresetClient(c);
		throw(SQL, "SQLstatement", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	*o = *m;
	/* hide query cache, this causes crashes in SQLtrans() due to uninitialized memory otherwise */
	m->qc = NULL;

	/* create private allocator */
	m->sa = NULL;
	SQLtrans(m);
	status = m->session->status;

	m->type = Q_PARSE;
	be = sql;
	sql = backend_create(m, c);
	if( sql == NULL)
		throw(SQL,"SQLstatement",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	sql->output_format = be->output_format;
	if (!output) {
		sql->output_format = OFMT_NONE;
	}
	// and do it again
	m->qc = NULL;
	m->caching = 0;
	m->user_id = m->role_id = USER_MONETDB;
	if (result)
		m->reply_size = -2; /* do not clean up result tables */

	/* mimic a client channel on which the query text is received */
	b = (buffer *) GDKmalloc(sizeof(buffer));
	if( b == NULL)
		throw(SQL,"sql.statement", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	n = GDKmalloc(len + 1 + 1);
	if( n == NULL)
		throw(SQL,"sql.statement", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	strncpy(n, *expr, len);
	n[len] = '\n';
	n[len + 1] = 0;
	len++;
	buffer_init(b, n, len);
	buf = buffer_rastream(b, "sqlstatement");
	scanner_init(&m->scanner, bstream_create(buf, b->len), NULL);
	m->scanner.mode = LINE_N;
	bstream_next(m->scanner.rs);

	m->params = NULL;
	m->argc = 0;
	m->session->auto_commit = 0;
	if (!m->sa)
		m->sa = sa_create();

	/*
	 * System has been prepared to parse it and generate code.
	 * Scan the complete string for SQL statements, stop at the first error.
	 */
	c->sqlcontext = sql;
	while (msg == MAL_SUCCEED && m->scanner.rs->pos < m->scanner.rs->len) {
		sql_rel *r;

		if (!m->sa)
			m->sa = sa_create();
		m->sym = NULL;
		if ((err = sqlparse(m)) ||
		    /* Only forget old errors on transaction boundaries */
		    (mvc_status(m) && m->type != Q_TRANS) || !m->sym) {
			if (!err)
				err = mvc_status(m);
			if (*m->errstr){
				if( strstr(m->errstr,"SQLSTATE"))
					msg = createException(PARSE, "SQLparser", "%s", m->errstr);
				else
					msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
				*m->errstr = 0;
			}
			sqlcleanup(m, err);
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
		MSinitClientPrg(c, "user", nme);
		oldvtop = c->curprg->def->vtop;
		oldstop = c->curprg->def->stop;
		r = sql_symbol2relation(m, m->sym);
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#SQLstatement:\n");
#endif
		scanner_query_processed(&(m->scanner));
		if ((err = mvc_status(m)) ) {
				if( strstr(m->errstr,"SQLSTATE"))
					msg = createException(PARSE, "SQLparser", "%s", m->errstr);
				else
					msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
			*m->errstr=0;
			msg = handle_error(m, status, msg);
			sqlcleanup(m, err);
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, c->glb, oldvtop);
			c->curprg->def->errors = 0;
			goto endofcompile;
		}
		/* generate MAL code */
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#SQLstatement:pre-compile\n");
		printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_NAME | LIST_MAL_VALUE  |  LIST_MAL_MAPI);
#endif
		if (backend_callinline(be, c) < 0 ||
		    backend_dumpstmt(be, c->curprg->def, r, 1, 1, NULL) < 0)
			err = 1;
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#SQLstatement:post-compile\n");
		printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_NAME | LIST_MAL_VALUE  |  LIST_MAL_MAPI);
#endif
		msg = SQLoptimizeFunction(c, c->curprg->def);

		if (err || c->curprg->def->errors || msg) {
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, c->glb, oldvtop);
			c->curprg->def->errors = 0;
			if( strstr(m->errstr,"SQLSTATE"))
				msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			else
				msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
			*m->errstr = 0;
			goto endofcompile;
		}
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#result of sql.eval()\n");
		printFunction(c->fdout, c->curprg->def, 0, c->listing);
#endif

		if (!output)
			sql->out = NULL;	/* no output stream */
		if (execute)
			msg = SQLrun(c,be,m);
		MSresetInstructions(c->curprg->def, oldstop);
		freeVariables(c, c->curprg->def, NULL, oldvtop);

		sqlcleanup(m, 0);

		/* construct a mock result set to determine schema */
		if (!execute && result) {
			/* 'inspired' by mvc_export_prepare() */
			if (is_topn(r->op))
				r = r->l;
			if (r && is_project(r->op) && r->exps) {
				node *n;
				int ncol = 0;
				res_table *res;
				for (n = r->exps->h; n; n = n->next) ncol++;
				res = res_table_create(m->session->tr, m->result_id++, 0, ncol, 1, NULL, NULL);
				for (n = r->exps->h; n; n = n->next) {
					const char *name, *rname;
					sql_exp *e = n->data;
					sql_subtype *t = exp_subtype(e);
					void *ptr =ATOMnil(t->type->localtype);

					if( ptr == NULL){
						msg = createException(SQL,"SQLstatement", SQLSTATE(HY001) MAL_MALLOC_FAIL);
						goto endofcompile;
					}
					name = e->name;
					if (!name && e->type == e_column && e->r)
						name = e->r;
					rname = e->rname;
					if (!rname && e->type == e_column && e->l)
						rname = e->l;
					if (res_col_create(m->session->tr, res, rname, name, t->type->sqlname, t->digits,
							   t->scale, t->type->localtype, ptr) == NULL) {
						msg = createException(SQL,"SQLstatement", SQLSTATE(HY001) MAL_MALLOC_FAIL);
						goto endofcompile;
					}
				}
				*result = res;
			}
		}

		if (!execute) {
			goto endofcompile;
		}
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#parse/execute result %d\n", err);
#endif
	}
	if (m->results) {
		if (result) { /* return all results sets */
			*result = m->results;
		} else {
			if (m->results == o->results)
				o->results = NULL;
			res_tables_destroy(m->results);
		}
		m->results = NULL;
	}
/*
 * We are done; a MAL procedure resides in the cache.
 */
endofcompile:
	if (execute)
		MSresetInstructions(c->curprg->def, 1);

	c->sqlcontext = be;
	backend_destroy(sql);
	GDKfree(n);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);
	if (m->sa)
		sa_destroy(m->sa);
	m->sa = NULL;
	m->sym = NULL;
	/* variable stack maybe resized, ie we need to keep the new stack */
	status = m->session->status;
	sizevars = m->sizevars;
	topvars = m->topvars;
	vars = m->vars;
	*m = *o;
	_DELETE(o);
	m->sizevars = sizevars;
	m->topvars = topvars;
	m->vars = vars;
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
		sqlcleanup(be->mvc, 0);
		return MAL_SUCCEED;
	}

#ifdef SQL_SCENARIO_DEBUG
	fprintf(stderr, "#Ready to execute SQL statement\n");
#endif

	if (c->curprg->def->stop == 1) {
		if (mvc_status(m)) {
			if (*m->errstr){
				if( strstr(m->errstr,"SQLSTATE"))
					msg = createException(PARSE, "SQLparser", "%s", m->errstr);
				else
					msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
				*m->errstr = 0;
			}
			goto cleanup_engine;
		}
		sqlcleanup(be->mvc, 0);
		return MAL_SUCCEED;
	}

	if (m->emode == m_prepare)
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
		msg = SQLrun(c,be,m);

cleanup_engine:
	if (m->type == Q_SCHEMA)
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
	sqlcleanup(be->mvc, (!msg) ? 0 : -1);
	MSresetInstructions(c->curprg->def, 1);
	freeVariables(c, c->curprg->def, NULL, be->vtop);
	be->language = oldlang;
	/*
	 * Any error encountered during execution should block further processing
	 * unless auto_commit has been set.
	 */
	return msg;
}

void SQLdestroyResult(res_table *destroy) {
   res_table_destroy(destroy);
}

/* a hook is provided to execute relational algebra expressions */
str
RAstatement(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pos = 0;
	str *expr = getArgReference_str(stk, pci, 1);
	bit *opt = getArgReference_bit(stk, pci, 2);
	backend *b = NULL;
	mvc *m = NULL;
	str msg;
	sql_rel *rel;
	list *refs;

	if ((msg = getSQLContext(c, mb, &m, &b)) != NULL)
		return msg;
	if ((msg = checkSQLContext(c)) != NULL)
		return msg;
	if (!m->sa)
		m->sa = sa_create();
	refs = sa_list(m->sa);
	rel = rel_read(m, *expr, &pos, refs);
	if (rel) {
		int oldvtop = c->curprg->def->vtop;
		int oldstop = c->curprg->def->stop;

		if (*opt)
			rel = rel_optimizer(m, rel);

		MSinitClientPrg(c, "user", "test");

		/* generate MAL code, ignoring any code generation error */
		if (backend_callinline(b, c) < 0 ||
		    backend_dumpstmt(b, c->curprg->def, rel, 1, 1, NULL) < 0) {
			msg = createException(SQL,"RAstatement","Program contains errors");
		} else {
			SQLaddQueryToCache(c);
			msg = SQLoptimizeFunction(c,c->curprg->def);
		}
		rel_destroy(rel);
		SQLrun(c,b,m);
		if (!msg) {
			resetMalBlk(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, NULL, oldvtop);
		}
	}
	return msg;
}

str
RAstatement2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pos = 0;
	str *mod = getArgReference_str(stk, pci, 1);
	str *nme = getArgReference_str(stk, pci, 2);
	str *expr = getArgReference_str(stk, pci, 3);
	str *sig = getArgReference_str(stk, pci, 4), c = *sig;
	backend *be = NULL;
	mvc *m = NULL;
	str msg;
	sql_rel *rel;
	list *refs, *ops;
	char buf[BUFSIZ];

	if ((msg = getSQLContext(cntxt, mb, &m, &be)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (!m->sa)
		m->sa = sa_create();

	//fprintf(stderr, "#(%s){{%s}}\n", *sig, *expr);
	//fflush(stderr);
       	ops = sa_list(m->sa);
	snprintf(buf, BUFSIZ, "%s %s", *sig, *expr);
	while (c && *c && !isspace(*c)) {
		char *vnme = c, *tnme; 
		char *p = strchr(++c, (int)' ');
		int d,s,nr;
		sql_subtype t;
		atom *a;

		*p++ = 0;
		nr = strtol(vnme+1, NULL, 10);
		tnme = p;
		p = strchr(p, (int)'(');
		*p++ = 0;
		tnme = sa_strdup(m->sa, tnme);

		d = strtol(p, &p, 10);
		p++; /* skip , */
		s = strtol(p, &p, 10);
		
		sql_find_subtype(&t, tnme, d, s);
		a = atom_general(m->sa, &t, NULL);
		/* the argument list may have holes and maybe out of order, ie
		 * don't use sql_add_arg, but special numbered version
		 * sql_set_arg(m, a, nr);
		 * */
		append(ops, exp_atom_ref(m->sa, nr, &t));
		sql_set_arg(m, nr, a);
		c = strchr(p, (int)',');
		if (c)
			c++;
	}
	refs = sa_list(m->sa);
	rel = rel_read(m, *expr, &pos, refs);
	if (!rel || monet5_create_relational_function(m, *mod, *nme, rel, NULL, ops, 0) < 0)
		throw(SQL, "sql.register", SQLSTATE(42000) "Cannot register %s", buf);
	rel_destroy(rel);
	sqlcleanup(m, 0);
	return msg;
}
