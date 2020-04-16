/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
	return msg;
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
			throw(SQL,"sql.prepare",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else
		argv = argvbuffer;

	if (pci->retc >= MAXARG){
		argrec = (ValRecord *) GDKmalloc(sizeof(ValRecord) * pci->retc);
		if( argrec == NULL){
			if( argv != argvbuffer)
				GDKfree(argv);
			throw(SQL,"sql.prepare",SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
		else 
			msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
		*m->errstr=0;
		return msg;
	}
	if (m->emode == m_execute && be->q->paramlen != m->argc)
		throw(SQL, "sql.prepare", SQLSTATE(42000) "EXEC called with wrong number of arguments: expected %d, got %d", be->q->paramlen, m->argc);
	MT_thread_setworking(m->query);
	// locate and inline the query template instruction
	mb = copyMalBlk(c->curprg->def);
	if (!mb) {
		MT_thread_setworking(NULL);
		throw(SQL, "sql.prepare", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	mb->history = c->curprg->def->history;
	c->curprg->def->history = 0;

	/* only consider a re-optimization when we are dealing with query templates */
	for ( i= 1; i < mb->stop;i++){
		p = getInstrPtr(mb,i);
		if( getFunctionId(p) &&  qc_isapreparedquerytemplate(getFunctionId(p) ) ){
			msg = SQLexecutePrepared(c, be, p->blk);
			freeMalBlk(mb);
			MT_thread_setworking(NULL);
			return msg;
		}
		if( getFunctionId(p) &&  p->blk && qc_isaquerytemplate(getFunctionId(p)) ) {
			mc = copyMalBlk(p->blk);
			if (!mc) {
				freeMalBlk(mb);
				MT_thread_setworking(NULL);
				throw(SQL, "sql.prepare", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			retc = p->retc;
			freeMalBlk(mb); // TODO can be factored out
			mb = mc;
			// declare the argument values as a constant
			// We use the knowledge that the arguments are first on the stack
			for (j = 0; j < m->argc; j++) {
				sql_subtype *pt = be->q->params + j;
				atom *arg = m->args[j];

				if (!atom_cast(m->sa, arg, pt)) {
					freeMalBlk(mb);
					MT_thread_setworking(NULL);
					throw(SQL, "sql.prepare", SQLSTATE(07001) "EXEC: wrong type for argument %d of " "query template : %s, expected %s", i + 1, atom_type(arg)->type->sqlname, pt->type->sqlname);
				}
				val= (ValPtr) &arg->data;
				if (VALcopy(&mb->var[j+retc].value, val) == NULL){
					freeMalBlk(mb);
					MT_thread_setworking(NULL);
					throw(MAL, "sql.prepare", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
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
	if( msg != MAL_SUCCEED){
		// freeMalBlk(mb);
		MT_thread_setworking(NULL);
		return msg;
	}
	mb->keephistory = FALSE;

	if (mb->errors){
		// freeMalBlk(mb);
		// mal block might be so broken free causes segfault
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
	// release the resources
	freeMalBlk(mb);
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
SQLstatementIntern(Client c, str *expr, str nme, bit execute, bit output, res_table **result)
{
	int status = 0, err = 0, oldvtop, oldstop = 1, inited = 0, label, ac, sizevars, topvars;
	mvc *o, *m;
	sql_var *vars;
	buffer *b;
	char *n, *mquery;
	bstream *bs;
	stream *buf;
	str msg = MAL_SUCCEED;
	backend *be, *sql = (backend *) c->sqlcontext;
	size_t len = strlen(*expr);

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
	/* hide query cache, this causes crashes in SQLtrans() due to uninitialized memory otherwise */
	m->qc = NULL;

	/* create private allocator */
	m->sa = NULL;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED) {
		if (inited)
			SQLresetClient(c);
		return msg;
	}
	status = m->session->status;

	m->type = Q_PARSE;
	be = sql;
	sql = backend_create(m, c);
	if( sql == NULL)
		throw(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	sql->output_format = be->output_format;
	if (!output) {
		sql->output_format = OFMT_NONE;
	}
	sql->depth++;
	// and do it again
	m->qc = NULL;
	m->caching = 0;
	m->user_id = m->role_id = USER_MONETDB;
	if (result)
		m->reply_size = -2; /* do not clean up result tables */

	/* mimic a client channel on which the query text is received */
	b = (buffer *) GDKmalloc(sizeof(buffer));
	if( b == NULL)
		throw(SQL,"sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	n = GDKmalloc(len + 1 + 1);
	if( n == NULL) {
		GDKfree(b);
		throw(SQL,"sql.statement", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	strncpy(n, *expr, len);
	n[len] = '\n';
	n[len + 1] = 0;
	len++;
	buffer_init(b, n, len);
	buf = buffer_rastream(b, "sqlstatement");
	if(buf == NULL) {
		buffer_destroy(b);//n and b will be freed by the buffer
		throw(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bs = bstream_create(buf, b->len);
	if(bs == NULL) {
		buffer_destroy(b);//n and b will be freed by the buffer
		throw(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	scanner_init(&m->scanner, bs, NULL);
	m->scanner.mode = LINE_N;
	bstream_next(m->scanner.rs);

	m->params = NULL;
	m->argc = 0;
	m->session->auto_commit = 0;
	if (!m->sa)
		m->sa = sa_create();
	if (!m->sa) {
		*m = *o;
		_DELETE(o);
		bstream_destroy(m->scanner.rs);
		throw(SQL,"sql.statement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/*
	 * System has been prepared to parse it and generate code.
	 * Scan the complete string for SQL statements, stop at the first error.
	 */
	c->sqlcontext = sql;
	while (msg == MAL_SUCCEED && m->scanner.rs->pos < m->scanner.rs->len) {
		sql_rel *r;

		if (!m->sa)
			m->sa = sa_create();
		if (!m->sa) {
			msg = createException(PARSE, "SQLparser",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto endofcompile;
		}
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
		if((msg = MSinitClientPrg(c, "user", nme)) != MAL_SUCCEED) {
			goto endofcompile;
		}
		oldvtop = c->curprg->def->vtop;
		oldstop = c->curprg->def->stop;
		r = sql_symbol2relation(m, m->sym);
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#SQLstatement:\n");
#endif
		scanner_query_processed(&(m->scanner));
		if ((err = mvc_status(m)) ) {
				if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
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
		be->depth++;
		if (backend_callinline(be, c) < 0 ||
		    backend_dumpstmt(be, c->curprg->def, r, 1, 1, NULL) < 0)
			err = 1;
		be->depth--;
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#SQLstatement:post-compile\n");
		printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_NAME | LIST_MAL_VALUE  |  LIST_MAL_MAPI);
#endif
		msg = SQLoptimizeFunction(c, c->curprg->def);

		if (err || c->curprg->def->errors || msg) {
			/* restore the state */
			char *error = NULL;
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, c->glb, oldvtop);
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
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout, "#result of sql.eval()\n");
		printFunction(c->fdout, c->curprg->def, 0, c->listing);
#endif

		if (!output)
			sql->out = NULL;	/* no output stream */
		be->depth++;
		if (execute)
			msg = SQLrun(c,be,m);
		be->depth--;
		MSresetInstructions(c->curprg->def, oldstop);
		freeVariables(c, c->curprg->def, NULL, oldvtop);
		sqlcleanup(m, 0);
		if (!execute)
			goto endofcompile;
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
	label = m->label;
	status = m->session->status;
	sizevars = m->sizevars;
	topvars = m->topvars;
	vars = m->vars;
	mquery = m->query;
	*m = *o;
	_DELETE(o);
	m->label = label;
	m->sizevars = sizevars;
	m->topvars = topvars;
	m->vars = vars;
	m->session->status = status;
	m->session->auto_commit = ac;
	m->query = mquery;
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
	if (m->type == Q_SCHEMA && m->qc != NULL)
		qc_clean(m->qc, false);
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

void
SQLdestroyResult(res_table *destroy)
{
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
	if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		return msg;
	if (!m->sa)
		m->sa = sa_create();
	if (!m->sa)
		return createException(SQL,"RAstatement",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	refs = sa_list(m->sa);
	rel = rel_read(m, *expr, &pos, refs);
	if (rel) {
		int oldvtop = c->curprg->def->vtop;
		int oldstop = c->curprg->def->stop;

		if (*opt && rel)
			rel = sql_processrelation(m, rel, 0);

		if ((msg = MSinitClientPrg(c, "user", "test")) != MAL_SUCCEED) {
			rel_destroy(rel);
			return msg;
		}

		/* generate MAL code, ignoring any code generation error */
		if (backend_callinline(b, c) < 0 ||
		    backend_dumpstmt(b, c->curprg->def, rel, 1, 1, NULL) < 0) {
			msg = createException(SQL,"RAstatement","Program contains errors"); // TODO: use macro definition.
		} else {
			SQLaddQueryToCache(c);
			msg = SQLoptimizeFunction(c,c->curprg->def);
		}
		rel_destroy(rel);
		if( msg == MAL_SUCCEED)
			msg = SQLrun(c,b,m);
		if (!msg) {
			resetMalBlk(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, NULL, oldvtop);
		}
		if (!msg)
			msg = mvc_commit(m, 0, NULL, false);
		else
			msg = mvc_rollback(m, 0, NULL, false);
	}
	return msg;
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
		m->sa = sa_create();
	if (!m->sa) {
		sqlcleanup(m, 0);
		return createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* keep copy of signature and relational expression */
	snprintf(buf, BUFSIZ, "%s %s", sig, expr);

	if (!stack_push_frame(m, NULL)) {
		sqlcleanup(m, 0);
		return createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	ops = sa_list(m->sa);
	while (sig && *sig && !isspace((unsigned char) *sig)) {
		char *vnme = sig, *tnme;
		char *p = strchr(++sig, (int)' ');
		int d,s,nr = -1;
		sql_subtype t;
		atom *a;

		*p++ = 0;
		/* vnme can be name or number */
		if (is_a_number(vnme+1))
			nr = strtol(vnme+1, NULL, 10);
		tnme = p;
		p = strchr(p, (int)'(');
		*p++ = 0;
		tnme = sa_strdup(m->sa, tnme);
		if (!tnme) {
			sqlcleanup(m, 0);
			return createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		d = strtol(p, &p, 10);
		p++; /* skip , */
		s = strtol(p, &p, 10);

		sql_find_subtype(&t, tnme, d, s);
		a = atom_general(m->sa, &t, NULL);
		/* the argument list may have holes and maybe out of order, ie
		 * don't use sql_add_arg, but special numbered version
		 * sql_set_arg(m, a, nr);
		 * */
		if (nr >= 0) { 
			append(ops, exp_atom_ref(m->sa, nr, &t));
			if (!sql_set_arg(m, nr, a)) {
				sqlcleanup(m, 0);
				return createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		} else {
			if (!stack_push_var(m, vnme+1, &t)) {
				sqlcleanup(m, 0);
				return createException(SQL,"RAstatement2",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			append(ops, exp_var(m->sa, sa_strdup(m->sa, vnme+1), &t, m->frame));
		}
		sig = strchr(p, (int)',');
		if (sig)
			sig++;
	}
	refs = sa_list(m->sa);
	rel = rel_read(m, expr, &pos, refs);
	stack_pop_frame(m);
	if (rel)
		rel = sql_processrelation(m, rel, 1);
	if (!rel) {
		if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
			msg = createException(SQL, "RAstatement2", "%s", m->errstr);
		else
			msg = createException(SQL, "RAstatement2", SQLSTATE(42000) "%s", m->errstr);
	} else if (rel && types && is_simple_project(rel->op)) { /* Test if types match */
		list *types_list = sa_list(m->sa);
		str token, rest;

		for (token = strtok_r(types, "%%", &rest); token; token = strtok_r(NULL, "%%", &rest))
			list_append(types_list, token);

		if (list_length(types_list) != list_length(rel->exps))
			msg = createException(SQL, "RAstatement2", SQLSTATE(42000) "The number of projections don't match between the generated plan and the expected one: %d != %d", 
								  list_length(types_list), list_length(rel->exps));
		else {
			int i = 1;
			for (node *n = rel->exps->h, *m = types_list->h ; n && m && !msg ; n = n->next, m = m->next) {
				sql_exp *e = (sql_exp *) n->data;
				sql_subtype *t = exp_subtype(e);
				str got = subtype2string(t), expected = (str) m->data;

				if (!got)
					msg = createException(SQL, "RAstatement2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				else if (strcmp(expected, got) != 0)
					msg = createException(SQL, "RAstatement2", SQLSTATE(42000) "Parameter %d has wrong SQL type, expected %s, but got %s instead", i, expected, got);
				GDKfree(got);
				i++;
			}
		}
	}
	if (!msg && monet5_create_relational_function(m, mod, nme, rel, NULL, ops, 0) < 0)
		msg = createException(SQL, "RAstatement2", "%s", m->errstr);
	rel_destroy(rel);
	sqlcleanup(m, 0);
	return msg;
}
