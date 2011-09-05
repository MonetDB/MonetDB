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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f sql_scenario
 * @t SQL catwalk management
 * @a N. Nes, M.L. Kersten
 * @+ SQL scenario
 * The SQL scenario implementation is a derivative of the MAL session scenario.
 *
 * It is also the first version that uses state records attached to
 * the client record. They are initialized as part of the initialization
 * phase of the scenario.
 *
 */
/*
 * @+ Scenario routines
 * Before we are can process SQL statements the global catalog
 * should be initialized. Thereafter, each time a client enters
 * we update its context descriptor to denote an SQL scenario.
 */
#include "monetdb_config.h"
#include "sql_scenario.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_env.h"
#include "sql_mvc.h"
#include "sql_readline.h"
#include "sql_user.h"
#include "mal_io.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_sabaoth.h"
#include "mal_debugger.h"
#include "mal_linker.h"
#include "bat5.h"
#include <mtime.h>
#include "optimizer.h"
#include "opt_statistics.h"
#include "opt_prelude.h"
#include <unistd.h>

static int SQLinitialized = 0;
static int SQLnewcatalog = 0;
static int SQLdebug = 0;
static char *sqlinit = NULL;
MT_Lock sql_contextLock;

static void
monet5_freestack(int clientid, backend_stack stk)
{
	MalStkPtr p = (ptr)stk;

	(void)clientid;
	if (p != NULL)
		freeStack(p);
#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#monet5_freestack\n");
#endif
}

static void
monet5_freecode(int clientid, backend_code code, backend_stack stk, int nr, char *name)
{
	(void) code;
	(void) stk;
	(void) nr;
	(void)clientid;
	SQLCacheRemove(MCgetClient(clientid), name);

#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#monet5_free:%d\n", nr);
#endif
}

str
SQLsession(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = (str*) getArgReference(stk, pci, 0);
	str msg = MAL_SUCCEED;

	(void)mb;
	if (SQLinitialized == 0 )
		SQLprelude();
	msg = setScenario(cntxt, "sql");
	*ret = 0;
	return msg;
}

str
SQLsession2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = (str*) getArgReference(stk, pci, 0);
	str msg = MAL_SUCCEED;

	(void)mb;
	if (SQLinitialized == 0 )
		SQLprelude();
	msg = setScenario(cntxt, "msql");
	*ret = 0;
	return msg;
}

static str SQLinit(void);

str
SQLprelude(void)
{
	int res = 0;
	str tmp;
	Scenario ms, s = getFreeScenario();
	if (!s)
		throw(MAL, "sql.start", "out of scenario slots");
	sqlinit = GDKgetenv("sqlinit");
	s->name = "S_Q_L";
	s->language = "sql";
	s->initSystem = NULL;
	s->exitSystem = "SQLexit";
	s->initClient = "SQLinitClient";
	s->exitClient = "SQLexitClient";
	s->reader = "SQLreader";
	s->parser = "SQLparser";
	s->engine = "SQLengine";
	if (GDKembedded) {
		tmp = SQLinit();
		if (tmp == MAL_SUCCEED)
			s->name = "sql";
		return tmp;
	}

	ms = getFreeScenario();
	if (!ms)
		throw(MAL, "sql.start", "out of scenario slots");

	ms->name = "M_S_Q_L";
	ms->language = "msql";
	ms->initSystem = NULL;
	ms->exitSystem = "SQLexit";
	ms->initClient = "SQLinitClient";
	ms->exitClient = "SQLexitClient";
	ms->reader = "MALreader";
	ms->parser = "MALparser";
	ms->optimizer = "MALoptimizer";
	/* ms->tactics = .. */
	ms->engine = "MALengine";
	fprintf(stdout, "# MonetDB/SQL module loaded\n");
	fflush(stdout); /* make merovingian see this *now* */
	tmp = SQLinit();
	if (tmp != MAL_SUCCEED)
		return(tmp);

	/* only register availability of scenarios AFTER we are inited! */
	s->name = "sql";
	tmp = SABAOTHmarchScenario(&res, &s->name);
	if (tmp != MAL_SUCCEED)
		return(tmp);
	ms->name = "msql";
	tmp = SABAOTHmarchScenario(&res, &ms->name);
	return tmp;
}

str
SQLepilogue(void)
{
	int res = 0;
	char *s = "sql";

	if( SQLinitialized){
		/*
		mvc_exit();
		SQLinitialized= FALSE;
		*/
	}
	/* this function is never called, but for the style of it, we clean
	 * up our own mess */
	if (!GDKembedded)
		return SABAOTHretreatScenario(&res, &s);
	return MAL_SUCCEED;
}

MT_Id sqllogthread, minmaxthread;

static str
SQLinit(void)
{
	char *debug_str = GDKgetenv("sql_debug");
	int readonly  = GDKgetenv_isyes("gdk_readonly");
	int single_user = GDKgetenv_isyes("gdk_single_user");
	char *gmt = "GMT", *dbname = NULL;
	tzone tz;

#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#SQLinit Monet 5\n");
#endif
	if (SQLinitialized)
		return MAL_SUCCEED;

	MT_lock_init( &sql_contextLock, "sql_contextLock");

	mal_set_lock(sql_contextLock,"SQL init");
	memset((char*)&be_funcs, 0, sizeof(backend_functions));
	be_funcs.fstack		= &monet5_freestack;
	be_funcs.fcode		= &monet5_freecode;
	be_funcs.fcreate_table_function	= &monet5_create_table_function;
	be_funcs.fresolve_function	= &monet5_resolve_function;
	monet5_user_init(&be_funcs);

	MTIMEtimezone(&tz, &gmt);
	(void) tz;
	if (debug_str)
		SQLdebug = strtol(debug_str,NULL,10);
	if (single_user)
		SQLdebug |= 64;
	if (readonly)
		SQLdebug |= 32;
	dbname = GDKgetenv("gdk_dbname");
	if (((SQLdebug&96)==96 && (SQLnewcatalog = mvc_init(dbname, FALSE, store_suro, 0)) < 0) ||
			((SQLdebug&96)==64 && (SQLnewcatalog = mvc_init(dbname, FALSE, store_su, 0)) < 0) ||
			((SQLdebug&96)==32 && (SQLnewcatalog = mvc_init(dbname, FALSE, store_ro, 0)) < 0) ||
			((SQLdebug&112)==0 && (SQLnewcatalog = mvc_init(dbname, FALSE, store_bat, 0)) < 0))
		throw(SQL, "SQLinit", "Catalogue initialization failed");
	SQLinitialized = TRUE;
	mal_unset_lock(sql_contextLock,"SQL init");
	if (!GDKembedded && MT_create_thread(&sqllogthread, (void (*)(void *)) mvc_logmanager, NULL, MT_THR_DETACHED) != 0) {
		mal_unset_lock(sql_contextLock,"SQL init");
		throw(SQL, "SQLinit", "Starting log manager failed");
	}
	if (0 && !GDKembedded && MT_create_thread(&minmaxthread, (void (*)(void *)) mvc_minmaxmanager, NULL, MT_THR_DETACHED) != 0) {
		mal_unset_lock(sql_contextLock,"SQL init");
		throw(SQL, "SQLinit", "Starting minmax manager failed");
	}
	return MAL_SUCCEED;
}

str
SQLexit(Client c)
{
#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#SQLexit\n");
#endif
	(void) c;		/* not used */
	if( SQLinitialized == FALSE)
		throw(SQL, "SQLexit", "Catalogue not available");
	return MAL_SUCCEED;
}

#define SQLglobal(name, val) \
	stack_push_var(sql, name, &ctype);	   \
	stack_set_var(sql, name, VALset(&src, ctype.type->localtype, val));

#define NR_GLOBAL_VARS 9
/* NR_GLOBAL_VAR should match exactly the number of variables created
   in global_variables */
/* initialize the global variable, ie make mvc point to these */
static int
global_variables(mvc *sql, char *user, char *schema)
{
	sql_subtype ctype;
	char *typename;
	lng sec = 0;
	bit T = TRUE;
	bit F = FALSE;
	ValRecord src;
	str optimizer;

 	typename = "int";
	sql_find_subtype(&ctype, typename, 0, 0);
	SQLglobal("debug", &sql->debug);

	typename = "varchar";
	sql_find_subtype(&ctype, typename, 1024, 0);
	SQLglobal("current_schema", schema);
	SQLglobal("current_user", user);
	SQLglobal("current_role", user);
	/* inherit the optimizer from the server */
	if (optimizerpipe )
		optimizer= optimizerpipe;
	else
		optimizer= GDKgetenv("sql_optimizer");
	if (optimizer == NULL)
		optimizer= "default_pipe";
	SQLglobal("optimizer", setOptimizers(optimizer));
	SQLglobal("trace","show,ticks,stmt");

	typename = "sec_interval";
	sql_find_subtype(&ctype, typename, 0, 0);
	SQLglobal("current_timezone", &sec);

	typename = "boolean";
	sql_find_subtype(&ctype, typename, 0, 0);
	SQLglobal("cache", &T);
	SQLglobal("history", &F);

	return 0;
}

static int
error(stream *out, char *str)
{
	char *p;

	if (!out)
		out = GDKerr;

	if (str == NULL)
		return 0;

	if (mnstr_errnr(out))
		return -1;
	while ((p = strchr(str, '\n')) != NULL) {
		p++;        /* include newline */
		if (*str != '!' && mnstr_write(out, "!", 1, 1) != 1)
			return -1;
		if (mnstr_write(out, str, p - str, 1) != 1)
			return -1;
		str = p;
	}
	if (str && *str) {
		if (*str != '!' && mnstr_write(out, "!", 1, 1) != 1)
			return -1;
		if (mnstr_write(out, str, strlen(str), 1) != 1 || mnstr_write(out, "\n", 1, 1) != 1)
			return -1;
	}
	return 0;
}

#define TRANS_ABORTED "!current transaction is aborted (please ROLLBACK)\n"

static int
handle_error(mvc *m, stream *out, int pstatus)
{
	int go = 1;
	char *buf = GDKerrbuf;

	/* transaction already broken */
	if (m->type != Q_TRANS && pstatus < 0) {
		if (mnstr_write(out, TRANS_ABORTED, sizeof(TRANS_ABORTED) - 1, 1) != 1) {
			go = !go;
		}
	} else {
		if (error(out, m->errstr) < 0 || (buf && buf[0] && error(out, buf) < 0)) {
			go = !go;
		}
	}
	/* reset error buffers */
	m->errstr[0] = 0;
	if (buf)
		buf[0] = 0;
	return go;
}


str
SQLinitClient(Client c)
{
	mvc *m;
	str schema;
	str msg = MAL_SUCCEED;
	backend *be;
	bstream *bfd = NULL;
	stream *fd = NULL;

#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#SQLinitClient\n");
#endif
	if (SQLinitialized == 0 )
		SQLprelude();
	/*
	 * @-
	 * Based on the initialization retun value we can prepare a SQLinit string with all information needed
	 * to initialize the catalog based on the mandatory scripts to be executed.
	 */
	if (sqlinit) { /* add sqlinit to the fdin stack */
		buffer *b = (buffer*)GDKmalloc(sizeof(buffer));
		size_t len = strlen(sqlinit);
		bstream *fdin;

		buffer_init(b, _strdup(sqlinit), len);
		fdin = bstream_create(buffer_rastream(b, "si"), b->len);
		bstream_next(fdin);
		MCpushClientInput(c, fdin, 0, "");
	}
	if (c->state[MAL_SCENARIO_PARSER] == 0) {
		m = mvc_create(c->idx, 0, SQLdebug, c->fdin, c->fdout);
		global_variables(m, "monetdb", "sys");
		if (isAdministrator(c))  /* console should return everything */
			m->reply_size = -1;
		be = (void *) backend_create(m, c);
	} else {
		m = c->state[MAL_SCENARIO_OPTIMIZE];
		be = c->state[MAL_SCENARIO_PARSER];
		mvc_reset(m, c->fdin, c->fdout, SQLdebug, NR_GLOBAL_VARS);
		backend_reset(be);
	}
	/* pass through credentials of the user if not console */
	schema = monet5_user_get_def_schema(m, c->user);
	if (!schema) {
		_DELETE(schema);
		throw(PERMD, "SQLinitClient", "schema authorization error");
	}
	_DELETE(schema);

	/*expect SQL text first */
	be->language = 'S';
	/* Set state, this indicates an initialized client scenario */
	c->state[MAL_SCENARIO_READER] = c;
	c->state[MAL_SCENARIO_PARSER] = be;
	c->state[MAL_SCENARIO_OPTIMIZE] = m;

	initSQLreferences();
	/* initialize the database with predefined SQL functions */
	if (SQLnewcatalog > 0) {
		char path[PATHLENGTH];
		str fullname;

		SQLnewcatalog = 0;
		snprintf(path, PATHLENGTH, "createdb");
		slash_2_dir_sep(path);
		fullname = MSP_locate_sqlscript(path, 1);
		if (fullname) {
			str filename = fullname;
			str p;
			fprintf(stdout, "# SQL catalog created, loading sql scripts once\n");
			do {
				p = strchr(filename, PATH_SEP);
				if (p)
					*p = '\0';
				fprintf(stdout, "# SQL loading sql scripts %s\n",filename);
				fd = open_rastream(filename);
				if (p)
					filename = p + 1;

				if (fd) {
					bfd = bstream_create(fd, 128 * BLOCK);
					if (bfd && bstream_next(bfd) >= 0)
						msg = SQLstatementIntern(c, &bfd->buf, "sql.init", TRUE, FALSE);
					bstream_destroy(bfd);
					if (m->sa)
						sa_destroy(m->sa);
					m->sa = NULL;
					if (msg)
						p = NULL;
				}
			} while (p);
			GDKfree(fullname);
		} else
			fprintf(stderr, "!could not read createdb.sql\n");
	}
	fflush(stdout);
	fflush(stderr);

	/* send error from create scripts back to the first client */
	if (msg) {
		error(c->fdout, msg);
		handle_error(m, c->fdout, 0);
		sqlcleanup(m, mvc_status(m));
	}
	return msg;
}

str
SQLexitClient(Client c)
{
#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#SQLexitClient\n");
#endif
	if (SQLinitialized == FALSE)
		throw(SQL, "SQLexitClient", "Catalogue not available");
	if (c->state[MAL_SCENARIO_PARSER] && c->state[MAL_SCENARIO_OPTIMIZE]) {
		mvc *m = (mvc *) c->state[MAL_SCENARIO_OPTIMIZE];
		if ( m == NULL)
			throw(SQL, "SQLexitClient", "MVC catalogue not available");

		assert(m->session);
		if (m->session->auto_commit && m->session->active) {
			if (mvc_status(m) >= 0 && mvc_commit(m, 0, NULL) < 0)
				(void) handle_error(m, c->fdout, 0);
		}
		if (m->session->active)
			mvc_rollback(m, 0, NULL);

		res_tables_destroy(m->results);
		m->results= NULL;

		/*
		if (m->session->tr)
			reset_functions(m->session->tr);
		*/
		{
			backend *be = c->state[MAL_SCENARIO_PARSER];

			mvc_destroy(m);
			backend_destroy(be);
			c->state[MAL_SCENARIO_OPTIMIZE] = NULL;
			c->state[MAL_SCENARIO_PARSER] = NULL;
		}
	}
	c->state[MAL_SCENARIO_READER] = NULL;
	return MAL_SUCCEED;
}

/*
 * @-
 * A statement received internally is simply appended for
 * execution
 */
str
SQLinitEnvironment(Client cntxt)
{
	return SQLinitClient(cntxt);
}
static void
SQLtrans(mvc *m)
{
	m->caching = m->cache;
	if (m && !m->session->active)
		mvc_trans(m);
}
/*
 * @-
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
BEWARE: SQLstatementIntern only commits after all statements found
in expr are executed, when autocommit mode is enabled.
*/
str
SQLstatementIntern(Client c, str *expr, str nme, int execute, bit output)
{
	int status = 0;
	int err = 0;
	mvc *o, *m;
	int ac, sizevars, topvars;
	sql_var *vars;
	buffer *b;
	char *n;
	stream *buf;
	str msg = MAL_SUCCEED;
	backend *be, *sql = ((backend *) c->state[MAL_SCENARIO_PARSER]);
	size_t len = strlen(*expr);

#ifdef _SQL_COMPILE
	mnstr_printf(c->fdout, "#SQLstatement:%s\n", *expr);
#endif
	if (!sql) {
		msg = SQLinitEnvironment(c);
		sql = ((backend *) c->state[MAL_SCENARIO_PARSER]);
	}
	if (msg)
		throw(SQL, "SQLstatement", "Catalogue not available");

	initSQLreferences();
	m = sql->mvc;
 	ac = m->session->auto_commit;
	o = NEW(mvc);
	if (!o)
		throw(SQL, "SQLstatement", "Out of memory");
	*o = *m;

	/* create private allocator */
	m->sa = NULL;
	SQLtrans(m);
	status = m->session->status;

	m->type= Q_PARSE;
	be = sql;
	sql = backend_create(m, c);
	m->qc = NULL;
	m->caching = 0;
	m->user_id = m->role_id = USER_MONETDB;

	b = (buffer*)GDKmalloc(sizeof(buffer));
	n = GDKmalloc(len + 1 + 1);
	strncpy(n, *expr, len);
	n[len] = '\n';
	n[len+1] = 0;
	len++;
	buffer_init(b, n, len);
	buf = buffer_rastream(b, "sqlstatement");
	scanner_init( &m->scanner, bstream_create(buf , b->len), NULL);
	m->scanner.mode = LINE_N;
	bstream_next(m->scanner.rs);

	m->params = NULL;
	m->argc = 0;
	m->session->auto_commit = 0;

	if (!m->sa)
		m->sa = sa_create();
	/*
	 * @-
	 * System has been prepared to parse it and generate code.
	 * Scan the complete string for SQL statements, stop at the first error.
	 */
	c->state[MAL_SCENARIO_PARSER] = sql;
	while( m->scanner.rs->pos < m->scanner.rs->len ){
		stmt *s;
		int oldvtop, oldstop;

		if (!m->sa)
			m->sa = sa_create();
		m->sym = NULL;
		if ( (err = sqlparse(m)) ||
			/* Only forget old errors on transaction boundaries */
			(mvc_status(m) && m->type != Q_TRANS) || !m->sym) {
			if (!err)
				err = mvc_status(m);
			if (m->errstr && *m->errstr)
				msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			*m->errstr = 0;
			sqlcleanup(m, err);
			execute = 0;
			if (!err)
				continue;
			goto endofcompile;
		}

		/*
		 * @-
		 * We have dealt with the first parsing step and advanced the input reader
		 * to the next statement (if any).
		 * Now is the time to also perform the semantic analysis,
		 * optimize and produce code.
		 * We don;t search the cache for a previous incarnation yet.
		 */
		MSinitClientPrg(c,"user",nme);
		oldvtop = c->curprg->def->vtop;
		oldstop = c->curprg->def->stop;
		s = sql_symbol2stmt(m, m->sym);
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout,"#SQLstatement:\n");
#endif
		scanner_query_processed(&(m->scanner));
		if (s==0 || (err = mvc_status(m))) {
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			handle_error(m, c->fdout, status);
			sqlcleanup(m, err);
			/* restore the state */
			resetMalBlk(c->curprg->def, oldstop);
			freeVariables(c,c->curprg->def, c->glb, oldvtop);
			c->curprg->def->errors = 0;
			goto endofcompile;
		}
		/* generate MAL code */
		backend_callinline(sql, c, s );
		addQueryToCache(c);

		if( c->curprg->def->errors){
			/* restore the state */
			resetMalBlk(c->curprg->def, oldstop);
			freeVariables(c,c->curprg->def, c->glb, oldvtop);
			c->curprg->def->errors = 0;
			goto endofcompile;
		}

#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout,"#result of sql.eval()\n");
		printFunction(c->fdout, c->curprg->def, 0, c->listing);
#endif

		if ( execute) {
			if (!output)
				sql->out = NULL; /* no output */
			msg = (str) runMAL(c, c->curprg->def, 1, 0, 0, 0);
			/*MSresetInstructions(c->curprg->def, 1);*/
			resetMalBlk(c->curprg->def, oldstop);
			/*freeVariables(c,c->curprg->def, 0, 0);*/
			freeVariables(c,c->curprg->def, c->glb, oldvtop);
		}
		sqlcleanup(m, 0);
		if (!execute)
			goto endofcompile;
#ifdef _SQL_COMPILE
	mnstr_printf(c->fdout, "#parse/execute result %d\n", err);
#endif
	}
/*
 * @-
 * We are done; a MAL procedure recides in the cache.
 */
endofcompile:
	if (execute)
		MSresetInstructions(c->curprg->def, 1);

	c->state[MAL_SCENARIO_PARSER] = be;
	backend_destroy(sql);
	GDKfree(n);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);
	if (m->sa)
		sa_destroy(m->sa);
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
	m->last = NULL;
	return msg;
}

str
SQLstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *expr = (str*) getArgReference(stk, pci, 1);
	bit output = TRUE;

	(void)mb;
	if (pci->argc == 3)
 		output = *(bit*) getArgReference(stk, pci, 2);

	return SQLstatementIntern(cntxt, expr, "SQLstatement", TRUE, output);
}

str
SQLcompile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = (str*) getArgReference(stk, pci, 0);
	str *expr = (str*) getArgReference(stk, pci, 1);
	str msg;

	(void)mb;
	*ret = NULL;
	msg = SQLstatementIntern(cntxt, expr, "SQLcompile", FALSE, FALSE);
	if( msg == MAL_SUCCEED)
		*ret= _strdup("SQLcompile");
	return msg;
}
/*
 * @-
 * Locate a file with SQL commands and execute it. For the time being a 1MB
 * file limit is implicitly imposed. If the file can not be located in the
 * script library, we assume it is sufficiently self descriptive.
 * (Respecting the file system context where the call is executed )
 */
str
SQLinclude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    	stream *fd;
	bstream *bfd;
	str *name = (str *) getArgReference(stk,pci,1);
	str msg = MAL_SUCCEED, fullname;
	str *expr;
	mvc *m;

    	fullname= MSP_locate_sqlscript(*name, 0);
	if ( fullname == NULL)
		fullname= *name;
	fd = open_rastream(fullname);
    	if (mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
        	mnstr_destroy(fd);
        	throw(MAL, "sql.include", "could not open file: %s\n", *name);
    	}
    	bfd = bstream_create(fd, 128 * BLOCK);
    	if( bstream_next(bfd) < 0)
        	throw(MAL,"sql.include","could not read %s\n", *name);

	expr = &bfd->buf;
	msg = SQLstatementIntern(cntxt, expr, "sql.include", TRUE, FALSE);
	bstream_destroy(bfd);
	m = cntxt->state[MAL_SCENARIO_OPTIMIZE];
	if (m->sa)
		sa_destroy(m->sa);
	m->sa = NULL;
	(void) mb;
	return msg;
}

/*
 * @-
 * The SQL reader collects a (sequence) of statements from the input
 * stream, but only when no unresolved 'nxt' character is visible.
 * In combination with SQLparser this ensures that all statements
 * are handled one by one.
 *
 * The SQLreader is called from two places: the SQL parser and
 * the MAL debugger.
 * The former only occurs during the parsing phase and the
 * second only during exection.
 * This means we can safely change the language setting for
 * the duration of these calls.
 */

static int SQLautocommit(Client c, mvc *m){
	if (m->session->auto_commit && m->session->active) {
		if (mvc_status(m) < 0) {
			mvc_rollback(m, 0, NULL);
		} else if (mvc_commit(m, 0, NULL) < 0) {
		 	return handle_error(m, c->fdout, 0);
		}
	}
	return TRUE;
}

/* #define _SQL_READER_DEBUG */
str
SQLreader(Client c)
{
	int go = TRUE;
	int more = TRUE;
	backend *be = ((backend *) c->state[MAL_SCENARIO_PARSER]);
	bstream *in = c->fdin;
	int language = -1;
	mvc *m = NULL;
	int blocked = isa_block_stream(in->s);

	if( SQLinitialized == FALSE){
		c->mode = FINISHING;
		return NULL;
	}
	if (!be || c->mode <= FINISHING) {
#ifdef _SQL_READER_DEBUG
	mnstr_printf(GDKout, "SQL client finished\n");
#endif
		c->mode = FINISHING;
		return NULL;
	}
#ifdef _SQL_READER_DEBUG
	mnstr_printf(GDKout, "#SQLparser: start reading SQL %s %s\n",
		(be->console?" from console":""),
		(blocked? "Blocked read":""));
#endif
	language = be->language;	/* 'S' for SQL, 'D' from debugger */
	m = be->mvc;
	m->errstr[0] = 0;
	/*
	 * @-
	 * Continue processing any left-over input from the previous round.
	 */

#ifdef _SQL_READER_DEBUG
	mnstr_printf(GDKout, "#pos %d len %d eof %d \n",
		in->pos, in->len, in->eof);
#endif
	/*
	 * @-
	 * Distinguish between console reading and mclient connections.
	 * The former comes with readline functionality.
	 */
	while(more) {
		more = FALSE;

		/* Different kinds of supported statements sequences
			A;	-- single line			s
			A \n B;	-- multi line			S
			A; B;   -- compound single block	s
			A;	-- many multi line
			B \n C; -- statements in one block	S
		*/
		/* auto_commit on end of statement */
		if (m->scanner.mode == LINE_N)
			go = SQLautocommit(c,m);

		if (go && in->pos >= in->len) {
		ssize_t rd;

		if (c->bak) {
#ifdef _SQL_READER_DEBUG
			mnstr_printf(GDKout, "Switch to backup stream\n");
#endif
			in = c->fdin;
			blocked = isa_block_stream(in->s);
			m->scanner.rs = c->fdin;
			c->fdin->pos += c->yycur;
			c->yycur = 0;
		}
		if (in->eof || !blocked) {
			language = (be->console) ? 'S' : 0;

			/* The rules of auto_commit require us to finish
			   and start a transaction on the start of a new statement (s A;B; case) */
			if (!(m->emod & mod_debug))
				go = SQLautocommit(c,m);

			if (go && ((!blocked && mnstr_write(c->fdout, c->prompt, c->promptlength, 1) != 1) || mnstr_flush(c->fdout))) {
				go = FALSE;
				break;
			}
			in->eof = 0;
		}
		if (in->buf == NULL) {
			more = FALSE;
			go = FALSE;
		} else if (go && (rd = bstream_next(in)) <= 0) {
#ifdef _SQL_READER_DEBUG
			mnstr_printf(GDKout, "#rd %d  language %d eof %d\n", rd, language, in->eof);
#endif
			if (be->language == 'D' && in->eof == 0)
				return 0;

			if (rd == 0 && language != 0 && in->eof && !be->console) {
				/* we hadn't seen the EOF before, so just try again
				   (this time with prompt) */
				more = TRUE;
				continue;
			}
			go = FALSE;
			break;
		} else if (go && !be->console && language == 0) {
			be->language = in->buf[in->pos++];
			if (be->language == 's') {
				be->language = 'S';
				m->scanner.mode = LINE_1;
			} else if (be->language == 'S') {
				m->scanner.mode = LINE_N;
			}
		}
#ifdef _SQL_READER_DEBUG
		mnstr_printf(GDKout, "#SQL blk:%s\n", in->buf + in->pos);
#endif
		}
	}
	if (!go || (strncmp(CURRENT(c), "\\q", 2) == 0)) {
		in->pos = in->len;	/* skip rest of the input */
		c->mode = FINISHING;
		return NULL;
	}
	return 0;
}

/*
 * @-
 * The SQL block is stored in the client input buffer, from which it
 * can be parsed by the SQL parser. The client structure contains
 * a small table of bounded tables. This should be reset before we
 * parse a new statement sequence.
 * @-
 * Before we parse the sql statement, we look for any variable settings
 * for specific commands.
 * The most important one is to prepare code to be handled by the debugger.
 * The current analysis is simple and fulfills our short-term needs.
 * A future version may analyze the parameter settings in more detail.
 */
static void
SQLsetDebugger(Client c, mvc *m, int onoff)
{
	if (m == 0 || !(m->emod & mod_debug))
		return;
	c->itrace='n';
	if( onoff){
		newStmt(c->curprg->def,"mdb","start");
		c->debugOptimizer = TRUE;
		c->curprg->def->keephistory = TRUE;
	} else {
		newStmt(c->curprg->def,"mdb","stop");
		c->debugOptimizer = FALSE;
		c->curprg->def->keephistory = FALSE;
	}
}

/*
 * @-
 * The trace operation collects the events in the BATs
 * and creates a secondary result set upon termination
 * of the query. This feature is extended with
 * a SQL variable to identify which trace flags are needed.
 * The control term 'keep' avoids clearing the performance tables,
 * which makes it possible to inspect the results later using
 * SQL itself. (Script needed to bind the BATs to a SQL table.)
 */
static void
SQLsetTrace(backend *be, Client c, bit onoff)
{
	int i = 0, j = 0;
	InstrPtr q;
	int n, r;
#define MAXCOLS 24
	int rs[MAXCOLS];
	str colname[MAXCOLS];
	int coltype[MAXCOLS];
	MalBlkPtr mb = c->curprg->def;
	str traceFlag, t,s, def= GDKstrdup("show,ticks,stmt");

	traceFlag = stack_get_string(be->mvc, "trace");
	if ( traceFlag && *traceFlag){
		GDKfree(def);
		def= GDKstrdup(traceFlag);
	}
	t= def;

	if (onoff){
		if ( strstr(def,"keep") == 0)
			(void) newStmt(mb,"profiler","reset");
		q = newStmt(mb,"profiler","setFilter");
		q = pushStr(mb,q, "*");
		q = pushStr(mb,q, "*");
		(void) newStmt(mb,"profiler","start");
	} else if (def && strstr(def, "show")) {
		(void) newStmt(mb, "profiler", "stop");

		do {
			s = t;
			t = strchr(t + 1, ',');
			if (t)
				*t = 0;
			if (strcmp("keep", s) && strcmp("show", s)) {
				q = newStmt(mb, profilerRef, "getTrace");
				q = pushStr(mb, q, s);
				n = getDestVar(q);
				q = newStmt(mb, algebraRef, "markH");
				q = pushArgument(mb, q, n);
				rs[i] = getDestVar(q);
				colname[i] = s;
				/* FIXME: type for name should come from
				 * mal_profiler.mx, second FIXME: check the user
				 * supplied values */
				if (
						strcmp(s, "time") == 0 ||
						strcmp(s, "pc") == 0 ||
						strcmp(s, "stmt") == 0
				) {
					coltype[i] = TYPE_str;
				} else if (
						strcmp(s, "ticks") == 0 ||
						strcmp(s, "rbytes") == 0 ||
						strcmp(s, "wbytes") == 0 ||
						strcmp(s, "reads") == 0 ||
						strcmp(s, "writes") == 0
				) {
					coltype[i] = TYPE_lng;
				} else if (
						strcmp(s, "thread") == 0
				) {
					coltype[i] = TYPE_int;
				}
				i++;
				if (i == MAXCOLS) /* just ignore the rest */
					break;
			}
		} while (t++);

		if (i > 0) {
			q = newStmt(mb, sqlRef, "resultSet");
			q = pushInt(mb, q, i);
			q = pushInt(mb, q, 1);
			q = pushArgument(mb, q, rs[0]);
			r = getDestVar(q);

			for (j = 0; j < i; j++) {
				q = newStmt(mb, sqlRef, "rsColumn");
				q = pushArgument(mb, q, r);
				q = pushStr(mb, q, ".trace");
				q = pushStr(mb, q, colname[j]);
				if (coltype[j] == TYPE_str) {
					q = pushStr(mb, q, "varchar");
					q = pushInt(mb, q, 1024);
				} else if (coltype[j] == TYPE_lng) {
					q = pushStr(mb, q, "bigint");
					q = pushInt(mb, q, 64);
				} else if (coltype[j] == TYPE_int) {
					q = pushStr(mb, q, "int");
					q = pushInt(mb, q, 32);
				}
				q = pushInt(mb, q, 0);
				(void) pushArgument(mb, q, rs[j]);
			}

			q = newStmt(mb, ioRef, "stdout");
			n = getDestVar(q);
			q = newStmt(mb, sqlRef, "exportResult");
			q = pushArgument(mb, q, n);
			(void) pushArgument(mb, q, r);
		}
	}
	GDKfree(def);
}

static void
SQLshowPlan(Client c)
{
	/* we should determine rendering requirements first */
	/* FIXME: unify this with direct printFunction() calls as used below */
	newStmt(c->curprg->def, "mdb", "listMapi");
}

/*
 * @-
 * The core part of the SQL interface, parse the query and
 * prepare the intermediate code.
 */

str
SQLparser(Client c)
{
	bstream *in = c->fdin;
	stream *out = c->fdout;
	str msg = NULL;
	backend *be;
	mvc *m;
	int oldvtop,oldstop;
	int pstatus = 0;
	int err = 0;

	be = ((backend *) c->state[MAL_SCENARIO_PARSER]);
	if (be == 0) {
		showException(SQL,"sql","SQL state descriptor missing\n");
		throw(SQL, "SQLparser", "State descriptor missing");
	}
	oldvtop = c->curprg->def->vtop;
	oldstop = c->curprg->def->stop;
	be->vtop = oldvtop;
#ifdef _SQL_PARSER_DEBUG
	mnstr_printf(GDKout, "#SQL compilation \n");
	printf("debugger? %d(%d)\n", (int)be->mvc->emode, (int)be->mvc->emod);
#endif
	m = be->mvc;
	m->type = Q_PARSE;
	SQLtrans(m);
	pstatus = m->session->status;

	/* sqlparse needs sql allocator to be available.  It can be NULL at
	 * this point if this is a recursive call. */
	if (!m->sa)
		m->sa = sa_create();

	if (m->history)
		be->mvc->Tparse = GDKusec();
	m->emode = m_normal;
	m->emod = mod_none;
	if (be->language == 'X') {
		int n = 0, v, off, len;

		if( strncmp(in->buf + in->pos,"export ",7)==0 )
			n = sscanf(in->buf + in->pos+7, "%d %d %d", &v, &off, &len);

		if (n == 2 || n == 3) {
			mvc_export_chunk(m, out, v, off, n == 3 ? len : m->reply_size);

			in->pos = in->len;	/* HACK: should use parsed lenght */
			return NULL;
		}
		if( strncmp(in->buf+in->pos,"close ",6)==0 ){
			res_table *t;

			v = (int) strtol(in->buf+in->pos+6,NULL,0);
 			t = res_tables_find(m->results, v);
			if (t)
				m->results = res_tables_remove(m->results, t);
			in->pos = in->len;	/* HACK: should use parsed lenght */
			return NULL;
		}
		if( strncmp(in->buf+in->pos,"release ", 8)==0 ){
			cq *q = NULL;

			v = (int) strtol(in->buf+in->pos+ 8,NULL,0);
			if ((q = qc_find(m->qc, v)) != NULL)
					qc_delete(m->qc, q);
			in->pos = in->len;	/* HACK: should use parsed lenght */
			return NULL;
		}
		if( strncmp(in->buf+in->pos,"auto_commit ", 12)==0 ){
			int commit;
			v= (int) strtol(in->buf+in->pos+12,NULL,10);
			commit = (!m->session->auto_commit && v);
			m->session->auto_commit = (v)?1:0;
			m->session->ac_on_commit = m->session->auto_commit;
			if (m->session->active) {
				if (commit && mvc_commit(m, 0, NULL)< 0)
					throw(SQL, "SQLparser", "Xauto_commit (commit) failed");
				else if (!commit && mvc_rollback(m, 0, NULL) < 0)
					throw(SQL, "SQLparser", "Xauto_commit (rollback) failed");
			}
			in->pos = in->len;	/* HACK: should use parsed lenght */
			return NULL;
		}
		if( strncmp(in->buf+in->pos,"reply_size ", 11)==0 ){
			v= (int) strtol(in->buf+in->pos+11,NULL,10);
			m->reply_size = v;
			in->pos = in->len;	/* HACK: should use parsed lenght */
			return NULL;
		}
		if (strncmp(in->buf + in->pos, "sizeheader", 10) == 0) {
			v = (int) strtol(in->buf + in->pos + 10, NULL, 10);
			m->sizeheader = v != 0;
			in->pos = in->len;	/* HACK: should use parsed lenght */
			return NULL;
		}
		if( strncmp(in->buf + in->pos,"quit",4)==0 ) {
			c->mode = FINISHING;
			return NULL;
		}
		throw(SQL, "SQLparser", "Unrecognized X command");
	}
	if (be->language != 'S' ) {
		throw(SQL, "SQLparser", "Unrecognized language prefix");
	}

	if ((err = sqlparse(m)) && m->debug&1){
		/* switch to different language mode */
		char oldlang= be->language;
		be->language= 'D';
		runMALDebugger(c,c->curprg);
		be->language= oldlang;
	}
	if( err ||
	    /* Only forget old errors on transaction boundaries */
	    (mvc_status(m) && m->type != Q_TRANS) || !m->sym) {
		if (!err && m->scanner.started) /* repeat old errors, with a parsed query */
			err = mvc_status(m);
		if (err) {
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			handle_error(m, c->fdout, pstatus);
		}
		sqlcleanup(m, err);
		goto finalize;
	}
	/*
	 * @-
	 * We have dealt with the first parsing step and advanced the input reader
	 * to the next statement (if any).
	 * Now is the time to also perform the semantic analysis, optimize and
	 * produce code.
	 */
	be->q = NULL;
	if (m->emode == m_execute) {
		assert(m->sym->data.lval->h->type == type_int);
		be->q = qc_find(m->qc, m->sym->data.lval->h->data.i_val);
		if (!be->q) {
			err = -1;
			sql_error(m, 2, "no prepared statement with the given id\n");
			sqlcleanup(m, err);
			goto finalize;
		}
		m->emode = m_inplace;
		scanner_query_processed(&(m->scanner));
	} else if ((be->q = qc_match(m->qc, m->sym, m->args, m->argc, m->scanner.key ^ m->session->schema->base.id))) {
		if (m->emod & mod_debug)
			SQLsetDebugger(c, m, TRUE);
		if (m->emod & mod_trace)
			SQLsetTrace(be, c, TRUE);
		if (m->emode != m_explain && !(m->emod & (mod_debug|mod_trace)))
			m->emode = m_inplace;
		scanner_query_processed(&(m->scanner));
	} else {
		stmt *s = sql_symbol2stmt(m, m->sym);

		if (s==0 || (err = mvc_status(m) && m->type != Q_TRANS)) {
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			handle_error(m, c->fdout, pstatus);
			sqlcleanup(m, err);
			goto finalize;
		}
		assert(s);
		/* generate and call the MAL code */
		if (m->emode == m_explain)
			SQLshowPlan(c);
		if (m->emod & mod_trace)
			SQLsetTrace(be, c, TRUE);
		if (m->emod & mod_debug)
			SQLsetDebugger(c, m, TRUE);
		if ((m->emode != m_inplace && m->emode != m_prepare && !m->caching && m->emode != m_explain) || s->type == st_none || m->type == Q_TRANS) {
			InstrPtr p;
			MalBlkPtr curBlk;

			scanner_query_processed(&(m->scanner));
			backend_callinline(be, c, s );

			curBlk = c->curprg->def;
			p = newFcnCall(curBlk, "optimizer", "remap");
			typeChecker(c->nspace, curBlk, p, FALSE);
			p = newFcnCall(curBlk, "optimizer", "multiplex");
			typeChecker(c->nspace, curBlk, p, FALSE);
			optimizeMALBlock(c, curBlk);
			c->curprg->def = curBlk;

			if( m->emode == m_inplace)
				m->emode = m_normal;
		} else {
			/* generate a factory instantiation */
			be->q = qc_insert(m->qc,
					  m->sa,      /* the allocator */
					  m->sym,     /* the sql symbol tree */
					  m->args,    /* the argument list */
					  m->argc,
					  m->scanner.key ^ m->session->schema->base.id,/* the statement hash key */
					  (m->emode == m_prepare)?Q_PREPARE:
					  m->type,/* the type of the statement */
					  sql_escape_str(QUERY(m->scanner)));
			scanner_query_processed(&(m->scanner));
			be->q->code =
				(backend_code)backend_dumpproc(be, c, be->q, s);
			be->q->stk = 0;

			/* passed over to query cache, used during dumpproc */
			m->sa = NULL;
			m->sym = NULL;

			/* register name in the namespace */
			be->q->name = putName(be->q->name, strlen(be->q->name));
			if (m->emode == m_normal && m->emod == mod_none)
				m->emode = m_inplace;
		}
	}
	if (be->q){
		if (m->emode == m_prepare)
			err = mvc_export_prepare(m, c->fdout, be->q, "");
		else if (m->emode == m_inplace ){
			/* everything ready for a fast call */
		} else /* call procedure generation (only in cache mode) */
			backend_call(be, c, be->q);
	}

	/*
	 * @-
	 * In the final phase we add any debugging control
	 */
	if (m->emod & mod_trace)
		SQLsetTrace(be, c, FALSE);
	if (m->emod & mod_debug)
		SQLsetDebugger(c, m, FALSE);

	/*
	 * @-
	 * During the execution of the query exceptions can be raised.
	 * The default action is to print them out at the end of the
	 * query block.
	 */
	if (err == 0) {
		pushEndInstruction(c->curprg->def);

		chkTypes(c->nspace, c->curprg->def, TRUE); /* resolve types */
		/* we know more in this case then
			chkProgram(c->nspace, c->curprg->def); */
		if (c->curprg->def->errors) {
			if (m->emod & mod_debug) {
				/* switch to differnt language mode */
				char oldlang= be->language;
				be->language= 'D';
				runMALDebugger(c,c->curprg);
				be->language= oldlang;
			}
			showErrors(c);
			/* restore the state */
			resetMalBlk(c->curprg->def, oldstop);
			freeVariables(c,c->curprg->def, c->glb, oldvtop);
			c->curprg->def->errors = 0;
			msg = createException(PARSE, "SQLparser", "Semantic errors");
		}
	}
/*
 * @-
 * Inspect the variables for post code-generation actions.
 */
finalize:
	if (m->emode == m_explain  && be->q && be->q->code)
		printFunction(GDKout, ((Symbol)(be->q->code))->def, 0, LIST_MAL_STMT  | LIST_MAL_UDF | LIST_MAPI);
	/*
	 * @-
	 * Gather the statistics for post analysis. It should preferably
	 * be stored in an SQL table
	 */
	if (msg)
		sqlcleanup(m, 0);
	return msg;
}

/*
 * @-
 * Execution of the SQL program is delegated to the MALengine.
 * Different cases should be distinguished. The default is to
 * hand over the MAL block derived by the parser for execution.
 * However, when we received an Execute call, we make a shortcut
 * and prepare the stack for immediate execution
 */
static str
SQLexecutePrepared(Client c, backend *be, cq *q )
{
	mvc *m = be->mvc;
	int argc, parc;
	ValPtr *argv, argvbuffer[MAXARG], v;
	ValRecord *argrec, argrecbuffer[MAXARG];
	MalBlkPtr mb;
	MalStkPtr glb;
	InstrPtr pci;
	int i;
	str ret;

#ifdef DEBUG_CALLMAL
	mnstr_printf(GDKout,"SQLexecute\n");
	printFunction(GDKout, ((Symbol)q->code)->def, 0, LIST_MAL_ALL);
#endif
	mb = ((Symbol)q->code)->def;
	if ( mb->errors )
		throw(SQL, "SQLengine", "Program contains errors");
	pci = getInstrPtr(mb,0);
	if( pci->argc >= MAXARG)
		argv = (ValPtr *) GDKmalloc(sizeof(ValPtr) * pci->argc);
	else
		argv = argvbuffer;

	if( pci->retc >= MAXARG)
		argrec = (ValRecord *)GDKmalloc(sizeof(ValRecord) * pci->retc);
	else
		argrec = argrecbuffer;

	/* prepare the target variables */
	for(i=0; i<pci->retc; i++){
		argv[i] = argrec+i;
		argv[i]->vtype= getVarGDKType(mb,i);
	}

	argc = m->argc;
	parc = q->paramlen;

	if (argc != parc) {
		if( pci->argc >= MAXARG)
			GDKfree(argv);
		if( pci->retc >= MAXARG)
			GDKfree(argrec);
		throw(SQL, "sql.prepare", "wrong number of arguments for prepared statement: %d, expected %d", argc, parc);
	} else {
		for (i = 0; i < m->argc; i++) {
			atom *arg = m->args[i];
			sql_subtype *pt = q->params + i;

			if (!atom_cast(arg, pt)) {
				/*sql_error(c, 003, buf); */
				if (pci->argc >= MAXARG)
					GDKfree(argv);
				if (pci->retc >= MAXARG)
					GDKfree(argrec);
				throw(SQL, "sql.prepare", "wrong type for argument %d of "
						"prepared statement: %s, expected %s",
						i + 1, atom_type(arg)->type->sqlname,
						pt->type->sqlname);
			}
			argv[pci->retc + i] = &arg->data;
		}
	}
	glb = (MalStkPtr)(q->stk);
	ret= callMAL(c, mb, &glb, argv, (m->emod & mod_debug?'n':0));
	if (ret && SQLdebug&16)
		printFunction(c->fdout, mb, 0, LIST_MAL_STMT | LIST_MAPI );
	/* cleanup the arguments */
	for(i=pci->retc; i<pci->argc; i++) {
		garbageElement(c,v= &glb->stk[pci->argv[i]]);
		v->vtype= TYPE_int;
		v->val.ival= int_nil;
	}
	q->stk = (backend_stack)glb;
	if (glb && SQLdebug&1)
		printStack(GDKstdout, mb, glb);
	if( pci->argc >= MAXARG)
		GDKfree(argv);
	if( pci->retc >= MAXARG)
		GDKfree(argrec);
	return ret;
}

str SQLrecompile(Client c, backend *be);

static str
SQLengineIntern(Client c, backend *be)
{
	str msg = MAL_SUCCEED;
	MalStkPtr oldglb = c->glb;
	char oldlang= be->language;
	mvc *m = be->mvc;
	InstrPtr p;
	MalBlkPtr mb;

	if ( oldlang == 'X'){ 	/* return directly from X-commands */
		sqlcleanup(be->mvc, 0);
		return MAL_SUCCEED;
	}

	if (m->emode == m_explain) {
		sqlcleanup(be->mvc, 0);
		goto cleanup_engine;
	}
	if (c->curprg->def->errors){
		sqlcleanup(be->mvc, 0);
		throw(SQL, "SQLengine", "Program contains errors");
	}
#ifdef SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#Ready to execute SQL statement\n");
#endif

	if( c->curprg->def->stop == 1 ){
		sqlcleanup(be->mvc, 0);
		return MAL_SUCCEED;
	}
	if (m->emode == m_inplace) {
		msg = SQLexecutePrepared(c, be, be->q );
		goto cleanup_engine;
	}
	if( m->emode == m_prepare){
		goto cleanup_engine;
	} else if( m->emode == m_explain ){
		/*
		 * @-
		 * If you want to see the detailed code, we have to pick it up from
		 * the cache as well. This calls for finding the call to the
		 * cached routine, which may be hidden . For now we take a shortcut.
		 */
		if( be->q) {
			InstrPtr p;
			p = getInstrPtr(c->curprg->def,1);
			if (p->blk)
				printFunction(c->fdout, p->blk, 0, c->listing | LIST_MAPI );
		}
		c->curprg->def->errors = -1; /* don;t execute */
	}
	c->glb = 0;
	be->language= 'D';
	/*
	 * @-
	 * The code below is copied from MALengine, which handles execution
	 * in the context of a user global environment. We have a private
	 * environment.
	 */
	if( MALcommentsOnly(c->curprg->def)) {
		msg = MAL_SUCCEED;
	} else {
		msg = (str) runMAL(c, c->curprg->def, 1, 0, 0, 0);
	}

cleanup_engine:
	if (msg) {
		if (getExceptionType(msg) == OPTIMIZER) {
			resetMalBlk( c->curprg->def, 1);
			/* resetInstructions(c->curprg->def, 1);*/
			freeVariables(c,c->curprg->def, c->glb, be->vtop);
			be->language = oldlang;
			c->glb = oldglb;
			return SQLrecompile(c, be);
		} else {
			str p = getExceptionPlace(msg);
			showException(getExceptionType(msg), p, "%s", getExceptionMessage(msg));
			GDKfree(p);
		}
		showErrors(c);
		m->session->status = -10;
	}
/*
 * @-
 * If we are dealing with a {runonce} plan, the query cache should
 * be adjusted too.
 */
/* postpone */

	mb= c->curprg->def;
	if (be->q && mb &&
	    varGetProp(mb, getArg(p = getInstrPtr(mb,0), 0), runonceProp)){
		SQLCacheRemove(c, getFunctionId(p));
		/* this should invalidate any match */
		be->q->key= -1;
		be->q->paramlen = -1;
		/* qc_delete(be->q) */
	}
	be->q = NULL;
	sqlcleanup(be->mvc, 0);
	resetMalBlk( c->curprg->def, 1);
	/* resetInstructions(c->curprg->def, 1);*/
	freeVariables(c,c->curprg->def, c->glb, be->vtop);
	be->language = oldlang;
	/*
	 * @-
	 * Any error encountered during execution should block further processing
	 * unless auto_commit has been set.
	 */
	c->glb = oldglb;
	return msg;
}

str
SQLrecompile(Client c, backend *be)
{
	stmt *s;
	mvc *m = be->mvc;
	int oldvtop = c->curprg->def->vtop;
	int oldstop = c->curprg->def->stop;

	SQLCacheRemove(c, be->q->name);
	s = sql_symbol2stmt(m, be->q->s);
	be->q->code = (backend_code)backend_dumpproc(be, c, be->q, s);
	be->q->stk = 0;

	pushEndInstruction(c->curprg->def);

	chkTypes(c->nspace, c->curprg->def, TRUE); /* resolve types */
	if (c->curprg->def->errors) {
		showErrors(c);
		/* restore the state */
		resetMalBlk(c->curprg->def, oldstop);
		freeVariables(c,c->curprg->def, c->glb, oldvtop);
		c->curprg->def->errors = 0;
		throw(SQL, "SQLrecompile", "Semantic errors");
	}
	return SQLengineIntern(c, be);
}

str
SQLengine(Client c)
{
	backend *be = ((backend *) c->state[MAL_SCENARIO_PARSER]);
	return SQLengineIntern(c, be);
}

/*
 * @-
 * Assertion errors detected during the execution of a code block
 * raises an exception. An debugger dump is generated upon request
 * to ease debugging.
 */
str
SQLassert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	bit *flg = (bit*) getArgReference(stk,pci, 1);
	str *msg = (str*) getArgReference(stk,pci, 2);
	(void) cntxt;
	(void)mb;
	if (*flg){
		/* mdbDump(mb,stk,pci);*/
		throw(SQL, "assert", "%s", *msg);
	}
	return MAL_SUCCEED;
}

str
SQLassertInt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	int *flg = (int*) getArgReference(stk,pci, 1);
	str *msg = (str*) getArgReference(stk,pci, 2);
	(void) cntxt;
	(void)mb;
	if (*flg){
		/* mdbDump(mb,stk,pci);*/
		throw(SQL, "assert", "%s", *msg);
	}
	return MAL_SUCCEED;
}

str
SQLassertWrd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	wrd *flg = (wrd*) getArgReference(stk,pci, 1);
	str *msg = (str*) getArgReference(stk,pci, 2);
	(void) cntxt;
	(void)mb;
	if (*flg){
		/* mdbDump(mb,stk,pci);*/
		throw(SQL, "assert", "%s", *msg);
	}
	return MAL_SUCCEED;
}

str
SQLassertLng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	lng *flg = (lng*) getArgReference(stk,pci, 1);
	str *msg = (str*) getArgReference(stk,pci, 2);
	(void) cntxt;
	(void)mb;
	if (*flg){
		/* mdbDump(mb,stk,pci);*/
		throw(SQL, "assert", "%s", *msg);
	}
	return MAL_SUCCEED;
}

str
SQLCacheRemove(Client c, str nme)
{
	Symbol s;

#ifdef _SQL_CACHE_DEBUG
	mnstr_printf(GDKout, "SQLCacheRemove %s\n", nme);
#endif

	s= findSymbolInModule(c->nspace, nme);
	if (s == NULL)
		throw(MAL, "cache.remove", "internal error, symbol missing\n");
	if( getInstrPtr(s->def,0)->token == FACTORYsymbol)
		shutdownFactoryByName(c,c->nspace, nme);
	else
		deleteSymbol(c->nspace,s);
	return MAL_SUCCEED;
}
