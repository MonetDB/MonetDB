/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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
#include "mal_backend.h"
#include "sql_scenario.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_assert.h"
#include "sql_execute.h"
#include "sql_env.h"
#include "sql_mvc.h"
#include "sql_readline.h"
#include "sql_user.h"
#include "sql_datetime.h"
#include "sql_optimizer.h"
#include "mal_io.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_debugger.h"
#include "mal_linker.h"
#include "bat5.h"
#include "msabaoth.h"
#include <mtime.h>
#include "optimizer.h"
#include "opt_statistics.h"
#include "opt_prelude.h"
#include "opt_pipes.h"
#include "opt_mitosis.h"
#include <unistd.h>
#include "sql_upgrades.h"

static int SQLinitialized = 0;
static int SQLnewcatalog = 0;
int SQLdebug = 0;
static char *sqlinit = NULL;
MT_Lock sql_contextLock MT_LOCK_INITIALIZER("sql_contextLock");

static void
monet5_freestack(int clientid, backend_stack stk)
{
	MalStkPtr p = (ptr) stk;

	(void) clientid;
	if (p != NULL)
		freeStack(p);
#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#monet5_freestack\n");
#endif
}

static void
monet5_freecode(int clientid, backend_code code, backend_stack stk, int nr, char *name)
{
	str msg;

	(void) code;
	(void) stk;
	(void) nr;
	(void) clientid;
	msg = SQLCacheRemove(MCgetClient(clientid), name);
	if (msg)
		GDKfree(msg);	/* do something with error? */

#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#monet5_free:%d\n", nr);
#endif
}

str
SQLsession(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	str logmsg;
	int cnt=0;

	(void) mb;
	(void) stk;
	(void) pci;
	if (SQLinitialized == 0 && (msg = SQLprelude(NULL)) != MAL_SUCCEED)
		return msg;
	msg = setScenario(cntxt, "sql");
	// Wait for any recovery process to be finished
	do {
		MT_sleep_ms(1000);
		logmsg = GDKgetenv("recovery");
		if( logmsg== NULL && ++cnt  == 5)
			throw(SQL,"SQLinit","#WARNING server not ready, recovery in progress\n");
    }while (logmsg == NULL);
	return msg;
}

str
SQLsession2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	str logmsg;
	int cnt=0;

	(void) mb;
	(void) stk;
	(void) pci;
	if (SQLinitialized == 0 && (msg = SQLprelude(NULL)) != MAL_SUCCEED)
		return msg;
	msg = setScenario(cntxt, "msql");
	// Wait for any recovery process to be finished
	do {
		MT_sleep_ms(1000);
		logmsg = GDKgetenv("recovery");
		if( logmsg== NULL && ++cnt  == 5)
			throw(SQL,"SQLinit","#WARNING server not ready, recovery in progress\n");
    }while (logmsg == NULL);
	return msg;
}

static str SQLinit(void);

str
SQLprelude(void *ret)
{
	str tmp;
	Scenario ms, s = getFreeScenario();

	(void) ret;
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
	tmp = SQLinit();
	if (tmp != MAL_SUCCEED)
		return (tmp);
	fprintf(stdout, "# MonetDB/SQL module loaded\n");
	fflush(stdout);		/* make merovingian see this *now* */

	/* only register availability of scenarios AFTER we are inited! */
	s->name = "sql";
	tmp = msab_marchScenario(s->name);
	if (tmp != MAL_SUCCEED)
		return (tmp);
	ms->name = "msql";
	tmp = msab_marchScenario(ms->name);
	return tmp;
}

str
SQLepilogue(void *ret)
{
	char *s = "sql", *m = "msql";
	str res;

	(void) ret;
	if (SQLinitialized) {
		mvc_exit();
		SQLinitialized = FALSE;
	}
	/* this function is never called, but for the style of it, we clean
	 * up our own mess */
	res = msab_retreatScenario(m);
	if (!res)
		return msab_retreatScenario(s);
	return res;
}

MT_Id sqllogthread, minmaxthread;

static str
SQLinit(void)
{
	char *debug_str = GDKgetenv("sql_debug"), *msg = MAL_SUCCEED;
	int readonly = GDKgetenv_isyes("gdk_readonly");
	int single_user = GDKgetenv_isyes("gdk_single_user");
	const char *gmt = "GMT";
	tzone tz;

#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#SQLinit Monet 5\n");
#endif
	if (SQLinitialized)
		return MAL_SUCCEED;

#ifdef NEED_MT_LOCK_INIT
	MT_lock_init(&sql_contextLock, "sql_contextLock");
#endif

	MT_lock_set(&sql_contextLock);
	memset((char *) &be_funcs, 0, sizeof(backend_functions));
	be_funcs.fstack = &monet5_freestack;
	be_funcs.fcode = &monet5_freecode;
	be_funcs.fresolve_function = &monet5_resolve_function;
	monet5_user_init(&be_funcs);

	msg = MTIMEtimezone(&tz, &gmt);
	if (msg)
		return msg;
	(void) tz;
	if (debug_str)
		SQLdebug = strtol(debug_str, NULL, 10);
	if (single_user)
		SQLdebug |= 64;
	if (readonly)
		SQLdebug |= 32;
	if ((SQLnewcatalog = mvc_init(SQLdebug, store_bat, readonly, single_user, 0)) < 0)
		throw(SQL, "SQLinit", "Catalogue initialization failed");
	SQLinitialized = TRUE;
	MT_lock_unset(&sql_contextLock);
	if (MT_create_thread(&sqllogthread, (void (*)(void *)) mvc_logmanager, NULL, MT_THR_DETACHED) != 0) {
		throw(SQL, "SQLinit", "Starting log manager failed");
	}
#if 0
	if (MT_create_thread(&minmaxthread, (void (*)(void *)) mvc_minmaxmanager, NULL, MT_THR_DETACHED) != 0) {
		throw(SQL, "SQLinit", "Starting minmax manager failed");
	}
#endif
	return MAL_SUCCEED;
}

str
SQLexit(Client c)
{
#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#SQLexit\n");
#endif
	(void) c;		/* not used */
	if (SQLinitialized == FALSE)
		throw(SQL, "SQLexit", "Catalogue not available");
	return MAL_SUCCEED;
}

#define SQLglobal(name, val) \
	stack_push_var(sql, name, &ctype);	   \
	stack_set_var(sql, name, VALset(&src, ctype.type->localtype, val));

#define NR_GLOBAL_VARS 10
/* NR_GLOBAL_VAR should match exactly the number of variables created
   in global_variables */
/* initialize the global variable, ie make mvc point to these */
static int
global_variables(mvc *sql, char *user, char *schema)
{
	sql_subtype ctype;
	char *typename;
	lng sec = 0;
	bit F = FALSE;
	ValRecord src;
	str opt;

	typename = "int";
	sql_find_subtype(&ctype, typename, 0, 0);
	SQLglobal("debug", &sql->debug);
	SQLglobal("cache", &sql->cache);

	typename = "varchar";
	sql_find_subtype(&ctype, typename, 1024, 0);
	SQLglobal("current_schema", schema);
	SQLglobal("current_user", user);
	SQLglobal("current_role", user);

	/* inherit the optimizer from the server */
	opt = GDKgetenv("sql_optimizer");
	if (!opt)
		opt = "default_pipe";
	SQLglobal("optimizer", opt);

	typename = "sec_interval";
	sql_find_subtype(&ctype, typename, inttype2digits(ihour, isec), 0);
	SQLglobal("current_timezone", &sec);

	typename = "boolean";
	sql_find_subtype(&ctype, typename, 0, 0);
	SQLglobal("history", &F);

	typename = "bigint";
	sql_find_subtype(&ctype, typename, 0, 0);
	SQLglobal("last_id", &sql->last_id);
	SQLglobal("rowcnt", &sql->rowcnt);
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
		p++;		/* include newline */
		if (*str !='!' && mnstr_write(out, "!", 1, 1) != 1)
			return -1;
		if (mnstr_write(out, str, p - str, 1) != 1)
			 return -1;
		str = p;
	}
	if (str &&*str) {
		if (*str !='!' && mnstr_write(out, "!", 1, 1) != 1)
			return -1;
		if (mnstr_write(out, str, strlen(str), 1) != 1 || mnstr_write(out, "\n", 1, 1) != 1)
			 return -1;
	}
	return 0;
}

#define TRANS_ABORTED "!25005!current transaction is aborted (please ROLLBACK)\n"

int
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

int
SQLautocommit(Client c, mvc *m)
{
	if (m->session->auto_commit && m->session->active) {
		if (mvc_status(m) < 0) {
			RECYCLEdrop(0);
			mvc_rollback(m, 0, NULL);
		} else if (mvc_commit(m, 0, NULL) < 0) {
			return handle_error(m, c->fdout, 0);
		}
	}
	return TRUE;
}

void
SQLtrans(mvc *m)
{
	m->caching = m->cache;
	if (!m->session->active) {
		sql_session *s;

		mvc_trans(m);
		s = m->session;
		if (!s->schema) {
			s->schema_name = monet5_user_get_def_schema(m, m->user_id);
			assert(s->schema_name);
			s->schema = find_sql_schema(s->tr, s->schema_name);
			assert(s->schema);
		}
	}
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
	static int maybeupgrade = 1;

#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#SQLinitClient\n");
#endif
	if (SQLinitialized == 0 && (msg = SQLprelude(NULL)) != MAL_SUCCEED)
		return msg;
	MT_lock_set(&sql_contextLock);
	/*
	 * Based on the initialization return value we can prepare a SQLinit
	 * string with all information needed to initialize the catalog
	 * based on the mandatory scripts to be executed.
	 */
	if (sqlinit) {		/* add sqlinit to the fdin stack */
		buffer *b = (buffer *) GDKmalloc(sizeof(buffer));
		size_t len = strlen(sqlinit);
		bstream *fdin;

		buffer_init(b, _STRDUP(sqlinit), len);
		fdin = bstream_create(buffer_rastream(b, "si"), b->len);
		bstream_next(fdin);
		MCpushClientInput(c, fdin, 0, "");
	}
	if (c->sqlcontext == 0) {
		m = mvc_create(c->idx, 0, SQLdebug, c->fdin, c->fdout);
		global_variables(m, "monetdb", "sys");
		if (isAdministrator(c) || strcmp(c->scenario, "msql") == 0)	/* console should return everything */
			m->reply_size = -1;
		be = (void *) backend_create(m, c);
	} else {
		be = c->sqlcontext;
		m = be->mvc;
		mvc_reset(m, c->fdin, c->fdout, SQLdebug, NR_GLOBAL_VARS);
		backend_reset(be);
	}
	if (m->session->tr)
		reset_functions(m->session->tr);
	/* pass through credentials of the user if not console */
	schema = monet5_user_set_def_schema(m, c->user);
	if (!schema) {
		_DELETE(schema);
		throw(PERMD, "SQLinitClient", "08004!schema authorization error");
	}
	_DELETE(schema);

	/*expect SQL text first */
	be->language = 'S';
	/* Set state, this indicates an initialized client scenario */
	c->state[MAL_SCENARIO_READER] = c;
	c->state[MAL_SCENARIO_PARSER] = c;
	c->state[MAL_SCENARIO_OPTIMIZE] = c;
	c->sqlcontext = be;

	initSQLreferences();
	/* initialize the database with predefined SQL functions */
	if (SQLnewcatalog == 0) {
		/* check whether table sys.systemfunctions exists: if
		 * it doesn't, this is probably a restart of the
		 * server after an incomplete initialization */
		sql_schema *s = mvc_bind_schema(m, "sys");
		sql_table *t = s ? mvc_bind_table(m, s, "systemfunctions") : NULL;
		if (t == NULL)
			SQLnewcatalog = 1;
	}
	if (SQLnewcatalog > 0) {
		char path[PATHLENGTH];
		str fullname;

		SQLnewcatalog = 0;
		maybeupgrade = 0;
		snprintf(path, PATHLENGTH, "createdb");
		slash_2_dir_sep(path);
		fullname = MSP_locate_sqlscript(path, 1);
		if (fullname) {
			str filename = fullname;
			str p, n;
			fprintf(stdout, "# SQL catalog created, loading sql scripts once\n");
			do {
				p = strchr(filename, PATH_SEP);
				if (p)
					*p = '\0';
				if ((n = strrchr(filename, DIR_SEP)) == NULL) {
					n = filename;
				} else {
					n++;
				}
				fprintf(stdout, "# loading sql script: %s\n", n);
				fd = open_rastream(filename);
				if (p)
					filename = p + 1;

				if (fd) {
					size_t sz;
					sz = getFileSize(fd);
					if (sz > (size_t) 1 << 29) {
						mnstr_destroy(fd);
						msg = createException(MAL, "createdb", "file %s too large to process", filename);
					} else {
						bfd = bstream_create(fd, sz == 0 ? (size_t) (128 * BLOCK) : sz);
						if (bfd && bstream_next(bfd) >= 0)
							msg = SQLstatementIntern(c, &bfd->buf, "sql.init", TRUE, FALSE, NULL);
						bstream_destroy(bfd);
					}
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
	} else {		/* handle upgrades */
		if (!m->sa)
			m->sa = sa_create();
		if (maybeupgrade)
			SQLupgrades(c,m);
		maybeupgrade = 0;
	}
	MT_lock_unset(&sql_contextLock);
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
	if (c->sqlcontext) {
		backend *be = NULL;
		mvc *m = NULL;
		if (c->sqlcontext == NULL)
			throw(SQL, "SQLexitClient", "MVC catalogue not available");
		be = (backend *) c->sqlcontext;
		m = be->mvc;

		assert(m->session);
		if (m->session->auto_commit && m->session->active) {
			if (mvc_status(m) >= 0 && mvc_commit(m, 0, NULL) < 0)
				(void) handle_error(m, c->fdout, 0);
		}
		if (m->session->active) {
			RECYCLEdrop(0);
			mvc_rollback(m, 0, NULL);
		}

		res_tables_destroy(m->results);
		m->results = NULL;

		mvc_destroy(m);
		backend_destroy(be);
		c->state[MAL_SCENARIO_OPTIMIZE] = NULL;
		c->state[MAL_SCENARIO_PARSER] = NULL;
		c->sqlcontext = NULL;
	}
	c->state[MAL_SCENARIO_READER] = NULL;
	return MAL_SUCCEED;
}

/*
 * A statement received internally is simply appended for
 * execution
 */
str
SQLinitEnvironment(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return SQLinitClient(cntxt);
}


str
SQLstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *expr = getArgReference_str(stk, pci, 1);
	bit output = TRUE;

	(void) mb;
	if (pci->argc == 3)
		output = *getArgReference_bit(stk, pci, 2);

	return SQLstatementIntern(cntxt, expr, "SQLstatement", TRUE, output, NULL);
}

str
SQLcompile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = getArgReference_str(stk, pci, 0);
	str *expr = getArgReference_str(stk, pci, 1);
	str msg;

	(void) mb;
	*ret = NULL;
	msg = SQLstatementIntern(cntxt, expr, "SQLcompile", FALSE, FALSE, NULL);
	if (msg == MAL_SUCCEED)
		*ret = _STRDUP("SQLcompile");
	return msg;
}

/*
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
	str *name = getArgReference_str(stk, pci, 1);
	str msg = MAL_SUCCEED, fullname;
	str *expr;
	mvc *m;
	size_t sz;

	fullname = MSP_locate_sqlscript(*name, 0);
	if (fullname == NULL)
		fullname = *name;
	fd = open_rastream(fullname);
	if (mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
		mnstr_destroy(fd);
		throw(MAL, "sql.include", "could not open file: %s\n", *name);
	}
	sz = getFileSize(fd);
	if (sz > (size_t) 1 << 29) {
		mnstr_destroy(fd);
		throw(MAL, "sql.include", "file %s too large to process", fullname);
	}
	bfd = bstream_create(fd, sz == 0 ? (size_t) (128 * BLOCK) : sz);
	if (bstream_next(bfd) < 0) {
		bstream_destroy(bfd);
		throw(MAL, "sql.include", "could not read %s\n", *name);
	}

	expr = &bfd->buf;
	msg = SQLstatementIntern(cntxt, expr, "sql.include", TRUE, FALSE, NULL);
	bstream_destroy(bfd);
	m = ((backend *) cntxt->sqlcontext)->mvc;
	if (m->sa)
		sa_destroy(m->sa);
	m->sa = NULL;
	(void) mb;
	return msg;
}

/*
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

/* #define _SQL_READER_DEBUG */
str
SQLreader(Client c)
{
	int go = TRUE;
	int more = TRUE;
	int commit_done = FALSE;
	backend *be = (backend *) c->sqlcontext;
	bstream *in = c->fdin;
	int language = -1;
	mvc *m = NULL;
	int blocked = isa_block_stream(in->s);

	if (SQLinitialized == FALSE) {
		c->mode = FINISHCLIENT;
		return NULL;
	}
	if (!be || c->mode <= FINISHCLIENT) {
#ifdef _SQL_READER_DEBUG
		mnstr_printf(GDKout, "#SQL client finished\n");
#endif
		c->mode = FINISHCLIENT;
		return NULL;
	}
#ifdef _SQL_READER_DEBUG
	mnstr_printf(GDKout, "#SQLparser: start reading SQL %s %s\n", (be->console ? " from console" : ""), (blocked ? "Blocked read" : ""));
#endif
	language = be->language;	/* 'S' for SQL, 'D' from debugger */
	m = be->mvc;
	m->errstr[0] = 0;
	/*
	 * Continue processing any left-over input from the previous round.
	 */

#ifdef _SQL_READER_DEBUG
	mnstr_printf(GDKout, "#pos %d len %d eof %d \n", in->pos, in->len, in->eof);
#endif
	/*
	 * Distinguish between console reading and mclient connections.
	 * The former comes with readline functionality.
	 */
	while (more) {
		more = FALSE;

		/* Different kinds of supported statements sequences
		   A;   -- single line                  s
		   A \n B;      -- multi line                   S
		   A; B;   -- compound single block     s
		   A;   -- many multi line
		   B \n C; -- statements in one block   S
		 */
		/* auto_commit on end of statement */
		if (m->scanner.mode == LINE_N && !commit_done) {
			go = SQLautocommit(c, m);
			commit_done = TRUE;
		}

		if (go && in->pos >= in->len) {
			ssize_t rd;

			if (c->bak) {
#ifdef _SQL_READER_DEBUG
				mnstr_printf(GDKout, "#Switch to backup stream\n");
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
				if (!(m->emod & mod_debug) && !commit_done) {
					go = SQLautocommit(c, m);
					commit_done = TRUE;
				}

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

				if (rd == 0 && language !=0 && in->eof && !be->console) {
					/* we hadn't seen the EOF before, so just try again
					   (this time with prompt) */
					more = TRUE;
					continue;
				}
				go = FALSE;
				break;
			} else if (go && !be->console && language == 0) {
				if (in->buf[in->pos] == 's' && !in->eof) {
					while ((rd = bstream_next(in)) > 0)
						;
				}
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
	if ( (c->stimeout && (GDKusec() - c->session) > c->stimeout) || !go || (strncmp(CURRENT(c), "\\q", 2) == 0)) {
		in->pos = in->len;	/* skip rest of the input */
		c->mode = FINISHCLIENT;
		return NULL;
	}
	return 0;
}

/*
 * The SQL block is stored in the client input buffer, from which it
 * can be parsed by the SQL parser. The client structure contains
 * a small table of bounded tables. This should be reset before we
 * parse a new statement sequence.
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
	c->itrace = 'n';
	if (onoff) {
		newStmt(c->curprg->def, "mdb", "start");
		c->debugOptimizer = TRUE;
		c->curprg->def->keephistory = TRUE;
	} else {
		newStmt(c->curprg->def, "mdb", "stop");
		c->debugOptimizer = FALSE;
		c->curprg->def->keephistory = FALSE;
	}
}

/*
 * The trace operation collects the events in the BATs
 * and creates a secondary result set upon termination
 * of the query. 
 */
static void
SQLsetTrace(backend *be, Client cntxt, bit onoff)
{
	InstrPtr q, resultset;
	InstrPtr tbls, cols, types, clen, scale;
	MalBlkPtr mb = cntxt->curprg->def;
	int k;

	(void) be;
	if (onoff) {
		(void) newStmt(mb, "profiler", "start");
		initTrace();
	} else {
		(void) newStmt(mb, "profiler", "stop");
		/* cook a new resultSet instruction */
		resultset = newInstruction(mb,ASSIGNsymbol);
		setModuleId(resultset, sqlRef);
		setFunctionId(resultset, resultSetRef);
	    getArg(resultset,0)= newTmpVariable(mb,TYPE_int);


		/* build table defs */
		tbls = newStmt(mb,batRef, newRef);
		setVarType(mb, getArg(tbls,0), newBatType(TYPE_oid, TYPE_str));
		tbls = pushType(mb, tbls, TYPE_oid);
		tbls = pushType(mb, tbls, TYPE_str);
		resultset= pushArgument(mb,resultset, getArg(tbls,0));

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q,getArg(tbls,0));
		q= pushStr(mb,q,".trace");
		k= getArg(q,0);

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q,k);
		q= pushStr(mb,q,".trace");

		/* build colum defs */
		cols = newStmt(mb,batRef, newRef);
		setVarType(mb, getArg(cols,0), newBatType(TYPE_oid, TYPE_str));
		cols = pushType(mb, cols, TYPE_oid);
		cols = pushType(mb, cols, TYPE_str);
		resultset= pushArgument(mb,resultset, getArg(cols,0));

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q,getArg(cols,0));
		q= pushStr(mb,q,"usec");
		k= getArg(q,0);

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q, getArg(cols,0));
		q= pushStr(mb,q,"statement");

		/* build type defs */
		types = newStmt(mb,batRef, newRef);
		setVarType(mb, getArg(types,0), newBatType(TYPE_oid, TYPE_str));
		types = pushType(mb, types, TYPE_oid);
		types = pushType(mb, types, TYPE_str);
		resultset= pushArgument(mb,resultset, getArg(types,0));

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q, getArg(types,0));
		q= pushStr(mb,q,"bigint");
		k= getArg(q,0);

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q, k);
		q= pushStr(mb,q,"clob");

		/* build scale defs */
		clen = newStmt(mb,batRef, newRef);
		setVarType(mb, getArg(clen,0), newBatType(TYPE_oid, TYPE_int));
		clen = pushType(mb, clen, TYPE_oid);
		clen = pushType(mb, clen, TYPE_int);
		resultset= pushArgument(mb,resultset, getArg(clen,0));

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q, getArg(clen,0));
		q= pushInt(mb,q,64);
		k= getArg(q,0);

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q, k);
		q= pushInt(mb,q,0);

		/* build scale defs */
		scale = newStmt(mb,batRef, newRef);
		setVarType(mb, getArg(scale,0), newBatType(TYPE_oid, TYPE_int));
		scale = pushType(mb, scale, TYPE_oid);
		scale = pushType(mb, scale, TYPE_int);
		resultset= pushArgument(mb,resultset, getArg(scale,0));

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb,q, getArg(scale,0));
		q= pushInt(mb,q,0);
		k= getArg(q,0);

		q= newStmt(mb,batRef,appendRef);
		q= pushArgument(mb, q, k);
		q= pushInt(mb,q,0);

		/* add the ticks column */

		q = newStmt(mb, profilerRef, "getTrace");
		q = pushStr(mb, q, putName("usec",4));
		resultset= pushArgument(mb,resultset, getArg(q,0));

		/* add the stmt column */
		q = newStmt(mb, profilerRef, "getTrace");
		q = pushStr(mb, q, putName("stmt",4));
		resultset= pushArgument(mb,resultset, getArg(q,0));

		pushInstruction(mb,resultset);
	}
}

#define MAX_QUERY 	(64*1024*1024)

static int
caching(mvc *m)
{
	return m->caching;
}

static int
cachable(mvc *m, stmt *s)
{
	if (m->emode == m_prepare)
		return 1;
	if (m->emode == m_plan || m->type == Q_TRANS ||	/*m->type == Q_SCHEMA || cachable to make sure we have trace on alter statements  */
	    (s && s->type == st_none) || sa_size(m->sa) > MAX_QUERY)
		return 0;
	return 1;
}

/*
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
	int oldvtop, oldstop;
	int pstatus = 0;
	int err = 0, opt = 0;

	be = (backend *) c->sqlcontext;
	if (be == 0) {
		/* tell the client */
		mnstr_printf(out, "!SQL state descriptor missing, aborting\n");
		mnstr_flush(out);
		/* leave a message in the log */
		fprintf(stderr, "SQL state descriptor missing, cannot handle client!\n");
		/* stop here, instead of printing the exception below to the
		 * client in an endless loop */
		c->mode = FINISHCLIENT;
		throw(SQL, "SQLparser", "State descriptor missing");
	}
	oldvtop = c->curprg->def->vtop;
	oldstop = c->curprg->def->stop;
	be->vtop = oldvtop;
#ifdef _SQL_PARSER_DEBUG
	mnstr_printf(GDKout, "#SQL compilation \n");
	printf("debugger? %d(%d)\n", (int) be->mvc->emode, (int) be->mvc->emod);
#endif
	m = be->mvc;
	m->type = Q_PARSE;
	SQLtrans(m);
	pstatus = m->session->status;

	/* sqlparse needs sql allocator to be available.  It can be NULL at
	 * this point if this is a recursive call. */
	if (!m->sa)
		m->sa = sa_create();

	m->emode = m_normal;
	m->emod = mod_none;
	if (be->language == 'X') {
		int n = 0, v, off, len;

		if (strncmp(in->buf + in->pos, "export ", 7) == 0)
			n = sscanf(in->buf + in->pos + 7, "%d %d %d", &v, &off, &len);

		if (n == 2 || n == 3) {
			mvc_export_chunk(be, out, v, off, n == 3 ? len : m->reply_size);

			in->pos = in->len;	/* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "close ", 6) == 0) {
			res_table *t;

			v = (int) strtol(in->buf + in->pos + 6, NULL, 0);
			t = res_tables_find(m->results, v);
			if (t)
				m->results = res_tables_remove(m->results, t);
			in->pos = in->len;	/* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "release ", 8) == 0) {
			cq *q = NULL;

			v = (int) strtol(in->buf + in->pos + 8, NULL, 0);
			if ((q = qc_find(m->qc, v)) != NULL)
				 qc_delete(m->qc, q);
			in->pos = in->len;	/* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "auto_commit ", 12) == 0) {
			int commit;
			v = (int) strtol(in->buf + in->pos + 12, NULL, 10);
			commit = (!m->session->auto_commit && v);
			m->session->auto_commit = (v) != 0;
			m->session->ac_on_commit = m->session->auto_commit;
			if (m->session->active) {
				if (commit && mvc_commit(m, 0, NULL) < 0) {
					mnstr_printf(out, "!COMMIT: commit failed while " "enabling auto_commit\n");
					msg = createException(SQL, "SQLparser", "Xauto_commit (commit) failed");
				} else if (!commit && mvc_rollback(m, 0, NULL) < 0) {
					RECYCLEdrop(0);
					mnstr_printf(out, "!COMMIT: rollback failed while " "disabling auto_commit\n");
					msg = createException(SQL, "SQLparser", "Xauto_commit (rollback) failed");
				}
			}
			in->pos = in->len;	/* HACK: should use parsed length */
			if (msg != NULL)
				goto finalize;
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "reply_size ", 11) == 0) {
			v = (int) strtol(in->buf + in->pos + 11, NULL, 10);
			if (v < -1) {
				msg = createException(SQL, "SQLparser", "reply_size cannot be negative");
				goto finalize;
			}
			m->reply_size = v;
			in->pos = in->len;	/* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "sizeheader", 10) == 0) {
			v = (int) strtol(in->buf + in->pos + 10, NULL, 10);
			m->sizeheader = v != 0;
			in->pos = in->len;	/* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "quit", 4) == 0) {
			c->mode = FINISHCLIENT;
			return MAL_SUCCEED;
		}
		mnstr_printf(out, "!unrecognized X command: %s\n", in->buf + in->pos);
		msg = createException(SQL, "SQLparser", "unrecognized X command");
		goto finalize;
	}
	if (be->language !='S') {
		mnstr_printf(out, "!unrecognized language prefix: %ci\n", be->language);
		msg = createException(SQL, "SQLparser", "unrecognized language prefix: %c", be->language);
		goto finalize;
	}

	if ((err = sqlparse(m)) ||
	    /* Only forget old errors on transaction boundaries */
	    (mvc_status(m) && m->type != Q_TRANS) || !m->sym) {
		if (!err &&m->scanner.started)	/* repeat old errors, with a parsed query */
			err = mvc_status(m);
		if (err) {
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			handle_error(m, c->fdout, pstatus);
		}
		sqlcleanup(m, err);
		goto finalize;
	}
	assert(m->session->schema != NULL);
	/*
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
			mnstr_printf(out, "!07003!EXEC: no prepared statement with id: %d\n", m->sym->data.lval->h->data.i_val);
			msg = createException(SQL, "PREPARE", "no prepared statement with id: %d", m->sym->data.lval->h->data.i_val);
			handle_error(m, c->fdout, pstatus);
			sqlcleanup(m, err);
			goto finalize;
		} else if (be->q->type != Q_PREPARE) {
			err = -1;
			mnstr_printf(out, "!07005!EXEC: given handle id is not for a " "prepared statement: %d\n", m->sym->data.lval->h->data.i_val);
			msg = createException(SQL, "PREPARE", "is not a prepared statement: %d", m->sym->data.lval->h->data.i_val);
			handle_error(m, c->fdout, pstatus);
			sqlcleanup(m, err);
			goto finalize;
		}
		m->emode = m_inplace;
		scanner_query_processed(&(m->scanner));
	} else if (caching(m) && cachable(m, NULL) && m->emode != m_prepare && (be->q = qc_match(m->qc, m->sym, m->args, m->argc, m->scanner.key ^ m->session->schema->base.id)) != NULL) {
		// look for outdated plans
		if ( OPTmitosisPlanOverdue(c, be->q->name) ){
			msg = SQLCacheRemove(c, be->q->name);
			qc_delete(be->mvc->qc, be->q);
			goto recompilequery;
		}

		if (m->emod & mod_debug)
			SQLsetDebugger(c, m, TRUE);
		if (m->emod & mod_trace)
			SQLsetTrace(be, c, TRUE);
		if (!(m->emod & (mod_explain | mod_debug | mod_trace | mod_dot)))
			m->emode = m_inplace;
		scanner_query_processed(&(m->scanner));
	} else {
		sql_rel *r;
		stmt *s;
recompilequery:
		r = sql_symbol2relation(m, m->sym);
		s = sql_relation2stmt(m, r);

		if (s == 0 || (err = mvc_status(m) && m->type != Q_TRANS)) {
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			handle_error(m, c->fdout, pstatus);
			sqlcleanup(m, err);
			goto finalize;
		}
		assert(s);

		/* generate the MAL code */
		if (m->emod & mod_trace)
			SQLsetTrace(be, c, TRUE);
		if (m->emod & mod_debug)
			SQLsetDebugger(c, m, TRUE);
		if (!caching(m) || !cachable(m, s)) {
			scanner_query_processed(&(m->scanner));
			if (backend_callinline(be, c, s, 0) == 0) {
				opt = 1;
			} else {
				err = 1;
			}
		} else {
			/* generate a factory instantiation */
			be->q = qc_insert(m->qc, m->sa,	/* the allocator */
					  r,	/* keep relational query */
					  m->sym,	/* the sql symbol tree */
					  m->args,	/* the argument list */
					  m->argc, m->scanner.key ^ m->session->schema->base.id,	/* the statement hash key */
					  m->emode == m_prepare ? Q_PREPARE : m->type,	/* the type of the statement */
					  sql_escape_str(QUERY(m->scanner)));
			scanner_query_processed(&(m->scanner));
			be->q->code = (backend_code) backend_dumpproc(be, c, be->q, s);
			if (!be->q->code)
				err = 1;
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
	if (err)
		m->session->status = -10;
	if (err == 0) {
		if (be->q) {
			if (m->emode == m_prepare)
				err = mvc_export_prepare(m, c->fdout, be->q, "");
			else if (m->emode == m_inplace) {
				/* everything ready for a fast call */
			} else {	/* call procedure generation (only in cache mode) */
				backend_call(be, c, be->q);
			}
		}

		/* In the final phase we add any debugging control */
		if (m->emod & mod_trace)
			SQLsetTrace(be, c, FALSE);
		if (m->emod & mod_debug)
			SQLsetDebugger(c, m, FALSE);

		/*
		 * During the execution of the query exceptions can be raised.
		 * The default action is to print them out at the end of the
		 * query block.
		 */
		pushEndInstruction(c->curprg->def);

		chkTypes(c->fdout, c->nspace, c->curprg->def, TRUE);	/* resolve types */
		if (opt) {
			MalBlkPtr mb = c->curprg->def;

			trimMalBlk(mb);
			chkProgram(c->fdout, c->nspace, mb);
			addOptimizers(c, mb, "default_pipe");
			msg = optimizeMALBlock(c, mb);
			if (msg != MAL_SUCCEED) {
				sqlcleanup(m, err);
				goto finalize;
			}
			c->curprg->def = mb;
		}
		//printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_ALL);
		/* we know more in this case than chkProgram(c->fdout, c->nspace, c->curprg->def); */
		if (c->curprg->def->errors) {
			showErrors(c);
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, c->glb, oldvtop);
			c->curprg->def->errors = 0;
			msg = createException(PARSE, "SQLparser", "Semantic errors");
		}
	}
      finalize:
	if (msg)
		sqlcleanup(m, 0);
	return msg;
}


str
SQLrecompile(Client c, backend *be)
{
	stmt *s;
	mvc *m = be->mvc;
	int oldvtop = c->curprg->def->vtop;
	int oldstop = c->curprg->def->stop;
	str msg;

	msg = SQLCacheRemove(c, be->q->name);
	if( msg )
		GDKfree(msg);
	s = sql_relation2stmt(m, be->q->rel);
	be->q->code = (backend_code) backend_dumpproc(be, c, be->q, s);
	be->q->stk = 0;

	pushEndInstruction(c->curprg->def);

	chkTypes(c->fdout, c->nspace, c->curprg->def, TRUE);	/* resolve types */
	if (!be->q->code || c->curprg->def->errors) {
		showErrors(c);
		/* restore the state */
		MSresetInstructions(c->curprg->def, oldstop);
		freeVariables(c, c->curprg->def, c->glb, oldvtop);
		c->curprg->def->errors = 0;
		throw(SQL, "SQLrecompile", "M0M27!semantic errors");
	}
	return SQLengineIntern(c, be);
}

str
SQLengine(Client c)
{
	backend *be = (backend *) c->sqlcontext;
	return SQLengineIntern(c, be);
}

str
SQLCacheRemove(Client c, str nme)
{
	Symbol s;

#ifdef _SQL_CACHE_DEBUG
	mnstr_printf(GDKout, "#SQLCacheRemove %s\n", nme);
#endif

	s = findSymbolInModule(c->nspace, nme);
	if (s == NULL)
		throw(MAL, "cache.remove", "internal error, symbol missing\n");
	if (getInstrPtr(s->def, 0)->token == FACTORYsymbol)
		shutdownFactoryByName(c, c->nspace, nme);
	else
		deleteSymbol(c->nspace, s);
	return MAL_SUCCEED;
}
