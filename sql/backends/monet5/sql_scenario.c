/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * (authors) N. Nes, M.L. Kersten
 * The SQL scenario implementation is a derivative of the MAL session scenario.
 *
 */
/*
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
#include "sql_user.h"
#include "sql_datetime.h"
#include "mal_io.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_debugger.h"
#include "mal_linker.h"
#include "mal_runtime.h"
#include "mal_utils.h"
#include "bat5.h"
#include "wlc.h"
#include "wlr.h"
#include "msabaoth.h"
#include "gdk_time.h"
#include "optimizer.h"
#include "opt_prelude.h"
#include "opt_pipes.h"
#include "opt_mitosis.h"
#include <unistd.h>
#include "sql_upgrades.h"

static int SQLinitialized = 0;
static int SQLnewcatalog = 0;
int SQLdebug = 0;
static const char *sqlinit = NULL;
static MT_Lock sql_contextLock = MT_LOCK_INITIALIZER("sql_contextLock");

static void
monet5_freestack(int clientid, backend_stack stk)
{
	MalStkPtr p = (ptr) stk;

	(void) clientid;
	if (p != NULL)
		freeStack(p);
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
		freeException(msg);	/* do something with error? */
}

static str SQLinit(Client c);

str
//SQLprelude(void *ret)
SQLprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str tmp;
	Scenario ms, s = getFreeScenario();

	(void) mb;
	(void) stk;
	(void) pci;
	if (!s)
		throw(MAL, "sql.start", SQLSTATE(42000) "out of scenario slots");
	sqlinit = GDKgetenv("sqlinit");
	*s = (struct SCENARIO) {
		.name = "S_Q_L",
		.language = "sql",
		.exitSystem = "SQLexit",
		.exitSystemCmd = SQLexit,
		.initClient = "SQLinitClient",
		.initClientCmd = SQLinitClient,
		.exitClient = "SQLexitClient",
		.exitClientCmd = SQLexitClient,
		.reader = "SQLreader",
		.readerCmd = SQLreader,
		.parser = "SQLparser",
		.parserCmd = SQLparser,
		.engine = "SQLengine",
		.engineCmd = SQLengine,
		.callback = "SQLcallback",
		.callbackCmd = SQLcallback,
	};
	ms = getFreeScenario();
	if (!ms)
		throw(MAL, "sql.start", SQLSTATE(42000) "out of scenario slots");

	*ms = (struct SCENARIO) {
		.name = "M_S_Q_L",
		.language = "msql",
		.exitSystem = "SQLexit",
		.exitSystemCmd = SQLexit,
		.initClient = "SQLinitClientFromMAL",
		.initClientCmd = SQLinitClientFromMAL,
		.exitClient = "SQLexitClient",
		.exitClientCmd = SQLexitClient,
		.reader = "MALreader",
		.readerCmd = MALreader,
		.parser = "MALparser",
		.parserCmd = MALparser,
		.optimizer = "MALoptimizer",
		.optimizerCmd = MALoptimizer,
		.engine = "MALengine",
		.engineCmd = MALengine,
		.callback = "MALcallback",
		.callbackCmd = MALcallback,
	};

	tmp = SQLinit(cntxt);
	if (tmp != MAL_SUCCEED) {
		TRC_CRITICAL(SQL_PARSER, "Fatal error during initialization: %s\n", tmp);
		freeException(tmp);
		if ((tmp = GDKerrbuf) && *tmp)
			TRC_CRITICAL(SQL_PARSER, SQLSTATE(42000) "GDK reported: %s\n", tmp);
		fflush(stderr);
		exit(1);
	}
#ifndef HAVE_EMBEDDED
	fprintf(stdout, "# MonetDB/SQL module loaded\n");
	fflush(stdout);		/* make merovingian see this *now* */
#endif
	if (GDKinmemory()) {
		s->name = "sql";
		ms->name = "msql";
		return MAL_SUCCEED;
	}
	/* only register availability of scenarios AFTER we are inited! */
	s->name = "sql";
	tmp = msab_marchScenario(s->name);
	if (tmp != NULL) {
		char *err = createException(MAL, "sql.start", "%s", tmp);
		free(tmp);
		return err;
	}
	ms->name = "msql";
	tmp = msab_marchScenario(ms->name);
	if (tmp != NULL) {
		char *err = createException(MAL, "sql.start", "%s", tmp);
		free(tmp);
		return err;
	}
	return MAL_SUCCEED;
}

str
SQLexit(Client c)
{
	(void) c;		/* not used */
	MT_lock_set(&sql_contextLock);
	if (SQLinitialized) {
		mvc_exit();
		SQLinitialized = FALSE;
	}
	MT_lock_unset(&sql_contextLock);
	return MAL_SUCCEED;
}

str
SQLepilogue(void *ret)
{
	char *s = "sql", *m = "msql";
	str res;

	(void) ret;
	(void) SQLexit(NULL);
	/* this function is never called, but for the style of it, we clean
	 * up our own mess */
	if (!GDKinmemory()) {
		res = msab_retreatScenario(m);
		if (!res)
			res = msab_retreatScenario(s);
		if (res != NULL) {
			char *err = createException(MAL, "sql.start", "%s", res);
			free(res);
			return err;
		}
	}
	return MAL_SUCCEED;
}

#define SQLglobal(name, val) \
	if (!stack_push_var(sql, name, &ctype) || !stack_set_var(sql, name, VALset(&src, ctype.type->localtype, (char*)(val)))) \
		failure--;

/* NR_GLOBAL_VAR should match exactly the number of variables created in global_variables */
/* initialize the global variable, ie make mvc point to these */
static int
global_variables(mvc *sql, const char *user, const char *schema)
{
	sql_subtype ctype;
	lng sec = 0;
	ValRecord src;
	const char *opt;
	int failure = 0;

	sql_find_subtype(&ctype, "int", 0, 0);
	SQLglobal("debug", &sql->debug);
	SQLglobal("cache", &sql->cache);

	sql_find_subtype(&ctype,  "varchar", 1024, 0);
	SQLglobal("current_schema", schema);
	SQLglobal("current_user", user);
	SQLglobal("current_role", user);

	/* inherit the optimizer from the server */
	opt = GDKgetenv("sql_optimizer");
	if (!opt)
		opt = "default_pipe";
	SQLglobal("optimizer", opt);

	sql_find_subtype(&ctype, "sec_interval", inttype2digits(ihour, isec), 0);
	SQLglobal("current_timezone", &sec);

	sql_find_subtype(&ctype, "bigint", 0, 0);
	SQLglobal("last_id", &sql->last_id);
	SQLglobal("rowcnt", &sql->rowcnt);
	return failure;
}

static const char *
SQLgetquery(Client c)
{
	if (c) {
		backend *be = c->sqlcontext;
		if (be) {
			mvc *m = be->mvc;
			if (m)
				return m->query;
		}
	}
	return NULL;
}

static char*
SQLprepareClient(Client c, int login)
{
	mvc *m = NULL;
	backend *be = NULL;
	str msg = MAL_SUCCEED;

	c->getquery = SQLgetquery;
	if (c->sqlcontext == 0) {
		m = mvc_create(c->idx, 0, SQLdebug, c->fdin, c->fdout);
		if (m == NULL) {
			msg = createException(SQL,"sql.initClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (global_variables(m, "monetdb", "sys") < 0) {
			mvc_destroy(m);
			msg = createException(SQL,"sql.initClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (c->scenario && strcmp(c->scenario, "msql") == 0)
			m->reply_size = -1;
		be = (void *) backend_create(m, c);
		if ( be == NULL) {
			msg = createException(SQL,"sql.initClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	} else {
		be = c->sqlcontext;
		m = be->mvc;
		/* Only reset if there is no active transaction which
		 * can happen when we combine sql.init with msql.
		*/
		if (m->session->tr->active)
			return NULL;
		if (mvc_reset(m, c->fdin, c->fdout, SQLdebug) < 0) {
			msg = createException(SQL,"sql.initClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		backend_reset(be);
	}
	if (m->session->tr)
		reset_functions(m->session->tr);
	MT_lock_unset(&sql_contextLock);
	if (login) {
		str schema = monet5_user_set_def_schema(m, c->user);
		if (!schema) {
			msg = createException(PERMD,"sql.initClient", SQLSTATE(08004) "Schema authorization error");
			goto bailout;
		}
		_DELETE(schema);
	}

bailout:
	MT_lock_set(&sql_contextLock);
	/*expect SQL text first */
	be->language = 'S';
	/* Set state, this indicates an initialized client scenario */
	c->state[MAL_SCENARIO_READER] = c;
	c->state[MAL_SCENARIO_PARSER] = c;
	c->state[MAL_SCENARIO_OPTIMIZE] = c;
	c->sqlcontext = be;
	if (msg)
		c->mode = FINISHCLIENT;
	return msg;
}

str
SQLresetClient(Client c)
{
	str msg = MAL_SUCCEED, other = MAL_SUCCEED;

	if (c->sqlcontext == NULL)
		throw(SQL, "SQLexitClient", SQLSTATE(42000) "MVC catalogue not available");
	if (c->sqlcontext) {
		backend *be = c->sqlcontext;
		mvc *m = be->mvc;

		assert(m->session);
		if (m->session->auto_commit && m->session->tr->active) {
			if (mvc_status(m) >= 0)
				msg = mvc_commit(m, 0, NULL, false);
		}
		if (m->session->tr->active)
			other = mvc_rollback(m, 0, NULL, false);

		res_tables_destroy(m->results);
		m->results = NULL;

		mvc_destroy(m);
		backend_destroy(be);
		c->state[MAL_SCENARIO_OPTIMIZE] = NULL;
		c->state[MAL_SCENARIO_PARSER] = NULL;
		c->sqlcontext = NULL;
	}
	c->state[MAL_SCENARIO_READER] = NULL;
	if (other && !msg)
		msg = other;
	else if (other && msg)
		freeException(other);
	return msg;
}

MT_Id sqllogthread, idlethread;

#ifdef HAVE_EMBEDDED
extern char* createdb_inline;
#endif

static str
SQLinit(Client c)
{
	const char *debug_str = GDKgetenv("sql_debug");
	char *msg = MAL_SUCCEED, *other = MAL_SUCCEED;
	bool readonly = GDKgetenv_isyes("gdk_readonly");
	bool single_user = GDKgetenv_isyes("gdk_single_user");
	static int maybeupgrade = 1;
	backend *be = NULL;
	mvc *m = NULL;

	MT_lock_set(&sql_contextLock);

	if (SQLinitialized) {
		MT_lock_unset(&sql_contextLock);
		return MAL_SUCCEED;
	}

	be_funcs = (backend_functions) {
		.fstack = &monet5_freestack,
		.fcode = &monet5_freecode,
		.fresolve_function = &monet5_resolve_function,
	};
	monet5_user_init(&be_funcs);

	if (debug_str)
		SQLdebug = strtol(debug_str, NULL, 10);
	if (single_user)
		SQLdebug |= 64;
	if (readonly)
		SQLdebug |= 32;
	if ((SQLnewcatalog = mvc_init(SQLdebug, GDKinmemory() ? store_mem : store_bat, readonly, single_user, 0)) < 0) {
		MT_lock_unset(&sql_contextLock);
		throw(SQL, "SQLinit", SQLSTATE(42000) "Catalogue initialization failed");
	}
	SQLinitialized = TRUE;
	sqlinit = GDKgetenv("sqlinit");
	if (sqlinit) {		/* add sqlinit to the fdin stack */
		buffer *b = (buffer *) GDKmalloc(sizeof(buffer));
		size_t len = strlen(sqlinit);
		char* cbuf = _STRDUP(sqlinit);
		stream *buf;
		bstream *fdin;

		if ( b == NULL || cbuf == NULL) {
			MT_lock_unset(&sql_contextLock);
			GDKfree(b);
			GDKfree(cbuf);
			throw(SQL,"sql.init",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		buffer_init(b, cbuf, len);
		buf = buffer_rastream(b, "si");
		if ( buf == NULL) {
			MT_lock_unset(&sql_contextLock);
			buffer_destroy(b);
			throw(SQL,"sql.init",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		fdin = bstream_create(buf, b->len);
		if ( fdin == NULL) {
			MT_lock_unset(&sql_contextLock);
			buffer_destroy(b);
			throw(SQL,"sql.init",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		bstream_next(fdin);
		if ( MCpushClientInput(c, fdin, 0, "") < 0)
			TRC_ERROR(SQL_PARSER, "Could not switch client input stream\n");
	}
	if ((msg = SQLprepareClient(c, 0)) != NULL) {
		MT_lock_unset(&sql_contextLock);
		TRC_INFO(SQL_PARSER, "%s\n", msg);
		return msg;
	}
	be = c->sqlcontext;
	m = be->mvc;
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
		SQLnewcatalog = 0;
		maybeupgrade = 0;

#ifdef HAVE_EMBEDDED
		size_t createdb_len = strlen(createdb_inline);
		buffer* createdb_buf;
		stream* createdb_stream;
		bstream* createdb_bstream;
		if ((createdb_buf = GDKmalloc(sizeof(buffer))) == NULL) {
			MT_lock_unset(&sql_contextLock);
			throw(MAL, "createdb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		buffer_init(createdb_buf, createdb_inline, createdb_len);
		if ((createdb_stream = buffer_rastream(createdb_buf, "createdb.sql")) == NULL) {
			MT_lock_unset(&sql_contextLock);
			GDKfree(createdb_buf);
			throw(MAL, "createdb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if ((createdb_bstream = bstream_create(createdb_stream, createdb_len)) == NULL) {
			MT_lock_unset(&sql_contextLock);
			close_stream(createdb_stream);
			GDKfree(createdb_buf);
			throw(MAL, "createdb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (bstream_next(createdb_bstream) >= 0)
			msg = SQLstatementIntern(c, &createdb_bstream->buf, "sql.init", TRUE, FALSE, NULL);
		else
			msg = createException(MAL, "createdb", SQLSTATE(42000) "Could not load inlined createdb script");

		bstream_destroy(createdb_bstream);
		GDKfree(createdb_buf);
		if (m->sa)
			sa_destroy(m->sa);
		m->sa = NULL;

#else
		char path[FILENAME_MAX];
		str fullname;

		snprintf(path, FILENAME_MAX, "createdb");
		slash_2_dir_sep(path);
		fullname = MSP_locate_sqlscript(path, 1);
		if (fullname) {
			str filename = fullname, p, n;

			fprintf(stdout, "# SQL catalog created, loading sql scripts once\n");
			do {
				stream *fd = NULL;

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
						close_stream(fd);
						msg = createException(MAL, "createdb", SQLSTATE(42000) "File %s too large to process", filename);
					} else {
						bstream *bfd = NULL;

						if ((bfd = bstream_create(fd, sz == 0 ? (size_t) (128 * BLOCK) : sz)) == NULL) {
							close_stream(fd);
							msg = createException(MAL, "createdb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						} else {
							if (bstream_next(bfd) >= 0)
								msg = SQLstatementIntern(c, &bfd->buf, "sql.init", TRUE, FALSE, NULL);
							bstream_destroy(bfd);
						}
					}
				} else
					msg = createException(MAL, "createdb", SQLSTATE(HY013) "Couldn't open file %s", filename);
			} while (p && msg == MAL_SUCCEED);
			GDKfree(fullname);
		} else
			msg = createException(MAL, "createdb", SQLSTATE(HY013) "Could not read createdb.sql");

		/* Commit after all the startup scripts have been processed */
		assert(m->session->tr->active);
		if (mvc_status(m) < 0 || msg)
			other = mvc_rollback(m, 0, NULL, false);
		else
			other = mvc_commit(m, 0, NULL, false);

		if (other && !msg) /* 'msg' variable might be set or not, as well as 'other'. Throw the earliest one */
			msg = other;
		else if (other)
			freeException(other);

		if (msg)
			TRC_INFO(SQL_PARSER, "%s\n", msg);
#endif
	} else {		/* handle upgrades */
		if (!m->sa)
			m->sa = sa_create();
		if (!m->sa) {
			msg = createException(MAL, "createdb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else if (maybeupgrade) {
			if ((msg = SQLtrans(m)) == MAL_SUCCEED) {
				int res = SQLupgrades(c, m);
				/* Commit at the end of the upgrade */
				assert(m->session->tr->active);
				if (mvc_status(m) < 0 || res)
					msg = mvc_rollback(m, 0, NULL, false);
				else
					msg = mvc_commit(m, 0, NULL, false);
			}
		}
		maybeupgrade = 0;
	}
	fflush(stdout);
	fflush(stderr);

	/* send error from create scripts back to the first client */
	if (msg) {
		msg = handle_error(m, 0, msg);
		*m->errstr = 0;
		sqlcleanup(m, mvc_status(m));
	}

	other = SQLresetClient(c);
	MT_lock_unset(&sql_contextLock);
	if (other && !msg) /* 'msg' variable might be set or not, as well as 'other'. Throw the earliest one */
		msg = other;
	else if (other)
		freeException(other);
	if (msg != MAL_SUCCEED)
		return msg;

	if (GDKinmemory())
		return MAL_SUCCEED;

	if ((sqllogthread = THRcreate((void (*)(void *)) mvc_logmanager, NULL, MT_THR_DETACHED, "logmanager")) == 0) {
		throw(SQL, "SQLinit", SQLSTATE(42000) "Starting log manager failed");
	}
	if (!(SQLdebug&1024)) {
		if ((idlethread = THRcreate((void (*)(void *)) mvc_idlemanager, NULL, MT_THR_DETACHED, "idlemanager")) == 0) {
			throw(SQL, "SQLinit", SQLSTATE(42000) "Starting idle manager failed");
		}
	}
	if ( wlc_state == WLC_STARTUP)
		return WLCinit();
	return MAL_SUCCEED;
}

#define TRANS_ABORTED SQLSTATE(25005) "Current transaction is aborted (please ROLLBACK)\n"

str
handle_error(mvc *m, int pstatus, str msg)
{
	str new = 0, newmsg= MAL_SUCCEED;

	/* transaction already broken */
	if (m->type != Q_TRANS && pstatus < 0) {
		new = createException(SQL,"sql.execute",TRANS_ABORTED);
	} else if ( GDKerrbuf && GDKerrbuf[0]){
		new = GDKstrdup(GDKerrbuf);
		GDKerrbuf[0] = 0;
	} else if ( *m->errstr){
		new = GDKstrdup(m->errstr);
		m->errstr[0] = 0;
	}
	if ( new && msg){
		newmsg = GDKzalloc( strlen(msg) + strlen(new) + 64);
		if (newmsg == NULL) {
			newmsg = createException(SQL, "sql.execute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else {
			strcpy(newmsg, msg);
			/* strcat(newmsg,"!"); */
			strcat(newmsg,new);
		}
		freeException(new);
		freeException(msg);
	} else
	if ( msg)
		newmsg = msg;
	else
	if ( new)
		newmsg = new;
	return newmsg;
}

str
SQLautocommit(mvc *m)
{
	str msg = MAL_SUCCEED;

	if (m->session->auto_commit && m->session->tr->active) {
		if (mvc_status(m) < 0) {
			msg = mvc_rollback(m, 0, NULL, false);
		} else {
			msg = mvc_commit(m, 0, NULL, false);
		}
	}
	return msg;
}

str
SQLtrans(mvc *m)
{
	m->caching = m->cache;
	if (!m->session->tr->active) {
		sql_session *s;

		if (mvc_trans(m) < 0)
			throw(SQL, "sql.trans", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		s = m->session;
		if (!s->schema) {
			if (s->schema_name)
				GDKfree(s->schema_name);
			s->schema_name = monet5_user_get_def_schema(m, m->user_id);
			if (!s->schema_name) {
				mvc_cancel_session(m);
				throw(SQL, "sql.trans", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			assert(s->schema_name);
			s->schema = find_sql_schema(s->tr, s->schema_name);
			assert(s->schema);
		}
	}
	return MAL_SUCCEED;
}

str
SQLinitClient(Client c)
{
	str msg = MAL_SUCCEED;

	MT_lock_set(&sql_contextLock);
	if (SQLinitialized == 0) {// && (msg = SQLprelude(NULL)) != MAL_SUCCEED)
		MT_lock_unset(&sql_contextLock);
		return msg;
	}
#ifndef HAVE_EMBEDDED
	msg = SQLprepareClient(c, 1);
#else
	msg = SQLprepareClient(c, 0);
#endif
	MT_lock_unset(&sql_contextLock);
	return msg;
}

str
SQLinitClientFromMAL(Client c)
{
	str msg = MAL_SUCCEED;

	if ((msg = SQLinitClient(c)) != MAL_SUCCEED) {
		c->mode = FINISHCLIENT;
		return msg;
	}

	mvc* m = ((backend*) c->sqlcontext)->mvc;

	/* Crucial step:
	 * MAL scripts that interact with the sql module
	 * must have a properly initialized transaction.
	 */
	if ((msg = SQLtrans(m)) != MAL_SUCCEED) {
		c->mode = FINISHCLIENT;
		return msg;
	}
	return msg;
}

str
SQLexitClient(Client c)
{
	str err;

	MT_lock_set(&sql_contextLock);
	if (SQLinitialized == FALSE) {
		MT_lock_unset(&sql_contextLock);
		throw(SQL, "SQLexitClient", SQLSTATE(42000) "Catalogue not available");
	}
	err = SQLresetClient(c);
	MT_lock_unset(&sql_contextLock);
	if (err != MAL_SUCCEED)
		return err;
	MALexitClient(c);
	return MAL_SUCCEED;
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
	if ((msg = SQLstatementIntern(cntxt, expr, "SQLcompile", FALSE, FALSE, NULL)) != MAL_SUCCEED)
		return msg;
	if ((*ret = _STRDUP("SQLcompile")) == NULL)
		throw(SQL,"sql.compile",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
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
		close_stream(fd);
		throw(MAL, "sql.include", SQLSTATE(42000) "could not open file: %s\n", *name);
	}
	sz = getFileSize(fd);
	if (sz > (size_t) 1 << 29) {
		close_stream(fd);
		throw(MAL, "sql.include", SQLSTATE(42000) "file %s too large to process", fullname);
	}
	if ((bfd = bstream_create(fd, sz == 0 ? (size_t) (128 * BLOCK) : sz)) == NULL) {
		close_stream(fd);
		throw(MAL, "sql.include", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (bstream_next(bfd) < 0) {
		bstream_destroy(bfd);
		throw(MAL, "sql.include", SQLSTATE(42000) "could not read %s\n", *name);
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

str
SQLreader(Client c)
{
	bool go = true;
	str msg = MAL_SUCCEED;
	bool more = true;
	bool commit_done = false;
	backend *be = (backend *) c->sqlcontext;
	bstream *in = c->fdin;
	int language = -1;
	mvc *m = NULL;
	bool blocked = isa_block_stream(in->s);
	int isSQLinitialized;

	MT_lock_set(&sql_contextLock);
	isSQLinitialized = SQLinitialized;
	MT_lock_unset(&sql_contextLock);

	if (isSQLinitialized == FALSE) {
		c->mode = FINISHCLIENT;
		return MAL_SUCCEED;
	}
	if (!be || c->mode <= FINISHCLIENT) {
		c->mode = FINISHCLIENT;
		return MAL_SUCCEED;
	}
	language = be->language;	/* 'S' for SQL, 'D' from debugger */
	m = be->mvc;
	m->errstr[0] = 0;
	/*
	 * Continue processing any left-over input from the previous round.
	 */

	while (more) {
		more = false;

		/* Different kinds of supported statements sequences
		   A;   -- single line                  s
		   A \n B;      -- multi line                   S
		   A; B;   -- compound single block     s
		   A;   -- many multi line
		   B \n C; -- statements in one block   S
		 */
		/* auto_commit on end of statement */
		if (language != 'D' && m->scanner.mode == LINE_N && !commit_done) {
			msg = SQLautocommit(m);
			if (msg)
				break;
			commit_done = true;
		}
		if (m->session->tr && m->session->tr->active)
			c->idle = 0;

		if (go && in->pos >= in->len) {
			ssize_t rd;

			if (c->bak) {
				in = c->fdin;
				blocked = isa_block_stream(in->s);
				m->scanner.rs = c->fdin;
				c->fdin->pos += c->yycur;
				c->yycur = 0;
			}
			if (in->eof || !blocked) {
				if (language != 'D')
					language = 0;

				/* The rules of auto_commit require us to finish
				   and start a transaction on the start of a new statement (s A;B; case) */
				if (language != 'D' && !(m->emod & mod_debug) && !commit_done) {
					msg = SQLautocommit(m);
					if (msg)
						break;
					commit_done = true;
				}

				if (go && ((!blocked && mnstr_write(c->fdout, c->prompt, c->promptlength, 1) != 1) || mnstr_flush(c->fdout))) {
					go = false;
					break;
				}
				in->eof = false;
			}
			if (in->buf == NULL) {
				more = false;
				go = false;
			} else if (go && (rd = bstream_next(in)) <= 0) {
				if (be->language == 'D' && !in->eof) {
					in->pos++;// skip 's' or 'S'
					return msg;
				}

				if (rd == 0 && language !=0 && in->eof) {
					/* we hadn't seen the EOF before, so just try again
					   (this time with prompt) */
					more = true;
					continue;
				}
				go = false;
				break;
			} else if (go && language == 0) {
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
			} else if (go && language == 'D' && !in->eof) {
				in->pos++;// skip 's' or 'S'
			}
		}
	}
	if ( (c->sessiontimeout && (GDKusec() - c->session) > c->sessiontimeout) || !go || (strncmp(CURRENT(c), "\\q", 2) == 0)) {
		in->pos = in->len;	/* skip rest of the input */
		c->mode = FINISHCLIENT;
		return msg;
	}
	return msg;
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

#define MAX_QUERY 	(64*1024*1024)

static int
caching(mvc *m)
{
	return m->caching;
}

static int
cachable(mvc *m, sql_rel *r)
{
	if (m->emode == m_prepare)	/* prepared plans are always cached */
		return 1;
	if (m->emode == m_plan)		/* we plan to display without execution */
		return 0;
	if (m->type == Q_TRANS )	/* m->type == Q_SCHEMA || cachable to make sure we have trace on alter statements  */
		return 0;
	/* we don't store queries with a large footprint */
	if (r && sa_size(m->sa) > MAX_QUERY)
		return 0;
	return 1;
}

/*
 * The core part of the SQL interface, parse the query and
 * store away the template (non)optimized code in the query cache
 * and the MAL module
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
	int err = 0, opt = 0, preparedid = -1;
	char *q = NULL;
	oid tag = 0;

	be = (backend *) c->sqlcontext;
	if (be == 0) {
		/* leave a message in the log */
		TRC_ERROR(SQL_PARSER, "SQL state description is missing, cannot handle client!\n");
		/* stop here, instead of printing the exception below to the
		 * client in an endless loop */
		c->mode = FINISHCLIENT;
		throw(SQL, "SQLparser", SQLSTATE(42000) "State descriptor missing, aborting");
	}
	oldvtop = c->curprg->def->vtop;
	oldstop = c->curprg->def->stop;
	be->vtop = oldvtop;
	m = be->mvc;
	m->type = Q_PARSE;
	m->Topt = 0;
	/* clean up old stuff */
	q = m->query;
	m->query = NULL;
	GDKfree(q);		/* may be NULL */

	if (be->language != 'X') {
		if ((msg = SQLtrans(m)) != MAL_SUCCEED) {
			c->mode = FINISHCLIENT;
			return msg;
		}
	}
	pstatus = m->session->status;

	/* sqlparse needs sql allocator to be available.  It can be NULL at
	 * this point if this is a recursive call. */
	if (!m->sa)
		m->sa = sa_create();
	if (!m->sa) {
		c->mode = FINISHCLIENT;
		throw(SQL, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL " for SQL allocator");
	}

	m->emode = m_normal;
	m->emod = mod_none;
	if (be->language == 'X') {
		int n = 0, v, off, len;

		if (strncmp(in->buf + in->pos, "export ", 7) == 0)
			n = sscanf(in->buf + in->pos + 7, "%d %d %d", &v, &off, &len);

		if (n == 2 || n == 3) {
			if (n == 2)
				len = m->reply_size;
			if (mvc_export_chunk(be, out, v, off, len < 0 ? BUN_NONE : (BUN) len)) {
				msg = createException(SQL, "SQLparser", SQLSTATE(45000) "Result set construction failed");
				goto finalize;
			}

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
			if (m->session->tr->active) {
				if (commit) {
					msg = mvc_commit(m, 0, NULL, true);
				} else {
					msg = mvc_rollback(m, 0, NULL, true);
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
				msg = createException(SQL, "SQLparser", SQLSTATE(42000) "Reply_size cannot be negative");
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
		msg = createException(SQL, "SQLparser", SQLSTATE(42000) "Unrecognized X command: %s\n", in->buf + in->pos);
		goto finalize;
	}
	if (be->language !='S') {
		msg = createException(SQL, "SQLparser", SQLSTATE(42000) "Unrecognized language prefix: %ci\n", be->language);
		in->pos = in->len;	/* skip rest of the input */
		c->mode = FINISHCLIENT; /* and disconnect, as client doesn't respect the mapi protocol */
		goto finalize;
	}

	// generate and set the tag in the mal block of the clients current program.
	tag = runtimeProfileSetTag(c);
	assert(tag == c->curprg->def->tag);
	(void) tag;

	if(malProfileMode > 0)
		generic_event("sql_parse",
					 (struct GenericEvent)
					 { &c->idx, &(c->curprg->def->tag), NULL, NULL, 0 },
					 0);

	if ((err = sqlparse(m)) ||
	    /* Only forget old errors on transaction boundaries */
	    (mvc_status(m) && m->type != Q_TRANS) || !m->sym) {
		if (!err &&m->scanner.started)	/* repeat old errors, with a parsed query */
			err = mvc_status(m);
		if (err && *m->errstr) {
			if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
				msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			else
				msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
			*m->errstr = 0;
		}
		if (m->sym)
			msg = handle_error(m, pstatus, msg);
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
	q = query_cleaned(QUERY(m->scanner));
	m->query = q;

	if(malProfileMode > 0) {
		str escaped_query = m->query ? mal_quote(m->query, strlen(m->query)) : NULL;
		generic_event("sql_parse",
					  (struct GenericEvent)
					  { &c->idx, &(c->curprg->def->tag), NULL, escaped_query, m->query ? 0 : 1, },
					  1);
		GDKfree(escaped_query);
	}

	if (q == NULL) {
		err = 1;
		msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else if (m->emode == m_execute || m->emode == m_deallocate) {
		if (m->emode == m_execute) {
			assert(m->sym->data.lval->h->type == type_int);
			preparedid = m->sym->data.lval->h->data.i_val;

			assert(preparedid >= 0);
			be->q = qc_find(m->qc, preparedid);
		} else { /* m_deallocate case */
			AtomNode *an = (AtomNode *) m->sym;
			assert(m->sym->type == type_symbol && an->a->data.vtype == TYPE_int);
			preparedid = an->a->data.val.ival;

			if (preparedid > -1) /* The -1 case represents the deallocate the entire query cache */
				be->q = qc_find(m->qc, preparedid);
		}

		if (preparedid > -1) {
			const char *mode = (m->emode == m_execute) ? "EXEC" : "DEALLOC";
			if (!be->q) {
				err = -1;
				msg = createException(SQL, mode, SQLSTATE(07003) "No prepared statement with id: %d\n", preparedid);
				*m->errstr = 0;
				msg = handle_error(m, pstatus, msg);
				sqlcleanup(m, err);
				goto finalize;
			} else if (!be->q->prepared) {
				err = -1;
				msg = createException(SQL, mode, SQLSTATE(07005) "Given handle id is not for a prepared statement: %d\n", preparedid);
				*m->errstr = 0;
				msg = handle_error(m, pstatus, msg);
				sqlcleanup(m, err);
				goto finalize;
			}
		}

		m->type = (m->emode == m_execute) ? be->q->type : Q_SCHEMA; /* TODO DEALLOCATE statements don't fit for Q_SCHEMA */
		scanner_query_processed(&(m->scanner));
	} else if (caching(m) && cachable(m, NULL) && m->emode != m_prepare && (be->q = qc_match(m->qc, m, m->sym, m->args, m->argc, m->scanner.key ^ m->session->schema->base.id)) != NULL) {
		/* query template was found in the query cache */
		scanner_query_processed(&(m->scanner));
		m->no_mitosis = be->q->no_mitosis;
	} else {
		sql_rel *r = sql_symbol2relation(m, m->sym);

		if (!r || (err = mvc_status(m) && m->type != Q_TRANS && *m->errstr)) {
			if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
				msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			else
				msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
			*m->errstr = 0;
			msg = handle_error(m, pstatus, msg);
			sqlcleanup(m, err);
			goto finalize;
		}

		if ((!caching(m) || !cachable(m, r)) && m->emode != m_prepare) {
			/* Query template should not be cached */
			scanner_query_processed(&(m->scanner));

			err = 0;

			if(malProfileMode > 0)
				generic_event("rel_to_mal",
							  (struct GenericEvent)
							  { &c->idx, &(c->curprg->def->tag), NULL, NULL, m->query? 0 : 1 },
							  0);

			if (backend_callinline(be, c) < 0 ||
			    backend_dumpstmt(be, c->curprg->def, r, 1, 0, q) < 0)
				err = 1;
			else opt = 1;

			if(malProfileMode > 0)
				generic_event("rel_to_mal",
							  (struct GenericEvent)
							  { &c->idx, &(c->curprg->def->tag), NULL, NULL, m->query? 0 : 1 },
							  1);
		} else {
			/* Add the query tree to the SQL query cache
			 * and bake a MAL program for it. */
			char *q_copy = GDKstrdup(q), qname[IDLENGTH];

			be->q = NULL;
			(void) snprintf(qname, IDLENGTH, "%c%d_%d", (m->emode == m_prepare?'p':'s'), m->qc->id++, m->qc->clientid);
			if (!q_copy) {
				err = 1;
				msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			} else {
				be->q = qc_insert(m->qc, m->sa,	/* the allocator */
						  r,	/* keep relational query */
						  qname, /* its MAL name */
						  m->sym,	/* the sql symbol tree */
						  m->args,	/* the argument list */
						  m->argc, m->scanner.key ^ m->session->schema->base.id,	/* the statement hash key */
						  m->type,	/* the type of the statement */
						  q_copy,
						  m->no_mitosis,
						  m->emode == m_prepare);
			}
			if (!be->q) {
				err = 1;
				msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			scanner_query_processed(&(m->scanner));
			be->q->code = (backend_code) backend_dumpproc(be, c, be->q, r);
			if (!be->q->code)
				err = 1;
			be->q->stk = 0;

			/* passed over to query cache, used during dumpproc */
			m->sa = NULL;
			m->sym = NULL;
			/* register name in the namespace */
			be->q->name = putName(be->q->name);
			if (!be->q->name) {
				err = 1;
				msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	}
	if (err)
		m->session->status = -10;
	if (err == 0) {
		/* no parsing error encountered, finalize the code of the query wrapper */
		if (m->emode == m_deallocate) {
			assert(be->q || preparedid == -1);
			if (be->q) {
				qc_delete(m->qc, be->q);
			} else {
				qc_clean(m->qc, true);
			}
			/* For deallocate statements just export a simple output */
			err = mvc_export_operation(be, c->fdout, "", c->curprg->def->starttime, c->curprg->def->optimize);
		} else if (be->q) {
			if (m->emode == m_prepare) {
				/* For prepared queries, return a table with result set structure*/
				/* optimize the code block and rename it */
				err = mvc_export_prepare(m, c->fdout, be->q, "");
			} else if (m->emode == m_execute || m->emode == m_normal || m->emode == m_plan) {
				/* call procedure generation (only in cache mode) */
				if (backend_call(be, c, be->q) < 0)
					err = 3;
			}
		}

		if (!err) {
			pushEndInstruction(c->curprg->def);
			/* check the query wrapper for errors */
			if( msg == MAL_SUCCEED)
				msg = chkTypes(c->usermodule, c->curprg->def, TRUE);

			/* in case we had produced a non-cachable plan, the optimizer should be called */
			if (msg == MAL_SUCCEED && opt ) {

				if(malProfileMode > 0)
					generic_event("mal_opt",
								  (struct GenericEvent)
								  { &c->idx, &(c->curprg->def->tag), NULL, NULL, 0 },
								  0);

				msg = SQLoptimizeQuery(c, c->curprg->def);

				if(malProfileMode > 0)
					generic_event("mal_opt",
								  (struct GenericEvent)
								  { &c->idx, &(c->curprg->def->tag), NULL, NULL, msg == MAL_SUCCEED? 0 : 1 },
								  1);

				if (msg != MAL_SUCCEED) {
					str other = c->curprg->def->errors;
					c->curprg->def->errors = 0;
					MSresetInstructions(c->curprg->def, oldstop);
					freeVariables(c, c->curprg->def, NULL, oldvtop);
					if (other != msg)
						freeException(other);
					goto finalize;
				}
			}
		}
		//printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_ALL);
		/* we know more in this case than chkProgram(c->fdout, c->usermodule, c->curprg->def); */
		if (msg == MAL_SUCCEED && c->curprg->def->errors) {
			msg = c->curprg->def->errors;
			c->curprg->def->errors = 0;
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, NULL, oldvtop);
			if (msg == NULL && *m->errstr){
				if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
					msg = createException(PARSE, "SQLparser", "%s", m->errstr);
				else
					msg = createException(PARSE, "SQLparser", SQLSTATE(M0M27) "Semantic errors %s", m->errstr);
				*m->errstr = 0;
			} else if (msg) {
				str newmsg;
				newmsg = createException(PARSE, "SQLparser", SQLSTATE(M0M27) "Semantic errors %s", msg);
				freeException(msg);
				msg = newmsg;
			}
		}
	}
finalize:
	if (msg) {
		sqlcleanup(m, 0);
		q = m->query;
		m->query = NULL;
		GDKfree(q);
	}
	return msg;
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

	s = findSymbolInModule(c->usermodule, nme);
	if (s == NULL)
		throw(MAL, "cache.remove", SQLSTATE(42000) "internal error, symbol missing\n");
	deleteSymbol(c->usermodule, s);
	return MAL_SUCCEED;
}

str
SQLcallback(Client c, str msg)
{
	char *newerr = NULL;

	if (msg) {
		if (!(newerr = GDKmalloc(strlen(msg) + 1))) {
			msg = createException(SQL, "SQLcallback", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else {
			/* remove exception decoration */
			char *m, *n, *p, *s;
			size_t l;

			m = msg;
			p = newerr;
			while (m && *m) {
				n = strchr(m, '\n');
				s = getExceptionMessageAndState(m);
				if (n) {
					n++; /* include newline */
					l = n - s;
				} else {
					l = strlen(s);
				}
				memcpy(p, s, l);
				p += l;
				m = n;
			}
			*p = 0;
			freeException(msg);
			if (!(msg = GDKrealloc(newerr, strlen(newerr) + 1))) {
				GDKfree(newerr);
				msg = createException(SQL, "SQLcallback", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	}
	return MALcallback(c, msg);
}

str
SYSupdate_tables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = ((backend *) cntxt->sqlcontext)->mvc;

	(void) mb;
	(void) stk;
	(void) pci;

	sql_trans_update_tables(m->session->tr, mvc_bind_schema(m, "sys"));
	return MAL_SUCCEED;
}

str
SYSupdate_schemas(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = ((backend *) cntxt->sqlcontext)->mvc;

	(void) mb;
	(void) stk;
	(void) pci;

	sql_trans_update_schemas(m->session->tr);
	return MAL_SUCCEED;
}
