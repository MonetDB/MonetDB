/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
#include "sql_import.h"
#include "mal.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_debugger.h"
#include "mal_linker.h"
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

#define MAX_SQL_MODULES 128
static int sql_modules = 0;
static struct sql_module {
	const char *name;
	const unsigned char *code;
} sql_module[MAX_SQL_MODULES];

static int
sql_module_compare(const void *a, const void *b)
{
	const struct sql_module *l = a, *r = b;
	return strcmp(l->name, r->name);
}

void
sql_register(const char *name, const unsigned char *code)
{
	assert (sql_modules < MAX_SQL_MODULES);
	sql_module[sql_modules].name = name;
	sql_module[sql_modules].code = code;
	sql_modules++;
}

static sql_store SQLstore = NULL;
int SQLdebug = 0;
static const char *sqlinit = NULL;
static MT_Lock sql_contextLock = MT_LOCK_INITIALIZER(sql_contextLock);

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
		if (!GDKembedded()) {
			freeException(tmp);
			if ((tmp = GDKerrbuf) && *tmp)
				TRC_CRITICAL(SQL_PARSER, SQLSTATE(42000) "GDK reported: %s\n", tmp);
			fflush(stderr);
			exit(1);
		} else {
			return tmp;
		}
	}
	if (!GDKembedded()) {
		fprintf(stdout, "# MonetDB/SQL module loaded\n");
		fflush(stdout);		/* make merovingian see this *now* */
	}
	if (GDKinmemory(0) || GDKembedded()) {
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
	if (SQLstore) {
		mvc_exit(SQLstore);
		SQLstore = NULL;
	}
	MT_lock_unset(&sql_contextLock);
	return MAL_SUCCEED;
}

str
SQLepilogue(void *ret)
{
	char *s = "sql", *m = "msql", *msg;

	(void) ret;
	msg = SQLexit(NULL);
	freeException(msg);
	/* this function is never called, but for the style of it, we clean
	 * up our own mess */
	if (!GDKinmemory(0) && !GDKembedded()) {
		str res = msab_retreatScenario(m);
		if (!res)
			res = msab_retreatScenario(s);
		if (res != NULL) {
			char *err = createException(MAL, "sql.epilogue", "%s", res);
			free(res);
			return err;
		}
	}
	/* return scenarios */
	Scenario sc = findScenario(s);
	if (sc)
		sc->name = NULL;
	sc = findScenario(m);
	if (sc)
		sc->name = NULL;
	return MAL_SUCCEED;
}

static char*
SQLprepareClient(Client c, int login)
{
	mvc *m = NULL;
	backend *be = NULL;
	str msg = MAL_SUCCEED;

	if (c->sqlcontext == 0) {
		sql_allocator *sa = sa_create(NULL);
		if (sa == NULL) {
			msg = createException(SQL,"sql.initClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout2;
		}
		m = mvc_create(SQLstore, sa, c->idx, SQLdebug, c->fdin, c->fdout);
		if (m == NULL) {
			msg = createException(SQL,"sql.initClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout2;
		}
		if (c->scenario && strcmp(c->scenario, "msql") == 0)
			m->reply_size = -1;
		be = (void *) backend_create(m, c);
		if ( be == NULL) {
			mvc_destroy(m);
			msg = createException(SQL,"sql.initClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout2;
		}
	} else {
		assert(0);
#if 0
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
#endif
	}
	MT_lock_unset(&sql_contextLock);
	if (login) {
		switch (monet5_user_set_def_schema(m, c->user)) {
			case -1:
				msg = createException(SQL,"sql.initClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout1;
			case -2:
				msg = createException(SQL,"sql.initClient", SQLSTATE(42000) "The user was not found in the database, this session is going to terminate");
				goto bailout1;
			case -3:
				msg = createException(SQL,"sql.initClient", SQLSTATE(42000) "The user's default schema was not found, this session is going to terminate");
				goto bailout1;
			default:
				break;
		}
	}

	if (c->handshake_options) {
		char *strtok_state = NULL;
		char *tok = strtok_r(c->handshake_options, ",", &strtok_state);
		while (tok != NULL) {
			int value;
			if (sscanf(tok, "auto_commit=%d", &value) == 1) {
				bool auto_commit= value != 0;
				m->session->auto_commit = auto_commit;
				m->session->ac_on_commit = auto_commit;
			} else if (sscanf(tok, "reply_size=%d", &value) == 1) {
				if (value < -1) {
					msg = createException(SQL, "SQLprepareClient", SQLSTATE(42000) "Reply_size cannot be negative");
					goto bailout1;
				}
				m->reply_size = value;
			} else if (sscanf(tok, "size_header=%d", &value) == 1) {
					be->sizeheader = value != 0;
			} else if (sscanf(tok, "columnar_protocol=%d", &value) == 1) {
				c->protocol = (value != 0) ? PROTOCOL_COLUMNAR : PROTOCOL_9;
			} else if (sscanf(tok, "time_zone=%d", &value) == 1) {
				sql_schema *s = mvc_bind_schema(m, "sys");
				sql_var *var = find_global_var(m, s, "current_timezone");
				ValRecord val;
				VALinit(&val, TYPE_lng, &(lng){1000 * value});
				sql_update_var(m, s, "current_timezone", &val);
				sqlvar_set(var, &val);
			} else {
				msg = createException(SQL, "SQLprepareClient", SQLSTATE(42000) "unexpected handshake option: %s", tok);
				goto bailout1;
			}

			tok = strtok_r(NULL, ",", &strtok_state);
		}
	}


bailout1:
	MT_lock_set(&sql_contextLock);
bailout2:
	/* expect SQL text first */
	if (be)
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
		sql_allocator *pa = NULL;
		backend *be = c->sqlcontext;
		mvc *m = be->mvc;

		assert(m->session);
		if (m->session->auto_commit && m->session->tr->active) {
			if (mvc_status(m) >= 0)
				msg = mvc_commit(m, 0, NULL, false);
		}
		if (m->session->tr->active)
			other = mvc_rollback(m, 0, NULL, false);

		res_tables_destroy(be->results);
		be->results = NULL;

		pa = m->pa;
		mvc_destroy(m);
		backend_destroy(be);
		c->state[MAL_SCENARIO_OPTIMIZE] = NULL;
		c->state[MAL_SCENARIO_PARSER] = NULL;
		c->sqlcontext = NULL;
		sa_destroy(pa);
	}
	c->state[MAL_SCENARIO_READER] = NULL;
	if (other && !msg)
		msg = other;
	else if (other && msg)
		freeException(other);
	return msg;
}

MT_Id sqllogthread;

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
	const char *opt_pipe;

	if ((opt_pipe = GDKgetenv("sql_optimizer")) && !isOptimizerPipe(opt_pipe))
		throw(SQL, "sql.init", SQLSTATE(42000) "invalid sql optimizer pipeline %s", opt_pipe);

	MT_lock_set(&sql_contextLock);

	if (SQLstore) {
		MT_lock_unset(&sql_contextLock);
		return MAL_SUCCEED;
	}

	be_funcs.fcode = &monet5_freecode,
	be_funcs.fresolve_function = &monet5_resolve_function,
	be_funcs.fhas_module_function = &monet5_has_module,
	monet5_user_init(&be_funcs);

	if (debug_str)
		SQLdebug = strtol(debug_str, NULL, 10);
	if (single_user)
		SQLdebug |= 64;
	if (readonly)
		SQLdebug |= 32;

	if ((SQLstore = mvc_init(SQLdebug, GDKinmemory(0) ? store_mem : store_bat, readonly, single_user)) == NULL) {
		MT_lock_unset(&sql_contextLock);
		throw(SQL, "SQLinit", SQLSTATE(42000) "Catalogue initialization failed");
	}
	sqlinit = GDKgetenv("sqlinit");
	if (sqlinit) {		/* add sqlinit to the fdin stack */
		buffer *b = (buffer *) GDKmalloc(sizeof(buffer));
		size_t len = strlen(sqlinit);
		char* cbuf = _STRDUP(sqlinit);
		stream *buf;
		bstream *fdin;

		if ( b == NULL || cbuf == NULL) {
			mvc_exit(SQLstore);
			SQLstore = NULL;
			MT_lock_unset(&sql_contextLock);
			GDKfree(b);
			GDKfree(cbuf);
			throw(SQL,"sql.init",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		buffer_init(b, cbuf, len);
		buf = buffer_rastream(b, "si");
		if ( buf == NULL) {
			mvc_exit(SQLstore);
			SQLstore = NULL;
			MT_lock_unset(&sql_contextLock);
			buffer_destroy(b);
			throw(SQL,"sql.init",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		fdin = bstream_create(buf, b->len);
		if ( fdin == NULL) {
			mvc_exit(SQLstore);
			SQLstore = NULL;
			MT_lock_unset(&sql_contextLock);
			buffer_destroy(b);
			throw(SQL,"sql.init",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		bstream_next(fdin);
		if ( MCpushClientInput(c, fdin, 0, "") < 0)
			TRC_ERROR(SQL_PARSER, "Could not switch client input stream\n");
	}
	if ((msg = SQLprepareClient(c, 0)) != NULL) {
		mvc_exit(SQLstore);
		SQLstore = NULL;
		MT_lock_unset(&sql_contextLock);
		TRC_INFO(SQL_PARSER, "%s\n", msg);
		return msg;
	}
	be = c->sqlcontext;
	m = be->mvc;
	/* initialize the database with predefined SQL functions */
	sqlstore *store = SQLstore;
	if (store->first == 0) {
		/* check whether last created object trigger sys.system_update_tables (from 99_system.sql) exists.
		 * if it doesn't, this is probably a restart of the
		 * server after an incomplete initialization */
		if ((msg = SQLtrans(m)) == MAL_SUCCEED) {
			/* TODO there's a going issue with loading triggers due to system tables,
			   so at the moment check for existence of 'json' schema from 40_json.sql */
			if (!mvc_bind_schema(m, "json"))
				store->first = 1;
			msg = mvc_rollback(m, 0, NULL, false);
		}
		if (msg) {
			freeException(msg);
			msg = MAL_SUCCEED;
		}
	}
	if (store->first > 0) {
		store->first = 0;
		maybeupgrade = 0;

		qsort(sql_module, sql_modules, sizeof(sql_module[0]), sql_module_compare);
		for (int i = 0; i < sql_modules && !msg; i++) {
			const char *createdb_inline = (const char*)sql_module[i].code;

			msg = SQLstatementIntern(c, createdb_inline, "sql.init", TRUE, FALSE, NULL);
			if (m->sa)
				sa_destroy(m->sa);
			m->sa = NULL;
		}
		/* 99_system.sql */
		if (!msg) {
			const char *createdb_inline =
				"create trigger system_update_schemas after update on sys.schemas for each statement call sys_update_schemas();\n"
				"create trigger system_update_tables after update on sys._tables for each statement call sys_update_tables();\n"
				/* only system functions until now */
				"update sys.functions set system = true;\n"
				/* only system tables until now */
				"update sys._tables set system = true;\n"
				/* only system schemas until now */
				"update sys.schemas set system = true;\n"
				/* correct invalid FK schema ids, set them to schema id 2000
				 * (the "sys" schema) */
				"update sys.types set schema_id = 2000 where schema_id = 0 and schema_id not in (select id from sys.schemas);\n"
				"update sys.functions set schema_id = 2000 where schema_id = 0 and schema_id not in (select id from sys.schemas);\n";
			msg = SQLstatementIntern(c, createdb_inline, "sql.init", TRUE, FALSE, NULL);
			if (m->sa)
				sa_destroy(m->sa);
			m->sa = NULL;
		}
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
	} else {		/* handle upgrades */
		if (!m->sa)
			m->sa = sa_create(m->pa);
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
		sqlcleanup(be, mvc_status(m));
	}

	other = SQLresetClient(c);
	if (other && !msg) /* 'msg' variable might be set or not, as well as 'other'. Throw the earliest one */
		msg = other;
	else if (other)
		freeException(other);
	if (msg != MAL_SUCCEED) {
		mvc_exit(SQLstore);
		SQLstore = NULL;
		MT_lock_unset(&sql_contextLock);
		return msg;
	}

	if (GDKinmemory(0)) {
		MT_lock_unset(&sql_contextLock);
		return msg;
	}

	if ((sqllogthread = THRcreate((void (*)(void *)) mvc_logmanager, SQLstore, MT_THR_DETACHED, "logmanager")) == 0) {
		mvc_exit(SQLstore);
		SQLstore = NULL;
		MT_lock_unset(&sql_contextLock);
		throw(SQL, "SQLinit", SQLSTATE(42000) "Starting log manager failed");
	}
	if (wlc_state == WLC_STARTUP && GDKgetenv_istrue("wlc_enabled") && (msg = WLCinit()) != MAL_SUCCEED) {
		mvc_exit(SQLstore);
		SQLstore = NULL;
		MT_lock_unset(&sql_contextLock);
		return msg;
	}

	MT_lock_unset(&sql_contextLock);
	return MAL_SUCCEED;
}

#define TRANS_ABORTED SQLSTATE(25005) "Current transaction is aborted (please ROLLBACK)\n"

str
handle_error(mvc *m, int pstatus, str msg)
{
	str new = NULL, newmsg = MAL_SUCCEED;

	/* transaction already broken */
	if (m->type != Q_TRANS && pstatus < 0) {
		freeException(msg);
		return createException(SQL,"sql.execute",TRANS_ABORTED);
	} else if ( GDKerrbuf && GDKerrbuf[0]){
		new = GDKstrdup(GDKerrbuf);
		GDKerrbuf[0] = 0;
	} else if ( *m->errstr){
		new = GDKstrdup(m->errstr);
		m->errstr[0] = 0;
	}
	if ( new && msg){
		newmsg = concatErrors(msg, new);
		GDKfree(new);
	} else if (msg)
		newmsg = msg;
	else if (new) {
		newmsg = createException(SQL, "sql.execute", "%s", new);
		GDKfree(new);
	}
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
	if (!m->session->tr->active) {
		sql_session *s;

		switch (mvc_trans(m)) {
			case -1:
				throw(SQL, "sql.trans", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			case -3:
				throw(SQL, "sql.trans", SQLSTATE(42000) "The session's schema was not found, this transaction won't start");
			default:
				break;
		}
		s = m->session;
		if (!s->schema) {
			switch (monet5_user_get_def_schema(m, m->user_id, &s->schema_name)) {
				case -1:
					mvc_cancel_session(m);
					throw(SQL, "sql.trans", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				case -2:
					mvc_cancel_session(m);
					throw(SQL, "sql.trans", SQLSTATE(42000) "The user was not found in the database, this session is going to terminate");
				case -3:
					mvc_cancel_session(m);
					throw(SQL, "sql.trans", SQLSTATE(42000) "The user's default schema was not found, this session is going to terminate");
				default:
					break;
			}
			if (!(s->schema = find_sql_schema(s->tr, s->schema_name))) {
				mvc_cancel_session(m);
				throw(SQL, "sql.trans", SQLSTATE(42000) "The session's schema was not found, this session is going to terminate");
			}
		}
	}
	return MAL_SUCCEED;
}

str
SQLinitClient(Client c)
{
	str msg = MAL_SUCCEED;

	MT_lock_set(&sql_contextLock);
	if (!SQLstore) {
		MT_lock_unset(&sql_contextLock);
		throw(SQL, "SQLinitClient", SQLSTATE(42000) "Catalogue not available");
	}
	msg = SQLprepareClient(c, true);
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
	if (!SQLstore) {
		MT_lock_unset(&sql_contextLock);
		throw(SQL, "SQLexitClient", SQLSTATE(42000) "Catalogue not available");
	}
	err = SQLresetClient(c);
	MT_lock_unset(&sql_contextLock);
	if (err != MAL_SUCCEED)
		return err;
	err = MALexitClient(c);
	if (err != MAL_SUCCEED)
		return err;
	return MAL_SUCCEED;
}

str
SQLstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const char *expr = *getArgReference_str(stk, pci, 1);

	(void) mb;

	protocol_version backup = cntxt->protocol;

	if (pci->argc == 3 && *getArgReference_bit(stk, pci, 2))
		cntxt->protocol = PROTOCOL_COLUMNAR;

	str msg = SQLstatementIntern(cntxt, expr, "SQLstatement", TRUE, TRUE, NULL);

	cntxt->protocol = backup;

	return msg;
}

str
SQLcompile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = getArgReference_str(stk, pci, 0);
	str *expr = getArgReference_str(stk, pci, 1);
	str msg;

	(void) mb;
	*ret = NULL;
	if ((msg = SQLstatementIntern(cntxt, *expr, "SQLcompile", FALSE, FALSE, NULL)) != MAL_SUCCEED)
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
	mvc *m;
	size_t sz;

	fullname = MSP_locate_sqlscript(*name, 0);
	if (fullname == NULL)
		fullname = *name;
	fd = open_rastream(fullname);
	if (mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
		close_stream(fd);
		throw(MAL, "sql.include", SQLSTATE(42000) "%s\n", mnstr_peek_error(NULL));
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

	msg = SQLstatementIntern(cntxt, bfd->buf, "sql.include", TRUE, FALSE, NULL);
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

	if (!SQLstore || !be || c->mode <= FINISHCLIENT) {
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

				if (go && ((!blocked && mnstr_write(c->fdout, c->prompt, c->promptlength, 1) != 1) || mnstr_flush(c->fdout, MNSTR_FLUSH_DATA))) {
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

str
SQLparser(Client c)
{
	bstream *in = c->fdin;
	stream *out = c->fdout;
	str msg = NULL;
	backend *be;
	mvc *m;
	int oldvtop, oldstop, oldvid, ok;
	int pstatus = 0;
	int err = 0, opt, preparedid = -1;

	c->query = NULL;
	be = (backend *) c->sqlcontext;
	if (be == 0) {
		/* leave a message in the log */
		TRC_ERROR(SQL_PARSER, "SQL state description is missing, cannot handle client!\n");
		/* stop here, instead of printing the exception below to the
		 * client in an endless loop */
		c->mode = FINISHCLIENT;
		throw(SQL, "SQLparser", SQLSTATE(42000) "State descriptor missing, aborting");
	}
	oldvid = c->curprg->def->vid;
	oldvtop = c->curprg->def->vtop;
	oldstop = c->curprg->def->stop;
	be->vtop = oldvtop;
	be->vid = oldvid;

	m = be->mvc;
	m->type = Q_PARSE;
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
		m->sa = sa_create(m->pa);
	if (!m->sa) {
		c->mode = FINISHCLIENT;
		throw(SQL, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL " for SQL allocator");
	}
	if (eb_savepoint(&m->sa->eb)) {
		sa_reset(m->sa);

		throw(SQL, "SQLparser", SQLSTATE(HY001) MAL_MALLOC_FAIL " for SQL allocator");
	}
	opt = 0;
	preparedid = -1;

	m->emode = m_normal;
	m->emod = mod_none;
	if (be->language == 'X') {
		int n = 0, v, off, len;

		if (strncmp(in->buf + in->pos, "export ", 7) == 0)
			n = sscanf(in->buf + in->pos + 7, "%d %d %d", &v, &off, &len);

		if (n == 2 || n == 3) {
			if (n == 2)
				len = m->reply_size;
			if ((ok = mvc_export_chunk(be, out, v, off, len < 0 ? BUN_NONE : (BUN) len)) < 0) {
				msg = createException(SQL, "SQLparser", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(be, out, ok));
				goto finalize;
			}

			in->pos = in->len;	/* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "close ", 6) == 0) {
			res_table *t;

			v = (int) strtol(in->buf + in->pos + 6, NULL, 0);
			t = res_tables_find(be->results, v);
			if (t)
				be->results = res_tables_remove(be->results, t);
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
		static const char* columnar_protocol = "columnar_protocol ";
		if (strncmp(in->buf + in->pos, columnar_protocol, strlen(columnar_protocol)) == 0) {
			v = (int) strtol(in->buf + in->pos + strlen(columnar_protocol), NULL, 10);

			c->protocol = v?PROTOCOL_COLUMNAR:PROTOCOL_9;

			in->pos = in->len;	/* HACK: should use parsed length */
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
		if (strncmp(in->buf + in->pos, "sizeheader", 10) == 0) { // no underscore
			v = (int) strtol(in->buf + in->pos + 10, NULL, 10);
			be->sizeheader = v != 0;
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
		sqlcleanup(be, err);
		goto finalize;
	}
	/*
	 * We have dealt with the first parsing step and advanced the input reader
	 * to the next statement (if any).
	 * Now is the time to also perform the semantic analysis, optimize and
	 * produce code.
	 */
	be->q = NULL;
	c->query = query_cleaned(m->sa, QUERY(m->scanner));

	if (c->query == NULL) {
		err = 1;
		msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else if (m->emode == m_deallocate) {
		AtomNode *an = (AtomNode *) m->sym;
		assert(m->sym->type == type_symbol && an->a->data.vtype == TYPE_int);
		preparedid = an->a->data.val.ival;

		if (preparedid > -1) /* The -1 case represents the deallocate the entire query cache */
			be->q = qc_find(m->qc, preparedid);

		if (preparedid > -1) {
			const char *mode = "DEALLOC";
			if (!be->q) {
				err = -1;
				msg = createException(SQL, mode, SQLSTATE(07003) "No prepared statement with id: %d\n", preparedid);
				*m->errstr = 0;
				msg = handle_error(m, pstatus, msg);
				sqlcleanup(be, err);
				goto finalize;
			}
		}

		m->type = Q_SCHEMA; /* TODO DEALLOCATE statements don't fit for Q_SCHEMA */
		scanner_query_processed(&(m->scanner));
	} else {
		sql_rel *r = sql_symbol2relation(be, m->sym);

		if (!r || (err = mvc_status(m) && m->type != Q_TRANS && *m->errstr)) {
			if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
				msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			else
				msg = createException(PARSE, "SQLparser", SQLSTATE(42000) "%s", m->errstr);
			*m->errstr = 0;
			msg = handle_error(m, pstatus, msg);
			sqlcleanup(be, err);
			goto finalize;
		}

		if (m->emode != m_prepare) {
			scanner_query_processed(&(m->scanner));

			err = 0;
			setVarType(c->curprg->def, 0, 0);
			if (be->subbackend && be->subbackend->check(be->subbackend, r)) {
				res_table *rt = NULL;
				if (be->subbackend->exec(be->subbackend, r, be->result_id++, &rt) == NULL) { /* on error fall back */
					if (rt) {
						rt->next = be->results;
						be->results = rt;
					}
					return NULL;
				}
			}

			if (backend_dumpstmt(be, c->curprg->def, r, !(m->emod & mod_exec), 0, c->query) < 0)
				err = 1;
			else
				opt = (m->emod & mod_exec) == 0;//1;
		} else {
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
				assert(strlen(be->q->name) < IDLENGTH);
				be->q->name = putName(be->q->name);
				if (!be->q->name) {
					err = 1;
					msg = createException(PARSE, "SQLparser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
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
				qc_clean(m->qc);
			}
			/* For deallocate statements just export a simple output */
			if (!GDKembedded() && (err = mvc_export_operation(be, c->fdout, "", c->curprg->def->starttime, c->curprg->def->optimize)) < 0) {
				msg = createException(PARSE, "SQLparser", SQLSTATE(45000) "Export operation failed: %s", mvc_export_error(be, c->fdout, err));
				MSresetInstructions(c->curprg->def, oldstop);
				freeVariables(c, c->curprg->def, NULL, oldvtop, oldvid);
				goto finalize;
			}
		} else if (be->q) {
			assert(m->emode == m_prepare);
			/* For prepared queries, return a table with result set structure*/
			/* optimize the code block and rename it */
			if ((err = mvc_export_prepare(be, c->fdout)) < 0) {
				msg = createException(PARSE, "SQLparser", SQLSTATE(45000) "Export operation failed: %s", mvc_export_error(be, c->fdout, err));
				MSresetInstructions(c->curprg->def, oldstop);
				freeVariables(c, c->curprg->def, NULL, oldvtop, oldvid);
				goto finalize;
			}
		}

		pushEndInstruction(c->curprg->def);
		/* check the query wrapper for errors */
		if( msg == MAL_SUCCEED)
			msg = chkTypes(c->usermodule, c->curprg->def, TRUE);

		/* in case we had produced a non-cachable plan, the optimizer should be called */
		if (msg == MAL_SUCCEED && opt ) {
			msg = SQLoptimizeQuery(c, c->curprg->def);

			if (msg != MAL_SUCCEED) {
				str other = c->curprg->def->errors;
				/* In debugging mode you may want to assess what went wrong in the optimizers*/
#ifndef NDEBUG
				if( m->emod & mod_debug)
					runMALDebugger(c, c->curprg->def);
#endif
				c->curprg->def->errors = 0;
				MSresetInstructions(c->curprg->def, oldstop);
				freeVariables(c, c->curprg->def, NULL, oldvtop, oldvid);
				if (other != msg)
					freeException(other);
				goto finalize;
			}
		}
		//printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_ALL);
		/* we know more in this case than chkProgram(c->fdout, c->usermodule, c->curprg->def); */
		if (msg == MAL_SUCCEED && c->curprg->def->errors) {
			msg = c->curprg->def->errors;
			c->curprg->def->errors = 0;
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c, c->curprg->def, NULL, oldvtop, oldvid);
			if (msg == NULL && *m->errstr){
				if (strlen(m->errstr) > 6 && m->errstr[5] == '!')
					msg = createException(PARSE, "SQLparser", "%s", m->errstr);
				else
					msg = createException(PARSE, "SQLparser", SQLSTATE(M0M27) "Semantic errors %s", m->errstr);
				*m->errstr = 0;
			} else if (msg) {
				str newmsg = createException(PARSE, "SQLparser", SQLSTATE(M0M27) "Semantic errors %s", msg);
				freeException(msg);
				msg = newmsg;
			}
		}
	}
finalize:
	if (msg) {
		sqlcleanup(be, 0);
		c->query = NULL;
	}
	return msg;
}

str
SQLengine(Client c)
{
	backend *be = (backend *) c->sqlcontext;
	if (be && be->subbackend)
		be->subbackend->reset(be->subbackend);
	return SQLengineIntern(c, be);
}

str
SQLcallback(Client c, str msg)
{
	if (msg) {
		/* remove exception decoration */
		for (char *m = msg; m && *m; ) {
			char *n = strchr(m, '\n');
			char *s = getExceptionMessageAndState(m);
			mnstr_printf(c->fdout, "!%.*s\n", (int) (n - s), s);
			m = n;
			if (n) {
				m++; /* include newline */
			}
		}
		freeException(msg);
		return MAL_SUCCEED;
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
