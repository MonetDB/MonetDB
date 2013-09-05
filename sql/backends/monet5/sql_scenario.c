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
 * Copyright August 2008-2013 MonetDB B.V.
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
#include "mal_backend.h"
#include "sql_scenario.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_env.h"
#include "sql_mvc.h"
#include "sql_readline.h"
#include "sql_user.h"
#include "sql_datetime.h"
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
#include <unistd.h>

static int SQLinitialized = 0;
static int SQLnewcatalog = 0;
static int SQLdebug = 0;
static char *sqlinit = NULL;
MT_Lock sql_contextLock MT_LOCK_INITIALIZER("sql_contextLock");

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
	if (SQLinitialized == 0 && (msg = SQLprelude()) != MAL_SUCCEED)
		return msg;
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
	if (SQLinitialized == 0 && (msg = SQLprelude()) != MAL_SUCCEED)
		return msg;
	msg = setScenario(cntxt, "msql");
	*ret = 0;
	return msg;
}

static str SQLinit(void);

str
SQLprelude(void)
{
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
		return(tmp);
	fprintf(stdout, "# MonetDB/SQL module loaded\n");
	fflush(stdout); /* make merovingian see this *now* */

	/* only register availability of scenarios AFTER we are inited! */
	s->name = "sql";
	tmp = msab_marchScenario(s->name);
	if (tmp != MAL_SUCCEED)
		return(tmp);
	ms->name = "msql";
	tmp = msab_marchScenario(ms->name);
	return tmp;
}

str
SQLepilogue(void)
{
	char *s = "sql", *m = "msql";
	str res;

	if( SQLinitialized){
		mvc_exit();
		SQLinitialized= FALSE;
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
	char *debug_str = GDKgetenv("sql_debug");
	int readonly  = GDKgetenv_isyes("gdk_readonly");
	int single_user = GDKgetenv_isyes("gdk_single_user");
	char *gmt = "GMT";
	tzone tz;

#ifdef _SQL_SCENARIO_DEBUG
	mnstr_printf(GDKout, "#SQLinit Monet 5\n");
#endif
	if (SQLinitialized)
		return MAL_SUCCEED;

#ifdef NEED_MT_LOCK_INIT
	MT_lock_init( &sql_contextLock, "sql_contextLock");
#endif

	MT_lock_set(&sql_contextLock, "SQL init");
	memset((char*)&be_funcs, 0, sizeof(backend_functions));
	be_funcs.fstack		= &monet5_freestack;
	be_funcs.fcode		= &monet5_freecode;
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
	if ((SQLnewcatalog = mvc_init(FALSE, store_bat, readonly, single_user, 0)) < 0)
		throw(SQL, "SQLinit", "Catalogue initialization failed");
	SQLinitialized = TRUE;
	MT_lock_unset(&sql_contextLock, "SQL init");
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
	SQLglobal("trace","show,ticks,stmt");

	typename = "sec_interval";
	sql_find_subtype(&ctype, typename, inttype2digits(ihour, isec), 0);
	SQLglobal("current_timezone", &sec);

	typename = "boolean";
	sql_find_subtype(&ctype, typename, 0, 0);
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

#define TRANS_ABORTED "!25005!current transaction is aborted (please ROLLBACK)\n"

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

static str
sql_update_dec2011( Client c, mvc *m )
{
	node *nsch, *ntab, *ncol;
	sql_trans *tr;
	char *buf = GDKmalloc(1024), *err = NULL;
	size_t bufsize = 1024, pos = 0;

	buf[0] = 0;
 	tr = m->session->tr;
	for( nsch = tr->schemas.set->h; nsch; nsch= nsch->next) {
		sql_schema *s = nsch->data;

		if ( isalpha((int)s->base.name[0]) ) {
			if (!s->tables.set)
				continue;
			for( ntab = (s)->tables.set->h ;ntab; ntab = ntab->next){
				sql_table *t = ntab->data;

				if (!isTable(t) || !t->columns.set)
					continue;
				for ( ncol = (t)->columns.set->h; ncol; ncol= ncol->next){
					sql_column *c = (sql_column *) ncol->data;

					if (c->type.type->eclass == EC_INTERVAL &&
					    strcmp(c->type.type->sqlname, "sec_interval") == 0) {
						while (bufsize < pos + 100 + strlen(s->base.name) + strlen(t->base.name) + 3*strlen(c->base.name))
							buf = GDKrealloc(buf, bufsize += 1024);
						pos += snprintf(buf+pos, bufsize-pos, "update \"%s\".\"%s\" set \"%s\"=1000*\"%s\" where \"%s\" is not null;\n",
							s->base.name, t->base.name, c->base.name, c->base.name, c->base.name);
					}
				}
			}
		}
	}
	if (bufsize < pos + 400)
		buf = GDKrealloc(buf, bufsize += 1024);
	pos += snprintf(buf+pos, bufsize-pos, "create filter function sys.\"like\"(val string, pat string, esc string) external name pcre.like_filter;\n");
	pos += snprintf(buf+pos, bufsize-pos, "create filter function sys.\"ilike\"(val string, pat string, esc string) external name pcre.ilike_filter;\n");
	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('ilike', 'like') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_FILT);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_apr2012(Client c)
{
	char *buf = GDKmalloc(2048), *err = NULL;
	size_t bufsize = 2048, pos = 0;

	/* sys.median and sys.corr functions */
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.median(val TINYINT) returns TINYINT external name \"aggr\".\"median\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.median(val SMALLINT) returns SMALLINT external name \"aggr\".\"median\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.median(val INTEGER) returns INTEGER external name \"aggr\".\"median\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.median(val BIGINT) returns BIGINT external name \"aggr\".\"median\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.median(val REAL) returns REAL external name \"aggr\".\"median\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.median(val DOUBLE) returns DOUBLE external name \"aggr\".\"median\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.corr(e1 TINYINT, e2 TINYINT) returns TINYINT external name \"aggr\".\"corr\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.corr(e1 SMALLINT, e2 SMALLINT) returns SMALLINT external name \"aggr\".\"corr\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.corr(e1 INTEGER, e2 INTEGER) returns INTEGER external name \"aggr\".\"corr\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.corr(e1 BIGINT, e2 BIGINT) returns BIGINT external name \"aggr\".\"corr\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.corr(e1 REAL, e2 REAL) returns REAL external name \"aggr\".\"corr\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.corr(e1 DOUBLE, e2 DOUBLE) returns DOUBLE external name \"aggr\".\"corr\";\n");

	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('median', 'corr') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_AGGR);

	assert(pos < 2048);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_apr2012_sp1(Client c)
{
	char *buf = GDKmalloc(2048), *err = NULL;
	size_t bufsize = 2048, pos = 0;

	/* changes in createdb/25_debug.sql */
	pos += snprintf(buf+pos, bufsize-pos, "update sys.functions set type = %d, side_effect = false where type = %d and id not in (select func_id from sys.args where number = 0 and name = 'result');\n", F_PROC, F_FUNC);
	pos += snprintf(buf+pos, bufsize-pos, "drop function sys.storage;\n");
	pos += snprintf(buf+pos, bufsize-pos, "create function sys.storage() returns table (\"schema\" string, \"table\" string, \"column\" string, location string, \"count\" bigint, capacity bigint, width int, size bigint, hashsize bigint, sorted boolean) external name sql.storage;\n");
	pos += snprintf(buf+pos, bufsize-pos, "create function sys.optimizers() returns table (name string, def string, status string) external name sql.optimizers;\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop procedure sys.ra;\n");
	pos += snprintf(buf+pos, bufsize-pos, "create procedure sys.evalAlgebra( ra_stmt string, opt bool) external name sql.\"evalAlgebra\";\n");

	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('storage', 'optimizers') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_FUNC);
	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('evalalgebra') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_PROC);

	assert(pos < 2048);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jul2012(Client c)
{
	char *buf = GDKmalloc(2048), *err = NULL;
	size_t bufsize = 2048, pos = 0;

	/* new function sys.alpha */
	pos += snprintf(buf+pos, bufsize-pos, "create function sys.alpha(pdec double, pradius double) returns double external name sql.alpha;\n");

	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name = 'alpha' and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_FUNC);

	assert(pos < 2048);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2012(Client c)
{
	char *buf = GDKmalloc(2048), *err = NULL;
	size_t bufsize = 2048, pos = 0;

	/* new function sys.alpha */
	pos += snprintf(buf+pos, bufsize-pos, "drop function sys.zorder_slice;\n");

	assert(pos < 2048);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2012_sp1(Client c)
{
	char *buf = GDKmalloc(2048), *err = NULL;
	size_t bufsize = 2048, pos = 0;

	/* sys.stddev functions */
	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(TINYINT);\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(SMALLINT);\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(INTEGER);\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(BIGINT);\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(REAL);\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(DOUBLE);\n");

	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(DATE);\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(TIME);\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop aggregate sys.stddev(TIMESTAMP);\n");

	pos += snprintf(buf + pos, bufsize-pos, "delete from sys.systemfunctions where function_id in (select f.id from sys.functions f, sys.schemas s where f.name = 'stddev' and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_AGGR);

	assert(pos < 2048);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_feb2013(Client c)
{
	char *buf = GDKmalloc(4096), *err = NULL;
	size_t bufsize = 4096, pos = 0;

	/* sys.stddev_samp functions */
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val TINYINT) returns DOUBLE external name \"aggr\".\"stdev\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val SMALLINT) returns DOUBLE external name \"aggr\".\"stdev\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val INTEGER) returns DOUBLE external name \"aggr\".\"stdev\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val BIGINT) returns DOUBLE external name \"aggr\".\"stdev\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val REAL) returns DOUBLE external name \"aggr\".\"stdev\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val DOUBLE) returns DOUBLE external name \"aggr\".\"stdev\";\n");

	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val DATE) returns DOUBLE external name \"aggr\".\"stdev\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val TIME) returns DOUBLE external name \"aggr\".\"stdev\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val TIMESTAMP) returns DOUBLE external name \"aggr\".\"stdev\";\n");

	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val TINYINT) returns DOUBLE external name \"aggr\".\"stdevp\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val SMALLINT) returns DOUBLE external name \"aggr\".\"stdevp\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val INTEGER) returns DOUBLE external name \"aggr\".\"stdevp\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val BIGINT) returns DOUBLE external name \"aggr\".\"stdevp\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val REAL) returns DOUBLE external name \"aggr\".\"stdevp\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val DOUBLE) returns DOUBLE external name \"aggr\".\"stdevp\";\n");

	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val DATE) returns DOUBLE external name \"aggr\".\"stdevp\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val TIME) returns DOUBLE external name \"aggr\".\"stdevp\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val TIMESTAMP) returns DOUBLE external name \"aggr\".\"stdevp\";\n");

	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val TINYINT) returns DOUBLE external name \"aggr\".\"variance\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val SMALLINT) returns DOUBLE external name \"aggr\".\"variance\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val INTEGER) returns DOUBLE external name \"aggr\".\"variance\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val BIGINT) returns DOUBLE external name \"aggr\".\"variance\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val REAL) returns DOUBLE external name \"aggr\".\"variance\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val DOUBLE) returns DOUBLE external name \"aggr\".\"variance\";\n");

	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val DATE) returns DOUBLE external name \"aggr\".\"variance\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val TIME) returns DOUBLE external name \"aggr\".\"variance\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val TIMESTAMP) returns DOUBLE external name \"aggr\".\"variance\";\n");

	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val TINYINT) returns DOUBLE external name \"aggr\".\"variancep\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val SMALLINT) returns DOUBLE external name \"aggr\".\"variancep\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val INTEGER) returns DOUBLE external name \"aggr\".\"variancep\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val BIGINT) returns DOUBLE external name \"aggr\".\"variancep\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val REAL) returns DOUBLE external name \"aggr\".\"variancep\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val DOUBLE) returns DOUBLE external name \"aggr\".\"variancep\";\n");

	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val DATE) returns DOUBLE external name \"aggr\".\"variancep\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val TIME) returns DOUBLE external name \"aggr\".\"variancep\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val TIMESTAMP) returns DOUBLE external name \"aggr\".\"variancep\";\n");

	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('stddev_samp', 'stddev_pop', 'var_samp', 'var_pop') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_AGGR);

	assert(pos < 4096);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}


static str
sql_update_feb2013_sp1(Client c)
{
	size_t bufsize = 10240, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *fullname;
	FILE *fp = NULL;

	snprintf(buf, bufsize, "createdb%c75_storagemodel", DIR_SEP);
 	if ((fullname = MSP_locate_sqlscript(buf, 1)) != NULL) {
		fp = fopen(fullname, "r");
		GDKfree(fullname);
	}

	/* sys.stddev functions */
	pos += snprintf(buf+pos, bufsize-pos, "drop filter function sys.\"like\"(string, string, string);\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop filter function sys.\"ilike\"(string, string, string);\n");
	pos += snprintf(buf+pos, bufsize-pos, "create filter function sys.\"like\"(val string, pat string, esc string) external name algebra.likesubselect;\n");
	pos += snprintf(buf+pos, bufsize-pos, "create filter function sys.\"ilike\"(val string, pat string, esc string) external name algebra.ilikesubselect;\n");

	pos += snprintf(buf+pos, bufsize-pos, "drop function sys.storage;\n");
	if (fp) {
		pos += fread(buf+pos, 1, bufsize-pos, fp);
		fclose(fp);
	}

	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('like', 'ilike') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_FILT);
	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('storage', 'columnsize', 'heapsize', 'indexsize', 'storagemodel') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_FUNC);
	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name = 'storagemodelinit' and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_PROC);

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_feb2013_sp3(Client c)
{
	char *buf = GDKmalloc(4096), *err = NULL;
	size_t bufsize = 4096, pos = 0;

	/* aggregates on type WRD */
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_samp(val WRD) returns DOUBLE external name \"aggr\".\"stdev\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.stddev_pop(val WRD) returns DOUBLE external name \"aggr\".\"stdevp\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_samp(val WRD) returns DOUBLE external name \"aggr\".\"variance\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.var_pop(val WRD) returns DOUBLE external name \"aggr\".\"variancep\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.median(val WRD) returns WRD external name \"aggr\".\"median\";\n");
	pos += snprintf(buf+pos, bufsize-pos, "create aggregate sys.corr(e1 WRD, e2 WRD) returns WRD external name \"aggr\".\"corr\";\n");

	pos += snprintf(buf + pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'median', 'corr') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_AGGR);

	assert(pos < 4096);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2013(Client c)
{
	char *buf = GDKmalloc(10240), *err = NULL;
	size_t bufsize = 10240, pos = 0;
	char *fullname;
	FILE *fp1 = NULL, *fp2 = NULL, *fp3 = NULL;
	ValRecord *schvar = stack_get_var(((backend *) c->sqlcontext)->mvc, "current_schema");
	char *schema = NULL;

	if (schvar)
		schema = strdup(schvar->val.sval);

	snprintf(buf, bufsize, "createdb%c15_querylog", DIR_SEP);
 	if ((fullname = MSP_locate_sqlscript(buf, 1)) != NULL) {
		fp1 = fopen(fullname, "r");
		GDKfree(fullname);
	}
	snprintf(buf, bufsize, "createdb%c26_sysmon", DIR_SEP);
 	if ((fullname = MSP_locate_sqlscript(buf, 1)) != NULL) {
		fp2 = fopen(fullname, "r");
		GDKfree(fullname);
	}
	snprintf(buf, bufsize, "createdb%c40_json", DIR_SEP);
 	if ((fullname = MSP_locate_sqlscript(buf, 1)) != NULL) {
		fp3 = fopen(fullname, "r");
		GDKfree(fullname);
	}

	pos += snprintf(buf+pos, bufsize-pos, "set schema \"sys\";\n");

	/* new entry in 16_tracelog.sql */
	pos += snprintf(buf+pos, bufsize-pos, "create view sys.tracelog as select * from sys.tracelog();\n");

	/* deleted entry from 22_clients.sql */
	pos += snprintf(buf+pos, bufsize-pos, "drop function sys.clients;\n");

	/* added entry in 25_debug.sql */
	pos += snprintf(buf+pos, bufsize-pos, "create view sys.optimizers as select * from sys.optimizers();\n");
	pos += snprintf(buf+pos, bufsize-pos, "create view sys.environment as select * from sys.environment();\n");

	/* added entry in 75_storagemodel.sql */
	pos += snprintf(buf+pos, bufsize-pos, "create view sys.storage as select * from sys.storage();\n");
	pos += snprintf(buf+pos, bufsize-pos, "create view sys.storagemodel as select * from sys.storagemodel();\n");

	/* replaced 15_history.sql by 15_querylog.sql */
	pos += snprintf(buf+pos, bufsize-pos, "drop procedure sys.resetHistory;\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop procedure sys.keepCall;\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop procedure sys.keepQuery;\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop view sys.queryLog;\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop table sys.callHistory;\n");
	pos += snprintf(buf+pos, bufsize-pos, "drop table sys.queryHistory;\n");
	if (fp1) {
		pos += fread(buf+pos, 1, bufsize-pos, fp1);
		fclose(fp1);
	}

	/* new file 26_sysmon.sql */
	if (fp2) {
		pos += fread(buf+pos, 1, bufsize-pos, fp2);
		fclose(fp2);
	}

	/* new file 40_json.sql */
	if (fp3) {
		pos += fread(buf+pos, 1, bufsize-pos, fp3);
		fclose(fp3);
	}

	pos += snprintf(buf+pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('querylog_catalog', 'querylog_calls', 'queue', 'json_filter', 'json_filter_all', 'json_path', 'json_text', 'json_isvalid', 'json_isvalidobject', 'json_isvalidarray', 'json_length') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_FUNC);
	pos += snprintf(buf+pos, bufsize-pos, "insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('querylog_empty', 'querylog_enable', 'querylog_disable', 'pause', 'resume', 'sysmon_resume', 'stop') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_PROC);
	pos += snprintf(buf+pos, bufsize-pos, "update sys._tables set system = true where name in ('tracelog', 'optimizers', 'environment', 'storage', 'storagemodel') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	if (schema) {
		pos += snprintf(buf+pos, bufsize-pos, "set schema \"%s\";\n", schema);
		free(schema);
	}

	assert(pos < 10240);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
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
	if (SQLinitialized == 0 && (msg = SQLprelude()) != MAL_SUCCEED)
		return msg;
	/*
	 * Based on the initialization return value we can prepare a SQLinit
	 * string with all information needed to initialize the catalog
	 * based on the mandatory scripts to be executed.
	 */
	if (sqlinit) { /* add sqlinit to the fdin stack */
		buffer *b = (buffer*)GDKmalloc(sizeof(buffer));
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
		if (isAdministrator(c) || strcmp(c->scenario, "msql") == 0)  /* console should return everything */
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
	schema = monet5_user_get_def_schema(m, c->user);
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
	} else { /* handle upgrades */
		sql_subtype tp;
		char *err;

		if (!m->sa)
			m->sa = sa_create();
		/* if filter function sys.like(str,str,str) does not
		 * exist, we need to update */
        	sql_find_subtype(&tp, "clob", 0, 0);
		if (!sql_bind_func3(m->sa, mvc_bind_schema(m,"sys"), "like", &tp, &tp, &tp, F_FILT )) {
			if ((err = sql_update_dec2011(c, m)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
		/* if aggregate function sys.median(int) does not
		 * exist, we need to update */
        	sql_find_subtype(&tp, "int", 0, 0);
		if (!sql_bind_func(m->sa, mvc_bind_schema(m,"sys"), "median", &tp, NULL, F_AGGR )) {
			if ((err = sql_update_apr2012(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
		/* if function sys.optimizers() does not exist, we
		 * need to update */
		if (!sql_bind_func(m->sa, mvc_bind_schema(m,"sys"), "optimizers", NULL, NULL, F_FUNC )) {
			if ((err = sql_update_apr2012_sp1(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
		/* if function sys.alpa(double) does not
		 * exist, we need to update */
        	sql_find_subtype(&tp, "double", 0, 0);
		if (!sql_bind_func(m->sa, mvc_bind_schema(m,"sys"), "alpha", &tp, &tp, F_FUNC )) {
			if ((err = sql_update_jul2012(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
		/* if function sys.zorder_slice() does exist, we need
		 * to update */
		{
			list *l = sa_list(m->sa);
			sql_find_subtype(&tp, "int", 0, 0);
			list_append(l, &tp);
			list_append(l, &tp);
			list_append(l, &tp);
			list_append(l, &tp);
			if (sql_bind_func_(m->sa, mvc_bind_schema(m,"sys"),
					   "zorder_slice", l, F_FUNC )) {
				if ((err = sql_update_oct2012(c)) != NULL) {
					fprintf(stderr, "!%s\n", err);
					GDKfree(err);
				}
			}
		}
		/* if aggregate function sys.stddev(int) does
		 * exist, we need to update */
        	sql_find_subtype(&tp, "int", 0, 0);
		if (sql_bind_func(m->sa, mvc_bind_schema(m,"sys"), "stddev", &tp, NULL, F_AGGR )) {
			if ((err = sql_update_oct2012_sp1(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
		/* if aggregate function sys.stddev_samp(int) does not
		 * exist, we need to update */
        	sql_find_subtype(&tp, "int", 0, 0);
		if (!sql_bind_func(m->sa, mvc_bind_schema(m,"sys"), "stddev_samp", &tp, NULL, F_AGGR )) {
			if ((err = sql_update_feb2013(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
		/* if function sys.storagemodel() does not exist, we
		 * need to update */
		if (!sql_bind_func(m->sa, mvc_bind_schema(m,"sys"), "storagemodel", NULL, NULL, F_FUNC )) {
			if ((err = sql_update_feb2013_sp1(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
		/* if aggregate function sys.stddev_samp(wrd) does not
		 * exist, we need to update */
        	sql_find_subtype(&tp, "wrd", 0, 0);
		if (!sql_bind_func(m->sa, mvc_bind_schema(m,"sys"), "stddev_samp", &tp, NULL, F_AGGR )) {
			if ((err = sql_update_feb2013_sp3(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
		/* if function sys.querylog_catalog() does not exist, we
		 * need to update */
		if (!sql_bind_func(m->sa, mvc_bind_schema(m,"sys"), "querylog_catalog", NULL, NULL, F_FUNC )) {
			if ((err = sql_update_oct2013(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
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
	if (c->sqlcontext) {
		backend *be = NULL;
		mvc *m = NULL;
		if (c->sqlcontext == NULL)
			throw(SQL, "SQLexitClient", "MVC catalogue not available");
		be = (backend *)c->sqlcontext;
		m = be->mvc;

		assert(m->session);
		if (m->session->auto_commit && m->session->active) {
			if (mvc_status(m) >= 0 && mvc_commit(m, 0, NULL) < 0)
				(void) handle_error(m, c->fdout, 0);
		}
		if (m->session->active){
			RECYCLEdrop(0);
			mvc_rollback(m, 0, NULL);
		}

		res_tables_destroy(m->results);
		m->results= NULL;

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
	backend *be, *sql = (backend *) c->sqlcontext;
	size_t len = strlen(*expr);

#ifdef _SQL_COMPILE
	mnstr_printf(c->fdout, "#SQLstatement:%s\n", *expr);
#endif
	if (!sql) {
		msg = SQLinitEnvironment(c);
		sql = (backend *) c->sqlcontext;
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
	sql->output_format = be->output_format;
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
	c->sqlcontext = sql;
	while( m->scanner.rs->pos < m->scanner.rs->len ){
		sql_rel *r;
		stmt *s;
		int oldvtop, oldstop;
		MalStkPtr oldglb = c->glb;

		if (!m->sa)
			m->sa = sa_create();
		m->sym = NULL;
		if ( (err = sqlparse(m)) ||
			/* Only forget old errors on transaction boundaries */
			(mvc_status(m) && m->type != Q_TRANS) || !m->sym) {
			if (!err)
				err = mvc_status(m);
			if (*m->errstr)
				msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			*m->errstr = 0;
			sqlcleanup(m, err);
			execute = 0;
			if (!err)
				continue;
			assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
			c->glb = oldglb;
			goto endofcompile;
		}

		/*
		 * We have dealt with the first parsing step and advanced the input reader
		 * to the next statement (if any).
		 * Now is the time to also perform the semantic analysis,
		 * optimize and produce code.
		 * We don;t search the cache for a previous incarnation yet.
		 */
		MSinitClientPrg(c,"user",nme);
		oldvtop = c->curprg->def->vtop;
		oldstop = c->curprg->def->stop;
		r = sql_symbol2relation(m, m->sym);
		s = sql_relation2stmt(m, r);
#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout,"#SQLstatement:\n");
#endif
		scanner_query_processed(&(m->scanner));
		if (s==0 || (err = mvc_status(m))) {
			msg = createException(PARSE, "SQLparser", "%s", m->errstr);
			handle_error(m, c->fdout, status);
			sqlcleanup(m, err);
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c,c->curprg->def, c->glb, oldvtop);
			c->curprg->def->errors = 0;
			assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
			c->glb = oldglb;
			goto endofcompile;
		}
		/* generate MAL code */
		if (backend_callinline(sql, c, s ) == 0)
			addQueryToCache(c);
		else
			err = 1;

		if( err || c->curprg->def->errors){
			/* restore the state */
			MSresetInstructions(c->curprg->def, oldstop);
			freeVariables(c,c->curprg->def, c->glb, oldvtop);
			c->curprg->def->errors = 0;
			msg = createException(SQL, "SQLparser","Errors encountered in query");
			assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
			c->glb = oldglb;
			goto endofcompile;
		}

#ifdef _SQL_COMPILE
		mnstr_printf(c->fdout,"#result of sql.eval()\n");
		printFunction(c->fdout, c->curprg->def, 0, c->listing);
#endif

		if ( execute) {
			MalBlkPtr mb = c->curprg->def;

			if (!output)
				sql->out = NULL; /* no output */
			msg = runMAL(c, mb, 0, 0);
			MSresetInstructions(mb, oldstop);
			freeVariables(c, mb, c->glb, oldvtop);
		}
		sqlcleanup(m, 0);
		if (!execute) {
			assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
			c->glb = oldglb;
			goto endofcompile;
		}
#ifdef _SQL_COMPILE
	mnstr_printf(c->fdout, "#parse/execute result %d\n", err);
#endif
		assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
		c->glb = oldglb;
	}
/*
 * We are done; a MAL procedure recides in the cache.
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
		*ret= _STRDUP("SQLcompile");
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
	str *name = (str *) getArgReference(stk, pci, 1);
	str msg = MAL_SUCCEED, fullname;
	str *expr;
	mvc *m;

	fullname = MSP_locate_sqlscript(*name, 0);
	if (fullname == NULL)
		fullname = *name;
	fd = open_rastream(fullname);
	if (mnstr_errnr(fd) == MNSTR_OPEN_ERROR) {
		mnstr_destroy(fd);
		throw(MAL, "sql.include", "could not open file: %s\n", *name);
	}
	bfd = bstream_create(fd, 128 * BLOCK);
	if (bstream_next(bfd) < 0)
		throw(MAL, "sql.include", "could not read %s\n", *name);

	expr = &bfd->buf;
	msg = SQLstatementIntern(cntxt, expr, "sql.include", TRUE, FALSE);
	bstream_destroy(bfd);
	m = ((backend *)cntxt->sqlcontext)->mvc;
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
			RECYCLEdrop(0);
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
	int commit_done = FALSE;
	backend *be = (backend *) c->sqlcontext;
	bstream *in = c->fdin;
	int language = -1;
	mvc *m = NULL;
	int blocked = isa_block_stream(in->s);

	if (SQLinitialized == FALSE) {
		c->mode = FINISHING;
		return NULL;
	}
	if (!be || c->mode <= FINISHING) {
#ifdef _SQL_READER_DEBUG
		mnstr_printf(GDKout, "#SQL client finished\n");
#endif
		c->mode = FINISHING;
		return NULL;
	}
#ifdef _SQL_READER_DEBUG
	mnstr_printf(GDKout, "#SQLparser: start reading SQL %s %s\n",
			(be->console ? " from console" : ""),
			(blocked ? "Blocked read" : ""));
#endif
	language = be->language;    /* 'S' for SQL, 'D' from debugger */
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
	while (more) {
		more = FALSE;

		/* Different kinds of supported statements sequences
		    A;	-- single line			s
		    A \n B;	-- multi line			S
		    A; B;   -- compound single block	s
		    A;	-- many multi line
		    B \n C; -- statements in one block	S
		 */
		/* auto_commit on end of statement */
		if (m->scanner.mode == LINE_N && !commit_done) {
			go = SQLautocommit(c, m);
			commit_done=TRUE;
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
		in->pos = in->len;  /* skip rest of the input */
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

#define MAX_QUERY 	(64*1024*1024)

static int 
cachable( mvc *m, stmt *s ) 
{
	if (m->emode == m_plan ||
	   !m->caching ||
            m->type == Q_TRANS || /*m->type == Q_SCHEMA || cachable to make sure we have trace on alter statements  */
	    (s && s->type == st_none) || 
	    sa_size(m->sa) > MAX_QUERY)
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
	int err = 0;

	be = (backend *) c->sqlcontext;
	if (be == 0) {
		/* tell the client */
		mnstr_printf(out, "!SQL state descriptor missing, aborting\n");
		mnstr_flush(out);
		/* leave a message in the log */
		fprintf(stderr, "SQL state descriptor missing, cannot handle client!\n");
		/* stop here, instead of printing the exception below to the
		 * client in an endless loop */
		c->mode = FINISHING;
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

	if (m->history)
		be->mvc->Tparse = GDKusec();
	m->emode = m_normal;
	m->emod = mod_none;
	if (be->language == 'X') {
		int n = 0, v, off, len;

		if (strncmp(in->buf + in->pos, "export ", 7) == 0)
			n = sscanf(in->buf + in->pos + 7, "%d %d %d", &v, &off, &len);

		if (n == 2 || n == 3) {
			mvc_export_chunk(be, out, v, off, n == 3 ? len : m->reply_size);

			in->pos = in->len;  /* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "close ", 6) == 0) {
			res_table *t;

			v = (int) strtol(in->buf + in->pos + 6, NULL, 0);
			t = res_tables_find(m->results, v);
			if (t)
				m->results = res_tables_remove(m->results, t);
			in->pos = in->len;  /* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "release ", 8) == 0) {
			cq *q = NULL;

			v = (int) strtol(in->buf + in->pos + 8, NULL, 0);
			if ((q = qc_find(m->qc, v)) != NULL)
				qc_delete(m->qc, q);
			in->pos = in->len;  /* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "auto_commit ", 12) == 0) {
			int commit;
			v = (int) strtol(in->buf + in->pos + 12, NULL, 10);
			commit = (!m->session->auto_commit && v);
			m->session->auto_commit = (v) ? 1 : 0;
			m->session->ac_on_commit = m->session->auto_commit;
			if (m->session->active) {
				if (commit && mvc_commit(m, 0, NULL) < 0) {
					mnstr_printf(out, "!COMMIT: commit failed while "
							"enabling auto_commit\n");
					msg = createException(SQL, "SQLparser",
							"Xauto_commit (commit) failed");
				} else if (!commit && mvc_rollback(m, 0, NULL) < 0) {
					RECYCLEdrop(0);
					mnstr_printf(out, "!COMMIT: rollback failed while "
							"disabling auto_commit\n");
					msg = createException(SQL, "SQLparser",
							"Xauto_commit (rollback) failed");
				}
			}
			in->pos = in->len;  /* HACK: should use parsed length */
			if (msg != NULL)
				goto finalize;
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "reply_size ", 11) == 0) {
			v = (int) strtol(in->buf + in->pos + 11, NULL, 10);
			m->reply_size = v;
			in->pos = in->len;  /* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "sizeheader", 10) == 0) {
			v = (int) strtol(in->buf + in->pos + 10, NULL, 10);
			m->sizeheader = v != 0;
			in->pos = in->len;  /* HACK: should use parsed length */
			return MAL_SUCCEED;
		}
		if (strncmp(in->buf + in->pos, "quit", 4) == 0) {
			c->mode = FINISHING;
			return MAL_SUCCEED;
		}
		mnstr_printf(out, "!unrecognized X command: %s\n", in->buf + in->pos);
		msg = createException(SQL, "SQLparser", "unrecognized X command");
		goto finalize;
	}
	if (be->language != 'S') {
		mnstr_printf(out, "!unrecognized language prefix: %ci\n", be->language);
		msg = createException(SQL, "SQLparser",
				"unrecognized language prefix: %c", be->language);
		goto finalize;
	}

	if ( (err = sqlparse(m)) ||
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
			mnstr_printf(out, "!07003!EXEC: no prepared statement with id: %d\n",
					m->sym->data.lval->h->data.i_val);
			msg = createException(SQL, "PREPARE",
					"no prepared statement with id: %d",
					m->sym->data.lval->h->data.i_val);
			handle_error(m, c->fdout, pstatus);
			sqlcleanup(m, err);
			goto finalize;
		} else if (be->q->type != Q_PREPARE) {
			err = -1;
			mnstr_printf(out, "!07005!EXEC: given handle id is not for a "
					"prepared statement: %d\n",
					m->sym->data.lval->h->data.i_val);
			msg = createException(SQL, "PREPARE",
					"is not a prepared statement: %d",
					m->sym->data.lval->h->data.i_val);
			handle_error(m, c->fdout, pstatus);
			sqlcleanup(m, err);
			goto finalize;
		}
		m->emode = m_inplace;
		scanner_query_processed(&(m->scanner));
	} else if (cachable(m, NULL) && m->emode != m_prepare &&
                  (be->q = qc_match(m->qc, m->sym, m->args, m->argc, m->scanner.key ^ m->session->schema->base.id)) != NULL) {

		if (m->emod & mod_debug)
			SQLsetDebugger(c, m, TRUE);
		if (m->emod & mod_trace)
			SQLsetTrace(be, c, TRUE);
		if (!(m->emod & (mod_explain | mod_debug | mod_trace | mod_dot)))
			m->emode = m_inplace;
		scanner_query_processed(&(m->scanner));
	} else {
		sql_rel *r = sql_symbol2relation(m, m->sym);
		stmt *s = sql_relation2stmt(m, r);

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
		if (!cachable(m, s)) {
			MalBlkPtr mb;

			scanner_query_processed(&(m->scanner));
			if (backend_callinline(be, c, s) == 0) {
				trimMalBlk(c->curprg->def);
				mb = c->curprg->def;
				chkProgram(c->fdout, c->nspace, mb);
				addOptimizerPipe(c, mb, "minimal_pipe");
				msg = optimizeMALBlock(c, mb);
				if (msg != MAL_SUCCEED) {
					sqlcleanup(m, err);
					goto finalize;
				}
				c->curprg->def = mb;
			} else {
				err = 1;
			}
		} else {
			/* generate a factory instantiation */
			be->q = qc_insert(m->qc,
					m->sa,        /* the allocator */
					r,	      /* keep relational query */
					m->sym,       /* the sql symbol tree */
					m->args,      /* the argument list */
					m->argc,
					m->scanner.key ^ m->session->schema->base.id,  /* the statement hash key */
					m->emode == m_prepare ? Q_PREPARE :
					m->type,  /* the type of the statement */
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
			} else { /* call procedure generation (only in cache mode) */
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
		if (be->q)
			pushEndInstruction(c->curprg->def);

		chkTypes(c->fdout, c->nspace, c->curprg->def, TRUE); /* resolve types */
		/* we know more in this case than
		    chkProgram(c->fdout, c->nspace, c->curprg->def); */
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

/*
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
	Symbol qcode = q->code;

	if (!qcode || qcode->def->errors ) {
		if (!qcode && *m->errstr)
			return createException(PARSE, "SQLparser", "%s", m->errstr);
		throw(SQL, "SQLengine", "39000!program contains errors");
	}
	mb = qcode->def;
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
		throw(SQL, "sql.prepare", "07001!EXEC: wrong number of arguments for prepared statement: %d, expected %d", argc, parc);
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
				throw(SQL, "sql.prepare", "07001!EXEC: wrong type for argument %d of "
						"prepared statement: %s, expected %s",
						i + 1, atom_type(arg)->type->sqlname,
						pt->type->sqlname);
			}
			argv[pci->retc + i] = &arg->data;
		}
	}
	glb = (MalStkPtr)(q->stk);
	ret= callMAL(c, mb, &glb, argv, (m->emod & mod_debug?'n':0));
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

	if (oldlang == 'X'){ 	/* return directly from X-commands */
		sqlcleanup(be->mvc, 0);
		return MAL_SUCCEED;
	}

	if (m->emod & mod_explain) {
		if (be->q && be->q->code)
			printFunction(c->fdout, ((Symbol) (be->q->code))->def, 0, LIST_MAL_STMT | LIST_MAL_UDF | LIST_MAPI);
		else if (be->q) 
			msg = createException(PARSE, "SQLparser", "%s", (*m->errstr)?m->errstr:"39000!program contains errors");
		else if (c->curprg && c->curprg->def)
			printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_STMT | LIST_MAL_UDF | LIST_MAPI);
		goto cleanup_engine;
	}
	if (m->emod & mod_dot) {
		if (be->q && be->q->code)
			showFlowGraph(((Symbol) (be->q->code))->def, 0, "stdout-mapi");
		else if (be->q)
			msg = createException(PARSE, "SQLparser", "%s", (*m->errstr)?m->errstr:"39000!program contains errors");
		else if (c->curprg && c->curprg->def)
			showFlowGraph(c->curprg->def, 0, "stdout-mapi");
		goto cleanup_engine;
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

	if( m->emode == m_prepare)
		goto cleanup_engine;

	assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
	c->glb = 0;
	be->language = 'D';
	/*
	 * The code below is copied from MALengine, which handles execution
	 * in the context of a user global environment. We have a private
	 * environment.
	 */
	if (MALcommentsOnly(c->curprg->def)) {
		msg = MAL_SUCCEED;
	} else {
		msg = (str) runMAL(c, c->curprg->def, 0, 0);
	}

cleanup_engine:
	if (m->type == Q_SCHEMA) 
        	qc_clean(m->qc);
	if (msg) {
		enum malexception type = getExceptionType(msg);
		if (type == OPTIMIZER) {
			MSresetInstructions(c->curprg->def, 1);
			freeVariables(c,c->curprg->def, c->glb, be->vtop);
			be->language = oldlang;
			assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
			c->glb = oldglb;
			return SQLrecompile(c, be);
		} else {
			/* don't print exception decoration, just the message */
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
		}
		showErrors(c);
		m->session->status = -10;
	}

	mb = c->curprg->def;
	if (m->type != Q_SCHEMA && be->q && msg) {
		qc_delete(m->qc, be->q); 
	} else if (m->type != Q_SCHEMA && be->q && mb &&
	    varGetProp(mb, getArg(p = getInstrPtr(mb,0), 0), runonceProp)){
		SQLCacheRemove(c, getFunctionId(p));
		qc_delete(be->mvc->qc, be->q);
		///* this should invalidate any match */
		//be->q->key= -1;
		//be->q->paramlen = -1;
		///* qc_delete(be->q) */
	}
	be->q = NULL;
	sqlcleanup(be->mvc, (!msg)?0:-1);
	MSresetInstructions(c->curprg->def, 1);
	freeVariables(c,c->curprg->def, c->glb, be->vtop);
	be->language = oldlang;
	/*
	 * Any error encountered during execution should block further processing
	 * unless auto_commit has been set.
	 */
	assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
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
	s = sql_relation2stmt(m, be->q->rel);
	be->q->code = (backend_code)backend_dumpproc(be, c, be->q, s);
	be->q->stk = 0;

	pushEndInstruction(c->curprg->def);

	chkTypes(c->fdout, c->nspace, c->curprg->def, TRUE); /* resolve types */
	if (!be->q->code || c->curprg->def->errors) {
		showErrors(c);
		/* restore the state */
		MSresetInstructions(c->curprg->def, oldstop);
		freeVariables(c,c->curprg->def, c->glb, oldvtop);
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
		const char *sqlstate = "M0M29!";
		/* mdbDump(mb,stk,pci);*/
		if (strlen(*msg) > 6 && (*msg)[5] == '!' &&
		    (('0' <= (*msg)[0] && (*msg)[0] <= '9') ||
		     ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) &&
		    (('0' <= (*msg)[1] && (*msg)[1] <= '9') ||
		     ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') ||
		     ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) &&
		    (('0' <= (*msg)[3] && (*msg)[3] <= '9') ||
		     ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) &&
		    (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
		     ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
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
		const char *sqlstate = "M0M29!";
		/* mdbDump(mb,stk,pci);*/
		if (strlen(*msg) > 6 && (*msg)[5] == '!' &&
		    (('0' <= (*msg)[0] && (*msg)[0] <= '9') ||
		     ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) &&
		    (('0' <= (*msg)[1] && (*msg)[1] <= '9') ||
		     ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') ||
		     ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) &&
		    (('0' <= (*msg)[3] && (*msg)[3] <= '9') ||
		     ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) &&
		    (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
		     ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
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
		const char *sqlstate = "M0M29!";
		/* mdbDump(mb,stk,pci);*/
		if (strlen(*msg) > 6 && (*msg)[5] == '!' &&
		    (('0' <= (*msg)[0] && (*msg)[0] <= '9') ||
		     ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) &&
		    (('0' <= (*msg)[1] && (*msg)[1] <= '9') ||
		     ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') ||
		     ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) &&
		    (('0' <= (*msg)[3] && (*msg)[3] <= '9') ||
		     ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) &&
		    (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
		     ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
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
		const char *sqlstate = "M0M29!";
		/* mdbDump(mb,stk,pci);*/
		if (strlen(*msg) > 6 && (*msg)[5] == '!' &&
		    (('0' <= (*msg)[0] && (*msg)[0] <= '9') ||
		     ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) &&
		    (('0' <= (*msg)[1] && (*msg)[1] <= '9') ||
		     ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') ||
		     ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) &&
		    (('0' <= (*msg)[3] && (*msg)[3] <= '9') ||
		     ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) &&
		    (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
		     ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
	}
	return MAL_SUCCEED;
}

str
SQLCacheRemove(Client c, str nme)
{
	Symbol s;

#ifdef _SQL_CACHE_DEBUG
	mnstr_printf(GDKout, "#SQLCacheRemove %s\n", nme);
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
