/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * SQL upgrade code
 * N. Nes, M.L. Kersten, S. Mullender
 */
#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_execute.h"
#include "sql_mvc.h"
#include "gdk_time.h"
#include <unistd.h>
#include "sql_upgrades.h"
#include "rel_rel.h"
#include "rel_semantic.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"

#include "rel_remote.h"
#include "mal_authorize.h"

/* this function can be used to recreate the system tables (types,
 * functions, args) when internal types and/or functions have changed
 * (i.e. the ones in sql_types.c) */
static str
sql_fix_system_tables(Client c, mvc *sql, const char *prev_schema)
{
	size_t bufsize = 1000000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	node *n;
	sql_schema *s;
	static const char *boolnames[2] = {"false", "true"};

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	s = mvc_bind_schema(sql, "sys");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"delete from sys.dependencies where id < 2000;\n");

	/* recreate internal types */
	pos += snprintf(buf + pos, bufsize - pos,
			"delete from sys.types where id < 2000;\n");
	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;

		if (t->base.id >= FUNC_OIDS)
			continue;

		pos += snprintf(buf + pos, bufsize - pos,
				"insert into sys.types values"
				" (%d, '%s', '%s', %u, %u, %d, %d, %d);\n",
				t->base.id, t->base.name, t->sqlname, t->digits,
				t->scale, t->radix, (int) t->eclass,
				t->s ? t->s->base.id : s->base.id);
	}

	/* recreate internal functions */
	pos += snprintf(buf + pos, bufsize - pos,
			"delete from sys.functions where id < 2000;\n"
			"delete from sys.args where func_id not in"
			" (select id from sys.functions);\n");
	sqlstore *store = sql->session->tr->store;
	for (n = funcs->h; n; n = n->next) {
		sql_func *func = n->data;
		int number = 0;
		sql_arg *arg;
		node *m;

		if (func->base.id >= FUNC_OIDS)
			continue;

		pos += snprintf(buf + pos, bufsize - pos,
				"insert into sys.functions values"
				" (%d, '%s', '%s', '%s',"
				" %d, %d, %s, %s, %s, %d, %s, %s);\n",
				func->base.id, func->base.name,
				func->imp, func->mod, (int) FUNC_LANG_INT,
				(int) func->type,
				boolnames[func->side_effect],
				boolnames[func->varres],
				boolnames[func->vararg],
				func->s ? func->s->base.id : s->base.id,
				boolnames[func->system],
				boolnames[func->semantics]);
		if (func->res) {
			for (m = func->res->h; m; m = m->next, number++) {
				arg = m->data;
				pos += snprintf(buf + pos, bufsize - pos,
						"insert into sys.args"
						" values"
						" (%d, %d, 'res_%d',"
						" '%s', %u, %u, %d,"
						" %d);\n",
						store_next_oid(store),
						func->base.id,
						number,
						arg->type.type->sqlname,
						arg->type.digits,
						arg->type.scale,
						arg->inout, number);
			}
		}
		for (m = func->ops->h; m; m = m->next, number++) {
			arg = m->data;
			if (arg->name)
				pos += snprintf(buf + pos, bufsize - pos,
						"insert into sys.args"
						" values"
						" (%d, %d, '%s', '%s',"
						" %u, %u, %d, %d);\n",
						store_next_oid(store),
						func->base.id,
						arg->name,
						arg->type.type->sqlname,
						arg->type.digits,
						arg->type.scale,
						arg->inout, number);
			else
				pos += snprintf(buf + pos, bufsize - pos,
						"insert into sys.args"
						" values"
						" (%d, %d, 'arg_%d',"
						" '%s', %u, %u, %d,"
						" %d);\n",
						store_next_oid(store),
						func->base.id,
						number,
						arg->type.type->sqlname,
						arg->type.digits,
						arg->type.scale,
						arg->inout, number);
		}
	}

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#ifdef HAVE_HGE
static str
sql_update_hugeint(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 8192, pos = 0;
	char *buf, *err;

	if (!*systabfixed &&
	    (err = sql_fix_system_tables(c, sql, prev_schema)) != NULL)
		return err;
	*systabfixed = true;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	/* 80_udf_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function fuse(one bigint, two bigint)\n"
			"returns hugeint external name udf.fuse;\n");

	/* 90_generator_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.generate_series(first hugeint, \"limit\" hugeint)\n"
			"returns table (value hugeint)\n"
			"external name generator.series;\n"
			"create function sys.generate_series(first hugeint, \"limit\" hugeint, stepsize hugeint)\n"
			"returns table (value hugeint)\n"
			"external name generator.series;\n");

	/* 39_analytics_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate stddev_samp(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"stdev\";\n"
			"GRANT EXECUTE ON AGGREGATE stddev_samp(HUGEINT) TO PUBLIC;\n"
			"create window stddev_samp(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(HUGEINT) TO PUBLIC;\n"
			"create aggregate stddev_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"stdevp\";\n"
			"GRANT EXECUTE ON AGGREGATE stddev_pop(HUGEINT) TO PUBLIC;\n"
			"create window stddev_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(HUGEINT) TO PUBLIC;\n"
			"create aggregate var_samp(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"variance\";\n"
			"GRANT EXECUTE ON AGGREGATE var_samp(HUGEINT) TO PUBLIC;\n"
			"create window var_samp(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(HUGEINT) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window covar_samp(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create aggregate var_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"variancep\";\n"
			"GRANT EXECUTE ON AGGREGATE var_pop(HUGEINT) TO PUBLIC;\n"
			"create window var_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(HUGEINT) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window covar_pop(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create aggregate median(val HUGEINT) returns HUGEINT\n"
			" external name \"aggr\".\"median\";\n"
			"GRANT EXECUTE ON AGGREGATE median(HUGEINT) TO PUBLIC;\n"
			"create aggregate quantile(val HUGEINT, q DOUBLE) returns HUGEINT\n"
			" external name \"aggr\".\"quantile\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile(HUGEINT, DOUBLE) TO PUBLIC;\n"
			"create aggregate median_avg(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(HUGEINT) TO PUBLIC;\n"
			"create aggregate quantile_avg(val HUGEINT, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(HUGEINT, DOUBLE) TO PUBLIC;\n"
			"create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"corr\";\n"
			"GRANT EXECUTE ON AGGREGATE corr(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(HUGEINT, HUGEINT) TO PUBLIC;\n");

	/* 40_json_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function json.filter(js json, name hugeint)\n"
			"returns json external name json.filter;\n"
			"GRANT EXECUTE ON FUNCTION json.filter(json, hugeint) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and name in ('fuse') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in ('generate_series') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'covar_samp', 'var_pop', 'covar_pop', 'median', 'median_avg', 'quantile', 'quantile_avg', 'corr') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'covar_samp', 'var_pop', 'covar_pop', 'corr') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name = 'filter' and schema_id = (select id from sys.schemas where name = 'json') and type = %d;\n",
			(int) F_FUNC, (int) F_UNION, (int) F_AGGR, (int) F_ANALYTIC, (int) F_FUNC);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_drop_functions_dependencies_Xs_on_Ys(Client c, const char *prev_schema)
{
	size_t bufsize = 1600, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* remove functions which were created in sql/scripts/21_dependency_functions.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"DROP FUNCTION dependencies_schemas_on_users();\n"
			"DROP FUNCTION dependencies_owners_on_schemas();\n"
			"DROP FUNCTION dependencies_tables_on_views();\n"
			"DROP FUNCTION dependencies_tables_on_indexes();\n"
			"DROP FUNCTION dependencies_tables_on_triggers();\n"
			"DROP FUNCTION dependencies_tables_on_foreignKeys();\n"
			"DROP FUNCTION dependencies_tables_on_functions();\n"
			"DROP FUNCTION dependencies_columns_on_views();\n"
			"DROP FUNCTION dependencies_columns_on_keys();\n"
			"DROP FUNCTION dependencies_columns_on_indexes();\n"
			"DROP FUNCTION dependencies_columns_on_functions();\n"
			"DROP FUNCTION dependencies_columns_on_triggers();\n"
			"DROP FUNCTION dependencies_views_on_functions();\n"
			"DROP FUNCTION dependencies_views_on_triggers();\n"
			"DROP FUNCTION dependencies_functions_on_functions();\n"
			"DROP FUNCTION dependencies_functions_on_triggers();\n"
			"DROP FUNCTION dependencies_keys_on_foreignKeys();\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_apr2019(Client c, mvc *sql, const char *prev_schema)
{
	size_t bufsize = 3000, pos = 0;
	char *buf, *err;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");

	/* 15_querylog.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop procedure sys.querylog_enable(smallint);\n"
			"create procedure sys.querylog_enable(threshold integer) external name sql.querylog_enable;\n"
			"update sys.functions set system = true where system <> true and name = 'querylog_enable' and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n",
			(int) F_PROC);

	/* 17_temporal.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.date_trunc(txt string, t timestamp)\n"
			"returns timestamp\n"
			"external name sql.date_trunc;\n"
			"grant execute on function sys.date_trunc(string, timestamp) to public;\n"
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys') and name = 'date_trunc' and type = %d;\n", (int) F_FUNC);

	/* 22_clients.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create procedure sys.setprinttimeout(\"timeout\" integer)\n"
			"external name clients.setprinttimeout;\n"
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys') and name = 'setprinttimeout' and type = %d;\n", (int) F_PROC);

	/* 26_sysmon.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"grant execute on function sys.queue to public;\n"
			"grant select on sys.queue to public;\n");

	/* 51_sys_schema_extensions.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"ALTER TABLE sys.keywords SET READ WRITE;\n"
			"INSERT INTO sys.keywords VALUES ('WINDOW');\n"
		);
	t = mvc_bind_table(sql, s, "var_values");
	t->system = 0;	/* make it non-system else the drop view will fail */
	pos += snprintf(buf + pos, bufsize - pos,
			"DROP VIEW sys.var_values;\n"
			"CREATE VIEW sys.var_values (var_name, value) AS\n"
			"SELECT 'cache' AS var_name, convert(cache, varchar(10)) AS value UNION ALL\n"
			"SELECT 'current_role', current_role UNION ALL\n"
			"SELECT 'current_schema', current_schema UNION ALL\n"
			"SELECT 'current_timezone', current_timezone UNION ALL\n"
			"SELECT 'current_user', current_user UNION ALL\n"
			"SELECT 'debug', debug UNION ALL\n"
			"SELECT 'last_id', last_id UNION ALL\n"
			"SELECT 'optimizer', optimizer UNION ALL\n"
			"SELECT 'pi', pi() UNION ALL\n"
			"SELECT 'rowcnt', rowcnt;\n"
			"UPDATE sys._tables SET system = true WHERE name = 'var_values' AND schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys');\n"
			"GRANT SELECT ON sys.var_values TO PUBLIC;\n");

	/* 99_system.sql */
	t = mvc_bind_table(sql, s, "systemfunctions");
	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"drop table sys.systemfunctions;\n"
			"create view sys.systemfunctions as select id as function_id from sys.functions where system;\n"
			"grant select on sys.systemfunctions to public;\n"
			"update sys._tables set system = true where name = 'systemfunctions' and schema_id = (select id from sys.schemas where name = 'sys');\n");
	/* update type of "query" attribute of tables sys._tables and
	 * tmp_tables from varchar(2048) to varchar(1048576) */
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._columns set type_digits = 1048576 where name = 'query' and table_id in (select id from sys._tables t where t.name = '_tables' and t.schema_id in (select id from sys.schemas s where s.name in ('sys', 'tmp')));\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._columns set type_digits = 1048576 where name = 'query' and table_id in (select id from sys._tables t where t.name = 'tables' and t.schema_id in (select id from sys.schemas s where s.name = 'sys'));\n");

	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	if (err == MAL_SUCCEED) {
		pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
			       "ALTER TABLE sys.keywords SET READ ONLY;\n");

		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}

	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_storagemodel(Client c, mvc *sql, const char *prev_schema, bool oct2020_upgrade)
{
	size_t bufsize = 20000, pos = 0;
	char *buf, *err;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t;
	char *day_interval_str = oct2020_upgrade ? " 'day_interval'," : "";

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* set views and tables internally to non-system to allow drop commands to succeed without error */
	if ((t = mvc_bind_table(sql, s, "storage")) != NULL)
		t->system = 0;
	if ((t = mvc_bind_table(sql, s, "storagemodel")) != NULL)
		t->system = 0;
	if ((t = mvc_bind_table(sql, s, "storagemodelinput")) != NULL)
		t->system = 0;
	if ((t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
		t->system = 0;

	/* new 75_storagemodel.sql */
	pos += snprintf(buf + pos, bufsize - pos,
		"set schema sys;\n"
		/* drop objects in reverse order of original creation of old 75_storagemodel.sql */
		"drop view if exists sys.tablestoragemodel;\n"
		"drop view if exists sys.storagemodel cascade;\n"
		"drop function if exists sys.storagemodel() cascade;\n");

	if (oct2020_upgrade) {
		pos += snprintf(buf + pos, bufsize - pos,
			"drop function if exists sys.imprintsize(varchar(1024), bigint) cascade;\n");
	} else {
		pos += snprintf(buf + pos, bufsize - pos,
			"drop function if exists sys.imprintsize(bigint, clob) cascade;\n");
	}

	pos += snprintf(buf + pos, bufsize - pos,
		"drop function if exists sys.hashsize(boolean, bigint) cascade;\n");

	if (oct2020_upgrade) {
		pos += snprintf(buf + pos, bufsize - pos,
			"drop function if exists sys.columnsize(varchar(1024), bigint) cascade;\n"
			"drop function if exists sys.heapsize(varchar(1024), bigint, bigint, int) cascade;\n");
	} else {
		pos += snprintf(buf + pos, bufsize - pos,
			"drop function if exists sys.columnsize(clob, bigint, bigint) cascade;\n"
			"drop function if exists sys.heapsize(clob, bigint, int) cascade;\n");
	}

	pos += snprintf(buf + pos, bufsize - pos,
		"drop procedure if exists sys.storagemodelinit();\n"
		"drop table if exists sys.storagemodelinput cascade;\n"
		"drop view if exists sys.\"storage\" cascade;\n");

	if (oct2020_upgrade) {
		pos += snprintf(buf + pos, bufsize - pos,
			"drop function if exists sys.\"storage\"(varchar(1024), varchar(1024), varchar(1024)) cascade;\n"
			"drop function if exists sys.\"storage\"(varchar(1024), varchar(1024)) cascade;\n"
			"drop function if exists sys.\"storage\"(varchar(1024)) cascade;\n");
	} else {
		pos += snprintf(buf + pos, bufsize - pos,
			"drop function if exists sys.\"storage\"(clob, clob, clob) cascade;\n"
			"drop function if exists sys.\"storage\"(clob, clob) cascade;\n"
			"drop function if exists sys.\"storage\"(clob) cascade;\n");
	}

	/* new 75_storagemodel.sql */
	pos += snprintf(buf + pos, bufsize - pos,
		"drop function if exists sys.\"storage\"() cascade;\n"
		"create function sys.\"storage\"()\n"
		"returns table (\n"
		"	\"schema\" varchar(1024),\n"
		"	\"table\" varchar(1024),\n"
		"	\"column\" varchar(1024),\n"
		"	\"type\" varchar(1024),\n"
		"	\"mode\" varchar(15),\n"
		"	location varchar(1024),\n"
		"	\"count\" bigint,\n"
		"	typewidth int,\n"
		"	columnsize bigint,\n"
		"	heapsize bigint,\n"
		"	hashes bigint,\n"
		"	phash boolean,\n"
		"	\"imprints\" bigint,\n"
		"	sorted boolean,\n"
		"	revsorted boolean,\n"
		"	\"unique\" boolean,\n"
		"	orderidx bigint\n"
		")\n"
		"external name sql.\"storage\";\n"
		"create view sys.\"storage\" as\n"
		"select * from sys.\"storage\"()\n"
		" where (\"schema\", \"table\") in (\n"
		"	SELECT sch.\"name\", tbl.\"name\"\n"
		"	  FROM sys.\"tables\" AS tbl JOIN sys.\"schemas\" AS sch ON tbl.schema_id = sch.id\n"
		"	 WHERE tbl.\"system\" = FALSE)\n"
		"order by \"schema\", \"table\", \"column\";\n"
		"create view sys.\"tablestorage\" as\n"
		"select \"schema\", \"table\",\n"
		"	max(\"count\") as \"rowcount\",\n"
		"	count(*) as \"storages\",\n"
		"	sum(columnsize) as columnsize,\n"
		"	sum(heapsize) as heapsize,\n"
		"	sum(hashes) as hashsize,\n"
		"	sum(\"imprints\") as imprintsize,\n"
		"	sum(orderidx) as orderidxsize\n"
		" from sys.\"storage\"\n"
		"group by \"schema\", \"table\"\n"
		"order by \"schema\", \"table\";\n"
		"create view sys.\"schemastorage\" as\n"
		"select \"schema\",\n"
		"	count(*) as \"storages\",\n"
		"	sum(columnsize) as columnsize,\n"
		"	sum(heapsize) as heapsize,\n"
		"	sum(hashes) as hashsize,\n"
		"	sum(\"imprints\") as imprintsize,\n"
		"	sum(orderidx) as orderidxsize\n"
		" from sys.\"storage\"\n"
		"group by \"schema\"\n"
		"order by \"schema\";\n"
		"create function sys.\"storage\"(sname varchar(1024))\n"
		"returns table (\n"
		"	\"schema\" varchar(1024),\n"
		"	\"table\" varchar(1024),\n"
		"	\"column\" varchar(1024),\n"
		"	\"type\" varchar(1024),\n"
		"	\"mode\" varchar(15),\n"
		"	location varchar(1024),\n"
		"	\"count\" bigint,\n"
		"	typewidth int,\n"
		"	columnsize bigint,\n"
		"	heapsize bigint,\n"
		"	hashes bigint,\n"
		"	phash boolean,\n"
		"	\"imprints\" bigint,\n"
		"	sorted boolean,\n"
		"	revsorted boolean,\n"
		"	\"unique\" boolean,\n"
		"	orderidx bigint\n"
		")\n"
		"external name sql.\"storage\";\n"
		"create function sys.\"storage\"(sname varchar(1024), tname varchar(1024))\n"
		"returns table (\n"
		"	\"schema\" varchar(1024),\n"
		"	\"table\" varchar(1024),\n"
		"	\"column\" varchar(1024),\n"
		"	\"type\" varchar(1024),\n"
		"	\"mode\" varchar(15),\n"
		"	location varchar(1024),\n"
		"	\"count\" bigint,\n"
		"	typewidth int,\n"
		"	columnsize bigint,\n"
		"	heapsize bigint,\n"
		"	hashes bigint,\n"
		"	phash boolean,\n"
		"	\"imprints\" bigint,\n"
		"	sorted boolean,\n"
		"	revsorted boolean,\n"
		"	\"unique\" boolean,\n"
		"	orderidx bigint\n"
		")\n"
		"external name sql.\"storage\";\n"
		"create function sys.\"storage\"(sname varchar(1024), tname varchar(1024), cname varchar(1024))\n"
		"returns table (\n"
		"	\"schema\" varchar(1024),\n"
		"	\"table\" varchar(1024),\n"
		"	\"column\" varchar(1024),\n"
		"	\"type\" varchar(1024),\n"
		"	\"mode\" varchar(15),\n"
		"	location varchar(1024),\n"
		"	\"count\" bigint,\n"
		"	typewidth int,\n"
		"	columnsize bigint,\n"
		"	heapsize bigint,\n"
		"	hashes bigint,\n"
		"	phash boolean,\n"
		"	\"imprints\" bigint,\n"
		"	sorted boolean,\n"
		"	revsorted boolean,\n"
		"	\"unique\" boolean,\n"
		"	orderidx bigint\n"
		")\n"
		"external name sql.\"storage\";\n"
		"create table sys.storagemodelinput(\n"
		"	\"schema\" varchar(1024) NOT NULL,\n"
		"	\"table\" varchar(1024) NOT NULL,\n"
		"	\"column\" varchar(1024) NOT NULL,\n"
		"	\"type\" varchar(1024) NOT NULL,\n"
		"	typewidth int NOT NULL,\n"
		"	\"count\" bigint NOT NULL,\n"
		"	\"distinct\" bigint NOT NULL,\n"
		"	atomwidth int NOT NULL,\n"
		"	reference boolean NOT NULL DEFAULT FALSE,\n"
		"	sorted boolean,\n"
		"	\"unique\" boolean,\n"
		"	isacolumn boolean NOT NULL DEFAULT TRUE\n"
		");\n"
		"create procedure sys.storagemodelinit()\n"
		"begin\n"
		"	delete from sys.storagemodelinput;\n"
		"	insert into sys.storagemodelinput\n"
		"	select \"schema\", \"table\", \"column\", \"type\", typewidth, \"count\",\n"
		"		case when (\"unique\" or \"type\" IN ('varchar', 'char', 'clob', 'json', 'url', 'blob', 'geometry', 'geometrya'))\n"
		"			then \"count\" else 0 end,\n"
		"		case when \"count\" > 0 and heapsize >= 8192 and \"type\" in ('varchar', 'char', 'clob', 'json', 'url')\n"
		"			then cast((heapsize - 8192) / \"count\" as bigint)\n"
		"		when \"count\" > 0 and heapsize >= 32 and \"type\" in ('blob', 'geometry', 'geometrya')\n"
		"			then cast((heapsize - 32) / \"count\" as bigint)\n"
		"		else typewidth end,\n"
		"		FALSE, case sorted when true then true else false end, \"unique\", TRUE\n"
		"	  from sys.\"storage\";\n"
		"	update sys.storagemodelinput\n"
		"	   set reference = TRUE\n"
		"	 where (\"schema\", \"table\", \"column\") in (\n"
		"		SELECT fkschema.\"name\", fktable.\"name\", fkkeycol.\"name\"\n"
		"		  FROM	sys.\"keys\" AS fkkey,\n"
		"			sys.\"objects\" AS fkkeycol,\n"
		"			sys.\"tables\" AS fktable,\n"
		"			sys.\"schemas\" AS fkschema\n"
		"		WHERE fktable.\"id\" = fkkey.\"table_id\"\n"
		"		  AND fkkey.\"id\" = fkkeycol.\"id\"\n"
		"		  AND fkschema.\"id\" = fktable.\"schema_id\"\n"
		"		  AND fkkey.\"rkey\" > -1 );\n"
		"	update sys.storagemodelinput\n"
		"	   set isacolumn = FALSE\n"
		"	 where (\"schema\", \"table\", \"column\") NOT in (\n"
		"		SELECT sch.\"name\", tbl.\"name\", col.\"name\"\n"
		"		  FROM sys.\"schemas\" AS sch,\n"
		"			sys.\"tables\" AS tbl,\n"
		"			sys.\"columns\" AS col\n"
		"		WHERE sch.\"id\" = tbl.\"schema_id\"\n"
		"		  AND tbl.\"id\" = col.\"table_id\");\n"
		"end;\n"
		"create function sys.columnsize(tpe varchar(1024), count bigint)\n"
		"returns bigint\n"
		"begin\n"
		"	if tpe in ('tinyint', 'boolean')\n"
		"		then return count;\n"
		"	end if;\n"
		"	if tpe = 'smallint'\n"
		"		then return 2 * count;\n"
		"	end if;\n"
		"	if tpe in ('int', 'real', 'date', 'time', 'timetz', 'sec_interval',%s 'month_interval')\n"
		"		then return 4 * count;\n"
		"	end if;\n"
		"	if tpe in ('bigint', 'double', 'timestamp', 'timestamptz', 'inet', 'oid')\n"
		"		then return 8 * count;\n"
		"	end if;\n"
		"	if tpe in ('hugeint', 'decimal', 'uuid', 'mbr')\n"
		"		then return 16 * count;\n"
		"	end if;\n"
		"	if tpe in ('varchar', 'char', 'clob', 'json', 'url')\n"
		"		then return 4 * count;\n"
		"	end if;\n"
		"	if tpe in ('blob', 'geometry', 'geometrya')\n"
		"		then return 8 * count;\n"
		"	end if;\n"
		"	return 8 * count;\n"
		"end;\n"
		"create function sys.heapsize(tpe varchar(1024), count bigint, distincts bigint, avgwidth int)\n"
		"returns bigint\n"
		"begin\n"
		"	if tpe in ('varchar', 'char', 'clob', 'json', 'url')\n"
		"		then return 8192 + ((avgwidth + 8) * distincts);\n"
		"	end if;\n"
		"	if tpe in ('blob', 'geometry', 'geometrya')\n"
		"		then return 32 + (avgwidth * count);\n"
		"	end if;\n"
		"	return 0;\n"
		"end;\n"
		"create function sys.hashsize(b boolean, count bigint)\n"
		"returns bigint\n"
		"begin\n"
		"	if b = true\n"
		"		then return 8 * count;\n"
		"	end if;\n"
		"	return 0;\n"
		"end;\n"
		"create function sys.imprintsize(tpe varchar(1024), count bigint)\n"
		"returns bigint\n"
		"begin\n"
		"	if tpe in ('tinyint', 'boolean')\n"
		"		then return cast(0.2 * count as bigint);\n"
		"	end if;\n"
		"	if tpe = 'smallint'\n"
		"		then return cast(0.4 * count as bigint);\n"
		"	end if;\n"
		"	if tpe in ('int', 'real', 'date', 'time', 'timetz', 'sec_interval',%s 'month_interval')\n"
		"		then return cast(0.8 * count as bigint);\n"
		"	end if;\n"
		"	if tpe in ('bigint', 'double', 'timestamp', 'timestamptz', 'inet', 'oid')\n"
		"		then return cast(1.6 * count as bigint);\n"
		"	end if;\n"
		"	if tpe in ('hugeint', 'decimal', 'uuid', 'mbr')\n"
		"		then return cast(3.2 * count as bigint);\n"
		"	end if;\n"
		"	return 0;\n"
		"end;\n"
		"create view sys.storagemodel as\n"
		"select \"schema\", \"table\", \"column\", \"type\", \"count\",\n"
		"	sys.columnsize(\"type\", \"count\") as columnsize,\n"
		"	sys.heapsize(\"type\", \"count\", \"distinct\", \"atomwidth\") as heapsize,\n"
		"	sys.hashsize(\"reference\", \"count\") as hashsize,\n"
		"	case when isacolumn then sys.imprintsize(\"type\", \"count\") else 0 end as imprintsize,\n"
		"	case when (isacolumn and not sorted) then cast(8 * \"count\" as bigint) else 0 end as orderidxsize,\n"
		"	sorted, \"unique\", isacolumn\n"
		" from sys.storagemodelinput\n"
		"order by \"schema\", \"table\", \"column\";\n"
		"create view sys.tablestoragemodel as\n"
		"select \"schema\", \"table\",\n"
		"	max(\"count\") as \"rowcount\",\n"
		"	count(*) as \"storages\",\n"
		"	sum(sys.columnsize(\"type\", \"count\")) as columnsize,\n"
		"	sum(sys.heapsize(\"type\", \"count\", \"distinct\", \"atomwidth\")) as heapsize,\n"
		"	sum(sys.hashsize(\"reference\", \"count\")) as hashsize,\n"
		"	sum(case when isacolumn then sys.imprintsize(\"type\", \"count\") else 0 end) as imprintsize,\n"
		"	sum(case when (isacolumn and not sorted) then cast(8 * \"count\" as bigint) else 0 end) as orderidxsize\n"
		" from sys.storagemodelinput\n"
		"group by \"schema\", \"table\"\n"
		"order by \"schema\", \"table\";\n", day_interval_str, day_interval_str);
	assert(pos < bufsize);

	pos += snprintf(buf + pos, bufsize - pos,
		"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
		" and name in ('storage', 'tablestorage', 'schemastorage', 'storagemodelinput', 'storagemodel', 'tablestoragemodel');\n");
	pos += snprintf(buf + pos, bufsize - pos,
		"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
		" and name in ('storage') and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
		"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
		" and name in ('storagemodelinit') and type = %d;\n", (int) F_PROC);
	pos += snprintf(buf + pos, bufsize - pos,
		"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
		" and name in ('columnsize', 'heapsize', 'hashsize', 'imprintsize') and type = %d;\n", (int) F_FUNC);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_apr2019_sp1(Client c)
{
	char *err, *qry = "select c.id from sys.dependency_types dt, sys._columns c, sys.keys k, sys.objects o "
		"where k.id = o.id and o.name = c.name and c.table_id = k.table_id and dt.dependency_type_name = 'KEY' and k.type = 1 "
		"and not exists (select d.id from sys.dependencies d where d.id = c.id and d.depend_id = k.id and d.depend_type = dt.dependency_type_id);";
	res_table *output = NULL;

	/* Determine if missing dependency table entry for unique keys
	 * is required */
	err = SQLstatementIntern(c, qry, "update", true, false, &output);
	if (err == NULL) {
		BAT *b = BATdescriptor(output->cols[0].b);
		if (b) {
			if (BATcount(b) > 0) {
				/* required update for changeset 23e1231ada99 */
				qry = "insert into sys.dependencies (select c.id as id, k.id as depend_id, dt.dependency_type_id as depend_type from sys.dependency_types dt, sys._columns c, sys.keys k, sys.objects o where k.id = o.id and o.name = c.name and c.table_id = k.table_id and dt.dependency_type_name = 'KEY' and k.type = 1 and not exists (select d.id from sys.dependencies d where d.id = c.id and d.depend_id = k.id and d.depend_type = dt.dependency_type_id));\n";
				printf("Running database upgrade commands:\n%s\n", qry);
				err = SQLstatementIntern(c, qry, "update", true, false, NULL);
			}
			BBPunfix(b->batCacheid);
		}
		res_tables_destroy(output);
	}

	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_apr2019_sp2(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 1000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (!*systabfixed) {
		sql_fix_system_tables(c, sql, prev_schema);
		*systabfixed = true;
	}

	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");

	/* 11_times.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop procedure sys.times();\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#define FLUSH_INSERTS_IF_BUFFERFILLED /* Each new value should add about 20 bytes to the buffer, "flush" when is 200 bytes from being full */ \
	if (pos > 7900) { \
		pos += snprintf(buf + pos, bufsize - pos, \
						") as t1(c1,c2,c3) where t1.c1 not in (select \"id\" from sys.dependencies where depend_id = t1.c2);\n"); \
		assert(pos < bufsize); \
		printf("Running database upgrade commands:\n%s\n", buf); \
		err = SQLstatementIntern(c, buf, "update", true, false, NULL); \
		if (err) \
			goto bailout; \
		pos = 0; \
		pos += snprintf(buf + pos, bufsize - pos, "insert into sys.dependencies select c1, c2, c3 from (values"); \
		ppos = pos; \
		first = true; \
	}

static str
sql_update_nov2019_missing_dependencies(Client c, mvc *sql)
{
	size_t bufsize = 8192, pos = 0, ppos;
	char *err = NULL, *buf = GDKmalloc(bufsize);
	sql_allocator *old_sa = sql->sa;
	bool first = true;
	sql_trans *tr = sql->session->tr;
	struct os_iter si;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (!(sql->sa = sa_create(sql->pa))) {
		err = createException(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	pos += snprintf(buf + pos, bufsize - pos, "insert into sys.dependencies select c1, c2, c3 from (values");
	ppos = pos; /* later check if found updatable database objects */

	os_iterator(&si, sql->session->tr->cat->schemas, sql->session->tr, NULL);
	for (sql_base *b = oi_next(&si); b; oi_next(&si)) {
		sql_schema *s = (sql_schema*)b;

		struct os_iter oi;
		os_iterator(&oi, s->funcs, sql->session->tr, NULL);
		for (sql_base *b = oi_next(&oi); b; oi_next(&oi)) {
			sql_func *f = (sql_func*)b;

			if (f->query && f->lang == FUNC_LANG_SQL) {
				char *relt;
				sql_rel *r = NULL;

				if (!(relt = sa_strdup(sql->sa, f->query))) {
					err = createException(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}

				r = rel_parse(sql, s, relt, m_deps);
				if (r)
					r = sql_processrelation(sql, r, 0, 0);
				if (r) {
					list *id_l = rel_dependencies(sql, r);

					for (node *o = id_l->h ; o ; o = o->next) {
						sqlid next = *(sqlid*) o->data;
						if (next != f->base.id) {
							pos += snprintf(buf + pos, bufsize - pos, "%s(%d,%d,%d)", first ? "" : ",", next,
											f->base.id, (int)(!IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY));
							first = false;
							FLUSH_INSERTS_IF_BUFFERFILLED
						}
					}
				} else if (sql->session->status == -1) {
					sql->session->status = 0;
					sql->errstr[0] = 0;
				}
			}
		}
		if (s->tables) {
			struct os_iter oi;
			os_iterator(&oi, s->tables, tr, NULL);
			for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
				sql_table *t = (sql_table*) b;

				if (t->query && isView(t)) {
					char *relt;
					sql_rel *r = NULL;

					if (!(relt = sa_strdup(sql->sa, t->query))) {
						err = createException(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}

					r = rel_parse(sql, s, relt, m_deps);
					if (r)
						r = sql_processrelation(sql, r, 0, 0);
					if (r) {
						list *id_l = rel_dependencies(sql, r);

						for (node *o = id_l->h ; o ; o = o->next) {
							sqlid next = *(sqlid*) o->data;
							if (next != t->base.id) {
								pos += snprintf(buf + pos, bufsize - pos, "%s(%d,%d,%d)", first ? "" : ",",
												next, t->base.id, (int) VIEW_DEPENDENCY);
								first = false;
								FLUSH_INSERTS_IF_BUFFERFILLED
							}
						}
					}
				}
				if (t->triggers)
					for (node *mm = ol_first_node(t->triggers); mm; mm = mm->next) {
						sql_trigger *tr = (sql_trigger*) mm->data;
						char *relt;
						sql_rel *r = NULL;

						if (!(relt = sa_strdup(sql->sa, tr->statement))) {
							err = createException(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
							goto bailout;
						}

						r = rel_parse(sql, s, relt, m_deps);
						if (r)
							r = sql_processrelation(sql, r, 0, 0);
						if (r) {
							list *id_l = rel_dependencies(sql, r);

							for (node *o = id_l->h ; o ; o = o->next) {
								sqlid next = *(sqlid*) o->data;
								if (next != tr->base.id) {
									pos += snprintf(buf + pos, bufsize - pos, "%s(%d,%d,%d)", first ? "" : ",",
													next, tr->base.id, (int) TRIGGER_DEPENDENCY);
									first = false;
									FLUSH_INSERTS_IF_BUFFERFILLED
								}
							}
						}
					}
			}
		}
	}

	if (ppos != pos) { /* found updatable functions */
		pos += snprintf(buf + pos, bufsize - pos,
						") as t1(c1,c2,c3) where t1.c1 not in (select \"id\" from sys.dependencies where depend_id = t1.c2);\n");

		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}

bailout:
	if (sql->sa)
		sa_destroy(sql->sa);
	sql->sa = old_sa;
	GDKfree(buf);
	return err;
}

static str
sql_update_nov2019(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 16384, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);
	res_table *output;
	BAT *b;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos,
			"select id from sys.args where func_id in (select id from sys.functions where schema_id = (select id from sys.schemas where name = 'sys') and name = 'second' and func = 'sql_seconds') and number = 0 and type_scale = 3;\n");
	err = SQLstatementIntern(c, buf, "update", 1, 0, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) > 0 && !*systabfixed) {
			err = sql_fix_system_tables(c, sql, prev_schema);
			*systabfixed = true;
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);
	if (err) {
		GDKfree(buf);
		return err;
	}

	pos = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"create function sys.deltas (\"schema\" string)"
			" returns table (\"id\" int, \"cleared\" boolean, \"immutable\" bigint, \"inserted\" bigint, \"updates\" bigint, \"deletes\" bigint, \"level\" int)"
			" external name \"sql\".\"deltas\";\n"
			"create function sys.deltas (\"schema\" string, \"table\" string)"
			" returns table (\"id\" int, \"cleared\" boolean, \"immutable\" bigint, \"inserted\" bigint, \"updates\" bigint, \"deletes\" bigint, \"level\" int)"
			" external name \"sql\".\"deltas\";\n"
			"create function sys.deltas (\"schema\" string, \"table\" string, \"column\" string)"
			" returns table (\"id\" int, \"cleared\" boolean, \"immutable\" bigint, \"inserted\" bigint, \"updates\" bigint, \"deletes\" bigint, \"level\" int)"
			" external name \"sql\".\"deltas\";\n"
			"create aggregate median_avg(val TINYINT) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(TINYINT) TO PUBLIC;\n"
			"create aggregate median_avg(val SMALLINT) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(SMALLINT) TO PUBLIC;\n"
			"create aggregate median_avg(val INTEGER) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(INTEGER) TO PUBLIC;\n"
			"create aggregate median_avg(val BIGINT) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(BIGINT) TO PUBLIC;\n"
			"create aggregate median_avg(val DECIMAL) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(DECIMAL) TO PUBLIC;\n"
			"create aggregate median_avg(val REAL) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(REAL) TO PUBLIC;\n"
			"create aggregate median_avg(val DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile_avg(val TINYINT, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(TINYINT, DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile_avg(val SMALLINT, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(SMALLINT, DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile_avg(val INTEGER, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(INTEGER, DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile_avg(val BIGINT, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(BIGINT, DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile_avg(val DECIMAL, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(DECIMAL, DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile_avg(val REAL, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(REAL, DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile_avg(val DOUBLE, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(DOUBLE, DOUBLE) TO PUBLIC;\n");
#ifdef HAVE_HGE
	pos += snprintf(buf + pos, bufsize - pos,
				"create aggregate median_avg(val HUGEINT) returns DOUBLE\n"
				" external name \"aggr\".\"median_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE median_avg(HUGEINT) TO PUBLIC;\n"
				"create aggregate quantile_avg(val HUGEINT, q DOUBLE) returns DOUBLE\n"
				" external name \"aggr\".\"quantile_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile_avg(HUGEINT, DOUBLE) TO PUBLIC;\n");
#endif
	/* 60/61_wlcr signatures migrations */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop procedure master();\n"
			"drop procedure master(string);\n"
			"drop procedure stopmaster();\n"
			"drop procedure masterbeat(int);\n"
			"drop function masterClock();\n"
			"drop function masterTick();\n"
			"drop procedure replicate();\n"
			"drop procedure replicate(timestamp);\n"
			"drop procedure replicate(string);\n"
			"drop procedure replicate(string, timestamp);\n"
			"drop procedure replicate(string, tinyint);\n"
			"drop procedure replicate(string, smallint);\n"
			"drop procedure replicate(string, integer);\n"
			"drop procedure replicate(string, bigint);\n"
			"drop procedure replicabeat(integer);\n"
			"drop function replicaClock();\n"
			"drop function replicaTick();\n"

			"create schema wlc;\n"
			"create procedure wlc.master()\n"
			"external name wlc.master;\n"
			"create procedure wlc.master(path string)\n"
			"external name wlc.master;\n"
			"create procedure wlc.stop()\n"
			"external name wlc.stop;\n"
			"create procedure wlc.flush()\n"
			"external name wlc.flush;\n"
			"create procedure wlc.beat( duration int)\n"
			"external name wlc.\"setbeat\";\n"
			"create function wlc.clock() returns string\n"
			"external name wlc.\"getclock\";\n"
			"create function wlc.tick() returns bigint\n"
			"external name wlc.\"gettick\";\n"

			"create schema wlr;\n"
			"create procedure wlr.master(dbname string)\n"
			"external name wlr.master;\n"
			"create procedure wlr.stop()\n"
			"external name wlr.stop;\n"
			"create procedure wlr.accept()\n"
			"external name wlr.accept;\n"
			"create procedure wlr.replicate()\n"
			"external name wlr.replicate;\n"
			"create procedure wlr.replicate(pointintime timestamp)\n"
			"external name wlr.replicate;\n"
			"create procedure wlr.replicate(id tinyint)\n"
			"external name wlr.replicate;\n"
			"create procedure wlr.replicate(id smallint)\n"
			"external name wlr.replicate;\n"
			"create procedure wlr.replicate(id integer)\n"
			"external name wlr.replicate;\n"
			"create procedure wlr.replicate(id bigint)\n"
			"external name wlr.replicate;\n"
			"create procedure wlr.beat(duration integer)\n"
			"external name wlr.\"setbeat\";\n"
			"create function wlr.clock() returns string\n"
			"external name wlr.\"getclock\";\n"
			"create function wlr.tick() returns bigint\n"
			"external name wlr.\"gettick\";\n"
		);

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('deltas') and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('median_avg', 'quantile_avg') and type = %d;\n", (int) F_AGGR);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.schemas set system = true where name in ('wlc', 'wlr');\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'wlc')"
			" and name in ('clock', 'tick') and type = %d;\n", (int) F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'wlc')"
			" and name in ('master', 'stop', 'flush', 'beat') and type = %d;\n", (int) F_PROC);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'wlr')"
			" and name in ('clock', 'tick') and type = %d;\n", (int) F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'wlr')"
			" and name in ('master', 'stop', 'accept', 'replicate', 'beat') and type = %d;\n", (int) F_PROC);

	/* 39_analytics.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate stddev_samp(val INTERVAL SECOND) returns DOUBLE\n"
			"external name \"aggr\".\"stdev\";\n"
			"GRANT EXECUTE ON AGGREGATE stddev_samp(INTERVAL SECOND) TO PUBLIC;\n"
			"create aggregate stddev_samp(val INTERVAL MONTH) returns DOUBLE\n"
			"external name \"aggr\".\"stdev\";\n"
			"GRANT EXECUTE ON AGGREGATE stddev_samp(INTERVAL MONTH) TO PUBLIC;\n"

			"create aggregate stddev_pop(val INTERVAL SECOND) returns DOUBLE\n"
			"external name \"aggr\".\"stdevp\";\n"
			"GRANT EXECUTE ON AGGREGATE stddev_pop(INTERVAL SECOND) TO PUBLIC;\n"
			"create aggregate stddev_pop(val INTERVAL MONTH) returns DOUBLE\n"
			"external name \"aggr\".\"stdevp\";\n"
			"GRANT EXECUTE ON AGGREGATE stddev_pop(INTERVAL MONTH) TO PUBLIC;\n"

			"create aggregate var_samp(val INTERVAL SECOND) returns DOUBLE\n"
			"external name \"aggr\".\"variance\";\n"
			"GRANT EXECUTE ON AGGREGATE var_samp(INTERVAL SECOND) TO PUBLIC;\n"
			"create aggregate var_samp(val INTERVAL MONTH) returns DOUBLE\n"
			"external name \"aggr\".\"variance\";\n"
			"GRANT EXECUTE ON AGGREGATE var_samp(INTERVAL MONTH) TO PUBLIC;\n"

			"create aggregate var_pop(val INTERVAL SECOND) returns DOUBLE\n"
			"external name \"aggr\".\"variancep\";\n"
			"GRANT EXECUTE ON AGGREGATE var_pop(INTERVAL SECOND) TO PUBLIC;\n"
			"create aggregate var_pop(val INTERVAL MONTH) returns DOUBLE\n"
			"external name \"aggr\".\"variancep\";\n"
			"GRANT EXECUTE ON AGGREGATE var_pop(INTERVAL MONTH) TO PUBLIC;\n"

			"create aggregate median(val INTERVAL SECOND) returns INTERVAL SECOND\n"
			"external name \"aggr\".\"median\";\n"
			"GRANT EXECUTE ON AGGREGATE median(INTERVAL SECOND) TO PUBLIC;\n"
			"create aggregate median(val INTERVAL MONTH) returns INTERVAL MONTH\n"
			"external name \"aggr\".\"median\";\n"
			"GRANT EXECUTE ON AGGREGATE median(INTERVAL MONTH) TO PUBLIC;\n"

			"create aggregate quantile(val INTERVAL SECOND, q DOUBLE) returns INTERVAL SECOND\n"
			"external name \"aggr\".\"quantile\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile(INTERVAL SECOND, DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile(val INTERVAL MONTH, q DOUBLE) returns INTERVAL MONTH\n"
			"external name \"aggr\".\"quantile\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile(INTERVAL MONTH, DOUBLE) TO PUBLIC;\n"
		);

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'median', 'quantile') and type = %d;\n", (int) F_AGGR);

	/* The MAL implementation of functions json.text(string) and json.text(int) do not exist */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function json.text(string);\n"
			"drop function json.text(int);\n");

	/* The first argument to copyfrom is a PTR type */
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.args set type = 'ptr' where"
			" func_id = (select id from sys.functions where name = 'copyfrom' and func = 'copy_from' and mod = 'sql' and type = %d) and name = 'arg_1';\n", (int) F_UNION);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#ifdef HAVE_HGE
static str
sql_update_nov2019_sp1_hugeint(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 1024, pos = 0;
	char *buf, *err;

	if (!*systabfixed &&
	    (err = sql_fix_system_tables(c, sql, prev_schema)) != NULL)
		return err;
	*systabfixed = true;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	/* 39_analytics_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate median_avg(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(HUGEINT) TO PUBLIC;\n"
			"create aggregate quantile_avg(val HUGEINT, q DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(HUGEINT, DOUBLE) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and name in ('median_avg', 'quantile_avg') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n", (int) F_AGGR);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_update_jun2020(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	sql_table *t;
	size_t bufsize = 32768, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);
	sql_schema *sys = mvc_bind_schema(sql, "sys");

	if (!*systabfixed &&
	    (err = sql_fix_system_tables(c, sql, prev_schema)) != NULL)
		return err;
	*systabfixed = true;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n");

	/* convert old PYTHON2 and PYTHON2_MAP to PYTHON and PYTHON_MAP
	 * see also function load_func() in store.c */
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set language = language - 2 where language in (8, 9);\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.args set name = name || '_' || cast(number as string) where name in ('arg', 'res') and func_id in (select id from sys.functions f where f.system);\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.dependencies values ((select id from sys.functions where name = 'ms_trunc' and schema_id = (select id from sys.schemas where name = 'sys')), (select id from sys.functions where name = 'ms_round' and schema_id = (select id from sys.schemas where name = 'sys')), (select dependency_type_id from sys.dependency_types where dependency_type_name = 'FUNCTION'));\n");

	/* 12_url */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function isaURL(url);\n"
			"CREATE function isaURL(theUrl string) RETURNS BOOL\n"
			" EXTERNAL NAME url.\"isaURL\";\n"
			"GRANT EXECUTE ON FUNCTION isaURL(string) TO PUBLIC;\n"
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'isaurl' and type = %d;\n", (int) F_FUNC);

	/* 13_date.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function str_to_time(string, string);\n"
			"drop function time_to_str(time, string);\n"
			"drop function str_to_timestamp(string, string);\n"
			"drop function timestamp_to_str(timestamp, string);\n"
			"create function str_to_time(s string, format string) returns time with time zone\n"
			" external name mtime.\"str_to_time\";\n"
			"create function time_to_str(d time with time zone, format string) returns string\n"
			" external name mtime.\"time_to_str\";\n"
			"create function str_to_timestamp(s string, format string) returns timestamp with time zone\n"
			" external name mtime.\"str_to_timestamp\";\n"
			"create function timestamp_to_str(d timestamp with time zone, format string) returns string\n"
			" external name mtime.\"timestamp_to_str\";\n"
			"grant execute on function str_to_time to public;\n"
			"grant execute on function time_to_str to public;\n"
			"grant execute on function str_to_timestamp to public;\n"
			"grant execute on function timestamp_to_str to public;\n"
			"update sys.functions set system = true where system <> true and name in"
			" ('str_to_time', 'str_to_timestamp', 'time_to_str', 'timestamp_to_str')"
			" and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n", (int) F_FUNC);

	/* 16_tracelog */
	t = mvc_bind_table(sql, sys, "tracelog");
	t->system = 0; /* make it non-system else the drop view will fail */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tracelog;\n"
			"drop function sys.tracelog();\n"
			"create function sys.tracelog()\n"
			" returns table (\n"
			"  ticks bigint, -- time in microseconds\n"
			"  stmt string  -- actual statement executed\n"
			" )\n"
			" external name sql.dump_trace;\n"
			"create view sys.tracelog as select * from sys.tracelog();\n"
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'tracelog' and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'tracelog';\n");

	/* 17_temporal.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function sys.epoch(bigint);\n"
			"drop function sys.epoch(int);\n"
			"drop function sys.epoch(timestamp);\n"
			"drop function sys.epoch(timestamp with time zone);\n"
			"create function sys.epoch(sec BIGINT) returns TIMESTAMP WITH TIME ZONE\n"
			" external name mtime.epoch;\n"
			"create function sys.epoch(sec INT) returns TIMESTAMP WITH TIME ZONE\n"
			" external name mtime.epoch;\n"
			"create function sys.epoch(ts TIMESTAMP WITH TIME ZONE) returns INT\n"
			" external name mtime.epoch;\n"
			"create function sys.date_trunc(txt string, t timestamp with time zone)\n"
			"returns timestamp with time zone\n"
			"external name sql.date_trunc;\n"
			"grant execute on function sys.date_trunc(string, timestamp with time zone) to public;\n"
			"grant execute on function sys.epoch (BIGINT) to public;\n"
			"grant execute on function sys.epoch (INT) to public;\n"
			"grant execute on function sys.epoch (TIMESTAMP WITH TIME ZONE) to public;\n"
			"update sys.functions set system = true where system <> true and name in"
			" ('epoch', 'date_trunc')"
			" and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n", (int) F_FUNC);

	/* 22_clients */
	t = mvc_bind_table(sql, sys, "sessions");
	t->system = 0; /* make it non-system else the drop view will fail */

	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.sessions;\n"
			"drop function sys.sessions;\n"
 			"create function sys.sessions()\n"
			"returns table(\n"
				"\"sessionid\" int,\n"
				"\"username\" string,\n"
				"\"login\" timestamp,\n"
				"\"idle\" timestamp,\n"
				"\"optimizer\" string,\n"
				"\"sessiontimeout\" int,\n"
				"\"querytimeout\" int,\n"
				"\"workerlimit\" int,\n"
				"\"memorylimit\" int)\n"
 			" external name sql.sessions;\n"
			"create view sys.sessions as select * from sys.sessions();\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"grant execute on procedure sys.settimeout(bigint) to public;\n"
			"grant execute on procedure sys.settimeout(bigint,bigint) to public;\n"
			"grant execute on procedure sys.setsession(bigint) to public;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create procedure sys.setoptimizer(\"optimizer\" string)\n"
			" external name clients.setoptimizer;\n"
			"grant execute on procedure sys.setoptimizer(string) to public;\n"
			"create procedure sys.setquerytimeout(\"query\" int)\n"
			" external name clients.setquerytimeout;\n"
			"grant execute on procedure sys.setquerytimeout(int) to public;\n"
			"create procedure sys.setsessiontimeout(\"timeout\" int)\n"
			" external name clients.setsessiontimeout;\n"
			"grant execute on procedure sys.setsessiontimeout(int) to public;\n"
			"create procedure sys.setworkerlimit(\"limit\" int)\n"
			" external name clients.setworkerlimit;\n"
			"grant execute on procedure sys.setworkerlimit(int) to public;\n"
			"create procedure sys.setmemorylimit(\"limit\" int)\n"
			" external name clients.setmemorylimit;\n"
			"grant execute on procedure sys.setmemorylimit(int) to public;\n"
			"create procedure sys.setoptimizer(\"sessionid\" int, \"optimizer\" string)\n"
			" external name clients.setoptimizer;\n"
			"create procedure sys.setquerytimeout(\"sessionid\" int, \"query\" int)\n"
			" external name clients.setquerytimeout;\n"
			"create procedure sys.setsessiontimeout(\"sessionid\" int, \"query\" int)\n"
			" external name clients.setsessiontimeout;\n"
			"create procedure sys.setworkerlimit(\"sessionid\" int, \"limit\" int)\n"
			" external name clients.setworkerlimit;\n"
			"create procedure sys.setmemorylimit(\"sessionid\" int, \"limit\" int)\n"
			" external name clients.setmemorylimit;\n"
			"create procedure sys.stopsession(\"sessionid\" int)\n"
			" external name clients.stopsession;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.prepared_statements()\n"
			"returns table(\n"
			"\"sessionid\" int,\n"
			"\"username\" string,\n"
			"\"statementid\" int,\n"
			"\"statement\" string,\n"
			"\"created\" timestamp)\n"
			" external name sql.prepared_statements;\n"
			"grant execute on function sys.prepared_statements to public;\n"
			"create view sys.prepared_statements as select * from sys.prepared_statements();\n"
			"grant select on sys.prepared_statements to public;\n"
			"create function sys.prepared_statements_args()\n"
			"returns table(\n"
			"\"statementid\" int,\n"
			"\"type\" string,\n"
			"\"type_digits\" int,\n"
			"\"type_scale\" int,\n"
			"\"inout\" tinyint,\n"
			"\"number\" int,\n"
			"\"schema\" string,\n"
			"\"table\" string,\n"
			"\"column\" string)\n"
			" external name sql.prepared_statements_args;\n"
			"grant execute on function sys.prepared_statements_args to public;\n"
			"create view sys.prepared_statements_args as select * from sys.prepared_statements_args();\n"
			"grant select on sys.prepared_statements_args to public;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('sessions', 'prepared_statements', 'prepared_statements_args') and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('sessions', 'prepared_statements', 'prepared_statements_args');\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('setoptimizer', 'setquerytimeout', 'setsessiontimeout', 'setworkerlimit', 'setmemorylimit', 'setoptimizer', 'stopsession') and type = %d;\n", (int) F_PROC);

	/* 25_debug */
	pos += snprintf(buf + pos, bufsize - pos,
			"create procedure sys.suspend_log_flushing()\n"
			" external name sql.suspend_log_flushing;\n"
			"create procedure sys.resume_log_flushing()\n"
			" external name sql.resume_log_flushing;\n"
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('suspend_log_flushing', 'resume_log_flushing') and type = %d;\n", (int) F_PROC);

	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.debug(flag string) returns integer\n"
			" external name mdb.\"setDebug\";\n"
			"create function sys.debugflags()\n"
			" returns table(flag string, val bool)\n"
			" external name mdb.\"getDebugFlags\";\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('debug') and type = %d;\n"
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('debugflags') and type = %d;\n",
			(int) F_FUNC, (int) F_UNION);

	/* 26_sysmon */
	t = mvc_bind_table(sql, sys, "queue");
	t->system = 0; /* make it non-system else the drop view will fail */

	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.queue;\n"
			"drop function sys.queue;\n"
			"create function sys.queue()\n"
			"returns table(\n"
			"\"tag\" bigint,\n"
			"\"sessionid\" int,\n"
			"\"username\" string,\n"
			"\"started\" timestamp,\n"
			"\"status\" string,\n"
			"\"query\" string,\n"
			"\"progress\" int,\n"
			"\"workers\" int,\n"
			"\"memory\" int)\n"
			" external name sysmon.queue;\n"
			"grant execute on function sys.queue to public;\n"
			"create view sys.queue as select * from sys.queue();\n"
			"grant select on sys.queue to public;\n"

			"drop procedure sys.pause(int);\n"
			"drop procedure sys.resume(int);\n"
			"drop procedure sys.stop(int);\n"

			"grant execute on procedure sys.pause(bigint) to public;\n"
			"grant execute on procedure sys.resume(bigint) to public;\n"
			"grant execute on procedure sys.stop(bigint) to public;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'queue' and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'queue';\n");

	/* 39_analytics.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create window stddev_samp(val TINYINT) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(TINYINT) TO PUBLIC;\n"
			"create window stddev_samp(val SMALLINT) returns DOUBLE"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(SMALLINT) TO PUBLIC;\n"
			"create window stddev_samp(val INTEGER) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(INTEGER) TO PUBLIC;\n"
			"create window stddev_samp(val BIGINT) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(BIGINT) TO PUBLIC;\n"
			"create window stddev_samp(val REAL) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(REAL) TO PUBLIC;\n"
			"create window stddev_samp(val DOUBLE) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(DOUBLE) TO PUBLIC;\n"
			"create window stddev_samp(val INTERVAL SECOND) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(INTERVAL SECOND) TO PUBLIC;\n"
			"create window stddev_samp(val INTERVAL MONTH) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(INTERVAL MONTH) TO PUBLIC;\n"
			"create window stddev_pop(val TINYINT) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(TINYINT) TO PUBLIC;\n"
			"create window stddev_pop(val SMALLINT) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(SMALLINT) TO PUBLIC;\n"
			"create window stddev_pop(val INTEGER) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(INTEGER) TO PUBLIC;\n"
			"create window stddev_pop(val BIGINT) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(BIGINT) TO PUBLIC;\n"
			"create window stddev_pop(val REAL) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(REAL) TO PUBLIC;\n"
			"create window stddev_pop(val DOUBLE) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(DOUBLE) TO PUBLIC;\n"
			"create window stddev_pop(val INTERVAL SECOND) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(INTERVAL SECOND) TO PUBLIC;\n"
			"create window stddev_pop(val INTERVAL MONTH) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(INTERVAL MONTH) TO PUBLIC;\n"
			"create window var_samp(val TINYINT) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(TINYINT) TO PUBLIC;\n"
			"create window var_samp(val SMALLINT) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(SMALLINT) TO PUBLIC;\n"
			"create window var_samp(val INTEGER) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(INTEGER) TO PUBLIC;\n"
			"create window var_samp(val BIGINT) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(BIGINT) TO PUBLIC;\n"
			"create window var_samp(val REAL) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(REAL) TO PUBLIC;\n"
			"create window var_samp(val DOUBLE) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(DOUBLE) TO PUBLIC;\n"
			"create window var_samp(val INTERVAL SECOND) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(INTERVAL SECOND) TO PUBLIC;\n"
			"create window var_samp(val INTERVAL MONTH) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(INTERVAL MONTH) TO PUBLIC;\n"
			"create window var_pop(val TINYINT) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(TINYINT) TO PUBLIC;\n"
			"create window var_pop(val SMALLINT) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(SMALLINT) TO PUBLIC;\n"
			"create window var_pop(val INTEGER) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(INTEGER) TO PUBLIC;\n"
			"create window var_pop(val BIGINT) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(BIGINT) TO PUBLIC;\n"
			"create window var_pop(val REAL) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(REAL) TO PUBLIC;\n"
			"create window var_pop(val DOUBLE) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(DOUBLE) TO PUBLIC;\n"
			"create window var_pop(val INTERVAL SECOND) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(INTERVAL SECOND) TO PUBLIC;\n"
			"create window var_pop(val INTERVAL MONTH) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(INTERVAL MONTH) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 TINYINT, e2 TINYINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(TINYINT, TINYINT) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 SMALLINT, e2 SMALLINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(SMALLINT, SMALLINT) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 INTEGER, e2 INTEGER) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(INTEGER, INTEGER) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 BIGINT, e2 BIGINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(BIGINT, BIGINT) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 REAL, e2 REAL) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(REAL, REAL) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 DOUBLE, e2 DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(DOUBLE, DOUBLE) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 INTERVAL SECOND, e2 INTERVAL SECOND) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(INTERVAL SECOND, INTERVAL SECOND) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 INTERVAL MONTH, e2 INTERVAL MONTH) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(INTERVAL MONTH, INTERVAL MONTH) TO PUBLIC;\n"
			"create window covar_samp(e1 TINYINT, e2 TINYINT) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(TINYINT, TINYINT) TO PUBLIC;\n"
			"create window covar_samp(e1 SMALLINT, e2 SMALLINT) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(SMALLINT, SMALLINT) TO PUBLIC;\n"
			"create window covar_samp(e1 INTEGER, e2 INTEGER) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(INTEGER, INTEGER) TO PUBLIC;\n"
			"create window covar_samp(e1 BIGINT, e2 BIGINT) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(BIGINT, BIGINT) TO PUBLIC;\n"
			"create window covar_samp(e1 REAL, e2 REAL) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(REAL, REAL) TO PUBLIC;\n"
			"create window covar_samp(e1 DOUBLE, e2 DOUBLE) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(DOUBLE, DOUBLE) TO PUBLIC;\n"
			"create window covar_samp(e1 INTERVAL SECOND, e2 INTERVAL SECOND) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(INTERVAL SECOND, INTERVAL SECOND) TO PUBLIC;\n"
			"create window covar_samp(e1 INTERVAL MONTH, e2 INTERVAL MONTH) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(INTERVAL MONTH, INTERVAL MONTH) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 TINYINT, e2 TINYINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(TINYINT, TINYINT) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 SMALLINT, e2 SMALLINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(SMALLINT, SMALLINT) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 INTEGER, e2 INTEGER) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(INTEGER, INTEGER) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 BIGINT, e2 BIGINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(BIGINT, BIGINT) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 REAL, e2 REAL) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(REAL, REAL) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 DOUBLE, e2 DOUBLE) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(DOUBLE, DOUBLE) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 INTERVAL SECOND, e2 INTERVAL SECOND) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(INTERVAL SECOND, INTERVAL SECOND) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 INTERVAL MONTH, e2 INTERVAL MONTH) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(INTERVAL MONTH, INTERVAL MONTH) TO PUBLIC;\n"
			"create window covar_pop(e1 TINYINT, e2 TINYINT) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(TINYINT, TINYINT) TO PUBLIC;\n"
			"create window covar_pop(e1 SMALLINT, e2 SMALLINT) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(SMALLINT, SMALLINT) TO PUBLIC;\n"
			"create window covar_pop(e1 INTEGER, e2 INTEGER) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(INTEGER, INTEGER) TO PUBLIC;\n"
			"create window covar_pop(e1 BIGINT, e2 BIGINT) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(BIGINT, BIGINT) TO PUBLIC;\n"
			"create window covar_pop(e1 REAL, e2 REAL) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(REAL, REAL) TO PUBLIC;\n"
			"create window covar_pop(e1 DOUBLE, e2 DOUBLE) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(DOUBLE, DOUBLE) TO PUBLIC;\n"
			"create window covar_pop(e1 INTERVAL SECOND, e2 INTERVAL SECOND) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(INTERVAL SECOND, INTERVAL SECOND) TO PUBLIC;\n"
			"create window covar_pop(e1 INTERVAL MONTH, e2 INTERVAL MONTH) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(INTERVAL MONTH, INTERVAL MONTH) TO PUBLIC;\n"
			"create aggregate corr(e1 INTERVAL SECOND, e2 INTERVAL SECOND) returns DOUBLE\n"
			" external name \"aggr\".\"corr\";\n"
			"GRANT EXECUTE ON AGGREGATE corr(INTERVAL SECOND, INTERVAL SECOND) TO PUBLIC;\n"
			"create aggregate corr(e1 INTERVAL MONTH, e2 INTERVAL MONTH) returns DOUBLE\n"
			" external name \"aggr\".\"corr\";\n"
			"GRANT EXECUTE ON AGGREGATE corr(INTERVAL MONTH, INTERVAL MONTH) TO PUBLIC;\n"
			"create window corr(e1 TINYINT, e2 TINYINT) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(TINYINT, TINYINT) TO PUBLIC;\n"
			"create window corr(e1 SMALLINT, e2 SMALLINT) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(SMALLINT, SMALLINT) TO PUBLIC;\n"
			"create window corr(e1 INTEGER, e2 INTEGER) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(INTEGER, INTEGER) TO PUBLIC;\n"
			"create window corr(e1 BIGINT, e2 BIGINT) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(BIGINT, BIGINT) TO PUBLIC;\n"
			"create window corr(e1 REAL, e2 REAL) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(REAL, REAL) TO PUBLIC;\n"
			"create window corr(e1 DOUBLE, e2 DOUBLE) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(DOUBLE, DOUBLE) TO PUBLIC;\n"
			"create window corr(e1 INTERVAL SECOND, e2 INTERVAL SECOND) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(INTERVAL SECOND, INTERVAL SECOND) TO PUBLIC;\n"
			"create window corr(e1 INTERVAL MONTH, e2 INTERVAL MONTH) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(INTERVAL MONTH, INTERVAL MONTH) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
		"create window sys.group_concat(str STRING) returns STRING\n"
		" external name \"sql\".\"str_group_concat\";\n"
		"GRANT EXECUTE ON WINDOW sys.group_concat(STRING) TO PUBLIC;\n"
		"create window sys.group_concat(str STRING, sep STRING) returns STRING\n"
		" external name \"sql\".\"str_group_concat\";\n"
		"GRANT EXECUTE ON WINDOW sys.group_concat(STRING, STRING) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and name in"
			" ('stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'covar_samp', 'covar_pop', 'corr', 'group_concat')"
			" and schema_id = (select id from sys.schemas where name = 'sys') and type in (%d, %d);\n", (int) F_ANALYTIC, (int) F_AGGR);

	pos += snprintf(buf + pos, bufsize - pos,
			"DROP AGGREGATE stddev_samp(date);\n"
			"DROP AGGREGATE stddev_samp(time);\n"
			"DROP AGGREGATE stddev_samp(timestamp);\n"
			"DROP AGGREGATE stddev_pop(date);\n"
			"DROP AGGREGATE stddev_pop(time);\n"
			"DROP AGGREGATE stddev_pop(timestamp);\n"
			"DROP AGGREGATE var_samp(date);\n"
			"DROP AGGREGATE var_samp(time);\n"
			"DROP AGGREGATE var_samp(timestamp);\n"
			"DROP AGGREGATE var_pop(date);\n"
			"DROP AGGREGATE var_pop(time);\n"
			"DROP AGGREGATE var_pop(timestamp);\n");

	/* 51_sys_schema_extensions */
	pos += snprintf(buf + pos, bufsize - pos,
			"ALTER TABLE sys.keywords SET READ WRITE;\n"
			"DELETE FROM sys.keywords where keyword IN ('NOCYCLE','NOMAXVALUE','NOMINVALUE');\n"
			"insert into sys.keywords values ('ANALYZE'),('AT'),('AUTHORIZATION'),('CACHE'),('CENTURY'),('COLUMN'),('CLIENT'),"
			"('CUBE'),('CYCLE'),('DATA'),('DATE'),('DEBUG'),('DECADE'),('DEALLOCATE'),('DIAGNOSTICS'),('DISTINCT'),"
			"('DOW'),('DOY'),('EXEC'),('EXECUTE'),('EXPLAIN'),('FIRST'),('FWF'),('GROUPING'),('GROUPS'),('INCREMENT'),"
			"('INTERVAL'),('KEY'),('LANGUAGE'),('LARGE'),('LAST'),('LATERAL'),('LEVEL'),('LOADER'),('MATCH'),('MATCHED'),('MAXVALUE'),"
			"('MINVALUE'),('NAME'),('NO'),('NULLS'),('OBJECT'),('OPTIONS'),('PASSWORD'),('PLAN'),('PRECISION'),('PREP'),('PREPARE'),"
			"('QUARTER'),('RELEASE'),('REPLACE'),('ROLLUP'),('SCHEMA'),('SEED'),('SERVER'),('SESSION'),('SETS'),('SIZE'),"
			"('STATEMENT'),('TABLE'),('TEMP'),('TEMPORARY'),('TEXT'),('TIME'),('TIMESTAMP'),('TRACE'),('TYPE'),"
			"('WEEK'),('YEAR'),('ZONE');\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"ALTER TABLE sys.function_languages SET READ WRITE;\n"
			"DELETE FROM sys.function_languages where language_keyword IN ('PYTHON2','PYTHON2_MAP');\n");

	/* 58_hot_snapshot */
	pos += snprintf(buf + pos, bufsize - pos,
			"create procedure sys.hot_snapshot(tarfile string)\n"
			" external name sql.hot_snapshot;\n"
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('hot_snapshot') and type = %d;\n", (int) F_PROC);

	/* 81_tracer.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"CREATE SCHEMA logging;\n"
			"CREATE PROCEDURE logging.flush()\n"
			" EXTERNAL NAME logging.flush;\n"
			"CREATE PROCEDURE logging.setcomplevel(comp_id STRING, lvl_id STRING)\n"
			" EXTERNAL NAME logging.setcomplevel;\n"
			"CREATE PROCEDURE logging.resetcomplevel(comp_id STRING)\n"
			" EXTERNAL NAME logging.resetcomplevel;\n"
			"CREATE PROCEDURE logging.setlayerlevel(layer_id STRING, lvl_id STRING)\n"
			" EXTERNAL NAME logging.setlayerlevel;\n"
			"CREATE PROCEDURE logging.resetlayerlevel(layer_id STRING)\n"
			" EXTERNAL NAME logging.resetlayerlevel;\n"
			"CREATE PROCEDURE logging.setflushlevel(lvl_id STRING)\n"
			" EXTERNAL NAME logging.setflushlevel;\n"
			"CREATE PROCEDURE logging.resetflushlevel()\n"
			" EXTERNAL NAME logging.resetflushlevel;\n"
			"CREATE PROCEDURE logging.setadapter(adapter_id STRING)\n"
			" EXTERNAL NAME logging.setadapter;\n"
			"CREATE PROCEDURE logging.resetadapter()\n"
			" EXTERNAL NAME logging.resetadapter;\n"
			"CREATE FUNCTION logging.compinfo()\n"
			"RETURNS TABLE(\n"
			" \"id\" int,\n"
			" \"component\" string,\n"
			" \"log_level\" string\n"
			")\n"
			"EXTERNAL NAME logging.compinfo;\n"
			"GRANT EXECUTE ON FUNCTION logging.compinfo TO public;\n"
			"CREATE view logging.compinfo AS SELECT * FROM logging.compinfo();\n"
			"GRANT SELECT ON logging.compinfo TO public;\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.schemas set system = true where name = 'logging';\n"
			"update sys.functions set system = true where system <> true and name in"
			" ('flush', 'setcomplevel', 'resetcomplevel', 'setlayerlevel', 'resetlayerlevel', 'setflushlevel', 'resetflushlevel', 'setadapter', 'resetadapter')"
			" and schema_id = (select id from sys.schemas where name = 'logging') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in"
			" ('compinfo')"
			" and schema_id = (select id from sys.schemas where name = 'logging') and type = %d;\n"
			"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'logging')"
			" and name = 'compinfo';\n",
			(int) F_PROC, (int) F_UNION);

	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	if (err == MAL_SUCCEED) {
		pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
			       "ALTER TABLE sys.keywords SET READ ONLY;\n"
			       "ALTER TABLE sys.function_languages SET READ ONLY;\n");
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jun2020_bam(Client c, mvc *m, const char *prev_schema)
{
	size_t bufsize = 10240, pos = 0;
	char *err = NULL, *buf;
	res_table *output;
	BAT *b;
	sql_schema *s = mvc_bind_schema(m, "bam");
	sql_table *t;

	if (s == NULL || !s->system)
		return NULL;	/* no system schema "bam": nothing to do */

	buf = GDKmalloc(bufsize);
	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	s->system = 0;
	if ((t = mvc_bind_table(m, s, "files")) != NULL)
		t->system = 0;
	if ((t = mvc_bind_table(m, s, "sq")) != NULL)
		t->system = 0;
	if ((t = mvc_bind_table(m, s, "rg")) != NULL)
		t->system = 0;
	if ((t = mvc_bind_table(m, s, "pg")) != NULL)
		t->system = 0;
	if ((t = mvc_bind_table(m, s, "export")) != NULL)
		t->system = 0;

	/* check if any of the tables in the bam schema have any content */
	pos += snprintf(buf + pos, bufsize - pos,
			"select sum(count) from sys.storage('bam');\n");
	err = SQLstatementIntern(c, buf, "update", 1, 0, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	pos = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema sys;\n"
			"update sys.schemas set system = false where name = 'bam';\n"
			"update sys._tables set system = false where schema_id in (select id from sys.schemas where name = 'bam');\n"
			"drop procedure bam.bam_loader_repos;\n"
			"drop procedure bam.bam_loader_files;\n"
			"drop procedure bam.bam_loader_file;\n"
			"drop procedure bam.bam_drop_file;\n"
			"drop function bam.bam_flag;\n"
			"drop function bam.reverse_seq;\n"
			"drop function bam.reverse_qual;\n"
			"drop function bam.seq_length;\n"
			"drop function bam.seq_char;\n"
			"drop procedure bam.sam_export;\n"
			"drop procedure bam.bam_export;\n");
	if (b) {
		if (BATcount(b) > 0 && *(lng *) Tloc(b, 0) == 0) {
			/* tables in bam schema are empty: drop them */
			pos += snprintf(buf + pos, bufsize - pos,
					"drop table bam.sq;\n"
					"drop table bam.rg;\n"
					"drop table bam.pg;\n"
					"drop table bam.export;\n"
					"drop table bam.files;\n"
					"drop schema bam;\n");
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);

	GDKfree(buf);
	return err;
}

#ifdef HAVE_HGE
static str
sql_update_jun2020_sp1_hugeint(Client c, const char *prev_schema)
{
	size_t bufsize = 8192, pos = 0;
	char *buf, *err;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	/* 39_analytics_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create window stddev_samp(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"stdev\";\n"
			"GRANT EXECUTE ON WINDOW stddev_samp(HUGEINT) TO PUBLIC;\n"
			"create window stddev_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"stdevp\";\n"
			"GRANT EXECUTE ON WINDOW stddev_pop(HUGEINT) TO PUBLIC;\n"
			"create window var_samp(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"variance\";\n"
			"GRANT EXECUTE ON WINDOW var_samp(HUGEINT) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window covar_samp(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window var_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(HUGEINT) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window covar_pop(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(HUGEINT, HUGEINT) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and name in ('covar_samp', 'covar_pop') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'covar_samp', 'var_pop', 'covar_pop', 'corr') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n",
			(int) F_AGGR, (int) F_ANALYTIC);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_update_jun2020_sp2(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	/* we need to update the system tables, but only if we haven't done
	 * so already, and if we are actually upgrading the database */
	if (*systabfixed)
		return MAL_SUCCEED;		/* already done */

	char *buf = "select id from sys.functions where name = 'nullif' and schema_id = (select id from sys.schemas where name = 'sys');\n";
	res_table *output;
	char *err = SQLstatementIntern(c, buf, "update", 1, 0, &output);
	if (err == NULL) {
		BAT *b = BATdescriptor(output->cols[0].b);
		if (b) {
			if (BATcount(b) == 0) {
				err = sql_fix_system_tables(c, sql, prev_schema);
				*systabfixed = true;
			}
			BBPunfix(b->batCacheid);
		}
		res_table_destroy(output);
	}
	return err;
}

static str
sql_update_oscar_lidar(Client c)
{
	char *query =
		"drop procedure sys.lidarattach(string);\n"
		"drop procedure sys.lidarload(string);\n"
		"drop procedure sys.lidarexport(string, string, string);\n";
	printf("Running database upgrade commands:\n%s\n", query);
	return SQLstatementIntern(c, query, "update", true, false, NULL);
}

static str
sql_update_oscar(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 8192, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);
	sql_schema *sys = mvc_bind_schema(sql, "sys");
	res_table *output;
	BAT *b;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* if column 6 of sys.queue is named "progress" we need to update */
	pos += snprintf(buf + pos, bufsize - pos,
			"select name from sys._columns where table_id = (select id from sys._tables where name = 'queue' and schema_id = (select id from sys.schemas where name = 'sys')) and number = 6;\n");
	err = SQLstatementIntern(c, buf, "update", true, false, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		BATiter bi = bat_iterator(b);
		if (BATcount(b) > 0 && strcmp(BUNtail(bi, 0), "progress") == 0) {
			if (!*systabfixed &&
				(err = sql_fix_system_tables(c, sql, prev_schema)) != NULL)
				return err;
			*systabfixed = true;

			pos = 0;
			pos += snprintf(buf + pos, bufsize - pos,
					"set schema \"sys\";\n");

			/* the real update of sys.env() has happened
			 * in load_func, here we merely update the
			 * sys.functions table */
			pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set"
					" mod = 'inspect',"
					" func = 'CREATE FUNCTION env() RETURNS TABLE( name varchar(1024), value varchar(2048)) EXTERNAL NAME inspect.\"getEnvironment\";'"
					" where schema_id = (select id from sys.schemas where name = 'sys')"
					" and name = 'env' and type = %d;\n",
					(int) F_UNION);

			/* 26_sysmon */
			sql_table *t;
			t = mvc_bind_table(sql, sys, "queue");
			t->system = 0; /* make it non-system else the drop view will fail */

			pos += snprintf(buf + pos, bufsize - pos,
					"drop view sys.queue;\n"
					"drop function sys.queue;\n"
					"create function sys.queue()\n"
					"returns table(\n"
					"\"tag\" bigint,\n"
					"\"sessionid\" int,\n"
					"\"username\" string,\n"
					"\"started\" timestamp,\n"
					"\"status\" string,\n"
					"\"query\" string,\n"
					"\"finished\" timestamp,\n"
					"\"workers\" int,\n"
					"\"memory\" int)\n"
					" external name sysmon.queue;\n"
					"grant execute on function sys.queue to public;\n"
					"create view sys.queue as select * from sys.queue();\n"
					"grant select on sys.queue to public;\n"
					"drop procedure sys.pause(bigint);\n"
					"drop procedure sys.resume(bigint);\n"
					"drop procedure sys.stop(bigint);\n"
					"create procedure sys.pause(tag bigint)\n"
					"external name sysmon.pause;\n"
					"grant execute on procedure sys.pause(bigint) to public;\n"
					"create procedure sys.resume(tag bigint)\n"
					"external name sysmon.resume;\n"
					"grant execute on procedure sys.resume(bigint) to public;\n"
					"create procedure sys.stop(tag bigint)\n"
					"external name sysmon.stop;\n"
					"grant execute on procedure sys.stop(bigint) to public;\n");

			pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
					" and name = 'queue' and type = %d;\n"
					"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
					" and name in ('pause', 'resume', 'stop') and type = %d;\n",
					(int) F_UNION, (int) F_PROC);
			pos += snprintf(buf + pos, bufsize - pos,
					"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
					" and name = 'queue';\n");

			/* scoping branch changes */
			pos += snprintf(buf + pos, bufsize - pos,
					"drop function \"sys\".\"var\"();\n"
					"create function \"sys\".\"var\"() "
					"returns table("
					"\"schema\" string, "
					"\"name\" string, "
					"\"type\" string, "
					"\"value\" string) "
					"external name \"sql\".\"sql_variables\";\n"
					"grant execute on function \"sys\".\"var\" to public;\n");

			pos += snprintf(buf + pos, bufsize - pos,
					"create procedure sys.hot_snapshot(tarfile string, onserver bool)\n"
					"external name sql.hot_snapshot;\n"
					"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
					" and name in ('hot_snapshot') and type = %d;\n",
					(int) F_PROC);
			/* .snapshot user */
			pos += snprintf(buf + pos, bufsize - pos,
				"create user \".snapshot\"\n"
				" with encrypted password '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'\n"
				" name 'Snapshot User'\n"
				" schema sys;\n"
				"grant execute on procedure sys.hot_snapshot(string) to \".snapshot\";\n"
				"grant execute on procedure sys.hot_snapshot(string, bool) to \".snapshot\";\n"
			);

			/* update system tables so that the content
			 * looks more like what it would be if sys.var
			 * had been defined by the C code in
			 * sql_create_env() */
			pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set system = true,"
					//" func = 'CREATE FUNCTION \"sys\".\"var\"() RETURNS TABLE(\"schema\" string, \"name\" string, \"type\" string, \"value\" string) EXTERNAL NAME \"sql\".\"sql_variables\";',"
					" language = 2, side_effect = false where name = 'var' and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
					"update sys.args set type = 'char' where func_id = (select id from sys.functions where name = 'var' and schema_id = (select id from sys.schemas where name = 'sys') and type = %d) and type = 'clob';\n"
					"update sys.privileges set grantor = 0 where obj_id = (select id from sys.functions where name = 'var' and schema_id = (select id from sys.schemas where name = 'sys') and type = %d);\n",
					(int) F_UNION,
					(int) F_UNION,
					(int) F_UNION);

			/* SQL functions without backend implementations */
			pos += snprintf(buf + pos, bufsize - pos,
					"DROP FUNCTION \"sys\".\"getcontent\"(url);\n"
					"DROP AGGREGATE \"json\".\"output\"(json);\n");

			/* Move sys.degrees,sys.radians,sys.like and sys.ilike to sql_types.c definitions (I did this at the bat_logger) Remove the obsolete entries at privileges table */
			pos += snprintf(buf + pos, bufsize - pos,
					"delete from privileges where obj_id in (select obj_id from privileges left join functions on privileges.obj_id = functions.id where functions.id is null and privileges.obj_id not in ((SELECT tables.id from tables), 0));\n");

			pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
			assert(pos < bufsize);

			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2020(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 4096, pos = 0;
	char *buf = NULL, *err = NULL;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t;
	res_table *output = NULL;
	BAT *b = NULL;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* if view sys.var_values mentions the query cache ('cache') we need
	   to update */
	pos += snprintf(buf + pos, bufsize - pos,
					"select id from sys._tables where name = 'var_values' and query like '%%''cache''%%' and schema_id = (select id from sys.schemas where name = 'sys');\n");
	err = SQLstatementIntern(c, buf, "update", true, false, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) > 0) {
			if (!*systabfixed &&
				(err = sql_fix_system_tables(c, sql, prev_schema)) != NULL)
				return err;
			*systabfixed = true;

			pos = 0;
			pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");

			/* 51_sys_schema_extensions.sql */
			t = mvc_bind_table(sql, s, "var_values");
			t->system = 0;	/* make it non-system else the drop view will fail */
			pos += snprintf(buf + pos, bufsize - pos,
							"DROP VIEW sys.var_values;\n"
							"CREATE VIEW sys.var_values (var_name, value) AS\n"
							"SELECT 'current_role', current_role UNION ALL\n"
							"SELECT 'current_schema', current_schema UNION ALL\n"
							"SELECT 'current_timezone', current_timezone UNION ALL\n"
							"SELECT 'current_user', current_user UNION ALL\n"
							"SELECT 'debug', debug UNION ALL\n"
							"SELECT 'last_id', last_id UNION ALL\n"
							"SELECT 'optimizer', optimizer UNION ALL\n"
							"SELECT 'pi', pi() UNION ALL\n"
							"SELECT 'rowcnt', rowcnt;\n"
							"UPDATE sys._tables SET system = true WHERE name = 'var_values' AND schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys');\n"
							"GRANT SELECT ON sys.var_values TO PUBLIC;\n");
			/* 26_sysmon.sql */
			pos += snprintf(buf + pos, bufsize - pos,
					"create function sys.user_statistics()\n"
					"returns table(\n"
						" username string,\n"
						" querycount bigint,\n"
						" totalticks bigint,\n"
						" started timestamp,\n"
						" finished timestamp,\n"
						" maxticks bigint,\n"
						" maxquery string\n"
					")\n"
					"external name sysmon.user_statistics;\n"
					"update sys.functions set system = true where system <> true and name = 'user_statistics' and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n", (int) F_UNION);

			/* Remove entries on sys.args table without correspondents on sys.functions table */
			pos += snprintf(buf + pos, bufsize - pos,
					"delete from sys.args where id in (select args.id from sys.args left join sys.functions on args.func_id = functions.id where functions.id is null);\n");

			/* 39_analytics.sql */
			pos += snprintf(buf + pos, bufsize - pos,
					"DROP AGGREGATE stddev_samp(INTERVAL SECOND);\n"
					"DROP AGGREGATE stddev_samp(INTERVAL MONTH);\n"
					"DROP WINDOW stddev_samp(INTERVAL SECOND);\n"
					"DROP WINDOW stddev_samp(INTERVAL MONTH);\n"
					"DROP AGGREGATE stddev_pop(INTERVAL SECOND);\n"
					"DROP AGGREGATE stddev_pop(INTERVAL MONTH);\n"
					"DROP WINDOW stddev_pop(INTERVAL SECOND);\n"
					"DROP WINDOW stddev_pop(INTERVAL MONTH);\n"
					"DROP AGGREGATE var_samp(INTERVAL SECOND);\n"
					"DROP AGGREGATE var_samp(INTERVAL MONTH);\n"
					"DROP WINDOW var_samp(INTERVAL SECOND);\n"
					"DROP WINDOW var_samp(INTERVAL MONTH);\n"
					"DROP AGGREGATE var_pop(INTERVAL SECOND);\n"
					"DROP AGGREGATE var_pop(INTERVAL MONTH);\n"
					"DROP WINDOW var_pop(INTERVAL SECOND);\n"
					"DROP WINDOW var_pop(INTERVAL MONTH);\n"
					"DROP AGGREGATE covar_samp(INTERVAL SECOND,INTERVAL SECOND);\n"
					"DROP AGGREGATE covar_samp(INTERVAL MONTH,INTERVAL MONTH);\n"
					"DROP WINDOW covar_samp(INTERVAL SECOND,INTERVAL SECOND);\n"
					"DROP WINDOW covar_samp(INTERVAL MONTH,INTERVAL MONTH);\n"
					"DROP AGGREGATE covar_pop(INTERVAL SECOND,INTERVAL SECOND);\n"
					"DROP AGGREGATE covar_pop(INTERVAL MONTH,INTERVAL MONTH);\n"
					"DROP WINDOW covar_pop(INTERVAL SECOND,INTERVAL SECOND);\n"
					"DROP WINDOW covar_pop(INTERVAL MONTH,INTERVAL MONTH);\n"
					"DROP AGGREGATE corr(INTERVAL SECOND,INTERVAL SECOND);\n"
					"DROP AGGREGATE corr(INTERVAL MONTH,INTERVAL MONTH);\n"
					"DROP WINDOW corr(INTERVAL SECOND,INTERVAL SECOND);\n"
					"DROP WINDOW corr(INTERVAL MONTH,INTERVAL MONTH);\n"
					"create aggregate median(val INTERVAL DAY) returns INTERVAL DAY\n"
					" external name \"aggr\".\"median\";\n"
					"GRANT EXECUTE ON AGGREGATE median(INTERVAL DAY) TO PUBLIC;\n"
					"create aggregate quantile(val INTERVAL DAY, q DOUBLE) returns INTERVAL DAY\n"
					" external name \"aggr\".\"quantile\";\n"
					"GRANT EXECUTE ON AGGREGATE quantile(INTERVAL DAY, DOUBLE) TO PUBLIC;\n"
					"update sys.functions set system = true where system <> true and name in ('median', 'quantile') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n", (int) F_AGGR);

			/* 90_generator.sql */
			pos += snprintf(buf + pos, bufsize - pos,
					"create function sys.generate_series(first timestamp, \"limit\" timestamp, stepsize interval day) returns table (value timestamp)\n"
					" external name generator.series;\n"
					"update sys.functions set system = true where system <> true and name in ('generate_series') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n", (int) F_UNION);

			/* 51_sys_schema_extensions.sql */
			pos += snprintf(buf + pos, bufsize - pos,
					"ALTER TABLE sys.keywords SET READ WRITE;\n"
					"insert into sys.keywords values ('EPOCH');\n");

			pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
			pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
			if (err != MAL_SUCCEED)
				goto bailout;

			pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
					"ALTER TABLE sys.keywords SET READ ONLY;\n");
			pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
			if (err != MAL_SUCCEED)
				goto bailout;
			err = sql_update_storagemodel(c, sql, prev_schema, true); /* because of day interval addition, we have to recreate the storagmodel views */
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (output)
		res_table_destroy(output);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2020_sp1(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 1024, pos = 0;
	char *buf = NULL, *err = NULL;

	if (!sql_bind_func(sql, "sys", "uuid", sql_bind_localtype("int"), NULL, F_FUNC)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';

		if ((buf = GDKmalloc(bufsize)) == NULL)
			throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

		if (!*systabfixed && (err = sql_fix_system_tables(c, sql, prev_schema)) != NULL) {
			GDKfree(buf);
			return err;
		}
		*systabfixed = true;

		pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");
		/* 45_uuid.sql */
		pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.uuid(d int) returns uuid\n"
			" external name uuid.\"new\";\n"
			"GRANT EXECUTE ON FUNCTION sys.uuid(int) TO PUBLIC;\n"
			"update sys.functions set system = true where system <> true and name = 'uuid' and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n", (int) F_FUNC);

		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_default(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 65536, pos = 0;
	char *buf = NULL, *err = NULL;
	res_table *output = NULL;
	BAT *b = NULL;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* if the keyword STREAM is in the list of keywords, upgrade */
	pos += snprintf(buf + pos, bufsize - pos,
					"select keyword from sys.keywords where keyword = 'STREAM';\n");
	assert(pos < bufsize);
	if ((err = SQLstatementIntern(c, buf, "update", true, false, &output)))
		goto bailout;
	if ((b = BATdescriptor(output->cols[0].b))) {
		if (BATcount(b) == 1) {
			if (!*systabfixed && (err = sql_fix_system_tables(c, sql, prev_schema)) != NULL)
				goto bailout;
			*systabfixed = true;

			pos = snprintf(buf, bufsize, "set schema \"sys\";\n");

			/* 20_vacuum.sql */
			pos += snprintf(buf + pos, bufsize - pos,
							"drop procedure sys.shrink(string, string);\n"
							"drop procedure sys.reuse(string, string);\n"
							"drop procedure sys.vacuum(string, string);\n");

			/* 25_debug.sql */
			pos += snprintf(buf + pos, bufsize - pos,
							"drop procedure sys.flush_log();\n");

			/* fix up dependencies for function getproj4 (if it exists) */
			pos += snprintf(buf + pos, bufsize - pos,
							"delete from sys.dependencies d where d.depend_id = (select id from sys.functions where name = 'getproj4' and schema_id = 2000) and id in (select id from sys._columns where name not in ('proj4text', 'srid'));\n");

			/* 41_json.sql */
			pos += snprintf(buf + pos, bufsize - pos,
							"drop function json.isobject(string);\n"
							"drop function json.isarray(string);\n"
							"drop function json.isvalid(json);\n"
							"create function json.isvalid(js json)\n"
							"returns bool begin return true; end;\n"
							"grant execute on function json.isvalid(json) to public;\n"
							"update sys.functions set system = true"
							" where schema_id = (select id from sys.schemas where name = 'json')"
							" and name = 'isvalid';\n");

			/* 51_sys_schema_extensions, remove stream table entries and update window function description */
			pos += snprintf(buf + pos, bufsize - pos,
					"ALTER TABLE sys.keywords SET READ WRITE;\n"
					"DELETE FROM sys.keywords where keyword = 'STREAM';\n"
					"INSERT INTO sys.keywords VALUES ('BIG'), ('LITTLE'), ('NATIVE'), ('ENDIAN'), ('CURRENT_SCHEMA'), ('CURRENT_TIMEZONE'), ('IMPRINTS'), ('ORDERED'), ('PATH'), ('ROLE'), ('ROW'), ('VALUE');\n"
					"ALTER TABLE sys.table_types SET READ WRITE;\n"
					"DELETE FROM sys.table_types where table_type_id = 4;\n"
					"ALTER TABLE sys.function_types SET READ WRITE;\n"
					"UPDATE sys.function_types SET function_type_keyword = 'WINDOW' WHERE function_type_id = 6;\n");

			/* 52_describe.sql */
			pos += snprintf(buf + pos, bufsize - pos,
					"CREATE FUNCTION sys.describe_type(ctype string, digits integer, tscale integer)\n"
					"  RETURNS string\n"
					"BEGIN\n"
					"  RETURN\n"
					"    CASE ctype\n"
					"      WHEN 'bigint' THEN 'BIGINT'\n"
					"      WHEN 'blob' THEN\n"
					"        CASE digits\n"
					"          WHEN 0 THEN 'BINARY LARGE OBJECT'\n"
					"          ELSE 'BINARY LARGE OBJECT(' || digits || ')'\n"
					"        END\n"
					"      WHEN 'boolean' THEN 'BOOLEAN'\n"
					"      WHEN 'char' THEN\n"
					"        CASE digits\n"
					"          WHEN 1 THEN 'CHARACTER'\n"
					"          ELSE 'CHARACTER(' || digits || ')'\n"
					"        END\n"
					"      WHEN 'clob' THEN\n"
					"    CASE digits\n"
					"      WHEN 0 THEN 'CHARACTER LARGE OBJECT'\n"
					"      ELSE 'CHARACTER LARGE OBJECT(' || digits || ')'\n"
					"    END\n"
					"      WHEN 'date' THEN 'DATE'\n"
					"      WHEN 'day_interval' THEN 'INTERVAL DAY'\n"
					"      WHEN ctype = 'decimal' THEN\n"
					"      CASE\n"
					"      WHEN (digits = 1 AND tscale = 0) OR digits = 0 THEN 'DECIMAL'\n"
					"      WHEN tscale = 0 THEN 'DECIMAL(' || digits || ')'\n"
					"      WHEN digits = 39 THEN 'DECIMAL(' || 38 || ',' || tscale || ')'\n"
					"      WHEN digits = 19 AND (SELECT COUNT(*) = 0 FROM sys.types WHERE sqlname = 'hugeint' ) THEN 'DECIMAL(' || 18 || ',' || tscale || ')'\n"
					"      ELSE 'DECIMAL(' || digits || ',' || tscale || ')'\n"
					"    END\n"
					"      WHEN 'double' THEN\n"
					"    CASE\n"
					"      WHEN digits = 53 and tscale = 0 THEN 'DOUBLE'\n"
					"      WHEN tscale = 0 THEN 'FLOAT(' || digits || ')'\n"
					"      ELSE 'FLOAT(' || digits || ',' || tscale || ')'\n"
					"    END\n"
					"      WHEN 'geometry' THEN\n"
					"    CASE digits\n"
					"      WHEN 4 THEN 'GEOMETRY(POINT' ||\n"
					"            CASE tscale\n"
					"              WHEN 0 THEN ''\n"
					"              ELSE ',' || tscale\n"
					"            END || ')'\n"
					"      WHEN 8 THEN 'GEOMETRY(LINESTRING' ||\n"
					"            CASE tscale\n"
					"              WHEN 0 THEN ''\n"
					"              ELSE ',' || tscale\n"
					"            END || ')'\n"
					"      WHEN 16 THEN 'GEOMETRY(POLYGON' ||\n"
					"            CASE tscale\n"
					"              WHEN 0 THEN ''\n"
					"              ELSE ',' || tscale\n"
					"            END || ')'\n"
					"      WHEN 20 THEN 'GEOMETRY(MULTIPOINT' ||\n"
					"            CASE tscale\n"
					"              WHEN 0 THEN ''\n"
					"              ELSE ',' || tscale\n"
					"            END || ')'\n"
					"      WHEN 24 THEN 'GEOMETRY(MULTILINESTRING' ||\n"
					"            CASE tscale\n"
					"              WHEN 0 THEN ''\n"
					"              ELSE ',' || tscale\n"
					"            END || ')'\n"
					"      WHEN 28 THEN 'GEOMETRY(MULTIPOLYGON' ||\n"
					"            CASE tscale\n"
					"              WHEN 0 THEN ''\n"
					"              ELSE ',' || tscale\n"
					"            END || ')'\n"
					"      WHEN 32 THEN 'GEOMETRY(GEOMETRYCOLLECTION' ||\n"
					"            CASE tscale\n"
					"              WHEN 0 THEN ''\n"
					"              ELSE ',' || tscale\n"
					"            END || ')'\n"
					"      ELSE 'GEOMETRY'\n"
					"        END\n"
					"      WHEN 'hugeint' THEN 'HUGEINT'\n"
					"      WHEN 'int' THEN 'INTEGER'\n"
					"      WHEN 'month_interval' THEN\n"
					"    CASE digits\n"
					"      WHEN 1 THEN 'INTERVAL YEAR'\n"
					"      WHEN 2 THEN 'INTERVAL YEAR TO MONTH'\n"
					"      WHEN 3 THEN 'INTERVAL MONTH'\n"
					"    END\n"
					"      WHEN 'real' THEN\n"
					"    CASE\n"
					"      WHEN digits = 24 and tscale = 0 THEN 'REAL'\n"
					"      WHEN tscale = 0 THEN 'FLOAT(' || digits || ')'\n"
					"      ELSE 'FLOAT(' || digits || ',' || tscale || ')'\n"
					"    END\n"
					"      WHEN 'sec_interval' THEN\n"
					"    CASE digits\n"
					"      WHEN 4 THEN 'INTERVAL DAY'\n"
					"      WHEN 5 THEN 'INTERVAL DAY TO HOUR'\n"
					"      WHEN 6 THEN 'INTERVAL DAY TO MINUTE'\n"
					"      WHEN 7 THEN 'INTERVAL DAY TO SECOND'\n"
					"      WHEN 8 THEN 'INTERVAL HOUR'\n"
					"      WHEN 9 THEN 'INTERVAL HOUR TO MINUTE'\n"
					"      WHEN 10 THEN 'INTERVAL HOUR TO SECOND'\n"
					"      WHEN 11 THEN 'INTERVAL MINUTE'\n"
					"      WHEN 12 THEN 'INTERVAL MINUTE TO SECOND'\n"
					"      WHEN 13 THEN 'INTERVAL SECOND'\n"
					"    END\n"
					"      WHEN 'smallint' THEN 'SMALLINT'\n"
					"      WHEN 'time' THEN\n"
					"    CASE digits\n"
					"      WHEN 1 THEN 'TIME'\n"
					"      ELSE 'TIME(' || (digits - 1) || ')'\n"
					"    END\n"
					"      WHEN 'timestamp' THEN\n"
					"    CASE digits\n"
					"      WHEN 7 THEN 'TIMESTAMP'\n"
					"      ELSE 'TIMESTAMP(' || (digits - 1) || ')'\n"
					"    END\n"
					"      WHEN 'timestamptz' THEN\n"
					"    CASE digits\n"
					"      WHEN 7 THEN 'TIMESTAMP'\n"
					"      ELSE 'TIMESTAMP(' || (digits - 1) || ')'\n"
					"    END || ' WITH TIME ZONE'\n"
					"      WHEN 'timetz' THEN\n"
					"    CASE digits\n"
					"      WHEN 1 THEN 'TIME'\n"
					"      ELSE 'TIME(' || (digits - 1) || ')'\n"
					"    END || ' WITH TIME ZONE'\n"
					"      WHEN 'tinyint' THEN 'TINYINT'\n"
					"      WHEN 'varchar' THEN 'CHARACTER VARYING(' || digits || ')'\n"
					"      ELSE\n"
					"        CASE\n"
					"          WHEN lower(ctype) = ctype THEN upper(ctype)\n"
					"          ELSE '\"' || ctype || '\"'\n"
					"        END || CASE digits\n"
					"      WHEN 0 THEN ''\n"
					"          ELSE '(' || digits || CASE tscale\n"
					"        WHEN 0 THEN ''\n"
					"            ELSE ',' || tscale\n"
					"          END || ')'\n"
					"    END\n"
					"    END;\n"
					"END;\n"
					"CREATE FUNCTION sys.SQ (s STRING) RETURNS STRING BEGIN RETURN ' ''' || sys.replace(s,'''','''''') || ''' '; END;\n"
					"CREATE FUNCTION sys.DQ (s STRING) RETURNS STRING BEGIN RETURN '\"' || sys.replace(s,'\"','\"\"') || '\"'; END; --TODO: Figure out why this breaks with the space\n"
					"CREATE FUNCTION sys.FQN(s STRING, t STRING) RETURNS STRING BEGIN RETURN sys.DQ(s) || '.' || sys.DQ(t); END;\n"
					"CREATE FUNCTION sys.ALTER_TABLE(s STRING, t STRING) RETURNS STRING BEGIN RETURN 'ALTER TABLE ' || sys.FQN(s, t) || ' '; END;\n"
					"--We need pcre to implement a header guard which means adding the schema of an object explicitely to its identifier.\n"
					"CREATE FUNCTION sys.replace_first(ori STRING, pat STRING, rep STRING, flg STRING) RETURNS STRING EXTERNAL NAME \"pcre\".\"replace_first\";\n"
					"CREATE FUNCTION sys.schema_guard(sch STRING, nme STRING, stmt STRING) RETURNS STRING BEGIN\n"
					"RETURN\n"
					"    SELECT sys.replace_first(stmt, '(\\\\s*\"?' || sch ||  '\"?\\\\s*\\\\.|)\\\\s*\"?' || nme || '\"?\\\\s*', ' ' || sys.FQN(sch, nme) || ' ', 'imsx');\n"
					"END;\n"
					"CREATE VIEW sys.describe_constraints AS\n"
					"    SELECT\n"
					"        s.name sch,\n"
					"        t.name tbl,\n"
					"        kc.name col,\n"
					"        k.name con,\n"
					"        CASE WHEN k.type = 0 THEN 'PRIMARY KEY' WHEN k.type = 1 THEN 'UNIQUE' END tpe\n"
					"    FROM sys.schemas s, sys._tables t, sys.objects kc, sys.keys k\n"
					"    WHERE kc.id = k.id\n"
					"        AND k.table_id = t.id\n"
					"        AND s.id = t.schema_id\n"
					"        AND t.system = FALSE\n"
					"        AND k.type in (0, 1)\n"
					"        AND t.type IN (0, 6);\n"
					"CREATE VIEW sys.describe_indices AS\n"
					"    WITH it (id, idx) AS (VALUES (0, 'INDEX'), (4, 'IMPRINTS INDEX'), (5, 'ORDERED INDEX')) --UNIQUE INDEX wraps to INDEX.\n"
					"    SELECT\n"
					"        i.name ind,\n"
					"        s.name sch,\n"
					"        t.name tbl,\n"
					"        c.name col,\n"
					"        it.idx tpe\n"
					"    FROM\n"
					"        sys.idxs AS i LEFT JOIN sys.keys AS k ON i.name = k.name,\n"
					"        sys.objects AS kc,\n"
					"        sys._columns AS c,\n"
					"        sys.schemas s,\n"
					"        sys._tables AS t,\n"
					"        it\n"
					"    WHERE\n"
					"        i.table_id = t.id\n"
					"        AND i.id = kc.id\n"
					"        AND kc.name = c.name\n"
					"        AND t.id = c.table_id\n"
					"        AND t.schema_id = s.id\n"
					"        AND k.type IS NULL\n"
					"        AND i.type = it.id\n"
					"    ORDER BY i.name, kc.nr;\n"
					"CREATE VIEW sys.describe_column_defaults AS\n"
					"    SELECT\n"
					"        s.name sch,\n"
					"        t.name tbl,\n"
					"        c.name col,\n"
					"        c.\"default\" def\n"
					"    FROM sys.schemas s, sys.tables t, sys.columns c\n"
					"    WHERE\n"
					"        s.id = t.schema_id AND\n"
					"        t.id = c.table_id AND\n"
					"        s.name <> 'tmp' AND\n"
					"        NOT t.system AND\n"
					"        c.\"default\" IS NOT NULL;\n"
					"CREATE VIEW sys.describe_foreign_keys AS\n"
					"        WITH action_type (id, act) AS (VALUES\n"
					"            (0, 'NO ACTION'),\n"
					"            (1, 'CASCADE'),\n"
					"            (2, 'RESTRICT'),\n"
					"            (3, 'SET NULL'),\n"
					"            (4, 'SET DEFAULT'))\n"
					"        SELECT\n"
					"            fs.name fk_s,\n"
					"            fkt.name fk_t,\n"
					"            fkkc.name fk_c,\n"
					"            fkkc.nr o,\n"
					"            fkk.name fk,\n"
					"            ps.name pk_s,\n"
					"            pkt.name pk_t,\n"
					"            pkkc.name pk_c,\n"
					"            ou.act on_update,\n"
					"            od.act on_delete\n"
					"        FROM sys._tables fkt,\n"
					"            sys.objects fkkc,\n"
					"            sys.keys fkk,\n"
					"            sys._tables pkt,\n"
					"            sys.objects pkkc,\n"
					"            sys.keys pkk,\n"
					"            sys.schemas ps,\n"
					"            sys.schemas fs,\n"
					"            action_type ou,\n"
					"            action_type od\n"
					"        WHERE fkt.id = fkk.table_id\n"
					"        AND pkt.id = pkk.table_id\n"
					"        AND fkk.id = fkkc.id\n"
					"        AND pkk.id = pkkc.id\n"
					"        AND fkk.rkey = pkk.id\n"
					"        AND fkkc.nr = pkkc.nr\n"
					"        AND pkt.schema_id = ps.id\n"
					"        AND fkt.schema_id = fs.id\n"
					"        AND (fkk.\"action\" & 255)         = od.id\n"
					"        AND ((fkk.\"action\" >> 8) & 255)  = ou.id\n"
					"        ORDER BY fkk.name, fkkc.nr;\n"
					"--TODO: CRASHES when this function gets inlined into describe_tables\n"
					"CREATE FUNCTION sys.get_merge_table_partition_expressions(tid INT) RETURNS STRING\n"
					"BEGIN\n"
					"    RETURN\n"
					"        SELECT\n"
					"            CASE WHEN tp.table_id IS NOT NULL THEN    --updatable merge table\n"
					"                ' PARTITION BY ' ||\n"
					"                CASE\n"
					"                    WHEN bit_and(tp.type, 2) = 2\n"
					"                    THEN 'VALUES '\n"
					"                    ELSE 'RANGE '\n"
					"                END ||\n"
					"                CASE\n"
					"                    WHEN bit_and(tp.type, 4) = 4 --column expression\n"
					"                    THEN 'ON ' || '(' || (SELECT sys.DQ(c.name) || ')' FROM sys.columns c WHERE c.id = tp.column_id)\n"
					"                    ELSE 'USING ' || '(' || tp.expression || ')' --generic expression\n"
					"                END\n"
					"            ELSE                                    --read only partition merge table.\n"
					"                ''\n"
					"            END\n"
					"        FROM (VALUES (tid)) t(id) LEFT JOIN sys.table_partitions tp ON t.id = tp.table_id;\n"
					"END;\n"
					"--TODO: gives mergejoin errors when inlined\n"
					"CREATE FUNCTION sys.get_remote_table_expressions(s STRING, t STRING) RETURNS STRING BEGIN\n"
					"    RETURN SELECT ' ON ' || sys.SQ(uri) || ' WITH USER ' || sys.SQ(username) || ' ENCRYPTED PASSWORD ' || sys.SQ(\"hash\") FROM sys.remote_table_credentials(s ||'.' || t);\n"
					"END;\n"
					"CREATE VIEW sys.describe_tables AS\n"
					"    SELECT\n"
					"        t.id o,\n"
					"        s.name sch,\n"
					"        t.name tab,\n"
					"        ts.table_type_name typ,\n"
					"        (SELECT\n"
					"            ' (' ||\n"
					"            GROUP_CONCAT(\n"
					"                sys.DQ(c.name) || ' ' ||\n"
					"                sys.describe_type(c.type, c.type_digits, c.type_scale) ||\n"
					"                ifthenelse(c.\"null\" = 'false', ' NOT NULL', '')\n"
					"            , ', ') || ')'\n"
					"        FROM sys._columns c\n"
					"        WHERE c.table_id = t.id) col,\n"
					"        CASE\n"
					"            WHEN ts.table_type_name = 'REMOTE TABLE' THEN\n"
					"                sys.get_remote_table_expressions(s.name, t.name)\n"
					"            WHEN ts.table_type_name = 'MERGE TABLE' THEN\n"
					"                sys.get_merge_table_partition_expressions(t.id)\n"
					"            WHEN ts.table_type_name = 'VIEW' THEN\n"
					"                sys.schema_guard(s.name, t.name, t.query)\n"
					"            ELSE\n"
					"                ''\n"
					"        END opt\n"
					"    FROM sys.schemas s, sys.table_types ts, sys.tables t\n"
					"    WHERE ts.table_type_name IN ('TABLE', 'VIEW', 'MERGE TABLE', 'REMOTE TABLE', 'REPLICA TABLE')\n"
					"        AND t.system = FALSE\n"
					"        AND s.id = t.schema_id\n"
					"        AND ts.table_type_id = t.type\n"
					"        AND s.name <> 'tmp';\n"
					"CREATE VIEW sys.describe_triggers AS\n"
					"        SELECT\n"
					"            s.name sch,\n"
					"            t.name tab,\n"
					"            tr.name tri,\n"
					"            tr.statement def\n"
					"        FROM sys.schemas s, sys.tables t, sys.triggers tr\n"
					"        WHERE s.id = t.schema_id AND t.id = tr.table_id AND NOT t.system;\n"
					"CREATE VIEW sys.describe_comments AS\n"
					"        SELECT\n"
					"            o.id id,\n"
					"            o.tpe tpe,\n"
					"            o.nme fqn,\n"
					"            c.remark rem\n"
					"        FROM (\n"
					"            SELECT id, 'SCHEMA', sys.DQ(name) FROM sys.schemas\n"
					"            UNION ALL\n"
					"            SELECT t.id, CASE WHEN ts.table_type_name = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END, sys.FQN(s.name, t.name)\n"
					"            FROM sys.schemas s JOIN sys.tables t ON s.id = t.schema_id JOIN sys.table_types ts ON t.type = ts.table_type_id\n"
					"            WHERE NOT s.name <> 'tmp'\n"
					"            UNION ALL\n"
					"            SELECT c.id, 'COLUMN', sys.FQN(s.name, t.name) || '.' || sys.DQ(c.name) FROM sys.columns c, sys.tables t, sys.schemas s WHERE c.table_id = t.id AND t.schema_id = s.id\n"
					"            UNION ALL\n"
					"            SELECT idx.id, 'INDEX', sys.FQN(s.name, idx.name) FROM sys.idxs idx, sys._tables t, sys.schemas s WHERE idx.table_id = t.id AND t.schema_id = s.id\n"
					"            UNION ALL\n"
					"            SELECT seq.id, 'SEQUENCE', sys.FQN(s.name, seq.name) FROM sys.sequences seq, sys.schemas s WHERE seq.schema_id = s.id\n"
					"            UNION ALL\n"
					"            SELECT f.id, ft.function_type_keyword, sys.FQN(s.name, f.name) FROM sys.functions f, sys.function_types ft, sys.schemas s WHERE f.type = ft.function_type_id AND f.schema_id = s.id\n"
					"            ) AS o(id, tpe, nme)\n"
					"            JOIN sys.comments c ON c.id = o.id;\n"
					"CREATE VIEW sys.fully_qualified_functions AS\n"
					"    WITH fqn(id, tpe, sig, num) AS\n"
					"    (\n"
					"        SELECT\n"
					"            f.id,\n"
					"            ft.function_type_keyword,\n"
					"            CASE WHEN a.type IS NULL THEN\n"
					"                s.name || '.' || f.name || '()'\n"
					"            ELSE\n"
					"                s.name || '.' || f.name || '(' || group_concat(sys.describe_type(a.type, a.type_digits, a.type_scale), ',') OVER (PARTITION BY f.id ORDER BY a.number)  || ')'\n"
					"            END,\n"
					"            a.number\n"
					"        FROM sys.schemas s, sys.function_types ft, sys.functions f LEFT JOIN sys.args a ON f.id = a.func_id\n"
					"        WHERE s.id= f.schema_id AND f.type = ft.function_type_id\n"
					"    )\n"
					"    SELECT\n"
					"        fqn1.id id,\n"
					"        fqn1.tpe tpe,\n"
					"        fqn1.sig nme\n"
					"    FROM\n"
					"        fqn fqn1 JOIN (SELECT id, max(num) FROM fqn GROUP BY id)  fqn2(id, num)\n"
					"        ON fqn1.id = fqn2.id AND (fqn1.num = fqn2.num OR fqn1.num IS NULL AND fqn2.num is NULL);\n"
					"CREATE VIEW sys.describe_privileges AS\n"
					"    SELECT\n"
					"        CASE\n"
					"            WHEN o.tpe IS NULL AND pc.privilege_code_name = 'SELECT' THEN --GLOBAL privileges: SELECT maps to COPY FROM\n"
					"                'COPY FROM'\n"
					"            WHEN o.tpe IS NULL AND pc.privilege_code_name = 'UPDATE' THEN --GLOBAL privileges: UPDATE maps to COPY INTO\n"
					"                'COPY INTO'\n"
					"            ELSE\n"
					"                o.nme\n"
					"        END o_nme,\n"
					"        CASE\n"
					"            WHEN o.tpe IS NOT NULL THEN\n"
					"                o.tpe\n"
					"            ELSE\n"
					"                'GLOBAL'\n"
					"        END o_tpe,\n"
					"        pc.privilege_code_name p_nme,\n"
					"        a.name a_nme,\n"
					"        g.name g_nme,\n"
					"        p.grantable grantable\n"
					"    FROM\n"
					"        sys.privileges p LEFT JOIN\n"
					"        (\n"
					"        SELECT t.id, s.name || '.' || t.name , 'TABLE'\n"
					"            from sys.schemas s, sys.tables t where s.id = t.schema_id\n"
					"        UNION ALL\n"
					"            SELECT c.id, s.name || '.' || t.name || '.' || c.name, 'COLUMN'\n"
					"            FROM sys.schemas s, sys.tables t, sys.columns c where s.id = t.schema_id AND t.id = c.table_id\n"
					"        UNION ALL\n"
					"            SELECT f.id, f.nme, f.tpe\n"
					"            FROM sys.fully_qualified_functions f\n"
					"        ) o(id, nme, tpe) ON o.id = p.obj_id,\n"
					"        sys.privilege_codes pc,\n"
					"        auths a, auths g\n"
					"    WHERE\n"
					"        p.privileges = pc.privilege_code_id AND\n"
					"        p.auth_id = a.id AND\n"
					"        p.grantor = g.id;\n"
					"CREATE FUNCTION sys.describe_table(schemaName string, tableName string)\n"
					"  RETURNS TABLE(name string, query string, type string, id integer, remark string)\n"
					"BEGIN\n"
					"    RETURN SELECT t.name, t.query, tt.table_type_name, t.id, c.remark\n"
					"        FROM sys.schemas s, sys.table_types tt, sys._tables t\n"
					"        LEFT OUTER JOIN sys.comments c ON t.id = c.id\n"
					"            WHERE s.name = schemaName\n"
					"            AND t.schema_id = s.id\n"
					"            AND t.name = tableName\n"
					"            AND t.type = tt.table_type_id;\n"
					"END;\n"
					"CREATE VIEW sys.describe_user_defined_types AS\n"
					"    SELECT\n"
					"        s.name sch,\n"
					"        t.sqlname sql_tpe,\n"
					"        t.systemname ext_tpe\n"
					"    FROM sys.types t JOIN sys.schemas s ON t.schema_id = s.id\n"
					"    WHERE\n"
					"        t.eclass = 18 AND\n"
					"        (\n"
					"            (s.name = 'sys' AND t.sqlname not in ('geometrya', 'mbr', 'url', 'inet', 'json', 'uuid', 'xml')) OR\n"
					"            (s.name <> 'sys')\n"
					"        );\n"
					"CREATE VIEW sys.describe_partition_tables AS\n"
					"    SELECT \n"
					"        m_sch,\n"
					"        m_tbl,\n"
					"        p_sch,\n"
					"        p_tbl,\n"
					"        CASE\n"
					"            WHEN p_raw_type IS NULL THEN 'READ ONLY'\n"
					"            WHEN (p_raw_type = 'VALUES' AND pvalues IS NULL) OR (p_raw_type = 'RANGE' AND minimum IS NULL AND maximum IS NULL AND with_nulls) THEN 'FOR NULLS'\n"
					"            ELSE p_raw_type\n"
					"        END AS tpe,\n"
					"        pvalues,\n"
					"        minimum,\n"
					"        maximum,\n"
					"        with_nulls\n"
					"    FROM \n"
					"    (WITH\n"
					"        tp(\"type\", table_id) AS\n"
					"        (SELECT CASE WHEN (table_partitions.\"type\" & 2) = 2 THEN 'VALUES' ELSE 'RANGE' END, table_partitions.table_id FROM sys.table_partitions),\n"
					"        subq(m_tid, p_mid, \"type\", m_sch, m_tbl, p_sch, p_tbl) AS\n"
					"        (SELECT m_t.id, p_m.id, m_t.\"type\", m_s.name, m_t.name, p_s.name, p_m.name\n"
					"        FROM sys.schemas m_s, sys._tables m_t, sys.dependencies d, sys.schemas p_s, sys._tables p_m\n"
					"        WHERE m_t.\"type\" IN (3, 6)\n"
					"            AND m_t.schema_id = m_s.id\n"
					"            AND m_s.name <> 'tmp'\n"
					"            AND m_t.system = FALSE\n"
					"            AND m_t.id = d.depend_id\n"
					"            AND d.id = p_m.id\n"
					"            AND p_m.schema_id = p_s.id\n"
					"        ORDER BY m_t.id, p_m.id)\n"
					"    SELECT\n"
					"        subq.m_sch,\n"
					"        subq.m_tbl,\n"
					"        subq.p_sch,\n"
					"        subq.p_tbl,\n"
					"        tp.\"type\" AS p_raw_type,\n"
					"        CASE WHEN tp.\"type\" = 'VALUES'\n"
					"            THEN (SELECT GROUP_CONCAT(vp.value, ',')FROM sys.value_partitions vp WHERE vp.table_id = subq.p_mid)\n"
					"            ELSE NULL\n"
					"        END AS pvalues,\n"
					"        CASE WHEN tp.\"type\" = 'RANGE'\n"
					"            THEN (SELECT minimum FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
					"            ELSE NULL\n"
					"        END AS minimum,\n"
					"        CASE WHEN tp.\"type\" = 'RANGE'\n"
					"            THEN (SELECT maximum FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
					"            ELSE NULL\n"
					"        END AS maximum,\n"
					"        CASE WHEN tp.\"type\" = 'VALUES'\n"
					"            THEN EXISTS(SELECT vp.value FROM sys.value_partitions vp WHERE vp.table_id = subq.p_mid AND vp.value IS NULL)\n"
					"            ELSE (SELECT rp.with_nulls FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
					"        END AS with_nulls\n"
					"    FROM \n"
					"        subq LEFT OUTER JOIN tp\n"
					"        ON subq.m_tid = tp.table_id) AS tmp_pi;\n"
					"CREATE VIEW sys.describe_sequences AS\n"
					"    SELECT\n"
					"        s.name as sch,\n"
					"        seq.name as seq,\n"
					"        seq.\"start\" s,\n"
					"        get_value_for(s.name, seq.name) AS rs,\n"
					"        seq.\"minvalue\" mi,\n"
					"        seq.\"maxvalue\" ma,\n"
					"        seq.\"increment\" inc,\n"
					"        seq.\"cacheinc\" cache,\n"
					"        seq.\"cycle\" cycle\n"
					"    FROM sys.sequences seq, sys.schemas s\n"
					"    WHERE s.id = seq.schema_id\n"
					"    AND s.name <> 'tmp'\n"
					"    ORDER BY s.name, seq.name;\n"
					"CREATE VIEW sys.describe_functions AS\n"
					"    SELECT\n"
					"        f.id o,\n"
					"        s.name sch,\n"
					"        f.name fun,\n"
					"        f.func def\n"
					"    FROM sys.functions f JOIN sys.schemas s ON f.schema_id = s.id WHERE s.name <> 'tmp' AND NOT f.system;\n"
					"CREATE FUNCTION sys.describe_columns(schemaName string, tableName string)\n"
					"    RETURNS TABLE(name string, type string, digits integer, scale integer, Nulls boolean, cDefault string, number integer, sqltype string, remark string)\n"
					"BEGIN\n"
					"    RETURN SELECT c.name, c.\"type\", c.type_digits, c.type_scale, c.\"null\", c.\"default\", c.number, sys.describe_type(c.\"type\", c.type_digits, c.type_scale), com.remark\n"
					"        FROM sys._tables t, sys.schemas s, sys._columns c\n"
					"        LEFT OUTER JOIN sys.comments com ON c.id = com.id\n"
					"            WHERE c.table_id = t.id\n"
					"            AND t.name = tableName\n"
					"            AND t.schema_id = s.id\n"
					"            AND s.name = schemaName\n"
					"        ORDER BY c.number;\n"
					"END;\n"
					"CREATE FUNCTION sys.describe_function(schemaName string, functionName string)\n"
					"    RETURNS TABLE(id integer, name string, type string, language string, remark string)\n"
					"BEGIN\n"
					"    RETURN SELECT f.id, f.name, ft.function_type_keyword, fl.language_keyword, c.remark\n"
					"        FROM sys.functions f\n"
					"        JOIN sys.schemas s ON f.schema_id = s.id\n"
					"        JOIN sys.function_types ft ON f.type = ft.function_type_id\n"
					"        LEFT OUTER JOIN sys.function_languages fl ON f.language = fl.language_id\n"
					"        LEFT OUTER JOIN sys.comments c ON f.id = c.id\n"
					"        WHERE f.name=functionName AND s.name = schemaName;\n"
					"END;\n");

			/* 75_storagemodel.sql not changed but dependencies changed
			 * since sys.objects has a new column */
			pos += snprintf(buf + pos, bufsize - pos,
					"drop procedure sys.storagemodelinit();\n"
					"create procedure sys.storagemodelinit()\n"
					"begin\n"
					"    delete from sys.storagemodelinput;\n"
					"    insert into sys.storagemodelinput\n"
					"    select \"schema\", \"table\", \"column\", \"type\", typewidth, \"count\",\n"
					"        -- assume all variable size types contain distinct values\n"
					"        case when (\"unique\" or \"type\" IN ('varchar', 'char', 'clob', 'json', 'url', 'blob', 'geometry', 'geometrya'))\n"
					"            then \"count\" else 0 end,\n"
					"        case when \"count\" > 0 and heapsize >= 8192 and \"type\" in ('varchar', 'char', 'clob', 'json', 'url')\n"
					"            -- string heaps have a header of 8192\n"
					"            then cast((heapsize - 8192) / \"count\" as bigint)\n"
					"        when \"count\" > 0 and heapsize >= 32 and \"type\" in ('blob', 'geometry', 'geometrya')\n"
					"            -- binary data heaps have a header of 32\n"
					"            then cast((heapsize - 32) / \"count\" as bigint)\n"
					"        else typewidth end,\n"
					"        FALSE, case sorted when true then true else false end, \"unique\", TRUE\n"
					"      from sys.\"storage\";  -- view sys.\"storage\" excludes system tables (as those are not useful to be modeled for storagesize by application users)\n"
					"    update sys.storagemodelinput\n"
					"       set reference = TRUE\n"
					"     where (\"schema\", \"table\", \"column\") in (\n"
					"        SELECT fkschema.\"name\", fktable.\"name\", fkkeycol.\"name\"\n"
					"          FROM    sys.\"keys\" AS fkkey,\n"
					"            sys.\"objects\" AS fkkeycol,\n"
					"            sys.\"tables\" AS fktable,\n"
					"            sys.\"schemas\" AS fkschema\n"
					"        WHERE fktable.\"id\" = fkkey.\"table_id\"\n"
					"          AND fkkey.\"id\" = fkkeycol.\"id\"\n"
					"          AND fkschema.\"id\" = fktable.\"schema_id\"\n"
					"          AND fkkey.\"rkey\" > -1 );\n"
					"    update sys.storagemodelinput\n"
					"       set isacolumn = FALSE\n"
					"     where (\"schema\", \"table\", \"column\") NOT in (\n"
					"        SELECT sch.\"name\", tbl.\"name\", col.\"name\"\n"
					"          FROM sys.\"schemas\" AS sch,\n"
					"            sys.\"tables\" AS tbl,\n"
					"            sys.\"columns\" AS col\n"
					"        WHERE sch.\"id\" = tbl.\"schema_id\"\n"
					"          AND tbl.\"id\" = col.\"table_id\");\n"
					"end;\n"
					"update sys.functions set system = true where name = 'storagemodelinit' and schema_id = 2000;\n");

			/* 76_dump.sql */
			pos += snprintf(buf + pos, bufsize - pos,
					"CREATE VIEW sys.dump_create_roles AS\n"
					"    SELECT\n"
					"        'CREATE ROLE ' || sys.dq(name) || ';' stmt FROM sys.auths\n"
					"    WHERE name NOT IN (SELECT name FROM sys.db_user_info)\n"
					"    AND grantor <> 0;\n"
					"CREATE VIEW sys.dump_create_users AS\n"
					"    SELECT\n"
					"        'CREATE USER ' ||  sys.dq(ui.name) ||  ' WITH ENCRYPTED PASSWORD ' ||\n"
					"        sys.sq(sys.password_hash(ui.name)) ||\n"
					"    ' NAME ' || sys.sq(ui.fullname) ||  ' SCHEMA sys;' stmt\n"
					"    FROM sys.db_user_info ui, sys.schemas s\n"
					"    WHERE ui.default_schema = s.id\n"
					"        AND ui.name <> 'monetdb'\n"
					"        AND ui.name <> '.snapshot';\n"
					"CREATE VIEW sys.dump_create_schemas AS\n"
					"    SELECT\n"
					"        'CREATE SCHEMA ' ||  sys.dq(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || a.name, ' ') || ';' stmt\n"
					"    FROM sys.schemas s, sys.auths a\n"
					"    WHERE s.authorization = a.id AND s.system = FALSE;\n"
					"CREATE VIEW sys.dump_add_schemas_to_users AS\n"
					"    SELECT\n"
					"        'ALTER USER ' || sys.dq(ui.name) || ' SET SCHEMA ' || sys.dq(s.name) || ';' stmt\n"
					"    FROM sys.db_user_info ui, sys.schemas s\n"
					"    WHERE ui.default_schema = s.id\n"
					"        AND ui.name <> 'monetdb'\n"
					"        AND ui.name <> '.snapshot'\n"
					"        AND s.name <> 'sys';\n"
					"CREATE VIEW sys.dump_grant_user_privileges AS\n"
					"    SELECT\n"
					"        'GRANT ' || sys.dq(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', sys.dq(a1.name)) || ';' stmt\n"
					"    FROM sys.auths a1, sys.auths a2, sys.user_role ur\n"
					"    WHERE a1.id = ur.login_id AND a2.id = ur.role_id;\n"
					"CREATE VIEW sys.dump_table_constraint_type AS\n"
					"    SELECT\n"
					"        'ALTER TABLE ' || sys.DQ(sch) || '.' || sys.DQ(tbl) ||\n"
					"        ' ADD CONSTRAINT ' || sys.DQ(con) || ' '||\n"
					"        tpe || ' (' || GROUP_CONCAT(sys.DQ(col), ', ') || ');' stmt\n"
					"    FROM sys.describe_constraints GROUP BY sch, tbl, con, tpe;\n"
					"CREATE VIEW sys.dump_indices AS\n"
					"    SELECT\n"
					"        'CREATE ' || tpe || ' ' ||\n"
					"        sys.DQ(ind) || ' ON ' || sys.DQ(sch) || '.' || sys.DQ(tbl) ||\n"
					"        '(' || GROUP_CONCAT(col) || ');' stmt\n"
					"    FROM sys.describe_indices GROUP BY ind, tpe, sch, tbl;\n"
					"CREATE VIEW sys.dump_column_defaults AS\n"
					"    SELECT 'ALTER TABLE ' || sys.FQN(sch, tbl) || ' ALTER COLUMN ' || sys.DQ(col) || ' SET DEFAULT ' || def || ';' stmt\n"
					"    FROM sys.describe_column_defaults;\n"
					"CREATE VIEW sys.dump_foreign_keys AS\n"
					"    SELECT\n"
					"        'ALTER TABLE ' || sys.DQ(fk_s) || '.'|| sys.DQ(fk_t) || ' ADD CONSTRAINT ' || sys.DQ(fk) || ' ' ||\n"
					"        'FOREIGN KEY(' || GROUP_CONCAT(sys.DQ(fk_c), ',') ||') ' ||\n"
					"        'REFERENCES ' || sys.DQ(pk_s) || '.' || sys.DQ(pk_t) || '(' || GROUP_CONCAT(sys.DQ(pk_c), ',') || ') ' ||\n"
					"        'ON DELETE ' || on_delete || ' ON UPDATE ' || on_update ||\n"
					"        ';' stmt\n"
					"    FROM sys.describe_foreign_keys GROUP BY fk_s, fk_t, pk_s, pk_t, fk, on_delete, on_update;\n"
					"CREATE VIEW sys.dump_partition_tables AS\n"
					"    SELECT\n"
					"        sys.ALTER_TABLE(m_sch, m_tbl) || ' ADD TABLE ' || sys.FQN(p_sch, p_tbl) ||\n"
					"        CASE \n"
					"            WHEN tpe = 'VALUES' THEN ' AS PARTITION IN (' || pvalues || ')'\n"
					"            WHEN tpe = 'RANGE' THEN ' AS PARTITION FROM ' || ifthenelse(minimum IS NOT NULL, sys.SQ(minimum), 'RANGE MINVALUE') || ' TO ' || ifthenelse(maximum IS NOT NULL, sys.SQ(maximum), 'RANGE MAXVALUE')\n"
					"            WHEN tpe = 'FOR NULLS' THEN ' AS PARTITION FOR NULL VALUES'\n"
					"            ELSE '' --'READ ONLY'\n"
					"        END ||\n"
					"        CASE WHEN tpe in ('VALUES', 'RANGE') AND with_nulls THEN ' WITH NULL VALUES' ELSE '' END ||\n"
					"        ';' stmt\n"
					"    FROM sys.describe_partition_tables;\n"
					"CREATE VIEW sys.dump_sequences AS\n"
					"    SELECT\n"
					"        'CREATE SEQUENCE ' || sys.FQN(sch, seq) || ' AS BIGINT ' ||\n"
					"        CASE WHEN \"s\" <> 0 THEN 'START WITH ' || \"rs\" ELSE '' END ||\n"
					"        CASE WHEN \"inc\" <> 1 THEN ' INCREMENT BY ' || \"inc\" ELSE '' END ||\n"
					"        CASE WHEN \"mi\" <> 0 THEN ' MINVALUE ' || \"mi\" ELSE '' END ||\n"
					"        CASE WHEN \"ma\" <> 0 THEN ' MAXVALUE ' || \"ma\" ELSE '' END ||\n"
					"        CASE WHEN \"cache\" <> 1 THEN ' CACHE ' || \"cache\" ELSE '' END ||\n"
					"        CASE WHEN \"cycle\" THEN ' CYCLE' ELSE '' END || ';' stmt\n"
					"    FROM sys.describe_sequences;\n"
					"CREATE VIEW sys.dump_start_sequences AS\n"
					"    SELECT\n"
					"        'UPDATE sys.sequences seq SET start = ' || s  ||\n"
					"        ' WHERE name = ' || sys.SQ(seq) ||\n"
					"        ' AND schema_id = (SELECT s.id FROM sys.schemas s WHERE s.name = ' || sys.SQ(sch) || ');' stmt\n"
					"    FROM sys.describe_sequences;\n"
					"CREATE VIEW sys.dump_functions AS\n"
					"    SELECT f.o o, sys.schema_guard(f.sch, f.fun, f.def) stmt FROM sys.describe_functions f;\n"
					"CREATE VIEW sys.dump_tables AS\n"
					"    SELECT\n"
					"        t.o o,\n"
					"        CASE\n"
					"            WHEN t.typ <> 'VIEW' THEN\n"
					"                'CREATE ' || t.typ || ' ' || sys.FQN(t.sch, t.tab) || t.col || t.opt || ';'\n"
					"            ELSE\n"
					"                t.opt\n"
					"        END stmt\n"
					"    FROM sys.describe_tables t;\n"
					"CREATE VIEW sys.dump_triggers AS\n"
					"    SELECT sys.schema_guard(sch, tab, def) stmt FROM sys.describe_triggers;\n"
					"CREATE VIEW sys.dump_comments AS\n"
					"    SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || sys.SQ(c.rem) || ';' stmt FROM sys.describe_comments c;\n"
					"CREATE VIEW sys.dump_user_defined_types AS\n"
					"        SELECT 'CREATE TYPE ' || sys.FQN(sch, sql_tpe) || ' EXTERNAL NAME ' || sys.DQ(ext_tpe) || ';' stmt FROM sys.describe_user_defined_types;\n"
					"CREATE VIEW sys.dump_privileges AS\n"
					"    SELECT\n"
					"        'INSERT INTO sys.privileges VALUES (' ||\n"
					"            CASE\n"
					"                WHEN dp.o_tpe = 'GLOBAL' THEN\n"
					"                    '0,'\n"
					"                WHEN dp.o_tpe = 'TABLE' THEN\n"
					"                    '(SELECT t.id FROM sys.schemas s, sys.tables t WHERE s.id = t.schema_id' ||\n"
					"                        ' AND s.name || ''.'' || t.name =' || sys.SQ(dp.o_nme) || '),'\n"
					"                WHEN dp.o_tpe = 'COLUMN' THEN\n"
					"                    '(SELECT c.id FROM sys.schemas s, sys.tables t, sys.columns c WHERE s.id = t.schema_id AND t.id = c.table_id' ||\n"
					"                        ' AND s.name || ''.'' || t.name || ''.'' || c.name =' || sys.SQ(dp.o_nme) || '),'\n"
					"                ELSE -- FUNCTION-LIKE\n"
					"                    '(SELECT fqn.id FROM sys.fully_qualified_functions fqn WHERE' ||\n"
					"                        ' fqn.nme = ' || sys.SQ(dp.o_nme) || ' AND fqn.tpe = ' || sys.SQ(dp.o_tpe) || '),'\n"
					"            END ||\n"
					"            '(SELECT id FROM sys.auths a WHERE a.name = ' || sys.SQ(dp.a_nme) || '),' ||\n"
					"            '(SELECT pc.privilege_code_id FROM sys.privilege_codes pc WHERE pc.privilege_code_name = ' || sys.SQ(p_nme) || '),'\n"
					"            '(SELECT id FROM sys.auths g WHERE g.name = ' || sys.SQ(dp.g_nme) || '),' ||\n"
					"            dp.grantable ||\n"
					"        ');' stmt\n"
					"    FROM sys.describe_privileges dp;\n"
					"CREATE PROCEDURE sys.EVAL(stmt STRING) EXTERNAL NAME sql.eval;\n"
					"CREATE FUNCTION sys.esc(s STRING) RETURNS STRING BEGIN RETURN '\"' || sys.replace(sys.replace(sys.replace(s,E'\\\\', E'\\\\\\\\'), E'\\n', E'\\\\n'), '\"', E'\\\\\"') || '\"'; END;\n"
					"CREATE FUNCTION sys.prepare_esc(s STRING, t STRING) RETURNS STRING\n"
					"BEGIN\n"
					"    RETURN\n"
					"        CASE\n"
					"            WHEN (t = 'varchar' OR t ='char' OR t = 'clob' OR t = 'json' OR t = 'geometry' OR t = 'url') THEN\n"
					"                'CASE WHEN ' || sys.DQ(s) || ' IS NULL THEN ''null'' ELSE ' || 'sys.esc(' || sys.DQ(s) || ')' || ' END'\n"
					"            ELSE\n"
					"                'CASE WHEN ' || sys.DQ(s) || ' IS NULL THEN ''null'' ELSE CAST(' || sys.DQ(s) || ' AS STRING) END'\n"
					"        END;\n"
					"END;\n"
					"CREATE TABLE sys.dump_statements(o INT, s STRING);\n"
					"CREATE PROCEDURE sys._dump_table_data(sch STRING, tbl STRING) BEGIN\n"
					"    DECLARE k INT;\n"
					"    SET k = (SELECT MIN(c.id) FROM sys.columns c, sys.tables t WHERE c.table_id = t.id AND t.name = tbl);\n"
					"    IF k IS NOT NULL THEN\n"
					"        DECLARE cname STRING;\n"
					"        DECLARE ctype STRING;\n"
					"        SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);\n"
					"        SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);\n"
					"        DECLARE COPY_INTO_STMT STRING;\n"
					"        DECLARE _cnt INT;\n"
					"        SET _cnt = (SELECT MIN(s.count) FROM sys.storage() s WHERE s.schema = sch AND s.table = tbl);\n"
					"        IF _cnt > 0 THEN\n"
					"            SET COPY_INTO_STMT = 'COPY ' || _cnt ||  ' RECORDS INTO ' || sys.FQN(sch, tbl) || '(' || sys.DQ(cname);\n"
					"            DECLARE SELECT_DATA_STMT STRING;\n"
					"            SET SELECT_DATA_STMT = 'SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), ' || sys.prepare_esc(cname, ctype);\n"
					"            DECLARE M INT;\n"
					"            SET M = (SELECT MAX(c.id) FROM sys.columns c, sys.tables t WHERE c.table_id = t.id AND t.name = tbl);\n"
					"            WHILE (k < M) DO\n"
					"                SET k = (SELECT MIN(c.id) FROM sys.columns c, sys.tables t WHERE c.table_id = t.id AND t.name = tbl AND c.id > k);\n"
					"                SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);\n"
					"                SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);\n"
					"                SET COPY_INTO_STMT = (COPY_INTO_STMT || ', ' || sys.DQ(cname));\n"
					"                SET SELECT_DATA_STMT = SELECT_DATA_STMT || '|| ''|'' || ' || sys.prepare_esc(cname, ctype);\n"
					"            END WHILE;\n"
					"            SET COPY_INTO_STMT = (COPY_INTO_STMT || ') FROM STDIN USING DELIMITERS ''|'',E''\\\\n'',''\"'';');\n"
					"            SET SELECT_DATA_STMT =  SELECT_DATA_STMT || ' FROM ' || sys.FQN(sch, tbl);\n"
					"            insert into sys.dump_statements VALUES ( (SELECT COUNT(*) FROM sys.dump_statements) + 1, COPY_INTO_STMT);\n"
					"            CALL sys.EVAL('INSERT INTO sys.dump_statements ' || SELECT_DATA_STMT || ';');\n"
					"        END IF;\n"
					"    END IF;\n"
					"END;\n"
					"CREATE PROCEDURE sys.dump_table_data() BEGIN\n"
					"    DECLARE i INT;\n"
					"    SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);\n"
					"    IF i IS NOT NULL THEN\n"
					"        DECLARE M INT;\n"
					"        SET M = (SELECT MAX(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);\n"
					"        DECLARE sch STRING;\n"
					"        DECLARE tbl STRING;\n"
					"        WHILE i < M DO\n"
					"            set sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
					"            set tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
					"            CALL sys._dump_table_data(sch, tbl);\n"
					"            SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system AND t.id > i);\n"
					"        END WHILE;\n"
					"        set sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
					"        set tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
					"        CALL sys._dump_table_data(sch, tbl);\n"
					"    END IF;\n"
					"END;\n"
					"CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)\n"
					"BEGIN\n"
					"    SET SCHEMA sys;\n"
					"    TRUNCATE sys.dump_statements;\n"
					"    INSERT INTO sys.dump_statements VALUES (1, 'START TRANSACTION;');\n"
					"    INSERT INTO sys.dump_statements VALUES ( (SELECT COUNT(*) FROM sys.dump_statements) + 1, 'SET SCHEMA \"sys\";');\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_roles;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_users;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_schemas;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_user_defined_types;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_add_schemas_to_users;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_grant_user_privileges;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_sequences;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_start_sequences;\n"
					"    --functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(ORDER BY stmts.o), stmts.s\n"
					"    FROM (\n"
					"            SELECT * FROM sys.dump_functions f\n"
					"            UNION\n"
					"            SELECT * FROM sys.dump_tables t\n"
					"        ) AS stmts(o, s);\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_defaults;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_constraint_type;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_indices;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_foreign_keys;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_partition_tables;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_triggers;\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_comments;\n"
					"    --We are dumping ALL privileges so we need to erase existing privileges on the receiving side;\n"
					"    INSERT INTO sys.dump_statements VALUES ( (SELECT COUNT(*) FROM sys.dump_statements) + 1, 'TRUNCATE sys.privileges;');\n"
					"    INSERT INTO sys.dump_statements SELECT  (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_privileges;\n"
					"    IF NOT DESCRIBE THEN\n"
					"        CALL sys.dump_table_data();\n"
					"    END IF;\n"
					"    INSERT INTO sys.dump_statements VALUES ( (SELECT COUNT(*) FROM sys.dump_statements) + 1, 'COMMIT;');\n"
					"    RETURN sys.dump_statements;\n"
					"END;\n");

			// Set the system flag for the new dump and describe SQL objects.
			pos += snprintf(buf + pos, bufsize - pos,
					"UPDATE sys.functions f SET system = true WHERE\n"
					"    schema_id = 2000 AND\n"
					"    (\n"
					"        (\n"
					"            type = (select function_type_id from sys.function_types where function_type_name = 'Function returning a table') AND\n"
					"            name in (\n"
					"                'dump_database',\n"
					"                'describe_table',\n"
					"                'describe_columns',\n"
					"                'describe_function'\n"
					"            )\n"
					"        ) OR\n"
					"        (\n"
					"            type = (select function_type_id from sys.function_types where function_type_name = 'Scalar function') AND\n"
					"            name in (\n"
					"                'sq',\n"
					"                'dq',\n"
					"                'fqn',\n"
					"                'alter_table',\n"
					"                'replace_first',\n"
					"                'schema_guard',\n"
					"                'get_merge_table_partition_expressions',\n"
					"                'get_remote_table_expressions',\n"
					"                'esc',\n"
					"                'prepare_esc',\n"
					"                'current_size_dump_statements',\n"
					"                'describe_type'\n"
					"            )\n"
					"        ) OR\n"
					"        (\n"
					"            type = (select function_type_id from sys.function_types where function_type_keyword = 'PROCEDURE') AND\n"
					"            name in (\n"
					"                'eval',\n"
					"                '_dump_table_data',\n"
					"                'dump_table_data'\n"
					"            )\n"
					"        )\n"
					");\n"
					"UPDATE sys._tables SET system = true WHERE \n"
					"    schema_id = 2000 AND\n"
					"    (\n"
					"        (\n"
					"            type = (select table_type_id from sys.table_types where table_type_name = 'TABLE') AND\n"
					"            name = 'dump_statements'\n"
					"        ) OR\n"
					"        (\n"
					"            type = (select table_type_id from sys.table_types where table_type_name = 'VIEW') AND\n"
					"            name in (\n"
					"                'describe_constraints',\n"
					"                'describe_indices',\n"
					"                'describe_column_defaults',\n"
					"                'describe_foreign_keys',\n"
					"                'describe_tables',\n"
					"                'describe_triggers',\n"
					"                'describe_comments',\n"
					"                'fully_qualified_functions',\n"
					"                'describe_privileges',\n"
					"                'describe_user_defined_types',\n"
					"                'describe_partition_tables',\n"
					"                'describe_sequences',\n"
					"                'describe_functions',\n"
					"                'dump_create_roles',\n"
					"                'dump_create_users',\n"
					"                'dump_create_schemas',\n"
					"                'dump_add_schemas_to_users',\n"
					"                'dump_grant_user_privileges',\n"
					"                'dump_table_constraint_type',\n"
					"                'dump_indices',\n"
					"                'dump_column_defaults',\n"
					"                'dump_foreign_keys',\n"
					"                'dump_partition_tables',\n"
					"                'dump_sequences',\n"
					"                'dump_start_sequences',\n"
					"                'dump_functions',\n"
					"                'dump_tables',\n"
					"                'dump_triggers',\n"
					"                'dump_comments',\n"
					"                'dump_user_defined_types',\n"
					"                'dump_privileges',\n"
					"                'dump_statements')\n"
					"        )\n"
					"    );\n");

			/* scoping2 branch changes, the 'users' view has to be re-created because of the 'schema_path' addition on 'db_user_info' table
			   However 'dependency_schemas_on_users' has a dependency on 'users', so it has to be re-created as well */
			t = mvc_bind_table(sql, s, "users");
			t->system = 0;	/* make it non-system else the drop view will fail */
			t = mvc_bind_table(sql, s, "dependency_schemas_on_users");
			t->system = 0;	/* make it non-system else the drop view will fail */
			pos += snprintf(buf + pos, bufsize - pos,
					"DROP VIEW sys.dependency_schemas_on_users;\n"
					"DROP VIEW sys.users;\n"

					"ALTER TABLE sys.db_user_info ADD COLUMN schema_path CLOB;\n"
					"UPDATE sys.db_user_info SET schema_path = '\"sys\"';\n"

					"CREATE VIEW sys.users AS\n"
					"SELECT u.\"name\" AS \"name\", ui.\"fullname\", ui.\"default_schema\", ui.\"schema_path\"\n"
					" FROM sys.db_users() AS u\n"
					" LEFT JOIN \"sys\".\"db_user_info\" AS ui ON u.\"name\" = ui.\"name\";\n"
					"CREATE VIEW sys.dependency_schemas_on_users AS\n"
					" SELECT s.id AS schema_id, s.name AS schema_name, u.name AS user_name, CAST(6 AS smallint) AS depend_type\n"
					" FROM sys.users AS u, sys.schemas AS s\n"
					" WHERE u.default_schema = s.id\n"
					" ORDER BY s.name, u.name;\n"
					"GRANT SELECT ON sys.dependency_schemas_on_users TO PUBLIC;\n"
					"update sys._tables set system = true where system <> true and name in ('users','dependency_schemas_on_users')"
					" and schema_id = 2000 and type = %d;\n", (int) tt_view);

			pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
			pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			if ((err = SQLstatementIntern(c, buf, "update", true, false, NULL)) != MAL_SUCCEED)
				goto bailout;

			pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
					"ALTER TABLE sys.keywords SET READ ONLY;\n"
					"ALTER TABLE sys.table_types SET READ ONLY;\n"
					"ALTER TABLE sys.function_types SET READ ONLY;\n");
			pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (output)
		res_table_destroy(output);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

int
SQLupgrades(Client c, mvc *m)
{
	sql_subtype tp;
	sql_subfunc *f;
	char *err, *prev_schema = GDKstrdup(get_string_global_var(m, "current_schema"));
	sql_schema *s = mvc_bind_schema(m, "sys");
	sql_table *t;
	bool systabfixed = false;

	if (prev_schema == NULL) {
		TRC_CRITICAL(SQL_PARSER, "Allocation failure while running SQL upgrades\n");
		return -1;
	}

#ifdef HAVE_HGE
	sql_find_subtype(&tp, "hugeint", 0, 0);
	if (!sql_bind_func(m, s->base.name, "var_pop", &tp, NULL, F_AGGR)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_hugeint(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	}
#endif

	f = sql_bind_func_(m, s->base.name, "env", NULL, F_UNION);
	m->session->status = 0; /* if the function was not found clean the error */
	m->errstr[0] = '\0';
	sqlstore *store = m->session->tr->store;
	if (f && sql_privilege(m, ROLE_PUBLIC, f->func->base.id, PRIV_EXECUTE) != PRIV_EXECUTE) {
		sql_table *privs = find_sql_table(m->session->tr, s, "privileges");
		int pub = ROLE_PUBLIC, p = PRIV_EXECUTE, zero = 0;

		store->table_api.table_insert(m->session->tr, privs, &f->func->base.id, &pub, &p, &zero, &zero);
	}


	if (sql_bind_func(m, s->base.name, "dependencies_schemas_on_users", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_owners_on_schemas", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_views", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_indexes", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_triggers", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_foreignkeys", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_functions", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_views", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_keys", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_indexes", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_functions", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_triggers", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_views_on_functions", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_views_on_triggers", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_functions_on_functions", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_functions_on_triggers", NULL, NULL, F_UNION)
	 && sql_bind_func(m, s->base.name, "dependencies_keys_on_foreignkeys", NULL, NULL, F_UNION)	) {
		if ((err = sql_drop_functions_dependencies_Xs_on_Ys(c, prev_schema)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	} else {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
	}


	if ((t = mvc_bind_table(m, s, "systemfunctions")) != NULL &&
	    t->type == tt_table) {
		if (!systabfixed &&
		    (err = sql_fix_system_tables(c, m, prev_schema)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
		systabfixed = true;
		if ((err = sql_update_apr2019(c, m, prev_schema)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	}

	/* when function storagemodel() exists and views tablestorage
	 * and schemastorage don't, then upgrade storagemodel to match
	 * 75_storagemodel.sql */
	if (sql_bind_func(m, s->base.name, "storagemodel", NULL, NULL, F_UNION)
	 && (t = mvc_bind_table(m, s, "tablestorage")) == NULL
	 && (t = mvc_bind_table(m, s, "schemastorage")) == NULL ) {
		if ((err = sql_update_storagemodel(c, m, prev_schema, false)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	} else {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
	}

	if ((err = sql_update_apr2019_sp1(c)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		GDKfree(prev_schema);
		return -1;
	}

	if (sql_bind_func(m, s->base.name, "times", NULL, NULL, F_PROC)) {
		if ((err = sql_update_apr2019_sp2(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	} else {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
	}

	sql_find_subtype(&tp, "varchar", 0, 0);
	if (!sql_bind_func3(m, s->base.name, "deltas", &tp, &tp, &tp, F_UNION)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_nov2019_missing_dependencies(c, m)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
		if (!systabfixed &&
		    (err = sql_fix_system_tables(c, m, prev_schema)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
		systabfixed = true;
		if ((err = sql_update_nov2019(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	}

#ifdef HAVE_HGE
	sql_find_subtype(&tp, "hugeint", 0, 0);
	if (!sql_bind_func(m, s->base.name, "median_avg", &tp, NULL, F_AGGR)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_nov2019_sp1_hugeint(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	}
#endif

	if (!sql_bind_func(m, s->base.name, "suspend_log_flushing", NULL, NULL, F_PROC)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_jun2020(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	}

	if ((err = sql_update_jun2020_bam(c, m, prev_schema)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		GDKfree(prev_schema);
		return -1;
	}

#ifdef HAVE_HGE
	sql_find_subtype(&tp, "hugeint", 0, 0);
	if (!sql_bind_func(m, s->base.name, "covar_pop", &tp, &tp, F_AGGR)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_jun2020_sp1_hugeint(c, prev_schema)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	}
#endif

	if ((err = sql_update_jun2020_sp2(c, m, prev_schema, &systabfixed)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		GDKfree(prev_schema);
		return -1;
	}

	sql_find_subtype(&tp, "varchar", 0, 0);
	if (sql_bind_func(m, s->base.name, "lidarattach", &tp, NULL, F_PROC)) {
		if ((err = sql_update_oscar_lidar(c)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			GDKfree(prev_schema);
			return -1;
		}
	} else {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
	}

	if ((err = sql_update_oscar(c, m, prev_schema, &systabfixed)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		GDKfree(prev_schema);
		return -1;
	}

	if ((err = sql_update_oct2020(c, m, prev_schema, &systabfixed)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		GDKfree(prev_schema);
		return -1;
	}

	if ((err = sql_update_oct2020_sp1(c, m, prev_schema, &systabfixed)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		GDKfree(prev_schema);
		return -1;
	}

	if ((err = sql_update_default(c, m, prev_schema, &systabfixed)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		GDKfree(prev_schema);
		return -1;
	}

	GDKfree(prev_schema);
	return 0;
}
