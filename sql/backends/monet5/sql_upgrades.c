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

#include "mal_authorize.h"

/* this function can be used to recreate the system tables (types,
 * functions, args) when internal types and/or functions have changed
 * (i.e. the ones in sql_types.c) */
static str
sql_fix_system_tables(Client c, mvc *sql)
{
	size_t bufsize = 1000000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	node *n;
	sql_schema *s;
	static const char *boolnames[2] = {"false", "true"};

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	s = mvc_bind_schema(sql, "sys");

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
				t->base.id, t->impl, t->base.name, t->digits,
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

		if (func->private || func->base.id >= FUNC_OIDS)
			continue;

		pos += snprintf(buf + pos, bufsize - pos,
				"insert into sys.functions values"
				" (%d, '%s', '%s', '%s',"
				" %d, %d, %s, %s, %s, %d, %s, %s);\n",
				func->base.id, func->base.name,
				sql_func_imp(func), sql_func_mod(func), (int) FUNC_LANG_INT,
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
						arg->type.type->base.name,
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
						arg->type.type->base.name,
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
						arg->type.type->base.name,
						arg->type.digits,
						arg->type.scale,
						arg->inout, number);
		}
	}

	assert(pos < bufsize);
	printf("Running database upgrade commands to update system tables.\n\n");
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
check_sys_tables(Client c, mvc *m, sql_schema *s)
{
	struct {
		const char *name;
		const char *func;
		const char *type;
		sql_ftype ftype;
	} tests[] = {
		/* tests a few internal functions: the last one created, the
		 * first one created, and one of the first ones created after
		 * the geom module */
		{ "sys_update_tables", "update_tables", NULL,   F_PROC, },
		{ "length",            "nitems",        "blob", F_FUNC, },
		{ "isnull",            "isnil",         "void", F_FUNC, },
		{0},
	};

	/* if any of the tested function's internal ID does not match the ID
	 * in the sys.functions table, we recreate the internal part of the
	 * system tables */
	for (int i = 0; tests[i].name; i++) {
		bool needsystabfix = true;
		sql_subtype tp, *tpp;
		if (tests[i].type) {
			sql_find_subtype(&tp, tests[i].type, 0, 0);
			tpp = &tp;
		} else {
			tpp = NULL;
		}
		sql_subfunc *f = sql_bind_func(m, s->base.name, tests[i].name, tpp, NULL, tests[i].ftype, true);
		if (f == NULL)
			throw(SQL, __func__, "cannot find procedure sys.%s(%s)", tests[i].name, tests[i].type ? tests[i].type : "");
		sqlid id = f->func->base.id;
		char buf[128];
		snprintf(buf, sizeof(buf), "select id from sys.functions where name = '%s' and func = '%s' and schema_id = 2000;\n", tests[i].name, tests[i].func);
		res_table *output = NULL;
		char *err = SQLstatementIntern(c, buf, "update", true, false, &output);
		if (err)
			return err;
		BAT *b;
		if ((b = BATdescriptor(output->cols[0].b)) != NULL) {
			if (BATcount(b) > 0) {
				BATiter bi = bat_iterator(b);
				needsystabfix = * (int *) BUNtloc(bi, 0) != id;
				bat_iterator_end(&bi);
			}
			BBPunfix(b->batCacheid);
		}
		res_table_destroy(output);
		if (needsystabfix)
			return sql_fix_system_tables(c, m);
	}
	return NULL;
}

#ifdef HAVE_HGE
static str
sql_update_hugeint(Client c, mvc *sql)
{
	size_t bufsize = 8192, pos = 0;
	char *buf, *err;

	(void) sql;
	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

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
			"update sys.functions set system = true where system <> true and name in ('generate_series') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'covar_samp', 'var_pop', 'covar_pop', 'median', 'median_avg', 'quantile', 'quantile_avg', 'corr') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'covar_samp', 'var_pop', 'covar_pop', 'corr') and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n"
			"update sys.functions set system = true where system <> true and name = 'filter' and schema_id = (select id from sys.schemas where name = 'json') and type = %d;\n",
			(int) F_UNION, (int) F_AGGR, (int) F_ANALYTIC, (int) F_FUNC);

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

#ifdef HAVE_SHP
static str
sql_update_shp(Client c)
{
	const char *query = "create procedure SHPattach(fname string) external name shp.attach;\ncreate procedure SHPload(fid integer) external name shp.import;\ncreate procedure SHPload(fid integer, filter geometry) external name shp.import;\nupdate sys.functions set system = true where schema_id = 2000 and name in ('shpattach', 'shpload');\n";
	printf("Running database upgrade commands:\n%s\n", query);
	fflush(stdout);
	return SQLstatementIntern(c, query, "update", true, false, NULL);
}
#endif

static str
sql_update_generator(Client c)
{
	const char *query = "update sys.args set name = 'limit' where name = 'last' and func_id in (select id from sys.functions where schema_id = 2000 and name = 'generate_series' and func like '% last %');\n"
		"update sys.functions set func = replace(func, ' last ', ' \"limit\" ') where schema_id = 2000 and name = 'generate_series' and func like '% last %';\n";
	return SQLstatementIntern(c, query, "update", true, false, NULL);
}

static str
sql_drop_functions_dependencies_Xs_on_Ys(Client c)
{
	size_t bufsize = 1600, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* remove functions which were created in sql/scripts/21_dependency_functions.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"DROP FUNCTION dependencies_schemas_on_users() CASCADE;\n"
			"DROP FUNCTION dependencies_owners_on_schemas() CASCADE;\n"
			"DROP FUNCTION dependencies_tables_on_views() CASCADE;\n"
			"DROP FUNCTION dependencies_tables_on_indexes() CASCADE;\n"
			"DROP FUNCTION dependencies_tables_on_triggers() CASCADE;\n"
			"DROP FUNCTION dependencies_tables_on_foreignKeys() CASCADE;\n"
			"DROP FUNCTION dependencies_tables_on_functions() CASCADE;\n"
			"DROP FUNCTION dependencies_columns_on_views() CASCADE;\n"
			"DROP FUNCTION dependencies_columns_on_keys() CASCADE;\n"
			"DROP FUNCTION dependencies_columns_on_indexes() CASCADE;\n"
			"DROP FUNCTION dependencies_columns_on_functions() CASCADE;\n"
			"DROP FUNCTION dependencies_columns_on_triggers() CASCADE;\n"
			"DROP FUNCTION dependencies_views_on_functions() CASCADE;\n"
			"DROP FUNCTION dependencies_views_on_triggers() CASCADE;\n"
			"DROP FUNCTION dependencies_functions_on_functions() CASCADE;\n"
			"DROP FUNCTION dependencies_functions_on_triggers() CASCADE;\n"
			"DROP FUNCTION dependencies_keys_on_foreignKeys() CASCADE;\n");

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_storagemodel(Client c, mvc *sql, bool oct2020_upgrade)
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
		/* drop objects in reverse order of original creation of old 75_storagemodel.sql */
		"drop view if exists sys.tablestoragemodel cascade;\n"
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
		"drop procedure if exists sys.storagemodelinit() cascade;\n"
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

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#define FLUSH_INSERTS_IF_BUFFERFILLED									\
	do {																\
		/* Each new value should add about 20 bytes to the buffer, */	\
		/* "flush" when is 200 bytes from being full */					\
		if (pos > 7900) {												\
			pos += snprintf(buf + pos, bufsize - pos,					\
							") as t1(c1,c2,c3) where t1.c1 not in (select \"id\" from sys.dependencies where depend_id = t1.c2);\n"); \
			assert(pos < bufsize);										\
			printf("Running database upgrade commands:\n%s\n", buf);	\
			fflush(stdout);												\
			err = SQLstatementIntern(c, buf, "update", true, false, NULL); \
			if (err)													\
				goto bailout;											\
			pos = 0;													\
			pos += snprintf(buf + pos, bufsize - pos, "insert into sys.dependencies select c1, c2, c3 from (values"); \
			ppos = pos;													\
			first = true;												\
		}																\
	} while (0)

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
	for (sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema*)b;

		struct os_iter oi;
		os_iterator(&oi, s->funcs, sql->session->tr, NULL);
		for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
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
					r = sql_processrelation(sql, r, 0, 0, 0, 0);
				if (r) {
					list *id_l = rel_dependencies(sql, r);

					for (node *o = id_l->h ; o ; o = o->next) {
						sqlid next = ((sql_base*) o->data)->id;
						if (next != f->base.id) {
							pos += snprintf(buf + pos, bufsize - pos, "%s(%d,%d,%d)", first ? "" : ",", next,
											f->base.id, (int)(!IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY));
							first = false;
							FLUSH_INSERTS_IF_BUFFERFILLED;
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
						r = sql_processrelation(sql, r, 0, 0, 0, 0);
					if (r) {
						list *id_l = rel_dependencies(sql, r);

						for (node *o = id_l->h ; o ; o = o->next) {
							sqlid next = ((sql_base*) o->data)->id;
							if (next != t->base.id) {
								pos += snprintf(buf + pos, bufsize - pos, "%s(%d,%d,%d)", first ? "" : ",",
												next, t->base.id, (int) VIEW_DEPENDENCY);
								first = false;
								FLUSH_INSERTS_IF_BUFFERFILLED;
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
							r = sql_processrelation(sql, r, 0, 0, 0, 0);
						if (r) {
							list *id_l = rel_dependencies(sql, r);

							for (node *o = id_l->h ; o ; o = o->next) {
								sqlid next = ((sql_base*) o->data)->id;
								if (next != tr->base.id) {
									pos += snprintf(buf + pos, bufsize - pos, "%s(%d,%d,%d)", first ? "" : ",",
													next, tr->base.id, (int) TRIGGER_DEPENDENCY);
									first = false;
									FLUSH_INSERTS_IF_BUFFERFILLED;
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
		fflush(stdout);
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
sql_update_nov2019(Client c, mvc *sql)
{
	size_t bufsize = 16384, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);

	(void) sql;
	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos,
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
			"drop procedure master() cascade;\n"
			"drop procedure master(string) cascade;\n"
			"drop procedure stopmaster() cascade;\n"
			"drop procedure masterbeat(int) cascade;\n"
			"drop function masterClock() cascade;\n"
			"drop function masterTick() cascade;\n"
			"drop procedure replicate() cascade;\n"
			"drop procedure replicate(timestamp) cascade;\n"
			"drop procedure replicate(string) cascade;\n"
			"drop procedure replicate(string, timestamp) cascade;\n"
			"drop procedure replicate(string, tinyint) cascade;\n"
			"drop procedure replicate(string, smallint) cascade;\n"
			"drop procedure replicate(string, integer) cascade;\n"
			"drop procedure replicate(string, bigint) cascade;\n"
			"drop procedure replicabeat(integer) cascade;\n"
			"drop function replicaClock() cascade;\n"
			"drop function replicaTick() cascade;\n"

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
			"drop function json.text(string) cascade;\n"
			"drop function json.text(int) cascade;\n");

	/* The first argument to copyfrom is a PTR type */
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.args set type = 'ptr' where"
			" func_id = (select id from sys.functions where name = 'copyfrom' and func = 'copy_from' and mod = 'sql' and type = %d) and name = 'arg_1';\n", (int) F_UNION);

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#ifdef HAVE_HGE
static str
sql_update_nov2019_sp1_hugeint(Client c, mvc *sql)
{
	size_t bufsize = 1024, pos = 0;
	char *buf, *err;

	(void) sql;
	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

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

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_update_jun2020(Client c, mvc *sql)
{
	sql_table *t;
	size_t bufsize = 32768, pos = 0;
	char *err = NULL, *buf = NULL;
	sql_schema *sys = mvc_bind_schema(sql, "sys");

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

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
			"drop function isaURL(url) cascade;\n"
			"CREATE function isaURL(theUrl string) RETURNS BOOL\n"
			" EXTERNAL NAME url.\"isaURL\";\n"
			"GRANT EXECUTE ON FUNCTION isaURL(string) TO PUBLIC;\n"
			"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'isaurl' and type = %d;\n", (int) F_FUNC);

	/* 13_date.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function str_to_time(string, string) cascade;\n"
			"drop function time_to_str(time, string) cascade;\n"
			"drop function str_to_timestamp(string, string) cascade;\n"
			"drop function timestamp_to_str(timestamp, string) cascade;\n"
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
			"drop view sys.tracelog cascade;\n"
			"drop function sys.tracelog() cascade;\n"
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
			"drop function sys.epoch(bigint) cascade;\n"
			"drop function sys.epoch(int) cascade;\n"
			"drop function sys.epoch(timestamp) cascade;\n"
			"drop function sys.epoch(timestamp with time zone) cascade;\n"
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
			"drop view sys.sessions cascade;\n"
			"drop function sys.sessions cascade;\n"
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
			"drop view sys.queue cascade;\n"
			"drop function sys.queue cascade;\n"
			"create function sys.queue()\n"
			"returns table(\n"
			"\"tag\" bigint,\n"
			"\"sessionid\" int,\n"
			"\"username\" string,\n"
			"\"started\" timestamp,\n"
			"\"status\" string,\n"
			"\"query\" string,\n"
			"\"progress\" int,\n"
			"\"maxworkers\" int,\n"
			"\"footprint\" int)\n"
			" external name sysmon.queue;\n"
			"grant execute on function sys.queue to public;\n"
			"create view sys.queue as select * from sys.queue();\n"
			"grant select on sys.queue to public;\n"

			"drop procedure sys.pause(int) cascade;\n"
			"drop procedure sys.resume(int) cascade;\n"
			"drop procedure sys.stop(int) cascade;\n"

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
			"DROP AGGREGATE stddev_samp(date) CASCADE;\n"
			"DROP AGGREGATE stddev_samp(time) CASCADE;\n"
			"DROP AGGREGATE stddev_samp(timestamp) CASCADE;\n"
			"DROP AGGREGATE stddev_pop(date) CASCADE;\n"
			"DROP AGGREGATE stddev_pop(time) CASCADE;\n"
			"DROP AGGREGATE stddev_pop(timestamp) CASCADE;\n"
			"DROP AGGREGATE var_samp(date) CASCADE;\n"
			"DROP AGGREGATE var_samp(time) CASCADE;\n"
			"DROP AGGREGATE var_samp(timestamp) CASCADE;\n"
			"DROP AGGREGATE var_pop(date) CASCADE;\n"
			"DROP AGGREGATE var_pop(time) CASCADE;\n"
			"DROP AGGREGATE var_pop(timestamp) CASCADE;\n");

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
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	if (err == MAL_SUCCEED) {
		pos = snprintf(buf, bufsize,
			       "ALTER TABLE sys.keywords SET READ ONLY;\n"
			       "ALTER TABLE sys.function_languages SET READ ONLY;\n");
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		fflush(stdout);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jun2020_bam(Client c, mvc *m)
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
			"update sys.schemas set system = false where name = 'bam';\n"
			"update sys._tables set system = false where schema_id in (select id from sys.schemas where name = 'bam');\n"
			"drop procedure bam.bam_loader_repos cascade;\n"
			"drop procedure bam.bam_loader_files cascade;\n"
			"drop procedure bam.bam_loader_file cascade;\n"
			"drop procedure bam.bam_drop_file cascade;\n"
			"drop function bam.bam_flag cascade;\n"
			"drop function bam.reverse_seq cascade;\n"
			"drop function bam.reverse_qual cascade;\n"
			"drop function bam.seq_length cascade;\n"
			"drop function bam.seq_char cascade;\n"
			"drop procedure bam.sam_export cascade;\n"
			"drop procedure bam.bam_export cascade;\n");
	if (b) {
		if (BATcount(b) > 0 && *(lng *) Tloc(b, 0) == 0) {
			/* tables in bam schema are empty: drop them */
			pos += snprintf(buf + pos, bufsize - pos,
					"drop table bam.sq cascade;\n"
					"drop table bam.rg cascade;\n"
					"drop table bam.pg cascade;\n"
					"drop table bam.export cascade;\n"
					"drop table bam.files cascade;\n"
					"drop schema bam cascade;\n");
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);

	GDKfree(buf);
	return err;
}

#ifdef HAVE_HGE
static str
sql_update_jun2020_sp1_hugeint(Client c)
{
	size_t bufsize = 8192, pos = 0;
	char *buf, *err;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

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

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_update_oscar_lidar(Client c)
{
	char *query =
		"drop procedure sys.lidarattach(string) cascade;\n"
		"drop procedure sys.lidarload(string) cascade;\n"
		"drop procedure sys.lidarexport(string, string, string) cascade;\n";
	printf("Running database upgrade commands:\n%s\n", query);
	fflush(stdout);
	return SQLstatementIntern(c, query, "update", true, false, NULL);
}

static str
sql_update_oscar(Client c, mvc *sql)
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
		BATiter bi = bat_iterator_nolock(b);
		if (BATcount(b) > 0 && strcmp(BUNtail(bi, 0), "progress") == 0) {
			pos = 0;

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
					"drop view sys.queue cascade;\n"
					"drop function sys.queue cascade;\n"
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
					"drop procedure sys.pause(bigint) cascade;\n"
					"drop procedure sys.resume(bigint) cascade;\n"
					"drop procedure sys.stop(bigint) cascade;\n"
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
					"drop function \"sys\".\"var\"() cascade;\n"
					"create function \"sys\".\"var\"() "
					"returns table("
					"\"schema\" string, "
					"\"name\" string, "
					"\"type\" string, "
					"\"value\" string) "
					"external name \"sql\".\"sql_variables\";\n"
					"grant execute on function \"sys\".\"var\" to public;\n");

			pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set system = true"
					" where name = 'var' and schema_id = (select id from sys.schemas where name = 'sys');\n"
					"update sys.privileges set grantor = 0 where obj_id = (select id from sys.functions where name = 'var' and schema_id = (select id from sys.schemas where name = 'sys') and type = %d);\n",
					(int) F_UNION);

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

			/* SQL functions without backend implementations */
			pos += snprintf(buf + pos, bufsize - pos,
					"DROP FUNCTION \"sys\".\"getcontent\"(url) CASCADE;\n"
					"DROP AGGREGATE \"json\".\"output\"(json) CASCADE;\n");

			/* Move sys.degrees,sys.radians,sys.like and sys.ilike to sql_types.c definitions (I did this at the bat_logger) Remove the obsolete entries at privileges table */
			pos += snprintf(buf + pos, bufsize - pos,
					"delete from privileges where obj_id in (select obj_id from privileges left join functions on privileges.obj_id = functions.id where functions.id is null and privileges.obj_id not in ((SELECT tables.id from tables), 0));\n");

			assert(pos < bufsize);

			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2020(Client c, mvc *sql)
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
			pos = 0;

			/* 51_sys_schema_extensions.sql */
			t = mvc_bind_table(sql, s, "var_values");
			t->system = 0;	/* make it non-system else the drop view will fail */
			pos += snprintf(buf + pos, bufsize - pos,
							"DROP VIEW sys.var_values CASCADE;\n"
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
					"DROP AGGREGATE stddev_samp(INTERVAL SECOND) CASCADE;\n"
					"DROP AGGREGATE stddev_samp(INTERVAL MONTH) CASCADE;\n"
					"DROP WINDOW stddev_samp(INTERVAL SECOND) CASCADE;\n"
					"DROP WINDOW stddev_samp(INTERVAL MONTH) CASCADE;\n"
					"DROP AGGREGATE stddev_pop(INTERVAL SECOND) CASCADE;\n"
					"DROP AGGREGATE stddev_pop(INTERVAL MONTH) CASCADE;\n"
					"DROP WINDOW stddev_pop(INTERVAL SECOND) CASCADE;\n"
					"DROP WINDOW stddev_pop(INTERVAL MONTH) CASCADE;\n"
					"DROP AGGREGATE var_samp(INTERVAL SECOND) CASCADE;\n"
					"DROP AGGREGATE var_samp(INTERVAL MONTH) CASCADE;\n"
					"DROP WINDOW var_samp(INTERVAL SECOND) CASCADE;\n"
					"DROP WINDOW var_samp(INTERVAL MONTH) CASCADE;\n"
					"DROP AGGREGATE var_pop(INTERVAL SECOND) CASCADE;\n"
					"DROP AGGREGATE var_pop(INTERVAL MONTH) CASCADE;\n"
					"DROP WINDOW var_pop(INTERVAL SECOND) CASCADE;\n"
					"DROP WINDOW var_pop(INTERVAL MONTH) CASCADE;\n"
					"DROP AGGREGATE covar_samp(INTERVAL SECOND,INTERVAL SECOND) CASCADE;\n"
					"DROP AGGREGATE covar_samp(INTERVAL MONTH,INTERVAL MONTH) CASCADE;\n"
					"DROP WINDOW covar_samp(INTERVAL SECOND,INTERVAL SECOND) CASCADE;\n"
					"DROP WINDOW covar_samp(INTERVAL MONTH,INTERVAL MONTH) CASCADE;\n"
					"DROP AGGREGATE covar_pop(INTERVAL SECOND,INTERVAL SECOND) CASCADE;\n"
					"DROP AGGREGATE covar_pop(INTERVAL MONTH,INTERVAL MONTH) CASCADE;\n"
					"DROP WINDOW covar_pop(INTERVAL SECOND,INTERVAL SECOND) CASCADE;\n"
					"DROP WINDOW covar_pop(INTERVAL MONTH,INTERVAL MONTH) CASCADE;\n"
					"DROP AGGREGATE corr(INTERVAL SECOND,INTERVAL SECOND) CASCADE;\n"
					"DROP AGGREGATE corr(INTERVAL MONTH,INTERVAL MONTH) CASCADE;\n"
					"DROP WINDOW corr(INTERVAL SECOND,INTERVAL SECOND) CASCADE;\n"
					"DROP WINDOW corr(INTERVAL MONTH,INTERVAL MONTH) CASCADE;\n"
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

			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
			if (err != MAL_SUCCEED)
				goto bailout;

			pos = snprintf(buf, bufsize,
					"ALTER TABLE sys.keywords SET READ ONLY;\n");
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
			if (err != MAL_SUCCEED)
				goto bailout;
			err = sql_update_storagemodel(c, sql, true); /* because of day interval addition, we have to recreate the storagmodel views */
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
sql_update_oct2020_sp1(Client c, mvc *sql)
{
	size_t bufsize = 1024, pos = 0;
	char *buf = NULL, *err = NULL;

	if (!sql_bind_func(sql, "sys", "uuid", sql_bind_localtype("int"), NULL, F_FUNC, true)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';

		if ((buf = GDKmalloc(bufsize)) == NULL)
			throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

		/* 45_uuid.sql */
		pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.uuid(d int) returns uuid\n"
			" external name uuid.\"new\";\n"
			"GRANT EXECUTE ON FUNCTION sys.uuid(int) TO PUBLIC;\n"
			"update sys.functions set system = true where system <> true and name = 'uuid' and schema_id = (select id from sys.schemas where name = 'sys') and type = %d;\n", (int) F_FUNC);

		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		fflush(stdout);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jul2021(Client c, mvc *sql)
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
			/* 20_vacuum.sql */
			pos += snprintf(buf + pos, bufsize - pos,
							"drop procedure sys.shrink(string, string) cascade;\n"
							"drop procedure sys.reuse(string, string) cascade;\n"
							"drop procedure sys.vacuum(string, string) cascade;\n");

			/* 22_clients.sql */
			pos += snprintf(buf + pos, bufsize - pos,
							"create function sys.current_sessionid() returns int\n"
							"external name clients.current_sessionid;\n"
							"grant execute on function sys.current_sessionid to public;\n"
							"update sys.functions set system = true where system <> true and schema_id = 2000 and name = 'current_sessionid' and type = %d;\n", (int) F_FUNC);

			/* 25_debug.sql */
			pos += snprintf(buf + pos, bufsize - pos,
							"drop procedure sys.flush_log() cascade;\n");

			pos += snprintf(buf + pos, bufsize - pos,
							"drop function sys.deltas(string) cascade;\n"
							"drop function sys.deltas(string, string) cascade;\n"
							"drop function sys.deltas(string, string, string) cascade;\n");
			pos += snprintf(buf + pos, bufsize - pos,
							"create function sys.deltas (\"schema\" string)\n"
							"returns table (\"id\" int, \"segments\" bigint, \"all\" bigint, \"inserted\" bigint, \"updates\" bigint, \"deletes\" bigint, \"level\" int)\n"
							"external name \"sql\".\"deltas\";\n"
							"create function sys.deltas (\"schema\" string, \"table\" string)\n"
							"returns table (\"id\" int, \"segments\" bigint, \"all\" bigint, \"inserted\" bigint, \"updates\" bigint, \"deletes\" bigint, \"level\" int)\n"
							"external name \"sql\".\"deltas\";\n"
							"create function sys.deltas (\"schema\" string, \"table\" string, \"column\" string)\n"
							"returns table (\"id\" int, \"segments\" bigint, \"all\" bigint, \"inserted\" bigint, \"updates\" bigint, \"deletes\" bigint, \"level\" int)\n"
							"external name \"sql\".\"deltas\";\n"
							"update sys.functions set system = true"
							" where schema_id = 2000 and name = 'deltas';\n");

			/* 26_sysmon */
			t = mvc_bind_table(sql, s, "queue");
			t->system = 0; /* make it non-system else the drop view will fail */

			pos += snprintf(buf + pos, bufsize - pos,
							"drop view sys.queue cascade;\n"
							"drop function sys.queue cascade;\n"
							"create function sys.queue()\n"
							"returns table(\n"
							"\"tag\" bigint,\n"
							"\"sessionid\" int,\n"
							"\"username\" string,\n"
							"\"started\" timestamp,\n"
							"\"status\" string,\n"
							"\"query\" string,\n"
							"\"finished\" timestamp,\n"
							"\"maxworkers\" int,\n"
							"\"footprint\" int\n"
							")\n"
							"external name sysmon.queue;\n"
							"grant execute on function sys.queue to public;\n"
							"create view sys.queue as select * from sys.queue();\n"
							"grant select on sys.queue to public;\n");
			pos += snprintf(buf + pos, bufsize - pos,
							"update sys.functions set system = true where system <> true and schema_id = 2000"
							" and name = 'queue' and type = %d;\n", (int) F_UNION);
			pos += snprintf(buf + pos, bufsize - pos,
							"update sys._tables set system = true where schema_id = 2000"
							" and name = 'queue';\n");

			/* fix up dependencies for function getproj4 (if it exists) */
			pos += snprintf(buf + pos, bufsize - pos,
							"delete from sys.dependencies d where d.depend_id = (select id from sys.functions where name = 'getproj4' and schema_id = 2000) and id in (select id from sys._columns where name not in ('proj4text', 'srid'));\n");

			/* 41_json.sql */
			pos += snprintf(buf + pos, bufsize - pos,
							"drop function json.isobject(string) cascade;\n"
							"drop function json.isarray(string) cascade;\n"
							"drop function json.isvalid(json) cascade;\n"
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
					"drop procedure sys.storagemodelinit() cascade;\n"
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
					"UPDATE sys.functions SET system = true WHERE\n"
					"    system <> true AND\n"
					"    schema_id = 2000 AND\n"
					"    type = %d AND\n"
					"    name in (\n"
					"        'describe_columns',\n"
					"        'describe_function',\n"
					"        'describe_table',\n"
					"        'dump_database'\n"
					"    );\n",
					F_UNION);
			pos += snprintf(buf + pos, bufsize - pos,
					"UPDATE sys.functions SET system = true WHERE\n"
					"    system <> true AND\n"
					"    schema_id = 2000 AND\n"
					"    type = %d AND\n"
					"    name in (\n"
					"        'alter_table',\n"
					"        'describe_type',\n"
					"        'dq',\n"
					"        'esc',\n"
					"        'fqn',\n"
					"        'get_merge_table_partition_expressions',\n"
					"        'get_remote_table_expressions',\n"
					"        'prepare_esc',\n"
					"        'replace_first',\n"
					"        'schema_guard',\n"
					"        'sq'\n"
					"    );\n",
					F_FUNC);
			pos += snprintf(buf + pos, bufsize - pos,
					"UPDATE sys.functions SET system = true WHERE\n"
					"    system <> true AND\n"
					"    schema_id = 2000 AND\n"
					"    type = %d AND\n"
					"    name in (\n"
					"        '_dump_table_data',\n"
					"        'dump_table_data',\n"
					"        'eval'\n"
					"    );\n",
					F_PROC);
			pos += snprintf(buf + pos, bufsize - pos,
					"UPDATE sys._tables SET system = true WHERE\n"
					"    system <> true AND\n"
					"    schema_id = 2000 AND\n"
					"    type = %d AND\n"
					"    name = 'dump_statements';\n",
					(int) tt_table);
			pos += snprintf(buf + pos, bufsize - pos,
					"UPDATE sys._tables SET system = true WHERE\n"
					"    system <> true AND\n"
					"    schema_id = 2000 AND\n"
					"    type = %d AND\n"
					"    name in (\n"
					"        'describe_column_defaults',\n"
					"        'describe_comments',\n"
					"        'describe_constraints',\n"
					"        'describe_foreign_keys',\n"
					"        'describe_functions',\n"
					"        'describe_indices',\n"
					"        'describe_partition_tables',\n"
					"        'describe_privileges',\n"
					"        'describe_sequences',\n"
					"        'describe_tables',\n"
					"        'describe_triggers',\n"
					"        'describe_user_defined_types',\n"
					"        'dump_add_schemas_to_users',\n"
					"        'dump_column_defaults',\n"
					"        'dump_comments',\n"
					"        'dump_create_roles',\n"
					"        'dump_create_schemas',\n"
					"        'dump_create_users',\n"
					"        'dump_foreign_keys',\n"
					"        'dump_functions',\n"
					"        'dump_grant_user_privileges',\n"
					"        'dump_indices',\n"
					"        'dump_partition_tables',\n"
					"        'dump_privileges',\n"
					"        'dump_sequences',\n"
					"        'dump_start_sequences',\n"
					"        'dump_statements',\n"
					"        'dump_table_constraint_type',\n"
					"        'dump_tables',\n"
					"        'dump_triggers',\n"
					"        'dump_user_defined_types',\n"
					"        'fully_qualified_functions'\n"
					"    );\n",
					(int) tt_view);

			/* scoping2 branch changes, the 'users' view has to be re-created because of the 'schema_path' addition on 'db_user_info' table
			   However 'dependency_schemas_on_users' has a dependency on 'users', so it has to be re-created as well */
			t = mvc_bind_table(sql, s, "users");
			t->system = 0;	/* make it non-system else the drop view will fail */
			t = mvc_bind_table(sql, s, "dependency_schemas_on_users");
			t->system = 0;	/* make it non-system else the drop view will fail */
			pos += snprintf(buf + pos, bufsize - pos,
					"DROP VIEW sys.dependency_schemas_on_users CASCADE;\n"
					"DROP VIEW sys.users CASCADE;\n"

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

			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			if ((err = SQLstatementIntern(c, buf, "update", true, false, NULL)) != MAL_SUCCEED)
				goto bailout;

			pos = snprintf(buf, bufsize,
					"ALTER TABLE sys.keywords SET READ ONLY;\n"
					"ALTER TABLE sys.table_types SET READ ONLY;\n"
					"ALTER TABLE sys.function_types SET READ ONLY;\n");
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
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

/* upgrades after Jul2021_3 build */
static str
sql_update_jul2021_5(Client c, mvc *sql)
{
	size_t bufsize = 65536, pos = 0;
	char *buf = NULL, *err = NULL;
	res_table *output = NULL;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* if the string 'partition of merge table' is not in the sys.ids
	 * query, upgrade */
	pos += snprintf(buf + pos, bufsize - pos,
					"select query from sys._tables where name = 'ids' and schema_id = 2000 and query like '%%partition of merge table%%';\n");
	assert(pos < bufsize);
	if ((err = SQLstatementIntern(c, buf, "update", true, false, &output)) == NULL) {
		BAT *b;
		if ((b = BATdescriptor(output->cols[0].b))) {
			if (BATcount(b) == 0) {
				/* 21_dependency_views.sql */
				t = mvc_bind_table(sql, s, "ids");
				t->system = 0;	/* make it non-system else the drop view will fail */
				t = mvc_bind_table(sql, s, "dependencies_vw");
				t->system = 0;	/* make it non-system else the drop view will fail */
				pos += snprintf(buf + pos, bufsize - pos,
								"drop view sys.dependencies_vw cascade;\n"
								"drop view sys.ids cascade;\n");
				pos += snprintf(buf + pos, bufsize - pos,
								"CREATE VIEW sys.ids (id, name, schema_id, table_id, table_name, obj_type, sys_table) AS\n"
								"SELECT id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'author' AS obj_type, 'sys.auths' AS sys_table FROM sys.auths UNION ALL\n"
								"SELECT id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'schema', 'sys.schemas' FROM sys.schemas UNION ALL\n"
								"SELECT id, name, schema_id, id as table_id, name as table_name, case when type = 1 then 'view' else 'table' end, 'sys._tables' FROM sys._tables UNION ALL\n"
								"SELECT id, name, schema_id, id as table_id, name as table_name, case when type = 1 then 'view' else 'table' end, 'tmp._tables' FROM tmp._tables UNION ALL\n"
								"SELECT c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'sys._columns' FROM sys._columns c JOIN sys._tables t ON c.table_id = t.id UNION ALL\n"
								"SELECT c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'tmp._columns' FROM tmp._columns c JOIN tmp._tables t ON c.table_id = t.id UNION ALL\n"
								"SELECT k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'sys.keys' FROM sys.keys k JOIN sys._tables t ON k.table_id = t.id UNION ALL\n"
								"SELECT k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'tmp.keys' FROM tmp.keys k JOIN tmp._tables t ON k.table_id = t.id UNION ALL\n"
								"SELECT i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index', 'sys.idxs' FROM sys.idxs i JOIN sys._tables t ON i.table_id = t.id UNION ALL\n"
								"SELECT i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index', 'tmp.idxs' FROM tmp.idxs i JOIN tmp._tables t ON i.table_id = t.id UNION ALL\n"
								"SELECT g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'sys.triggers' FROM sys.triggers g JOIN sys._tables t ON g.table_id = t.id UNION ALL\n"
								"SELECT g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'tmp.triggers' FROM tmp.triggers g JOIN tmp._tables t ON g.table_id = t.id UNION ALL\n"
								"SELECT id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, case when type = 2 then 'procedure' else 'function' end, 'sys.functions' FROM sys.functions UNION ALL\n"
								"SELECT a.id, a.name, f.schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, case when f.type = 2 then 'procedure arg' else 'function arg' end, 'sys.args' FROM sys.args a JOIN sys.functions f ON a.func_id = f.id UNION ALL\n"
								"SELECT id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'sequence', 'sys.sequences' FROM sys.sequences UNION ALL\n"
								"SELECT o.id, o.name, pt.schema_id, pt.id, pt.name, 'partition of merge table', 'sys.objects' FROM sys.objects o JOIN sys._tables pt ON o.sub = pt.id JOIN sys._tables mt ON o.nr = mt.id WHERE mt.type = 3 UNION ALL\n"
								"SELECT id, sqlname, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'type', 'sys.types' FROM sys.types WHERE id > 2000\n"
								" ORDER BY id;\n"
								"GRANT SELECT ON sys.ids TO PUBLIC;\n");
				pos += snprintf(buf + pos, bufsize - pos,
								"CREATE VIEW sys.dependencies_vw AS\n"
								"SELECT d.id, i1.obj_type, i1.name,\n"
								"       d.depend_id as used_by_id, i2.obj_type as used_by_obj_type, i2.name as used_by_name,\n"
								"       d.depend_type, dt.dependency_type_name\n"
								"  FROM sys.dependencies d\n"
								"  JOIN sys.ids i1 ON d.id = i1.id\n"
								"  JOIN sys.ids i2 ON d.depend_id = i2.id\n"
								"  JOIN sys.dependency_types dt ON d.depend_type = dt.dependency_type_id\n"
								" ORDER BY id, depend_id;\n"
								"GRANT SELECT ON sys.dependencies_vw TO PUBLIC;\n");
				pos += snprintf(buf + pos, bufsize - pos,
								"UPDATE sys._tables SET system = true WHERE name in ('ids', 'dependencies_vw') AND schema_id = 2000;\n");

				assert(pos < bufsize);
				printf("Running database upgrade commands:\n%s\n", buf);
				fflush(stdout);
				err = SQLstatementIntern(c, buf, "update", true, false, NULL);
			}
			BBPunfix(b->batCacheid);
		}
		res_table_destroy(output);
	}

	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jan2022(Client c, mvc *sql)
{
	sql_subtype tp;
	size_t bufsize = 65536, pos = 0;
	char *buf = NULL, *err = NULL;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t;

	/* this bit of code is to upgrade from a Jan2022 RC to the Jan2022 release */
	sql_allocator *old_sa = sql->sa;
	if ((sql->sa = sa_create(sql->pa)) != NULL) {
		list *l;
		if ((l = sa_list(sql->sa)) != NULL) {
			sql_find_subtype(&tp, "varchar", 0, 0);
			list_append(l, &tp);
			list_append(l, &tp);
			list_append(l, &tp);
			if (sql_bind_func_(sql, s->base.name, "strimp_create", l, F_PROC, true)) {
				/* do the upgrade by removing the two functions */
				const char *query =
					"drop filter function sys.strimp_filter(string, string) cascade;\n"
					"drop procedure sys.strimp_create(string, string, string) cascade;\n";
				printf("Running database upgrade commands:\n%s\n", query);
				fflush(stdout);
				err = SQLstatementIntern(c, query, "update", true, false, NULL);
			}
			sql->session->status = 0; /* if the function was not found clean the error */
			sql->errstr[0] = '\0';
		}
		sa_destroy(sql->sa);
		if (err)
			return err;
	}
	sql->sa = old_sa;

	sql_find_subtype(&tp, "bigint", 0, 0);
	if (!sql_bind_func(sql, s->base.name, "epoch", &tp, NULL, F_FUNC, true)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
		/* nothing to do */
		return NULL;
	}

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* sys.epoch_ms now returns a decimal(18,3) */
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys.args set type = 'decimal', type_digits = 18, type_scale = 3 where func_id in (select id from sys.functions where name = 'epoch_ms' and schema_id = 2000) and number = 0 and type = 'bigint';\n");

	/* 16_tracelog */
	t = mvc_bind_table(sql, s, "tracelog");
	t->system = 0; /* make it non-system else the drop view will fail */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tracelog cascade;\n"
			"drop function sys.tracelog() cascade;\n"
			"create function sys.tracelog()\n"
			" returns table (\n"
			"  ticks bigint, -- time in microseconds\n"
			"  stmt string,  -- actual statement executed\n"
			"  event string  -- profiler event executed\n"
			" )\n"
			" external name sql.dump_trace;\n"
			"create view sys.tracelog as select * from sys.tracelog();\n"
			"update sys._tables set system = true where system <> true and schema_id = 2000"
			" and name = 'tracelog';\n"
			"update sys.functions set system = true where system <> true and schema_id = 2000"
			" and name = 'tracelog' and type = %d;\n", (int) F_UNION);

	/* 17_temporal.sql */
	pos += snprintf(buf + pos, bufsize - pos,
					"drop function sys.epoch(bigint) cascade;\n");
	pos += snprintf(buf + pos, bufsize - pos,
					"create function sys.epoch(sec DECIMAL(18,3)) "
					"returns TIMESTAMP WITH TIME ZONE\n"
					"external name mtime.epoch;\n"
					"grant execute on function sys.epoch (DECIMAL(18,3)) to public;\n"
					"update sys.functions set system = true where system <> true and name in ('epoch') and schema_id = 2000 and type = %d;\n", F_FUNC);

	/* 25_debug.sql */
	pos += snprintf(buf + pos, bufsize - pos,
					"drop function sys.malfunctions() cascade;\n"
					"create function sys.malfunctions()\n"
					" returns table(\"module\" string, \"function\" string, \"signature\" string, \"address\" string, \"comment\" string)\n"
					" external name \"manual\".\"functions\";\n"
					"create view sys.malfunctions as select * from sys.malfunctions();\n"
					"update sys._tables set system = true where system <> true and schema_id = 2000"
					" and name = 'malfunctions';\n"
					"update sys.functions set system = true where system <> true and schema_id = 2000"
					" and name = 'malfunctions';\n");

	/* 21_dependency_views.sql */
	t = mvc_bind_table(sql, s, "ids");
	t->system = 0; /* make it non-system else the drop view will fail */
	t = mvc_bind_table(sql, s, "dependencies_vw");
	t->system = 0;	/* make it non-system else the drop view will fail */
	pos += snprintf(buf + pos, bufsize - pos,
					"drop view sys.dependencies_vw cascade;\n" /* depends on sys.ids */
					"drop view sys.ids cascade;\n"
					"CREATE VIEW sys.ids (id, name, schema_id, table_id, table_name, obj_type, sys_table, system) AS\n"
					"SELECT id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'author' AS obj_type, 'sys.auths' AS sys_table, (name in ('public','sysadmin','monetdb','.snapshot')) AS system FROM sys.auths UNION ALL\n"
					"SELECT id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, ifthenelse(system, 'system schema', 'schema'), 'sys.schemas', system FROM sys.schemas UNION ALL\n"
					"SELECT t.id, name, t.schema_id, t.id as table_id, t.name as table_name, cast(lower(tt.table_type_name) as varchar(40)), 'sys.tables', t.system FROM sys.tables t left outer join sys.table_types tt on t.type = tt.table_type_id UNION ALL\n"
					"SELECT c.id, c.name, t.schema_id, c.table_id, t.name as table_name, ifthenelse(t.system, 'system column', 'column'), 'sys._columns', t.system FROM sys._columns c JOIN sys._tables t ON c.table_id = t.id UNION ALL\n"
					"SELECT c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'tmp._columns', t.system FROM tmp._columns c JOIN tmp._tables t ON c.table_id = t.id UNION ALL\n"
					"SELECT k.id, k.name, t.schema_id, k.table_id, t.name as table_name, ifthenelse(t.system, 'system key', 'key'), 'sys.keys', t.system FROM sys.keys k JOIN sys._tables t ON k.table_id = t.id UNION ALL\n"
					"SELECT k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'tmp.keys', t.system FROM tmp.keys k JOIN tmp._tables t ON k.table_id = t.id UNION ALL\n"
					"SELECT i.id, i.name, t.schema_id, i.table_id, t.name as table_name, ifthenelse(t.system, 'system index', 'index'), 'sys.idxs', t.system FROM sys.idxs i JOIN sys._tables t ON i.table_id = t.id UNION ALL\n"
					"SELECT i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index' , 'tmp.idxs', t.system FROM tmp.idxs i JOIN tmp._tables t ON i.table_id = t.id UNION ALL\n"
					"SELECT g.id, g.name, t.schema_id, g.table_id, t.name as table_name, ifthenelse(t.system, 'system trigger', 'trigger'), 'sys.triggers', t.system FROM sys.triggers g JOIN sys._tables t ON g.table_id = t.id UNION ALL\n"
					"SELECT g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'tmp.triggers', t.system FROM tmp.triggers g JOIN tmp._tables t ON g.table_id = t.id UNION ALL\n"
					"SELECT f.id, f.name, f.schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, cast(ifthenelse(f.system, 'system ', '') || lower(ft.function_type_keyword) as varchar(40)), 'sys.functions', f.system FROM sys.functions f left outer join sys.function_types ft on f.type = ft.function_type_id UNION ALL\n"
					"SELECT a.id, a.name, f.schema_id, a.func_id as table_id, f.name as table_name, cast(ifthenelse(f.system, 'system ', '') || lower(ft.function_type_keyword) || ' arg' as varchar(44)), 'sys.args', f.system FROM sys.args a JOIN sys.functions f ON a.func_id = f.id left outer join sys.function_types ft on f.type = ft.function_type_id UNION ALL\n"
					"SELECT id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'sequence', 'sys.sequences', false FROM sys.sequences UNION ALL\n"
					"SELECT o.id, o.name, pt.schema_id, pt.id, pt.name, 'partition of merge table', 'sys.objects', false FROM sys.objects o JOIN sys._tables pt ON o.sub = pt.id JOIN sys._tables mt ON o.nr = mt.id WHERE mt.type = 3 UNION ALL\n"
					"SELECT id, sqlname, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'type', 'sys.types', (sqlname in ('inet','json','url','uuid')) FROM sys.types WHERE id > 2000\n"
					" ORDER BY id;\n"
					"GRANT SELECT ON sys.ids TO PUBLIC;\n");
	pos += snprintf(buf + pos, bufsize - pos,
					"CREATE VIEW sys.dependencies_vw AS\n"
					"SELECT d.id, i1.obj_type, i1.name,\n"
					"       d.depend_id as used_by_id, i2.obj_type as used_by_obj_type, i2.name as used_by_name,\n"
					"       d.depend_type, dt.dependency_type_name\n"
					"  FROM sys.dependencies d\n"
					"  JOIN sys.ids i1 ON d.id = i1.id\n"
					"  JOIN sys.ids i2 ON d.depend_id = i2.id\n"
					"  JOIN sys.dependency_types dt ON d.depend_type = dt.dependency_type_id\n"
					" ORDER BY id, depend_id;\n"
					"GRANT SELECT ON sys.dependencies_vw TO PUBLIC;\n");
	pos += snprintf(buf + pos, bufsize - pos,
					"UPDATE sys._tables SET system = true WHERE name in ('ids', 'dependencies_vw') AND schema_id = 2000;\n");

	/* 52_describe.sql; but we need to drop most everything from
	 * 76_dump.sql first */
	t = mvc_bind_table(sql, s, "describe_comments");
	t->system = 0;
	t = mvc_bind_table(sql, s, "describe_constraints");
	t->system = 0;
	t = mvc_bind_table(sql, s, "describe_functions");
	t->system = 0;
	t = mvc_bind_table(sql, s, "describe_partition_tables");
	t->system = 0;
	t = mvc_bind_table(sql, s, "describe_privileges");
	t->system = 0;
	t = mvc_bind_table(sql, s, "describe_sequences");
	t->system = 0;
	t = mvc_bind_table(sql, s, "describe_tables");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_add_schemas_to_users");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_column_defaults");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_comments");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_create_roles");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_create_schemas");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_create_users");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_foreign_keys");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_functions");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_grant_user_privileges");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_indices");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_partition_tables");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_privileges");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_sequences");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_start_sequences");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_table_constraint_type");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_tables");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_triggers");
	t->system = 0;
	t = mvc_bind_table(sql, s, "dump_user_defined_types");
	t->system = 0;
	t = mvc_bind_table(sql, s, "fully_qualified_functions");
	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
					/* drop dependant stuff from 76_dump.sql */
					"drop function sys.dump_database(boolean) cascade;\n"
					"drop procedure sys.dump_table_data() cascade;\n"
					"drop procedure sys._dump_table_data(string, string) cascade;\n"
					"drop function sys.prepare_esc(string, string) cascade;\n"
					"drop function sys.esc(string) cascade;\n"
					"drop view sys.dump_privileges cascade;\n"
					"drop view sys.dump_user_defined_types cascade;\n"
					"drop view sys.dump_comments cascade;\n"
					"drop view sys.dump_triggers cascade;\n"
					"drop view sys.dump_tables cascade;\n"
					"drop view sys.dump_functions cascade;\n"
					"drop view sys.dump_start_sequences cascade;\n"
					"drop view sys.dump_sequences cascade;\n"
					"drop view sys.dump_partition_tables cascade;\n"
					"drop view sys.dump_foreign_keys cascade;\n"
					"drop view sys.dump_column_defaults cascade;\n"
					"drop view sys.dump_indices cascade;\n"
					"drop view sys.dump_table_constraint_type cascade;\n"
					"drop view sys.dump_grant_user_privileges cascade;\n"
					"drop view sys.dump_add_schemas_to_users cascade;\n"
					"drop view sys.dump_create_schemas cascade;\n"
					"drop view sys.dump_create_users cascade;\n"
					"drop view sys.dump_create_roles cascade;\n"

					"drop view sys.describe_functions cascade;\n"
					"drop view sys.describe_partition_tables cascade;\n"
					"drop view sys.describe_privileges cascade;\n"
					"drop view sys.fully_qualified_functions cascade;\n"
					"drop view sys.describe_comments cascade;\n"
					"drop view sys.describe_tables cascade;\n"
					"drop view sys.describe_sequences cascade;\n"
					"drop function sys.schema_guard(string, string, string) cascade;\n"
					"drop function sys.get_remote_table_expressions(string, string) cascade;\n"
					"drop function sys.get_merge_table_partition_expressions(int) cascade;\n"
					"drop view sys.describe_constraints cascade;\n"
					"drop function sys.alter_table(string, string) cascade;\n"
					"drop function sys.FQN(string, string) cascade;\n"
					"drop function sys.sq(string) cascade;\n");
	pos += snprintf(buf + pos, bufsize - pos,
					"CREATE FUNCTION sys.SQ (s STRING) RETURNS STRING BEGIN RETURN '''' || sys.replace(s,'''','''''') || ''''; END;\n"
					"CREATE FUNCTION sys.FQN(s STRING, t STRING) RETURNS STRING BEGIN RETURN '\"' || sys.replace(s,'\"','\"\"') || '\".\"' || sys.replace(t,'\"','\"\"') || '\"'; END;\n"
					"CREATE FUNCTION sys.schema_guard(sch STRING, nme STRING, stmt STRING) RETURNS STRING BEGIN\n"
					"RETURN\n"
					"    SELECT sys.replace_first(stmt, '(\\\\s*\"?' || sch ||  '\"?\\\\s*\\\\.|)\\\\s*\"?' || nme || '\"?\\\\s*', ' ' || sys.FQN(sch, nme) || ' ', 'imsx');\n"
					"END;\n"
					"CREATE VIEW sys.describe_constraints AS\n"
					"	SELECT\n"
					"		s.name sch,\n"
					"		t.name tbl,\n"
					"		kc.name col,\n"
					"		k.name con,\n"
					"		CASE k.type WHEN 0 THEN 'PRIMARY KEY' WHEN 1 THEN 'UNIQUE' END tpe\n"
					"	FROM sys.schemas s, sys._tables t, sys.objects kc, sys.keys k\n"
					"	WHERE kc.id = k.id\n"
					"		AND k.table_id = t.id\n"
					"		AND s.id = t.schema_id\n"
					"		AND t.system = FALSE\n"
					"		AND k.type in (0, 1);\n"
					"CREATE FUNCTION sys.get_merge_table_partition_expressions(tid INT) RETURNS STRING\n"
					"BEGIN\n"
					"	RETURN\n"
					"		SELECT\n"
					"			CASE WHEN tp.table_id IS NOT NULL THEN\n"
					"				' PARTITION BY ' ||\n"
					"				ifthenelse(bit_and(tp.type, 2) = 2, 'VALUES ', 'RANGE ') ||\n"
					"				CASE\n"
					"					WHEN bit_and(tp.type, 4) = 4\n"
					"					THEN 'ON ' || '(' || (SELECT sys.DQ(c.name) || ')' FROM sys.columns c WHERE c.id = tp.column_id)\n"
					"					ELSE 'USING ' || '(' || tp.expression || ')'\n"
					"				END\n"
					"			ELSE\n"
					"				''\n"
					"			END\n"
					"		FROM (VALUES (tid)) t(id) LEFT JOIN sys.table_partitions tp ON t.id = tp.table_id;\n"
					"END;\n"
					"CREATE FUNCTION sys.get_remote_table_expressions(s STRING, t STRING) RETURNS STRING BEGIN\n"
					"	RETURN SELECT ' ON ' || sys.SQ(uri) || ' WITH USER ' || sys.SQ(username) || ' ENCRYPTED PASSWORD ' || sys.SQ(\"hash\") FROM sys.remote_table_credentials(s ||'.' || t);\n"
					"END;\n"
					"CREATE VIEW sys.describe_tables AS\n"
					"	SELECT\n"
					"		t.id o,\n"
					"		s.name sch,\n"
					"		t.name tab,\n"
					"		ts.table_type_name typ,\n"
					"		(SELECT\n"
					"			' (' ||\n"
					"			GROUP_CONCAT(\n"
					"				sys.DQ(c.name) || ' ' ||\n"
					"				sys.describe_type(c.type, c.type_digits, c.type_scale) ||\n"
					"				ifthenelse(c.\"null\" = 'false', ' NOT NULL', '')\n"
					"			, ', ') || ')'\n"
					"		FROM sys._columns c\n"
					"		WHERE c.table_id = t.id) col,\n"
					"		CASE ts.table_type_name\n"
					"			WHEN 'REMOTE TABLE' THEN\n"
					"				sys.get_remote_table_expressions(s.name, t.name)\n"
					"			WHEN 'MERGE TABLE' THEN\n"
					"				sys.get_merge_table_partition_expressions(t.id)\n"
					"			WHEN 'VIEW' THEN\n"
					"				sys.schema_guard(s.name, t.name, t.query)\n"
					"			ELSE\n"
					"				''\n"
					"		END opt\n"
					"	FROM sys.schemas s, sys.table_types ts, sys.tables t\n"
					"	WHERE ts.table_type_name IN ('TABLE', 'VIEW', 'MERGE TABLE', 'REMOTE TABLE', 'REPLICA TABLE')\n"
					"		AND t.system = FALSE\n"
					"		AND s.id = t.schema_id\n"
					"		AND ts.table_type_id = t.type\n"
					"		AND s.name <> 'tmp';\n"
					"CREATE VIEW sys.fully_qualified_functions AS\n"
					"	WITH fqn(id, tpe, sig, num) AS\n"
					"	(\n"
					"		SELECT\n"
					"			f.id,\n"
					"			ft.function_type_keyword,\n"
					"			CASE WHEN a.type IS NULL THEN\n"
					"				sys.fqn(s.name, f.name) || '()'\n"
					"			ELSE\n"
					"				sys.fqn(s.name, f.name) || '(' || group_concat(sys.describe_type(a.type, a.type_digits, a.type_scale), ',') OVER (PARTITION BY f.id ORDER BY a.number)  || ')'\n"
					"			END,\n"
					"			a.number\n"
					"		FROM sys.schemas s, sys.function_types ft, sys.functions f LEFT JOIN sys.args a ON f.id = a.func_id\n"
					"		WHERE s.id= f.schema_id AND f.type = ft.function_type_id\n"
					"	)\n"
					"	SELECT\n"
					"		fqn1.id id,\n"
					"		fqn1.tpe tpe,\n"
					"		fqn1.sig nme\n"
					"	FROM\n"
					"		fqn fqn1 JOIN (SELECT id, max(num) FROM fqn GROUP BY id)  fqn2(id, num)\n"
					"		ON fqn1.id = fqn2.id AND (fqn1.num = fqn2.num OR fqn1.num IS NULL AND fqn2.num is NULL);\n"
					"CREATE VIEW sys.describe_comments AS\n"
					"		SELECT\n"
					"			o.id id,\n"
					"			o.tpe tpe,\n"
					"			o.nme fqn,\n"
					"			c.remark rem\n"
					"		FROM (\n"
					"			SELECT id, 'SCHEMA', sys.DQ(name) FROM sys.schemas\n"
					"			UNION ALL\n"
					"			SELECT t.id, ifthenelse(ts.table_type_name = 'VIEW', 'VIEW', 'TABLE'), sys.FQN(s.name, t.name)\n"
					"			FROM sys.schemas s JOIN sys.tables t ON s.id = t.schema_id JOIN sys.table_types ts ON t.type = ts.table_type_id\n"
					"			WHERE s.name <> 'tmp'\n"
					"			UNION ALL\n"
					"			SELECT c.id, 'COLUMN', sys.FQN(s.name, t.name) || '.' || sys.DQ(c.name) FROM sys.columns c, sys.tables t, sys.schemas s WHERE c.table_id = t.id AND t.schema_id = s.id\n"
					"			UNION ALL\n"
					"			SELECT idx.id, 'INDEX', sys.FQN(s.name, idx.name) FROM sys.idxs idx, sys._tables t, sys.schemas s WHERE idx.table_id = t.id AND t.schema_id = s.id\n"
					"			UNION ALL\n"
					"			SELECT seq.id, 'SEQUENCE', sys.FQN(s.name, seq.name) FROM sys.sequences seq, sys.schemas s WHERE seq.schema_id = s.id\n"
					"			UNION ALL\n"
					"			SELECT f.id, ft.function_type_keyword, qf.nme FROM sys.functions f, sys.function_types ft, sys.schemas s, sys.fully_qualified_functions qf WHERE f.type = ft.function_type_id AND f.schema_id = s.id AND qf.id = f.id\n"
					"			) AS o(id, tpe, nme)\n"
					"			JOIN sys.comments c ON c.id = o.id;\n"
					"CREATE VIEW sys.describe_privileges AS\n"
					"	SELECT\n"
					"		CASE\n"
					"			WHEN o.tpe IS NULL AND pc.privilege_code_name = 'SELECT' THEN --GLOBAL privileges: SELECT maps to COPY FROM\n"
					"				'COPY FROM'\n"
					"			WHEN o.tpe IS NULL AND pc.privilege_code_name = 'UPDATE' THEN --GLOBAL privileges: UPDATE maps to COPY INTO\n"
					"				'COPY INTO'\n"
					"			ELSE\n"
					"				o.nme\n"
					"		END o_nme,\n"
					"		coalesce(o.tpe, 'GLOBAL') o_tpe,\n"
					"		pc.privilege_code_name p_nme,\n"
					"		a.name a_nme,\n"
					"		g.name g_nme,\n"
					"		p.grantable grantable\n"
					"	FROM\n"
					"		sys.privileges p LEFT JOIN\n"
					"		(\n"
					"		SELECT t.id, s.name || '.' || t.name , 'TABLE'\n"
					"			from sys.schemas s, sys.tables t where s.id = t.schema_id\n"
					"		UNION ALL\n"
					"			SELECT c.id, s.name || '.' || t.name || '.' || c.name, 'COLUMN'\n"
					"			FROM sys.schemas s, sys.tables t, sys.columns c where s.id = t.schema_id AND t.id = c.table_id\n"
					"		UNION ALL\n"
					"			SELECT f.id, f.nme, f.tpe\n"
					"			FROM sys.fully_qualified_functions f\n"
					"		) o(id, nme, tpe) ON o.id = p.obj_id,\n"
					"		sys.privilege_codes pc,\n"
					"		auths a, auths g\n"
					"	WHERE\n"
					"		p.privileges = pc.privilege_code_id AND\n"
					"		p.auth_id = a.id AND\n"
					"		p.grantor = g.id;\n"
					"CREATE VIEW sys.describe_partition_tables AS\n"
					"	SELECT \n"
					"		m_sch,\n"
					"		m_tbl,\n"
					"		p_sch,\n"
					"		p_tbl,\n"
					"		CASE\n"
					"			WHEN p_raw_type IS NULL THEN 'READ ONLY'\n"
					"			WHEN (p_raw_type = 'VALUES' AND pvalues IS NULL) OR (p_raw_type = 'RANGE' AND minimum IS NULL AND maximum IS NULL AND with_nulls) THEN 'FOR NULLS'\n"
					"			ELSE p_raw_type\n"
					"		END AS tpe,\n"
					"		pvalues,\n"
					"		minimum,\n"
					"		maximum,\n"
					"		with_nulls\n"
					"	FROM \n"
					"    (WITH\n"
					"		tp(\"type\", table_id) AS\n"
					"		(SELECT ifthenelse((table_partitions.\"type\" & 2) = 2, 'VALUES', 'RANGE'), table_partitions.table_id FROM sys.table_partitions),\n"
					"		subq(m_tid, p_mid, \"type\", m_sch, m_tbl, p_sch, p_tbl) AS\n"
					"		(SELECT m_t.id, p_m.id, m_t.\"type\", m_s.name, m_t.name, p_s.name, p_m.name\n"
					"		FROM sys.schemas m_s, sys._tables m_t, sys.dependencies d, sys.schemas p_s, sys._tables p_m\n"
					"		WHERE m_t.\"type\" IN (3, 6)\n"
					"			AND m_t.schema_id = m_s.id\n"
					"			AND m_s.name <> 'tmp'\n"
					"			AND m_t.system = FALSE\n"
					"			AND m_t.id = d.depend_id\n"
					"			AND d.id = p_m.id\n"
					"			AND p_m.schema_id = p_s.id\n"
					"		ORDER BY m_t.id, p_m.id)\n"
					"	SELECT\n"
					"		subq.m_sch,\n"
					"		subq.m_tbl,\n"
					"		subq.p_sch,\n"
					"		subq.p_tbl,\n"
					"		tp.\"type\" AS p_raw_type,\n"
					"		CASE WHEN tp.\"type\" = 'VALUES'\n"
					"			THEN (SELECT GROUP_CONCAT(vp.value, ',') FROM sys.value_partitions vp WHERE vp.table_id = subq.p_mid)\n"
					"			ELSE NULL\n"
					"		END AS pvalues,\n"
					"		CASE WHEN tp.\"type\" = 'RANGE'\n"
					"			THEN (SELECT minimum FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
					"			ELSE NULL\n"
					"		END AS minimum,\n"
					"		CASE WHEN tp.\"type\" = 'RANGE'\n"
					"			THEN (SELECT maximum FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
					"			ELSE NULL\n"
					"		END AS maximum,\n"
					"		CASE WHEN tp.\"type\" = 'VALUES'\n"
					"			THEN EXISTS(SELECT vp.value FROM sys.value_partitions vp WHERE vp.table_id = subq.p_mid AND vp.value IS NULL)\n"
					"			ELSE (SELECT rp.with_nulls FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
					"		END AS with_nulls\n"
					"	FROM \n"
					"		subq LEFT OUTER JOIN tp\n"
					"		ON subq.m_tid = tp.table_id) AS tmp_pi;\n"
					"CREATE VIEW sys.describe_functions AS\n"
					"	WITH func_args_all(func_id, number, max_number, func_arg) AS\n"
					"	(\n"
					"		SELECT\n"
					"			func_id,\n"
					"			number,\n"
					"			max(number) OVER (PARTITION BY func_id ORDER BY number DESC),\n"
					"			group_concat(sys.dq(name) || ' ' || sys.describe_type(type, type_digits, type_scale),', ') OVER (PARTITION BY func_id ORDER BY number)\n"
					"		FROM sys.args\n"
					"		WHERE inout = 1\n"
					"	),\n"
					"	func_args(func_id, func_arg) AS\n"
					"	(\n"
					"		SELECT func_id, func_arg\n"
					"		FROM func_args_all\n"
					"		WHERE number = max_number\n"
					"	),\n"
					"	func_rets_all(func_id, number, max_number, func_ret, func_ret_type) AS\n"
					"	(\n"
					"		SELECT\n"
					"			func_id,\n"
					"			number,\n"
					"			max(number) OVER (PARTITION BY func_id ORDER BY number DESC),\n"
					"			group_concat(sys.dq(name) || ' ' || sys.describe_type(type, type_digits, type_scale),', ') OVER (PARTITION BY func_id ORDER BY number),\n"
					"			group_concat(sys.describe_type(type, type_digits, type_scale),', ') OVER (PARTITION BY func_id ORDER BY number)\n"
					"		FROM sys.args\n"
					"		WHERE inout = 0\n"
					"	),\n"
					"	func_rets(func_id, func_ret, func_ret_type) AS\n"
					"	(\n"
					"		SELECT\n"
					"			func_id,\n"
					"			func_ret,\n"
					"			func_ret_type\n"
					"		FROM func_rets_all\n"
					"		WHERE number = max_number\n"
					"	)\n"
					"	SELECT\n"
					"		f.id o,\n"
					"		s.name sch,\n"
					"		f.name fun,\n"
					"		CASE WHEN f.language IN (1, 2) THEN f.func ELSE 'CREATE ' || ft.function_type_keyword || ' ' || sys.FQN(s.name, f.name) || '(' || coalesce(fa.func_arg, '') || ')' || CASE WHEN f.type = 5 THEN ' RETURNS TABLE (' || coalesce(fr.func_ret, '') || ')' WHEN f.type IN (1,3) THEN ' RETURNS ' || fr.func_ret_type ELSE '' END || CASE WHEN fl.language_keyword IS NULL THEN '' ELSE ' LANGUAGE ' || fl.language_keyword END || ' ' || f.func END def\n"
					"	FROM sys.functions f\n"
					"		LEFT OUTER JOIN func_args fa ON fa.func_id = f.id\n"
					"		LEFT OUTER JOIN func_rets fr ON fr.func_id = f.id\n"
					"		JOIN sys.schemas s ON f.schema_id = s.id\n"
					"		JOIN sys.function_types ft ON f.type = ft.function_type_id\n"
					"		LEFT OUTER JOIN sys.function_languages fl ON f.language = fl.language_id\n"
					"	WHERE s.name <> 'tmp' AND NOT f.system;\n"
					"CREATE VIEW sys.describe_sequences AS\n"
					"	SELECT\n"
					"		s.name sch,\n"
					"		seq.name seq,\n"
					"		seq.\"start\" s,\n"
					"		get_value_for(s.name, seq.name) rs,\n"
					"		seq.\"minvalue\" mi,\n"
					"		seq.\"maxvalue\" ma,\n"
					"		seq.\"increment\" inc,\n"
					"		seq.\"cacheinc\" cache,\n"
					"		seq.\"cycle\" cycle,\n"
					"		CASE WHEN seq.\"minvalue\" = -9223372036854775807 AND seq.\"increment\" > 0 AND seq.\"start\" =  1 THEN TRUE ELSE FALSE END nomin,\n"
					"		CASE WHEN seq.\"maxvalue\" =  9223372036854775807 AND seq.\"increment\" < 0 AND seq.\"start\" = -1 THEN TRUE ELSE FALSE END nomax,\n"
					"		CASE\n"
					"			WHEN seq.\"minvalue\" = 0 AND seq.\"increment\" > 0 THEN NULL\n"
					"			WHEN seq.\"minvalue\" <> -9223372036854775807 THEN seq.\"minvalue\"\n"
					"			ELSE\n"
					"				CASE\n"
					"					WHEN seq.\"increment\" < 0  THEN NULL\n"
					"					ELSE CASE WHEN seq.\"start\" = 1 THEN NULL ELSE seq.\"maxvalue\" END\n"
					"				END\n"
					"		END rmi,\n"
					"		CASE\n"
					"			WHEN seq.\"maxvalue\" = 0 AND seq.\"increment\" < 0 THEN NULL\n"
					"			WHEN seq.\"maxvalue\" <> 9223372036854775807 THEN seq.\"maxvalue\"\n"
					"			ELSE\n"
					"				CASE\n"
					"					WHEN seq.\"increment\" > 0  THEN NULL\n"
					"					ELSE CASE WHEN seq.\"start\" = -1 THEN NULL ELSE seq.\"maxvalue\" END\n"
					"				END\n"
					"		END rma\n"
					"	FROM sys.sequences seq, sys.schemas s\n"
					"	WHERE s.id = seq.schema_id\n"
					"	AND s.name <> 'tmp'\n"
					"	ORDER BY s.name, seq.name;\n"
					"GRANT SELECT ON sys.describe_constraints TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_indices TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_column_defaults TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_foreign_keys TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_tables TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_triggers TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_comments TO PUBLIC;\n"
					"GRANT SELECT ON sys.fully_qualified_functions TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_privileges TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_user_defined_types TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_partition_tables TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_sequences TO PUBLIC;\n"
					"GRANT SELECT ON sys.describe_functions TO PUBLIC;\n");
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set system = true where system <> true and name in ('sq', 'fqn', 'get_merge_table_partition_expressions', 'get_remote_table_expressions', 'schema_guard') and schema_id = 2000 and type = %d;\n", F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys._tables set system = true where name in ('describe_constraints', 'describe_tables', 'fully_qualified_functions', 'describe_comments', 'describe_privileges', 'describe_partition_tables', 'describe_sequences', 'describe_functions') AND schema_id = 2000;\n");

	/* 76_dump.sql (most everything already dropped) */
	pos += snprintf(buf + pos, bufsize - pos,
					"CREATE VIEW sys.dump_create_roles AS\n"
					"  SELECT\n"
					"    'CREATE ROLE ' || sys.dq(name) || ';' stmt,\n"
					"    name user_name\n"
					"    FROM sys.auths\n"
					"   WHERE name NOT IN (SELECT name FROM sys.db_user_info)\n"
					"     AND grantor <> 0;\n"
					"CREATE VIEW sys.dump_create_users AS\n"
					"  SELECT\n"
					"    'CREATE USER ' || sys.dq(ui.name) || ' WITH ENCRYPTED PASSWORD ' ||\n"
					"      sys.sq(sys.password_hash(ui.name)) ||\n"
					"      ' NAME ' || sys.sq(ui.fullname) || ' SCHEMA sys' || ifthenelse(ui.schema_path = '\"sys\"', '', ' SCHEMA PATH ' || sys.sq(ui.schema_path)) || ';' stmt,\n"
					"    ui.name user_name\n"
					"    FROM sys.db_user_info ui, sys.schemas s\n"
					"   WHERE ui.default_schema = s.id\n"
					"     AND ui.name <> 'monetdb'\n"
					"     AND ui.name <> '.snapshot';\n"
					"CREATE VIEW sys.dump_create_schemas AS\n"
					"  SELECT\n"
					"    'CREATE SCHEMA ' || sys.dq(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || sys.dq(a.name), ' ') || ';' stmt,\n"
					"    s.name schema_name\n"
					"    FROM sys.schemas s, sys.auths a\n"
					"   WHERE s.authorization = a.id AND s.system = FALSE;\n"
					"CREATE VIEW sys.dump_add_schemas_to_users AS\n"
					"  SELECT\n"
					"    'ALTER USER ' || sys.dq(ui.name) || ' SET SCHEMA ' || sys.dq(s.name) || ';' stmt,\n"
					"    s.name schema_name,\n"
					"    ui.name user_name\n"
					"    FROM sys.db_user_info ui, sys.schemas s\n"
					"   WHERE ui.default_schema = s.id\n"
					"     AND ui.name <> 'monetdb'\n"
					"     AND ui.name <> '.snapshot'\n"
					"     AND s.name <> 'sys';\n"
					"CREATE VIEW sys.dump_grant_user_privileges AS\n"
					"  SELECT\n"
					"    'GRANT ' || sys.dq(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', sys.dq(a1.name)) || ';' stmt,\n"
					"    a2.name grantee,\n"
					"    a1.name grantor\n"
					"    FROM sys.auths a1, sys.auths a2, sys.user_role ur\n"
					"   WHERE a1.id = ur.login_id AND a2.id = ur.role_id;\n"
					"CREATE VIEW sys.dump_table_constraint_type AS\n"
					"  SELECT\n"
					"    'ALTER TABLE ' || sys.FQN(sch, tbl) || ' ADD CONSTRAINT ' || sys.DQ(con) || ' '||\n"
					"      tpe || ' (' || GROUP_CONCAT(sys.DQ(col), ', ') || ');' stmt,\n"
					"    sch schema_name,\n"
					"    tbl table_name,\n"
					"    con constraint_name\n"
					"    FROM sys.describe_constraints GROUP BY sch, tbl, con, tpe;\n"
					"CREATE VIEW sys.dump_table_grants AS\n"
					"  WITH table_grants (sname, tname, grantee, grants, grantor, grantable)\n"
					"  AS (SELECT s.name, t.name, a.name, sum(p.privileges), g.name, p.grantable\n"
					"	FROM sys.schemas s, sys.tables t, sys.auths a, sys.privileges p, sys.auths g\n"
					"       WHERE p.obj_id = t.id AND p.auth_id = a.id AND t.schema_id = s.id AND t.system = FALSE AND p.grantor = g.id\n"
					"       GROUP BY s.name, t.name, a.name, g.name, p.grantable\n"
					"       ORDER BY s.name, t.name, a.name, g.name, p.grantable)\n"
					"  SELECT\n"
					"    'GRANT ' || pc.privilege_code_name || ' ON TABLE ' || sys.FQN(sname, tname)\n"
					"      || ' TO ' || ifthenelse(grantee = 'public', 'PUBLIC', sys.dq(grantee))\n"
					"      || CASE WHEN grantable = 1 THEN ' WITH GRANT OPTION' ELSE '' END || ';' stmt,\n"
					"    sname schema_name,\n"
					"    tname table_name,\n"
					"    grantee\n"
					"    FROM table_grants LEFT OUTER JOIN sys.privilege_codes pc ON grants = pc.privilege_code_id;\n"
					"CREATE VIEW sys.dump_column_grants AS\n"
					"  SELECT\n"
					"    'GRANT ' || pc.privilege_code_name || '(' || sys.dq(c.name) || ') ON ' || sys.FQN(s.name, t.name)\n"
					"      || ' TO ' || ifthenelse(a.name = 'public', 'PUBLIC', sys.dq(a.name))\n"
					"      || CASE WHEN p.grantable = 1 THEN ' WITH GRANT OPTION' ELSE '' END || ';' stmt,\n"
					"    s.name schema_name,\n"
					"    t.name table_name,\n"
					"    c.name column_name,\n"
					"    a.name grantee\n"
					"    FROM sys.schemas s,\n"
					"	 sys.tables t,\n"
					"	 sys.columns c,\n"
					"	 sys.auths a,\n"
					"	 sys.privileges p,\n"
					"	 sys.auths g,\n"
					"	 sys.privilege_codes pc\n"
					"   WHERE p.obj_id = c.id\n"
					"     AND c.table_id = t.id\n"
					"     AND p.auth_id = a.id\n"
					"     AND t.schema_id = s.id\n"
					"     AND NOT t.system\n"
					"     AND p.grantor = g.id\n"
					"     AND p.privileges = pc.privilege_code_id\n"
					"   ORDER BY s.name, t.name, c.name, a.name, g.name, p.grantable;\n"
					"CREATE VIEW sys.dump_function_grants AS\n"
					"  WITH func_args_all(func_id, number, max_number, func_arg) AS\n"
					"  (SELECT a.func_id,\n"
					"	  a.number,\n"
					"	  max(a.number) OVER (PARTITION BY a.func_id ORDER BY a.number DESC),\n"
					"	  group_concat(sys.describe_type(a.type, a.type_digits, a.type_scale), ', ') OVER (PARTITION BY a.func_id ORDER BY a.number)\n"
					"     FROM sys.args a\n"
					"    WHERE a.inout = 1),\n"
					"  func_args(func_id, func_arg) AS\n"
					"  (SELECT func_id, func_arg FROM func_args_all WHERE number = max_number)\n"
					"  SELECT\n"
					"    'GRANT ' || pc.privilege_code_name || ' ON ' || ft.function_type_keyword || ' '\n"
					"      || sys.FQN(s.name, f.name) || '(' || coalesce(fa.func_arg, '') || ') TO '\n"
					"      || ifthenelse(a.name = 'public', 'PUBLIC', sys.dq(a.name))\n"
					"      || CASE WHEN p.grantable = 1 THEN ' WITH GRANT OPTION' ELSE '' END || ';' stmt,\n"
					"    s.name schema_name,\n"
					"    f.name function_name,\n"
					"    a.name grantee\n"
					"    FROM sys.schemas s,\n"
					"	 sys.functions f LEFT OUTER JOIN func_args fa ON f.id = fa.func_id,\n"
					"	 sys.auths a,\n"
					"	 sys.privileges p,\n"
					"	 sys.auths g,\n"
					"	 sys.function_types ft,\n"
					"	 sys.privilege_codes pc\n"
					"   WHERE s.id = f.schema_id\n"
					"     AND f.id = p.obj_id\n"
					"     AND p.auth_id = a.id\n"
					"     AND p.grantor = g.id\n"
					"     AND p.privileges = pc.privilege_code_id\n"
					"     AND f.type = ft.function_type_id\n"
					"     AND NOT f.system\n"
					"   ORDER BY s.name, f.name, a.name, g.name, p.grantable;\n"
					"CREATE VIEW sys.dump_indices AS\n"
					"  SELECT\n"
					"    'CREATE ' || tpe || ' ' || sys.DQ(ind) || ' ON ' || sys.FQN(sch, tbl) || '(' || GROUP_CONCAT(col) || ');' stmt,\n"
					"    sch schema_name,\n"
					"    tbl table_name,\n"
					"    ind index_name\n"
					"    FROM sys.describe_indices GROUP BY ind, tpe, sch, tbl;\n"
					"CREATE VIEW sys.dump_column_defaults AS\n"
					"  SELECT 'ALTER TABLE ' || sys.FQN(sch, tbl) || ' ALTER COLUMN ' || sys.DQ(col) || ' SET DEFAULT ' || def || ';' stmt,\n"
					"	 sch schema_name,\n"
					"	 tbl table_name,\n"
					"	 col column_name\n"
					"    FROM sys.describe_column_defaults;\n"
					"CREATE VIEW sys.dump_foreign_keys AS\n"
					"  SELECT\n"
					"    'ALTER TABLE ' || sys.FQN(fk_s, fk_t) || ' ADD CONSTRAINT ' || sys.DQ(fk) || ' ' ||\n"
					"      'FOREIGN KEY(' || GROUP_CONCAT(sys.DQ(fk_c), ',') ||') ' ||\n"
					"      'REFERENCES ' || sys.FQN(pk_s, pk_t) || '(' || GROUP_CONCAT(sys.DQ(pk_c), ',') || ') ' ||\n"
					"      'ON DELETE ' || on_delete || ' ON UPDATE ' || on_update ||\n"
					"      ';' stmt,\n"
					"    fk_s foreign_schema_name,\n"
					"    fk_t foreign_table_name,\n"
					"    pk_s primary_schema_name,\n"
					"    pk_t primary_table_name,\n"
					"    fk key_name\n"
					"    FROM sys.describe_foreign_keys GROUP BY fk_s, fk_t, pk_s, pk_t, fk, on_delete, on_update;\n"
					"CREATE VIEW sys.dump_partition_tables AS\n"
					"  SELECT\n"
					"    'ALTER TABLE ' || sys.FQN(m_sch, m_tbl) || ' ADD TABLE ' || sys.FQN(p_sch, p_tbl) ||\n"
					"      CASE \n"
					"      WHEN tpe = 'VALUES' THEN ' AS PARTITION IN (' || pvalues || ')'\n"
					"      WHEN tpe = 'RANGE' THEN ' AS PARTITION FROM ' || ifthenelse(minimum IS NOT NULL, sys.SQ(minimum), 'RANGE MINVALUE') || ' TO ' || ifthenelse(maximum IS NOT NULL, sys.SQ(maximum), 'RANGE MAXVALUE')\n"
					"      WHEN tpe = 'FOR NULLS' THEN ' AS PARTITION FOR NULL VALUES'\n"
					"      ELSE ''\n"
					"      END ||\n"
					"      CASE WHEN tpe in ('VALUES', 'RANGE') AND with_nulls THEN ' WITH NULL VALUES' ELSE '' END ||\n"
					"      ';' stmt,\n"
					"    m_sch merge_schema_name,\n"
					"    m_tbl merge_table_name,\n"
					"    p_sch partition_schema_name,\n"
					"    p_tbl partition_table_name\n"
					"    FROM sys.describe_partition_tables;\n"
					"CREATE VIEW sys.dump_sequences AS\n"
					"  SELECT\n"
					"    'CREATE SEQUENCE ' || sys.FQN(sch, seq) || ' AS BIGINT ' ||\n"
					"    CASE WHEN \"s\" <> 0 THEN 'START WITH ' ||  \"rs\" ELSE '' END ||\n"
					"    CASE WHEN \"inc\" <> 1 THEN ' INCREMENT BY ' ||  \"inc\" ELSE '' END ||\n"
					"    CASE\n"
					"      WHEN nomin THEN ' NO MINVALUE'\n"
					"      WHEN rmi IS NOT NULL THEN ' MINVALUE ' || rmi\n"
					"      ELSE ''\n"
					"    END ||\n"
					"    CASE\n"
					"      WHEN nomax THEN ' NO MAXVALUE'\n"
					"      WHEN rma IS NOT NULL THEN ' MAXVALUE ' || rma\n"
					"      ELSE ''\n"
					"    END ||\n"
					"    CASE WHEN \"cache\" <> 1 THEN ' CACHE ' ||  \"cache\" ELSE '' END ||\n"
					"    CASE WHEN \"cycle\" THEN ' CYCLE' ELSE '' END ||\n"
					"    ';' stmt,\n"
					"    sch schema_name,\n"
					"    seq seqname\n"
					"    FROM sys.describe_sequences;\n"
					"CREATE VIEW sys.dump_start_sequences AS\n"
					"  SELECT\n"
					"    'UPDATE sys.sequences seq SET start = ' || s ||\n"
					"      ' WHERE name = ' || sys.SQ(seq) ||\n"
					"      ' AND schema_id = (SELECT s.id FROM sys.schemas s WHERE s.name = ' || sys.SQ(sch) || ');' stmt,\n"
					"    sch schema_name,\n"
					"    seq sequence_name\n"
					"    FROM sys.describe_sequences;\n"
					"CREATE VIEW sys.dump_functions AS\n"
					"  SELECT f.o o, sys.schema_guard(f.sch, f.fun, f.def) stmt,\n"
					"	 f.sch schema_name,\n"
					"	 f.fun function_name\n"
					"    FROM sys.describe_functions f;\n"
					"CREATE VIEW sys.dump_tables AS\n"
					"  SELECT\n"
					"    t.o o,\n"
					"    CASE\n"
					"      WHEN t.typ <> 'VIEW' THEN\n"
					"      'CREATE ' || t.typ || ' ' || sys.FQN(t.sch, t.tab) || t.col || t.opt || ';'\n"
					"      ELSE\n"
					"      t.opt\n"
					"      END stmt,\n"
					"    t.sch schema_name,\n"
					"    t.tab table_name\n"
					"    FROM sys.describe_tables t;\n"
					"CREATE VIEW sys.dump_triggers AS\n"
					"  SELECT sys.schema_guard(sch, tab, def) stmt,\n"
					"	 sch schema_name,\n"
					"	 tab table_name,\n"
					"	 tri trigger_name\n"
					"    FROM sys.describe_triggers;\n"
					"CREATE VIEW sys.dump_comments AS\n"
					"  SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || sys.SQ(c.rem) || ';' stmt FROM sys.describe_comments c;\n"
					"CREATE VIEW sys.dump_user_defined_types AS\n"
					"  SELECT 'CREATE TYPE ' || sys.FQN(sch, sql_tpe) || ' EXTERNAL NAME ' || sys.DQ(ext_tpe) || ';' stmt,\n"
					"	 sch schema_name,\n"
					"	 sql_tpe type_name\n"
					"    FROM sys.describe_user_defined_types;\n"
					"CREATE FUNCTION sys.esc(s STRING) RETURNS STRING BEGIN RETURN '\"' || sys.replace(sys.replace(sys.replace(s,E'\\\\', E'\\\\\\\\'), E'\\n', E'\\\\n'), '\"', E'\\\\\"') || '\"'; END;\n"
					"CREATE FUNCTION sys.prepare_esc(s STRING, t STRING) RETURNS STRING\n"
					"BEGIN\n"
					"  RETURN\n"
					"    CASE\n"
					"    WHEN (t = 'varchar' OR t ='char' OR t = 'clob' OR t = 'json' OR t = 'geometry' OR t = 'url') THEN\n"
					"    'CASE WHEN ' || sys.DQ(s) || ' IS NULL THEN ''null'' ELSE ' || 'sys.esc(' || sys.DQ(s) || ')' || ' END'\n"
					"    ELSE\n"
					"    'CASE WHEN ' || sys.DQ(s) || ' IS NULL THEN ''null'' ELSE CAST(' || sys.DQ(s) || ' AS STRING) END'\n"
					"    END;\n"
					"END;\n"
					"CREATE PROCEDURE sys.dump_table_data(sch STRING, tbl STRING)\n"
					"BEGIN\n"
					"  DECLARE k INT;\n"
					"  SET k = (SELECT MIN(c.id) FROM sys.columns c, sys.tables t, sys.schemas s WHERE c.table_id = t.id AND t.name = tbl AND t.schema_id = s.id AND s.name = sch);\n"
					"  IF k IS NOT NULL THEN\n"
					"    DECLARE cname STRING;\n"
					"    DECLARE ctype STRING;\n"
					"    SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);\n"
					"    SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);\n"
					"    DECLARE COPY_INTO_STMT STRING;\n"
					"    DECLARE _cnt INT;\n"
					"    SET _cnt = (SELECT count FROM sys.storage(sch, tbl, cname));\n"
					"    IF _cnt > 0 THEN\n"
					"      SET COPY_INTO_STMT = 'COPY ' || _cnt || ' RECORDS INTO ' || sys.FQN(sch, tbl) || '(' || sys.DQ(cname);\n"
					"      DECLARE SELECT_DATA_STMT STRING;\n"
					"      SET SELECT_DATA_STMT = 'SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), ' || sys.prepare_esc(cname, ctype);\n"
					"      DECLARE M INT;\n"
					"      SET M = (SELECT MAX(c.id) FROM sys.columns c, sys.tables t WHERE c.table_id = t.id AND t.name = tbl);\n"
					"      WHILE (k < M) DO\n"
					"	SET k = (SELECT MIN(c.id) FROM sys.columns c, sys.tables t WHERE c.table_id = t.id AND t.name = tbl AND c.id > k);\n"
					"        SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);\n"
					"	SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);\n"
					"	SET COPY_INTO_STMT = (COPY_INTO_STMT || ', ' || sys.DQ(cname));\n"
					"	SET SELECT_DATA_STMT = SELECT_DATA_STMT || '|| ''|'' || ' || sys.prepare_esc(cname, ctype);\n"
					"      END WHILE;\n"
					"      SET COPY_INTO_STMT = (COPY_INTO_STMT || ') FROM STDIN USING DELIMITERS ''|'',E''\\\\n'',''\"'';');\n"
					"      SET SELECT_DATA_STMT = SELECT_DATA_STMT || ' FROM ' || sys.FQN(sch, tbl);\n"
					"      insert into sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, COPY_INTO_STMT);\n"
					"      CALL sys.EVAL('INSERT INTO sys.dump_statements ' || SELECT_DATA_STMT || ';');\n"
					"    END IF;\n"
					"  END IF;\n"
					"END;\n"
					"CREATE PROCEDURE sys.dump_table_data()\n"
					"BEGIN\n"
					"  DECLARE i INT;\n"
					"  SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);\n"
					"  IF i IS NOT NULL THEN\n"
					"    DECLARE M INT;\n"
					"    SET M = (SELECT MAX(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);\n"
					"    DECLARE sch STRING;\n"
					"    DECLARE tbl STRING;\n"
					"    WHILE i < M DO\n"
					"      set sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
					"      set tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
					"      CALL sys.dump_table_data(sch, tbl);\n"
					"      SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system AND t.id > i);\n"
					"    END WHILE;\n"
					"    set sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
					"    set tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
					"    CALL sys.dump_table_data(sch, tbl);\n"
					"  END IF;\n"
					"END;\n"
					"CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)\n"
					"BEGIN\n"
					"  SET SCHEMA sys;\n"
					"  TRUNCATE sys.dump_statements;\n"
					"  INSERT INTO sys.dump_statements VALUES (1, 'START TRANSACTION;');\n"
					"  INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'SET SCHEMA \"sys\";');\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_roles;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_users;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_schemas;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_user_defined_types;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_add_schemas_to_users;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_grant_user_privileges;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_sequences;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(ORDER BY stmts.o), stmts.s\n"
					"				    FROM (\n"
					"				      SELECT f.o, f.stmt FROM sys.dump_functions f\n"
					"				       UNION\n"
					"				      SELECT t.o, t.stmt FROM sys.dump_tables t\n"
					"				    ) AS stmts(o, s);\n"
					"  IF NOT DESCRIBE THEN\n"
					"    CALL sys.dump_table_data();\n"
					"  END IF;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_start_sequences;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_defaults;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_constraint_type;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_indices;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_foreign_keys;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_partition_tables;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_triggers;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_comments;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_grants;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_grants;\n"
					"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_function_grants;\n"
					"  INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'COMMIT;');\n"
					"  RETURN sys.dump_statements;\n"
					"END;\n");
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set system = true where system <> true and name in ('esc', 'prepare_esc') and schema_id = 2000 and type = %d;\n", F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set system = true where system <> true and name in ('dump_database') and schema_id = 2000 and type = %d;\n", F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set system = true where system <> true and name in ('dump_table_data') and schema_id = 2000 and type = %d;\n", F_PROC);
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys._tables set system = true where name in ('dump_create_roles', 'dump_create_users', 'dump_create_schemas', 'dump_add_schemas_to_users', 'dump_grant_user_privileges', 'dump_table_constraint_type', 'dump_table_grants', 'dump_column_grants', 'dump_function_grants', 'dump_indices', 'dump_column_defaults', 'dump_foreign_keys', 'dump_partition_tables', 'dump_sequences', 'dump_start_sequences', 'dump_functions', 'dump_tables', 'dump_triggers', 'dump_comments', 'dump_user_defined_types') AND schema_id = 2000;\n");

	/* 80_udf.sql (removed) */
	pos += snprintf(buf + pos, bufsize - pos,
					"drop function sys.reverse(string) cascade;\n"
					"drop all function sys.fuse cascade;\n");

	/* 26_sysmon.sql */
	pos += snprintf(buf + pos, bufsize - pos,
					"create procedure sys.vacuum(sname string, tname string, cname string)\n"
					"	external name sql.vacuum;\n"
					"create procedure sys.vacuum(sname string, tname string, cname string, interval int)\n"
					"	external name sql.vacuum;\n"
					"create procedure sys.stop_vacuum(sname string, tname string, cname string)\n"
					"	external name sql.stop_vacuum;\n");
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set system = true where system <> true and name in ('vacuum', 'stop_vacuum') and schema_id = 2000 and type = %d;\n", F_PROC);

	/* 10_sys_schema_extension.sql */
	pos += snprintf(buf + pos, bufsize - pos,
					"CREATE TABLE sys.fkey_actions (\n"
					"    action_id   SMALLINT NOT NULL PRIMARY KEY,\n"
					"    action_name VARCHAR(15) NOT NULL);\n"
					"INSERT INTO sys.fkey_actions (action_id, action_name) VALUES\n"
					"  (0, 'NO ACTION'),\n"
					"  (1, 'CASCADE'),\n"
					"  (2, 'RESTRICT'),\n"
					"  (3, 'SET NULL'),\n"
					"  (4, 'SET DEFAULT');\n"
					"ALTER TABLE sys.fkey_actions SET READ ONLY;\n"
					"GRANT SELECT ON sys.fkey_actions TO PUBLIC;\n"
					"CREATE VIEW sys.fkeys AS\n"
					"SELECT id, table_id, type, name, rkey, update_action_id, upd.action_name as update_action, delete_action_id, del.action_name as delete_action FROM (\n"
					" SELECT id, table_id, type, name, rkey, cast(((\"action\" >> 8) & 255) as smallint) as update_action_id, cast((\"action\" & 255) as smallint) AS delete_action_id FROM sys.keys WHERE type = 2\n"
					" UNION ALL\n"
					" SELECT id, table_id, type, name, rkey, cast(((\"action\" >> 8) & 255) as smallint) as update_action_id, cast((\"action\" & 255) as smallint) AS delete_action_id FROM tmp.keys WHERE type = 2\n"
					") AS fks\n"
					"JOIN sys.fkey_actions upd ON fks.update_action_id = upd.action_id\n"
					"JOIN sys.fkey_actions del ON fks.delete_action_id = del.action_id;\n"
					"GRANT SELECT ON sys.fkeys TO PUBLIC;\n"
					);
	pos += snprintf(buf + pos, bufsize - pos,
					"update sys._tables set system = true where name in ('fkey_actions', 'fkeys') AND schema_id = 2000;\n");

	/* recreate SQL functions that just need to be recompiled since the
	 * MAL functions's "unsafe" property was changed */
	sql_schema *lg = mvc_bind_schema(sql, "logging");
	t = mvc_bind_table(sql, lg, "compinfo");
	t->system = 0;
	t = mvc_bind_table(sql, s, "schemastorage");
	t->system = 0;
	t = mvc_bind_table(sql, s, "tablestorage");
	t->system = 0;
	t = mvc_bind_table(sql, s, "storage");
	t->system = 0;
	t = mvc_bind_table(sql, s, "rejects");
	t->system = 0;
	t = mvc_bind_table(sql, s, "queue");
	t->system = 0;
	t = mvc_bind_table(sql, s, "optimizers");
	t->system = 0;
	t = mvc_bind_table(sql, s, "prepared_statements_args");
	t->system = 0;
	t = mvc_bind_table(sql, s, "prepared_statements");
	t->system = 0;
	t = mvc_bind_table(sql, s, "sessions");
	t->system = 0;
	t = mvc_bind_table(sql, s, "querylog_calls");
	t->system = 0;
	t = mvc_bind_table(sql, s, "querylog_history");
	t->system = 0;
	t = mvc_bind_table(sql, s, "querylog_catalog");
	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
					"drop view logging.compinfo cascade;\n"
					"drop function logging.compinfo cascade;\n"
					"drop procedure sys.storagemodelinit() cascade;\n"
					"drop view sys.schemastorage cascade;\n"
					"drop view sys.tablestorage cascade;\n"
					"drop view sys.storage cascade;\n"
					"drop function sys.storage() cascade;\n"
					"drop function wlr.tick cascade;\n"
					"drop function wlr.clock cascade;\n"
					"drop function wlc.tick cascade;\n"
					"drop function wlc.clock cascade;\n"
					"drop function profiler.getlimit cascade;\n"
					"drop view sys.rejects cascade;\n"
					"drop function sys.rejects cascade;\n"
					"drop function sys.user_statistics cascade;\n"
					"drop view sys.queue cascade;\n"
					"drop function sys.queue cascade;\n"
					"drop function sys.debugflags cascade;\n"
					"drop function sys.bbp cascade;\n"
					"drop view sys.optimizers cascade;\n"
					"drop function sys.optimizers cascade;\n"
					"drop function sys.querycache cascade;\n"
					"drop function sys.optimizer_stats cascade;\n"
					"drop function sys.current_sessionid cascade;\n"
					"drop view sys.prepared_statements_args cascade;\n"
					"drop function sys.prepared_statements_args cascade;\n"
					"drop view sys.prepared_statements cascade;\n"
					"drop function sys.prepared_statements cascade;\n"
					"drop view sys.sessions cascade;\n"
					"drop function sys.sessions cascade;\n"
					"drop view sys.querylog_history cascade;\n"
					"drop view sys.querylog_calls cascade;\n"
					"drop function sys.querylog_calls cascade;\n"
					"drop view sys.querylog_catalog cascade;\n"
					"drop function sys.querylog_catalog cascade;\n"
					"create function sys.querylog_catalog()\n"
					"returns table(\n"
					" id oid,\n"
					" owner string,\n"
					" defined timestamp,\n"
					" query string,\n"
					" pipe string,\n"
					" \"plan\" string,\n"
					" mal int,\n"
					" optimize bigint\n"
					")\n"
					"external name sql.querylog_catalog;\n"
					"create view sys.querylog_catalog as select * from sys.querylog_catalog();\n"
					"create function sys.querylog_calls()\n"
					"returns table(\n"
					" id oid,\n"
					" \"start\" timestamp,\n"
					" \"stop\" timestamp,\n"
					" arguments string,\n"
					" tuples bigint,\n"
					" run bigint,\n"
					" ship bigint,\n"
					" cpu int,\n"
					" io int\n"
					")\n"
					"external name sql.querylog_calls;\n"
					"create view sys.querylog_calls as select * from sys.querylog_calls();\n"
					"create view sys.querylog_history as\n"
					"select qd.*, ql.\"start\",ql.\"stop\", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.io\n"
					"from sys.querylog_catalog() qd, sys.querylog_calls() ql\n"
					"where qd.id = ql.id and qd.owner = user;\n"
					"create function sys.sessions()\n"
					"returns table(\n"
					" \"sessionid\" int,\n"
					" \"username\" string,\n"
					" \"login\" timestamp,\n"
					" \"idle\" timestamp,\n"
					" \"optimizer\" string,\n"
					" \"sessiontimeout\" int,\n"
					" \"querytimeout\" int,\n"
					" \"workerlimit\" int,\n"
					" \"memorylimit\" int\n"
					")\n"
					"external name sql.sessions;\n"
					"create view sys.sessions as select * from sys.sessions();\n"
					"create function sys.prepared_statements()\n"
					"returns table(\n"
					" \"sessionid\" int,\n"
					" \"username\" string,\n"
					" \"statementid\" int,\n"
					" \"statement\" string,\n"
					" \"created\" timestamp\n"
					")\n"
					"external name sql.prepared_statements;\n"
					"grant execute on function sys.prepared_statements to public;\n"
					"create view sys.prepared_statements as select * from sys.prepared_statements();\n"
					"grant select on sys.prepared_statements to public;\n"
					"create function sys.prepared_statements_args()\n"
					"returns table(\n"
					" \"statementid\" int,\n"
					" \"type\" string,\n"
					" \"type_digits\" int,\n"
					" \"type_scale\" int,\n"
					" \"inout\" tinyint,\n"
					" \"number\" int,\n"
					" \"schema\" string,\n"
					" \"table\" string,\n"
					" \"column\" string\n"
					")\n"
					"external name sql.prepared_statements_args;\n"
					"grant execute on function sys.prepared_statements_args to public;\n"
					"create view sys.prepared_statements_args as select * from sys.prepared_statements_args();\n"
					"grant select on sys.prepared_statements_args to public;\n"
					"create function sys.current_sessionid() returns int\n"
					"external name clients.current_sessionid;\n"
					"grant execute on function sys.current_sessionid to public;\n"
					"create function sys.optimizer_stats()\n"
					" returns table (optname string, count int, timing bigint)\n"
					" external name inspect.optimizer_stats;\n"
					"create function sys.querycache()\n"
					" returns table (query string, count int)\n"
					" external name sql.dump_cache;\n"
					"create function sys.optimizers ()\n"
					" returns table (name string, def string, status string)\n"
					" external name sql.optimizers;\n"
					"create view sys.optimizers as select * from sys.optimizers();\n"
					"create function sys.bbp ()\n"
					" returns table (id int, name string,\n"
					" ttype string, count bigint, refcnt int, lrefcnt int,\n"
					" location string, heat int, dirty string,\n"
					" status string, kind string)\n"
					" external name bbp.get;\n"
					"create function sys.debugflags()\n"
					" returns table(flag string, val bool)\n"
					" external name mdb.\"getDebugFlags\";\n"
					"create function sys.queue()\n"
					"returns table(\n"
					" \"tag\" bigint,\n"
					" \"sessionid\" int,\n"
					" \"username\" string,\n"
					" \"started\" timestamp,\n"
					" \"status\" string,\n"
					" \"query\" string,\n"
					" \"finished\" timestamp,\n"
					" \"maxworkers\" int,\n"
					" \"footprint\" int\n"
					")\n"
					"external name sysmon.queue;\n"
					"grant execute on function sys.queue to public;\n"
					"create view sys.queue as select * from sys.queue();\n"
					"grant select on sys.queue to public;\n"
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
					"create function sys.rejects()\n"
					"returns table(\n"
					" rowid bigint,\n"
					" fldid int,\n"
					" \"message\" string,\n"
					" \"input\" string\n"
					")\n"
					"external name sql.copy_rejects;\n"
					"grant execute on function rejects to public;\n"
					"create view sys.rejects as select * from sys.rejects();\n"
					"create function profiler.getlimit() returns integer external name profiler.getlimit;\n"
					"create function wlc.clock() returns string\n"
					"external name wlc.\"getclock\";\n"
					"create function wlc.tick() returns bigint\n"
					"external name wlc.\"gettick\";\n"
					"create function wlr.clock() returns string\n"
					"external name wlr.\"getclock\";\n"
					"create function wlr.tick() returns bigint\n"
					"external name wlr.\"gettick\";\n"
					"create function sys.\"storage\"()\n"
					"returns table (\n"
					" \"schema\" varchar(1024),\n"
					" \"table\" varchar(1024),\n"
					" \"column\" varchar(1024),\n"
					" \"type\" varchar(1024),\n"
					" \"mode\" varchar(15),\n"
					" location varchar(1024),\n"
					" \"count\" bigint,\n"
					" typewidth int,\n"
					" columnsize bigint,\n"
					" heapsize bigint,\n"
					" hashes bigint,\n"
					" phash boolean,\n"
					" \"imprints\" bigint,\n"
					" sorted boolean,\n"
					" revsorted boolean,\n"
					" \"unique\" boolean,\n"
					" orderidx bigint\n"
					")\n"
					"external name sql.\"storage\";\n"
					"create view sys.\"storage\" as\n"
					"select * from sys.\"storage\"()\n"
					" where (\"schema\", \"table\") in (\n"
					" select sch.\"name\", tbl.\"name\"\n"
					" from sys.\"tables\" as tbl join sys.\"schemas\" as sch on tbl.schema_id = sch.id\n"
					" where tbl.\"system\" = false)\n"
					"order by \"schema\", \"table\", \"column\";\n"
					"create view sys.\"tablestorage\" as\n"
					"select \"schema\", \"table\",\n"
					" max(\"count\") as \"rowcount\",\n"
					" count(*) as \"storages\",\n"
					" sum(columnsize) as columnsize,\n"
					" sum(heapsize) as heapsize,\n"
					" sum(hashes) as hashsize,\n"
					" sum(\"imprints\") as imprintsize,\n"
					" sum(orderidx) as orderidxsize\n"
					" from sys.\"storage\"\n"
					"group by \"schema\", \"table\"\n"
					"order by \"schema\", \"table\";\n"
					"create view sys.\"schemastorage\" as\n"
					"select \"schema\",\n"
					" count(*) as \"storages\",\n"
					" sum(columnsize) as columnsize,\n"
					" sum(heapsize) as heapsize,\n"
					" sum(hashes) as hashsize,\n"
					" sum(\"imprints\") as imprintsize,\n"
					" sum(orderidx) as orderidxsize\n"
					" from sys.\"storage\"\n"
					"group by \"schema\"\n"
					"order by \"schema\";\n"
					"create procedure sys.storagemodelinit()\n"
					"begin\n"
					" delete from sys.storagemodelinput;\n"
					" insert into sys.storagemodelinput\n"
					" select \"schema\", \"table\", \"column\", \"type\", typewidth, \"count\",\n"
					" case when (\"unique\" or \"type\" in ('varchar', 'char', 'clob', 'json', 'url', 'blob', 'geometry', 'geometrya'))\n"
					" then \"count\" else 0 end,\n"
					" case when \"count\" > 0 and heapsize >= 8192 and \"type\" in ('varchar', 'char', 'clob', 'json', 'url')\n"
					" then cast((heapsize - 8192) / \"count\" as bigint)\n"
					" when \"count\" > 0 and heapsize >= 32 and \"type\" in ('blob', 'geometry', 'geometrya')\n"
					" then cast((heapsize - 32) / \"count\" as bigint)\n"
					" else typewidth end,\n"
					" false, case sorted when true then true else false end, \"unique\", true\n"
					" from sys.\"storage\";\n"
					" update sys.storagemodelinput\n"
					" set reference = true\n"
					" where (\"schema\", \"table\", \"column\") in (\n"
					" select fkschema.\"name\", fktable.\"name\", fkkeycol.\"name\"\n"
					" from sys.\"keys\" as fkkey,\n"
					" sys.\"objects\" as fkkeycol,\n"
					" sys.\"tables\" as fktable,\n"
					" sys.\"schemas\" as fkschema\n"
					" where fktable.\"id\" = fkkey.\"table_id\"\n"
					" and fkkey.\"id\" = fkkeycol.\"id\"\n"
					" and fkschema.\"id\" = fktable.\"schema_id\"\n"
					" and fkkey.\"rkey\" > -1 );\n"
					" update sys.storagemodelinput\n"
					" set isacolumn = false\n"
					" where (\"schema\", \"table\", \"column\") not in (\n"
					" select sch.\"name\", tbl.\"name\", col.\"name\"\n"
					" from sys.\"schemas\" as sch,\n"
					" sys.\"tables\" as tbl,\n"
					" sys.\"columns\" as col\n"
					" where sch.\"id\" = tbl.\"schema_id\"\n"
					" and tbl.\"id\" = col.\"table_id\");\n"
					"end;\n"
					"create function logging.compinfo()\n"
					"returns table(\n"
					" \"id\" int,\n"
					" \"component\" string,\n"
					" \"log_level\" string\n"
					")\n"
					"external name logging.compinfo;\n"
					"grant execute on function logging.compinfo to public;\n"
					"create view logging.compinfo as select * from logging.compinfo();\n"
					"grant select on logging.compinfo to public;\n"
					"update sys._tables set system = true where system <> true and schema_id = 2000 and name in ('schemastorage', 'tablestorage', 'storage', 'rejects', 'queue', 'optimizers', 'prepared_statements_args', 'prepared_statements', 'sessions', 'querylog_history', 'querylog_calls', 'querylog_catalog');\n"
					"update sys._tables set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'logging') and name = 'compinfo';\n"
					"update sys.functions set system = true where system <> true and schema_id = 2000 and name in ('storagemodelinit', 'storage', 'rejects', 'user_statistics', 'queue', 'debugflags', 'bbp', 'optimizers', 'querycache', 'optimizer_stats', 'current_sessionid', 'prepared_statements_args', 'prepared_statements', 'sessions', 'querylog_calls', 'querylog_catalog');\n"
					"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'logging') and name = 'compinfo';\n"
					"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'profiler') and name = 'getlimit';\n"
					"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'wlc') and name in ('clock', 'tick');\n"
					"update sys.functions set system = true where system <> true and schema_id = (select id from sys.schemas where name = 'wlr') and name in ('clock', 'tick');\n"
		);

	/* 99_system.sql */
	t = mvc_bind_table(sql, s, "systemfunctions");
	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.systemfunctions cascade;\n");

	/* 80_statistics.sql */
	t = mvc_bind_table(sql, s, "statistics");
	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
				"drop table sys.statistics cascade;\n"
				"drop procedure sys.analyze(int,bigint) cascade;\n"
				"drop procedure sys.analyze(int,bigint,string) cascade;\n"
				"drop procedure sys.analyze(int,bigint,string,string) cascade;\n"
				"drop procedure sys.analyze(int,bigint,string,string,string) cascade;\n"
				"create procedure sys.\"analyze\"()\n"
				"external name sql.\"analyze\";\n"
				"grant execute on procedure sys.\"analyze\"() to public;\n"
				"create procedure sys.\"analyze\"(\"sname\" varchar(1024))\n"
				"external name sql.\"analyze\";\n"
				"grant execute on procedure sys.\"analyze\"(varchar(1024)) to public;\n"
				"create procedure sys.\"analyze\"(\"sname\" varchar(1024), \"tname\" varchar(1024))\n"
				"external name sql.\"analyze\";\n"
				"grant execute on procedure sys.\"analyze\"(varchar(1024),varchar(1024)) to public;\n"
				"create procedure sys.\"analyze\"(\"sname\" varchar(1024), \"tname\" varchar(1024), \"cname\" varchar(1024))\n"
				"external name sql.\"analyze\";\n"
				"grant execute on procedure sys.\"analyze\"(varchar(1024),varchar(1024),varchar(1024)) to public;\n"
				"create function sys.\"statistics\"()\n"
				"returns table (\n"
				"	\"column_id\" integer,\n"
				"	\"schema\" varchar(1024),\n"
				"	\"table\" varchar(1024),\n"
				"	\"column\" varchar(1024),\n"
				"	\"type\" varchar(1024),\n"
				"	\"width\" integer,\n"
				"	\"count\" bigint,\n"
				"	\"unique\" boolean,\n"
				"	\"nils\" boolean,\n"
				"	\"minval\" string,\n"
				"	\"maxval\" string,\n"
				"	\"sorted\" boolean,\n"
				"	\"revsorted\" boolean\n"
				")\n"
				"external name sql.\"statistics\";\n"
				"grant execute on function sys.\"statistics\"() to public;\n"
				"create view sys.\"statistics\" as\n"
				"select * from sys.\"statistics\"()\n"
				"-- exclude system tables\n"
				"where (\"schema\", \"table\") in (\n"
				"	SELECT sch.\"name\", tbl.\"name\"\n"
				"	FROM sys.\"tables\" AS tbl JOIN sys.\"schemas\" AS sch ON tbl.schema_id = sch.id\n"
				"	WHERE tbl.\"system\" = FALSE)\n"
				"order by \"schema\", \"table\", \"column\";\n"
				"grant select on sys.\"statistics\" to public;\n"
				"create function sys.\"statistics\"(\"sname\" varchar(1024))\n"
				"returns table (\n"
				"	\"column_id\" integer,\n"
				"	\"schema\" varchar(1024),\n"
				"	\"table\" varchar(1024),\n"
				"	\"column\" varchar(1024),\n"
				"	\"type\" varchar(1024),\n"
				"	\"width\" integer,\n"
				"	\"count\" bigint,\n"
				"	\"unique\" boolean,\n"
				"	\"nils\" boolean,\n"
				"	\"minval\" string,\n"
				"	\"maxval\" string,\n"
				"	\"sorted\" boolean,\n"
				"	\"revsorted\" boolean\n"
				")\n"
				"external name sql.\"statistics\";\n"
				"grant execute on function sys.\"statistics\"(varchar(1024)) to public;\n"
				"create function sys.\"statistics\"(\"sname\" varchar(1024), \"tname\" varchar(1024))\n"
				"returns table (\n"
				"	\"column_id\" integer,\n"
				"	\"schema\" varchar(1024),\n"
				"	\"table\" varchar(1024),\n"
				"	\"column\" varchar(1024),\n"
				"	\"type\" varchar(1024),\n"
				"	\"width\" integer,\n"
				"	\"count\" bigint,\n"
				"	\"unique\" boolean,\n"
				"	\"nils\" boolean,\n"
				"	\"minval\" string,\n"
				"	\"maxval\" string,\n"
				"	\"sorted\" boolean,\n"
				"	\"revsorted\" boolean\n"
				")\n"
				"external name sql.\"statistics\";\n"
				"grant execute on function sys.\"statistics\"(varchar(1024),varchar(1024)) to public;\n"
				"create function sys.\"statistics\"(\"sname\" varchar(1024), \"tname\" varchar(1024), \"cname\" varchar(1024))\n"
				"returns table (\n"
				"	\"column_id\" integer,\n"
				"	\"schema\" varchar(1024),\n"
				"	\"table\" varchar(1024),\n"
				"	\"column\" varchar(1024),\n"
				"	\"type\" varchar(1024),\n"
				"	\"width\" integer,\n"
				"	\"count\" bigint,\n"
				"	\"unique\" boolean,\n"
				"	\"nils\" boolean,\n"
				"	\"minval\" string,\n"
				"	\"maxval\" string,\n"
				"	\"sorted\" boolean,\n"
				"	\"revsorted\" boolean\n"
				")\n"
				"external name sql.\"statistics\";\n"
				"grant execute on function sys.\"statistics\"(varchar(1024),varchar(1024),varchar(1024)) to public;\n"
				"update sys._tables set system = true where system <> true and schema_id = 2000 and name = 'statistics';\n"
				"update sys.functions set system = true where system <> true and schema_id = 2000 and name in ('analyze','statistics');\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	fflush(stdout);
	err = SQLstatementIntern(c, buf, "update", true, false, NULL);

	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_sep2022(Client c, mvc *sql)
{
	size_t bufsize = 65536, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);
	res_table *output;
	BAT *b;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* if sys.db_user_info does not have a column password, we need to
	 * add a bunch of columns */
	pos = snprintf(buf, bufsize,
				   "select id from sys._columns where table_id = (select id from sys._tables where name = 'db_user_info') and name = 'password';\n");
	if ((err = SQLstatementIntern(c, buf, "update", true, false, &output))) {
		GDKfree(buf);
		return err;
	}
	if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) == 0) {
		pos = 0;
		pos += snprintf(buf + pos, bufsize - pos,
						"alter table sys.db_user_info add column max_memory bigint;\n"
						"alter table sys.db_user_info add column max_workers int;\n"
						"alter table sys.db_user_info add column optimizer varchar(1024);\n"
						"alter table sys.db_user_info add column default_role int;\n"
						"alter table sys.db_user_info add column password varchar(256);\n");
		pos += snprintf(buf + pos, bufsize - pos,
						"update sys.db_user_info u set max_memory = 0, max_workers = 0, optimizer = 'default_pipe', default_role = (select id from sys.auths a where a.name = u.name);\n");
		int endprint = (int) pos;
		bat bid;
		BAT *u = NULL, *p = NULL, *d = NULL;
		if ((bid = BBPindex("M5system_auth_user")) == 0 ||
			(u = BATdescriptor(bid)) == NULL ||
			(bid = BBPindex("M5system_auth_passwd_v2")) == 0 ||
			(p = BATdescriptor(bid)) == NULL ||
			(bid = BBPindex("M5system_auth_deleted")) == 0 ||
			(d = BATdescriptor(bid)) == NULL) {
			if (u)
				BBPunfix(u->batCacheid);
			if (p)
				BBPunfix(p->batCacheid);
			if (d)
				BBPunfix(d->batCacheid);
			throw(SQL, __func__, INTERNAL_BAT_ACCESS);
		}
		BATiter ui = bat_iterator(u);
		BATiter pi = bat_iterator(p);
		for (oid i = 0; i < ui.count; i++) {
			if (BUNfnd(d, &i) == BUN_NONE) {
				const char *user = BUNtvar(ui, i);
				const char *pass = BUNtvar(pi, i);
				if (pos + 4 * (strlen(user) + strlen(pass)) + 64 >= bufsize) {
					char *nbuf = GDKrealloc(buf, bufsize + 65536);
					if (nbuf == NULL) {
						err = createException(SQL, __func__, MAL_MALLOC_FAIL);
						break;
					}
					buf = nbuf;
					bufsize += 65536;
				}
				pos += snprintf(buf + pos, bufsize - pos,
								"update sys.db_user_info set password = e'");
				for (const char *p = pass; *p; p++) {
					if (*p < '\040' || *p >= '\177') {
						/* control character or high bit set */
						pos += snprintf(buf + pos, bufsize - pos,
										"\\%03o", (unsigned char) *p);
					} else {
						if (*p == '\\' || *p == '\'')
							buf[pos++] = *p;
						buf[pos++] = *p;
					}
				}
				pos += snprintf(buf + pos, bufsize - pos,
								"' where name = e'");
				for (const char *p = user; *p; p++) {
					if (*p < '\040' || *p >= '\177') {
						/* control character or high bit set */
						pos += snprintf(buf + pos, bufsize - pos,
										"\\%03o", (unsigned char) *p);
					} else {
						if (*p == '\\' || *p == '\'')
							buf[pos++] = *p;
						buf[pos++] = *p;
					}
				}
				pos += snprintf(buf + pos, bufsize - pos,
								"';\n");
			}
		}
		if (err == MAL_SUCCEED) {
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%.*s-- and copying passwords\n\n", endprint, buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
		bat_iterator_end(&ui);
		bat_iterator_end(&pi);
		bat authbats[4];
		authbats[0] = 0;
		authbats[1] = u->batCacheid;
		authbats[2] = p->batCacheid;
		authbats[3] = d->batCacheid;
		if (err == MAL_SUCCEED &&
			(BATmode(u, true) != GDK_SUCCEED ||
			 BATmode(p, true) != GDK_SUCCEED ||
			 BATmode(d, true) != GDK_SUCCEED ||
			 BBPrename(u, NULL) != 0 ||
			 BBPrename(p, NULL) != 0 ||
			 BBPrename(d, NULL) != 0 ||
			 TMsubcommit_list(authbats, NULL, 4, getBBPlogno(), getBBPtransid()) != GDK_SUCCEED)) {
				fprintf(stderr, "Committing removal of old user/password BATs failed\n");
		}
		BBPunfix(u->batCacheid);
		BBPunfix(p->batCacheid);
		BBPunfix(d->batCacheid);

		if (err == MAL_SUCCEED) {
			sql_schema *s = mvc_bind_schema(sql, "sys");
			sql_table *t = mvc_bind_table(sql, s, "roles");
			t->system = 0;
			t = mvc_bind_table(sql, s, "users");
			t->system = 0;
			t = mvc_bind_table(sql, s, "dependency_schemas_on_users");
			t->system = 0;
			pos = 0;
			pos += snprintf(buf + pos, bufsize - pos,
							"drop view sys.dependency_schemas_on_users cascade;\n"
							"drop view sys.roles cascade;\n"
							"drop view sys.users cascade;\n"
							"drop function sys.db_users() cascade;\n"
							"CREATE VIEW sys.roles AS SELECT id, name, grantor FROM sys.auths a WHERE a.name NOT IN (SELECT u.name FROM sys.db_user_info u);\n"
							"GRANT SELECT ON sys.roles TO PUBLIC;\n"
							"CREATE VIEW sys.users AS SELECT name, fullname, default_schema, schema_path, max_memory, max_workers, optimizer, default_role FROM sys.db_user_info;\n"
							"GRANT SELECT ON sys.users TO PUBLIC;\n"
							"CREATE FUNCTION sys.db_users() RETURNS TABLE(name varchar(2048)) RETURN SELECT name FROM sys.db_user_info;\n"
							"CREATE VIEW sys.dependency_schemas_on_users AS\n"
							"SELECT s.id AS schema_id, s.name AS schema_name, u.name AS user_name, CAST(6 AS smallint) AS depend_type\n"
							" FROM sys.db_user_info AS u, sys.schemas AS s\n"
							" WHERE u.default_schema = s.id\n"
							" ORDER BY s.name, u.name;\n"
							"GRANT SELECT ON sys.dependency_schemas_on_users TO PUBLIC;\n"
							"update sys._tables set system = true where name in ('users', 'roles', 'dependency_schemas_on_users') AND schema_id = 2000;\n"
							"update sys.functions set system = true where system <> true and name in ('db_users') and schema_id = 2000 and type = %d;\n", F_UNION);
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
	}
	res_table_destroy(output);
	output = NULL;
	if (err != MAL_SUCCEED) {
		GDKfree(buf);
		return err;
	}

	/* if 'describe_partition_tables' system view doesn't use 'vals'
	 * CTE, re-create it; while we're at it, also update the sequence
	 * dumping code */
	pos = snprintf(buf, bufsize,
			"select 1 from sys.tables where schema_id = (select \"id\" from sys.schemas where \"name\" = 'sys') and \"name\" = 'describe_partition_tables' and \"query\" not like '%%vals%%';\n");
	if ((err = SQLstatementIntern(c, buf, "update", true, false, &output))) {
		GDKfree(buf);
		return err;
	}

	if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) > 0) {
		/* 52_describe.sql; but we need to drop dependencies from 76_dump.sql first */
		sql_schema *s = mvc_bind_schema(sql, "sys");
		sql_table *t = mvc_bind_table(sql, s, "describe_partition_tables");
		t->system = 0;
		t = mvc_bind_table(sql, s, "dump_partition_tables");
		t->system = 0;
		t = mvc_bind_table(sql, s, "dump_sequences");
		t->system = 0;
		t = mvc_bind_table(sql, s, "dump_start_sequences");
		t->system = 0;
		t = mvc_bind_table(sql, s, "describe_tables");
		t->system = 0;
		t = mvc_bind_table(sql, s, "dump_tables");
		t->system = 0;
		t = mvc_bind_table(sql, s, "dump_create_users");
		t->system = 0;
		t = mvc_bind_table(sql, s, "dump_functions");
		t->system = 0;
		t = mvc_bind_table(sql, s, "dump_triggers");
		t->system = 0;

		pos = 0;
		pos += snprintf(buf + pos, bufsize - pos,
			/* drop dependent stuff from 76_dump.sql */
			"drop function sys.dump_database(boolean) cascade;\n"
			"drop procedure sys.dump_table_data() cascade;\n"
			"drop procedure sys.dump_table_data(string, string) cascade;\n"
			"drop view sys.dump_partition_tables cascade;\n"
			"drop view sys.describe_partition_tables cascade;\n"
			"drop view sys.dump_sequences cascade;\n"
			"drop view sys.dump_start_sequences cascade;\n"
			"drop view sys.dump_tables cascade;\n"
			"drop view sys.describe_tables cascade;\n"
			"drop view sys.dump_create_users cascade;\n"
			"drop view sys.dump_functions cascade;\n"
			"drop view sys.dump_triggers cascade;\n"
			"drop function sys.schema_guard cascade;\n"
			"drop function sys.replace_first(string, string, string, string) cascade;\n");

		pos += snprintf(buf + pos, bufsize - pos,
			"CREATE FUNCTION sys.schema_guard(sch STRING, nme STRING, stmt STRING) RETURNS STRING BEGIN\n"
			"RETURN\n"
			" SELECT 'SET SCHEMA ' || sys.dq(sch) || '; ' || stmt;\n"
			"END;\n"
			"CREATE VIEW sys.dump_functions AS\n"
			" SELECT f.o o, sys.schema_guard(f.sch, f.fun, f.def) stmt,\n"
			" f.sch schema_name,\n"
			" f.fun function_name\n"
			" FROM sys.describe_functions f;\n"
			"CREATE VIEW sys.dump_triggers AS\n"
			" SELECT sys.schema_guard(sch, tab, def) stmt,\n"
			" sch schema_name,\n"
			" tab table_name,\n"
			" tri trigger_name\n"
			" FROM sys.describe_triggers;\n"
			"CREATE VIEW sys.describe_partition_tables AS\n"
			" SELECT\n"
			" m_sch,\n"
			" m_tbl,\n"
			" p_sch,\n"
			" p_tbl,\n"
			" CASE\n"
			" WHEN p_raw_type IS NULL THEN 'READ ONLY'\n"
			" WHEN (p_raw_type = 'VALUES' AND pvalues IS NULL) OR (p_raw_type = 'RANGE' AND minimum IS NULL AND maximum IS NULL AND with_nulls) THEN 'FOR NULLS'\n"
			" ELSE p_raw_type\n"
			" END AS tpe,\n"
			" pvalues,\n"
			" minimum,\n"
			" maximum,\n"
			" with_nulls\n"
			" FROM\n"
			" (WITH\n"
			" tp(\"type\", table_id) AS\n"
			" (SELECT ifthenelse((table_partitions.\"type\" & 2) = 2, 'VALUES', 'RANGE'), table_partitions.table_id FROM sys.table_partitions),\n"
			" subq(m_tid, p_mid, \"type\", m_sch, m_tbl, p_sch, p_tbl) AS\n"
			" (SELECT m_t.id, p_m.id, m_t.\"type\", m_s.name, m_t.name, p_s.name, p_m.name\n"
			" FROM sys.schemas m_s, sys._tables m_t, sys.dependencies d, sys.schemas p_s, sys._tables p_m\n"
			" WHERE m_t.\"type\" IN (3, 6)\n"
			" AND m_t.schema_id = m_s.id\n"
			" AND m_s.name <> 'tmp'\n"
			" AND m_t.system = FALSE\n"
			" AND m_t.id = d.depend_id\n"
			" AND d.id = p_m.id\n"
			" AND p_m.schema_id = p_s.id\n"
			" ORDER BY m_t.id, p_m.id),\n"
			" vals(id,vals) as\n"
			" (SELECT vp.table_id, GROUP_CONCAT(vp.value, ',') FROM sys.value_partitions vp GROUP BY vp.table_id)\n"
			" SELECT\n"
			" subq.m_sch,\n"
			" subq.m_tbl,\n"
			" subq.p_sch,\n"
			" subq.p_tbl,\n"
			" tp.\"type\" AS p_raw_type,\n"
			" CASE WHEN tp.\"type\" = 'VALUES'\n"
			" THEN (SELECT vals.vals FROM vals WHERE vals.id = subq.p_mid)\n"
			" ELSE NULL\n"
			" END AS pvalues,\n"
			" CASE WHEN tp.\"type\" = 'RANGE'\n"
			" THEN (SELECT minimum FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
			" ELSE NULL\n"
			" END AS minimum,\n"
			" CASE WHEN tp.\"type\" = 'RANGE'\n"
			" THEN (SELECT maximum FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
			" ELSE NULL\n"
			" END AS maximum,\n"
			" CASE WHEN tp.\"type\" = 'VALUES'\n"
			" THEN EXISTS(SELECT vp.value FROM sys.value_partitions vp WHERE vp.table_id = subq.p_mid AND vp.value IS NULL)\n"
			" ELSE (SELECT rp.with_nulls FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)\n"
			" END AS with_nulls\n"
			" FROM\n"
			" subq LEFT OUTER JOIN tp\n"
			" ON subq.m_tid = tp.table_id) AS tmp_pi;\n"
			"GRANT SELECT ON sys.describe_partition_tables TO PUBLIC;\n"
			"CREATE VIEW sys.dump_partition_tables AS\n"
			"SELECT\n"
			" 'ALTER TABLE ' || sys.FQN(m_sch, m_tbl) || ' ADD TABLE ' || sys.FQN(p_sch, p_tbl) ||\n"
			" CASE\n"
			" WHEN tpe = 'VALUES' THEN ' AS PARTITION IN (' || pvalues || ')'\n"
			" WHEN tpe = 'RANGE' THEN ' AS PARTITION FROM ' || ifthenelse(minimum IS NOT NULL, sys.SQ(minimum), 'RANGE MINVALUE') || ' TO ' || ifthenelse(maximum IS NOT NULL, sys.SQ(maximum), 'RANGE MAXVALUE')\n"
			" WHEN tpe = 'FOR NULLS' THEN ' AS PARTITION FOR NULL VALUES'\n"
			" ELSE '' --'READ ONLY'\n"
			" END ||\n"
			" CASE WHEN tpe in ('VALUES', 'RANGE') AND with_nulls THEN ' WITH NULL VALUES' ELSE '' END ||\n"
			" ';' stmt,\n"
			" m_sch merge_schema_name,\n"
			" m_tbl merge_table_name,\n"
			" p_sch partition_schema_name,\n"
			" p_tbl partition_table_name\n"
			" FROM sys.describe_partition_tables;\n"
			"CREATE VIEW sys.dump_sequences AS\n"
			" SELECT\n"
			" 'CREATE SEQUENCE ' || sys.FQN(sch, seq) || ' AS BIGINT;' stmt,\n"
			" sch schema_name,\n"
			" seq seqname\n"
			" FROM sys.describe_sequences;\n"
			"CREATE VIEW sys.dump_start_sequences AS\n"
			" SELECT 'ALTER SEQUENCE ' || sys.FQN(sch, seq) ||\n"
			" CASE WHEN s = 0 THEN '' ELSE ' RESTART WITH ' || rs END ||\n"
			" CASE WHEN inc = 1 THEN '' ELSE ' INCREMENT BY ' || inc END ||\n"
			" CASE WHEN nomin THEN ' NO MINVALUE' WHEN rmi IS NULL THEN '' ELSE ' MINVALUE ' || rmi END ||\n"
			" CASE WHEN nomax THEN ' NO MAXVALUE' WHEN rma IS NULL THEN '' ELSE ' MAXVALUE ' || rma END ||\n"
			" CASE WHEN \"cache\" = 1 THEN '' ELSE ' CACHE ' || \"cache\" END ||\n"
			" CASE WHEN \"cycle\" THEN '' ELSE ' NO' END || ' CYCLE;' stmt,\n"
			" sch schema_name,\n"
			" seq sequence_name\n"
			" FROM sys.describe_sequences;\n"
			"CREATE PROCEDURE sys.dump_table_data(sch STRING, tbl STRING)\n"
			"BEGIN\n"
			" DECLARE tid INT;\n"
			" SET tid = (SELECT MIN(t.id) FROM sys.tables t, sys.schemas s WHERE t.name = tbl AND t.schema_id = s.id AND s.name = sch);\n"
			" IF tid IS NOT NULL THEN\n"
			" DECLARE k INT;\n"
			" DECLARE m INT;\n"
			" SET k = (SELECT MIN(c.id) FROM sys.columns c WHERE c.table_id = tid);\n"
			" SET m = (SELECT MAX(c.id) FROM sys.columns c WHERE c.table_id = tid);\n"
			" IF k IS NOT NULL AND m IS NOT NULL THEN\n"
			" DECLARE cname STRING;\n"
			" DECLARE ctype STRING;\n"
			" DECLARE _cnt INT;\n"
			" SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);\n"
			" SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);\n"
			" SET _cnt = (SELECT count FROM sys.storage(sch, tbl, cname));\n"
			" IF _cnt > 0 THEN\n"
			" DECLARE COPY_INTO_STMT STRING;\n"
			" DECLARE SELECT_DATA_STMT STRING;\n"
			" SET COPY_INTO_STMT = 'COPY ' || _cnt || ' RECORDS INTO ' || sys.FQN(sch, tbl) || '(' || sys.DQ(cname);\n"
			" SET SELECT_DATA_STMT = 'SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), ' || sys.prepare_esc(cname, ctype);\n"
			" WHILE (k < m) DO\n"
			" SET k = (SELECT MIN(c.id) FROM sys.columns c WHERE c.table_id = tid AND c.id > k);\n"
			" SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);\n"
			" SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);\n"
			" SET COPY_INTO_STMT = (COPY_INTO_STMT || ', ' || sys.DQ(cname));\n"
			" SET SELECT_DATA_STMT = (SELECT_DATA_STMT || '|| ''|'' || ' || sys.prepare_esc(cname, ctype));\n"
			" END WHILE;\n"
			" SET COPY_INTO_STMT = (COPY_INTO_STMT || ') FROM STDIN USING DELIMITERS ''|'',E''\\\\n'',''\"'';');\n"
			" SET SELECT_DATA_STMT = (SELECT_DATA_STMT || ' FROM ' || sys.FQN(sch, tbl));\n"
			" INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, COPY_INTO_STMT);\n"
			" CALL sys.EVAL('INSERT INTO sys.dump_statements ' || SELECT_DATA_STMT || ';');\n"
			" END IF;\n"
			" END IF;\n"
			" END IF;\n"
			"END;\n"
			"CREATE PROCEDURE sys.dump_table_data()\n"
			"BEGIN\n"
			" DECLARE i INT;\n"
			" SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);\n"
			" IF i IS NOT NULL THEN\n"
			" DECLARE M INT;\n"
			" SET M = (SELECT MAX(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);\n"
			" DECLARE sch STRING;\n"
			" DECLARE tbl STRING;\n"
			" WHILE i IS NOT NULL AND i <= M DO\n"
			" SET sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
			" SET tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);\n"
			" CALL sys.dump_table_data(sch, tbl);\n"
			" SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system AND t.id > i);\n"
			" END WHILE;\n"
			" END IF;\n"
			"END;\n"
			"CREATE VIEW sys.dump_create_users AS\n"
			" SELECT\n"
			" 'CREATE USER ' || sys.dq(ui.name) || ' WITH ENCRYPTED PASSWORD ' ||\n"
			" sys.sq(sys.password_hash(ui.name)) ||\n"
			" ' NAME ' || sys.sq(ui.fullname) || ' SCHEMA sys' || ifthenelse(ui.schema_path = '\"sys\"', '', ' SCHEMA PATH ' || sys.sq(ui.schema_path)) || ';' stmt,\n"
			" ui.name user_name\n"
			" FROM sys.db_user_info ui, sys.schemas s\n"
			" WHERE ui.default_schema = s.id\n"
			" AND ui.name <> 'monetdb'\n"
			" AND ui.name <> '.snapshot';\n");

		pos += snprintf(buf + pos, bufsize - pos,
			"CREATE VIEW sys.describe_tables AS\n"
			" SELECT\n"
			" t.id o,\n"
			" s.name sch,\n"
			" t.name tab,\n"
			" ts.table_type_name typ,\n"
			" (SELECT\n"
			" ' (' ||\n"
			" GROUP_CONCAT(\n"
			" sys.DQ(c.name) || ' ' ||\n"
			" sys.describe_type(c.type, c.type_digits, c.type_scale) ||\n"
			" ifthenelse(c.\"null\" = 'false', ' NOT NULL', '')\n"
			" , ', ') || ')'\n"
			" FROM sys._columns c\n"
			" WHERE c.table_id = t.id) col,\n"
			" CASE ts.table_type_name\n"
			" WHEN 'REMOTE TABLE' THEN\n"
			" sys.get_remote_table_expressions(s.name, t.name)\n"
			" WHEN 'MERGE TABLE' THEN\n"
			" sys.get_merge_table_partition_expressions(t.id)\n"
			" WHEN 'VIEW' THEN\n"
			" sys.schema_guard(s.name, t.name, t.query)\n"
			" ELSE\n"
			" ''\n"
			" END opt\n"
			" FROM sys.schemas s, sys.table_types ts, sys.tables t\n"
			" WHERE ts.table_type_name IN ('TABLE', 'VIEW', 'MERGE TABLE', 'REMOTE TABLE', 'REPLICA TABLE', 'UNLOGGED TABLE')\n"
			" AND t.system = FALSE\n"
			" AND s.id = t.schema_id\n"
			" AND ts.table_type_id = t.type\n"
			" AND s.name <> 'tmp';\n"
			"GRANT SELECT ON sys.describe_tables TO PUBLIC;\n"
			"CREATE VIEW sys.dump_tables AS\n"
			" SELECT\n"
			" t.o o,\n"
			" CASE\n"
			" WHEN t.typ <> 'VIEW' THEN\n"
			" 'CREATE ' || t.typ || ' ' || sys.FQN(t.sch, t.tab) || t.col || t.opt || ';'\n"
			" ELSE\n"
			" t.opt\n"
			" END stmt,\n"
			" t.sch schema_name,\n"
			" t.tab table_name\n"
			" FROM sys.describe_tables t;\n"
			"CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)\n"
			"BEGIN\n"
			" SET SCHEMA sys;\n"
			" TRUNCATE sys.dump_statements;\n"
			" INSERT INTO sys.dump_statements VALUES (1, 'START TRANSACTION;');\n"
			" INSERT INTO sys.dump_statements VALUES (2, 'SET SCHEMA \"sys\";');\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_roles;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_users;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_schemas;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_user_defined_types;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_add_schemas_to_users;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_grant_user_privileges;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_sequences;\n"
			" --functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(ORDER BY stmts.o), stmts.s\n"
			" FROM (\n"
			" SELECT f.o, f.stmt FROM sys.dump_functions f\n"
			" UNION ALL\n"
			" SELECT t.o, t.stmt FROM sys.dump_tables t\n"
			" ) AS stmts(o, s);\n"
			" -- dump table data before adding constraints and fixing sequences\n"
			" IF NOT DESCRIBE THEN\n"
			" CALL sys.dump_table_data();\n"
			" END IF;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_start_sequences;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_defaults;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_constraint_type;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_indices;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_foreign_keys;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_partition_tables;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_triggers;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_comments;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_grants;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_grants;\n"
			" INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_function_grants;\n"
			" --TODO Improve performance of dump_table_data.\n"
			" --TODO loaders ,procedures, window and filter sys.functions.\n"
			" --TODO look into order dependent group_concat\n"
			" INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'COMMIT;');\n"
			" RETURN sys.dump_statements;\n"
			"END;\n");
		pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = true where name in ('describe_partition_tables', 'dump_partition_tables', 'dump_sequences', 'dump_start_sequences', 'describe_tables', 'dump_tables', 'dump_create_users', 'dump_functions', 'dump_triggers') AND schema_id = 2000;\n");
		pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and name in ('dump_table_data') and schema_id = 2000 and type = %d;\n", F_PROC);
		pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and name in ('dump_database') and schema_id = 2000 and type = %d;\n", F_UNION);
		pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and name in ('schema_guard') and schema_id = 2000 and type = %d;\n", F_FUNC);

		/* 12_url.sql */
		pos += snprintf(buf + pos, bufsize - pos,
						"CREATE function sys.url_extract_host(url string, no_www bool) RETURNS STRING\n"
						"EXTERNAL NAME url.\"extractURLHost\";\n"
						"GRANT EXECUTE ON FUNCTION url_extract_host(string, bool) TO PUBLIC;\n"
						"update sys.functions set system = true where system <> true and name = 'url_extract_host' and schema_id = 2000 and type = %d;\n", F_FUNC);

		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		fflush(stdout);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}
	res_table_destroy(output);
	output = NULL;
	if (err != MAL_SUCCEED) {
		GDKfree(buf);
		return err;
	}

	/* 10_sys_schema_extensions */
	/* if the keyword LOCKED is in the list of keywords, upgrade */
	pos = snprintf(buf, bufsize, "select keyword from sys.keywords where keyword = 'LOCKED';\n");
	assert(pos < bufsize);
	if ((err = SQLstatementIntern(c, buf, "update", true, false, &output)))
		goto bailout;
	if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) > 0) {
		pos = snprintf(buf, bufsize,
			"ALTER TABLE sys.keywords SET READ WRITE;\n"
			"DELETE FROM sys.keywords WHERE keyword IN ('LOCKED');\n");
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		fflush(stdout);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		if (err == MAL_SUCCEED) {
			pos = snprintf(buf, bufsize, "ALTER TABLE sys.keywords SET READ ONLY;\n");
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
	}
	res_table_destroy(output);
	output = NULL;
	if (err != MAL_SUCCEED) {
		GDKfree(buf);
		return err;
	}

	/* if the table type UNLOGGED TABLE is not in the list of table
	 * types, upgrade */
	pos = snprintf(buf, bufsize, "select table_type_name from sys.table_types where table_type_name = 'UNLOGGED TABLE';\n");
	assert(pos < bufsize);
	if ((err = SQLstatementIntern(c, buf, "update", true, false, &output)))
		goto bailout;
	if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) == 0) {
		pos = snprintf(buf, bufsize,
				"ALTER TABLE sys.table_types SET READ WRITE;\n"
				"INSERT INTO sys.table_types VALUES (7, 'UNLOGGED TABLE');\n");
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		fflush(stdout);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		if (err == MAL_SUCCEED) {
			pos = snprintf(buf, bufsize, "ALTER TABLE sys.table_types SET READ ONLY;\n");
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
	}
	res_table_destroy(output);
	output = NULL;

	/* 16_tracelog */
	pos = snprintf(buf, bufsize,
				   "select f.id "
				   "from sys.schemas s, "
						"sys.functions f, "
						"sys.auths a, "
						"sys.privileges p, "
						"sys.auths g, "
						"sys.function_types ft, "
						"sys.privilege_codes pc "
				   "where s.id = f.schema_id "
					 "and f.id = p.obj_id "
					 "and p.auth_id = a.id "
					 "and p.grantor = g.id "
					 "and p.privileges = pc.privilege_code_id "
					 "and f.type = ft.function_type_id "
					 "and s.name = 'sys' "
					 "and f.name = 'tracelog' "
					 "and ft.function_type_keyword = 'FUNCTION';\n");
	assert(pos < bufsize);
	if ((err = SQLstatementIntern(c, buf, "update", true, false, &output)))
		goto bailout;
	if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) == 0) {
		pos = snprintf(buf, bufsize,
					   "grant execute on function sys.tracelog to public;\n"
					   "grant select on sys.tracelog to public;\n");
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		fflush(stdout);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}
	res_table_destroy(output);
	output = NULL;
	if (err != MAL_SUCCEED) {
		GDKfree(buf);
		return err;
	}

bailout:
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
	char *err;
	sql_schema *s = mvc_bind_schema(m, "sys");

	if ((err = check_sys_tables(c, m, s)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

#ifdef HAVE_HGE
	sql_find_subtype(&tp, "hugeint", 0, 0);
	if (!sql_bind_func(m, s->base.name, "var_pop", &tp, NULL, F_AGGR, true)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_hugeint(c, m)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			return -1;
		}
	}
#endif

#ifdef HAVE_SHP
	if (backend_has_module(&(int){0}, "shp")) {
		sql_find_subtype(&tp, "varchar", 0, 0);
		if (!sql_bind_func(m, s->base.name, "shpattach", &tp, NULL, F_PROC, true)) {
			m->session->status = 0; /* if the function was not found clean the error */
			m->errstr[0] = '\0';
			if ((err = sql_update_shp(c)) != NULL) {
				TRC_CRITICAL(SQL_PARSER, "%s\n", err);
				freeException(err);
				return -1;
			}
		}
	}
#endif

	if ((err = sql_update_generator(c)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

	f = sql_bind_func_(m, s->base.name, "env", NULL, F_UNION, true);
	m->session->status = 0; /* if the function was not found clean the error */
	m->errstr[0] = '\0';
	sqlstore *store = m->session->tr->store;
	if (f && sql_privilege(m, ROLE_PUBLIC, f->func->base.id, PRIV_EXECUTE) != PRIV_EXECUTE) {
		sql_table *privs = find_sql_table(m->session->tr, s, "privileges");
		int pub = ROLE_PUBLIC, p = PRIV_EXECUTE, zero = 0, res;

		if ((res = store->table_api.table_insert(m->session->tr, privs, &f->func->base.id, &pub, &p, &zero, &zero)) != LOG_OK) {
			TRC_CRITICAL(SQL_PARSER, "Privilege creation during upgrade failed\n");
			return -1;
		}
	}

	if (sql_bind_func(m, s->base.name, "dependencies_schemas_on_users", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_owners_on_schemas", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_views", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_indexes", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_triggers", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_foreignkeys", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_tables_on_functions", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_views", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_keys", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_indexes", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_functions", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_columns_on_triggers", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_views_on_functions", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_views_on_triggers", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_functions_on_functions", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_functions_on_triggers", NULL, NULL, F_UNION, true)
	 && sql_bind_func(m, s->base.name, "dependencies_keys_on_foreignkeys", NULL, NULL, F_UNION, true)) {
		if ((err = sql_drop_functions_dependencies_Xs_on_Ys(c)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			return -1;
		}
	} else {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
	}

	sql_find_subtype(&tp, "varchar", 0, 0);
	if (!sql_bind_func3(m, s->base.name, "deltas", &tp, &tp, &tp, F_UNION, true)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_nov2019_missing_dependencies(c, m)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			return -1;
		}
		if ((err = sql_update_nov2019(c, m)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			return -1;
		}
	}

#ifdef HAVE_HGE
	sql_find_subtype(&tp, "hugeint", 0, 0);
	if (!sql_bind_func(m, s->base.name, "median_avg", &tp, NULL, F_AGGR, true)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_nov2019_sp1_hugeint(c, m)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			return -1;
		}
	}
#endif

	if (!sql_bind_func(m, s->base.name, "suspend_log_flushing", NULL, NULL, F_PROC, true)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_jun2020(c, m)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			return -1;
		}
	}

	if ((err = sql_update_jun2020_bam(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

#ifdef HAVE_HGE
	sql_find_subtype(&tp, "hugeint", 0, 0);
	if (!sql_bind_func(m, s->base.name, "covar_pop", &tp, &tp, F_AGGR, true)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_jun2020_sp1_hugeint(c)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			return -1;
		}
	}
#endif

	sql_find_subtype(&tp, "varchar", 0, 0);
	if (sql_bind_func(m, s->base.name, "lidarattach", &tp, NULL, F_PROC, true)) {
		if ((err = sql_update_oscar_lidar(c)) != NULL) {
			TRC_CRITICAL(SQL_PARSER, "%s\n", err);
			freeException(err);
			return -1;
		}
	} else {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
	}

	if ((err = sql_update_oscar(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

	if ((err = sql_update_oct2020(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

	if ((err = sql_update_oct2020_sp1(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

	if ((err = sql_update_jul2021(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

	if ((err = sql_update_jul2021_5(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

	if ((err = sql_update_jan2022(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

	if ((err = sql_update_sep2022(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		freeException(err);
		return -1;
	}

	return 0;
}
