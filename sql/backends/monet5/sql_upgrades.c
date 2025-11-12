/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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

		assert(t->d.impl); /* base types cannot be composite */
		pos += snprintf(buf + pos, bufsize - pos,
				"insert into sys.types values"
				" (%d, '%s', '%s', %u, %u, %d, %d, %d);\n",
				t->base.id, t->d.impl, t->base.name, t->digits,
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
						" %d, %d, %s, %s, %s, %d, %s, %s, %d);\n",
						func->base.id, func->base.name,
						sql_func_imp(func), sql_func_mod(func), (int) FUNC_LANG_INT,
						(int) func->type,
						boolnames[func->side_effect],
						boolnames[func->varres],
						boolnames[func->vararg],
						func->s ? func->s->base.id : s->base.id,
						boolnames[func->system],
						boolnames[func->semantics],
						func->order_required ? 2 : func->opt_order ? 1 : 0);
		if (func->res) {
			for (m = func->res->h; m; m = m->next, number++) {
				arg = m->data;
				pos += snprintf(buf + pos, bufsize - pos,
						"insert into sys.args"
						" values"
						" (%d, %d, 'res_%d',"
						" '%s', %u, %u, %d,"
						" %d, %d);\n",
						store_next_oid(store),
						func->base.id,
						number,
						arg->type.type->base.name,
						arg->type.digits,
						arg->type.scale,
						arg->inout, number, arg->type.multiset);
			}
		}
		for (m = func->ops->h; m; m = m->next, number++) {
			arg = m->data;
			if (arg->name)
				pos += snprintf(buf + pos, bufsize - pos,
						"insert into sys.args"
						" values"
						" (%d, %d, '%s', '%s',"
						" %u, %u, %d, %d, %d);\n",
						store_next_oid(store),
						func->base.id,
						arg->name,
						arg->type.type->base.name,
						arg->type.digits,
						arg->type.scale,
						arg->inout, number, arg->type.multiset);
			else
				pos += snprintf(buf + pos, bufsize - pos,
						"insert into sys.args"
						" values"
						" (%d, %d, 'arg_%d',"
						" '%s', %u, %u, %d,"
						" %d, %d);\n",
						store_next_oid(store),
						func->base.id,
						number,
						arg->type.type->base.name,
						arg->type.digits,
						arg->type.scale,
						arg->inout, number, arg->type.multiset);
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
		{ "quarter",           "quarter",       "date", F_FUNC, },
		{ "sys_update_tables", "update_tables", NULL,   F_PROC, },
		{ "length",            "nitems",        "blob", F_FUNC, },
		{ "isnull",            "isnil",         "void", F_FUNC, },
		{0},
	};
	char *err;

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
		sql_subfunc *f = sql_bind_func(m, s->base.name, tests[i].name, tpp, NULL, tests[i].ftype, true, true);
		if (f == NULL)
			throw(SQL, __func__, "cannot find procedure sys.%s(%s)", tests[i].name, tests[i].type ? tests[i].type : "");
		sqlid id = f->func->base.id;
		char buf[256];
		snprintf(buf, sizeof(buf),
				 "select id from sys.functions where name = '%s' and func = '%s' and schema_id = 2000;\n",
				 tests[i].name, tests[i].func);
		res_table *output = NULL;
		err = SQLstatementIntern(c, buf, "update", true, false, &output);
		if (err)
			return err;
		BAT *b;
		b = BATdescriptor(output->cols[0].b);
		res_table_destroy(output);
		if (b == NULL)
			throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if (BATcount(b) > 0) {
			BATiter bi = bat_iterator(b);
			needsystabfix = * (int *) BUNtloc(bi, 0) != id;
			bat_iterator_end(&bi);
		}
		BBPunfix(b->batCacheid);
		if (i == 0 && !needsystabfix) {
			snprintf(buf, sizeof(buf),
					 "select a.type from sys.functions f join sys.args a on f.id = a.func_id where f.name = 'quarter' and f.schema_id = 2000 and a.inout = 0 and a.type = 'int';\n");
			err = SQLstatementIntern(c, buf, "update", true, false, &output);
			if (err)
				return err;
			b = BATdescriptor(output->cols[0].b);
			res_table_destroy(output);
			if (b == NULL)
				throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			needsystabfix = BATcount(b) > 0;
			BBPunfix(b->batCacheid);
		}
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
			"external name generator.series;\n"
			"grant execute on function sys.generate_series(hugeint, hugeint) to public;\n"
			"grant execute on function sys.generate_series(hugeint, hugeint, hugeint) to public;\n");

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
			"create aggregate median(val HUGEINT) returns HUGEINT ORDERED\n"
			" external name \"aggr\".\"median\";\n"
			"GRANT EXECUTE ON AGGREGATE median(HUGEINT) TO PUBLIC;\n"
			"create aggregate quantile(val HUGEINT, q DOUBLE) returns HUGEINT ORDERED\n"
			" external name \"aggr\".\"quantile\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile(HUGEINT, DOUBLE) TO PUBLIC;\n"
			"create aggregate median_avg(val HUGEINT) returns DOUBLE ORDERED\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(HUGEINT) TO PUBLIC;\n"
			"create aggregate quantile_avg(val HUGEINT, q DOUBLE) returns DOUBLE ORDERED\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(HUGEINT, DOUBLE) TO PUBLIC;\n"
			"create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"corr\";\n"
			"GRANT EXECUTE ON AGGREGATE corr(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create aggregate median(val DECIMAL(38)) returns DECIMAL(38) ORDERED\n"
			" external name \"aggr\".\"median\";\n"
			"GRANT EXECUTE ON AGGREGATE median(DECIMAL(38)) TO PUBLIC;\n"
			"create aggregate median_avg(val DECIMAL(38)) returns DOUBLE ORDERED\n"
			" external name \"aggr\".\"median_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE median_avg(DECIMAL(38)) TO PUBLIC;\n"
			"create aggregate quantile(val DECIMAL(38), q DOUBLE) returns DECIMAL(38) ORDERED\n"
			" external name \"aggr\".\"quantile\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile(DECIMAL(38), DOUBLE) TO PUBLIC;\n"
			"create aggregate quantile_avg(val DECIMAL(38), q DOUBLE) returns DOUBLE ORDERED\n"
			" external name \"aggr\".\"quantile_avg\";\n"
			"GRANT EXECUTE ON AGGREGATE quantile_avg(DECIMAL(38), DOUBLE) TO PUBLIC;\n");

	/* 40_json_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function json.filter(js json, name hugeint)\n"
			"returns json external name json.filter;\n"
			"GRANT EXECUTE ON FUNCTION json.filter(json, hugeint) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where system <> true and name in ('generate_series') and schema_id = 2000 and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'covar_samp', 'var_pop', 'covar_pop', 'median', 'median_avg', 'quantile', 'quantile_avg', 'corr') and schema_id = 2000 and type = %d;\n"
			"update sys.functions set system = true where system <> true and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'covar_samp', 'var_pop', 'covar_pop', 'corr') and schema_id = 2000 and type = %d;\n"
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
sql_create_shp(Client c)
{
	//Create the new SHPload procedures
	static const char query[] = "create procedure SHPLoad(fname string, schemaname string, tablename string) external name shp.load;\n"
		"create procedure SHPLoad(fname string, tablename string) external name shp.load;\n"
		"update sys.functions set system = true where schema_id = 2000 and name in ('shpload');\n";
	printf("Running database upgrade commands:\n%s\n", query);
	fflush(stdout);
	return SQLstatementIntern(c, query, "update", true, false, NULL);
}
#endif

static str
sql_drop_shp(Client c)
{
	//Drop the old SHP procedures (upgrade from version before shpload upgrade)
	static const char query[] = "drop procedure if exists SHPattach(string) cascade;\n"
		"drop procedure if exists SHPload(integer) cascade;\n"
		"drop procedure if exists SHPload(integer, geometry) cascade;\n";
	printf("Running database upgrade commands:\n%s\n", query);
	fflush(stdout);
	return SQLstatementIntern(c, query, "update", true, false, NULL);
}

static str
sql_update_jan2022(Client c, mvc *sql)
{
	sql_subtype tp;
	size_t bufsize = 65536, pos = 0;
	char *buf = NULL, *err = NULL;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t;

	sql_find_subtype(&tp, "bigint", 0, 0);
	if (!sql_bind_func(sql, s->base.name, "epoch", &tp, NULL, F_FUNC, true, true)) {
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
					/* drop dependent stuff from 76_dump.sql */
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
					"drop function if exists wlr.tick cascade;\n"
					"drop function if exists wlr.clock cascade;\n"
					"drop function if exists wlc.tick cascade;\n"
					"drop function if exists wlc.clock cascade;\n"
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
					" returns table(\n"
					"  \"sessionid\" int,\n"
					"  \"username\" string,\n"
					"  \"login\" timestamp,\n"
					"  \"idle\" timestamp,\n"
					"  \"optimizer\" string,\n"
					"  \"sessiontimeout\" int,\n"
					"  \"querytimeout\" int,\n"
					"  \"workerlimit\" int,\n"
					"  \"memorylimit\" int,\n"
					"  \"language\" string,\n"
					"  \"peer\" string,\n"
					"  \"hostname\" string,\n"
					"  \"application\" string,\n"
					"  \"client\" string,\n"
					"  \"clientpid\" bigint,\n"
					"  \"remark\" string\n"
					" )\n"
					" external name sql.sessions;\n"
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
sql_update_sep2022(Client c, mvc *sql, sql_schema *s)
{
	size_t bufsize = 65536, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);
	res_table *output;
	BAT *b;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* if sys.db_user_info does not have a column password, we need to
	 * add a bunch of columns */
	sql_table *db_user_info = find_sql_table(sql->session->tr, s, "db_user_info");
	if (find_sql_column(db_user_info, "password") == NULL) {
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
			BBPreclaim(u);
			BBPreclaim(p);
			BBPreclaim(d);
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
			 TMsubcommit_list(authbats, NULL, 4, -1) != GDK_SUCCEED)) {
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

static str
sql_update_jun2023(Client c, mvc *sql, sql_schema *s)
{
	size_t bufsize = 65536, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);
	res_table *output;
	BAT *b;
	sql_subtype t1, t2;

	(void) sql;
	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* wlc/wlr support was removed */
	{
		sql_schema *wl = mvc_bind_schema(sql, "wlc");
		sql_schema *wr = mvc_bind_schema(sql, "wlr");
		if (wl != NULL || wr != NULL) {
			if (wl)
				wl->system = 0;
			if (wr)
				wr->system = 0;

			static const char query[] =
				"drop procedure if exists wlc.master() cascade;\n"
				"drop procedure if exists wlc.master(string) cascade;\n"
				"drop procedure if exists wlc.stop() cascade;\n"
				"drop procedure if exists wlc.flush() cascade;\n"
				"drop procedure if exists wlc.beat(int) cascade;\n"
				"drop function if exists wlc.clock() cascade;\n"
				"drop function if exists wlc.tick() cascade;\n"
				"drop procedure if exists wlr.master(string) cascade;\n"
				"drop procedure if exists wlr.stop() cascade;\n"
				"drop procedure if exists wlr.accept() cascade;\n"
				"drop procedure if exists wlr.replicate() cascade;\n"
				"drop procedure if exists wlr.replicate(timestamp) cascade;\n"
				"drop procedure if exists wlr.replicate(tinyint) cascade;\n"
				"drop procedure if exists wlr.replicate(smallint) cascade;\n"
				"drop procedure if exists wlr.replicate(integer) cascade;\n"
				"drop procedure if exists wlr.replicate(bigint) cascade;\n"
				"drop procedure if exists wlr.beat(integer) cascade;\n"
				"drop function if exists wlr.clock() cascade;\n"
				"drop function if exists wlr.tick() cascade;\n"
				"drop schema if exists wlc cascade;\n"
				"drop schema if exists wlr cascade;\n";
			printf("Running database upgrade commands:\n%s\n", query);
			fflush(stdout);
			err = SQLstatementIntern(c, query, "update", true, false, NULL);
		}
	}

	/* new function sys.regexp_replace */
	allocator *old_sa = sql->sa;
	if ((sql->sa = create_allocator(sql->pa, "MA_mvc", false)) != NULL) {
		list *l;
		if ((l = sa_list(sql->sa)) != NULL) {
			sql_subtype tp;
			sql_find_subtype(&tp, "varchar", 0, 0);
			list_append(l, &tp);
			list_append(l, &tp);
			list_append(l, &tp);
			list_append(l, &tp);
			if (!sql_bind_func_(sql, s->base.name, "regexp_replace", l, F_FUNC, true, true, false)) {
				pos = snprintf(buf, bufsize,
							   "create function sys.regexp_replace(ori string, pat string, rep string, flg string)\n"
							   "returns string external name pcre.replace;\n"
							   "grant execute on function regexp_replace(string, string, string, string) to public;\n"
							   "create function sys.regexp_replace(ori string, pat string, rep string)\n"
							   "returns string\n"
							   "begin\n"
							   " return sys.regexp_replace(ori, pat, rep, '');\n"
							   "end;\n"
							   "grant execute on function regexp_replace(string, string, string) to public;\n"
							   "update sys.functions set system = true where system <> true and name = 'regexp_replace' and schema_id = 2000 and type = %d;\n",
							   F_FUNC);
				assert(pos < bufsize);
				sql->session->status = 0;
				sql->errstr[0] = '\0';
				printf("Running database upgrade commands:\n%s\n", buf);
				fflush(stdout);
				err = SQLstatementIntern(c, buf, "update", true, false, NULL);
			}
			ma_destroy(sql->sa);
		}
	}
	sql->sa = old_sa;

	/* fixes for handling single quotes in strings so that we can run
	 * with raw_strings after having created a database without (and
	 * v.v.) */
	if ((err = SQLstatementIntern(c, "select id from sys.functions where name = 'dump_table_data' and schema_id = 2000 and func like '% R'')%';\n", "update", true, false, &output)) == NULL) {
		if (((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) == 0) || find_sql_table(sql->session->tr, s, "remote_user_info") == NULL) {
			sql_table *t;
			if ((t = mvc_bind_table(sql, s, "describe_tables")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "dump_create_users")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "dump_partition_tables")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "dump_comments")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "dump_tables")) != NULL)
				t->system = 0;
			pos = 0;
			pos += snprintf(buf + pos, bufsize - pos,
							"drop function if exists sys.dump_database(boolean) cascade;\n"
							"drop procedure if exists sys.dump_table_data() cascade;\n"
							"drop procedure if exists sys.dump_table_data(string, string) cascade;\n"
							"drop view if exists sys.dump_tables cascade;\n"
							"drop view if exists sys.dump_comments cascade;\n"
							"drop function if exists sys.prepare_esc(string, string) cascade;\n"
							"drop view if exists sys.dump_partition_tables cascade;\n"
							"drop view if exists sys.dump_create_users cascade;\n"
							"drop view if exists sys.describe_tables cascade;\n"
							"drop function if exists sys.get_remote_table_expressions(string, string) cascade;\n"
							"drop function if exists sys.remote_table_credentials(string) cascade;\n"
							"drop function if exists sys.sq(string) cascade;\n");
			if (find_sql_table(sql->session->tr, s, "remote_user_info") == NULL) {
				pos += snprintf(buf + pos, bufsize - pos,
								"create table sys.remote_user_info (table_id int, username varchar(1024), password varchar(256));\n"
								"create function sys.decypher (cypher string) returns string external name sql.decypher;\n"
								"update sys.functions set system = true where system <> true and name = 'decypher' and schema_id = 2000 and type = %d;\n"
								"update sys._tables set system = true where system <> true and name = 'remote_user_info' and schema_id = 2000;\n",
								F_FUNC);
			}
			pos += snprintf(buf + pos, bufsize - pos,
							"CREATE FUNCTION sys.SQ (s STRING) RETURNS STRING BEGIN RETURN '''' || sys.replace(s,'''','''''') || ''''; END;\n"
							"CREATE FUNCTION sys.get_remote_table_expressions(s STRING, t STRING) RETURNS STRING BEGIN\n"
							" RETURN SELECT ' ON ' || sys.SQ(tt.query) || ' WITH USER ' || sys.SQ(username) || ' ENCRYPTED PASSWORD ' || sys.SQ(sys.decypher(\"password\")) FROM sys.remote_user_info r, sys._tables tt, sys.schemas ss where tt.name = t and ss.name = s and tt.schema_id = ss.id and r.table_id = tt.id;\n"
							"END;\n"
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
							"CREATE VIEW sys.dump_create_users AS\n"
							" SELECT\n"
							" 'CREATE USER ' || sys.dq(ui.name) || ' WITH ENCRYPTED PASSWORD ' ||\n"
							" sys.sq(sys.password_hash(ui.name)) ||\n"
							" ' NAME ' || sys.sq(ui.fullname) || ' SCHEMA sys' || ifthenelse(ui.schema_path = '\"sys\"', '', ' SCHEMA PATH ' || sys.sq(ui.schema_path)) || ';' stmt,\n"
							" ui.name user_name\n"
							" FROM sys.db_user_info ui, sys.schemas s\n"
							" WHERE ui.default_schema = s.id\n"
							" AND ui.name <> 'monetdb'\n"
							" AND ui.name <> '.snapshot';\n"
							"CREATE VIEW sys.dump_partition_tables AS\n"
							" SELECT\n"
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
							"CREATE VIEW sys.dump_comments AS\n"
							" SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || sys.SQ(c.rem) || ';' stmt FROM sys.describe_comments c;\n"
							"CREATE FUNCTION sys.prepare_esc(s STRING, t STRING) RETURNS STRING\n"
							"BEGIN\n"
							" RETURN\n"
							" CASE\n"
							" WHEN (t = 'varchar' OR t ='char' OR t = 'clob' OR t = 'json' OR t = 'geometry' OR t = 'url') THEN\n"
							" 'CASE WHEN ' || sys.DQ(s) || ' IS NULL THEN ''null'' ELSE ' || 'sys.esc(' || sys.DQ(s) || ')' || ' END'\n"
							" ELSE\n"
							" 'CASE WHEN ' || sys.DQ(s) || ' IS NULL THEN ''null'' ELSE CAST(' || sys.DQ(s) || ' AS STRING) END'\n"
							" END;\n"
							"END;\n"
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
							" SET COPY_INTO_STMT = (COPY_INTO_STMT || R') FROM STDIN USING DELIMITERS ''|'',E''\\n'',''\"'';');\n"
							" SET SELECT_DATA_STMT = (SELECT_DATA_STMT || ' FROM ' || sys.FQN(sch, tbl));\n"
							" INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, COPY_INTO_STMT);\n"
							" CALL sys.EVAL('INSERT INTO sys.dump_statements ' || SELECT_DATA_STMT || ';');\n"
							" END IF;\n"
							" END IF;\n"
							" END IF;\n"
							" END;\n"
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
							"END;\n"
							"update sys.functions set system = true where system <> true and name in ('sq', 'get_remote_table_expressions', 'prepare_esc') and schema_id = 2000 and type = %d;\n"
							"update sys._tables set system = true where system <> true and name in ('describe_tables', 'dump_create_users', 'dump_partition_tables', 'dump_comments', 'dump_tables') and schema_id = 2000;\n"
							"update sys.functions set system = true where system <> true and name = 'dump_table_data' and schema_id = 2000 and type = %d;\n"
							"update sys.functions set system = true where system <> true and name = 'dump_database' and schema_id = 2000 and type = %d;\n"
							"GRANT SELECT ON sys.describe_tables TO PUBLIC;\n",
							F_FUNC, F_PROC, F_UNION);
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
		res_table_destroy(output);
		output = NULL;
	}

	/* Add new column 'function_id' to views
	 * sys.dependency_tables_on_functions and dependency_views_on_functions */
	{
		sql_table *t = find_sql_table(sql->session->tr, s, "dependency_tables_on_functions");
		if (t != NULL && find_sql_column(t, "function_id") == NULL) {
			t->system = 0;		/* sys.dependency_tables_on_functions */
			if ((t = mvc_bind_table(sql, s, "dependency_views_on_functions")) != NULL)
				t->system = 0;
			pos = 0;
			pos += snprintf(buf + pos, bufsize - pos,
							"drop view if exists sys.dependency_tables_on_functions cascade;\n"
							"drop view if exists sys.dependency_views_on_functions cascade;\n"
							"CREATE VIEW sys.dependency_tables_on_functions AS\n"
							"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name,"
							" f.id AS function_id, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type\n"
							"  FROM sys.functions AS f, sys.tables AS t, sys.dependencies AS dep\n"
							" WHERE t.id = dep.id AND f.id = dep.depend_id\n"
							"   AND dep.depend_type = 7 AND f.type <> 2 AND t.type NOT IN (1, 11)\n"
							" ORDER BY t.name, t.schema_id, f.name, f.id;\n"
							"GRANT SELECT ON sys.dependency_tables_on_functions TO PUBLIC;\n"
							"CREATE VIEW sys.dependency_views_on_functions AS\n"
							"SELECT v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name,"
							" f.id AS function_id, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type\n"
							"  FROM sys.functions AS f, sys.tables AS v, sys.dependencies AS dep\n"
							" WHERE v.id = dep.id AND f.id = dep.depend_id\n"
							"   AND dep.depend_type = 7 AND f.type <> 2 AND v.type IN (1, 11)\n"
							" ORDER BY v.name, v.schema_id, f.name, f.id;\n"
							"GRANT SELECT ON sys.dependency_views_on_functions TO PUBLIC;\n"
							"update sys._tables set system = true where system <> true and name in "
							"('dependency_tables_on_functions','dependency_views_on_functions') and schema_id = 2000;\n");
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			fflush(stdout);
			err = SQLstatementIntern(c, buf, "update", true, false, NULL);
		}
	}

	if (!sql_bind_func(sql, "sys", "database", NULL, NULL, F_FUNC, true, true)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
		pos = snprintf(buf, bufsize,
					   "create function sys.database ()\n"
					   "returns string\n"
					   "external name inspect.\"getDatabaseName\";\n"
					   "grant execute on function sys.database() to public;\n"
					   "update sys.functions set system = true where system <> true and name = 'database' and schema_id = 2000 and type = %d;\n",
					   (int) F_FUNC);
		printf("Running database upgrade commands:\n%s\n", buf);
		fflush(stdout);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}

	/* Add new sysadmin procedure calls: stop, pause and resume with two
	   arguments, first arg is query OID and second the user username that
	   the query in bound to. */
	sql_find_subtype(&t1, "bigint", 64, 0);
	sql_find_subtype(&t2, "varchar", 0, 0);
	if (!sql_bind_func(sql, "sys", "pause", &t1, &t2, F_PROC, true, true)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
		static const char query[] =
			"create function sys.queue(username string) returns table(\"tag\" bigint, \"sessionid\" int, \"username\" string, \"started\" timestamp, \"status\" string, \"query\" string, \"finished\" timestamp, \"maxworkers\" int, \"footprint\" int) external name sysmon.queue;\n"
			"create procedure sys.pause(tag bigint, username string) external name sysmon.pause;\n"
			"create procedure sys.resume(tag bigint, username string) external name sysmon.resume;\n"
			"create procedure sys.stop(tag bigint, username string) external name sysmon.stop;\n"
			"update sys.functions set system = true where system <> true and mod = 'sysmon' and name in ('stop', 'pause', 'resume', 'queue');\n";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
	}

	/* sys.settimeout and sys.setsession where removed */
	if (sql_bind_func(sql, "sys", "settimeout", &t1, NULL, F_PROC, true, true)) {
		static const char query[] =
			"drop procedure sys.settimeout(bigint) cascade;\n"
			"drop procedure sys.settimeout(bigint, bigint) cascade;\n"
			"drop procedure sys.setsession(bigint) cascade;\n";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
	} else {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
	}

	if (!sql_bind_func(sql, "sys", "jarowinkler", &t2, &t2, F_FUNC, true, true)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
		pos = snprintf(buf, bufsize,
					   "create function sys.levenshtein(x string, y string)\n"
					   "returns int external name txtsim.levenshtein;\n"
					   "grant execute on function levenshtein(string, string) to public;\n"
					   "create function sys.levenshtein(x string, y string, insdel int, rep int)\n"
					   "returns int external name txtsim.levenshtein;\n"
					   "grant execute on function levenshtein(string, string, int, int) to public;\n"
					   "create function sys.levenshtein(x string, y string, insdel int, rep int, trans int)\n"
					   "returns int external name txtsim.levenshtein;\n"
					   "grant execute on function levenshtein(string, string, int, int, int) to public;\n"
					   "create filter function sys.maxlevenshtein(x string, y string, k int)\n"
					   "external name txtsim.maxlevenshtein;\n"
					   "grant execute on filter function maxlevenshtein(string, string, int) to public;\n"
					   "create filter function sys.maxlevenshtein(x string, y string, k int, insdel int, rep int)\n"
					   "external name txtsim.maxlevenshtein;\n"
					   "grant execute on filter function maxlevenshtein(string, string, int, int, int) to public;\n"
					   "create function sys.jarowinkler(x string, y string)\n"
					   "returns double external name txtsim.jarowinkler;\n"
					   "grant execute on function jarowinkler(string, string) to public;\n"
					   "create filter function minjarowinkler(x string, y string, threshold double)\n"
					   "external name txtsim.minjarowinkler;\n"
					   "grant execute on filter function minjarowinkler(string, string, double) to public;\n"
					   "create function sys.dameraulevenshtein(x string, y string)\n"
					   "returns int external name txtsim.dameraulevenshtein;\n"
					   "grant execute on function dameraulevenshtein(string, string) to public;\n"
					   "create function sys.dameraulevenshtein(x string, y string, insdel int, rep int, trans int)\n"
					   "returns int external name txtsim.dameraulevenshtein;\n"
					   "grant execute on function dameraulevenshtein(string, string, int, int, int) to public;\n"

					   "create function sys.editdistance(x string, y string)\n"
					   "returns int external name txtsim.editdistance;\n"
					   "grant execute on function editdistance(string, string) to public;\n"
					   "create function sys.editdistance2(x string, y string)\n"
					   "returns int external name txtsim.editdistance2;\n"
					   "grant execute on function editdistance2(string, string) to public;\n"
					   "create function sys.soundex(x string)\n"
					   "returns string external name txtsim.soundex;\n"
					   "grant execute on function soundex(string) to public;\n"
					   "create function sys.difference(x string, y string)\n"
					   "returns int external name txtsim.stringdiff;\n"
					   "grant execute on function difference(string, string) to public;\n"
					   "create function sys.qgramnormalize(x string)\n"
					   "returns string external name txtsim.qgramnormalize;\n"
					   "grant execute on function qgramnormalize(string) to public;\n"

					   "create function asciify(x string)\n"
					   "returns string external name str.asciify;\n"
					   "grant execute on function asciify(string) to public;\n"
					   "create function sys.startswith(x string, y string)\n"
					   "returns boolean external name str.startswith;\n"
					   "grant execute on function startswith(string, string) to public;\n"
					   "create function sys.startswith(x string, y string, icase boolean)\n"
					   "returns boolean external name str.startswith;\n"
					   "grant execute on function startswith(string, string, boolean) to public;\n"
					   "create filter function sys.startswith(x string, y string)\n"
					   "external name str.startswith;\n"
					   "grant execute on filter function startswith(string, string) to public;\n"
					   "create filter function sys.startswith(x string, y string, icase boolean)\n"
					   "external name str.startswith;\n"
					   "grant execute on filter function startswith(string, string, boolean) to public;\n"
					   "create function sys.endswith(x string, y string)\n"
					   "returns boolean external name str.endswith;\n"
					   "grant execute on function endswith(string, string) to public;\n"
					   "create function sys.endswith(x string, y string, icase boolean)\n"
					   "returns boolean external name str.endswith;\n"
					   "grant execute on function endswith(string, string, boolean) to public;\n"
					   "create filter function sys.endswith(x string, y string)\n"
					   "external name str.endswith;\n"
					   "grant execute on filter function endswith(string, string) to public;\n"
					   "create filter function sys.endswith(x string, y string, icase boolean)\n"
					   "external name str.endswith;\n"
					   "grant execute on filter function endswith(string, string, boolean) to public;\n"
					   "create function sys.contains(x string, y string)\n"
					   "returns boolean external name str.contains;\n"
					   "grant execute on function contains(string, string) to public;\n"
					   "create function sys.contains(x string, y string, icase boolean)\n"
					   "returns boolean external name str.contains;\n"
					   "grant execute on function contains(string, string, boolean) to public;\n"
					   "create filter function sys.contains(x string, y string)\n"
					   "external name str.contains;\n"
					   "grant execute on filter function contains(string, string) to public;\n"
					   "create filter function sys.contains(x string, y string, icase boolean)\n"
					   "external name str.contains;\n"
					   "grant execute on filter function contains(string, string, boolean) to public;\n"

					   "update sys.functions set system = true where system <> true and name in ('levenshtein', 'dameraulevenshtein', 'jarowinkler', 'editdistance', 'editdistance2', 'soundex', 'difference', 'qgramnormalize') and schema_id = 2000 and type = %d;\n"
					   "update sys.functions set system = true where system <> true and name in ('maxlevenshtein', 'minjarowinkler') and schema_id = 2000 and type = %d;\n"
					   "update sys.functions set system = true where system <> true and name in ('asciify', 'startswith', 'endswith', 'contains') and schema_id = 2000 and type = %d;\n"
					   "update sys.functions set system = true where system <> true and name in ('startswith', 'endswith', 'contains') and schema_id = 2000 and type = %d;\n"

					   "delete from sys.triggers where name = 'system_update_tables' and table_id = 2067;\n",
					   F_FUNC, F_FILT, F_FUNC, F_FILT);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		fflush(stdout);
		err = SQLstatementIntern(c, buf, "update", true, false, NULL);
	}

	/* remote credentials where moved */
	sql_trans *tr = sql->session->tr;
	sqlstore *store = tr->store;
	sql_table *remote_user_info = find_sql_table(tr, s, "remote_user_info");
	sql_column *remote_user_info_id = find_sql_column(remote_user_info, "table_id");
	BAT *rt_key = NULL, *rt_username = NULL, *rt_pwhash = NULL, *rt_uri = NULL, *rt_deleted = NULL;
	if (!err && store->storage_api.count_col(tr, remote_user_info_id, 0) == 0 && BBPindex("M5system_auth_rt_key")) {

		rt_key = BATdescriptor(BBPindex("M5system_auth_rt_key"));
		rt_uri = BATdescriptor(BBPindex("M5system_auth_rt_uri"));
		rt_username = BATdescriptor(BBPindex("M5system_auth_rt_remoteuser"));
		rt_pwhash = BATdescriptor(BBPindex("M5system_auth_rt_hashedpwd"));
		rt_deleted = BATdescriptor(BBPindex("M5system_auth_rt_deleted"));
		if (rt_key == NULL || rt_username == NULL || rt_pwhash == NULL || rt_uri == NULL || rt_deleted == NULL) {
			/* cleanup remainders and continue or full stop ? */
			BBPreclaim(rt_key);
			BBPreclaim(rt_uri);
			BBPreclaim(rt_username);
			BBPreclaim(rt_pwhash);
			BBPreclaim(rt_deleted);
			throw(SQL, __func__, "cannot find M5system_auth bats");
		}

		BATiter ik = bat_iterator(rt_key);
		BATiter iu = bat_iterator(rt_username);
		BATiter ip = bat_iterator(rt_pwhash);
		for (oid p = 0; p < ik.count; p++) {
			if (BUNfnd(rt_deleted, &p) == BUN_NONE) {
				char *key = GDKstrdup(BUNtvar(ik, p));
				char *username = BUNtvar(iu, p);
				char *pwhash = BUNtvar(ip, p);

				if (!key) {
					bat_iterator_end(&ik);
					bat_iterator_end(&iu);
					bat_iterator_end(&ip);
					BBPunfix(rt_key->batCacheid);
					BBPunfix(rt_username->batCacheid);
					BBPunfix(rt_pwhash->batCacheid);
					BBPunfix(rt_deleted->batCacheid);
					throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				char *d = strchr(key, '.');
				/* . not found simply skip */
				if (d) {
					*d++ = '\0';
					sql_schema *s = find_sql_schema(tr, key);
					if (s) {
						sql_table *t = find_sql_table(tr, s, d);
						if (t && store->table_api.table_insert(tr, remote_user_info, &t->base.id, &username, &pwhash) != LOG_OK) {
							bat_iterator_end(&ik);
							bat_iterator_end(&iu);
							bat_iterator_end(&ip);
							BBPunfix(rt_key->batCacheid);
							BBPunfix(rt_username->batCacheid);
							BBPunfix(rt_pwhash->batCacheid);
							BBPunfix(rt_deleted->batCacheid);
							GDKfree(key);
							throw(SQL, __func__, "Failed to insert remote credentials during upgrade");
						}
					}
				}
				GDKfree(key);
			}
		}
		bat_iterator_end(&ik);
		bat_iterator_end(&iu);
		bat_iterator_end(&ip);
	}
	if (!err && rt_key) {
		bat rtauthbats[6];

		rtauthbats[0] = 0;
		rtauthbats[1] = rt_key->batCacheid;
		rtauthbats[2] = rt_uri->batCacheid;
		rtauthbats[3] = rt_username->batCacheid;
		rtauthbats[4] = rt_pwhash->batCacheid;
		rtauthbats[5] = rt_deleted->batCacheid;

		if (BATmode(rt_key, true) != GDK_SUCCEED ||
			BBPrename(rt_key, NULL) != 0 ||
			BATmode(rt_username, true) != GDK_SUCCEED ||
			BBPrename(rt_username, NULL) != 0 ||
			BATmode(rt_pwhash, true) != GDK_SUCCEED ||
			BBPrename(rt_pwhash, NULL) != 0 ||
			BATmode(rt_uri, true) != GDK_SUCCEED ||
			BBPrename(rt_uri, NULL) != 0 ||
			BATmode(rt_deleted, true) != GDK_SUCCEED ||
			BBPrename(rt_deleted, NULL) != 0 ||
			TMsubcommit_list(rtauthbats, NULL, 6, -1) != GDK_SUCCEED) {
			fprintf(stderr, "Committing removal of old remote user/password BATs failed\n");
		}
		BBPunfix(rt_key->batCacheid);
		BBPunfix(rt_username->batCacheid);
		BBPunfix(rt_pwhash->batCacheid);
		BBPunfix(rt_uri->batCacheid);
		BBPunfix(rt_deleted->batCacheid);
	}

	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jun2023_sp3(Client c, mvc *sql, sql_schema *s)
{
	(void)s;
	char *err = NULL;
	sql_subtype t1, t2;

	sql_find_subtype(&t1, "timestamp", 0, 0);
	sql_find_subtype(&t2, "varchar", 0, 0);

	if (!sql_bind_func(sql, "sys", "timestamp_to_str", &t1, &t2, F_FUNC, true, true)) {
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		char *query = GDKmalloc(512);
		if (query == NULL)
			throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

		snprintf(query, 512, "CREATE FUNCTION timestamp_to_str(d TIMESTAMP, format STRING) RETURNS STRING "
				 "EXTERNAL NAME mtime.\"timestamp_to_str\";\n"
				 "GRANT EXECUTE ON FUNCTION timestamp_to_str(TIMESTAMP, STRING) TO PUBLIC;\n"
				 "UPDATE sys.functions SET system = true WHERE system <> true AND name = 'timestamp_to_str' "
				 "AND schema_id = 2000 and type = %d;\n", F_FUNC);

		printf("Running database upgrade commands:\n%s\n", query);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		GDKfree(query);
	}

	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_dec2023_geom(Client c, mvc *sql, sql_schema *s)
{
	sql_subtype tp;
	char *err = NULL;

	/* the shp module was changed: drop the old stuff if it exists, only
	 * add the new stuff if the appropriate module is available */
	sql_find_subtype(&tp, "varchar", 0, 0);
	/* Drop old SHP procedures */
	if (sql_bind_func(sql, s->base.name, "shpattach", &tp, NULL, F_PROC, true, true)) {
		if ((err = sql_drop_shp(c)) != NULL)
			return err;
	} else {
		sql->session->status = 0; /* if the shpattach function was not found clean the error */
		sql->errstr[0] = '\0';
	}
#ifdef HAVE_GEOM
	if (backend_has_module(&(int){0}, "geom")) {
#ifdef HAVE_SHP
		if (backend_has_module(&(int){0}, "shp")) {
			/* if shpload with two varchar args does not exist, add the
			 * procedures */
			if (!sql_bind_func(sql, s->base.name, "shpload", &tp, &tp, F_PROC, true, true)) {
				sql->session->status = 0;
				sql->errstr[0] = '\0';
				if ((err = sql_create_shp(c)) != NULL)
					return err;
			}
		}
#endif
		sql_find_subtype(&tp, "geometry", 0, 0);
		if (!sql_bind_func(sql, s->base.name, "st_intersects_noindex", &tp, &tp, F_FILT, true, true)) {
			sql->session->status = 0;
			sql->errstr[0] = '\0';
			sql_table *t;
			if ((t = mvc_bind_table(sql, s, "geometry_columns")) != NULL)
				t->system = 0;
			static const char query[] =
				"drop function if exists sys.st_intersects(geometry, geometry) cascade;\n"
				"drop function if exists sys.st_dwithin(geometry, geometry, double) cascade;\n"
				"drop view if exists sys.geometry_columns cascade;\n"
				"drop function if exists sys.st_collect(geometry, geometry) cascade;\n"
				"drop aggregate if exists sys.st_collect(geometry) cascade;\n"
				"drop aggregate if exists sys.st_makeline(geometry) cascade;\n"
				"create view sys.geometry_columns as\n"
				" select cast(null as varchar(1)) as f_table_catalog,\n"
				"  s.name as f_table_schema,\n"
				"  t.name as f_table_name,\n"
				"  c.name as f_geometry_column,\n"
				"  cast(has_z(c.type_digits) + has_m(c.type_digits) +2 as integer) as coord_dimension,\n"
				"  c.type_scale as srid,\n"
				"  get_type(c.type_digits, 0) as geometry_type\n"
				" from sys.columns c, sys.tables t, sys.schemas s\n"
				" where c.table_id = t.id and t.schema_id = s.id\n"
				"  and c.type in (select sqlname from sys.types where systemname in ('wkb', 'wkba'));\n"
				"GRANT SELECT ON sys.geometry_columns TO PUBLIC;\n"
				"CREATE FUNCTION ST_Collect(geom1 Geometry, geom2 Geometry) RETURNS Geometry EXTERNAL NAME geom.\"Collect\";\n"
				"GRANT EXECUTE ON FUNCTION ST_Collect(Geometry, Geometry) TO PUBLIC;\n"
				"CREATE AGGREGATE ST_Collect(geom Geometry) RETURNS Geometry external name aggr.\"Collect\";\n"
				"GRANT EXECUTE ON AGGREGATE ST_Collect(Geometry) TO PUBLIC;\n"
				"CREATE FUNCTION ST_DistanceGeographic(geom1 Geometry, geom2 Geometry) RETURNS double EXTERNAL NAME geom.\"DistanceGeographic\";\n"
				"GRANT EXECUTE ON FUNCTION ST_DistanceGeographic(Geometry, Geometry) TO PUBLIC;\n"
				"CREATE FILTER FUNCTION ST_DWithinGeographic(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME geom.\"DWithinGeographic\";\n"
				"GRANT EXECUTE ON FILTER ST_DWithinGeographic(Geometry, Geometry, double) TO PUBLIC;\n"
				"CREATE FILTER FUNCTION ST_DWithin(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME rtree.\"DWithin\";\n"
				"GRANT EXECUTE ON FILTER ST_DWithin(Geometry, Geometry, double) TO PUBLIC;\n"
				"CREATE FILTER FUNCTION ST_DWithin_NoIndex(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME geom.\"DWithin_noindex\";\n"
				"GRANT EXECUTE ON FILTER ST_DWithin_NoIndex(Geometry, Geometry, double) TO PUBLIC;\n"
				"CREATE FUNCTION ST_DWithin2(geom1 Geometry, geom2 Geometry, bbox1 mbr, bbox2 mbr, dst double) RETURNS boolean EXTERNAL NAME geom.\"DWithin2\";\n"
				"GRANT EXECUTE ON FUNCTION ST_DWithin2(Geometry, Geometry, mbr, mbr, double) TO PUBLIC;\n"
				"CREATE FILTER FUNCTION ST_IntersectsGeographic(geom1 Geometry, geom2 Geometry) EXTERNAL NAME geom.\"IntersectsGeographic\";\n"
				"GRANT EXECUTE ON FILTER ST_IntersectsGeographic(Geometry, Geometry) TO PUBLIC;\n"
				"CREATE FILTER FUNCTION ST_Intersects(geom1 Geometry, geom2 Geometry) EXTERNAL NAME rtree.\"Intersects\";\n"
				"GRANT EXECUTE ON FILTER ST_Intersects(Geometry, Geometry) TO PUBLIC;\n"
				"CREATE FILTER FUNCTION ST_Intersects_NoIndex(geom1 Geometry, geom2 Geometry) EXTERNAL NAME geom.\"Intersects_noindex\";\n"
				"GRANT EXECUTE ON FILTER ST_Intersects_NoIndex(Geometry, Geometry) TO PUBLIC;\n"
				"CREATE AGGREGATE ST_MakeLine(geom Geometry) RETURNS Geometry external name aggr.\"MakeLine\";\n"
				"GRANT EXECUTE ON AGGREGATE ST_MakeLine(Geometry) TO PUBLIC;\n"
				"update sys.functions set system = true where system <> true and schema_id = 2000 and name in ('st_collect', 'st_distancegeographic', 'st_dwithingeographic', 'st_dwithin', 'st_dwithin_noindex', 'st_dwithin2', 'st_intersectsgeographic', 'st_intersects', 'st_intersects_noindex', 'st_makeline');\n"
				"update sys._tables set system = true where system <> true and schema_id = 2000 and name = 'geometry_columns';\n";
			printf("Running database upgrade commands:\n%s\n", query);
			fflush(stdout);
			err = SQLstatementIntern(c, query, "update", true, false, NULL);
		}
	}
#endif
	return err;
}

static str
sql_update_dec2023(Client c, mvc *sql, sql_schema *s)
{
	sql_subtype tp;
	sql_schema *info;
	char *err = NULL;
	res_table *output = NULL;

	sql_find_subtype(&tp, "varchar", 0, 0);
	if (sql_bind_func(sql, s->base.name, "similarity", &tp, &tp, F_FUNC, true, true)) {
		static const char query[] = "drop function sys.similarity(string, string) cascade;\n";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
	} else {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
	}

	if (mvc_bind_table(sql, s, "describe_accessible_tables") == NULL) {
		sql->session->status = 0; /* if the view was not found clean the error */
		sql->errstr[0] = '\0';
		static const char query[] =
		"CREATE VIEW sys.describe_accessible_tables AS\n"
		" SELECT\n"
		" schemas.name AS schema,\n"
		" tables.name  AS table,\n"
		" tt.table_type_name AS table_type,\n"
		" pc.privilege_code_name AS privs,\n"
		" p.privileges AS privs_code\n"
		" FROM privileges p\n"
		" JOIN sys.roles ON p.auth_id = roles.id\n"
		" JOIN sys.tables ON p.obj_id = tables.id\n"
		" JOIN sys.table_types tt ON tables.type = tt.table_type_id\n"
		" JOIN sys.schemas ON tables.schema_id = schemas.id\n"
		" JOIN sys.privilege_codes pc ON p.privileges = pc.privilege_code_id\n"
		" WHERE roles.name = current_role;\n"
		"GRANT SELECT ON sys.describe_accessible_tables TO PUBLIC;\n"
		"update sys._tables set system = true where system <> true and schema_id = 2000 and name = 'describe_accessible_tables';\n"

			/* PYTHON_MAP and PYTHON3_MAP have been removed */
			"alter table sys.function_languages set read write;\n"
			"delete from sys.function_languages where language_keyword like 'PYTHON%_MAP';\n"
			/* for these two, also see load_func() */
			"update sys.functions set language = language - 1 where language in (7, 11);\n"
			"update sys.functions set mod = 'pyapi3' where mod in ('pyapi', 'pyapi3map');\n"
			"commit;\n";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		if (err == MAL_SUCCEED) {
			static const char query2[] = "alter table sys.function_languages set read only;\n";
			printf("Running database upgrade commands:\n%s\n", query2);
			fflush(stdout);
			err = SQLstatementIntern(c, query2, "update", true, false, NULL);
		}
	}

	/* 52_describe.sql changes to update sys.describe_comments view */
	if ((err = SQLstatementIntern(c, "select id from sys.tables where name = 'describe_comments' and schema_id = 2000 and query like '% not t.system%';", "update", true, false, &output)) == NULL) {
		BAT *b;
		if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) == 0) {
			sql_table *t;
			/* set views internally to non-system to allow drop commands to succeed without error */
			if ((t = mvc_bind_table(sql, s, "describe_comments")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "dump_comments")) != NULL)
				t->system = 0;

			static const char cmds[] =
			"DROP FUNCTION IF EXISTS sys.dump_database(BOOLEAN) CASCADE;\n"
			"DROP VIEW IF EXISTS sys.dump_comments CASCADE;\n"
			"DROP VIEW IF EXISTS sys.describe_comments CASCADE;\n"
			"CREATE VIEW sys.describe_comments AS\n"
			"	SELECT o.id AS id, o.tpe AS tpe, o.nme AS fqn, cm.remark AS rem\n"
			"	FROM (\n"
			"		SELECT id, 'SCHEMA', sys.DQ(name) FROM sys.schemas WHERE NOT system\n"
			"		UNION ALL\n"
			"		SELECT t.id, ifthenelse(ts.table_type_name = 'VIEW', 'VIEW', 'TABLE'), sys.FQN(s.name, t.name)\n"
			"		  FROM sys.schemas s JOIN sys._tables t ON s.id = t.schema_id JOIN sys.table_types ts ON t.type = ts.table_type_id\n"
			"		 WHERE NOT t.system\n"
			"		UNION ALL\n"
			"		SELECT c.id, 'COLUMN', sys.FQN(s.name, t.name) || '.' || sys.DQ(c.name) FROM sys.columns c, sys._tables t, sys.schemas s WHERE NOT t.system AND c.table_id = t.id AND t.schema_id = s.id\n"
			"		UNION ALL\n"
			"		SELECT idx.id, 'INDEX', sys.FQN(s.name, idx.name) FROM sys.idxs idx, sys._tables t, sys.schemas s WHERE NOT t.system AND idx.table_id = t.id AND t.schema_id = s.id\n"
			"		UNION ALL\n"
			"		SELECT seq.id, 'SEQUENCE', sys.FQN(s.name, seq.name) FROM sys.sequences seq, sys.schemas s WHERE seq.schema_id = s.id\n"
			"		UNION ALL\n"
			"		SELECT f.id, ft.function_type_keyword, qf.nme FROM sys.functions f, sys.function_types ft, sys.schemas s, sys.fully_qualified_functions qf\n"
			"		 WHERE NOT f.system AND f.type = ft.function_type_id AND f.schema_id = s.id AND qf.id = f.id\n"
			"		) AS o(id, tpe, nme)\n"
			"	JOIN sys.comments cm ON cm.id = o.id;\n"
			"GRANT SELECT ON sys.describe_comments TO PUBLIC;\n"
			"CREATE VIEW sys.dump_comments AS\n"
			"  SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || sys.SQ(c.rem) || ';' stmt FROM sys.describe_comments c;\n"
			"CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)\n"
			"BEGIN\n"
			"  SET SCHEMA sys;\n"
			"  TRUNCATE sys.dump_statements;\n"
			"  INSERT INTO sys.dump_statements VALUES (1, 'START TRANSACTION;');\n"
			"  INSERT INTO sys.dump_statements VALUES (2, 'SET SCHEMA \"sys\";');\n"
			"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_roles;\n"
			"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_users;\n"
			"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_schemas;\n"
			"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_user_defined_types;\n"
			"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_add_schemas_to_users;\n"
			"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_grant_user_privileges;\n"
			"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_sequences;\n"
			"  --functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.\n"
			"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(ORDER BY stmts.o), stmts.s\n"
			"				    FROM (\n"
			"				      SELECT f.o, f.stmt FROM sys.dump_functions f\n"
			"				       UNION ALL\n"
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
			"END;\n"
			"update sys._tables set system = true where schema_id = 2000 and name in ('describe_comments','dump_comments');\n"
			"update sys.functions set system = true where system <> true and schema_id = 2000 and name = 'dump_database' and type = 5;\n";

			printf("Running database upgrade commands:\n%s\n", cmds);
			fflush(stdout);
			err = SQLstatementIntern(c, cmds, "update", true, false, NULL);
		}
		res_table_destroy(output);
		output = NULL;
	}

	/* 52_describe.sql New function sys.sql_datatype(mtype varchar(999), digits integer, tscale integer, nameonly boolean, shortname boolean) */
	allocator *old_sa = sql->sa;
	if ((sql->sa = create_allocator(sql->pa, "MA_mvc", false)) != NULL) {
		list *l;
		if ((l = sa_list(sql->sa)) != NULL) {
			sql_subtype t1, t2;
			sql_find_subtype(&t1, "int", 0, 0);
			sql_find_subtype(&t2, "boolean", 0, 0);
			list_append(l, &tp);
			list_append(l, &t1);
			list_append(l, &t1);
			list_append(l, &t2);
			list_append(l, &t2);
			if (!sql_bind_func_(sql, s->base.name, "sql_datatype", l, F_FUNC, true, true, false)) {
				static const char cmds[] =
				"CREATE FUNCTION sys.sql_datatype(mtype varchar(999), digits integer, tscale integer, nameonly boolean, shortname boolean)\n"
				"  RETURNS varchar(1024)\n"
				"BEGIN\n"
				"  RETURN\n"
				"    CASE mtype\n"
				"    WHEN 'char' THEN sys.ifthenelse(nameonly OR digits <= 1, sys.ifthenelse(shortname, 'CHAR', 'CHARACTER'), sys.ifthenelse(shortname, 'CHAR(', 'CHARACTER(') || digits || ')')\n"
				"    WHEN 'varchar' THEN sys.ifthenelse(nameonly OR digits = 0, sys.ifthenelse(shortname, 'VARCHAR', 'CHARACTER VARYING'), sys.ifthenelse(shortname, 'VARCHAR(', 'CHARACTER VARYING(') || digits || ')')\n"
				"    WHEN 'clob' THEN sys.ifthenelse(nameonly OR digits = 0, sys.ifthenelse(shortname, 'CLOB', 'CHARACTER LARGE OBJECT'), sys.ifthenelse(shortname, 'CLOB(', 'CHARACTER LARGE OBJECT(') || digits || ')')\n"
				"    WHEN 'blob' THEN sys.ifthenelse(nameonly OR digits = 0, sys.ifthenelse(shortname, 'BLOB', 'BINARY LARGE OBJECT'), sys.ifthenelse(shortname, 'BLOB(', 'BINARY LARGE OBJECT(') || digits || ')')\n"
				"    WHEN 'int' THEN 'INTEGER'\n"
				"    WHEN 'bigint' THEN 'BIGINT'\n"
				"    WHEN 'smallint' THEN 'SMALLINT'\n"
				"    WHEN 'tinyint' THEN 'TINYINT'\n"
				"    WHEN 'hugeint' THEN 'HUGEINT'\n"
				"    WHEN 'boolean' THEN 'BOOLEAN'\n"
				"    WHEN 'date' THEN 'DATE'\n"
				"    WHEN 'time' THEN sys.ifthenelse(nameonly OR digits = 1, 'TIME', 'TIME(' || (digits -1) || ')')\n"
				"    WHEN 'timestamp' THEN sys.ifthenelse(nameonly OR digits = 7, 'TIMESTAMP', 'TIMESTAMP(' || (digits -1) || ')')\n"
				"    WHEN 'timestamptz' THEN sys.ifthenelse(nameonly OR digits = 7, 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP(' || (digits -1) || ') WITH TIME ZONE')\n"
				"    WHEN 'timetz' THEN sys.ifthenelse(nameonly OR digits = 1, 'TIME WITH TIME ZONE', 'TIME(' || (digits -1) || ') WITH TIME ZONE')\n"
				"    WHEN 'decimal' THEN sys.ifthenelse(nameonly OR digits = 0, 'DECIMAL', 'DECIMAL(' || digits || sys.ifthenelse(tscale = 0, '', ',' || tscale) || ')')\n"
				"    WHEN 'double' THEN sys.ifthenelse(nameonly OR (digits = 53 AND tscale = 0), sys.ifthenelse(shortname, 'DOUBLE', 'DOUBLE PRECISION'), 'FLOAT(' || digits || ')')\n"
				"    WHEN 'real' THEN sys.ifthenelse(nameonly OR (digits = 24 AND tscale = 0), 'REAL', 'FLOAT(' || digits || ')')\n"
				"    WHEN 'day_interval' THEN 'INTERVAL DAY'\n"
				"    WHEN 'month_interval' THEN CASE digits WHEN 1 THEN 'INTERVAL YEAR' WHEN 2 THEN 'INTERVAL YEAR TO MONTH' WHEN 3 THEN 'INTERVAL MONTH' END\n"
				"    WHEN 'sec_interval' THEN\n"
				"	CASE digits\n"
				"	WHEN 4 THEN 'INTERVAL DAY'\n"
				"	WHEN 5 THEN 'INTERVAL DAY TO HOUR'\n"
				"	WHEN 6 THEN 'INTERVAL DAY TO MINUTE'\n"
				"	WHEN 7 THEN 'INTERVAL DAY TO SECOND'\n"
				"	WHEN 8 THEN 'INTERVAL HOUR'\n"
				"	WHEN 9 THEN 'INTERVAL HOUR TO MINUTE'\n"
				"	WHEN 10 THEN 'INTERVAL HOUR TO SECOND'\n"
				"	WHEN 11 THEN 'INTERVAL MINUTE'\n"
				"	WHEN 12 THEN 'INTERVAL MINUTE TO SECOND'\n"
				"	WHEN 13 THEN 'INTERVAL SECOND'\n"
				"	END\n"
				"    WHEN 'oid' THEN 'OID'\n"
				"    WHEN 'json' THEN sys.ifthenelse(nameonly OR digits = 0, 'JSON', 'JSON(' || digits || ')')\n"
				"    WHEN 'url' THEN sys.ifthenelse(nameonly OR digits = 0, 'URL', 'URL(' || digits || ')')\n"
				"    WHEN 'xml' THEN sys.ifthenelse(nameonly OR digits = 0, 'XML', 'XML(' || digits || ')')\n"
				"    WHEN 'geometry' THEN\n"
				"	sys.ifthenelse(nameonly, 'GEOMETRY',\n"
				"	CASE digits\n"
				"	WHEN 4 THEN 'GEOMETRY(POINT' || sys.ifthenelse(tscale = 0, ')', ',' || tscale || ')')\n"
				"	WHEN 8 THEN 'GEOMETRY(LINESTRING' || sys.ifthenelse(tscale = 0, ')', ',' || tscale || ')')\n"
				"	WHEN 16 THEN 'GEOMETRY(POLYGON' || sys.ifthenelse(tscale = 0, ')', ',' || tscale || ')')\n"
				"	WHEN 20 THEN 'GEOMETRY(MULTIPOINT' || sys.ifthenelse(tscale = 0, ')', ',' || tscale || ')')\n"
				"	WHEN 24 THEN 'GEOMETRY(MULTILINESTRING' || sys.ifthenelse(tscale = 0, ')', ',' || tscale || ')')\n"
				"	WHEN 28 THEN 'GEOMETRY(MULTIPOLYGON' || sys.ifthenelse(tscale = 0, ')', ',' || tscale || ')')\n"
				"	WHEN 32 THEN 'GEOMETRY(GEOMETRYCOLLECTION' || sys.ifthenelse(tscale = 0, ')', ',' || tscale || ')')\n"
				"	ELSE 'GEOMETRY'\n"
				"        END)\n"
				"    ELSE sys.ifthenelse(mtype = lower(mtype), upper(mtype), '\"' || mtype || '\"') || sys.ifthenelse(nameonly OR digits = 0, '', '(' || digits || sys.ifthenelse(tscale = 0, '', ',' || tscale) || ')')\n"
				"    END;\n"
				"END;\n"
				"GRANT EXECUTE ON FUNCTION sys.sql_datatype(varchar(999), integer, integer, boolean, boolean) TO PUBLIC;\n"
				"update sys.functions set system = true where system <> true and schema_id = 2000 and name = 'sql_datatype' and type = 1 and language = 2;\n";

				sql->session->status = 0;
				sql->errstr[0] = '\0';
				printf("Running database upgrade commands:\n%s\n", cmds);
				fflush(stdout);
				err = SQLstatementIntern(c, cmds, "update", true, false, NULL);
			}
		}
		ma_destroy(sql->sa);
	}
	sql->sa = old_sa;


	/* 91_information_schema.sql */
	info = mvc_bind_schema(sql, "information_schema");
	if (info == NULL) {
		sql->session->status = 0; /* if the schema was not found clean the error */
		sql->errstr[0] = '\0';
		static const char cmds[] =
		"CREATE SCHEMA INFORMATION_SCHEMA;\n"
		"COMMENT ON SCHEMA INFORMATION_SCHEMA IS 'ISO/IEC 9075-11 SQL/Schemata';\n"
		"update sys.schemas set system = true where name = 'information_schema';\n"

		"CREATE VIEW INFORMATION_SCHEMA.CHARACTER_SETS AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,\n"
		"  cast('UTF-8' AS varchar(16)) AS CHARACTER_SET_NAME,\n"
		"  cast('ISO/IEC 10646:2021' AS varchar(20)) AS CHARACTER_REPERTOIRE,\n"
		"  cast('UTF-8' AS varchar(16)) AS FORM_OF_USE,\n"
		"  cast(NULL AS varchar(1)) AS DEFAULT_COLLATE_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS DEFAULT_COLLATE_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS DEFAULT_COLLATE_NAME;\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.CHARACTER_SETS TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.SCHEMATA AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS CATALOG_NAME,\n"
		"  s.\"name\" AS SCHEMA_NAME,\n"
		"  a.\"name\" AS SCHEMA_OWNER,\n"
		"  cast(NULL AS varchar(1)) AS DEFAULT_CHARACTER_SET_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS DEFAULT_CHARACTER_SET_SCHEMA,\n"
		"  cast('UTF-8' AS varchar(16)) AS DEFAULT_CHARACTER_SET_NAME,\n"
		"  cast(NULL AS varchar(1)) AS SQL_PATH,\n"
		"  s.\"id\" AS schema_id,\n"
		"  s.\"system\" AS is_system,\n"
		"  cm.\"remark\" AS comments\n"
		" FROM sys.\"schemas\" s\n"
		" INNER JOIN sys.\"auths\" a ON s.\"owner\" = a.\"id\"\n"
		" LEFT OUTER JOIN sys.\"comments\" cm ON s.\"id\" = cm.\"id\"\n"
		" ORDER BY s.\"name\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.SCHEMATA TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.TABLES AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS TABLE_CATALOG,\n"
		"  s.\"name\" AS TABLE_SCHEMA,\n"
		"  t.\"name\" AS TABLE_NAME,\n"
		"  tt.\"table_type_name\" AS TABLE_TYPE,\n"
		"  cast(NULL AS varchar(1)) AS SELF_REFERENCING_COLUMN_NAME,\n"
		"  cast(NULL AS varchar(1)) AS REFERENCE_GENERATION,\n"
		"  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_NAME,\n"
		"  cast(sys.ifthenelse((t.\"type\" IN (0, 3, 7, 20, 30) AND t.\"access\" IN (0, 2)), 'YES', 'NO') AS varchar(3)) AS IS_INSERTABLE_INTO,\n"
		"  cast('NO' AS varchar(3)) AS IS_TYPED,\n"
		"  cast((CASE t.\"commit_action\" WHEN 1 THEN 'DELETE' WHEN 2 THEN 'PRESERVE' WHEN 3 THEN 'DROP' ELSE NULL END) AS varchar(10)) AS COMMIT_ACTION,\n"
		"  t.\"schema_id\" AS schema_id,\n"
		"  t.\"id\" AS table_id,\n"
		"  t.\"type\" AS table_type_id,\n"
		"  st.\"count\" AS row_count,\n"
		"  t.\"system\" AS is_system,\n"
		"  sys.ifthenelse(t.\"type\" IN (1, 11), TRUE, FALSE) AS is_view,\n"
		"  t.\"query\" AS query_def,\n"
		"  cm.\"remark\" AS comments\n"
		" FROM sys.\"tables\" t\n"
		" INNER JOIN sys.\"schemas\" s ON t.\"schema_id\" = s.\"id\"\n"
		" INNER JOIN sys.\"table_types\" tt ON t.\"type\" = tt.\"table_type_id\"\n"
		" LEFT OUTER JOIN sys.\"comments\" cm ON t.\"id\" = cm.\"id\"\n"
		" LEFT OUTER JOIN (SELECT DISTINCT \"schema\", \"table\", \"count\" FROM sys.\"statistics\"()) st ON (s.\"name\" = st.\"schema\" AND t.\"name\" = st.\"table\")\n"
		" ORDER BY s.\"name\", t.\"name\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.TABLES TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.VIEWS AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS TABLE_CATALOG,\n"
		"  s.\"name\" AS TABLE_SCHEMA,\n"
		"  t.\"name\" AS TABLE_NAME,\n"
		"  t.\"query\" AS VIEW_DEFINITION,\n"
		"  cast('NONE' AS varchar(10)) AS CHECK_OPTION,\n"
		"  cast('NO' AS varchar(3)) AS IS_UPDATABLE,\n"
		"  cast('NO' AS varchar(3)) AS INSERTABLE_INTO,\n"
		"  cast('NO' AS varchar(3)) AS IS_TRIGGER_UPDATABLE,\n"
		"  cast('NO' AS varchar(3)) AS IS_TRIGGER_DELETABLE,\n"
		"  cast('NO' AS varchar(3)) AS IS_TRIGGER_INSERTABLE_INTO,\n"
		"  t.\"schema_id\" AS schema_id,\n"
		"  t.\"id\" AS table_id,\n"
		"  cast(sys.ifthenelse(t.\"system\", t.\"type\" + 10 , t.\"type\") AS smallint) AS table_type_id,\n"
		"  t.\"system\" AS is_system,\n"
		"  cm.\"remark\" AS comments\n"
		" FROM sys.\"_tables\" t\n"
		" INNER JOIN sys.\"schemas\" s ON t.\"schema_id\" = s.\"id\"\n"
		" LEFT OUTER JOIN sys.\"comments\" cm ON t.\"id\" = cm.\"id\"\n"
		" WHERE t.\"type\" = 1\n"
		" ORDER BY s.\"name\", t.\"name\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.VIEWS TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.COLUMNS AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS TABLE_CATALOG,\n"
		"  s.\"name\" AS TABLE_SCHEMA,\n"
		"  t.\"name\" AS TABLE_NAME,\n"
		"  c.\"name\" AS COLUMN_NAME,\n"
		"  cast(1 + c.\"number\" AS int) AS ORDINAL_POSITION,\n"
		"  c.\"default\" AS COLUMN_DEFAULT,\n"
		"  cast(sys.ifthenelse(c.\"null\", 'YES', 'NO') AS varchar(3)) AS IS_NULLABLE,\n"
		"  cast(sys.\"sql_datatype\"(c.\"type\", c.\"type_digits\", c.\"type_scale\", true, true) AS varchar(1024)) AS DATA_TYPE,\n"
		"  cast(sys.ifthenelse(c.\"type\" IN ('varchar','clob','char','json','url','xml') AND c.\"type_digits\" > 0, c.\"type_digits\", NULL) AS int) AS CHARACTER_MAXIMUM_LENGTH,\n"
		"  cast(sys.ifthenelse(c.\"type\" IN ('varchar','clob','char','json','url','xml') AND c.\"type_digits\" > 0, 4 * cast(c.\"type_digits\" as bigint), NULL) AS bigint) AS CHARACTER_OCTET_LENGTH,\n"
		"  cast(sys.ifthenelse(c.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), c.\"type_digits\", NULL) AS int) AS NUMERIC_PRECISION,\n"
		"  cast(sys.ifthenelse(c.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','oid'), 2, sys.ifthenelse(c.\"type\" IN ('decimal','numeric'), 10, NULL)) AS int) AS NUMERIC_PRECISION_RADIX,\n"
		"  cast(sys.ifthenelse(c.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), c.\"type_scale\", NULL) AS int) AS NUMERIC_SCALE,\n"
		"  cast(sys.ifthenelse(c.\"type\" IN ('date','timestamp','timestamptz','time','timetz'), sys.ifthenelse(c.\"type_scale\" > 0, c.\"type_scale\" -1, 0), NULL) AS int) AS DATETIME_PRECISION,\n"
		"  cast(sys.ifthenelse(c.\"type\" IN ('day_interval','month_interval','sec_interval'), sys.\"sql_datatype\"(c.\"type\", c.\"type_digits\", c.\"type_scale\", true, true), NULL) AS varchar(40)) AS INTERVAL_TYPE,\n"
		"  cast(CASE c.\"type\" WHEN 'day_interval' THEN 0 WHEN 'month_interval' THEN 0 WHEN 'sec_interval' THEN (sys.ifthenelse(c.\"type_digits\" IN (7, 10, 12, 13), sys.ifthenelse(c.\"type_scale\" > 0, c.\"type_scale\", 3), 0)) ELSE NULL END AS int) AS INTERVAL_PRECISION,\n"
		"  cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,\n"
		"  cast(sys.ifthenelse(c.\"type\" IN ('varchar','clob','char','json','url','xml'), 'UTF-8', NULL) AS varchar(16)) AS CHARACTER_SET_NAME,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_NAME,\n"
		"  cast(NULL AS varchar(1)) AS DOMAIN_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS DOMAIN_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS DOMAIN_NAME,\n"
		"  cast(NULL AS varchar(1)) AS UDT_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS UDT_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS UDT_NAME,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_NAME,\n"
		"  cast(NULL AS int) AS MAXIMUM_CARDINALITY,\n"
		"  cast(NULL AS varchar(1)) AS DTD_IDENTIFIER,\n"
		"  cast('NO' AS varchar(3)) AS IS_SELF_REFERENCING,\n"
		"  cast(sys.ifthenelse(seq.\"name\" IS NULL OR c.\"null\", 'NO', 'YES') AS varchar(3)) AS IS_IDENTITY,\n"
		"  seq.\"name\" AS IDENTITY_GENERATION,\n"
		"  seq.\"start\" AS IDENTITY_START,\n"
		"  seq.\"increment\" AS IDENTITY_INCREMENT,\n"
		"  seq.\"maxvalue\" AS IDENTITY_MAXIMUM,\n"
		"  seq.\"minvalue\" AS IDENTITY_MINIMUM,\n"
		"  cast(sys.ifthenelse(seq.\"name\" IS NULL, NULL, sys.ifthenelse(seq.\"cycle\", 'YES', 'NO')) AS varchar(3)) AS IDENTITY_CYCLE,\n"
		"  cast(sys.ifthenelse(seq.\"name\" IS NULL, 'NO', 'YES') AS varchar(3)) AS IS_GENERATED,\n"
		"  cast(sys.ifthenelse(seq.\"name\" IS NULL, NULL, c.\"default\") AS varchar(1024)) AS GENERATION_EXPRESSION,\n"
		"  cast('NO' AS varchar(3)) AS IS_SYSTEM_TIME_PERIOD_START,\n"
		"  cast('NO' AS varchar(3)) AS IS_SYSTEM_TIME_PERIOD_END,\n"
		"  cast('NO' AS varchar(3)) AS SYSTEM_TIME_PERIOD_TIMESTAMP_GENERATION,\n"
		"  cast(sys.ifthenelse(t.\"type\" IN (0,3,7,20,30), 'YES', 'NO') AS varchar(3)) AS IS_UPDATABLE,\n"
		"  cast(NULL AS varchar(1)) AS DECLARED_DATA_TYPE,\n"
		"  cast(NULL AS int) AS DECLARED_NUMERIC_PRECISION,\n"
		"  cast(NULL AS int) AS DECLARED_NUMERIC_SCALE,\n"
		"  t.\"schema_id\" AS schema_id,\n"
		"  c.\"table_id\" AS table_id,\n"
		"  c.\"id\" AS column_id,\n"
		"  seq.\"id\" AS sequence_id,\n"
		"  t.\"system\" AS is_system,\n"
		"  cm.\"remark\" AS comments\n"
		" FROM sys.\"columns\" c\n"
		" INNER JOIN sys.\"tables\" t ON c.\"table_id\" = t.\"id\"\n"
		" INNER JOIN sys.\"schemas\" s ON t.\"schema_id\" = s.\"id\"\n"
		" LEFT OUTER JOIN sys.\"comments\" cm ON c.\"id\" = cm.\"id\"\n"
		" LEFT OUTER JOIN sys.\"sequences\" seq ON ((seq.\"name\"||'\"') = substring(c.\"default\", 3 + sys.\"locate\"('\".\"seq_',c.\"default\",14)))\n"
		" ORDER BY s.\"name\", t.\"name\", c.\"number\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.COLUMNS TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,\n"
		"  cast(NULL AS varchar(1024)) AS CONSTRAINT_SCHEMA,\n"
		"  cast(NULL AS varchar(1024)) AS CONSTRAINT_NAME,\n"
		"  cast(NULL AS varchar(1024)) AS CHECK_CLAUSE\n"
		" WHERE 1=0;\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.CHECK_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,\n"
		"  s.\"name\" AS CONSTRAINT_SCHEMA,\n"
		"  k.\"name\" AS CONSTRAINT_NAME,\n"
		"  cast(NULL AS varchar(1)) AS TABLE_CATALOG,\n"
		"  s.\"name\" AS TABLE_SCHEMA,\n"
		"  t.\"name\" AS TABLE_NAME,\n"
		"  cast(CASE k.\"type\" WHEN 0 THEN 'PRIMARY KEY' WHEN 1 THEN 'UNIQUE' WHEN 2 THEN 'FOREIGN KEY' ELSE NULL END AS varchar(16)) AS CONSTRAINT_TYPE,\n"
		"  cast('NO' AS varchar(3)) AS IS_DEFERRABLE,\n"
		"  cast('NO' AS varchar(3)) AS INITIALLY_DEFERRED,\n"
		"  cast('YES' AS varchar(3)) AS ENFORCED,\n"
		"  t.\"schema_id\" AS schema_id,\n"
		"  t.\"id\" AS table_id,\n"
		"  k.\"id\" AS key_id,\n"
		"  k.\"type\" AS key_type,\n"
		"  t.\"system\" AS is_system\n"
		" FROM (SELECT sk.\"id\", sk.\"table_id\", sk.\"name\", sk.\"type\" FROM sys.\"keys\" sk UNION ALL SELECT tk.\"id\", tk.\"table_id\", tk.\"name\", tk.\"type\" FROM tmp.\"keys\" tk) k\n"
		" INNER JOIN (SELECT st.\"id\", st.\"schema_id\", st.\"name\", st.\"system\" FROM sys.\"_tables\" st UNION ALL"
			" SELECT tt.\"id\", tt.\"schema_id\", tt.\"name\", tt.\"system\" FROM tmp.\"_tables\" tt) t ON k.\"table_id\" = t.\"id\"\n"
		" INNER JOIN sys.\"schemas\" s ON t.\"schema_id\" = s.\"id\"\n"
		" ORDER BY s.\"name\", t.\"name\", k.\"name\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,\n"
		"  s.\"name\" AS CONSTRAINT_SCHEMA,\n"
		"  fk.\"name\" AS CONSTRAINT_NAME,\n"
		"  cast(NULL AS varchar(1)) AS UNIQUE_CONSTRAINT_CATALOG,\n"
		"  uks.\"name\" AS UNIQUE_CONSTRAINT_SCHEMA,\n"
		"  uk.\"name\" AS UNIQUE_CONSTRAINT_NAME,\n"
		"  cast('FULL' AS varchar(7)) AS MATCH_OPTION,\n"
		"  fk.\"update_action\" AS UPDATE_RULE,\n"
		"  fk.\"delete_action\" AS DELETE_RULE,\n"
		"  t.\"schema_id\" AS fk_schema_id,\n"
		"  t.\"id\" AS fk_table_id,\n"
		"  t.\"name\" AS fk_table_name,\n"
		"  fk.\"id\" AS fk_key_id,\n"
		"  ukt.\"schema_id\" AS uc_schema_id,\n"
		"  uk.\"table_id\" AS uc_table_id,\n"
		"  ukt.\"name\" AS uc_table_name,\n"
		"  uk.\"id\" AS uc_key_id\n"
		" FROM sys.\"fkeys\" fk\n"
		" INNER JOIN sys.\"tables\" t ON t.\"id\" = fk.\"table_id\"\n"
		" INNER JOIN sys.\"schemas\" s ON s.\"id\" = t.\"schema_id\"\n"
		" LEFT OUTER JOIN sys.\"keys\" uk ON uk.\"id\" = fk.\"rkey\"\n"
		" LEFT OUTER JOIN sys.\"tables\" ukt ON ukt.\"id\" = uk.\"table_id\"\n"
		" LEFT OUTER JOIN sys.\"schemas\" uks ON uks.\"id\" = ukt.\"schema_id\"\n"
		" ORDER BY s.\"name\", t.\"name\", fk.\"name\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.ROUTINES AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS SPECIFIC_CATALOG,\n"
		"  s.\"name\" AS SPECIFIC_SCHEMA,\n"
		"  cast(f.\"name\"||'('||f.\"id\"||')' AS varchar(270)) AS SPECIFIC_NAME,\n"
		"  cast(NULL AS varchar(1)) AS ROUTINE_CATALOG,\n"
		"  s.\"name\" AS ROUTINE_SCHEMA,\n"
		"  f.\"name\" AS ROUTINE_NAME,\n"
		"  ft.\"function_type_keyword\" AS ROUTINE_TYPE,\n"
		"  cast(NULL AS varchar(1)) AS MODULE_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS MODULE_SCHEMA,\n"
		"  cast(f.\"mod\" AS varchar(128)) AS MODULE_NAME,\n"
		"  cast(NULL AS varchar(1)) AS UDT_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS UDT_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS UDT_NAME,\n"
		"  cast(CASE f.\"type\" WHEN 1 THEN sys.\"sql_datatype\"(a.\"type\", a.\"type_digits\", a.\"type_scale\", true, true) WHEN 2 THEN NULL WHEN 5 THEN 'TABLE' WHEN 7 THEN 'TABLE' ELSE NULL END AS varchar(1024)) AS DATA_TYPE,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('varchar','clob','char','json','url','xml') AND a.\"type_digits\" > 0, a.\"type_digits\", NULL) AS int) AS CHARACTER_MAXIMUM_LENGTH,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('varchar','clob','char','json','url','xml') AND a.\"type_digits\" > 0, 4 * cast(a.\"type_digits\" as bigint), NULL) AS bigint) AS CHARACTER_OCTET_LENGTH,\n"
		"  cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,\n"
		"  'UTF-8' AS CHARACTER_SET_NAME,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_NAME,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a.\"type_digits\", NULL) AS int) AS NUMERIC_PRECISION,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','oid'), 2, sys.ifthenelse(a.\"type\" IN ('decimal','numeric'), 10, NULL)) AS int) AS NUMERIC_PRECISION_RADIX,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a.\"type_scale\", NULL) AS int) AS NUMERIC_SCALE,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('date','timestamp','timestamptz','time','timetz'), a.\"type_scale\" -1, NULL) AS int) AS DATETIME_PRECISION,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('day_interval','month_interval','sec_interval'), sys.\"sql_datatype\"(a.\"type\", a.\"type_digits\", a.\"type_scale\", true, true), NULL) AS varchar(40)) AS INTERVAL_TYPE,\n"
		"  cast(CASE a.\"type\" WHEN 'day_interval' THEN 0 WHEN 'month_interval' THEN 0 WHEN 'sec_interval' THEN (sys.ifthenelse(a.\"type_digits\" IN (7, 10, 12, 13), sys.ifthenelse(a.\"type_scale\" > 0, a.\"type_scale\", 3), 0)) ELSE NULL END AS int) AS INTERVAL_PRECISION,\n"
		"  cast(NULL AS varchar(1)) AS TYPE_UDT_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS TYPE_UDT_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS TYPE_UDT_NAME,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_NAME,\n"
		"  cast(NULL AS int) AS MAXIMUM_CARDINALITY,\n"
		"  cast(NULL AS int) AS DTD_IDENTIFIER,\n"
		"  cast(sys.\"ifthenelse\"(sys.\"locate\"('begin',f.\"func\") > 0, sys.\"ifthenelse\"(sys.\"endswith\"(f.\"func\",';'), sys.\"substring\"(f.\"func\", sys.\"locate\"('begin',f.\"func\"), sys.\"length\"(sys.\"substring\"(f.\"func\", sys.\"locate\"('begin',f.\"func\")))-1), sys.\"substring\"(f.\"func\", sys.\"locate\"('begin',f.\"func\"))), NULL) AS varchar(8196)) AS ROUTINE_BODY,\n"
		"  f.\"func\" AS ROUTINE_DEFINITION,\n"
		"  cast(sys.\"ifthenelse\"(sys.\"locate\"('external name',f.\"func\") > 0, sys.\"ifthenelse\"(sys.\"endswith\"(f.\"func\",';'), sys.\"substring\"(f.\"func\", 14 + sys.\"locate\"('external name',f.\"func\"), sys.\"length\"(sys.\"substring\"(f.\"func\", 14 + sys.\"locate\"('external name',f.\"func\")))-1), sys.\"substring\"(f.\"func\", 14 + sys.\"locate\"('external name',f.\"func\"))), NULL) AS varchar(1024)) AS EXTERNAL_NAME,\n"
		"  fl.\"language_keyword\" AS EXTERNAL_LANGUAGE,\n"
		"  'GENERAL' AS PARAMETER_STYLE,\n"
		"  'YES' AS IS_DETERMINISTIC,\n"
		"  cast(sys.ifthenelse(f.\"side_effect\", 'MODIFIES', 'READ') AS varchar(10)) AS SQL_DATA_ACCESS,\n"
		"  cast(CASE f.\"type\" WHEN 2 THEN NULL ELSE 'NO' END AS varchar(3)) AS IS_NULL_CALL,\n"
		"  cast(NULL AS varchar(1)) AS SQL_PATH,\n"
		"  cast(NULL AS varchar(1)) AS SCHEMA_LEVEL_ROUTINE,\n"
		"  cast(NULL AS int) AS MAX_DYNAMIC_RESULT_SETS,\n"
		"  cast(NULL AS varchar(1)) AS IS_USER_DEFINED_CAST,\n"
		"  cast(NULL AS varchar(1)) AS IS_IMPLICITLY_INVOCABLE,\n"
		"  cast(NULL AS varchar(1)) AS SECURITY_TYPE,\n"
		"  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_NAME,\n"
		"  cast(NULL AS varchar(1)) AS AS_LOCATOR,\n"
		"  cast(NULL AS timestamp) AS CREATED,\n"
		"  cast(NULL AS timestamp) AS LAST_ALTERED,\n"
		"  cast(NULL AS varchar(1)) AS NEW_SAVEPOINT_LEVEL,\n"
		"  cast(NULL AS varchar(1)) AS IS_UDT_DEPENDENT,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_FROM_DATA_TYPE,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_AS_LOCATOR,\n"
		"  cast(NULL AS int) AS RESULT_CAST_CHAR_MAX_LENGTH,\n"
		"  cast(NULL AS int) AS RESULT_CAST_CHAR_OCTET_LENGTH,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_CHAR_SET_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_CHAR_SET_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_CHARACTER_SET_NAME,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_COLLATION_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_COLLATION_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_COLLATION_NAME,\n"
		"  cast(NULL AS int) AS RESULT_CAST_NUMERIC_PRECISION,\n"
		"  cast(NULL AS int) AS RESULT_CAST_NUMERIC_RADIX,\n"
		"  cast(NULL AS int) AS RESULT_CAST_NUMERIC_SCALE,\n"
		"  cast(NULL AS int) AS RESULT_CAST_DATETIME_PRECISION,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_INTERVAL_TYPE,\n"
		"  cast(NULL AS int) AS RESULT_CAST_INTERVAL_PRECISION,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_TYPE_UDT_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_TYPE_UDT_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_TYPE_UDT_NAME,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_SCOPE_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_SCOPE_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_SCOPE_NAME,\n"
		"  cast(NULL AS int) AS RESULT_CAST_MAX_CARDINALITY,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_DTD_IDENTIFIER,\n"
		"  cast(NULL AS varchar(1)) AS DECLARED_DATA_TYPE,\n"
		"  cast(NULL AS int) AS DECLARED_NUMERIC_PRECISION,\n"
		"  cast(NULL AS int) AS DECLARED_NUMERIC_SCALE,\n"
		"  cast(NULL AS varchar(1)) AS RESULT_CAST_FROM_DECLARED_DATA_TYPE,\n"
		"  cast(NULL AS int) AS RESULT_CAST_DECLARED_NUMERIC_PRECISION,\n"
		"  cast(NULL AS int) AS RESULT_CAST_DECLARED_NUMERIC_SCALE,\n"
		"  f.\"schema_id\" AS schema_id,\n"
		"  f.\"id\" AS function_id,\n"
		"  f.\"type\" AS function_type,\n"
		"  f.\"language\" AS function_language,\n"
		"  f.\"system\" AS is_system,\n"
		"  cm.\"remark\" AS comments\n"
		" FROM sys.\"functions\" f\n"
		" INNER JOIN sys.\"schemas\" s ON s.\"id\" = f.\"schema_id\"\n"
		" INNER JOIN sys.\"function_types\" ft ON ft.\"function_type_id\" = f.\"type\"\n"
		" INNER JOIN sys.\"function_languages\" fl ON fl.\"language_id\" = f.\"language\"\n"
		" LEFT OUTER JOIN sys.\"args\" a ON a.\"func_id\" = f.\"id\" and a.\"inout\" = 0 and a.\"number\" = 0\n"
		" LEFT OUTER JOIN sys.\"comments\" cm ON cm.\"id\" = f.\"id\"\n"
		" WHERE f.\"type\" in (1, 2, 5, 7)\n"
		" ORDER BY s.\"name\", f.\"name\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.ROUTINES TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.PARAMETERS AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS SPECIFIC_CATALOG,\n"
		"  s.\"name\" AS SPECIFIC_SCHEMA,\n"
		"  cast(f.\"name\"||'('||f.\"id\"||')' AS varchar(270)) AS SPECIFIC_NAME,\n"
		"  cast(sys.ifthenelse((a.\"inout\" = 0 OR f.\"type\" = 2), 1 + a.\"number\", sys.ifthenelse(f.\"type\" = 1, a.\"number\", (1 + a.\"number\" - f.count_out_cols))) AS int) AS ORDINAL_POSITION,\n"
		"  cast(sys.ifthenelse(a.\"inout\" = 0, 'OUT', sys.ifthenelse(a.\"inout\" = 1, 'IN', 'INOUT')) as varchar(5)) AS PARAMETER_MODE,\n"
		"  cast(sys.ifthenelse(a.\"inout\" = 0, 'YES', 'NO') as varchar(3)) AS IS_RESULT,\n"
		"  cast(NULL AS varchar(1)) AS AS_LOCATOR,\n"
		"  a.\"name\" AS PARAMETER_NAME,\n"
		"  cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_NAME,\n"
		"  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_NAME,\n"
		"  cast(sys.\"sql_datatype\"(a.\"type\", a.\"type_digits\", a.\"type_scale\", true, true) AS varchar(1024)) AS DATA_TYPE,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('varchar','clob','char','json','url','xml') AND a.\"type_digits\" > 0, a.\"type_digits\", NULL) AS int) AS CHARACTER_MAXIMUM_LENGTH,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('varchar','clob','char','json','url','xml') AND a.\"type_digits\" > 0, 4 * cast(a.\"type_digits\" as bigint), NULL) AS bigint) AS CHARACTER_OCTET_LENGTH,\n"
		"  cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('varchar','clob','char','json','url','xml'), 'UTF-8', NULL) AS varchar(16)) AS CHARACTER_SET_NAME,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS COLLATION_NAME,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a.\"type_digits\", NULL) AS int) AS NUMERIC_PRECISION,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','oid'), 2, sys.ifthenelse(a.\"type\" IN ('decimal','numeric'), 10, NULL)) AS int) AS NUMERIC_PRECISION_RADIX,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a.\"type_scale\", NULL) AS int) AS NUMERIC_SCALE,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('date','timestamp','timestamptz','time','timetz'), sys.ifthenelse(a.\"type_scale\" > 0, a.\"type_scale\" -1, 0), NULL) AS int) AS DATETIME_PRECISION,\n"
		"  cast(sys.ifthenelse(a.\"type\" IN ('day_interval','month_interval','sec_interval'), sys.\"sql_datatype\"(a.\"type\", a.\"type_digits\", a.\"type_scale\", true, true), NULL) AS varchar(40)) AS INTERVAL_TYPE,\n"
		"  cast(CASE a.\"type\" WHEN 'day_interval' THEN 0 WHEN 'month_interval' THEN 0 WHEN 'sec_interval' THEN (sys.ifthenelse(a.\"type_digits\" IN (7, 10, 12, 13), sys.ifthenelse(a.\"type_scale\" > 0, a.\"type_scale\", 3), 0)) ELSE NULL END AS int) AS INTERVAL_PRECISION,\n"
		"  cast(NULL AS varchar(1)) AS UDT_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS UDT_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS UDT_NAME,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_CATALOG,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_SCHEMA,\n"
		"  cast(NULL AS varchar(1)) AS SCOPE_NAME,\n"
		"  cast(NULL AS int) AS MAXIMUM_CARDINALITY,\n"
		"  cast(NULL AS varchar(1)) AS DTD_IDENTIFIER,\n"
		"  cast(NULL AS varchar(1)) AS DECLARED_DATA_TYPE,\n"
		"  cast(NULL AS int) AS DECLARED_NUMERIC_PRECISION,\n"
		"  cast(NULL AS int) AS DECLARED_NUMERIC_SCALE,\n"
		"  cast(NULL AS varchar(1)) AS PARAMETER_DEFAULT,\n"
		"  f.\"schema_id\" AS schema_id,\n"
		"  f.\"id\" AS function_id,\n"
		"  a.\"id\" AS arg_id,\n"
		"  f.\"name\" AS function_name,\n"
		"  f.\"type\" AS function_type,\n"
		"  f.\"system\" AS is_system\n"
		" FROM sys.\"args\" a\n"
		" INNER JOIN (SELECT fun.*, (select count(*) from sys.args a0 where a0.inout = 0 and a0.func_id = fun.id) as count_out_cols FROM sys.\"functions\" fun WHERE fun.\"type\" in (1, 2, 5, 7)) f ON f.\"id\" = a.\"func_id\"\n"
		" INNER JOIN sys.\"schemas\" s ON s.\"id\" = f.\"schema_id\"\n"
		" ORDER BY s.\"name\", f.\"name\", f.\"id\", a.\"inout\" DESC, a.\"number\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.PARAMETERS TO PUBLIC WITH GRANT OPTION;\n"

		"CREATE VIEW INFORMATION_SCHEMA.SEQUENCES AS SELECT\n"
		"  cast(NULL AS varchar(1)) AS SEQUENCE_CATALOG,\n"
		"  s.\"name\" AS SEQUENCE_SCHEMA,\n"
		"  sq.\"name\" AS SEQUENCE_NAME,\n"
		"  cast('BIGINT' AS varchar(16)) AS DATA_TYPE,\n"
		"  cast(64 AS SMALLINT) AS NUMERIC_PRECISION,\n"
		"  cast(2 AS SMALLINT) AS NUMERIC_PRECISION_RADIX,\n"
		"  cast(0 AS SMALLINT) AS NUMERIC_SCALE,\n"
		"  sq.\"start\" AS START_VALUE,\n"
		"  sq.\"minvalue\" AS MINIMUM_VALUE,\n"
		"  sq.\"maxvalue\" AS MAXIMUM_VALUE,\n"
		"  sq.\"increment\" AS INCREMENT,\n"
		"  cast(sys.ifthenelse(sq.\"cycle\", 'YES', 'NO') AS varchar(3)) AS CYCLE_OPTION,\n"
		"  cast(NULL AS varchar(16)) AS DECLARED_DATA_TYPE,\n"
		"  cast(NULL AS SMALLINT) AS DECLARED_NUMERIC_PRECISION,\n"
		"  cast(NULL AS SMALLINT) AS DECLARED_NUMERIC_SCALE,\n"
		"  sq.\"schema_id\" AS schema_id,\n"
		"  sq.\"id\" AS sequence_id,\n"
		"  get_value_for(s.\"name\", sq.\"name\") AS current_value,\n"
		"  sq.\"cacheinc\" AS cacheinc,\n"
		"  cm.\"remark\" AS comments\n"
		" FROM sys.\"sequences\" sq\n"
		" INNER JOIN sys.\"schemas\" s ON sq.\"schema_id\" = s.\"id\"\n"
		" LEFT OUTER JOIN sys.\"comments\" cm ON sq.\"id\" = cm.\"id\"\n"
		" ORDER BY s.\"name\", sq.\"name\";\n"
		"GRANT SELECT ON TABLE INFORMATION_SCHEMA.SEQUENCES TO PUBLIC WITH GRANT OPTION;\n"
		"\n"
		"update sys._tables set system = true where system <> true\n"
		" and schema_id = (select s.id from sys.schemas s where s.name = 'information_schema')\n"
		" and name in ('character_sets','check_constraints','columns','parameters','routines','schemata','sequences','referential_constraints','table_constraints','tables','views');\n";
		printf("Running database upgrade commands:\n%s\n", cmds);
		fflush(stdout);
		err = SQLstatementIntern(c, cmds, "update", true, false, NULL);
	}

	/* 77_storage.sql */
	sql_find_subtype(&tp, "varchar", 0, 0);

	if (!sql_bind_func(sql, s->base.name, "persist_unlogged", &tp, &tp, F_UNION, true, true)) {
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		static const char query[] =
			"CREATE FUNCTION sys.persist_unlogged(sname STRING, tname STRING)\n"
			"RETURNS TABLE(\"table\" STRING, \"table_id\" INT, \"rowcount\" BIGINT)\n"
			"EXTERNAL NAME sql.persist_unlogged;\n"
			"GRANT EXECUTE ON FUNCTION sys.persist_unlogged(string, string) TO PUBLIC;\n"
			"UPDATE sys.functions SET system = true WHERE system <> true AND\n"
			"name = 'persist_unlogged' AND schema_id = 2000 AND type = 5 AND language = 1;\n";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
	}

	return err;
}

static str
sql_update_dec2023_sp1(Client c, mvc *sql, sql_schema *s)
{
	char *err;
	res_table *output;
	BAT *b;

	(void) sql;
	(void) s;

	/* json.isvalid(json) has been fixed to return NULL on NULL input */
	err = SQLstatementIntern(c, "SELECT f.id FROM sys.functions f WHERE f.name = 'isvalid' AND f.schema_id = (SELECT s.id FROM sys.schemas s WHERE s.name = 'json') AND EXISTS (SELECT * FROM sys.args a WHERE a.func_id = f.id AND a.number = 1 AND a.type = 'json') AND f.func LIKE '%begin return true%';\n", "update", true, false, &output);
	if (err)
		return err;
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) > 0) {
			static const char query[] = "drop function json.isvalid(json);\n"
				"create function json.isvalid(js json)\n"
				"returns bool begin return case when js is NULL then NULL else true end; end;\n"
				"GRANT EXECUTE ON FUNCTION json.isvalid(json) TO PUBLIC;\n"
				"update sys.functions set system = true where system <> true and name = 'isvalid' and schema_id = (select id from sys.schemas where name = 'json');\n";
			assert(BATcount(b) == 1);
			printf("Running database upgrade commands:\n%s\n", query);
			fflush(stdout);
			err = SQLstatementIntern(c, query, "update", true, false, NULL);
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);
	return err;
}

static str
sql_update_dec2023_sp4(Client c, mvc *sql, sql_schema *s)
{
	char *err;
	res_table *output;
	BAT *b;

	(void) sql;
	(void) s;

	/* SQL optimizer fixes make that some "dependencies" are no longer
	 * dependencies (functions that depend on all columns of a table
	 * when it only actually uses a subset of the columns); while we're
	 * at it, also fix some ancient dependency changes where view did
	 * the same thing (i.e. the second delete will normally not delete
	 * anything) */
	err = SQLstatementIntern(c, "select * from sys.dependencies where (id, depend_id) in (select c.id, f.id from sys.functions f, sys._tables t, sys._columns c, sys.dependencies d where c.table_id = t.id and f.id = d.depend_id and c.id = d.id and f.schema_id = 2000 and t.schema_id = 2000 and (f.name, t.name, c.name) in (values ('describe_columns', '_columns', 'storage')));\n", "update", true, false, &output);
	if (err)
		return err;
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) > 0) {
			static const char query[] =
				"delete from sys.dependencies where (id, depend_id) in (select c.id, f.id from sys.functions f, sys._tables t, sys._columns c, sys.dependencies d where c.table_id = t.id and f.id = d.depend_id and c.id = d.id and f.schema_id = 2000 and t.schema_id = 2000 and (f.name, t.name, c.name) in (values ('describe_columns', '_columns', 'storage'), ('describe_function', 'function_languages', 'language_name'), ('describe_function', 'function_types', 'function_type_name'), ('describe_function', 'functions', 'func'), ('describe_function', 'functions', 'mod'), ('describe_function', 'functions', 'semantics'), ('describe_function', 'functions', 'side_effect'), ('describe_function', 'functions', 'system'), ('describe_function', 'functions', 'vararg'), ('describe_function', 'functions', 'varres'), ('describe_function', 'schemas', 'authorization'), ('describe_function', 'schemas', 'owner'), ('describe_function', 'schemas', 'system'), ('describe_table', '_tables', 'access'), ('describe_table', '_tables', 'commit_action'), ('describe_table', '_tables', 'system')));\n"
				"delete from sys.dependencies where (id, depend_id) in (select c.id, v.id from sys._tables v, sys._tables t, sys._columns c, sys.dependencies d where c.table_id = t.id and v.id = d.depend_id and c.id = d.id and v.schema_id = 2000 and t.schema_id = 2000 and (v.name, t.name, c.name) in (values ('dependency_columns_on_indexes', '_columns', 'name'), ('dependency_columns_on_indexes', '_columns', 'number'), ('dependency_columns_on_indexes', '_columns', 'storage'), ('dependency_columns_on_indexes', '_columns', 'table_id'), ('dependency_columns_on_indexes', '_columns', 'type_digits'), ('dependency_columns_on_indexes', 'keys', 'id'), ('dependency_columns_on_indexes', 'triggers', 'name'), ('dependency_columns_on_indexes', 'triggers', 'orientation'), ('dependency_columns_on_indexes', 'triggers', 'table_id'), ('dependency_columns_on_indexes', 'triggers', 'time'), ('dependency_columns_on_keys', '_columns', 'name'), ('dependency_columns_on_keys', '_columns', 'table_id'), ('dependency_columns_on_keys', '_columns', 'type'), ('dependency_columns_on_keys', '_columns', 'type_digits'), ('dependency_columns_on_keys', '_columns', 'type_scale'), ('dependency_columns_on_keys', 'triggers', 'name'), ('dependency_columns_on_keys', 'triggers', 'orientation'), ('dependency_columns_on_keys', 'triggers', 'table_id'), ('dependency_columns_on_keys', 'triggers', 'time'), ('dependency_columns_on_triggers', 'keys', 'name'), ('dependency_columns_on_triggers', 'keys', 'rkey'), ('dependency_columns_on_triggers', 'keys', 'type'), ('dependency_functions_on_triggers', 'keys', 'action'), ('dependency_functions_on_triggers', 'keys', 'name'), ('dependency_functions_on_triggers', 'keys', 'rkey'), ('dependency_functions_on_triggers', 'keys', 'type'), ('dependency_keys_on_foreignkeys', '_columns', 'default'), ('dependency_keys_on_foreignkeys', '_columns', 'name'), ('dependency_keys_on_foreignkeys', '_columns', 'table_id'), ('dependency_keys_on_foreignkeys', '_columns', 'type'), ('dependency_keys_on_foreignkeys', '_columns', 'type_digits'), ('dependency_keys_on_foreignkeys', '_columns', 'type_scale'), ('dependency_tables_on_foreignkeys', '_columns', 'default'), ('dependency_tables_on_foreignkeys', '_columns', 'name'), ('dependency_tables_on_foreignkeys', '_columns', 'table_id'), ('dependency_tables_on_foreignkeys', '_columns', 'type'), ('dependency_tables_on_foreignkeys', '_columns', 'type_digits'), ('dependency_tables_on_foreignkeys', '_columns', 'type_scale'), ('dependency_tables_on_indexes', '_columns', 'name'), ('dependency_tables_on_indexes', '_columns', 'number'), ('dependency_tables_on_indexes', '_columns', 'storage'), ('dependency_tables_on_indexes', '_columns', 'table_id'), ('dependency_tables_on_indexes', '_columns', 'type_digits'), ('dependency_tables_on_indexes', 'keys', 'id'), ('dependency_tables_on_triggers', 'keys', 'action'), ('dependency_tables_on_triggers', 'keys', 'name'), ('dependency_tables_on_triggers', 'keys', 'rkey'), ('dependency_tables_on_triggers', 'keys', 'type')));\n"
				"delete from sys.triggers where table_id not in (select id from sys._tables);\n"
				"commit;\n";
			assert(BATcount(b) == 1);
			printf("Running database upgrade commands:\n%s\n", query);
			fflush(stdout);
			err = SQLstatementIntern(c, query, "update", true, false, NULL);
			if (err == MAL_SUCCEED) {
				static const char query2[] =
					"create temporary table d as (select distinct * from sys.dependencies);\n"
					"delete from sys.dependencies;\n"
					"insert into sys.dependencies (select * from d);\n";
				printf("Running database upgrade commands:\n%s\n", query2);
				fflush(stdout);
				err = SQLstatementIntern(c, query2, "update", true, false, NULL);
			}
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);
	return err;
}

static str
sql_update_aug2024(Client c, mvc *sql, sql_schema *s)
{
	char *err;
	res_table *output;
	BAT *b;

	err = SQLstatementIntern(c, "SELECT id FROM sys.functions WHERE schema_id = 2000 AND name = 'describe_type' AND func LIKE '%sql_datatype%';\n", "update", true, false, &output);
	if (err)
		return err;
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) == 0) {
			/* do update */
			sql_table *t;
			static const char query1[] =
				"update sys._columns set type_digits = 7 where type = 'tinyint' and type_digits <> 7;\n"
				"update sys._columns set type_digits = 15 where type = 'smallint' and type_digits <> 15;\n"
				"update sys._columns set type_digits = 31 where type = 'int' and type_digits <> 31;\n"
				"update sys._columns set type_digits = 63 where type = 'bigint' and type_digits <> 63;\n"
				"update sys._columns set type_digits = 127 where type = 'hugeint' and type_digits <> 127;\n"
				"update sys._columns set type = 'varchar' where type in ('clob', 'char') and table_id in (select id from sys._tables where system and name <> 'netcdf_files');\n"
				"update sys.args set type_digits = 7 where type = 'tinyint' and type_digits <> 7;\n"
				"update sys.args set type_digits = 15 where type = 'smallint' and type_digits <> 15;\n"
				"update sys.args set type_digits = 31 where type = 'int' and type_digits <> 31;\n"
				"update sys.args set type_digits = 63 where type = 'bigint' and type_digits <> 63;\n"
				"update sys.args set type_digits = 127 where type = 'hugeint' and type_digits <> 127;\n"
				"update sys.args set type = 'varchar' where type in ('clob', 'char');\n"
				"drop aggregate median(decimal(18,3));\n"
				"drop aggregate median_avg(decimal(18,3));\n"
				"drop aggregate quantile(decimal(18,3), double);\n"
				"drop aggregate quantile_avg(decimal(18,3), double);\n"
				"create aggregate median(val DECIMAL(2)) returns DECIMAL(2)\n"
				" external name \"aggr\".\"median\";\n"
				"GRANT EXECUTE ON AGGREGATE median(DECIMAL(2)) TO PUBLIC;\n"
				"create aggregate median(val DECIMAL(4)) returns DECIMAL(4)\n"
				" external name \"aggr\".\"median\";\n"
				"GRANT EXECUTE ON AGGREGATE median(DECIMAL(4)) TO PUBLIC;\n"
				"create aggregate median(val DECIMAL(9)) returns DECIMAL(9)\n"
				" external name \"aggr\".\"median\";\n"
				"GRANT EXECUTE ON AGGREGATE median(DECIMAL(9)) TO PUBLIC;\n"
				"create aggregate median(val DECIMAL(18)) returns DECIMAL(18)\n"
				" external name \"aggr\".\"median\";\n"
				"GRANT EXECUTE ON AGGREGATE median(DECIMAL(18)) TO PUBLIC;\n"
				"create aggregate median_avg(val DECIMAL(2)) returns DOUBLE\n"
				" external name \"aggr\".\"median_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE median_avg(DECIMAL(2)) TO PUBLIC;\n"
				"create aggregate median_avg(val DECIMAL(4)) returns DOUBLE\n"
				" external name \"aggr\".\"median_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE median_avg(DECIMAL(4)) TO PUBLIC;\n"
				"create aggregate median_avg(val DECIMAL(9)) returns DOUBLE\n"
				" external name \"aggr\".\"median_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE median_avg(DECIMAL(9)) TO PUBLIC;\n"
				"create aggregate median_avg(val DECIMAL(18)) returns DOUBLE\n"
				" external name \"aggr\".\"median_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE median_avg(DECIMAL(18)) TO PUBLIC;\n"
				"create aggregate quantile(val DECIMAL(2), q DOUBLE) returns DECIMAL(2)\n"
				" external name \"aggr\".\"quantile\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile(DECIMAL(2), DOUBLE) TO PUBLIC;\n"
				"create aggregate quantile(val DECIMAL(4), q DOUBLE) returns DECIMAL(4)\n"
				" external name \"aggr\".\"quantile\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile(DECIMAL(4), DOUBLE) TO PUBLIC;\n"
				"create aggregate quantile(val DECIMAL(9), q DOUBLE) returns DECIMAL(9)\n"
				" external name \"aggr\".\"quantile\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile(DECIMAL(9), DOUBLE) TO PUBLIC;\n"
				"create aggregate quantile(val DECIMAL(18), q DOUBLE) returns DECIMAL(18)\n"
				" external name \"aggr\".\"quantile\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile(DECIMAL(18), DOUBLE) TO PUBLIC;\n"
				"create aggregate quantile_avg(val DECIMAL(2), q DOUBLE) returns DOUBLE\n"
				" external name \"aggr\".\"quantile_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile_avg(DECIMAL(2), DOUBLE) TO PUBLIC;\n"
				"create aggregate quantile_avg(val DECIMAL(4), q DOUBLE) returns DOUBLE\n"
				" external name \"aggr\".\"quantile_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile_avg(DECIMAL(4), DOUBLE) TO PUBLIC;\n"
				"create aggregate quantile_avg(val DECIMAL(9), q DOUBLE) returns DOUBLE\n"
				" external name \"aggr\".\"quantile_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile_avg(DECIMAL(9), DOUBLE) TO PUBLIC;\n"
				"create aggregate quantile_avg(val DECIMAL(18), q DOUBLE) returns DOUBLE\n"
				" external name \"aggr\".\"quantile_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile_avg(DECIMAL(18), DOUBLE) TO PUBLIC;\n"
				"drop function if exists sys.time_to_str(time with time zone, string) cascade;\n"
				"drop function if exists sys.timestamp_to_str(timestamp with time zone, string) cascade;\n"
				"create function time_to_str(d time, format string) returns string\n"
				" external name mtime.\"time_to_str\";\n"
				"create function time_to_str(d time with time zone, format string) returns string\n"
				" external name mtime.\"timetz_to_str\";\n"
				"create function timestamp_to_str(d timestamp with time zone, format string) returns string\n"
				" external name mtime.\"timestamptz_to_str\";\n"
				"grant execute on function time_to_str(time, string) to public;\n"
				"grant execute on function time_to_str(time with time zone, string) to public;\n"
				"grant execute on function timestamp_to_str(timestamp with time zone, string) to public;\n"
				"update sys.functions set system = true where not system and schema_id = 2000 and name in ('time_to_str', 'timestamp_to_str', 'median', 'median_avg', 'quantile', 'quantile_avg');\n"
				"drop function if exists sys.dump_database(boolean) cascade;\n"
				"drop view sys.dump_comments;\n"
				"drop view sys.dump_tables;\n"
				"drop view sys.dump_functions;\n"
				"drop view sys.dump_function_grants;\n"
				"drop function if exists sys.describe_columns(string, string) cascade;\n"
				"drop view sys.describe_functions;\n"
				"drop view sys.describe_privileges;\n"
				"drop view sys.describe_comments;\n"
				"drop view sys.fully_qualified_functions;\n"
				"drop view sys.describe_tables;\n"
				"drop function if exists sys.describe_type(string, integer, integer) cascade;\n"
				"CREATE FUNCTION sys.describe_type(ctype string, digits integer, tscale integer)\n"
				" RETURNS string\n"
				"BEGIN\n"
				" RETURN sys.sql_datatype(ctype, digits, tscale, false, false);\n"
				"END;\n"
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
				"CREATE VIEW sys.fully_qualified_functions AS\n"
				" WITH fqn(id, tpe, sig, num) AS\n"
				" (\n"
				" SELECT\n"
				" f.id,\n"
				" ft.function_type_keyword,\n"
				" CASE WHEN a.type IS NULL THEN\n"
				" sys.fqn(s.name, f.name) || '()'\n"
				" ELSE\n"
				" sys.fqn(s.name, f.name) || '(' || group_concat(sys.describe_type(a.type, a.type_digits, a.type_scale), ',') OVER (PARTITION BY f.id ORDER BY a.number)  || ')'\n"
				" END,\n"
				" a.number\n"
				" FROM sys.schemas s, sys.function_types ft, sys.functions f LEFT JOIN sys.args a ON f.id = a.func_id\n"
				" WHERE s.id= f.schema_id AND f.type = ft.function_type_id\n"
				" )\n"
				" SELECT\n"
				" fqn1.id id,\n"
				" fqn1.tpe tpe,\n"
				" fqn1.sig nme\n"
				" FROM\n"
				" fqn fqn1 JOIN (SELECT id, max(num) FROM fqn GROUP BY id)  fqn2(id, num)\n"
				" ON fqn1.id = fqn2.id AND (fqn1.num = fqn2.num OR fqn1.num IS NULL AND fqn2.num is NULL);\n"
				"CREATE VIEW sys.describe_comments AS\n"
				" SELECT o.id AS id, o.tpe AS tpe, o.nme AS fqn, cm.remark AS rem\n"
				" FROM (\n"
				" SELECT id, 'SCHEMA', sys.DQ(name) FROM sys.schemas WHERE NOT system\n"
				" UNION ALL\n"
				" SELECT t.id, ifthenelse(ts.table_type_name = 'VIEW', 'VIEW', 'TABLE'), sys.FQN(s.name, t.name)\n"
				" FROM sys.schemas s JOIN sys._tables t ON s.id = t.schema_id JOIN sys.table_types ts ON t.type = ts.table_type_id\n"
				" WHERE NOT t.system\n"
				" UNION ALL\n"
				" SELECT c.id, 'COLUMN', sys.FQN(s.name, t.name) || '.' || sys.DQ(c.name) FROM sys.columns c, sys._tables t, sys.schemas s WHERE NOT t.system AND c.table_id = t.id AND t.schema_id = s.id\n"
				" UNION ALL\n"
				" SELECT idx.id, 'INDEX', sys.FQN(s.name, idx.name) FROM sys.idxs idx, sys._tables t, sys.schemas s WHERE NOT t.system AND idx.table_id = t.id AND t.schema_id = s.id\n"
				" UNION ALL\n"
				" SELECT seq.id, 'SEQUENCE', sys.FQN(s.name, seq.name) FROM sys.sequences seq, sys.schemas s WHERE seq.schema_id = s.id\n"
				" UNION ALL\n"
				" SELECT f.id, ft.function_type_keyword, qf.nme FROM sys.functions f, sys.function_types ft, sys.schemas s, sys.fully_qualified_functions qf\n"
				" WHERE NOT f.system AND f.type = ft.function_type_id AND f.schema_id = s.id AND qf.id = f.id\n"
				" ) AS o(id, tpe, nme)\n"
				" JOIN sys.comments cm ON cm.id = o.id;\n"
				"CREATE VIEW sys.describe_privileges AS\n"
				" SELECT\n"
				" CASE\n"
				" WHEN o.tpe IS NULL AND pc.privilege_code_name = 'SELECT' THEN --GLOBAL privileges: SELECT maps to COPY FROM\n"
				" 'COPY FROM'\n"
				" WHEN o.tpe IS NULL AND pc.privilege_code_name = 'UPDATE' THEN --GLOBAL privileges: UPDATE maps to COPY INTO\n"
				" 'COPY INTO'\n"
				" ELSE\n"
				" o.nme\n"
				" END o_nme,\n"
				" coalesce(o.tpe, 'GLOBAL') o_tpe,\n"
				" pc.privilege_code_name p_nme,\n"
				" a.name a_nme,\n"
				" g.name g_nme,\n"
				" p.grantable grantable\n"
				" FROM\n"
				" sys.privileges p LEFT JOIN\n"
				" (\n"
				" SELECT t.id, s.name || '.' || t.name , 'TABLE'\n"
				" from sys.schemas s, sys.tables t where s.id = t.schema_id\n"
				" UNION ALL\n"
				" SELECT c.id, s.name || '.' || t.name || '.' || c.name, 'COLUMN'\n"
				" FROM sys.schemas s, sys.tables t, sys.columns c where s.id = t.schema_id AND t.id = c.table_id\n"
				" UNION ALL\n"
				" SELECT f.id, f.nme, f.tpe\n"
				" FROM sys.fully_qualified_functions f\n"
				" ) o(id, nme, tpe) ON o.id = p.obj_id,\n"
				" sys.privilege_codes pc,\n"
				" auths a, auths g\n"
				" WHERE\n"
				" p.privileges = pc.privilege_code_id AND\n"
				" p.auth_id = a.id AND\n"
				" p.grantor = g.id;\n"
				"CREATE VIEW sys.describe_functions AS\n"
				" WITH func_args_all(func_id, number, max_number, func_arg) AS\n"
				" (\n"
				" SELECT\n"
				" func_id,\n"
				" number,\n"
				" max(number) OVER (PARTITION BY func_id ORDER BY number DESC),\n"
				" group_concat(sys.dq(name) || ' ' || sys.describe_type(type, type_digits, type_scale),', ') OVER (PARTITION BY func_id ORDER BY number)\n"
				" FROM sys.args\n"
				" WHERE inout = 1\n"
				" ),\n"
				" func_args(func_id, func_arg) AS\n"
				" (\n"
				" SELECT func_id, func_arg\n"
				" FROM func_args_all\n"
				" WHERE number = max_number\n"
				" ),\n"
				" func_rets_all(func_id, number, max_number, func_ret, func_ret_type) AS\n"
				" (\n"
				" SELECT\n"
				" func_id,\n"
				" number,\n"
				" max(number) OVER (PARTITION BY func_id ORDER BY number DESC),\n"
				" group_concat(sys.dq(name) || ' ' || sys.describe_type(type, type_digits, type_scale),', ') OVER (PARTITION BY func_id ORDER BY number),\n"
				" group_concat(sys.describe_type(type, type_digits, type_scale),', ') OVER (PARTITION BY func_id ORDER BY number)\n"
				" FROM sys.args\n"
				" WHERE inout = 0\n"
				" ),\n"
				" func_rets(func_id, func_ret, func_ret_type) AS\n"
				" (\n"
				" SELECT\n"
				" func_id,\n"
				" func_ret,\n"
				" func_ret_type\n"
				" FROM func_rets_all\n"
				" WHERE number = max_number\n"
				" )\n"
				" SELECT\n"
				" f.id o,\n"
				" s.name sch,\n"
				" f.name fun,\n"
				" CASE WHEN f.language IN (1, 2) THEN f.func ELSE 'CREATE ' || ft.function_type_keyword || ' ' || sys.FQN(s.name, f.name) || '(' || coalesce(fa.func_arg, '') || ')' || CASE WHEN f.type = 5 THEN ' RETURNS TABLE (' || coalesce(fr.func_ret, '') || ')' WHEN f.type IN (1,3) THEN ' RETURNS ' || fr.func_ret_type ELSE '' END || CASE WHEN fl.language_keyword IS NULL THEN '' ELSE ' LANGUAGE ' || fl.language_keyword END || ' ' || f.func END def\n"
				" FROM sys.functions f\n"
				" LEFT OUTER JOIN func_args fa ON fa.func_id = f.id\n"
				" LEFT OUTER JOIN func_rets fr ON fr.func_id = f.id\n"
				" JOIN sys.schemas s ON f.schema_id = s.id\n"
				" JOIN sys.function_types ft ON f.type = ft.function_type_id\n"
				" LEFT OUTER JOIN sys.function_languages fl ON f.language = fl.language_id\n"
				" WHERE s.name <> 'tmp' AND NOT f.system;\n"
				"CREATE FUNCTION sys.describe_columns(schemaName string, tableName string)\n"
				" RETURNS TABLE(name string, type string, digits integer, scale integer, Nulls boolean, cDefault string, number integer, sqltype string, remark string)\n"
				"BEGIN\n"
				" RETURN SELECT c.name, c.\"type\", c.type_digits, c.type_scale, c.\"null\", c.\"default\", c.number, sys.describe_type(c.\"type\", c.type_digits, c.type_scale), com.remark\n"
				" FROM sys._tables t, sys.schemas s, sys._columns c\n"
				" LEFT OUTER JOIN sys.comments com ON c.id = com.id\n"
				" WHERE c.table_id = t.id\n"
				" AND t.name = tableName\n"
				" AND t.schema_id = s.id\n"
				" AND s.name = schemaName\n"
				" ORDER BY c.number;\n"
				"END;\n"
				"CREATE VIEW sys.dump_function_grants AS\n"
				" WITH func_args_all(func_id, number, max_number, func_arg) AS\n"
				" (SELECT a.func_id,\n"
				" a.number,\n"
				" max(a.number) OVER (PARTITION BY a.func_id ORDER BY a.number DESC),\n"
				" group_concat(sys.describe_type(a.type, a.type_digits, a.type_scale), ', ') OVER (PARTITION BY a.func_id ORDER BY a.number)\n"
				" FROM sys.args a\n"
				" WHERE a.inout = 1),\n"
				" func_args(func_id, func_arg) AS\n"
				" (SELECT func_id, func_arg FROM func_args_all WHERE number = max_number)\n"
				" SELECT\n"
				" 'GRANT ' || pc.privilege_code_name || ' ON ' || ft.function_type_keyword || ' '\n"
				" || sys.FQN(s.name, f.name) || '(' || coalesce(fa.func_arg, '') || ') TO '\n"
				" || ifthenelse(a.name = 'public', 'PUBLIC', sys.dq(a.name))\n"
				" || CASE WHEN p.grantable = 1 THEN ' WITH GRANT OPTION' ELSE '' END || ';' stmt,\n"
				" s.name schema_name,\n"
				" f.name function_name,\n"
				" a.name grantee\n"
				" FROM sys.schemas s,\n"
				" sys.functions f LEFT OUTER JOIN func_args fa ON f.id = fa.func_id,\n"
				" sys.auths a,\n"
				" sys.privileges p,\n"
				" sys.auths g,\n"
				" sys.function_types ft,\n"
				" sys.privilege_codes pc\n"
				" WHERE s.id = f.schema_id\n"
				" AND f.id = p.obj_id\n"
				" AND p.auth_id = a.id\n"
				" AND p.grantor = g.id\n"
				" AND p.privileges = pc.privilege_code_id\n"
				" AND f.type = ft.function_type_id\n"
				" AND NOT f.system\n"
				" ORDER BY s.name, f.name, a.name, g.name, p.grantable;\n"
				"CREATE VIEW sys.dump_functions AS\n"
				" SELECT f.o o, sys.schema_guard(f.sch, f.fun, f.def) stmt,\n"
				" f.sch schema_name,\n"
				" f.fun function_name\n"
				" FROM sys.describe_functions f;\n"
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
				"CREATE VIEW sys.dump_comments AS\n"
				" SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || sys.SQ(c.rem) || ';' stmt FROM sys.describe_comments c;\n"
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
				" --TODO loaders, procedures, window and filter sys.functions.\n"
				" --TODO look into order dependent group_concat\n"
				" INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'COMMIT;');\n"
				" RETURN sys.dump_statements;\n"
				"END;\n"
				"GRANT SELECT ON sys.describe_tables TO PUBLIC;\n"
				"GRANT SELECT ON sys.describe_comments TO PUBLIC;\n"
				"GRANT SELECT ON sys.fully_qualified_functions TO PUBLIC;\n"
				"GRANT SELECT ON sys.describe_privileges TO PUBLIC;\n"
				"GRANT SELECT ON sys.describe_functions TO PUBLIC;\n"
				"CREATE FUNCTION sys.check_constraint(sname STRING, cname STRING) RETURNS STRING EXTERNAL NAME sql.\"check\";\n"
				"grant execute on function sys.check_constraint to public;\n"
				"update sys.functions set system = true where not system and schema_id = 2000 and name in ('dump_database', 'describe_columns', 'describe_type', 'check_constraint');\n"
				"update sys._tables set system = true where not system and schema_id = 2000 and name in ('dump_comments', 'dump_tables', 'dump_functions', 'dump_function_grants', 'describe_functions', 'describe_privileges', 'describe_comments', 'fully_qualified_functions', 'describe_tables');\n";
			if ((t = mvc_bind_table(sql, s, "dump_comments")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "dump_tables")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "dump_functions")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "dump_function_grants")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "describe_functions")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "describe_privileges")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "describe_comments")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "fully_qualified_functions")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "describe_tables")) != NULL)
				t->system = 0;
			printf("Running database upgrade commands:\n%s\n", query1);
			fflush(stdout);
			err = SQLstatementIntern(c, query1, "update", true, false, NULL);
#ifdef HAVE_HGE
			if (err == MAL_SUCCEED) {
				sql_subtype tp;
				sql_find_subtype(&tp, "decimal", 38, 0);
				if (!sql_bind_func(sql, s->base.name, "median", &tp, NULL, F_AGGR, true, true)) {
					sql->session->status = 0;
					sql->errstr[0] = '\0';
					static const char query0[] =
						"create aggregate median(val DECIMAL(38)) returns DECIMAL(38)\n"
						" external name \"aggr\".\"median\";\n"
						"GRANT EXECUTE ON AGGREGATE median(DECIMAL(38)) TO PUBLIC;\n"
						"create aggregate median_avg(val DECIMAL(38)) returns DOUBLE\n"
						" external name \"aggr\".\"median_avg\";\n"
						"GRANT EXECUTE ON AGGREGATE median_avg(DECIMAL(38)) TO PUBLIC;\n"
						"create aggregate quantile(val DECIMAL(38), q DOUBLE) returns DECIMAL(38)\n"
						" external name \"aggr\".\"quantile\";\n"
						"GRANT EXECUTE ON AGGREGATE quantile(DECIMAL(38), DOUBLE) TO PUBLIC;\n"
						"create aggregate quantile_avg(val DECIMAL(38), q DOUBLE) returns DOUBLE\n"
						" external name \"aggr\".\"quantile_avg\";\n"
						"GRANT EXECUTE ON AGGREGATE quantile_avg(DECIMAL(38), DOUBLE) TO PUBLIC;\n"
						"grant execute on function sys.generate_series(hugeint, hugeint) to public;\n"
						"grant execute on function sys.generate_series(hugeint, hugeint, hugeint) to public;\n"
						"update sys.functions set system = true where system <> true and schema_id = 2000 and name in ('median', 'median_avg', 'quantile', 'quantile_avg');\n";
					printf("Running database upgrade commands:\n%s\n", query0);
					fflush(stdout);
					err = SQLstatementIntern(c, query0, "update", true, false, NULL);
				}
			}
#endif
			if (err == MAL_SUCCEED) {
				sql_subtype tp;
				sql_find_subtype(&tp, "smallint", 0, 0);
				if (!sql_bind_func(sql, s->base.name, "generate_series", &tp, &tp, F_UNION, true, true)) {
					sql->session->status = 0;
					sql->errstr[0] = '\0';
					static const char query[] =
						"create function sys.generate_series(first smallint, \"limit\" smallint)\n"
						"returns table (value smallint)\n"
						"external name generator.series;\n"
						"create function sys.generate_series(first smallint, \"limit\" smallint, stepsize smallint)\n"
						"returns table (value smallint)\n"
						"external name generator.series;\n"
						"update sys.functions set system = true where system <> true and name in ('generate_series') and schema_id = 2000;\n";
					printf("Running database upgrade commands:\n%s\n", query);
					fflush(stdout);
					err = SQLstatementIntern(c, query, "update", true, false, NULL);
				}
			}
			if (err == MAL_SUCCEED) {
				static const char query[] =
					"create function sys.generate_series(first date, \"limit\" date, stepsize interval month)\n"
					"returns table (value date)\n"
					"external name generator.series;\n"
					"create function sys.generate_series(first date, \"limit\" date, stepsize interval day)\n"
					"returns table (value date)\n"
					"external name generator.series;\n"
					"grant execute on function sys.generate_series(tinyint, tinyint) to public;\n"
					"grant execute on function sys.generate_series(tinyint, tinyint, tinyint) to public;\n"
					"grant execute on function sys.generate_series(smallint, smallint) to public;\n"
					"grant execute on function sys.generate_series(smallint, smallint, smallint) to public;\n"
					"grant execute on function sys.generate_series(int, int) to public;\n"
					"grant execute on function sys.generate_series(int, int, int) to public;\n"
					"grant execute on function sys.generate_series(bigint, bigint) to public;\n"
					"grant execute on function sys.generate_series(bigint, bigint, bigint) to public;\n"
					"grant execute on function sys.generate_series(real, real, real) to public;\n"
					"grant execute on function sys.generate_series(double, double, double) to public;\n"
					"grant execute on function sys.generate_series(decimal(10,2), decimal(10,2), decimal(10,2)) to public;\n"
					"grant execute on function sys.generate_series(date, date, interval month) to public;\n"
					"grant execute on function sys.generate_series(date, date, interval day) to public;\n"
					"grant execute on function sys.generate_series(timestamp, timestamp, interval second) to public;\n"
					"grant execute on function sys.generate_series(timestamp, timestamp, interval day) to public;\n"
					"update sys.functions set system = true where system <> true and name = 'generate_series' and schema_id = 2000;\n";
				sql->session->status = 0;
				sql->errstr[0] = '\0';
				printf("Running database upgrade commands:\n%s\n", query);
				fflush(stdout);
				err = SQLstatementIntern(c, query, "update", true, false, NULL);
			}
			if (err == MAL_SUCCEED) {
				static const char query[] =
					"drop view sys.sessions;\n"
					"drop function sys.sessions();\n"
					"create function sys.sessions()\n"
					" returns table(\n"
					"  \"sessionid\" int,\n"
					"  \"username\" string,\n"
					"  \"login\" timestamp,\n"
					"  \"idle\" timestamp,\n"
					"  \"optimizer\" string,\n"
					"  \"sessiontimeout\" int,\n"
					"  \"querytimeout\" int,\n"
					"  \"workerlimit\" int,\n"
					"  \"memorylimit\" int,\n"
					"  \"language\" string,\n"
					"  \"peer\" string,\n"
					"  \"hostname\" string,\n"
					"  \"application\" string,\n"
					"  \"client\" string,\n"
					"  \"clientpid\" bigint,\n"
					"  \"remark\" string\n"
					" )\n"
					" external name sql.sessions;\n"
					"create view sys.sessions as select * from sys.sessions();\n"
					"grant select on sys.sessions to public;\n"
					"create procedure sys.setclientinfo(property string, value string)\n"
					" external name clients.setinfo;\n"
					"grant execute on procedure sys.setclientinfo(string, string) to public;\n"
					"create table sys.clientinfo_properties(prop varchar(40) NOT NULL, session_attr varchar(40) NOT NULL);\n"
					"insert into sys.clientinfo_properties values\n"
					" ('ClientHostname', 'hostname'),\n"
					" ('ApplicationName', 'application'),\n"
					" ('ClientLibrary', 'client'),\n"
					" ('ClientPid', 'clientpid'),\n"
					" ('ClientRemark', 'remark');\n"
					"grant select on sys.clientinfo_properties to public;\n"
					"update sys.functions set system = true where schema_id = 2000 and name in ('setclientinfo', 'sessions');\n"
					"update sys._tables set system = true where schema_id = 2000 and name in ('clientinfo_properties', 'sessions');\n";

				t = mvc_bind_table(sql, s, "sessions");
				t->system = 0; /* make it non-system else the drop view will fail */
				printf("Running database upgrade commands:\n%s\n", query);
				fflush(stdout);
				err = SQLstatementIntern(c, query, "update", true, false, NULL);
			}
			if (err == MAL_SUCCEED) {
				static const char query[] = "alter table sys.clientinfo_properties SET READ ONLY;\n";
				printf("Running database upgrade commands:\n%s\n", query);
				fflush(stdout);
				err = SQLstatementIntern(c, query, "update", true, false, NULL);
			}
			if (err == MAL_SUCCEED) {
				static const char query[] =
					"DROP TABLE sys.key_types;\n"
					"CREATE TABLE sys.key_types (\n"
					"	key_type_id   SMALLINT NOT NULL PRIMARY KEY,\n"
					"	key_type_name VARCHAR(35) NOT NULL UNIQUE);\n"
					"INSERT INTO sys.key_types VALUES\n"
					"(0, 'Primary Key'),\n"
					"(1, 'Unique Key'),\n"
					"(2, 'Foreign Key'),\n"
					"(3, 'Unique Key With Nulls Not Distinct'),\n"
					"(4, 'Check Constraint');\n"
					"GRANT SELECT ON sys.key_types TO PUBLIC;\n"
					"UPDATE sys._tables SET system = true WHERE schema_id = 2000 AND name = 'key_types';\n";
				if ((t = mvc_bind_table(sql, s, "key_types")) != NULL)
					t->system = 0;
				printf("Running database upgrade commands:\n%s\n", query);
				fflush(stdout);
				err = SQLstatementIntern(c, query, "update", true, false, NULL);
			}
			if (err == MAL_SUCCEED) {
				static const char query[] = "ALTER TABLE sys.key_types SET READ ONLY;\n";
				printf("Running database upgrade commands:\n%s\n", query);
				fflush(stdout);
				err = SQLstatementIntern(c, query, "update", true, false, NULL);
			}
			if (err == MAL_SUCCEED) {
				static const char query[] =
					"DROP VIEW information_schema.check_constraints CASCADE;\n"
					"DROP VIEW information_schema.table_constraints CASCADE;\n"
					"CREATE VIEW INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS SELECT\n"
					"  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,\n"
					"  s.\"name\" AS CONSTRAINT_SCHEMA,\n"
					"  k.\"name\" AS CONSTRAINT_NAME,\n"
					"  cast(sys.check_constraint(s.\"name\", k.\"name\") AS varchar(2048)) AS CHECK_CLAUSE,\n"
					"  t.\"schema_id\" AS schema_id,\n"
					"  t.\"id\" AS table_id,\n"
					"  t.\"name\" AS table_name,\n"
					"  k.\"id\" AS key_id\n"
					" FROM (SELECT sk.\"id\", sk.\"table_id\", sk.\"name\" FROM sys.\"keys\" sk WHERE sk.\"type\" = 4 UNION ALL SELECT tk.\"id\", tk.\"table_id\", tk.\"name\" FROM tmp.\"keys\" tk WHERE tk.\"type\" = 4) k\n"
					" INNER JOIN (SELECT st.\"id\", st.\"schema_id\", st.\"name\" FROM sys.\"_tables\" st UNION ALL SELECT tt.\"id\", tt.\"schema_id\", tt.\"name\" FROM tmp.\"_tables\" tt) t ON k.\"table_id\" = t.\"id\"\n"
					" INNER JOIN sys.\"schemas\" s ON t.\"schema_id\" = s.\"id\"\n"
					" ORDER BY s.\"name\", t.\"name\", k.\"name\";\n"
					"GRANT SELECT ON TABLE INFORMATION_SCHEMA.CHECK_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;\n"

					"CREATE VIEW INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS SELECT\n"
					"  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,\n"
					"  s.\"name\" AS CONSTRAINT_SCHEMA,\n"
					"  k.\"name\" AS CONSTRAINT_NAME,\n"
					"  cast(NULL AS varchar(1)) AS TABLE_CATALOG,\n"
					"  s.\"name\" AS TABLE_SCHEMA,\n"
					"  t.\"name\" AS TABLE_NAME,\n"
					"  cast(CASE k.\"type\" WHEN 0 THEN 'PRIMARY KEY' WHEN 1 THEN 'UNIQUE' WHEN 2 THEN 'FOREIGN KEY' WHEN 3 THEN 'UNIQUE NULLS NOT DISTINCT' WHEN 4 THEN 'CHECK' ELSE NULL END AS varchar(26)) AS CONSTRAINT_TYPE,\n"
					"  cast('NO' AS varchar(3)) AS IS_DEFERRABLE,\n"
					"  cast('NO' AS varchar(3)) AS INITIALLY_DEFERRED,\n"
					"  cast('YES' AS varchar(3)) AS ENFORCED,\n"
					"  t.\"schema_id\" AS schema_id,\n"
					"  t.\"id\" AS table_id,\n"
					"  k.\"id\" AS key_id,\n"
					"  k.\"type\" AS key_type,\n"
					"  t.\"system\" AS is_system\n"
					" FROM (SELECT sk.\"id\", sk.\"table_id\", sk.\"name\", sk.\"type\" FROM sys.\"keys\" sk UNION ALL SELECT tk.\"id\", tk.\"table_id\", tk.\"name\", tk.\"type\" FROM tmp.\"keys\" tk) k\n"
					" INNER JOIN (SELECT st.\"id\", st.\"schema_id\", st.\"name\", st.\"system\" FROM sys.\"_tables\" st UNION ALL SELECT tt.\"id\", tt.\"schema_id\", tt.\"name\", tt.\"system\" FROM tmp.\"_tables\" tt) t ON k.\"table_id\" = t.\"id\"\n"
					" INNER JOIN sys.\"schemas\" s ON t.\"schema_id\" = s.\"id\"\n"
					" ORDER BY s.\"name\", t.\"name\", k.\"name\";\n"
					"GRANT SELECT ON TABLE INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;\n"
					"\n"
					"UPDATE sys._tables SET system = true where system <> true\n"
					" and schema_id = (select s.id from sys.schemas s where s.name = 'information_schema')\n"
					" and name in ('check_constraints','table_constraints');\n";
				sql_schema *infoschema = mvc_bind_schema(sql, "information_schema");
				if ((t = mvc_bind_table(sql, infoschema, "check_constraints")) != NULL)
					t->system = 0; /* make it non-system else the drop view will fail */
				if ((t = mvc_bind_table(sql, infoschema, "table_constraints")) != NULL)
					t->system = 0;
				printf("Running database upgrade commands:\n%s\n", query);
				fflush(stdout);
				err = SQLstatementIntern(c, query, "update", true, false, NULL);
			}
			if (err == MAL_SUCCEED) {
				sql_subtype tp;
				sql_find_subtype(&tp, "varchar", 0, 0);
				if (sql_bind_func(sql, s->base.name, "gzcompress", &tp, &tp, F_PROC, true, true)) {
					static const char query[] =
						"drop procedure if exists sys.gzcompress(string, string);\n"
						"drop procedure if exists sys.gzdecompress(string, string);\n"
						"drop procedure if exists sys.gztruncate(string, string);\n"
						"drop procedure if exists sys.gzexpand(string, string);\n";
					printf("Running database upgrade commands:\n%s\n", query);
					fflush(stdout);
					err = SQLstatementIntern(c, query, "update", true, false, NULL);
				} else {
					sql->session->status = 0;
					sql->errstr[0] = '\0';
				}
			}
			if (err == MAL_SUCCEED) {
				sql_subtype tp;
				sql_find_subtype(&tp, "varchar", 0, 0);
				if (sql_bind_func(sql, s->base.name, "newdictionary", &tp, &tp, F_PROC, true, true)) {
					static const char query[] =
						"drop procedure if exists sys.newdictionary(string, string);\n"
						"drop procedure if exists sys.dropdictionary(string, string);\n"
						"drop procedure if exists sys.initializedictionary();\n";
					printf("Running database upgrade commands:\n%s\n", query);
					fflush(stdout);
					err = SQLstatementIntern(c, query, "update", true, false, NULL);
				} else {
					sql->session->status = 0;
					sql->errstr[0] = '\0';
				}
			}
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);

	return err;
}

static str
sql_update_mar2025_geom(Client c, mvc *sql, sql_schema *s)
{
	str err;
	res_table *output = NULL;

	if ((err = SQLstatementIntern(c, "select * from sys._tables where name = 'geometry_columns' and schema_id = 2000 and query like '%''wkba''%';\n", "update", true, false, &output)) == MAL_SUCCEED) {
		BAT *b;
		if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) > 0) {
			sql_table *t = mvc_bind_table(sql, s, "geometry_columns");
			t->system = 0;
			static const char query[] = "drop view sys.geometry_columns;\n"
				"create view sys.geometry_columns as\n"
				" select cast(null as varchar(1)) as f_table_catalog,\n"
				" s.name as f_table_schema,\n"
				" t.name as f_table_name,\n"
				" c.name as f_geometry_column,\n"
				" cast(has_z(c.type_digits) + has_m(c.type_digits) +2 as integer) as coord_dimension,\n"
				" c.type_scale as srid,\n"
				" get_type(c.type_digits, 0) as geometry_type\n"
				" from sys.columns c, sys.tables t, sys.schemas s\n"
				" where c.table_id = t.id and t.schema_id = s.id\n"
				" and c.type in (select sqlname from sys.types where systemname = 'wkb');\n"
				"GRANT SELECT ON sys.geometry_columns TO PUBLIC;\n"
				"update sys._tables set system = true where system <> true and schema_id = 2000 and name = 'geometry_columns';\n"
				/* also clean up sys.privileges after drop of
				 * sys.st_interiorrings that was done in C in
				 * bat_logger.c */
				"delete from sys.privileges where (obj_id) not in (select id from (SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions) as t);\n";
			printf("Running database upgrade commands:\n%s\n", query);
			fflush(stdout);
			err = SQLstatementIntern(c, query, "update", true, false, NULL);
		}
		res_table_destroy(output);
	}
	return err;
}

static str
sql_update_mar2025(Client c, mvc *sql, sql_schema *s)
{
	char *err = MAL_SUCCEED;
	sql_subtype tp;
	res_table *output = NULL;

	sql_find_subtype(&tp, "varchar", 0, 0);
	if (!sql_bind_func(sql, s->base.name, "vacuum", &tp, &tp, F_PROC, true, true)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
		sql_table *t;
		t = mvc_bind_table(sql, s, "ids");
		t->system = 0;
		t = mvc_bind_table(sql, s, "dependencies_vw");
		t->system = 0;
		static const char query[] =
			"create procedure sys.vacuum(sname string, tname string)\n"
			"external name sql.vacuum;\n"
			"create procedure sys.vacuum(sname string, tname string, interval int)\n"
			"external name sql.vacuum;\n"
			"create procedure sys.stop_vacuum(sname string, tname string)\n"
			"external name sql.stop_vacuum;\n"
			"create function sys.unclosed_result_sets()\n"
			"returns table(\n"
			"	\"query_id\" oid,\n"
			"	\"res_id\" int\n"
			")\n"
			"external name sql.unclosed_result_sets;\n"
			"grant execute on function sys.unclosed_result_sets() to public;\n"
			"update sys.functions set system = true where system <> true and schema_id = 2000 and name in ('vacuum', 'stop_vacuum', 'unclosed_result_sets');\n"
			"drop view sys.dependencies_vw cascade;\n"
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
			"SELECT id, sqlname, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'type', 'sys.types', (sqlname in ('inet','json','url','uuid')) FROM sys.types\n"
			" ORDER BY id;\n"
			"GRANT SELECT ON sys.ids TO PUBLIC;\n"
			"CREATE VIEW sys.dependencies_vw AS\n"
			"SELECT d.id, i1.obj_type, i1.name,\n"
			"       d.depend_id as used_by_id, i2.obj_type as used_by_obj_type, i2.name as used_by_name,\n"
			"       d.depend_type, dt.dependency_type_name\n"
			"  FROM sys.dependencies d\n"
			"  JOIN sys.ids i1 ON d.id = i1.id\n"
			"  JOIN sys.ids i2 ON d.depend_id = i2.id\n"
			"  JOIN sys.dependency_types dt ON d.depend_type = dt.dependency_type_id\n"
			" ORDER BY id, depend_id;\n"
			"GRANT SELECT ON sys.dependencies_vw TO PUBLIC;\n"
			"update sys._tables set system = true where system <> true and schema_id = 2000 and name in ('ids', 'dependencies_vw');\n";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		if (err)
			return err;
	}

	if ((err = SQLstatementIntern(c, "select id from sys.functions where name = 'quantile' and schema_id = 2000 and contains(func, 'ordered');\n", "update", true, false, &output)) == MAL_SUCCEED) {
		BAT *b;
		if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) <= 2) {
			sql_table *t;
			t = mvc_bind_table(sql, s, "describe_comments");
			t->system = 0;
			t = mvc_bind_table(sql, s, "describe_functions");
			t->system = 0;
			t = mvc_bind_table(sql, s, "describe_privileges");
			t->system = 0;
			t = mvc_bind_table(sql, s, "dump_comments");
			t->system = 0;
			t = mvc_bind_table(sql, s, "dump_functions");
			t->system = 0;
			t = mvc_bind_table(sql, s, "fully_qualified_functions");
			t->system = 0;
			sql_schema *is = mvc_bind_schema(sql, "information_schema");
			t = mvc_bind_table(sql, is, "parameters");
			t->system = 0;
			char query[] = "update sys.functions set func = replace(func, E'\\n external', E' ordered\\n external') where name in ('quantile','quantile_avg','median','median_avg') and schema_id = 2000 and language = (select language_id from sys.function_languages where language_name = 'MAL') and type = (select function_type_id from sys.function_types where function_type_keyword = 'AGGREGATE') and not contains(func, 'ordered');\n"
				"update sys.functions set func = replace(func, E'\\n\\texternal', E' ordered\\n external') where name in ('quantile','quantile_avg','median','median_avg') and schema_id = 2000 and language = (select language_id from sys.function_languages where language_name = 'MAL') and type = (select function_type_id from sys.function_types where function_type_keyword = 'AGGREGATE') and not contains(func, 'ordered');\n"
				"update sys.functions set func = replace(func, E'\\nexternal', E' ordered\\n external') where name in ('quantile','quantile_avg','median','median_avg') and schema_id = 2000 and language = (select language_id from sys.function_languages where language_name = 'MAL') and type = (select function_type_id from sys.function_types where function_type_keyword = 'AGGREGATE') and not contains(func, 'ordered');\n"
				"update sys.functions set func = replace(func, E' external', E' with order\\n external') where name = 'group_concat' and schema_id = 2000 and language = (select language_id from sys.function_languages where language_name = 'MAL') and type = (select function_type_id from sys.function_types where function_type_keyword = 'AGGREGATE');\n"
				"drop function sys.dump_database(boolean) cascade;\n"
				"drop view sys.dump_functions cascade;\n"
				"drop view sys.dump_comments cascade;\n"
				"drop view sys.describe_comments cascade;\n"
				"drop view sys.describe_privileges cascade;\n"
				"drop view sys.fully_qualified_functions cascade;\n"
				"drop view sys.describe_functions cascade;\n"
				"CREATE VIEW sys.fully_qualified_functions AS\n"
				" SELECT\n"
				"  f.id id,\n"
				"  ft.function_type_keyword tpe,\n"
				"  sys.fqn(s.name, f.name) || '(' || group_concat(sys.describe_type(a.type, a.type_digits, a.type_scale), ',' order by a.number)  || ')' nme\n"
				" FROM sys.schemas s, sys.function_types ft, sys.functions f JOIN sys.args a ON f.id = a.func_id\n"
				" WHERE s.id= f.schema_id AND f.type = ft.function_type_id\n"
				" group by f.id, ft.function_type_keyword, f.name, s.name\n"
				" UNION\n"
				" SELECT f.id id,\n"
				"  ft.function_type_keyword tpe,\n"
				"  sys.fqn(s.name, f.name) || '()' nme\n"
				" FROM sys.schemas s, sys.function_types ft, sys.functions f\n"
				" WHERE s.id= f.schema_id AND f.type = ft.function_type_id and f.id not in ( select func_id from sys.args )\n"
				" group by f.id, ft.function_type_keyword, f.name, s.name;\n"
				"CREATE VIEW sys.describe_comments AS\n"
				" SELECT o.id AS id, o.tpe AS tpe, o.nme AS fqn, cm.remark AS rem\n"
				" FROM (\n"
				"  SELECT id, 'SCHEMA', sys.DQ(name) FROM sys.schemas WHERE NOT system\n"
				"  UNION ALL\n"
				"  SELECT t.id, ifthenelse(ts.table_type_name = 'VIEW', 'VIEW', 'TABLE'), sys.FQN(s.name, t.name)\n"
				"    FROM sys.schemas s JOIN sys._tables t ON s.id = t.schema_id JOIN sys.table_types ts ON t.type = ts.table_type_id\n"
				"   WHERE NOT t.system\n"
				"  UNION ALL\n"
				"  SELECT c.id, 'COLUMN', sys.FQN(s.name, t.name) || '.' || sys.DQ(c.name) FROM sys.columns c, sys._tables t, sys.schemas s WHERE NOT t.system AND c.table_id = t.id AND t.schema_id = s.id\n"
				"  UNION ALL\n"
				"  SELECT idx.id, 'INDEX', sys.FQN(s.name, idx.name) FROM sys.idxs idx, sys._tables t, sys.schemas s WHERE NOT t.system AND idx.table_id = t.id AND t.schema_id = s.id\n"
				"  UNION ALL\n"
				"  SELECT seq.id, 'SEQUENCE', sys.FQN(s.name, seq.name) FROM sys.sequences seq, sys.schemas s WHERE seq.schema_id = s.id\n"
				"  UNION ALL\n"
				"  SELECT f.id, ft.function_type_keyword, qf.nme FROM sys.functions f, sys.function_types ft, sys.schemas s, sys.fully_qualified_functions qf\n"
				"   WHERE NOT f.system AND f.type = ft.function_type_id AND f.schema_id = s.id AND qf.id = f.id\n"
				"  ) AS o(id, tpe, nme)\n"
				" JOIN sys.comments cm ON cm.id = o.id;\n"
				"CREATE VIEW sys.describe_privileges AS\n"
				" SELECT\n"
				"  CASE\n"
				"   WHEN o.tpe IS NULL AND pc.privilege_code_name = 'SELECT' THEN\n"
				"    'COPY FROM'\n"
				"   WHEN o.tpe IS NULL AND pc.privilege_code_name = 'UPDATE' THEN\n"
				"    'COPY INTO'\n"
				"   ELSE\n"
				"    o.nme\n"
				"  END o_nme,\n"
				"  coalesce(o.tpe, 'GLOBAL') o_tpe,\n"
				"  pc.privilege_code_name p_nme,\n"
				"  a.name a_nme,\n"
				"  g.name g_nme,\n"
				"  p.grantable grantable\n"
				" FROM\n"
				"  sys.privileges p LEFT JOIN\n"
				"  (\n"
				"  SELECT t.id, s.name || '.' || t.name , 'TABLE'\n"
				"   from sys.schemas s, sys.tables t where s.id = t.schema_id\n"
				"  UNION ALL\n"
				"   SELECT c.id, s.name || '.' || t.name || '.' || c.name, 'COLUMN'\n"
				"   FROM sys.schemas s, sys.tables t, sys.columns c where s.id = t.schema_id AND t.id = c.table_id\n"
				"  UNION ALL\n"
				"   SELECT f.id, f.nme, f.tpe\n"
				"   FROM sys.fully_qualified_functions f\n"
				"  ) o(id, nme, tpe) ON o.id = p.obj_id,\n"
				"  sys.privilege_codes pc,\n"
				"  auths a, auths g\n"
				" WHERE\n"
				"  p.privileges = pc.privilege_code_id AND\n"
				"  p.auth_id = a.id AND\n"
				"  p.grantor = g.id;\n"
				"CREATE VIEW sys.describe_functions AS\n"
				" WITH func_args(func_id, func_arg) AS\n"
				" (\n"
				"  SELECT\n"
				"   func_id,\n"
				"   group_concat(sys.dq(name) || ' ' || sys.describe_type(type, type_digits, type_scale),', ' order by number)\n"
				"  FROM sys.args\n"
				"  WHERE inout = 1\n"
				"  group by func_id\n"
				" ),\n"
				" func_rets(func_id, func_ret, func_ret_type) AS\n"
				" (\n"
				"  SELECT\n"
				"   func_id,\n"
				"   group_concat(sys.dq(name) || ' ' || sys.describe_type(type, type_digits, type_scale),', ' order by number),\n"
				"   group_concat(sys.describe_type(type, type_digits, type_scale),', ' order by number)\n"
				"  FROM sys.args\n"
				"  WHERE inout = 0\n"
				"  group by func_id\n"
				" )\n"
				" SELECT\n"
				"  f.id o,\n"
				"  s.name sch,\n"
				"  f.name fun,\n"
				"  CASE WHEN f.language IN (1, 2) THEN f.func ELSE 'CREATE ' || ft.function_type_keyword || ' ' || sys.FQN(s.name, f.name) || '(' || coalesce(fa.func_arg, '') || ')' || CASE WHEN f.type = 5 THEN ' RETURNS TABLE (' || coalesce(fr.func_ret, '') || ')' WHEN f.type IN (1,3) THEN ' RETURNS ' || fr.func_ret_type ELSE '' END || CASE WHEN fl.language_keyword IS NULL THEN '' ELSE ' LANGUAGE ' || fl.language_keyword END || ' ' || f.func END def\n"
				" FROM sys.functions f\n"
				"  LEFT OUTER JOIN func_args fa ON fa.func_id = f.id\n"
				"  LEFT OUTER JOIN func_rets fr ON fr.func_id = f.id\n"
				"  JOIN sys.schemas s ON f.schema_id = s.id\n"
				"  JOIN sys.function_types ft ON f.type = ft.function_type_id\n"
				"  LEFT OUTER JOIN sys.function_languages fl ON f.language = fl.language_id\n"
				" WHERE s.name <> 'tmp' AND NOT f.system;\n"
				"GRANT SELECT ON sys.describe_comments TO PUBLIC;\n"
				"GRANT SELECT ON sys.fully_qualified_functions TO PUBLIC;\n"
				"GRANT SELECT ON sys.describe_privileges TO PUBLIC;\n"
				"GRANT SELECT ON sys.describe_functions TO PUBLIC;\n"
				"CREATE VIEW sys.dump_functions AS\n"
				"  SELECT f.o o, sys.schema_guard(f.sch, f.fun, f.def) stmt,\n"
				"  f.sch schema_name,\n"
				"  f.fun function_name\n"
				"    FROM sys.describe_functions f;\n"
				"CREATE VIEW sys.dump_comments AS\n"
				"  SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || sys.SQ(c.rem) || ';' stmt FROM sys.describe_comments c;\n"
				"CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)\n"
				"BEGIN\n"
				"  SET SCHEMA sys;\n"
				"  TRUNCATE sys.dump_statements;\n"
				"  INSERT INTO sys.dump_statements VALUES (1, 'START TRANSACTION;');\n"
				"  INSERT INTO sys.dump_statements VALUES (2, 'SET SCHEMA \"sys\";');\n"
				"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_roles;\n"
				"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_users;\n"
				"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_schemas;\n"
				"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_user_defined_types;\n"
				"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_add_schemas_to_users;\n"
				"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_grant_user_privileges;\n"
				"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_sequences;\n"
				"  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(ORDER BY stmts.o), stmts.s\n"
				"        FROM (\n"
				"          SELECT f.o, f.stmt FROM sys.dump_functions f\n"
				"           UNION ALL\n"
				"          SELECT t.o, t.stmt FROM sys.dump_tables t\n"
				"        ) AS stmts(o, s);\n"
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
				"END;\n"
				"update sys.functions set system = true where not system and schema_id = 2000 and name = 'dump_database';\n"
				"update sys._tables set system = true where not system and schema_id = 2000 and name in ('dump_comments', 'dump_functions', 'describe_functions', 'describe_privileges', 'describe_comments', 'fully_qualified_functions');\n"
				"drop view information_schema.parameters cascade;\n"
				"CREATE VIEW INFORMATION_SCHEMA.PARAMETERS AS SELECT\n"
				" cast(NULL AS varchar(1)) AS SPECIFIC_CATALOG,\n"
				" s.\"name\" AS SPECIFIC_SCHEMA,\n"
				" cast(f.\"name\"||'('||f.\"id\"||')' AS varchar(270)) AS SPECIFIC_NAME, -- TODO: replace with full routine signature string. Note sys.fully_qualified_functions.nme does not produce the correct signature.\n"
				" cast(sys.ifthenelse((a.\"inout\" = 0 OR f.\"type\" = 2), 1 + a.\"number\", sys.ifthenelse(f.\"type\" = 1, a.\"number\", (1 + a.\"number\" - f.count_out_cols))) AS int) AS ORDINAL_POSITION,\n"
				" cast(sys.ifthenelse(a.\"inout\" = 0, 'OUT', sys.ifthenelse(a.\"inout\" = 1, 'IN', 'INOUT')) as varchar(5)) AS PARAMETER_MODE,  -- we do not yet support INOUT\n"
				" cast(sys.ifthenelse(a.\"inout\" = 0, 'YES', 'NO') as varchar(3)) AS IS_RESULT,\n"
				" cast(NULL AS varchar(1)) AS AS_LOCATOR,\n"
				" a.\"name\" AS PARAMETER_NAME,\n"
				" cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_CATALOG,\n"
				" cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_SCHEMA,\n"
				" cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_NAME,\n"
				" cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_CATALOG,\n"
				" cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_SCHEMA,\n"
				" cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_NAME,\n"
				" cast(sys.\"sql_datatype\"(a.\"type\", a.\"type_digits\", a.\"type_scale\", true, true) AS varchar(1024)) AS DATA_TYPE,\n"
				" cast(sys.ifthenelse(a.\"type\" IN ('varchar','clob','char','json','url','xml') AND a.\"type_digits\" > 0, a.\"type_digits\", NULL) AS int) AS CHARACTER_MAXIMUM_LENGTH,\n"
				" cast(sys.ifthenelse(a.\"type\" IN ('varchar','clob','char','json','url','xml') AND a.\"type_digits\" > 0, 4 * cast(a.\"type_digits\" as bigint), NULL) AS bigint) AS CHARACTER_OCTET_LENGTH,\n"
				" cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,\n"
				" cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,\n"
				" cast(sys.ifthenelse(a.\"type\" IN ('varchar','clob','char','json','url','xml'), 'UTF-8', NULL) AS varchar(16)) AS CHARACTER_SET_NAME,\n"
				" cast(NULL AS varchar(1)) AS COLLATION_CATALOG,\n"
				" cast(NULL AS varchar(1)) AS COLLATION_SCHEMA,\n"
				" cast(NULL AS varchar(1)) AS COLLATION_NAME,\n"
				" cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a.\"type_digits\", NULL) AS int) AS NUMERIC_PRECISION,\n"
				" cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','oid'), 2, sys.ifthenelse(a.\"type\" IN ('decimal','numeric'), 10, NULL)) AS int) AS NUMERIC_PRECISION_RADIX,\n"
				" cast(sys.ifthenelse(a.\"type\" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a.\"type_scale\", NULL) AS int) AS NUMERIC_SCALE,\n"
				" cast(sys.ifthenelse(a.\"type\" IN ('date','timestamp','timestamptz','time','timetz'), sys.ifthenelse(a.\"type_scale\" > 0, a.\"type_scale\" -1, 0), NULL) AS int) AS DATETIME_PRECISION,\n"
				" cast(sys.ifthenelse(a.\"type\" IN ('day_interval','month_interval','sec_interval'), sys.\"sql_datatype\"(a.\"type\", a.\"type_digits\", a.\"type_scale\", true, true), NULL) AS varchar(40)) AS INTERVAL_TYPE,\n"
				" cast(CASE a.\"type\" WHEN 'day_interval' THEN 0 WHEN 'month_interval' THEN 0 WHEN 'sec_interval' THEN (sys.ifthenelse(a.\"type_digits\" IN (7, 10, 12, 13), sys.ifthenelse(a.\"type_scale\" > 0, a.\"type_scale\", 3), 0)) ELSE NULL END AS int) AS INTERVAL_PRECISION,\n"
				" cast(NULL AS varchar(1)) AS UDT_CATALOG,\n"
				" cast(NULL AS varchar(1)) AS UDT_SCHEMA,\n"
				" cast(NULL AS varchar(1)) AS UDT_NAME,\n"
				" cast(NULL AS varchar(1)) AS SCOPE_CATALOG,\n"
				" cast(NULL AS varchar(1)) AS SCOPE_SCHEMA,\n"
				" cast(NULL AS varchar(1)) AS SCOPE_NAME,\n"
				" cast(NULL AS int) AS MAXIMUM_CARDINALITY,\n"
				" cast(NULL AS varchar(1)) AS DTD_IDENTIFIER,\n"
				" cast(NULL AS varchar(1)) AS DECLARED_DATA_TYPE,\n"
				" cast(NULL AS int) AS DECLARED_NUMERIC_PRECISION,\n"
				" cast(NULL AS int) AS DECLARED_NUMERIC_SCALE,\n"
				" cast(NULL AS varchar(1)) AS PARAMETER_DEFAULT,\n"
				" -- MonetDB column extensions\n"
				" f.\"schema_id\" AS schema_id,\n"
				" f.\"id\" AS function_id,\n"
				" a.\"id\" AS arg_id,\n"
				" f.\"name\" AS function_name,\n"
				" f.\"type\" AS function_type,\n"
				" f.\"system\" AS is_system\n"
				" FROM sys.\"args\" a\n"
				" INNER JOIN (SELECT fun.id, fun.schema_id, fun.name, fun.type, fun.system, (select count(*) from sys.args a0 where a0.inout = 0 and a0.func_id = fun.id) as count_out_cols FROM sys.\"functions\" fun WHERE fun.\"type\" in (1, 2, 5, 7)) f ON f.\"id\" = a.\"func_id\"\n"
				" INNER JOIN sys.\"schemas\" s ON s.\"id\" = f.\"schema_id\"\n"
				" ORDER BY s.\"name\", f.\"name\", f.\"id\", a.\"inout\" DESC, a.\"number\";\n"
				"GRANT SELECT ON TABLE INFORMATION_SCHEMA.PARAMETERS TO PUBLIC WITH GRANT OPTION;\n"
				"update sys._tables set system = true where not system and schema_id = (select id from sys.schemas where name = 'information_schema') and name = 'parameters';\n";
			printf("Running database upgrade commands:\n%s\n", query);
			fflush(stdout);
			err = SQLstatementIntern(c, query, "update", true, false, NULL);
		}
		res_table_destroy(output);
		if (err)
			return err;
	}

	sql_find_subtype(&tp, "date", 0, 0);
	if (!sql_bind_func(sql, s->base.name, "dayname", &tp, NULL, F_FUNC, true, true)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
		static const char query[] = "create function dayname(d date) returns varchar(10) return date_to_str(d, '%A');\n"
			"create function monthname(d date) returns varchar(10) return date_to_str(d, '%B');\n"
			"grant execute on function dayname(date) to public;\n"
			"grant execute on function monthname(date) to public;\n"
			"update sys.functions set system = true where system <> true and name in ('dayname', 'monthname') and schema_id = 2000 and type = (select function_type_id from sys.function_types where function_type_name = 'Scalar function');\n";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		if (err)
			return err;
	}

	sql_find_subtype(&tp, "varchar", 0, 0);
	if (!sql_bind_func(sql, s->base.name, "normalize_monetdb_url", &tp, NULL, F_FUNC, true, true)) {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
		static const char query[] =
			"create function sys.normalize_monetdb_url(u string)\n"
			"returns string external name sql.normalize_monetdb_url;\n"
			"grant execute on function sys.normalize_monetdb_url(string) to public;\n"
			"update sys.functions set system = true where system <> true and name = 'normalize_monetdb_url' and schema_id = 2000;\n"
			"update sys._tables set query = sys.normalize_monetdb_url(query) where type in (5,6);";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		if (err)
			return err;
	}

	return err;
}

static str
sql_update_mar2025_sp1(Client c, mvc *sql)
{
	char *err = NULL;
	res_table *output = NULL;
	BAT *b;

	/* 10_sys_schema_extension.sql */
	/* if the table type LOCAL TEMPORARY VIEW is
	 * not in the list of table_types, upgrade */
	static const char query1[] = "select table_type_name from sys.table_types where table_type_name = 'LOCAL TEMPORARY VIEW';";
	err = SQLstatementIntern(c, query1, "update", true, false, &output);
	if (err == MAL_SUCCEED && (b = BBPquickdesc(output->cols[0].b)) && BATcount(b) == 0) {
		static const char stmt2a[] =
			"ALTER TABLE sys.table_types SET READ WRITE;\n"
			"COMMIT;\n";
		printf("Running database upgrade commands:\n%s\n", stmt2a);
		fflush(stdout);
		err = SQLstatementIntern(c, stmt2a, "update", true, false, NULL);
		if (err == MAL_SUCCEED) {
			static const char stmt2b[] =
				"INSERT INTO sys.table_types VALUES (31, 'LOCAL TEMPORARY VIEW');\n"
				"COMMIT;\n";
			printf("Running database upgrade commands:\n%s\n", stmt2b);
			fflush(stdout);
			err = SQLstatementIntern(c, stmt2b, "update", true, false, NULL);
			if (err == MAL_SUCCEED) {
				static const char stmt3[] = "ALTER TABLE sys.table_types SET READ ONLY;\n";
				printf("Running database upgrade commands:\n%s\n", stmt3);
				fflush(stdout);
				err = SQLstatementIntern(c, stmt3, "update", true, false, NULL);
			}

			/* 91_information_schema.sql */
			/* correct definitions of views: information_schema.tables and information_schema.views */
			if (err == MAL_SUCCEED) {
				static const char query[] =
					"DROP VIEW information_schema.views CASCADE;\n"
					"DROP VIEW information_schema.tables CASCADE;\n"

					"CREATE VIEW INFORMATION_SCHEMA.TABLES AS SELECT\n"
					"  cast(NULL AS varchar(1)) AS TABLE_CATALOG,\n"
					"  s.\"name\" AS TABLE_SCHEMA,\n"
					"  t.\"name\" AS TABLE_NAME,\n"
					"  tt.\"table_type_name\" AS TABLE_TYPE,\n"
					"  cast(NULL AS varchar(1)) AS SELF_REFERENCING_COLUMN_NAME,\n"
					"  cast(NULL AS varchar(1)) AS REFERENCE_GENERATION,\n"
					"  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_CATALOG,\n"
					"  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_SCHEMA,\n"
					"  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_NAME,\n"
					"  cast(sys.ifthenelse((t.\"type\" IN (0, 3, 7, 20, 30) AND t.\"access\" IN (0, 2)), 'YES', 'NO') AS varchar(3)) AS IS_INSERTABLE_INTO,\n"
					"  cast('NO' AS varchar(3)) AS IS_TYPED,\n"
					"  cast((CASE t.\"commit_action\" WHEN 1 THEN 'DELETE' WHEN 2 THEN 'PRESERVE' WHEN 3 THEN 'DROP' ELSE NULL END) AS varchar(10)) AS COMMIT_ACTION,\n"
					"  -- MonetDB column extensions\n"
					"  t.\"schema_id\" AS schema_id,\n"
					"  t.\"id\" AS table_id,\n"
					"  t.\"type\" AS table_type_id,\n"
					"  st.\"count\" AS row_count,\n"
					"  t.\"system\" AS is_system,\n"
					"  sys.ifthenelse(t.\"type\" IN (1, 11, 21, 31), TRUE, FALSE) AS is_view,\n"
					"  t.\"query\" AS query_def,\n"
					"  cm.\"remark\" AS comments\n"
					" FROM sys.\"tables\" t\n"
					" INNER JOIN sys.\"schemas\" s ON t.\"schema_id\" = s.\"id\"\n"
					" INNER JOIN sys.\"table_types\" tt ON t.\"type\" = tt.\"table_type_id\"\n"
					" LEFT OUTER JOIN sys.\"comments\" cm ON t.\"id\" = cm.\"id\"\n"
					" LEFT OUTER JOIN (SELECT DISTINCT \"schema\", \"table\", \"count\" FROM sys.\"statistics\"()) st ON (s.\"name\" = st.\"schema\" AND t.\"name\" = st.\"table\")\n"
					" ORDER BY s.\"name\", t.\"name\";\n"
					"GRANT SELECT ON TABLE INFORMATION_SCHEMA.TABLES TO PUBLIC WITH GRANT OPTION;\n"

					"CREATE VIEW INFORMATION_SCHEMA.VIEWS AS SELECT\n"
					"  cast(NULL AS varchar(1)) AS TABLE_CATALOG,\n"
					"  s.\"name\" AS TABLE_SCHEMA,\n"
					"  t.\"name\" AS TABLE_NAME,\n"
					"  t.\"query\" AS VIEW_DEFINITION,\n"
					"  cast('NONE' AS varchar(10)) AS CHECK_OPTION,\n"
					"  cast('NO' AS varchar(3)) AS IS_UPDATABLE,\n"
					"  cast('NO' AS varchar(3)) AS INSERTABLE_INTO,\n"
					"  cast('NO' AS varchar(3)) AS IS_TRIGGER_UPDATABLE,\n"
					"  cast('NO' AS varchar(3)) AS IS_TRIGGER_DELETABLE,\n"
					"  cast('NO' AS varchar(3)) AS IS_TRIGGER_INSERTABLE_INTO,\n"
					"  -- MonetDB column extensions\n"
					"  t.\"schema_id\" AS schema_id,\n"
					"  t.\"id\" AS table_id,\n"
					"  t.\"type\" AS table_type_id,\n"
					"  t.\"system\" AS is_system,\n"
					"  cm.\"remark\" AS comments\n"
					" FROM sys.\"tables\" t\n"
					" INNER JOIN sys.\"schemas\" s ON t.\"schema_id\" = s.\"id\"\n"
					" LEFT OUTER JOIN sys.\"comments\" cm ON t.\"id\" = cm.\"id\"\n"
					" WHERE t.\"type\" IN (1, 11, 21, 31)\n"
					" ORDER BY s.\"name\", t.\"name\";\n"
					"GRANT SELECT ON TABLE INFORMATION_SCHEMA.VIEWS TO PUBLIC WITH GRANT OPTION;\n"
					"\n"
					"UPDATE sys._tables SET system = true where system <> true\n"
					" and schema_id = (select s.id from sys.schemas s where s.name = 'information_schema')\n"
					" and name in ('tables','views');\n";

				sql_schema *infoschema = mvc_bind_schema(sql, "information_schema");
				sql_table *t;
				if ((t = mvc_bind_table(sql, infoschema, "tables")) != NULL)
					t->system = 0; /* make it non-system else the drop view will fail */
				if ((t = mvc_bind_table(sql, infoschema, "views")) != NULL)
					t->system = 0;
				printf("Running database upgrade commands:\n%s\n", query);
				fflush(stdout);
				err = SQLstatementIntern(c, query, "update", true, false, NULL);
			}
		}
	}
	if (output != NULL) {
		res_table_destroy(output);
		output = NULL;
	}

	return err;
}

static str
sql_update_snapshot(Client c, mvc *sql, sql_schema *s)
{
	char *err;
	res_table *output;
	BAT *b;

	(void) sql;
	(void) s;

	/* json.isvalid(json) has been fixed to return NULL on NULL input */
	err = SQLstatementIntern(c, "select id from sys.functions where name = 'hot_snapshot' and schema_id = 2000 and func like '%omitunlogged%';\n", "update", true, false, &output);
	if (err)
		return err;
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) == 0) {
			const char *query = "create procedure sys.hot_snapshot(tarfile string, onserver bool, omitunlogged bool)\n"
				"external name sql.hot_snapshot;\n"
				"create procedure sys.hot_snapshot(tarfile string, onserver bool, omitunlogged bool, omitids string)\n"
				"external name sql.hot_snapshot;\n"
				"update sys.functions set system = true where system <> true and name = 'hot_snapshot' and schema_id = 2000;\n";
			printf("Running database upgrade commands:\n%s\n", query);
			fflush(stdout);
			err = SQLstatementIntern(c, query, "update", true, false, NULL);
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);
	return err;
}

static str
sql_update_default(Client c, mvc *sql, sql_schema *s)
{
	char *err;
	res_table *output;
	BAT *b;

	err = SQLstatementIntern(c, "select id from sys.types where sqlname = 'inet4' and schema_id = 2000;\n", "update", true, false, &output);
	if (err)
		return err;
	b = BATdescriptor(output->cols[0].b);
	if (b != NULL) {
		if (BATcount(b) == 0) {
			const char query[] =
				"create type inet4 external name inet4;\n"
				"create type inet6 external name inet6;\n"
				"create function contains(ip1 inet4, ip2 inet4, netmask tinyint)\n"
				" returns bool external name \"inet46\".\"inet4containsinet4\";\n"
				"create function contains(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint)\n"
				" returns bool external name \"inet46\".\"inet4containsinet4\";\n"
				"create function containsorequal(ip1 inet4, ip2 inet4, netmask tinyint)\n"
				" returns bool external name \"inet46\".\"inet4containsorequalinet4\";\n"
				"create function containsorequal(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint)\n"
				" returns bool external name \"inet46\".\"inet4containsorequalinet4\";\n"
				"create function containssymmetric(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint)\n"
				" returns bool external name \"inet46\".\"inet4containssymmetricinet4\";\n"
				"create function contains(ip1 inet6, ip2 inet6, netmask smallint)\n"
				" returns bool external name \"inet46\".\"inet6containsinet6\";\n"
				"create function contains(ip1 inet6, netmask1 smallint, ip2 inet6, netmask2 smallint)\n"
				" returns bool external name \"inet46\".\"inet6containsinet6\";\n"
				"create function containsorequal(ip1 inet6, ip2 inet6, netmask smallint)\n"
				" returns bool external name \"inet46\".\"inet6containsorequalinet6\";\n"
				"create function containsorequal(ip1 inet6, netmask1 smallint, ip2 inet6, netmask2 smallint)\n"
				" returns bool external name \"inet46\".\"inet6containsorequalinet6\";\n"
				"create function containssymmetric(ip1 inet6, netmask1 smallint, ip2 inet6, netmask2 smallint)\n"
				" returns bool external name \"inet46\".\"inet6containssymmetricinet6\";\n"
				"create function bit_not(ip1 inet4)\n"
				" returns inet4 external name \"calc\".\"not\";\n"
				"create function bit_and(ip1 inet4, ip2 inet4)\n"
				" returns inet4 external name \"calc\".\"and\";\n"
				"create function bit_or(ip1 inet4, ip2 inet4)\n"
				" returns inet4 external name \"calc\".\"or\";\n"
				"create function bit_xor(ip1 inet4, ip2 inet4)\n"
				" returns inet4 external name \"calc\".\"xor\";\n"
				"create function bit_not(ip1 inet6)\n"
				" returns inet6 external name \"calc\".\"not\";\n"
				"create function bit_and(ip1 inet6, ip2 inet6)\n"
				" returns inet6 external name \"calc\".\"and\";\n"
				"create function bit_or(ip1 inet6, ip2 inet6)\n"
				" returns inet6 external name \"calc\".\"or\";\n"
				"create function bit_xor(ip1 inet6, ip2 inet6)\n"
				" returns inet6 external name \"calc\".\"xor\";\n"
				"grant execute on function contains(inet4, inet4, tinyint) to public;\n"
				"grant execute on function contains(inet4, tinyint, inet4, tinyint) to public;\n"
				"grant execute on function containsorequal(inet4, inet4, tinyint) to public;\n"
				"grant execute on function containsorequal(inet4, tinyint, inet4, tinyint) to public;\n"
				"grant execute on function containssymmetric(inet4, tinyint, inet4, tinyint) to public;\n"
				"grant execute on function contains(inet6, inet6, smallint) to public;\n"
				"grant execute on function contains(inet6, smallint, inet6, smallint) to public;\n"
				"grant execute on function containsorequal(inet6, inet6, smallint) to public;\n"
				"grant execute on function containsorequal(inet6, smallint, inet6, smallint) to public;\n"
				"grant execute on function containssymmetric(inet6, smallint, inet6, smallint) to public;\n"
				"grant execute on function bit_not(inet4) to public;\n"
				"grant execute on function bit_and(inet4, inet4) to public;\n"
				"grant execute on function bit_or(inet4, inet4) to public;\n"
				"grant execute on function bit_xor(inet4, inet4) to public;\n"
				"grant execute on function bit_not(inet6) to public;\n"
				"grant execute on function bit_and(inet6, inet6) to public;\n"
				"grant execute on function bit_or(inet6, inet6) to public;\n"
				"grant execute on function bit_xor(inet6, inet6) to public;\n"
				"drop view sys.dependencies_vw cascade;\n"
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
				"SELECT id, sqlname, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'type', 'sys.types', (sqlname in ('inet','json','url','uuid','inet4','inet6')) FROM sys.types\n"
				" ORDER BY id;\n"
				"CREATE VIEW sys.dependencies_vw AS\n"
				"SELECT d.id, i1.obj_type, i1.name,\n"
				" d.depend_id as used_by_id, i2.obj_type as used_by_obj_type, i2.name as used_by_name,\n"
				" d.depend_type, dt.dependency_type_name\n"
				" FROM sys.dependencies d\n"
				" JOIN sys.ids i1 ON d.id = i1.id\n"
				" JOIN sys.ids i2 ON d.depend_id = i2.id\n"
				" JOIN sys.dependency_types dt ON d.depend_type = dt.dependency_type_id\n"
				" ORDER BY id, depend_id;\n"
				"GRANT SELECT ON sys.ids TO PUBLIC;\n"
				"GRANT SELECT ON sys.dependencies_vw TO PUBLIC;\n"
				"drop function sys.dump_database cascade;\n"
				"drop view sys.dump_user_defined_types cascade;\n"
				"drop view sys.describe_user_defined_types cascade;\n"
				"CREATE VIEW sys.describe_user_defined_types AS\n"
				" SELECT\n"
				" s.name sch,\n"
				" t.sqlname sql_tpe,\n"
				" t.systemname ext_tpe\n"
				" FROM sys.types t JOIN sys.schemas s ON t.schema_id = s.id\n"
				" WHERE\n"
				" t.eclass = 18 AND\n"
				" (\n"
				" (s.name = 'sys' AND t.sqlname not in ('geometrya', 'mbr', 'url', 'inet', 'json', 'uuid', 'xml', 'inet4', 'inet6')) OR\n"
				" (s.name <> 'sys')\n"
				" );\n"
				"GRANT SELECT ON sys.describe_user_defined_types TO PUBLIC;\n"
				"CREATE VIEW sys.dump_user_defined_types AS\n"
				" SELECT 'CREATE TYPE ' || sys.FQN(sch, sql_tpe) || ' EXTERNAL NAME ' || sys.DQ(ext_tpe) || ';' stmt,\n"
				" sch schema_name,\n"
				" sql_tpe type_name\n"
				" FROM sys.describe_user_defined_types;\n"
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
				" --TODO loaders, procedures, window and filter sys.functions.\n"
				" --TODO look into order dependent group_concat\n"
				" INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'COMMIT;');\n"
				" RETURN sys.dump_statements;\n"
				"END;\n"
				"drop view sys.storagemodel cascade;\n"
				"drop view sys.tablestoragemodel cascade;\n"
				"drop function sys.columnsize cascade;\n"
				"create function sys.columnsize(tpe varchar(1024), count bigint)\n"
				"returns bigint\n"
				"begin\n"
				" -- for fixed size types: typewidth_inbytes * count\n"
				" if tpe in ('tinyint', 'boolean')\n"
				" then return count;\n"
				" end if;\n"
				" if tpe = 'smallint'\n"
				" then return 2 * count;\n"
				" end if;\n"
				" if tpe in ('int', 'real', 'date', 'time', 'timetz', 'sec_interval', 'day_interval', 'month_interval', 'inet4')\n"
				" then return 4 * count;\n"
				" end if;\n"
				" if tpe in ('bigint', 'double', 'timestamp', 'timestamptz', 'inet', 'oid')\n"
				" then return 8 * count;\n"
				" end if;\n"
				" if tpe in ('hugeint', 'decimal', 'uuid', 'mbr', 'inet6')\n"
				" then return 16 * count;\n"
				" end if;\n"
				"\n"
				" -- for variable size types we compute the columnsize as refs (assume 4 bytes each for char strings) to the heap, excluding data in the var heap\n"
				" if tpe in ('varchar', 'char', 'clob', 'json', 'url')\n"
				" then return 4 * count;\n"
				" end if;\n"
				" if tpe in ('blob', 'geometry', 'geometrya')\n"
				" then return 8 * count;\n"
				" end if;\n"
				"\n"
				" return 8 * count;\n"
				"end;\n"
				"create view sys.storagemodel as\n"
				"select \"schema\", \"table\", \"column\", \"type\", \"count\",\n"
				" sys.columnsize(\"type\", \"count\") as columnsize,\n"
				" sys.heapsize(\"type\", \"count\", \"distinct\", \"atomwidth\") as heapsize,\n"
				" sys.hashsize(\"reference\", \"count\") as hashsize,\n"
				" case when isacolumn then sys.imprintsize(\"type\", \"count\") else 0 end as imprintsize,\n"
				" case when (isacolumn and not sorted) then cast(8 * \"count\" as bigint) else 0 end as orderidxsize,\n"
				" sorted, \"unique\", isacolumn\n"
				" from sys.storagemodelinput\n"
				"order by \"schema\", \"table\", \"column\";\n"
				"create view sys.tablestoragemodel as\n"
				"select \"schema\", \"table\",\n"
				" max(\"count\") as \"rowcount\",\n"
				" count(*) as \"storages\",\n"
				" sum(sys.columnsize(\"type\", \"count\")) as columnsize,\n"
				" sum(sys.heapsize(\"type\", \"count\", \"distinct\", \"atomwidth\")) as heapsize,\n"
				" sum(sys.hashsize(\"reference\", \"count\")) as hashsize,\n"
				" sum(case when isacolumn then sys.imprintsize(\"type\", \"count\") else 0 end) as imprintsize,\n"
				" sum(case when (isacolumn and not sorted) then cast(8 * \"count\" as bigint) else 0 end) as orderidxsize\n"
				" from sys.storagemodelinput\n"
				"group by \"schema\", \"table\"\n"
				"order by \"schema\", \"table\";\n"
				"update sys.functions set system = true where not system and schema_id = 2000 and name in ('contains', 'containsorequal', 'containssymmetric', 'bit_not', 'bit_and', 'bit_or', 'bit_xor', 'dump_database', 'columnsize');\n"
				"update sys._tables set system = true where not system and schema_id = 2000 and name in ('ids', 'dependencies_vw', 'describe_user_defined_types', 'dump_user_defined_types', 'storagemodel', 'tablestoragemodel');\n";
			sql_table *t;
			t = mvc_bind_table(sql, s, "dependencies_vw");
			t->system = 0; /* make it non-system else the drop view will fail */
			t = mvc_bind_table(sql, s, "ids");
			t->system = 0; /* make it non-system else the drop view will fail */
			t = mvc_bind_table(sql, s, "describe_user_defined_types");
			t->system = 0; /* make it non-system else the drop view will fail */
			t = mvc_bind_table(sql, s, "dump_user_defined_types");
			t->system = 0; /* make it non-system else the drop view will fail */
			t = mvc_bind_table(sql, s, "storagemodel");
			t->system = 0; /* make it non-system else the drop view will fail */
			t = mvc_bind_table(sql, s, "tablestoragemodel");
			t->system = 0; /* make it non-system else the drop view will fail */
			printf("Running database upgrade commands:\n%s\n", query);
			fflush(stdout);
			err = SQLstatementIntern(c, query, "update", true, false, NULL);
		}
		BBPreclaim(b);
	}
	res_table_destroy(output);
	if (err)
		return err;

	if (sql_bind_func(sql, "sys", "optimizer_stats", NULL, NULL, F_UNION, true, true)) {
		const char query[] = "drop function sys.optimizer_stats cascade;\n";
		printf("Running database upgrade commands:\n%s\n", query);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		if (err)
			return err;
	} else {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
	}

	if (sql_bind_func(sql, "profiler", "start", NULL, NULL, F_PROC, true, true)) {
		const char query[] = "drop procedure profiler.start cascade;\n"
			"drop procedure profiler.stop cascade;\n"
			"drop procedure profiler.setlimit cascade;\n"
			"drop function profiler.getlimit cascade;\n"
			"drop procedure profiler.setheartbeat cascade;\n";
		printf("Running database upgrade commands:\n%s\n", query);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		if (err)
			return err;
	} else {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
	}

	sql_subtype tp;
	sql_find_subtype(&tp, "varchar", 0, 0);
	if (!sql_bind_func(sql, "sys", "sha256", &tp, NULL, F_AGGR, true, true)) {
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		static const char query[] = "create aggregate sha1(val string)\n"
			"returns string with order external name aggr.sha1;\n"
			"grant execute on aggregate sha1 to public;\n"
			"create aggregate sha224(val string)\n"
			"returns string with order external name aggr.sha224;\n"
			"grant execute on aggregate sha224 to public;\n"
			"create aggregate sha256(val string)\n"
			"returns string with order external name aggr.sha256;\n"
			"grant execute on aggregate sha256 to public;\n"
			"create aggregate sha384(val string)\n"
			"returns string with order external name aggr.sha384;\n"
			"grant execute on aggregate sha384 to public;\n"
			"create aggregate sha512(val string)\n"
			"returns string with order external name aggr.sha512;\n"
			"grant execute on aggregate sha512 to public;\n"
			"create aggregate ripemd160(val string)\n"
			"returns string with order external name aggr.ripemd160;\n"
			"grant execute on aggregate ripemd160 to public;\n"
			"update sys.functions set system = true where not system and schema_id = 2000 and name in ('sha1', 'sha224', 'sha256', 'sha384', 'sha512', 'ripemd160') and type = 3;\n"; /* F_AGGR == 3 */
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		if (err)
			return err;
	}

	if ((err = SQLstatementIntern(c, "select keyword from sys.keywords where keyword = 'PLAN';\n", "update", true, false, &output)))
		return err;
	if ((b = BBPquickdesc(output->cols[0].b)) && BATcount(b) > 0) {
		const char query[] =
			"ALTER TABLE sys.keywords SET READ WRITE;\n"
			"DELETE FROM sys.keywords WHERE keyword = 'PLAN';\n";
		printf("Running database upgrade commands:\n%s\n", query);
		fflush(stdout);
		err = SQLstatementIntern(c, query, "update", true, false, NULL);
		if (err == MAL_SUCCEED) {
			const char query2[] = "ALTER TABLE sys.keywords SET READ ONLY;\n";
			printf("Running database upgrade commands:\n%s\n", query2);
			fflush(stdout);
			err = SQLstatementIntern(c, query2, "update", true, false, NULL);
		}
	}
	res_table_destroy(output);
	output = NULL;

	return err;
}

int
SQLupgrades(Client c, mvc *m)
{
	char *err;
	sql_schema *s = mvc_bind_schema(m, "sys");

	if ((err = check_sys_tables(c, m, s)) != NULL) {
		goto handle_error;
	}

#ifdef HAVE_HGE
	sql_subtype tp;
	sql_find_subtype(&tp, "hugeint", 0, 0);
	if (!sql_bind_func(m, s->base.name, "var_pop", &tp, NULL, F_AGGR, true, true)) {
		m->session->status = 0; /* if the function was not found clean the error */
		m->errstr[0] = '\0';
		if ((err = sql_update_hugeint(c, m)) != NULL) {
			goto handle_error;
		}
	}
#endif

	if ((err = sql_update_jan2022(c, m)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_sep2022(c, m, s)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_jun2023(c, m, s)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_dec2023_geom(c, m, s)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_jun2023_sp3(c, m, s)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_dec2023(c, m, s)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_dec2023_sp1(c, m, s)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_dec2023_sp4(c, m, s)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_aug2024(c, m, s)) != NULL) {
		goto handle_error;
	}

	if ((err = sql_update_mar2025(c, m, s)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		goto handle_error;
	}

	if ((err = sql_update_mar2025_geom(c, m, s)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		goto handle_error;
	}

	if ((err = sql_update_mar2025_sp1(c, m)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		goto handle_error;
	}

	if ((err = sql_update_snapshot(c, m, s)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		goto handle_error;
	}

	if ((err = sql_update_default(c, m, s)) != NULL) {
		TRC_CRITICAL(SQL_PARSER, "%s\n", err);
		goto handle_error;
	}

	return 0;

handle_error:
	GDKfatal("%s\n", err);
	freeException(err);
	return -1;
}
