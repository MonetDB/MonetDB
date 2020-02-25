/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * SQL upgrade code
 * N. Nes, M.L. Kersten, S. Mullender
 */
#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_execute.h"
#include "sql_mvc.h"
#include "mtime.h"
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
	for (n = funcs->h; n; n = n->next) {
		sql_func *func = n->data;
		int number = 0;
		sql_arg *arg;
		node *m;

		if (func->base.id >= FUNC_OIDS)
			continue;

		if (func->type == F_AGGR) {
			pos += snprintf(buf + pos, bufsize - pos,
					"insert into sys.functions values"
					" (%d, '%s', '%s', '%s', %d, %d, false,"
					" %s, %s, %d, %s);\n",
					func->base.id, func->base.name, func->imp,
					func->mod, (int) FUNC_LANG_INT, (int) func->type,
					func->varres ? "true" : "false",
					func->vararg ? "true" : "false",
					func->s ? func->s->base.id : s->base.id,
					func->system ? "true" : "false");
			arg = func->res->h->data;
			pos += snprintf(buf + pos, bufsize - pos,
					"insert into sys.args values"
					" (%d, %d, 'res', '%s', %u, %u, %d, 0);\n",
					store_next_oid(), func->base.id,
					arg->type.type->sqlname, arg->type.digits,
					arg->type.scale, arg->inout);
			if (func->ops->h) {
				arg = func->ops->h->data;
				pos += snprintf(buf + pos, bufsize - pos,
						"insert into sys.args values"
						" (%d, %d, 'arg', '%s', %u,"
						" %u, %d, 1);\n",
						store_next_oid(), func->base.id,
						arg->type.type->sqlname,
						arg->type.digits, arg->type.scale,
						arg->inout);
			}
		} else {
			pos += snprintf(buf + pos, bufsize - pos,
					"insert into sys.functions values"
					" (%d, '%s', '%s', '%s',"
					" %d, %d, %s, %s, %s, %d, %s);\n",
					func->base.id, func->base.name,
					func->imp, func->mod, (int) FUNC_LANG_INT,
					(int) func->type,
					func->side_effect ? "true" : "false",
					func->varres ? "true" : "false",
					func->vararg ? "true" : "false",
					func->s ? func->s->base.id : s->base.id,
					func->system ? "true" : "false");
			if (func->res) {
				for (m = func->res->h; m; m = m->next, number++) {
					arg = m->data;
					pos += snprintf(buf + pos, bufsize - pos,
							"insert into sys.args"
							" values"
							" (%d, %d, 'res_%d',"
							" '%s', %u, %u, %d,"
							" %d);\n",
							store_next_oid(),
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
							store_next_oid(),
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
							store_next_oid(),
							func->base.id,
							number,
							arg->type.type->sqlname,
							arg->type.digits,
							arg->type.scale,
							arg->inout, number);
			}
		}
	}

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
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
			"create aggregate stddev_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"stdevp\";\n"
			"GRANT EXECUTE ON AGGREGATE stddev_pop(HUGEINT) TO PUBLIC;\n"
			"create aggregate var_samp(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"variance\";\n"
			"GRANT EXECUTE ON AGGREGATE var_samp(HUGEINT) TO PUBLIC;\n"
			"create aggregate var_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"variancep\";\n"
			"GRANT EXECUTE ON AGGREGATE var_pop(HUGEINT) TO PUBLIC;\n"
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
			"GRANT EXECUTE ON AGGREGATE corr(HUGEINT, HUGEINT) TO PUBLIC;\n");

	/* 40_json_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function json.filter(js json, name hugeint)\n"
			"returns json external name json.filter;\n"
			"GRANT EXECUTE ON FUNCTION json.filter(json, hugeint) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where name in ('fuse', 'generate_series', 'stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'median', 'median_avg', 'quantile', 'quantile_avg', 'corr') and schema_id = (select id from sys.schemas where name = 'sys');\n"
			"update sys.functions set system = true where name = 'filter' and schema_id = (select id from sys.schemas where name = 'json');\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_update_geom(Client c, mvc *sql, int olddb, const char *prev_schema)
{
	size_t bufsize, pos = 0;
	char *buf, *err = NULL, *geomupgrade;
	geomsqlfix_fptr fixfunc;
	node *n;
	sql_schema *s = mvc_bind_schema(sql, "sys");

	if ((fixfunc = geomsqlfix_get()) == NULL)
		return NULL;

	geomupgrade = (*fixfunc)(olddb);
	if (geomupgrade == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	bufsize = strlen(geomupgrade) + 512;
	buf = GDKmalloc(bufsize);
	if (buf == NULL) {
		GDKfree(geomupgrade);
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");
	pos += snprintf(buf + pos, bufsize - pos, "%s", geomupgrade);
	GDKfree(geomupgrade);

	pos += snprintf(buf + pos, bufsize - pos, "delete from sys.types where systemname in ('mbr', 'wkb', 'wkba');\n");
	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;

		if (t->base.id < FUNC_OIDS &&
		    (strcmp(t->base.name, "mbr") == 0 ||
		     strcmp(t->base.name, "wkb") == 0 ||
		     strcmp(t->base.name, "wkba") == 0))
			pos += snprintf(buf + pos, bufsize - pos, "insert into sys.types values (%d, '%s', '%s', %u, %u, %d, %d, %d);\n",
							t->base.id, t->base.name, t->sqlname, t->digits, t->scale, t->radix, (int) t->eclass,
							t->s ? t->s->base.id : s->base.id);
	}

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jul2017(Client c, const char *prev_schema)
{
	size_t bufsize = 10000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *q1 = "select id from sys.functions where name = 'shpload' and schema_id = (select id from sys.schemas where name = 'sys');\n";
	res_table *output;
	BAT *b;

	if( buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"delete from sys._columns where table_id = (select id from sys._tables where name = 'connections' and schema_id = (select id from sys.schemas where name = 'sys'));\n"
			"delete from sys._tables where name = 'connections' and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* 09_like.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set side_effect = false where name in ('like', 'ilike') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* 25_debug.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function sys.malfunctions;\n"
			"create function sys.malfunctions() returns table(\"module\" string, \"function\" string, \"signature\" string, \"address\" string, \"comment\" string) external name \"manual\".\"functions\";\n"
			"drop function sys.optimizer_stats();\n"
			"create function sys.optimizer_stats() "
			"returns table (optname string, count int, timing bigint) "
			"external name inspect.optimizer_stats;\n"
			"update sys.functions set system = true where name in ('malfunctions', 'optimizer_stats') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* 46_profiler.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function profiler.getlimit() returns integer external name profiler.getlimit;\n"
			"create procedure profiler.setlimit(lim integer) external name profiler.setlimit;\n"
			"drop procedure profiler.setpoolsize;\n"
			"drop procedure profiler.setstream;\n"
			"update sys.functions set system = true where name in ('getlimit', 'setlimit') and schema_id = (select id from sys.schemas where name = 'profiler');\n");

	/* 51_sys_schema_extensions.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"ALTER TABLE sys.keywords SET READ ONLY;\n"
			"ALTER TABLE sys.table_types SET READ ONLY;\n"
			"ALTER TABLE sys.dependency_types SET READ ONLY;\n"

			"CREATE TABLE sys.function_types (\n"
			"function_type_id   SMALLINT NOT NULL PRIMARY KEY,\n"
			"function_type_name VARCHAR(30) NOT NULL UNIQUE);\n"
			"INSERT INTO sys.function_types (function_type_id, function_type_name) VALUES\n"
			"(1, 'Scalar function'), (2, 'Procedure'), (3, 'Aggregate function'), (4, 'Filter function'), (5, 'Function returning a table'),\n"
			"(6, 'Analytic function'), (7, 'Loader function');\n"
			"ALTER TABLE sys.function_types SET READ ONLY;\n"

			"CREATE TABLE sys.function_languages (\n"
			"language_id   SMALLINT NOT NULL PRIMARY KEY,\n"
			"language_name VARCHAR(20) NOT NULL UNIQUE);\n"
			"INSERT INTO sys.function_languages (language_id, language_name) VALUES\n"
			"(0, 'Internal C'), (1, 'MAL'), (2, 'SQL'), (3, 'R'), (6, 'Python'), (7, 'Python Mapped'), (8, 'Python2'), (9, 'Python2 Mapped'), (10, 'Python3'), (11, 'Python3 Mapped');\n"
			"ALTER TABLE sys.function_languages SET READ ONLY;\n"

			"CREATE TABLE sys.key_types (\n"
			"key_type_id   SMALLINT NOT NULL PRIMARY KEY,\n"
			"key_type_name VARCHAR(15) NOT NULL UNIQUE);\n"
			"INSERT INTO sys.key_types (key_type_id, key_type_name) VALUES\n"
			"(0, 'Primary Key'), (1, 'Unique Key'), (2, 'Foreign Key');\n"
			"ALTER TABLE sys.key_types SET READ ONLY;\n"

			"CREATE TABLE sys.index_types (\n"
			"index_type_id   SMALLINT NOT NULL PRIMARY KEY,\n"
			"index_type_name VARCHAR(25) NOT NULL UNIQUE);\n"
			"INSERT INTO sys.index_types (index_type_id, index_type_name) VALUES\n"
			"(0, 'Hash'), (1, 'Join'), (2, 'Order preserving hash'), (3, 'No-index'), (4, 'Imprint'), (5, 'Ordered');\n"
			"ALTER TABLE sys.index_types SET READ ONLY;\n"

			"CREATE TABLE sys.privilege_codes (\n"
			"privilege_code_id   INT NOT NULL PRIMARY KEY,\n"
			"privilege_code_name VARCHAR(30) NOT NULL UNIQUE);\n"
			"INSERT INTO sys.privilege_codes (privilege_code_id, privilege_code_name) VALUES\n"
			"(1, 'SELECT'), (2, 'UPDATE'), (4, 'INSERT'), (8, 'DELETE'), (16, 'EXECUTE'), (32, 'GRANT'),\n"
			"(3, 'SELECT,UPDATE'), (5, 'SELECT,INSERT'), (6, 'INSERT,UPDATE'), (7, 'SELECT,INSERT,UPDATE'),\n"
			"(9, 'SELECT,DELETE'), (10, 'UPDATE,DELETE'), (11, 'SELECT,UPDATE,DELETE'), (12, 'INSERT,DELETE'),\n"
			"(13, 'SELECT,INSERT,DELETE'), (14, 'INSERT,UPDATE,DELETE'), (15, 'SELECT,INSERT,UPDATE,DELETE');\n"
			"ALTER TABLE sys.privilege_codes SET READ ONLY;\n"

			"update sys._tables set system = true where name in ('function_languages', 'function_types', 'index_types', 'key_types', 'privilege_codes') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* 75_shp.sql, if shp extension available */
	err = SQLstatementIntern(c, &q1, "update", true, false, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) > 0) {
			pos += snprintf(buf + pos, bufsize - pos,
					"drop procedure SHPload(integer);\n"
					"create procedure SHPload(fid integer) external name shp.import;\n"
					"update sys.functions set system = true where name = 'shpload' and schema_id = (select id from sys.schemas where name = 'sys');\n");
		}
		BBPunfix(b->batCacheid);
	}
	res_tables_destroy(output);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jul2017_sp2(Client c)
{
	char *qry = "select obj_id from sys.privileges where auth_id = 1 and obj_id in (select id from sys._tables where name in ('keywords', 'table_types', 'dependency_types', 'function_types', 'function_languages', 'key_types', 'index_types', 'privilege_codes', 'environment')) and privileges = 1;\n";
	char *err = NULL;
	res_table *output;
	BAT *b;

	err = SQLstatementIntern(c, &qry, "update", true, false, &output);
	if (err) {
		return err;
	}

	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) < 9) {
			/* we are missing grants on these system tables, add them */
			size_t bufsize = 2048, pos = 0;
			char *buf = GDKmalloc(bufsize);

			if (buf == NULL)
				throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

			/* 51_sys_schema_extensions.sql and 25_debug.sql */
			pos += snprintf(buf + pos, bufsize - pos,
				"GRANT SELECT ON sys.keywords TO PUBLIC;\n"
				"GRANT SELECT ON sys.table_types TO PUBLIC;\n"
				"GRANT SELECT ON sys.dependency_types TO PUBLIC;\n"
				"GRANT SELECT ON sys.function_types TO PUBLIC;\n"
				"GRANT SELECT ON sys.function_languages TO PUBLIC;\n"
				"GRANT SELECT ON sys.key_types TO PUBLIC;\n"
				"GRANT SELECT ON sys.index_types TO PUBLIC;\n"
				"GRANT SELECT ON sys.privilege_codes TO PUBLIC;\n"
				"GRANT EXECUTE ON FUNCTION sys.environment() TO PUBLIC;\n"
				"GRANT SELECT ON sys.environment TO PUBLIC;\n"
				);
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
			GDKfree(buf);
		}
		BBPunfix(b->batCacheid);
	}
	res_tables_destroy(output);

	return err;		/* usually NULL */
}

static str
sql_update_jul2017_sp3(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	char *err = NULL;
	sql_schema *sys;
	sql_table *tab;
	sql_column *col;
	oid rid;

	/* if there is no value "sys_update_schemas" in
	 * sys.functions.name, we need to update the sys.functions
	 * table */
	sys = find_sql_schema(sql->session->tr, "sys");
	tab = find_sql_table(sys, "functions");
	col = find_sql_column(tab, "name");
	rid = table_funcs.column_find_row(sql->session->tr, col, "sys_update_schemas", NULL);
	if (is_oid_nil(rid) && !*systabfixed) {
		err = sql_fix_system_tables(c, sql, prev_schema);
		if (err != NULL)
			return err;
		*systabfixed = true;
	}
	/* if there is no value "system_update_schemas" in
	 * sys.triggers.name, we need to add the triggers */
	tab = find_sql_table(sys, "triggers");
	col = find_sql_column(tab, "name");
	rid = table_funcs.column_find_row(sql->session->tr, col, "system_update_schemas", NULL);
	if (is_oid_nil(rid)) {
		size_t bufsize = 1024, pos = 0;
		char *buf = GDKmalloc(bufsize);
		if (buf == NULL)
			throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		pos += snprintf(
			buf + pos,
			bufsize - pos,
			"set schema \"sys\";\n"
			"create trigger system_update_schemas after update on sys.schemas for each statement call sys_update_schemas();\n"
			"create trigger system_update_tables after update on sys._tables for each statement call sys_update_tables();\n");
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
		GDKfree(buf);
	}
	return err;
}

static str
sql_update_mar2018_geom(Client c, sql_table *t, const char *prev_schema)
{
	size_t bufsize = 10000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.geometry_columns cascade;\n"
			"create view sys.geometry_columns as\n"
			"\tselect cast(null as varchar(1)) as f_table_catalog,\n"
			"\t\ts.name as f_table_schema,\n"
			"\t\tt.name as f_table_name,\n"
			"\t\tc.name as f_geometry_column,\n"
			"\t\tcast(has_z(c.type_digits) + has_m(c.type_digits) +2 as integer) as coord_dimension,\n"
			"\t\tc.type_scale as srid,\n"
			"\t\tget_type(c.type_digits, 0) as type\n"
			"\tfrom sys.columns c, sys.tables t, sys.schemas s\n"
			"\twhere c.table_id = t.id and t.schema_id = s.id\n"
			"\t  and c.type in (select sqlname from sys.types where systemname in ('wkb', 'wkba'));\n"
			"GRANT SELECT ON sys.geometry_columns TO PUBLIC;\n"
			"update sys._tables set system = true where name = 'geometry_columns' and schema_id in (select id from schemas where name = 'sys');\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_mar2018(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 30000, pos = 0;
	char *buf, *err;
	sql_schema *s;
	sql_table *t;
	res_table *output;
	BAT *b;

	buf = "select id from sys.functions where name = 'quarter' and schema_id = (select id from sys.schemas where name = 'sys');\n";
	err = SQLstatementIntern(c, &buf, "update", true, false, &output);
	if (err)
		return err;
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) == 0 && !*systabfixed) {
			/* if there is no value "quarter" in
			 * sys.functions.name, we need to update the
			 * sys.functions table */
			err = sql_fix_system_tables(c, sql, prev_schema);
			if (err != NULL)
				return err;
			*systabfixed = true;
		}
		BBPunfix(b->batCacheid);
	}
	res_tables_destroy(output);

	buf = GDKmalloc(bufsize);
	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	s = mvc_bind_schema(sql, "sys");

	t = mvc_create_table(sql, s, "comments", tt_table, 1, SQL_PERSIST, 0, -1, 0);
	sql_column *col = mvc_create_column_(sql, t, "id", "int", 32);
	sql_key *k = sql_trans_create_ukey(sql->session->tr, t, "comments_id_pkey", pkey);
	k = sql_trans_create_kc(sql->session->tr, k, col);
	k = sql_trans_key_done(sql->session->tr, k);
	sql_trans_create_dependency(sql->session->tr, col->base.id, k->idx->base.id, INDEX_DEPENDENCY);
	col = mvc_create_column_(sql, t, "remark", "varchar", 65000);
	sql_trans_alter_null(sql->session->tr, col, 0);

	sql_table *privs = mvc_bind_table(sql, s, "privileges");
	int pub = ROLE_PUBLIC;
	int p = PRIV_SELECT;
	int zero = 0;
	table_funcs.table_insert(sql->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	/* 21_dependency_views.sql */
	pos += snprintf(buf + pos, bufsize - pos,
"CREATE VIEW sys.ids (id, name, schema_id, table_id, table_name, obj_type, sys_table) AS\n"
"SELECT id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'author' AS obj_type, 'sys.auths' AS sys_table FROM sys.auths UNION ALL\n"
"SELECT id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'schema', 'sys.schemas' FROM sys.schemas UNION ALL\n"
"SELECT id, name, schema_id, id as table_id, name as table_name, case when type = 1 then 'view' else 'table' end, 'sys._tables' FROM sys._tables UNION ALL\n"
"SELECT id, name, schema_id, id as table_id, name as table_name, case when type = 1 then 'view' else 'table' end, 'tmp._tables' FROM tmp._tables UNION ALL\n"
"SELECT c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'sys._columns' FROM sys._columns c JOIN sys._tables t ON c.table_id = t.id UNION ALL\n"
"SELECT c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'tmp._columns' FROM tmp._columns c JOIN tmp._tables t ON c.table_id = t.id UNION ALL\n"
"SELECT k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'sys.keys' FROM sys.keys k JOIN sys._tables t ON k.table_id = t.id UNION ALL\n"
"SELECT k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'tmp.keys' FROM tmp.keys k JOIN sys._tables t ON k.table_id = t.id UNION ALL\n"
"SELECT i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index', 'sys.idxs' FROM sys.idxs i JOIN sys._tables t ON i.table_id = t.id UNION ALL\n"
"SELECT i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index', 'tmp.idxs' FROM tmp.idxs i JOIN sys._tables t ON i.table_id = t.id UNION ALL\n"
"SELECT g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'sys.triggers' FROM sys.triggers g JOIN sys._tables t ON g.table_id = t.id UNION ALL\n"
"SELECT g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'tmp.triggers' FROM tmp.triggers g JOIN sys._tables t ON g.table_id = t.id UNION ALL\n"
"SELECT id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, case when type = 2 then 'procedure' else 'function' end, 'sys.functions' FROM sys.functions UNION ALL\n"
"SELECT a.id, a.name, f.schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, case when f.type = 2 then 'procedure arg' else 'function arg' end, 'sys.args' FROM sys.args a JOIN sys.functions f ON a.func_id = f.id UNION ALL\n"
"SELECT id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'sequence', 'sys.sequences' FROM sys.sequences UNION ALL\n"
"SELECT id, sqlname, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'type', 'sys.types' FROM sys.types WHERE id > 2000 /* exclude system types to prevent duplicates with auths.id */\n"
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
"CREATE VIEW sys.dependency_owners_on_schemas AS\n"
"SELECT a.name AS owner_name, s.id AS schema_id, s.name AS schema_name, CAST(1 AS smallint) AS depend_type\n"
"  FROM sys.schemas AS s, sys.auths AS a\n"
" WHERE s.owner = a.id\n"
" ORDER BY a.name, s.name;\n"
"GRANT SELECT ON sys.dependency_owners_on_schemas TO PUBLIC;\n"
"CREATE VIEW sys.dependency_columns_on_keys AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, c.id AS column_id, c.name AS column_name, k.id AS key_id, k.name AS key_name, CAST(kc.nr +1 AS int) AS key_col_nr, CAST(k.type AS smallint) AS key_type, CAST(4 AS smallint) AS depend_type\n"
"  FROM sys.columns AS c, sys.objects AS kc, sys.keys AS k, sys.tables AS t\n"
" WHERE k.table_id = c.table_id AND c.table_id = t.id AND kc.id = k.id AND kc.name = c.name\n"
"   AND k.type IN (0, 1)\n"
" ORDER BY t.schema_id, t.name, c.name, k.type, k.name, kc.nr;\n"
"GRANT SELECT ON sys.dependency_columns_on_keys TO PUBLIC;\n"
"CREATE VIEW sys.dependency_tables_on_views AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, dep.depend_type AS depend_type\n"
"  FROM sys.tables AS t, sys.tables AS v, sys.dependencies AS dep\n"
" WHERE t.id = dep.id AND v.id = dep.depend_id\n"
"   AND dep.depend_type = 5 AND t.type NOT IN (1, 11) AND v.type IN (1, 11)\n"
" ORDER BY t.schema_id, t.name, v.schema_id, v.name;\n"
"GRANT SELECT ON sys.dependency_tables_on_views TO PUBLIC;\n"
"CREATE VIEW sys.dependency_views_on_views AS\n"
"SELECT v1.schema_id AS view1_schema_id, v1.id AS view1_id, v1.name AS view1_name, v2.schema_id AS view2_schema_id, v2.id AS view2_id, v2.name AS view2_name, dep.depend_type AS depend_type\n"
"  FROM sys.tables AS v1, sys.tables AS v2, sys.dependencies AS dep\n"
" WHERE v1.id = dep.id AND v2.id = dep.depend_id\n"
"   AND dep.depend_type = 5 AND v1.type IN (1, 11) AND v2.type IN (1, 11)\n"
" ORDER BY v1.schema_id, v1.name, v2.schema_id, v2.name;\n"
"GRANT SELECT ON sys.dependency_views_on_views TO PUBLIC;\n"
"CREATE VIEW sys.dependency_columns_on_views AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, c.id AS column_id, c.name AS column_name, v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, dep.depend_type AS depend_type\n"
"  FROM sys.columns AS c, sys.tables AS v, sys.tables AS t, sys.dependencies AS dep\n"
" WHERE c.id = dep.id AND v.id = dep.depend_id AND c.table_id = t.id\n"
"   AND dep.depend_type = 5 AND v.type IN (1, 11)\n"
" ORDER BY t.schema_id, t.name, c.name, v.name;\n"
"GRANT SELECT ON sys.dependency_columns_on_views TO PUBLIC;\n"
"CREATE VIEW sys.dependency_functions_on_views AS\n"
"SELECT f.schema_id AS function_schema_id, f.id AS function_id, f.name AS function_name, v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS f, sys.tables AS v, sys.dependencies AS dep\n"
" WHERE f.id = dep.id AND v.id = dep.depend_id\n"
"   AND dep.depend_type = 5 AND v.type IN (1, 11)\n"
" ORDER BY f.schema_id, f.name, v.schema_id, v.name;\n"
"GRANT SELECT ON sys.dependency_functions_on_views TO PUBLIC;\n"
"CREATE VIEW sys.dependency_schemas_on_users AS\n"
"SELECT s.id AS schema_id, s.name AS schema_name, u.name AS user_name, CAST(6 AS smallint) AS depend_type\n"
"  FROM sys.users AS u, sys.schemas AS s\n"
" WHERE u.default_schema = s.id\n"
" ORDER BY s.name, u.name;\n"
"GRANT SELECT ON sys.dependency_schemas_on_users TO PUBLIC;\n"
"CREATE VIEW sys.dependency_tables_on_functions AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS f, sys.tables AS t, sys.dependencies AS dep\n"
" WHERE t.id = dep.id AND f.id = dep.depend_id\n"
"   AND dep.depend_type = 7 AND f.type <> 2 AND t.type NOT IN (1, 11)\n"
" ORDER BY t.name, t.schema_id, f.name, f.id;\n"
"GRANT SELECT ON sys.dependency_tables_on_functions TO PUBLIC;\n"
"CREATE VIEW sys.dependency_views_on_functions AS\n"
"SELECT v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS f, sys.tables AS v, sys.dependencies AS dep\n"
" WHERE v.id = dep.id AND f.id = dep.depend_id\n"
"   AND dep.depend_type = 7 AND f.type <> 2 AND v.type IN (1, 11)\n"
" ORDER BY v.name, v.schema_id, f.name, f.id;\n"
"GRANT SELECT ON sys.dependency_views_on_functions TO PUBLIC;\n"
"CREATE VIEW sys.dependency_columns_on_functions AS\n"
"SELECT c.table_id, c.id AS column_id, c.name, f.id AS function_id, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS f, sys.columns AS c, sys.dependencies AS dep\n"
" WHERE c.id = dep.id AND f.id = dep.depend_id\n"
"   AND dep.depend_type = 7 AND f.type <> 2\n"
" ORDER BY c.name, c.table_id, f.name, f.id;\n"
"GRANT SELECT ON sys.dependency_columns_on_functions TO PUBLIC;\n"
"CREATE VIEW sys.dependency_functions_on_functions AS\n"
"SELECT f1.schema_id, f1.id AS function_id, f1.name AS function_name, f1.type AS function_type,\n"
"       f2.schema_id AS used_in_function_schema_id, f2.id AS used_in_function_id, f2.name AS used_in_function_name, f2.type AS used_in_function_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS f1, sys.functions AS f2, sys.dependencies AS dep\n"
" WHERE f1.id = dep.id AND f2.id = dep.depend_id\n"
"   AND dep.depend_type = 7 AND f2.type <> 2\n"
" ORDER BY f1.name, f1.id, f2.name, f2.id;\n"
"GRANT SELECT ON sys.dependency_functions_on_functions TO PUBLIC;\n"
"CREATE VIEW sys.dependency_tables_on_triggers AS\n"
"(SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, tri.id AS trigger_id, tri.name AS trigger_name, CAST(8 AS smallint) AS depend_type\n"
"  FROM sys.tables AS t, sys.triggers AS tri\n"
" WHERE tri.table_id = t.id)\n"
"UNION\n"
"(SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, tri.id AS trigger_id, tri.name AS trigger_name, dep.depend_type AS depend_type\n"
"  FROM sys.tables AS t, sys.triggers AS tri, sys.dependencies AS dep\n"
" WHERE dep.id = t.id AND dep.depend_id = tri.id\n"
"   AND dep.depend_type = 8)\n"
" ORDER BY table_schema_id, table_name, trigger_name;\n"
"GRANT SELECT ON sys.dependency_tables_on_triggers TO PUBLIC;\n"
"CREATE VIEW sys.dependency_columns_on_triggers AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, tri.id AS trigger_id, tri.name AS trigger_name, c.id AS column_id, c.name AS column_name, dep.depend_type AS depend_type\n"
"  FROM sys.tables AS t, sys.columns AS c, sys.triggers AS tri, sys.dependencies AS dep\n"
" WHERE dep.id = c.id AND dep.depend_id = tri.id AND c.table_id = t.id\n"
"   AND dep.depend_type = 8\n"
" ORDER BY t.schema_id, t.name, tri.name, c.name;\n"
"GRANT SELECT ON sys.dependency_columns_on_triggers TO PUBLIC;\n"
"CREATE VIEW sys.dependency_functions_on_triggers AS\n"
"SELECT f.schema_id AS function_schema_id, f.id AS function_id, f.name AS function_name, f.type AS function_type,\n"
"       tri.id AS trigger_id, tri.name AS trigger_name, tri.table_id AS trigger_table_id, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS f, sys.triggers AS tri, sys.dependencies AS dep\n"
" WHERE dep.id = f.id AND dep.depend_id = tri.id\n"
"   AND dep.depend_type = 8\n"
" ORDER BY f.schema_id, f.name, tri.name;\n"
"GRANT SELECT ON sys.dependency_functions_on_triggers TO PUBLIC;\n"
"CREATE VIEW sys.dependency_tables_on_indexes AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, i.id AS index_id, i.name AS index_name, i.type AS index_type, CAST(10 AS smallint) AS depend_type\n"
"  FROM sys.tables AS t, sys.idxs AS i\n"
" WHERE i.table_id = t.id\n"
"    -- exclude internal system generated and managed indexes for enforcing declarative PKey and Unique constraints\n"
"   AND (i.table_id, i.name) NOT IN (SELECT k.table_id, k.name FROM sys.keys k)\n"
" ORDER BY t.schema_id, t.name, i.name;\n"
"GRANT SELECT ON sys.dependency_tables_on_indexes TO PUBLIC;\n"
"CREATE VIEW sys.dependency_columns_on_indexes AS\n"
"SELECT c.id AS column_id, c.name AS column_name, t.id AS table_id, t.name AS table_name, t.schema_id, i.id AS index_id, i.name AS index_name, i.type AS index_type, CAST(ic.nr +1 AS INT) AS seq_nr, CAST(10 AS smallint) AS depend_type\n"
"  FROM sys.tables AS t, sys.columns AS c, sys.objects AS ic, sys.idxs AS i\n"
" WHERE ic.name = c.name AND ic.id = i.id AND c.table_id = i.table_id AND c.table_id = t.id\n"
"    -- exclude internal system generated and managed indexes for enforcing declarative PKey and Unique constraints\n"
"   AND (i.table_id, i.name) NOT IN (SELECT k.table_id, k.name FROM sys.keys k)\n"
" ORDER BY c.name, t.name, t.schema_id, i.name, ic.nr;\n"
"GRANT SELECT ON sys.dependency_columns_on_indexes TO PUBLIC;\n"
"CREATE VIEW sys.dependency_tables_on_foreignkeys AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, fk.name AS fk_name, CAST(k.type AS smallint) AS key_type, CAST(11 AS smallint) AS depend_type\n"
"  FROM sys.tables AS t, sys.keys AS k, sys.keys AS fk\n"
" WHERE fk.rkey = k.id and k.table_id = t.id\n"
" ORDER BY t.schema_id, t.name, fk.name;\n"
"GRANT SELECT ON sys.dependency_tables_on_foreignkeys TO PUBLIC;\n"
"CREATE VIEW sys.dependency_keys_on_foreignkeys AS\n"
"SELECT k.table_id AS key_table_id, k.id AS key_id, k.name AS key_name, fk.table_id AS fk_table_id, fk.id AS fk_id, fk.name AS fk_name, CAST(k.type AS smallint) AS key_type, CAST(11 AS smallint) AS depend_type\n"
"  FROM sys.keys AS k, sys.keys AS fk\n"
" WHERE k.id = fk.rkey\n"
" ORDER BY k.name, fk.name;\n"
"GRANT SELECT ON sys.dependency_keys_on_foreignkeys TO PUBLIC;\n"
"CREATE VIEW sys.dependency_tables_on_procedures AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, p.id AS procedure_id, p.name AS procedure_name, p.type AS procedure_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS p, sys.tables AS t, sys.dependencies AS dep\n"
" WHERE t.id = dep.id AND p.id = dep.depend_id\n"
"   AND dep.depend_type = 13 AND p.type = 2 AND t.type NOT IN (1, 11)\n"
" ORDER BY t.name, t.schema_id, p.name, p.id;\n"
"GRANT SELECT ON sys.dependency_tables_on_procedures TO PUBLIC;\n"
"CREATE VIEW sys.dependency_views_on_procedures AS\n"
"SELECT v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, p.id AS procedure_id, p.name AS procedure_name, p.type AS procedure_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS p, sys.tables AS v, sys.dependencies AS dep\n"
" WHERE v.id = dep.id AND p.id = dep.depend_id\n"
"   AND dep.depend_type = 13 AND p.type = 2 AND v.type IN (1, 11)\n"
" ORDER BY v.name, v.schema_id, p.name, p.id;\n"
"GRANT SELECT ON sys.dependency_views_on_procedures TO PUBLIC;\n"
"CREATE VIEW sys.dependency_columns_on_procedures AS\n"
"SELECT c.table_id, c.id AS column_id, c.name AS column_name, p.id AS procedure_id, p.name AS procedure_name, p.type AS procedure_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS p, sys.columns AS c, sys.dependencies AS dep\n"
" WHERE c.id = dep.id AND p.id = dep.depend_id\n"
"   AND dep.depend_type = 13 AND p.type = 2\n"
" ORDER BY c.name, c.table_id, p.name, p.id;\n"
"GRANT SELECT ON sys.dependency_columns_on_procedures TO PUBLIC;\n"
"CREATE VIEW sys.dependency_functions_on_procedures AS\n"
"SELECT f.schema_id AS function_schema_id, f.id AS function_id, f.name AS function_name, f.type AS function_type,\n"
"       p.schema_id AS procedure_schema_id, p.id AS procedure_id, p.name AS procedure_name, p.type AS procedure_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS p, sys.functions AS f, sys.dependencies AS dep\n"
" WHERE f.id = dep.id AND p.id = dep.depend_id\n"
"   AND dep.depend_type = 13 AND p.type = 2\n"
" ORDER BY p.name, p.id, f.name, f.id;\n"
"GRANT SELECT ON sys.dependency_functions_on_procedures TO PUBLIC;\n"
"CREATE VIEW sys.dependency_columns_on_types AS\n"
"SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, dt.id AS type_id, dt.sqlname AS type_name, c.id AS column_id, c.name AS column_name, dep.depend_type AS depend_type\n"
"  FROM sys.tables AS t, sys.columns AS c, sys.types AS dt, sys.dependencies AS dep\n"
" WHERE dep.id = dt.id AND dep.depend_id = c.id AND c.table_id = t.id\n"
"   AND dep.depend_type = 15\n"
" ORDER BY dt.sqlname, t.name, c.name, c.id;\n"
"GRANT SELECT ON sys.dependency_columns_on_types TO PUBLIC;\n"
"CREATE VIEW sys.dependency_functions_on_types AS\n"
"SELECT dt.id AS type_id, dt.sqlname AS type_name, f.id AS function_id, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type\n"
"  FROM sys.functions AS f, sys.types AS dt, sys.dependencies AS dep\n"
" WHERE dep.id = dt.id AND dep.depend_id = f.id\n"
"   AND dep.depend_type = 15\n"
" ORDER BY dt.sqlname, f.name, f.id;\n"
"GRANT SELECT ON sys.dependency_functions_on_types TO PUBLIC;\n"
"CREATE VIEW sys.dependency_args_on_types AS\n"
"SELECT dt.id AS type_id, dt.sqlname AS type_name, f.id AS function_id, f.name AS function_name, a.id AS arg_id, a.name AS arg_name, a.number AS arg_nr, dep.depend_type AS depend_type\n"
"  FROM sys.args AS a, sys.functions AS f, sys.types AS dt, sys.dependencies AS dep\n"
" WHERE dep.id = dt.id AND dep.depend_id = a.id AND a.func_id = f.id\n"
"   AND dep.depend_type = 15\n"
" ORDER BY dt.sqlname, f.name, a.number, a.name;\n"
"GRANT SELECT ON sys.dependency_args_on_types TO PUBLIC;\n"
"UPDATE sys._tables SET system = true\n"
" WHERE name IN ('ids', 'dependencies_vw', 'dependency_owners_on_schemas', 'dependency_columns_on_keys',\n"
" 'dependency_tables_on_views', 'dependency_views_on_views', 'dependency_columns_on_views', 'dependency_functions_on_views',\n"
" 'dependency_schemas_on_users',\n"
" 'dependency_tables_on_functions', 'dependency_views_on_functions', 'dependency_columns_on_functions', 'dependency_functions_on_functions',\n"
" 'dependency_tables_on_triggers', 'dependency_columns_on_triggers', 'dependency_functions_on_triggers',\n"
" 'dependency_tables_on_indexes', 'dependency_columns_on_indexes',\n"
" 'dependency_tables_on_foreignkeys', 'dependency_keys_on_foreignkeys',\n"
" 'dependency_tables_on_procedures', 'dependency_views_on_procedures', 'dependency_columns_on_procedures', 'dependency_functions_on_procedures',\n"
" 'dependency_columns_on_types', 'dependency_functions_on_types', 'dependency_args_on_types')\n"
" AND schema_id IN (SELECT id FROM sys.schemas WHERE name = 'sys');\n"
	);

	/* 25_debug.sql */
	t = mvc_bind_table(sql, s, "environment");
	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.environment cascade;\n"
			"drop function sys.environment() cascade;\n"
			"create view sys.environment as select * from sys.env();\n"
			"GRANT SELECT ON sys.environment TO PUBLIC;\n"
			"update sys._tables set system = true where system = false and name = 'environment' and schema_id in (select id from sys.schemas where name = 'sys');\n");

	/* 39_analytics.sql, 39_analytics_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop aggregate corr(tinyint, tinyint);\n"
			"drop aggregate corr(smallint, smallint);\n"
			"drop aggregate corr(integer, integer);\n"
			"drop aggregate corr(bigint, bigint);\n"
			"drop aggregate corr(real, real);\n");
#ifdef HAVE_HGE
	if (have_hge)
		pos += snprintf(buf + pos, bufsize - pos,
				"drop aggregate corr(hugeint, hugeint);\n");
#endif
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate corr(e1 TINYINT, e2 TINYINT) returns DOUBLE\n\texternal name \"aggr\".\"corr\";\n"
			"grant execute on aggregate sys.corr(tinyint, tinyint) to public;\n"
			"create aggregate corr(e1 SMALLINT, e2 SMALLINT) returns DOUBLE\n\texternal name \"aggr\".\"corr\";\n"
			"grant execute on aggregate sys.corr(smallint, smallint) to public;\n"
			"create aggregate corr(e1 INTEGER, e2 INTEGER) returns DOUBLE\n\texternal name \"aggr\".\"corr\";\n"
			"grant execute on aggregate sys.corr(integer, integer) to public;\n"
			"create aggregate corr(e1 BIGINT, e2 BIGINT) returns DOUBLE\n\texternal name \"aggr\".\"corr\";\n"
			"grant execute on aggregate sys.corr(bigint, bigint) to public;\n"
			"create aggregate corr(e1 REAL, e2 REAL) returns DOUBLE\n\texternal name \"aggr\".\"corr\";\n"
			"grant execute on aggregate sys.corr(real, real) to public;\n");
#ifdef HAVE_HGE
	if (have_hge)
		pos += snprintf(buf + pos, bufsize - pos,
				"create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n\texternal name \"aggr\".\"corr\";\n"
			"grant execute on aggregate sys.corr(hugeint, hugeint) to public;\n");
#endif
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where name = 'corr' and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* 51_sys_schema_extensions.sql */
	t = mvc_bind_table(sql, s, "privilege_codes");
	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"CREATE VIEW sys.roles AS SELECT id, name, grantor FROM sys.auths a WHERE a.name NOT IN (SELECT u.name FROM sys.db_users() u);\n"
			"GRANT SELECT ON sys.roles TO PUBLIC;\n"
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
			"GRANT SELECT ON sys.var_values TO PUBLIC;\n"
			"UPDATE sys._tables SET system = true\n"
			" WHERE name IN ('roles', 'var_values') AND schema_id IN (SELECT id FROM sys.schemas WHERE name = 'sys');\n"
			"ALTER TABLE sys.privilege_codes SET READ WRITE;\n"
			"DROP TABLE sys.privilege_codes;\n"
			"CREATE TABLE sys.privilege_codes (\n"
			"    privilege_code_id   INT NOT NULL PRIMARY KEY,\n"
			"    privilege_code_name VARCHAR(40) NOT NULL UNIQUE);\n"
			"INSERT INTO sys.privilege_codes (privilege_code_id, privilege_code_name) VALUES\n"
			"  (1, 'SELECT'),\n"
			"  (2, 'UPDATE'),\n"
			"  (4, 'INSERT'),\n"
			"  (8, 'DELETE'),\n"
			"  (16, 'EXECUTE'),\n"
			"  (32, 'GRANT'),\n"
			"  (64, 'TRUNCATE'),\n"
			"  (3, 'SELECT,UPDATE'),\n"
			"  (5, 'SELECT,INSERT'),\n"
			"  (6, 'INSERT,UPDATE'),\n"
			"  (7, 'SELECT,INSERT,UPDATE'),\n"
			"  (9, 'SELECT,DELETE'),\n"
			"  (10, 'UPDATE,DELETE'),\n"
			"  (11, 'SELECT,UPDATE,DELETE'),\n"
			"  (12, 'INSERT,DELETE'),\n"
			"  (13, 'SELECT,INSERT,DELETE'),\n"
			"  (14, 'INSERT,UPDATE,DELETE'),\n"
			"  (15, 'SELECT,INSERT,UPDATE,DELETE'),\n"
			"  (65, 'SELECT,TRUNCATE'),\n"
			"  (66, 'UPDATE,TRUNCATE'),\n"
			"  (68, 'INSERT,TRUNCATE'),\n"
			"  (72, 'DELETE,TRUNCATE'),\n"
			"  (67, 'SELECT,UPDATE,TRUNCATE'),\n"
			"  (69, 'SELECT,INSERT,TRUNCATE'),\n"
			"  (73, 'SELECT,DELETE,TRUNCATE'),\n"
			"  (70, 'INSERT,UPDATE,TRUNCATE'),\n"
			"  (76, 'INSERT,DELETE,TRUNCATE'),\n"
			"  (74, 'UPDATE,DELETE,TRUNCATE'),\n"
			"  (71, 'SELECT,INSERT,UPDATE,TRUNCATE'),\n"
			"  (75, 'SELECT,UPDATE,DELETE,TRUNCATE'),\n"
			"  (77, 'SELECT,INSERT,DELETE,TRUNCATE'),\n"
			"  (78, 'INSERT,UPDATE,DELETE,TRUNCATE'),\n"
			"  (79, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE');\n"
			"ALTER TABLE sys.privilege_codes SET READ ONLY;\n"
			"GRANT SELECT ON sys.privilege_codes TO PUBLIC;\n"
			"UPDATE sys._tables "
			"SET system = TRUE "
			"WHERE name = 'privilege_codes' "
			"AND schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys');\n"
			"ALTER TABLE sys.keywords SET READ WRITE;\n"
			"INSERT INTO sys.keywords VALUES ('COMMENT'), ('CONTINUE'), ('START'), ('TRUNCATE');\n"
			"ALTER TABLE sys.function_types SET READ WRITE;\n"
			"ALTER TABLE function_types ADD COLUMN function_type_keyword VARCHAR(30);\n"
			"UPDATE sys.function_types SET function_type_keyword =\n"
			"    (SELECT kw FROM (VALUES\n"
			"        (1, 'FUNCTION'),\n"
			"        (2, 'PROCEDURE'),\n"
			"        (3, 'AGGREGATE'),\n"
			"        (4, 'FILTER FUNCTION'),\n"
			"        (5, 'FUNCTION'),\n"
			"        (6, 'FUNCTION'),\n"
			"        (7, 'LOADER'))\n"
			"    AS ft (id, kw) WHERE function_type_id = id);\n"
			"ALTER TABLE sys.function_types ALTER COLUMN function_type_keyword SET NOT NULL;\n"
			"ALTER TABLE sys.function_languages SET READ WRITE;\n"
			"ALTER TABLE sys.function_languages ADD COLUMN language_keyword VARCHAR(20);\n"
			"UPDATE sys.function_languages SET language_keyword =\n"
			"    (SELECT kw FROM (VALUES\n"
			"        (3, 'R'),\n"
			"        (6, 'PYTHON'),\n"
			"        (7, 'PYTHON_MAP'),\n"
			"        (8, 'PYTHON2'),\n"
			"        (9, 'PYTHON2_MAP'),\n"
			"        (10, 'PYTHON3'),\n"
			"        (11, 'PYTHON3_MAP'))\n"
			"    AS ft (id, kw) WHERE language_id = id);\n"
			"INSERT INTO sys.function_languages VALUES (4, 'C', 'C'), (12, 'C++', 'CPP');\n"
		);

	/* 60_wlcr.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create procedure master()\n"
			"external name wlc.master;\n"
			"create procedure master(path string)\n"
			"external name wlc.master;\n"
			"create procedure stopmaster()\n"
			"external name wlc.stopmaster;\n"
			"create procedure masterbeat( duration int)\n"
			"external name wlc.\"setmasterbeat\";\n"
			"create function masterClock() returns string\n"
			"external name wlc.\"getmasterclock\";\n"
			"create function masterTick() returns bigint\n"
			"external name wlc.\"getmastertick\";\n"
			"create procedure replicate()\n"
			"external name wlr.replicate;\n"
			"create procedure replicate(pointintime timestamp)\n"
			"external name wlr.replicate;\n"
			"create procedure replicate(dbname string)\n"
			"external name wlr.replicate;\n"
			"create procedure replicate(dbname string, pointintime timestamp)\n"
			"external name wlr.replicate;\n"
			"create procedure replicate(dbname string, id tinyint)\n"
			"external name wlr.replicate;\n"
			"create procedure replicate(dbname string, id smallint)\n"
			"external name wlr.replicate;\n"
			"create procedure replicate(dbname string, id integer)\n"
			"external name wlr.replicate;\n"
			"create procedure replicate(dbname string, id bigint)\n"
			"external name wlr.replicate;\n"
			"create procedure replicabeat(duration integer)\n"
			"external name wlr.\"setreplicabeat\";\n"
			"create function replicaClock() returns string\n"
			"external name wlr.\"getreplicaclock\";\n"
			"create function replicaTick() returns bigint\n"
			"external name wlr.\"getreplicatick\";\n"
			"update sys.functions set system = true where name in ('master', 'stopmaster', 'masterbeat', 'masterclock', 'mastertick', 'replicate', 'replicabeat', 'replicaclock', 'replicatick') and schema_id = (select id from sys.schemas where name = 'sys');\n"
		);

	/* comments */
	pos += snprintf(buf + pos, bufsize - pos,
			"UPDATE sys._tables\n"
			"SET system = true\n"
			"WHERE name = 'comments'\n"
			"AND schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys');\n"
		);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	if (err == MAL_SUCCEED) {
		pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
			       "ALTER TABLE sys.keywords SET READ ONLY;\n"
			       "ALTER TABLE sys.function_types SET READ ONLY;\n"
			       "ALTER TABLE sys.function_languages SET READ ONLY;\n");
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#ifdef HAVE_NETCDF
static str
sql_update_mar2018_netcdf(Client c, const char *prev_schema)
{
	size_t bufsize = 1000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");

	/* 74_netcdf.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"grant select on sys.netcdf_files to public;\n"
			"grant select on sys.netcdf_dims to public;\n"
			"grant select on sys.netcdf_vars to public;\n"
			"grant select on sys.netcdf_vardim to public;\n"
			"grant select on sys.netcdf_attrs to public;\n"
			"grant execute on procedure sys.netcdf_attach(varchar(256)) to public;\n"
			"grant execute on procedure sys.netcdf_importvar(integer, varchar(256)) to public;\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif	/* HAVE_NETCDF */

static str
sql_update_mar2018_sp1(Client c, const char *prev_schema)
{
	size_t bufsize = 2048, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"drop function sys.dependencies_functions_os_triggers();\n"
			"CREATE FUNCTION dependencies_functions_on_triggers()\n"
			"RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))\n"
			"RETURN TABLE (SELECT f.name, tri.name, 'DEP_TRIGGER' from functions as f, triggers as tri, dependencies as dep where dep.id = f.id AND dep.depend_id =tri.id AND dep.depend_type = 8);\n"
			"update sys.functions set system = true where name in ('dependencies_functions_on_triggers') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_remote_tables(Client c, mvc *sql, const char *prev_schema)
{
	res_table *output = NULL;
	char* err = MAL_SUCCEED, *buf;
	size_t bufsize = 1000, pos = 0;
	BAT *tbl = NULL, *uri = NULL;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* Create the SQL function needed to dump the remote table credentials */
	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.remote_table_credentials (tablename string)"
			" returns table (\"uri\" string, \"username\" string, \"hash\" string)"
			" external name sql.rt_credentials;\n"
			"update sys.functions set system = true where name = 'remote_table_credentials' and schema_id = (select id from sys.schemas where name = 'sys');\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "create function", true, false, NULL);
	if (err)
		goto bailout;

	pos = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"SELECT concat(concat(scm.name, '.'), tbl.name), tbl.query"
			" FROM sys._tables AS tbl JOIN sys.schemas AS scm ON"
			" tbl.schema_id=scm.id WHERE tbl.type=5;\n");

	assert(pos < bufsize);

	err = SQLstatementIntern(c, &buf, "get remote table names", true, false, &output);
	if (err)
		goto bailout;

	/* We executed the query, now process the results */
	tbl = BATdescriptor(output->cols[0].b);
	uri = BATdescriptor(output->cols[1].b);

	if (tbl && uri) {
		size_t cnt;
		assert(BATcount(tbl) == BATcount(uri));
		if ((cnt = BATcount(tbl)) > 0) {
			BATiter tbl_it = bat_iterator(tbl);
			BATiter uri_it = bat_iterator(uri);
			const void *restrict nil = ATOMnilptr(tbl->ttype);
			int (*cmp)(const void *, const void *) = ATOMcompare(tbl->ttype);
			const char *v;
			const char *u;
			const char *remote_server_uri;

			/* This is probably not correct: offsets? */
			for (BUN i = 0; i < cnt; i++) {
				v = BUNtvar(tbl_it, i);
				u = BUNtvar(uri_it, i);
				if (v == NULL || (*cmp)(v, nil) == 0 ||
				    u == NULL || (*cmp)(u, nil) == 0)
					goto bailout;

				/* Since the loop might fail, it might be a good idea
				 * to update the credentials as a second step
				 */
				remote_server_uri = mapiuri_uri((char *)u, sql->sa);
				if ((err = AUTHaddRemoteTableCredentials((char *)v, "monetdb", remote_server_uri, "monetdb", "monetdb", false)) != MAL_SUCCEED)
					goto bailout;
			}
		}
	} else {
		err = createException(SQL, "sql_update_remote_tables", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

bailout:
	if (tbl)
		BBPunfix(tbl->batCacheid);
	if (uri)
		BBPunfix(uri->batCacheid);
	if (output)
		res_table_destroy(output);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_replace_Mar2018_ids_view(Client c, mvc *sql, const char *prev_schema)
{
	size_t bufsize = 4400, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t = mvc_bind_table(sql, s, "ids");

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	t->system = 0;	/* make it non-system else the drop view will fail */
	t = mvc_bind_table(sql, s, "dependencies_vw");	/* dependencies_vw uses view sys.ids so must be removed first */
	t->system = 0;

	/* 21_dependency_views.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"DROP VIEW sys.dependencies_vw;\n"
			"DROP VIEW sys.ids;\n"

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
			"SELECT id, sqlname, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'type', 'sys.types' FROM sys.types WHERE id > 2000 /* exclude system types to prevent duplicates with auths.id */\n"
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

			"update sys._tables set system = true where name in ('ids', 'dependencies_vw') and schema_id in (select id from sys.schemas where name = 'sys');\n"
			);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_gsl(Client c, const char *prev_schema)
{
	size_t bufsize = 1024, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"drop function sys.chi2prob(double, double);\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_aug2018(Client c, mvc *sql, const char *prev_schema)
{
	size_t bufsize = 1000, pos = 0;
	char *buf, *err;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.group_concat(str string) returns string external name \"aggr\".\"str_group_concat\";\n"
			"grant execute on aggregate sys.group_concat(string) to public;\n"
			"create aggregate sys.group_concat(str string, sep string) returns string external name \"aggr\".\"str_group_concat\";\n"
			"grant execute on aggregate sys.group_concat(string, string) to public;\n"
			"update sys.functions set system = true where name in ('group_concat') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	if (err)
		goto bailout;
	err = sql_update_remote_tables(c, sql, prev_schema);

  bailout:
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_aug2018_sp2(Client c, const char *prev_schema)
{
	size_t bufsize = 1000, pos = 0;
	char *buf, *err;
	res_table *output;
	BAT *b;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* required update for changeset 23e1231ada99 */
	pos += snprintf(buf + pos, bufsize - pos,
			"select id from sys.functions where language <> 0 and not side_effect and type <> 4 and (type = 2 or (language <> 2 and id not in (select func_id from sys.args where inout = 1)));\n");
	err = SQLstatementIntern(c, &buf, "update", true, false, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) > 0) {
			pos = 0;
			pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");
			pos += snprintf(buf + pos, bufsize - pos,
					"update sys.functions set side_effect = true where language <> 0 and not side_effect and type <> 4 and (type = 2 or (language <> 2 and id not in (select func_id from sys.args where inout = 1)));\n");

			pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
		}
		BBPunfix(b->batCacheid);
	}
	res_table_destroy(output);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

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
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
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
			"update sys.functions set system = true where name = 'querylog_enable' and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* 17_temporal.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.date_trunc(txt string, t timestamp)\n"
			"returns timestamp\n"
			"external name sql.date_trunc;\n"
			"grant execute on function sys.date_trunc(string, timestamp) to public;\n"
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys') and name = 'date_trunc' and type = %d;\n", (int) F_FUNC);

	/* 22_clients.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create procedure sys.setprinttimeout(\"timeout\" integer)\n"
			"external name clients.setprinttimeout;\n"
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys') and name = 'setprinttimeout' and type = %d;\n", (int) F_PROC);

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

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	if (err == MAL_SUCCEED) {
		pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
			       "ALTER TABLE sys.keywords SET READ ONLY;\n");

		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	}

	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_storagemodel(Client c, mvc *sql, const char *prev_schema)
{
	size_t bufsize = 20000, pos = 0;
	char *buf, *err;
	sql_schema *s = mvc_bind_schema(sql, "sys");
	sql_table *t;

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
		"drop function if exists sys.storagemodel() cascade;\n"
		"drop function if exists sys.imprintsize(bigint, clob) cascade;\n"
		"drop function if exists sys.hashsize(boolean, bigint) cascade;\n"
		"drop function if exists sys.heapsize(clob, bigint, int) cascade;\n"
		"drop function if exists sys.columnsize(clob, bigint, bigint) cascade;\n"
		"drop procedure if exists sys.storagemodelinit();\n"
		"drop table if exists sys.storagemodelinput cascade;\n"
		"drop view if exists sys.\"storage\" cascade;\n"
		"drop function if exists sys.\"storage\"(clob, clob, clob) cascade;\n"
		"drop function if exists sys.\"storage\"(clob, clob) cascade;\n"
		"drop function if exists sys.\"storage\"(clob) cascade;\n"
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
		"	if tpe in ('int', 'real', 'date', 'time', 'timetz', 'sec_interval', 'month_interval')\n"
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
		"	if tpe in ('int', 'real', 'date', 'time', 'timetz', 'sec_interval', 'month_interval')\n"
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
		"order by \"schema\", \"table\";\n"
	);
	assert(pos < bufsize);

	pos += snprintf(buf + pos, bufsize - pos,
		"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
		" and name in ('storage', 'tablestorage', 'schemastorage', 'storagemodelinput', 'storagemodel', 'tablestoragemodel');\n");
	pos += snprintf(buf + pos, bufsize - pos,
		"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
		" and name in ('storage') and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
		"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
		" and name in ('storagemodelinit') and type = %d;\n", (int) F_PROC);
	pos += snprintf(buf + pos, bufsize - pos,
		"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
		" and name in ('columnsize', 'heapsize', 'hashsize', 'imprintsize') and type = %d;\n", (int) F_FUNC);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
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
	err = SQLstatementIntern(c, &qry, "update", true, false, &output);
	if (err == NULL) {
		BAT *b = BATdescriptor(output->cols[0].b);
		if (b) {
			if (BATcount(b) > 0) {
				/* required update for changeset 23e1231ada99 */
				qry = "insert into sys.dependencies (select c.id as id, k.id as depend_id, dt.dependency_type_id as depend_type from sys.dependency_types dt, sys._columns c, sys.keys k, sys.objects o where k.id = o.id and o.name = c.name and c.table_id = k.table_id and dt.dependency_type_name = 'KEY' and k.type = 1 and not exists (select d.id from sys.dependencies d where d.id = c.id and d.depend_id = k.id and d.depend_type = dt.dependency_type_id));\n";
				printf("Running database upgrade commands:\n%s\n", qry);
				err = SQLstatementIntern(c, &qry, "update", true, false, NULL);
			}
			BBPunfix(b->batCacheid);
		}
	}
	if (output != NULL)
		res_tables_destroy(output);

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
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#define FLUSH_INSERTS_IF_BUFFERFILLED /* Each new value should add about 20 bytes to the buffer, "flush" when is 200 bytes from being full */ \
	if (pos > 7900) { \
		pos += snprintf(buf + pos, bufsize - pos, \
						") as t1(c1,c2,c3) where t1.c1 not in (select \"id\" from sys.dependencies where depend_id = t1.c2);\n"); \
		assert(pos < bufsize); \
		printf("Running database upgrade commands:\n%s\n", buf); \
		err = SQLstatementIntern(c, &buf, "update", true, false, NULL); \
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

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (!(sql->sa = sa_create())) {
		err = createException(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	pos += snprintf(buf + pos, bufsize - pos, "insert into sys.dependencies select c1, c2, c3 from (values");
	ppos = pos; /* later check if found updatable database objects */

	for (node *n = sql->session->tr->schemas.set->h; n; n = n->next) {
		sql_schema *s = (sql_schema*) n->data;

		if (s->funcs.set)
			for (node *m = s->funcs.set->h; m; m = m->next) {
				sql_func *f = (sql_func*) m->data;

				if (f->query && f->lang == FUNC_LANG_SQL) {
					char *relt;
					sql_rel *r = NULL;

					if (!(relt = sa_strdup(sql->sa, f->query))) {
						err = createException(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}

					r = rel_parse(sql, s, relt, m_deps);
					if (r)
						r = sql_processrelation(sql, r, 0);
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
		if (s->tables.set)
			for (node *m = s->tables.set->h; m; m = m->next) {
				sql_table *t = (sql_table*) m->data;

				if (t->query && isView(t)) {
					char *relt;
					sql_rel *r = NULL;

					if (!(relt = sa_strdup(sql->sa, t->query))) {
						err = createException(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}

					r = rel_parse(sql, s, relt, m_deps);
					if (r)
						r = sql_processrelation(sql, r, 0);
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
				if (t->triggers.set)
					for (node *mm = t->triggers.set->h; mm; mm = mm->next) {
						sql_trigger *tr = (sql_trigger*) mm->data;
						char *relt;
						sql_rel *r = NULL;

						if (!(relt = sa_strdup(sql->sa, tr->statement))) {
							err = createException(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
							goto bailout;
						}

						r = rel_parse(sql, s, relt, m_deps);
						if (r)
							r = sql_processrelation(sql, r, 0);
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

	if (ppos != pos) { /* found updatable functions */
		pos += snprintf(buf + pos, bufsize - pos,
						") as t1(c1,c2,c3) where t1.c1 not in (select \"id\" from sys.dependencies where depend_id = t1.c2);\n");

		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
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
	err = SQLstatementIntern(c, &buf, "update", 1, 0, &output);
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
			"\n"
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
	if (have_hge) {
		pos += snprintf(buf + pos, bufsize - pos,
				"create aggregate median_avg(val HUGEINT) returns DOUBLE\n"
				" external name \"aggr\".\"median_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE median_avg(HUGEINT) TO PUBLIC;\n"
				"create aggregate quantile_avg(val HUGEINT, q DOUBLE) returns DOUBLE\n"
				" external name \"aggr\".\"quantile_avg\";\n"
				"GRANT EXECUTE ON AGGREGATE quantile_avg(HUGEINT, DOUBLE) TO PUBLIC;\n");
	}
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
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('deltas') and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('median_avg', 'quantile_avg') and type = %d;\n", (int) F_AGGR);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.schemas set system = true where name in ('wlc', 'wlr');\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'wlc')"
			" and name in ('clock', 'tick') and type = %d;\n", (int) F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'wlc')"
			" and name in ('master', 'stop', 'flush', 'beat') and type = %d;\n", (int) F_PROC);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'wlr')"
			" and name in ('clock', 'tick') and type = %d;\n", (int) F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'wlr')"
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
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'median', 'quantile') and type = %d;\n", (int) F_AGGR);

	/* The MAL implementation of functions json.text(string) and json.text(int) do not exist */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function json.text(string);\n"
			"drop function json.text(int);\n");

	/* The first argument to copyfrom is a PTR type */
	pos += snprintf(buf + pos, bufsize - pos,
			"update \"sys\".\"args\" set \"type\" = 'ptr' where"
			" \"func_id\" = (select \"id\" from \"sys\".\"functions\" where \"name\" = 'copyfrom' and \"func\" = 'copy_from' and \"mod\" = 'sql') and \"name\" = 'arg_1';\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
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
			"update sys.functions set system = true where name in ('median_avg', 'quantile_avg') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_update_linear_hashing(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	sql_table *t;
	size_t bufsize = 8192, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);
	sql_schema *sys = mvc_bind_schema(sql, "sys");

	if (!*systabfixed &&
	    (err = sql_fix_system_tables(c, sql, prev_schema)) != NULL)
		return err;
	*systabfixed = true;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"create procedure suspend_log_flushing()\n"
			" external name sql.suspend_log_flushing;\n"
			"create procedure resume_log_flushing()\n"
			" external name sql.resume_log_flushing;\n"
			"create procedure hot_snapshot(tarfile string)\n"
			" external name sql.hot_snapshot;\n"
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('suspend_log_flushing', 'resume_log_flushing', 'hot_snapshot') and type = %d;\n", (int) F_PROC);

	/* 12_url */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function isaURL(url);\n"
			"CREATE function isaURL(theUrl string) RETURNS BOOL\n"
			" EXTERNAL NAME url.\"isaURL\";\n"
			"GRANT EXECUTE ON FUNCTION isaURL(string) TO PUBLIC;\n"
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'isaurl' and type = %d;\n", (int) F_FUNC);

	/* 16_tracelog */
	t = mvc_bind_table(sql, sys, "tracelog");
	t->system = 0; /* make it non-system else the drop view will fail */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tracelog;\n"
			"drop function sys.tracelog();\n"
			"create function sys.tracelog()\n"
			"	returns table (\n"
			"		ticks bigint, -- time in microseconds\n"
			"		stmt string  -- actual statement executed\n"
			"	)\n"
			" external name sql.dump_trace;\n"
			"create view sys.tracelog as select * from sys.tracelog();\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'tracelog' and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'tracelog';\n");

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
			"create procedure sys.setoptimizer(\"optimizer\" string)\n"
			" external name clients.setoptimizer;\n"
			"create procedure sys.setquerytimeout(\"query\" int)\n"
			" external name clients.setquerytimeout;\n"
			"create procedure sys.setsessiontimeout(\"timeout\" int)\n"
			" external name clients.setsessiontimeout;\n"
			"create procedure sys.setworkerlimit(\"limit\" int)\n"
			" external name clients.setworkerlimit;\n"
			"create procedure sys.setmemorylimit(\"limit\" int)\n"
			" external name clients.setmemorylimit;\n"
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
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('sessions', 'prepared_statements', 'prepared_statements_args') and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('sessions', 'prepared_statements', 'prepared_statements_args');\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('setoptimizer', 'setquerytimeout', 'setsessiontimeout', 'setworkerlimit', 'setmemorylimit', 'setoptimizer', 'stopsession') and type = %d;\n", (int) F_PROC);

	/* 25_debug */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.debug(flag string) returns integer\n"
			" external name mdb.\"setDebug\";\n"
			"create function sys.debugflags()\n"
			" returns table(flag string, val bool)\n"
			" external name mdb.\"getDebugFlags\";\n"
			"create procedure sys.\"sleep\"(msecs int)\n"
			" external name \"alarm\".\"sleep\";\n"
			"create function sys.\"sleep\"(msecs int) returns integer\n"
			" external name \"alarm\".\"sleep\";\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('debug', 'debugflags', 'sleep');\n");

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
			" external name sql.sysmon_queue;\n"
			"grant execute on function sys.queue to public;\n"
			"create view sys.queue as select * from sys.queue();\n"
			"grant select on sys.queue to public;\n"

			"create procedure sys.pause(tag tinyint)\n"
			"external name sql.sysmon_pause;\n"
			"create procedure sys.resume(tag tinyint)\n"
			"external name sql.sysmon_resume;\n"
			"create procedure sys.stop(tag tinyint)\n"
			"external name sql.sysmon_stop;\n"

			"create procedure sys.pause(tag smallint)\n"
			"external name sql.sysmon_pause;\n"
			"create procedure sys.resume(tag smallint)\n"
			"external name sql.sysmon_resume;\n"
			"create procedure sys.stop(tag smallint)\n"
			"external name sql.sysmon_stop;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'queue' and type = %d;\n", (int) F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name in ('pause', 'resume', 'stop') and type = %d;\n", (int) F_PROC);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'sys')"
			" and name = 'queue';\n");

	/* 51_sys_schema_extensions */
	pos += snprintf(buf + pos, bufsize - pos,
			"ALTER TABLE sys.keywords SET READ WRITE;\n"
			"DELETE FROM sys.keywords where \"keyword\" IN ('NOCYCLE','NOMAXVALUE','NOMINVALUE');\n"
			"insert into sys.keywords values ('ANALYZE'),('AT'),('AUTHORIZATION'),('CACHE'),('CENTURY'),('COLUMN'),('CLIENT'),"
			"('CUBE'),('CYCLE'),('DATA'),('DATE'),('DEBUG'),('DECADE'),('DEALLOCATE'),('DIAGNOSTICS'),('DISTINCT'),"
			"('DOW'),('DOY'),('EXEC'),('EXECUTE'),('EXPLAIN'),('FIRST'),('FWF'),('GROUPING'),('GROUPS'),('INCREMENT'),"
			"('INTERVAL'),('KEY'),('LANGUAGE'),('LARGE'),('LAST'),('LATERAL'),('LEVEL'),('LOADER'),('MATCH'),('MATCHED'),('MAXVALUE'),"
			"('MINVALUE'),('NAME'),('NO'),('NULLS'),('OBJECT'),('OPTIONS'),('PASSWORD'),('PLAN'),('PRECISION'),('PREP'),('PREPARE'),"
			"('QUARTER'),('RELEASE'),('REPLACE'),('ROLLUP'),('SCHEMA'),('SEED'),('SERVER'),('SESSION'),('SETS'),('SIZE'),"
			"('STATEMENT'),('TABLE'),('TEMP'),('TEMPORARY'),('TEXT'),('TIME'),('TIMESTAMP'),('TRACE'),('TYPE'),('UNIONJOIN'),"
			"('WEEK'),('YEAR'),('ZONE');\n");

	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	if (err == MAL_SUCCEED) {
		pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
			       "ALTER TABLE sys.keywords SET READ ONLY;\n");
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_default(Client c, mvc *sql, const char *prev_schema, bool *systabfixed)
{
	size_t bufsize = 32768, pos = 0;
	char *err = NULL, *buf = GDKmalloc(bufsize);

	if (!*systabfixed &&
	    (err = sql_fix_system_tables(c, sql, prev_schema)) != NULL)
		return err;
	*systabfixed = true;

	if (buf == NULL)
		throw(SQL, __func__, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n");

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

#ifdef HAVE_HGE
	if (have_hge) {
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
			"create window var_pop(val HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"variancep\";\n"
			"GRANT EXECUTE ON WINDOW var_pop(HUGEINT) TO PUBLIC;\n"
			"create aggregate covar_samp(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariance\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_samp(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window covar_samp(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"covariance\";\n"
			"GRANT EXECUTE ON WINDOW covar_samp(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create aggregate covar_pop(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"aggr\".\"covariancep\";\n"
			"GRANT EXECUTE ON AGGREGATE covar_pop(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window covar_pop(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"covariancep\";\n"
			"GRANT EXECUTE ON WINDOW covar_pop(HUGEINT, HUGEINT) TO PUBLIC;\n"
			"create window corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			" external name \"sql\".\"corr\";\n"
			"GRANT EXECUTE ON WINDOW corr(HUGEINT, HUGEINT) TO PUBLIC;\n");
	}
#endif

	pos += snprintf(buf + pos, bufsize - pos,
		"create window sys.group_concat(str STRING) returns STRING\n"
		" external name \"sql\".\"str_group_concat\";\n"
		"GRANT EXECUTE ON WINDOW sys.group_concat(STRING) TO PUBLIC;\n"
		"create window sys.group_concat(str STRING, sep STRING) returns STRING\n"
		" external name \"sql\".\"str_group_concat\";\n"
		"GRANT EXECUTE ON WINDOW sys.group_concat(STRING, STRING) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where name in"
			" ('stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'covar_samp', 'covar_pop', 'corr', 'group_concat')"
			" and schema_id = (select id from sys.schemas where name = 'sys');\n");

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

	/* 81_tracer.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"CREATE SCHEMA logging;\n"
			"CREATE PROCEDURE logging.flush()\n"
			" EXTERNAL NAME logging.flush;\n"
			"CREATE PROCEDURE logging.setcomplevel(comp_id INT, lvl_id INT)\n"
			" EXTERNAL NAME logging.setcomplevel;\n"
			"CREATE PROCEDURE logging.resetcomplevel(comp_id INT)\n"
			" EXTERNAL NAME logging.resetcomplevel;\n"
			"CREATE PROCEDURE logging.setlayerlevel(layer_id INT, lvl_id INT)\n"
			" EXTERNAL NAME logging.setlayerlevel;\n"
			"CREATE PROCEDURE logging.resetlayerlevel(layer_id INT)\n"
			" EXTERNAL NAME logging.resetlayerlevel;\n"
			"CREATE PROCEDURE logging.setflushlevel(lvl_id INT)\n"
			" EXTERNAL NAME logging.setflushlevel;\n"
			"CREATE PROCEDURE logging.resetflushlevel()\n"
			" EXTERNAL NAME logging.resetflushlevel;\n"
			"CREATE PROCEDURE logging.setadapter(adapter_id INT)\n"
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
			"update sys.functions set system = true where name in"
			" ('flush', 'setcomplevel', 'resetcomplevel', 'setlayerlevel', 'resetlayerlevel', 'setflushlevel', 'resetflushlevel', 'setadapter', 'resetadapter', 'compinfo')"
			" and schema_id = (select id from sys.schemas where name = 'logging');\n"
			"update sys._tables set system = true where schema_id = (select id from sys.schemas where name = 'logging')"
			" and name = 'compinfo';\n");

	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	if (err == MAL_SUCCEED) {
		pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
			       "ALTER TABLE sys.keywords SET READ ONLY;\n");
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", prev_schema);
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", true, false, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_default_bam(Client c, mvc *m, const char *prev_schema)
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
	err = SQLstatementIntern(c, &buf, "update", 1, 0, &output);
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
		if (BATcount(b) > 0 && ((lng *) b->theap.base)[0] == 0) {
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
	err = SQLstatementIntern(c, &buf, "update", true, false, NULL);

	GDKfree(buf);
	return err;
}

int
SQLupgrades(Client c, mvc *m)
{
	sql_subtype tp;
	sql_subfunc *f;
	char *err, *prev_schema = GDKstrdup(stack_get_string(m, "current_schema"));
	sql_schema *s = mvc_bind_schema(m, "sys");
	sql_table *t;
	sql_column *col;
	bool systabfixed = false;
	int res = 0;

	if (!prev_schema) {
		TRC_CRITICAL(SQL_UPGRADES, "Allocation failure while running SQL upgrades\n");
		res = -1;
	}

#ifdef HAVE_HGE
	if (!res && have_hge) {
		sql_find_subtype(&tp, "hugeint", 0, 0);
		if (!sql_bind_func(m->sa, s, "var_pop", &tp, NULL, F_AGGR)) {
			if ((err = sql_update_hugeint(c, m, prev_schema, &systabfixed)) != NULL) {
				TRC_ERROR(SQL_UPGRADES, "%s\n", err);
				freeException(err);
				res = -1;
			}
		}
	}
#endif

	f = sql_bind_func_(m->sa, s, "env", NULL, F_UNION);
	if (!res && f && sql_privilege(m, ROLE_PUBLIC, f->func->base.id, PRIV_EXECUTE) != PRIV_EXECUTE) {
		sql_table *privs = find_sql_table(s, "privileges");
		int pub = ROLE_PUBLIC, p = PRIV_EXECUTE, zero = 0;

		table_funcs.table_insert(m->session->tr, privs, &f->func->base.id, &pub, &p, &zero, &zero);
	}

	/* If the point type exists, but the geometry type does not
	 * exist any more at the "sys" schema (i.e., the first part of
	 * the upgrade has been completed succesfully), then move on
	 * to the second part */
	if (!res && find_sql_type(s, "point") != NULL) {
		/* type sys.point exists: this is an old geom-enabled
		 * database */
		if ((err = sql_update_geom(c, m, 1, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	} else if (!res && geomsqlfix_get() != NULL) {
		/* the geom module is loaded... */
		sql_find_subtype(&tp, "clob", 0, 0);
		if (!sql_bind_func(m->sa, s, "st_wkttosql",
				   &tp, NULL, F_FUNC)) {
			/* ... but the database is not geom-enabled */
			if ((err = sql_update_geom(c, m, 0, prev_schema)) != NULL) {
				TRC_ERROR(SQL_UPGRADES, "%s\n", err);
				freeException(err);
				res = -1;
			}
		}
	}

	if (!res && mvc_bind_table(m, s, "function_languages") == NULL) {
		if ((err = sql_update_jul2017(c, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	if (!res && (err = sql_update_jul2017_sp2(c)) != NULL) {
		TRC_ERROR(SQL_UPGRADES, "%s\n", err);
		freeException(err);
		res = -1;
	}

	if (!res && (err = sql_update_jul2017_sp3(c, m, prev_schema, &systabfixed)) != NULL) {
		TRC_ERROR(SQL_UPGRADES, "%s\n", err);
		freeException(err);
		res = -1;
	}

	if (!res && (t = mvc_bind_table(m, s, "geometry_columns")) != NULL &&
	    (col = mvc_bind_column(m, t, "coord_dimension")) != NULL &&
	    strcmp(col->type.type->sqlname, "int") != 0) {
		if ((err = sql_update_mar2018_geom(c, t, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	if (!res && mvc_bind_schema(m, "wlc") == NULL &&
	    !sql_bind_func(m->sa, s, "master", NULL, NULL, F_PROC)) {
		if ((err = sql_update_mar2018(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
#ifdef HAVE_NETCDF
		if (mvc_bind_table(m, s, "netcdf_files") != NULL &&
		    (err = sql_update_mar2018_netcdf(c, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
#endif
	}

	if (!res && sql_bind_func(m->sa, s, "dependencies_functions_os_triggers", NULL, NULL, F_UNION)) {
		if ((err = sql_update_mar2018_sp1(c, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	if (!res && mvc_bind_table(m, s, "ids") != NULL) {
		/* determine if sys.ids needs to be updated (only the version of Mar2018) */
		char * qry = "select id from sys._tables where name = 'ids' and query like '% tmp.keys k join sys._tables% tmp.idxs i join sys._tables% tmp.triggers g join sys._tables% ';";
		res_table *output = NULL;
		err = SQLstatementIntern(c, &qry, "update", true, false, &output);
		if (err) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		} else {
			BAT *b = BATdescriptor(output->cols[0].b);
			if (b) {
				if (BATcount(b) > 0) {
					/* yes old view definition exists, it needs to be replaced */
					if ((err = sql_replace_Mar2018_ids_view(c, m, prev_schema)) != NULL) {
						TRC_ERROR(SQL_UPGRADES, "%s\n", err);
						freeException(err);
						res = -1;
					}
				}
				BBPunfix(b->batCacheid);
			}
		}
		if (output != NULL)
			res_tables_destroy(output);
	}

	/* temporarily use variable `err' to check existence of MAL
	 * module gsl */
	if (!res && (((err = getName("gsl")) == NULL || getModule(err) == NULL))) {
		/* no MAL module gsl, check for SQL function sys.chi2prob */
		sql_find_subtype(&tp, "double", 0, 0);
		if (sql_bind_func(m->sa, s, "chi2prob", &tp, &tp, F_FUNC)) {
			/* sys.chi2prob exists, but there is no
			 * implementation */
			if ((err = sql_update_gsl(c, prev_schema)) != NULL) {
				TRC_ERROR(SQL_UPGRADES, "%s\n", err);
				freeException(err);
				res = -1;
			}
		}
	}

	sql_find_subtype(&tp, "clob", 0, 0);
	if (!res && sql_bind_func(m->sa, s, "group_concat", &tp, NULL, F_AGGR) == NULL) {
		if ((err = sql_update_aug2018(c, m, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	if (!res && sql_bind_func(m->sa, s, "dependencies_schemas_on_users", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_owners_on_schemas", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_tables_on_views", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_tables_on_indexes", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_tables_on_triggers", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_tables_on_foreignkeys", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_tables_on_functions", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_columns_on_views", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_columns_on_keys", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_columns_on_indexes", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_columns_on_functions", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_columns_on_triggers", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_views_on_functions", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_views_on_triggers", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_functions_on_functions", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_functions_on_triggers", NULL, NULL, F_UNION)
	 && sql_bind_func(m->sa, s, "dependencies_keys_on_foreignkeys", NULL, NULL, F_UNION)	) {
		if ((err = sql_drop_functions_dependencies_Xs_on_Ys(c, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	if (!res && (err = sql_update_aug2018_sp2(c, prev_schema)) != NULL) {
		TRC_ERROR(SQL_UPGRADES, "%s\n", err);
		freeException(err);
		res = -1;
	}

	if (!res && (t = mvc_bind_table(m, s, "systemfunctions")) != NULL &&
	    t->type == tt_table) {
		if (!systabfixed &&
		    (err = sql_fix_system_tables(c, m, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
		systabfixed = true;
		if ((err = sql_update_apr2019(c, m, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	/* when function storagemodel() exists and views tablestorage
	 * and schemastorage don't, then upgrade storagemodel to match
	 * 75_storagemodel.sql */
	if (!res && sql_bind_func(m->sa, s, "storagemodel", NULL, NULL, F_UNION)
	 && (t = mvc_bind_table(m, s, "tablestorage")) == NULL
	 && (t = mvc_bind_table(m, s, "schemastorage")) == NULL ) {
		if ((err = sql_update_storagemodel(c, m, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	if (!res && (err = sql_update_apr2019_sp1(c)) != NULL) {
		TRC_ERROR(SQL_UPGRADES, "%s\n", err);
		freeException(err);
		res = -1;
	}

	if (!res && sql_bind_func(m->sa, s, "times", NULL, NULL, F_PROC)) {
		if (!res && (err = sql_update_apr2019_sp2(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	sql_find_subtype(&tp, "string", 0, 0);
	if (!res && !sql_bind_func3(m->sa, s, "deltas", &tp, &tp, &tp, F_UNION)) {
		if ((err = sql_update_nov2019_missing_dependencies(c, m)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
		if (!systabfixed &&
		    (err = sql_fix_system_tables(c, m, prev_schema)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
		systabfixed = true;
		if ((err = sql_update_nov2019(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

#ifdef HAVE_HGE
	if (!res && have_hge) {
		sql_find_subtype(&tp, "hugeint", 0, 0);
		if (!sql_bind_func(m->sa, s, "median_avg", &tp, NULL, F_AGGR)) {
			if ((err = sql_update_nov2019_sp1_hugeint(c, m, prev_schema, &systabfixed)) != NULL) {
				TRC_ERROR(SQL_UPGRADES, "%s\n", err);
				freeException(err);
				res = -1;
			}
		}
	}
#endif

	if (!res && !sql_bind_func(m->sa, s, "suspend_log_flushing", NULL, NULL, F_PROC)) {
		if ((err = sql_update_linear_hashing(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	sql_find_subtype(&tp, "tinyint", 0, 0);
	if (!sql_bind_func(m->sa, s, "stddev_samp", &tp, NULL, F_ANALYTIC)) {
		if ((err = sql_update_default(c, m, prev_schema, &systabfixed)) != NULL) {
			TRC_ERROR(SQL_UPGRADES, "%s\n", err);
			freeException(err);
			res = -1;
		}
	}

	if (!res &&
	    (err = sql_update_default_bam(c, m, prev_schema)) != NULL) {
		TRC_ERROR(SQL_UPGRADES, "%s\n", err);
		freeException(err);
		res = -1;
	}

	GDKfree(prev_schema);
	return res;
}
