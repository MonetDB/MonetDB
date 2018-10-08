/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

#include "rel_remote.h"
#include "mal_authorize.h"

#ifdef HAVE_EMBEDDED
#define printf(fmt,...) ((void) 0)
#endif

/* this function can be used to recreate the system tables (types,
 * functions, args) when internal types and/or functions have changed
 * (i.e. the ones in sql_types.c) */
static str
sql_fix_system_tables(Client c, mvc *sql)
{
	size_t bufsize = 1000000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");
	node *n;
	sql_schema *s;

	if (buf == NULL)
		throw(SQL, "sql_fix_system_tables", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	s = mvc_bind_schema(sql, "sys");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"delete from sys.dependencies where id < 2000;\n");

	/* recreate internal types */
	pos += snprintf(buf + pos, bufsize - pos,
			"delete from sys.types where id < 2000;\n");
	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;

		if (t->base.id >= 2000)
			continue;

		pos += snprintf(buf + pos, bufsize - pos,
				"insert into sys.types values"
				" (%d, '%s', '%s', %u, %u, %d, %d, %d);\n",
				t->base.id, t->base.name, t->sqlname, t->digits,
				t->scale, t->radix, t->eclass,
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

		if (func->base.id >= 2000)
			continue;

		pos += snprintf(buf + pos, bufsize - pos,
				"insert into sys.functions values"
				" (%d, '%s', '%s', '%s',"
				" %d, %d, %s, %s, %s, %d, %s);\n",
				func->base.id, func->base.name,
				func->imp, func->mod, FUNC_LANG_INT,
				func->type,
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
	for (n = aggrs->h; n; n = n->next) {
		sql_func *aggr = n->data;
		sql_arg *arg;

		if (aggr->base.id >= 2000)
			continue;

		pos += snprintf(buf + pos, bufsize - pos,
				"insert into sys.functions values"
				" (%d, '%s', '%s', '%s', %d, %d, false,"
				" %s, %s, %d, %s);\n",
				aggr->base.id, aggr->base.name, aggr->imp,
				aggr->mod, FUNC_LANG_INT, aggr->type,
				aggr->varres ? "true" : "false",
				aggr->vararg ? "true" : "false",
				aggr->s ? aggr->s->base.id : s->base.id,
				aggr->system ? "true" : "false");
		arg = aggr->res->h->data;
		pos += snprintf(buf + pos, bufsize - pos,
				"insert into sys.args values"
				" (%d, %d, 'res', '%s', %u, %u, %d, 0);\n",
				store_next_oid(), aggr->base.id,
				arg->type.type->sqlname, arg->type.digits,
				arg->type.scale, arg->inout);
		if (aggr->ops->h) {
			arg = aggr->ops->h->data;
			pos += snprintf(buf + pos, bufsize - pos,
					"insert into sys.args values"
					" (%d, %d, 'arg', '%s', %u,"
					" %u, %d, 1);\n",
					store_next_oid(), aggr->base.id,
					arg->type.type->sqlname,
					arg->type.digits, arg->type.scale,
					arg->inout);
		}
	}

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#ifdef HAVE_HGE
static str
sql_update_hugeint(Client c, mvc *sql)
{
	size_t bufsize = 8192, pos = 0;
	char *buf, *err;
	char *schema;
	sql_schema *s;

	if ((err = sql_fix_system_tables(c, sql)) != NULL)
		return err;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, "sql_update_hugeint", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	schema = stack_get_string(sql, "current_schema");

	s = mvc_bind_schema(sql, "sys");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function fuse(one bigint, two bigint)\n"
			"returns hugeint\n"
			"external name udf.fuse;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.generate_series(first hugeint, last hugeint)\n"
			"returns table (value hugeint)\n"
			"external name generator.series;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.generate_series(first hugeint, last hugeint, stepsize hugeint)\n"
			"returns table (value hugeint)\n"
			"external name generator.series;\n");

	/* 39_analytics_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate stddev_samp(val HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"stdev\";\n"
			"create aggregate stddev_pop(val HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"stdevp\";\n"
			"create aggregate var_samp(val HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"variance\";\n"
			"create aggregate var_pop(val HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"variancep\";\n"
			"create aggregate median(val HUGEINT) returns HUGEINT\n"
			"    external name \"aggr\".\"median\";\n"
			"create aggregate quantile(val HUGEINT, q DOUBLE) returns HUGEINT\n"
			"    external name \"aggr\".\"quantile\";\n"
			"create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"corr\";\n");

	/* 40_json_hge.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function json.filter(js json, name hugeint)\n"
			"returns json\n"
			"external name json.filter;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tablestoragemodel;\n"
			"create view sys.tablestoragemodel\n"
			"as select \"schema\",\"table\",max(count) as \"count\",\n"
			"  sum(columnsize) as columnsize,\n"
			"  sum(heapsize) as heapsize,\n"
			"  sum(hashes) as hashes,\n"
			"  sum(\"imprints\") as \"imprints\",\n"
			"  sum(case when sorted = false then 8 * count else 0 end) as auxiliary\n"
			"from sys.storagemodel() group by \"schema\",\"table\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where name in ('fuse', 'generate_series', 'stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'median', 'quantile', 'corr') and schema_id = (select id from sys.schemas where name = 'sys');\n"
			"update sys.functions set system = true where name = 'filter' and schema_id = (select id from sys.schemas where name = 'json');\n"
			"update sys._tables set system = true where name = 'tablestoragemodel' and schema_id = (select id from sys.schemas where name = 'sys');\n");

	if (s != NULL) {
		sql_table *t;

		if ((t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
			t->system = 0;
	}

	pos += snprintf(buf + pos, bufsize - pos,
			"grant execute on aggregate sys.stddev_samp(hugeint) to public;\n"
			"grant execute on aggregate sys.stddev_pop(hugeint) to public;\n"
			"grant execute on aggregate sys.var_samp(hugeint) to public;\n"
			"grant execute on aggregate sys.var_pop(hugeint) to public;\n"
			"grant execute on aggregate sys.median(hugeint) to public;\n"
			"grant execute on aggregate sys.quantile(hugeint, double) to public;\n"
			"grant execute on aggregate sys.corr(hugeint, hugeint) to public;\n"
			"grant execute on function json.filter(json, hugeint) to public;\n");

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_update_geom(Client c, mvc *sql, int olddb)
{
	size_t bufsize, pos = 0;
	char *buf, *err = NULL;
	char *geomupgrade;
	char *schema = stack_get_string(sql, "current_schema");
	geomsqlfix_fptr fixfunc;
	node *n;
	sql_schema *s = mvc_bind_schema(sql, "sys");

	if ((fixfunc = geomsqlfix_get()) == NULL)
		return NULL;

	geomupgrade = (*fixfunc)(olddb);
	if (geomupgrade == NULL)
		throw(SQL, "sql_update_geom", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	bufsize = strlen(geomupgrade) + 512;
	buf = GDKmalloc(bufsize);
	if (buf == NULL) {
		GDKfree(geomupgrade);
		throw(SQL, "sql_update_geom", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");
	pos += snprintf(buf + pos, bufsize - pos, "%s", geomupgrade);
	GDKfree(geomupgrade);

	pos += snprintf(buf + pos, bufsize - pos, "delete from sys.types where systemname in ('mbr', 'wkb', 'wkba');\n");
	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;

		if (t->base.id < 2000 &&
		    (strcmp(t->base.name, "mbr") == 0 ||
		     strcmp(t->base.name, "wkb") == 0 ||
		     strcmp(t->base.name, "wkba") == 0))
			pos += snprintf(buf + pos, bufsize - pos, "insert into sys.types values (%d, '%s', '%s', %u, %u, %d, %d, %d);\n", t->base.id, t->base.name, t->sqlname, t->digits, t->scale, t->radix, t->eclass, t->s ? t->s->base.id : s->base.id);
	}

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_dec2016(Client c, mvc *sql)
{
	size_t bufsize = 10240, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");
	sql_schema *s;

	if (buf == NULL)
		throw(SQL, "sql_update_dec2016", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	s = mvc_bind_schema(sql, "sys");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	{
		sql_table *t;

		if ((t = mvc_bind_table(sql, s, "storagemodel")) != NULL)
			t->system = 0;
		if ((t = mvc_bind_table(sql, s, "storagemodelinput")) != NULL)
			t->system = 0;
		if ((t = mvc_bind_table(sql, s, "storage")) != NULL)
			t->system = 0;
		if ((t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
			t->system = 0;
	}

	/* 18_index.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create procedure sys.createorderindex(sys string, tab string, col string)\n"
			"external name sql.createorderindex;\n"
			"create procedure sys.droporderindex(sys string, tab string, col string)\n"
			"external name sql.droporderindex;\n");

	/* 24_zorder.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function sys.zorder_decode_y;\n"
			"drop function sys.zorder_decode_x;\n"
			"drop function sys.zorder_encode;\n");

	/* 75_storagemodel.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tablestoragemodel;\n"
			"drop view sys.storagemodel;\n"
			"drop function sys.storagemodel();\n"
			"drop procedure sys.storagemodelinit();\n"
			"drop function sys.\"storage\"(string, string, string);\n"
			"drop function sys.\"storage\"(string, string);\n"
			"drop function sys.\"storage\"(string);\n"
			"drop view sys.\"storage\";\n"
			"drop function sys.\"storage\"();\n"
			"alter table sys.storagemodelinput add column \"revsorted\" boolean;\n"
			"alter table sys.storagemodelinput add column \"unique\" boolean;\n"
			"alter table sys.storagemodelinput add column \"orderidx\" bigint;\n"
			"create function sys.\"storage\"()\n"
			"returns table (\n"
			" \"schema\" string,\n"
			" \"table\" string,\n"
			" \"column\" string,\n"
			" \"type\" string,\n"
			" \"mode\" string,\n"
			" location string,\n"
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
			"create view sys.\"storage\" as select * from sys.\"storage\"();\n"
			"create function sys.\"storage\"( sname string)\n"
			"returns table (\n"
			" \"schema\" string,\n"
			" \"table\" string,\n"
			" \"column\" string,\n"
			" \"type\" string,\n"
			" \"mode\" string,\n"
			" location string,\n"
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
			"create function sys.\"storage\"( sname string, tname string)\n"
			"returns table (\n"
			" \"schema\" string,\n"
			" \"table\" string,\n"
			" \"column\" string,\n"
			" \"type\" string,\n"
			" \"mode\" string,\n"
			" location string,\n"
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
			"create function sys.\"storage\"( sname string, tname string, cname string)\n"
			"returns table (\n"
			" \"schema\" string,\n"
			" \"table\" string,\n"
			" \"column\" string,\n"
			" \"type\" string,\n"
			" \"mode\" string,\n"
			" location string,\n"
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
			"create procedure sys.storagemodelinit()\n"
			"begin\n"
			" delete from sys.storagemodelinput;\n"
			" insert into sys.storagemodelinput\n"
			" select X.\"schema\", X.\"table\", X.\"column\", X.\"type\", X.typewidth, X.count, 0, X.typewidth, false, X.sorted, X.revsorted, X.\"unique\", X.orderidx from sys.\"storage\"() X;\n"
			" update sys.storagemodelinput\n"
			" set reference = true\n"
			" where concat(concat(\"schema\",\"table\"), \"column\") in (\n"
			"  SELECT concat( concat(\"fkschema\".\"name\", \"fktable\".\"name\"), \"fkkeycol\".\"name\" )\n"
			"  FROM \"sys\".\"keys\" AS    \"fkkey\",\n"
			"    \"sys\".\"objects\" AS \"fkkeycol\",\n"
			"    \"sys\".\"tables\" AS  \"fktable\",\n"
			"    \"sys\".\"schemas\" AS \"fkschema\"\n"
			"  WHERE   \"fktable\".\"id\" = \"fkkey\".\"table_id\"\n"
			"   AND \"fkkey\".\"id\" = \"fkkeycol\".\"id\"\n"
			"   AND \"fkschema\".\"id\" = \"fktable\".\"schema_id\"\n"
			"   AND \"fkkey\".\"rkey\" > -1);\n"
			" update sys.storagemodelinput\n"
			" set \"distinct\" = \"count\"\n"
			" where \"type\" = 'varchar' or \"type\"='clob';\n"
			"end;\n"
			"create function sys.storagemodel()\n"
			"returns table (\n"
			" \"schema\" string,\n"
			" \"table\" string,\n"
			" \"column\" string,\n"
			" \"type\" string,\n"
			" \"count\" bigint,\n"
			" columnsize bigint,\n"
			" heapsize bigint,\n"
			" hashes bigint,\n"
			" \"imprints\" bigint,\n"
			" sorted boolean,\n"
			" revsorted boolean,\n"
			" \"unique\" boolean,\n"
			" orderidx bigint)\n"
			"begin\n"
			" return select I.\"schema\", I.\"table\", I.\"column\", I.\"type\", I.\"count\",\n"
			" columnsize(I.\"type\", I.count, I.\"distinct\"),\n"
			" heapsize(I.\"type\", I.\"distinct\", I.\"atomwidth\"),\n"
			" hashsize(I.\"reference\", I.\"count\"),\n"
			" imprintsize(I.\"count\",I.\"type\"),\n"
			" I.sorted, I.revsorted, I.\"unique\", I.orderidx\n"
			" from sys.storagemodelinput I;\n"
			"end;\n"
			"create view sys.storagemodel as select * from sys.storagemodel();\n"
			"create view sys.tablestoragemodel\n"
			"as select \"schema\",\"table\",max(count) as \"count\",\n"
			" sum(columnsize) as columnsize,\n"
			" sum(heapsize) as heapsize,\n"
			" sum(hashes) as hashes,\n"
			" sum(\"imprints\") as \"imprints\",\n"
			" sum(case when sorted = false then 8 * count else 0 end) as auxiliary\n"
			"from sys.storagemodel() group by \"schema\",\"table\";\n"
			"update sys._tables set system = true where name in ('storage', 'storagemodel', 'tablestoragemodel') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* 80_statistics.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"alter table sys.statistics add column \"revsorted\" boolean;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where name in ('storage', 'storagemodel') and type = %d and schema_id = (select id from sys.schemas where name = 'sys');\n",
			F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where name in ('createorderindex', 'droporderindex', 'storagemodelinit') and type = %d and schema_id = (select id from sys.schemas where name = 'sys');\n",
			F_PROC);

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_dec2016_sp2(Client c, mvc *sql)
{
	size_t bufsize = 2048, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");
	res_table *output;
	BAT *b;

	if (buf == NULL)
		throw(SQL, "sql_update_dec2016_sp2", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	pos += snprintf(buf + pos, bufsize - pos, "select id from sys.types where sqlname = 'decimal' and digits = %d;\n",
#ifdef HAVE_HGE
			have_hge ? 39 :
#endif
			19);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) > 0) {
			pos = 0;
			pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

#ifdef HAVE_HGE
			if (have_hge) {
				pos += snprintf(buf + pos, bufsize - pos,
						"update sys.types set digits = 38 where sqlname = 'decimal' and digits = 39;\n"
						"update sys.args set type_digits = 38 where type = 'decimal' and type_digits = 39;\n");
			} else
#endif
				pos += snprintf(buf + pos, bufsize - pos,
						"update sys.types set digits = 18 where sqlname = 'decimal' and digits = 19;\n"
						"update sys.args set type_digits = 18 where type = 'decimal' and type_digits = 19;\n");

			if (schema)
				pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
			pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
		}
		BBPunfix(b->batCacheid);
	}
	res_tables_destroy(output);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_dec2016_sp3(Client c, mvc *sql)
{
	size_t bufsize = 2048, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	if (buf == NULL)
		throw(SQL, "sql_update_dec2016_sp3", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"drop procedure sys.settimeout(bigint);\n"
			"drop procedure sys.settimeout(bigint,bigint);\n"
			"drop procedure sys.setsession(bigint);\n"
			"create procedure sys.settimeout(\"query\" bigint) external name clients.settimeout;\n"
			"create procedure sys.settimeout(\"query\" bigint, \"session\" bigint) external name clients.settimeout;\n"
			"create procedure sys.setsession(\"timeout\" bigint) external name clients.setsession;\n"
			"update sys.functions set system = true where name in ('settimeout', 'setsession') and schema_id = (select id from sys.schemas where name = 'sys');\n");
	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jul2017(Client c, mvc *sql)
{
	size_t bufsize = 10000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");
	char *q1 = "select id from sys.functions where name = 'shpload' and schema_id = (select id from sys.schemas where name = 'sys');\n";
	res_table *output;
	BAT *b;

	if( buf == NULL)
		throw(SQL, "sql_update_jul2017", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	err = SQLstatementIntern(c, &q1, "update", 1, 0, &output);
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

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
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

	err = SQLstatementIntern(c, &qry, "update", 1, 0, &output);
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
				throw(SQL, "sql_update_jul2017_sp2", SQLSTATE(HY001) MAL_MALLOC_FAIL);

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
			pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
			assert(pos < bufsize);
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
			GDKfree(buf);
		}
		BBPunfix(b->batCacheid);
	}
	res_tables_destroy(output);

	return err;		/* usually NULL */
}

static str
sql_update_jul2017_sp3(Client c, mvc *sql)
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
	if (is_oid_nil(rid)) {
		err = sql_fix_system_tables(c, sql);
		if (err != NULL)
			return err;
	}
	/* if there is no value "system_update_schemas" in
	 * sys.triggers.name, we need to add the triggers */
	tab = find_sql_table(sys, "triggers");
	col = find_sql_column(tab, "name");
	rid = table_funcs.column_find_row(sql->session->tr, col, "system_update_schemas", NULL);
	if (is_oid_nil(rid)) {
		char *schema = stack_get_string(sql, "current_schema");
		size_t bufsize = 1024, pos = 0;
		char *buf = GDKmalloc(bufsize);
		if (buf == NULL)
			throw(SQL, "sql_update_jul2017_sp3", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		pos += snprintf(
			buf + pos,
			bufsize - pos,
			"set schema \"sys\";\n"
			"create trigger system_update_schemas after update on sys.schemas for each statement call sys_update_schemas();\n"
			"create trigger system_update_tables after update on sys._tables for each statement call sys_update_tables();\n");
		if (schema)
			pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
		assert(pos < bufsize);
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
		GDKfree(buf);
	}
	return err;
}

static str
sql_update_mar2018_geom(Client c, mvc *sql, sql_table *t)
{
	size_t bufsize = 10000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	if (buf == NULL)
		throw(SQL, "sql_update_mar2018_geom", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_mar2018(Client c, mvc *sql)
{
	size_t bufsize = 30000, pos = 0;
	char *buf, *err;
	char *schema;
	sql_schema *s;
	sql_table *t;
	res_table *output;
	BAT *b;

	buf = "select id from sys.functions where name = 'quarter' and schema_id = (select id from sys.schemas where name = 'sys');\n";
	err = SQLstatementIntern(c, &buf, "update", 1, 0, &output);
	if (err)
		return err;
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) == 0) {
			/* if there is no value "quarter" in
			 * sys.functions.name, we need to update the
			 * sys.functions table */
			err = sql_fix_system_tables(c, sql);
			if (err != NULL)
				return err;
		}
		BBPunfix(b->batCacheid);
	}
	res_tables_destroy(output);

	schema = stack_get_string(sql, "current_schema");
	buf = GDKmalloc(bufsize);
	if (buf == NULL)
		throw(SQL, "sql_update_mar2018", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			"SELECT 'history', history UNION ALL\n"
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

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	if (err == MAL_SUCCEED) {
		schema = stack_get_string(sql, "current_schema");
		pos = snprintf(buf, bufsize, "set schema \"sys\";\n"
			       "ALTER TABLE sys.keywords SET READ ONLY;\n"
			       "ALTER TABLE sys.function_types SET READ ONLY;\n"
			       "ALTER TABLE sys.function_languages SET READ ONLY;\n");
		if (schema)
			pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#ifdef HAVE_NETCDF
static str
sql_update_mar2018_netcdf(Client c, mvc *sql)
{
	size_t bufsize = 1000, pos = 0;
	char *buf, *err;
	char *schema;

	schema = stack_get_string(sql, "current_schema");
	buf = GDKmalloc(bufsize);
	if (buf == NULL)
		throw(SQL, "sql_update_mar2018_netcdf", SQLSTATE(HY001) MAL_MALLOC_FAIL);

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

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif	/* HAVE_NETCDF */

#ifdef HAVE_SAMTOOLS
static str
sql_update_mar2018_samtools(Client c, mvc *sql)
{
	size_t bufsize = 2000, pos = 0;
	char *buf, *err;
	char *schema;
	sql_schema *s = mvc_bind_schema(sql, "bam");

	if (s == NULL)
		return MAL_SUCCEED;

	schema = stack_get_string(sql, "current_schema");
	buf = GDKmalloc(bufsize);
	if (buf == NULL)
		throw(SQL, "sql_update_mar2018_samtools", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");

	/* 85_bam.sql */
	list *l = sa_list(sql->sa);
	sql_subtype tpi, tps;
	sql_find_subtype(&tpi, "int", 0, 0);
	sql_find_subtype(&tps, "clob", 0, 0);
	list_append(l, &tpi);
	list_append(l, &tps);
	list_append(l, &tpi);
	list_append(l, &tps);
	if (sql_bind_func_(sql->sa, s, "seq_char", l, F_FUNC) == NULL) {
		pos += snprintf(buf + pos, bufsize - pos,
				"CREATE FUNCTION bam.seq_char(ref_pos INT, alg_seq STRING, alg_pos INT, alg_cigar STRING)\n"
				"RETURNS CHAR(1) EXTERNAL NAME bam.seq_char;\n"
				"update sys.functions set system = true where name in ('seq_char') and schema_id = (select id from sys.schemas where name = 'bam');\n");
	}
	sql_find_subtype(&tpi, "smallint", 0, 0);
	if (sql_bind_func3(sql->sa, s, "bam_loader_repos", &tps, &tpi, &tpi, F_PROC) != NULL) {
		pos += snprintf(buf + pos, bufsize - pos,
				"drop procedure bam.bam_loader_repos(string, smallint, smallint);\n"
				"drop procedure bam.bam_loader_files(string, smallint, smallint);\n");
	}
	if (sql_bind_func(sql->sa, s, "bam_loader_repos", &tps, &tpi, F_PROC) == NULL) {
		pos += snprintf(buf + pos, bufsize - pos,
				"CREATE PROCEDURE bam.bam_loader_repos(bam_repos STRING, dbschema SMALLINT)\n"
				"EXTERNAL NAME bam.bam_loader_repos;\n"
				"CREATE PROCEDURE bam.bam_loader_files(bam_files STRING, dbschema SMALLINT)\n"
				"EXTERNAL NAME bam.bam_loader_files;\n"
				"update sys.functions set system = true where name in ('bam_loader_repos', 'bam_loader_files') and schema_id = (select id from sys.schemas where name = 'bam');\n");
	}

	pos += snprintf(buf + pos, bufsize - pos,
			"GRANT SELECT ON bam.files TO PUBLIC;\n"
			"GRANT SELECT ON bam.sq TO PUBLIC;\n"
			"GRANT SELECT ON bam.rg TO PUBLIC;\n"
			"GRANT SELECT ON bam.pg TO PUBLIC;\n"
			"GRANT SELECT ON bam.export TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION bam.bam_flag(SMALLINT, STRING) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION bam.reverse_seq(STRING) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION bam.reverse_qual(STRING) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION bam.seq_length(STRING) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION bam.seq_char(INT, STRING, INT, STRING) TO PUBLIC;\n"
			"GRANT EXECUTE ON PROCEDURE bam.bam_loader_repos(STRING, SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON PROCEDURE bam.bam_loader_files(STRING, SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON PROCEDURE bam.bam_loader_file(STRING, SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON PROCEDURE bam.bam_drop_file(BIGINT, SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON PROCEDURE bam.sam_export(STRING) TO PUBLIC;\n"
			"GRANT EXECUTE ON PROCEDURE bam.bam_export(STRING) TO PUBLIC;\n");

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif	/* HAVE_SAMTOOLS */

static str
sql_update_mar2018_sp1(Client c, mvc *sql)
{
	size_t bufsize = 2048, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	if (buf == NULL)
		throw(SQL, "sql_update_mar2018_sp1", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"drop function sys.dependencies_functions_os_triggers();\n"
			"CREATE FUNCTION dependencies_functions_on_triggers()\n"
			"RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))\n"
			"RETURN TABLE (SELECT f.name, tri.name, 'DEP_TRIGGER' from functions as f, triggers as tri, dependencies as dep where dep.id = f.id AND dep.depend_id =tri.id AND dep.depend_type = 8);\n"
			"update sys.functions set system = true where name in ('dependencies_functions_on_triggers') and schema_id = (select id from sys.schemas where name = 'sys');\n");
	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_remote_tables(Client c, mvc *sql)
{
	res_table *output;
	str err = MAL_SUCCEED;
	size_t bufsize = 1000, pos = 0;
	char *buf;
	char *schema;
	BAT *tbl = NULL;
	BAT *uri = NULL;

	schema = stack_get_string(sql, "current_schema");
	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, "sql_update_remote_tables", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	/* Create the SQL function needed to dump the remote table credentials */
	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.remote_table_credentials (tablename string)"
			" returns table (\"uri\" string, \"username\" string, \"hash\" string)"
			" external name sql.rt_credentials;\n"
			"update sys.functions set system = true where name = 'remote_table_credentials' and schema_id = (select id from sys.schemas where name = 'sys');\n");

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "create function", 1, 0, NULL);
	if (err)
		goto bailout;

	pos = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"SELECT concat(concat(scm.name, '.'), tbl.name), tbl.query"
			" FROM sys._tables AS tbl JOIN sys.schemas AS scm ON"
			" tbl.schema_id=scm.id WHERE tbl.type=5;\n");

	assert(pos < bufsize);

	err = SQLstatementIntern(c, &buf, "get remote table names", 1, 0, &output);
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
				v = BUNtail(tbl_it, i);
				u = BUNtail(uri_it, i);
				if (v == NULL || (*cmp)(v, nil) == 0 ||
				    u == NULL || (*cmp)(u, nil) == 0) {
					BBPunfix(tbl->batCacheid);
					BBPunfix(uri->batCacheid);
					goto bailout;
				}

				/* Since the loop might fail, it might be a good idea
				 * to update the credentials as a second step
				 */
				remote_server_uri = mapiuri_uri((char *)u, sql->sa);
				AUTHaddRemoteTableCredentials((char *)v, "monetdb", remote_server_uri, "monetdb", "monetdb", false);
			}
		}
		BBPunfix(tbl->batCacheid);
		BBPunfix(uri->batCacheid);
	}
	res_table_destroy(output);

  bailout:
	GDKfree(buf);
	return err;
}

static str
sql_replace_Mar2018_ids_view(Client c, mvc *sql)
{
	size_t bufsize = 4400, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema;
	sql_schema *s;
	sql_table *t;

	if (buf == NULL)
		throw(SQL, "sql_replace_Mar2018_ids_view", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	schema = stack_get_string(sql, "current_schema");
	s = mvc_bind_schema(sql, "sys");
	t = mvc_bind_table(sql, s, "ids");
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

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_gsl(Client c, mvc *sql)
{
	size_t bufsize = 1024, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	if (buf == NULL)
		throw(SQL, "sql_update_gsl", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"drop function sys.chi2prob(double, double);\n");
	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_aug2018(Client c, mvc *sql)
{
	size_t bufsize = 1000, pos = 0;
	char *buf, *err;
	char *schema;

	schema = stack_get_string(sql, "current_schema");
	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, "sql_update_aug2018", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.group_concat(str string) returns string external name \"aggr\".\"str_group_concat\";\n"
			"grant execute on aggregate sys.group_concat(string) to public;\n"
			"create aggregate sys.group_concat(str string, sep string) returns string external name \"aggr\".\"str_group_concat\";\n"
			"grant execute on aggregate sys.group_concat(string, string) to public;\n"
			"update sys.functions set system = true where name in ('group_concat') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	if (err)
		goto bailout;
	err = sql_update_remote_tables(c, sql);

  bailout:
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_default(Client c, mvc *sql)
{
	size_t bufsize = 2000, pos = 0;
	char *buf, *err;
	char *schema;
	sql_schema *s;
	sql_table *t;

	schema = stack_get_string(sql, "current_schema");
	if ((buf = GDKmalloc(bufsize)) == NULL)
		throw(SQL, "sql_update_default", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	s = mvc_bind_schema(sql, "sys");

	pos += snprintf(buf + pos, bufsize - pos, "set schema sys;\n");

	/* 99_system.sql */
	t = mvc_bind_table(sql, s, "systemfunctions");
	t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"drop table sys.systemfunctions;\n"
			"create view sys.systemfunctions as select id as function_id from sys.functions where system;\n"
			"grant select on sys.systemfunctions to public;\n"
			"update sys._tables set system = true where name = 'systemfunctions' and schema_id = (select id from sys.schemas where name = 'sys');\n");

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);

	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_drop_functions_dependencies_Xs_on_Ys(Client c, mvc *sql)
{
	size_t bufsize = 1600, pos = 0;
	char *schema = NULL, *err = NULL;
	char *buf = GDKmalloc(bufsize);

	if (buf == NULL)
		throw(SQL, "sql_drop_functions_dependencies_Xs_on_Ys", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	schema = stack_get_string(sql, "current_schema");
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
	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	pos += snprintf(buf + pos, bufsize - pos, "commit;\n");
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

void
SQLupgrades(Client c, mvc *m)
{
	sql_subtype tp;
	sql_subfunc *f;
	char *err;
	sql_schema *s = mvc_bind_schema(m, "sys");
	sql_table *t;
	sql_column *col;

#ifdef HAVE_HGE
	if (have_hge) {
		sql_find_subtype(&tp, "hugeint", 0, 0);
		if (!sql_bind_aggr(m->sa, s, "var_pop", &tp)) {
			if ((err = sql_update_hugeint(c, m)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				freeException(err);
			}
		}
	}
#endif

	f = sql_bind_func_(m->sa, s, "env", NULL, F_UNION);
	if (f && sql_privilege(m, ROLE_PUBLIC, f->func->base.id, PRIV_EXECUTE, 0) != PRIV_EXECUTE) {
		sql_table *privs = find_sql_table(s, "privileges");
		int pub = ROLE_PUBLIC, p = PRIV_EXECUTE, zero = 0;

		table_funcs.table_insert(m->session->tr, privs, &f->func->base.id, &pub, &p, &zero, &zero);
	}

	/* If the point type exists, but the geometry type does not
	 * exist any more at the "sys" schema (i.e., the first part of
	 * the upgrade has been completed succesfully), then move on
	 * to the second part */
	if (find_sql_type(s, "point") != NULL) {
		/* type sys.point exists: this is an old geom-enabled
		 * database */
		if ((err = sql_update_geom(c, m, 1)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	} else if (geomsqlfix_get() != NULL) {
		/* the geom module is loaded... */
		sql_find_subtype(&tp, "clob", 0, 0);
		if (!sql_bind_func(m->sa, s, "st_wkttosql",
				   &tp, NULL, F_FUNC)) {
			/* ... but the database is not geom-enabled */
			if ((err = sql_update_geom(c, m, 0)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				freeException(err);
			}
		}
	}

	sql_find_subtype(&tp, "clob", 0, 0);
	if (!sql_bind_func3(m->sa, s, "createorderindex", &tp, &tp, &tp, F_PROC)) {
		if ((err = sql_update_dec2016(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	if ((err = sql_update_dec2016_sp2(c, m)) != NULL) {
		fprintf(stderr, "!%s\n", err);
		freeException(err);
	}

	sql_find_subtype(&tp, "bigint", 0, 0);
	if ((f = sql_bind_func(m->sa, s, "settimeout", &tp, NULL, F_PROC)) != NULL &&
	     /* The settimeout function used to be in the sql module */
	     f->func->sql && f->func->query && strstr(f->func->query, "sql") != NULL) {
		if ((err = sql_update_dec2016_sp3(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	if (mvc_bind_table(m, s, "function_languages") == NULL) {
		if ((err = sql_update_jul2017(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	if ((err = sql_update_jul2017_sp2(c)) != NULL) {
		fprintf(stderr, "!%s\n", err);
		freeException(err);
	}

	if ((err = sql_update_jul2017_sp3(c, m)) != NULL) {
		fprintf(stderr, "!%s\n", err);
		freeException(err);
	}

	if ((t = mvc_bind_table(m, s, "geometry_columns")) != NULL &&
	    (col = mvc_bind_column(m, t, "coord_dimension")) != NULL &&
	    strcmp(col->type.type->sqlname, "int") != 0) {
		if ((err = sql_update_mar2018_geom(c, m, t)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	if (!sql_bind_func(m->sa, s, "master", NULL, NULL, F_PROC)) {
		if ((err = sql_update_mar2018(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
#ifdef HAVE_NETCDF
		if (mvc_bind_table(m, s, "netcdf_files") != NULL &&
		    (err = sql_update_mar2018_netcdf(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
#endif
#ifdef HAVE_SAMTOOLS
		if ((err = sql_update_mar2018_samtools(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
#endif
	}

	if (sql_bind_func(m->sa, s, "dependencies_functions_os_triggers", NULL, NULL, F_UNION)) {
		if ((err = sql_update_mar2018_sp1(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	if (mvc_bind_table(m, s, "ids") != NULL) {
		/* determine if sys.ids needs to be updated (only the version of Mar2018) */
		char * qry = "select id from sys._tables where name = 'ids' and query like '% tmp.keys k join sys._tables% tmp.idxs i join sys._tables% tmp.triggers g join sys._tables% ';";
		res_table *output = NULL;
		err = SQLstatementIntern(c, &qry, "update", 1, 0, &output);
		if (err) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		} else {
			BAT *b = BATdescriptor(output->cols[0].b);
			if (b) {
				if (BATcount(b) > 0) {
					/* yes old view definition exists, it needs to be replaced */
					if ((err = sql_replace_Mar2018_ids_view(c, m)) != NULL) {
						fprintf(stderr, "!%s\n", err);
						freeException(err);
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
	if ((err = getName("gsl")) == NULL || getModule(err) == NULL) {
		/* no MAL module gsl, check for SQL function sys.chi2prob */
		sql_find_subtype(&tp, "double", 0, 0);
		if (sql_bind_func(m->sa, s, "chi2prob", &tp, &tp, F_FUNC)) {
			/* sys.chi2prob exists, but there is no
			 * implementation */
			if ((err = sql_update_gsl(c, m)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				freeException(err);
			}
		}
	}

	sql_find_subtype(&tp, "clob", 0, 0);
	if (sql_bind_aggr(m->sa, s, "group_concat", &tp) == NULL) {
		if ((err = sql_update_aug2018(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	if ((t = mvc_bind_table(m, s, "systemfunctions")) != NULL &&
	    t->type == tt_table) {
		if ((err = sql_update_default(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	if (sql_bind_func(m->sa, s, "dependencies_schemas_on_users", NULL, NULL, F_UNION)
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
		if ((err = sql_drop_functions_dependencies_Xs_on_Ys(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}
}
