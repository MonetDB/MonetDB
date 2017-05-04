/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * SQL upgrade code
 * N. Nes, M.L. Kersten, S. Mullender
 */
#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_scenario.h"
#include "sql_mvc.h"
#include <mtime.h>
#include <unistd.h>
#include "sql_upgrades.h"

#ifdef HAVE_EMBEDDED
#define printf(fmt,...) ((void) 0)
#endif

/* Because of a difference of computing hash values for single vs bulk operators we need to drop and recreate all constraints/indices */
static str
sql_update_oct2014_2(Client c, mvc *sql)
{
	size_t bufsize = 8192*2, pos = 0, recreate = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	res_table *fresult = NULL, *presult = NULL, *iresult = NULL;

	/* get list of all foreign keys */
	pos += snprintf(buf + pos, bufsize - pos,
			"SELECT fs.name, ft.name, fk.name, fk.\"action\", ps.name, pt.name FROM sys.keys fk, sys.tables ft, sys.schemas fs, sys.keys pk, sys.tables pt, sys.schemas ps WHERE fk.type = 2 AND (SELECT count(*) FROM sys.objects o WHERE o.id = fk.id) > 1 AND ft.id = fk.table_id AND ft.schema_id = fs.id AND fk.rkey = pk.id AND pk.table_id = pt.id AND pt.schema_id = ps.id;\n");
	err = SQLstatementIntern(c, &buf, "update", 1, 0, &fresult);

	/* get all primary/unique keys */
	pos = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"SELECT s.name, t.name, k.name, k.type FROM sys.keys k, sys.tables t, sys.schemas s WHERE k.type < 2 AND (SELECT count(*) FROM sys.objects o WHERE o.id = k.id) > 1 AND t.id = k.table_id AND t.schema_id = s.id;\n");
	err = SQLstatementIntern(c, &buf, "update", 1, 0, &presult);

	/* get indices */
	pos = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"SELECT s.name, t.name, i.name FROM sys.idxs i, sys.schemas s, sys.tables t WHERE i.table_id = t.id AND t.schema_id = s.id AND t.system = FALSE AND (SELECT count(*) FROM sys.objects o WHERE o.id = i.id) > 1;\n");
	err = SQLstatementIntern(c, &buf, "update", 1, 0, &iresult);

	if (fresult) {
		BATiter fs_name = bat_iterator(BATdescriptor(fresult->cols[0].b));
		BATiter ft_name = bat_iterator(BATdescriptor(fresult->cols[1].b));
		BATiter fk_name = bat_iterator(BATdescriptor(fresult->cols[2].b));
		BATiter fk_action = bat_iterator(BATdescriptor(fresult->cols[3].b));
		BATiter ps_name = bat_iterator(BATdescriptor(fresult->cols[4].b));
		BATiter pt_name = bat_iterator(BATdescriptor(fresult->cols[5].b));
		oid id = 0, cnt = BATcount(fs_name.b);

		pos = 0;
		/* get list of all foreign key objects */
		for(id = 0; id<cnt; id++) {
			char *fsname = (str)BUNtail(fs_name, id);
			char *ftname = (str)BUNtail(ft_name, id);
			char *fkname = (str)BUNtail(fk_name, id);
			int *fkaction = (int*)BUNtail(fk_action, id);
			int on_delete = ((*fkaction) & 0xFF);
			int on_update = (((*fkaction)>>8) & 0xFF);
			char *psname = (str)BUNtail(ps_name, id);
			char *ptname = (str)BUNtail(pt_name, id);
			sql_schema *s = mvc_bind_schema(sql, fsname);
			sql_key *k = mvc_bind_key(sql, s, fkname);
			sql_ukey *r = ((sql_fkey*)k)->rkey;
			char *sep = "";
			node *n;

			/* create recreate calls */
			pos += snprintf(buf + pos, bufsize - pos, "ALTER table \"%s\".\"%s\" ADD CONSTRAINT \"%s\" FOREIGN KEY (", fsname, ftname, fkname);
			for (n = k->columns->h; n; n = n->next) {
				sql_kc *kc = n->data;

				pos += snprintf(buf + pos, bufsize - pos, "%s\"%s\"", sep, kc->c->base.name);
				sep = ", ";
			}
			pos += snprintf(buf + pos, bufsize - pos, ") REFERENCES \"%s\".\"%s\" (", psname, ptname );
			sep = "";
			for (n = r->k.columns->h; n; n = n->next) {
				sql_kc *kc = n->data;

				pos += snprintf(buf + pos, bufsize - pos, "%s\"%s\"", sep, kc->c->base.name);
				sep = ", ";
			}
			pos += snprintf(buf + pos, bufsize - pos, ") ");
			if (on_delete != 2)
				pos += snprintf(buf + pos, bufsize - pos, "%s", (on_delete==0?"NO ACTION":on_delete==1?"CASCADE":on_delete==3?"SET NULL":"SET DEFAULT"));

			if (on_update != 2)
				pos += snprintf(buf + pos, bufsize - pos, "%s", (on_update==0?"NO ACTION":on_update==1?"CASCADE":on_update==3?"SET NULL":"SET DEFAULT"));
			pos += snprintf(buf + pos, bufsize - pos, ";\n");
			assert(pos < bufsize);

			/* drop foreign key */
			mvc_drop_key(sql, s, k, 0 /* drop_action?? */);
		}
		if (pos) {
			SQLautocommit(c, sql);
			SQLtrans(sql);
			recreate = pos;
		}
	}

	if (presult) {
		BATiter s_name = bat_iterator(BATdescriptor(presult->cols[0].b));
		BATiter t_name = bat_iterator(BATdescriptor(presult->cols[1].b));
		BATiter k_name = bat_iterator(BATdescriptor(presult->cols[2].b));
		BATiter k_type = bat_iterator(BATdescriptor(presult->cols[3].b));
		oid id = 0, cnt = BATcount(s_name.b);
		char *buf = GDKmalloc(bufsize);

		pos = 0;
		for(id = 0; id<cnt; id++) {
			char *sname = (str)BUNtail(s_name, id);
			char *tname = (str)BUNtail(t_name, id);
			char *kname = (str)BUNtail(k_name, id);
			int *ktype = (int*)BUNtail(k_type, id);
			sql_schema *s = mvc_bind_schema(sql, sname);
			sql_key *k = mvc_bind_key(sql, s, kname);
			node *n;
			char *sep = "";

			/* create recreate calls */
			pos += snprintf(buf + pos, bufsize - pos, "ALTER table \"%s\".\"%s\" ADD CONSTRAINT \"%s\" %s (", sname, tname, kname, *ktype == 0?"PRIMARY KEY":"UNIQUE");
			for (n = k->columns->h; n; n = n->next) {
				sql_kc *kc = n->data;

				pos += snprintf(buf + pos, bufsize - pos, "%s\"%s\"", sep, kc->c->base.name);
				sep = ", ";
			}
			pos += snprintf(buf + pos, bufsize - pos, ");\n" );
			assert(pos < bufsize);

			/* drop primary/unique key */
			mvc_drop_key(sql, s, k, 0 /* drop_action?? */);
		}
		if (pos) {
			SQLautocommit(c, sql);
			SQLtrans(sql);

			/* recreate primary and unique keys */
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
			if (!err)
				SQLautocommit(c, sql);
			SQLtrans(sql);
		}
		GDKfree(buf);
	}

	if (iresult) {
		BATiter s_name = bat_iterator(BATdescriptor(iresult->cols[0].b));
		BATiter t_name = bat_iterator(BATdescriptor(iresult->cols[1].b));
		BATiter i_name = bat_iterator(BATdescriptor(iresult->cols[2].b));
		oid id = 0, cnt = BATcount(s_name.b);
		char *buf = GDKmalloc(bufsize);

		pos = 0;
		for(id = 0; id<cnt; id++) {
			char *sname = (str)BUNtail(s_name, id);
			char *tname = (str)BUNtail(t_name, id);
			char *iname = (str)BUNtail(i_name, id);
			sql_schema *s = mvc_bind_schema(sql, sname);
			sql_idx *k = mvc_bind_idx(sql, s, iname);
			node *n;
			char *sep = "";

			if (!k || k->key)
				continue;
			/* create recreate calls */
			pos += snprintf(buf + pos, bufsize - pos, "CREATE INDEX \"%s\" ON \"%s\".\"%s\" (", iname, sname, tname);
			for (n = k->columns->h; n; n = n->next) {
				sql_kc *kc = n->data;

				pos += snprintf(buf + pos, bufsize - pos, "%s\"%s\"", sep, kc->c->base.name);
				sep = ", ";
			}
			pos += snprintf(buf + pos, bufsize - pos, ");\n" );
			assert(pos < bufsize);

			/* drop index */
			mvc_drop_idx(sql, s, k);
		}
		if (pos) {
			SQLautocommit(c, sql);
			SQLtrans(sql);

			/* recreate indices */
			printf("Running database upgrade commands:\n%s\n", buf);
			err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
			if (!err)
				SQLautocommit(c, sql);
			SQLtrans(sql);
		}
		GDKfree(buf);
	}


	/* recreate foreign keys */
	if (recreate) {
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
		if (!err)
			SQLautocommit(c, sql);
		SQLtrans(sql);
	}
	return err;
}

static str
sql_update_oct2014(Client c, mvc *sql)
{
	size_t bufsize = 8192*2, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");
	sql_table *t;
	sql_schema *s;

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	/* cleanup columns of dropped views */
	pos += snprintf(buf + pos, bufsize - pos,
			"delete from _columns where table_id not in (select id from _tables);\n");

	/* add new columns */
	store_next_oid(); /* reserve id for max(id)+1 */
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into _columns values( (select max(id)+1 from _columns), 'system', 'boolean', 1, 0, (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='schemas'), NULL, true, 4, NULL);\n");
	store_next_oid();
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into _columns values( (select max(id)+1 from _columns), 'varres', 'boolean', 1, 0, (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='functions'), NULL, true, 7, NULL);\n");
	store_next_oid();
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into _columns values( (select max(id)+1 from _columns), 'vararg', 'boolean', 1, 0, (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='functions'), NULL, true, 8, NULL);\n");
	store_next_oid();
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into _columns values( (select max(id)+1 from _columns), 'inout', 'tinyint', 8, 0, (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='args'), NULL, true, 6, NULL);\n");
	store_next_oid();
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into _columns values( (select max(id)+1 from _columns), 'language', 'int', 32, 0, (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='functions'), NULL, true, 9, NULL);\n"
			"delete from _columns where table_id in (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='functions') and name='sql';\n");

	/* correct column numbers */
	pos += snprintf(buf + pos, bufsize - pos,
			"update _columns set number='9' where name = 'schema_id' and table_id in (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='functions');\n"
			"update _columns set number='7' where name = 'number' and table_id in (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='args');\n"
			"update _columns set number='4' where name = 'language' and table_id in (select _tables.id from _tables join schemas on _tables.schema_id=schemas.id where schemas.name='sys' and _tables.name='functions');\n");

	/* remove table return types (#..), ie tt_generated from
	 * _tables/_columns */
	pos += snprintf(buf + pos, bufsize - pos,
			"delete from _columns where table_id in (select id from _tables where name like '#%%');\n"
			"delete from _tables where name like '#%%';\n");

	/* all UNION functions need to be drop and recreated */
	/* keep views depending on UNION funcs */

	pos += snprintf(buf + pos, bufsize - pos,
			"create table upgradeOct2014_views as (select s.name, t.query from tables t, schemas s where s.id = t.schema_id and t.id in (select d.depend_id from dependencies d where d.id in (select f.id from functions f where f.type = 5 and f.language <> 0) and d.depend_type = 5)) with data;\n"
			"create table upgradeOct2014 as (select s.name, f.func, f.id from functions f, schemas s where s.id = f.schema_id and f.type = 5 and f.language <> 0) with data;\n"
			"create table upgradeOct2014_changes (c bigint);\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function drop_func_upgrade_oct2014( id integer ) returns int external name sql.drop_func_upgrade_oct2014;\n"
			"insert into upgradeOct2014_changes select drop_func_upgrade_oct2014(id) from upgradeOct2014;\n"
			"drop function drop_func_upgrade_oct2014;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function create_func_upgrade_oct2014( sname string, f string ) returns int external name sql.create_func_upgrade_oct2014;\n"
			"insert into upgradeOct2014_changes select create_func_upgrade_oct2014(name, func) from upgradeOct2014;\n"
			"drop function create_func_upgrade_oct2014;\n");

	/* recreate views depending on union funcs */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function create_view_upgrade_oct2014( sname string, f string ) returns int external name sql.create_view_upgrade_oct2014;\n"
			"insert into upgradeOct2014_changes select create_view_upgrade_oct2014(name, query) from upgradeOct2014_views;\n"
			"update _tables set system = true where name in ('tables', 'columns', 'users', 'querylog_catalog', 'querylog_calls', 'querylog_history', 'tracelog', 'sessions', 'optimizers', 'environment', 'queue', 'storage', 'storagemodel', 'tablestoragemodel') and schema_id = (select id from schemas where name = 'sys');\n"
			"insert into systemfunctions (select id from functions where name in ('bbp', 'db_users', 'dependencies_columns_on_functions', 'dependencies_columns_on_indexes', 'dependencies_columns_on_keys', 'dependencies_columns_on_triggers', 'dependencies_columns_on_views', 'dependencies_functions_on_functions', 'dependencies_functions_os_triggers', 'dependencies_keys_on_foreignkeys', 'dependencies_owners_on_schemas', 'dependencies_schemas_on_users', 'dependencies_tables_on_foreignkeys', 'dependencies_tables_on_functions', 'dependencies_tables_on_indexes', 'dependencies_tables_on_triggers', 'dependencies_tables_on_views', 'dependencies_views_on_functions', 'dependencies_views_on_triggers', 'env', 'environment', 'generate_series', 'optimizers', 'optimizer_stats', 'querycache', 'querylog_calls', 'querylog_catalog', 'queue', 'sessions', 'storage', 'storagemodel', 'tojsonarray', 'tracelog', 'var') and schema_id = (select id from schemas where name = 'sys') and id not in (select function_id from systemfunctions));\n"
			"delete from systemfunctions where function_id not in (select id from functions);\n"
			"drop function create_view_upgrade_oct2014;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"drop table upgradeOct2014_views;\n"
			"drop table upgradeOct2014_changes;\n"
			"drop table upgradeOct2014;\n");

	/* change in 25_debug.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function sys.bbp;\n"
			"create function sys.bbp() returns table (id int, name string, htype string, ttype string, count BIGINT, refcnt int, lrefcnt int, location string, heat int, dirty string, status string, kind string) external name bbp.get;\n");

	/* new file 40_json.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create schema json;\n"

			"create type json external name json;\n"

			"create function json.filter(js json, pathexpr string)\n"
			"returns json external name json.filter;\n"

			"create function json.filter(js json, name tinyint)\n"
			"returns json external name json.filter;\n"

			"create function json.filter(js json, name integer)\n"
			"returns json external name json.filter;\n"

			"create function json.filter(js json, name bigint)\n"
			"returns json external name json.filter;\n"

			"create function json.text(js json, e string)\n"
			"returns string external name json.text;\n"

			"create function json.number(js json)\n"
			"returns float external name json.number;\n"

			"create function json.\"integer\"(js json)\n"
			"returns bigint external name json.\"integer\";\n"

			"create function json.isvalid(js string)\n"
			"returns bool external name json.isvalid;\n"

			"create function json.isobject(js string)\n"
			"returns bool external name json.isobject;\n"

			"create function json.isarray(js string)\n"
			"returns bool external name json.isarray;\n"

			"create function json.isvalid(js json)\n"
			"returns bool external name json.isvalid;\n"

			"create function json.isobject(js json)\n"
			"returns bool external name json.isobject;\n"

			"create function json.isarray(js json)\n"
			"returns bool external name json.isarray;\n"

			"create function json.length(js json)\n"
			"returns integer external name json.length;\n"

			"create function json.keyarray(js json)\n"
			"returns json external name json.keyarray;\n"

			"create function json.valuearray(js json)\n"
			"returns  json external name json.valuearray;\n"

			"create function json.text(js json)\n"
			"returns string external name json.text;\n"
			"create function json.text(js string)\n"
			"returns string external name json.text;\n"
			"create function json.text(js int)\n"
			"returns string external name json.text;\n"


			"create aggregate json.output(js json)\n"
			"returns string external name json.output;\n"

			"create aggregate json.tojsonarray( x string ) returns string external name aggr.jsonaggr;\n"
			"create aggregate json.tojsonarray( x double ) returns string external name aggr.jsonaggr;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.schemas set system = true where name = 'json';\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('filter', 'text', 'number', 'integer', 'isvalid', 'isobject', 'isarray', 'length', 'keyarray', 'valuearray') and f.type = %d and f.schema_id = s.id and s.name = 'json');\n",
			F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('output', 'tojsonarray') and f.type = %d and f.schema_id = s.id and s.name = 'json');\n",
			F_AGGR);

	/* new file 41_md5sum.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.md5(v string) returns string external name clients.md5sum;\n");

	/* new file 45_uuid.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create type uuid external name uuid;\n"
			"create function sys.uuid() returns uuid external name uuid.\"new\";\n"
			"create function sys.isaUUID(u uuid) returns uuid external name uuid.\"isaUUID\";\n");

	/* change to 75_storage functions */
	s = mvc_bind_schema(sql, "sys");
	if (s && (t = mvc_bind_table(sql, s, "storage")) != NULL)
		t->system = 0;
	if (s && (t = mvc_bind_table(sql, s, "storagemodel")) != NULL)
		t->system = 0;
	if (s && (t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
		t->system = 0;
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = false where name in ('storage','storagemodel','tablestoragemodel') and schema_id = (select id from sys.schemas where name = 'sys');\n"
			"drop view sys.storage;\n"
			"drop function sys.storage();\n"
			"drop view sys.storagemodel;\n"
			"drop view sys.tablestoragemodel;\n"
			"drop function sys.storagemodel();\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.storage() returns table (\"schema\" string, \"table\" string, \"column\" string, \"type\" string, location string, \"count\" bigint, typewidth int, columnsize bigint, heapsize bigint, hashes bigint, imprints bigint, sorted boolean) external name sql.storage;\n"
			"create view sys.storage as select * from sys.storage();\n");


	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.hashsize(b boolean, i bigint) returns bigint begin if  b = true then return 8 * i; end if; return 0; end;");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.imprintsize(i bigint, nme string) returns bigint begin if nme = 'boolean' or nme = 'tinyint' or nme = 'smallint' or nme = 'int'	or nme = 'bigint'	or nme = 'decimal'	or nme = 'date' or nme = 'timestamp' or nme = 'real' or nme = 'double' then return cast( i * 0.12 as bigint); end if ; return 0; end;");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.storagemodel() returns table ("
			"    \"schema\" string, \"table\" string, \"column\" string, \"type\" string, \"count\" bigint,"
			"    columnsize bigint, heapsize bigint, hashes bigint, imprints bigint, sorted boolean)"
			"begin return select I.\"schema\", I.\"table\", I.\"column\", I.\"type\", I.\"count\","
			"  columnsize(I.\"type\", I.count, I.\"distinct\"),"
			"  heapsize(I.\"type\", I.\"distinct\", I.\"atomwidth\"),"
			"  hashsize(I.\"reference\", I.\"count\"),"
			"  imprintsize(I.\"count\",I.\"type\"),"
			"  I.sorted"
			"  from sys.storagemodelinput I;"
			"end;\n");
	pos += snprintf(buf + pos, bufsize - pos,
			"create view sys.tablestoragemodel"
			" as select \"schema\",\"table\",max(count) as \"count\","
			"    sum(columnsize) as columnsize,"
			"    sum(heapsize) as heapsize,"
			"    sum(hashes) as hashes,"
			"    sum(imprints) as imprints,"
			"    sum(case when sorted = false then 8 * count else 0 end) as auxiliary"
			" from sys.storagemodel() group by \"schema\",\"table\";\n"
			"create view sys.storagemodel as select * from sys.storagemodel();\n"
			"update sys._tables set system = true where name in ('storage','storagemodel','tablestoragemodel') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* new file 90_generator.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.generate_series(first tinyint, last tinyint)\n"
			"returns table (value tinyint)\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first tinyint, last tinyint, stepsize tinyint)\n"
			"returns table (value tinyint)\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first int, last int)\n"
			"returns table (value int)\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first int, last int, stepsize int)\n"
			"returns table (value int)\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first bigint, last bigint)\n"
			"returns table (value bigint)\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first bigint, last bigint, stepsize bigint)\n"
			"returns table (value bigint)\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first real, last real, stepsize real)\n"
			"returns table (value real)\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first double, last double, stepsize double)\n"
			"returns table (value double)\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first decimal(10,2), last decimal(10,2), stepsize decimal(10,2))\n"
			"returns table (value decimal(10,2))\n"
			"external name generator.series;\n"

			"create function sys.generate_series(first timestamp, last timestamp, stepsize interval second)\n"
			"returns table (value timestamp)\n"
			"external name generator.series;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('hashsize', 'imprintsize', 'isauuid', 'md5', 'uuid') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_FUNC);

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('bbp', 'db_users', 'env', 'generate_series', 'storage', 'storagemodel', 'var') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_UNION);

	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	if (err == MAL_SUCCEED)
		return sql_update_oct2014_2(c, sql);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2014_sp1(Client c, mvc *sql)
{
	size_t bufsize = 8192*2, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"create table upgradeOct2014_views as (select s.name, t.query from tables t, schemas s where s.id = t.schema_id and t.id in (select d.depend_id from dependencies d where d.id in (select f.id from functions f where f.func ilike '%%returns table%%' and f.name not in ('env', 'var', 'db_users') and f.language <> 0) and d.depend_type = 5)) with data;\n"
			"create table upgradeOct2014 as select s.name, f.func, f.id from functions f, schemas s where s.id = f.schema_id and f.func ilike '%%returns table%%' and f.name not in ('env', 'var', 'db_users') and f.language <> 0 order by f.id with data;\n"
			"create table upgradeOct2014_changes (c bigint);\n"

			"create function drop_func_upgrade_oct2014( id integer ) returns int external name sql.drop_func_upgrade_oct2014;\n"
			"insert into upgradeOct2014_changes select drop_func_upgrade_oct2014(id) from upgradeOct2014;\n"
			"drop function drop_func_upgrade_oct2014;\n"

			"create function create_func_upgrade_oct2014( sname string, f string ) returns int external name sql.create_func_upgrade_oct2014;\n"
			"insert into upgradeOct2014_changes select create_func_upgrade_oct2014(name, func) from upgradeOct2014;\n"
			"drop function create_func_upgrade_oct2014;\n"

			"create function create_view_upgrade_oct2014( sname string, f string ) returns int external name sql.create_view_upgrade_oct2014;\n"
			"insert into upgradeOct2014_changes select create_view_upgrade_oct2014(name, query) from upgradeOct2014_views;\n"
			"update _tables set system = true where name in ('tables', 'columns', 'users', 'querylog_catalog', 'querylog_calls', 'querylog_history', 'tracelog', 'sessions', 'optimizers', 'environment', 'queue', 'storage', 'storagemodel', 'tablestoragemodel') and schema_id = (select id from schemas where name = 'sys');\n"
			"insert into systemfunctions (select id from functions where name in ('bbp', 'db_users', 'dependencies_columns_on_functions', 'dependencies_columns_on_indexes', 'dependencies_columns_on_keys', 'dependencies_columns_on_triggers', 'dependencies_columns_on_views', 'dependencies_functions_on_functions', 'dependencies_functions_os_triggers', 'dependencies_keys_on_foreignkeys', 'dependencies_owners_on_schemas', 'dependencies_schemas_on_users', 'dependencies_tables_on_foreignkeys', 'dependencies_tables_on_functions', 'dependencies_tables_on_indexes', 'dependencies_tables_on_triggers', 'dependencies_tables_on_views', 'dependencies_views_on_functions', 'dependencies_views_on_triggers', 'env', 'environment', 'generate_series', 'optimizers', 'optimizer_stats', 'querycache', 'querylog_calls', 'querylog_catalog', 'queue', 'sessions', 'storage', 'storagemodel', 'tojsonarray', 'tracelog', 'var') and schema_id = (select id from schemas where name = 'sys') and id not in (select function_id from systemfunctions));\n"
			"delete from systemfunctions where function_id not in (select id from functions);\n"
			"drop function create_view_upgrade_oct2014;\n"

			"drop table upgradeOct2014_views;\n"
			"drop table upgradeOct2014_changes;\n"
			"drop table upgradeOct2014;\n");

	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2014_sp2(Client c, mvc *sql)
{
	size_t bufsize = 8192, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"drop view sys.tablestoragemodel;\n"
			"create view sys.tablestoragemodel\n"
			"as select \"schema\",\"table\",max(count) as \"count\",\n"
			"  sum(columnsize) as columnsize,\n"
			"  sum(heapsize) as heapsize,\n"
			"  sum(hashes) as hashes,\n"
			"  sum(imprints) as imprints,\n"
			"  sum(case when sorted = false then 8 * count else 0 end) as auxiliary\n"
			"from sys.storagemodel() group by \"schema\",\"table\";\n"
			"update _tables set system = true where name = 'tablestoragemodel' and schema_id = (select id from schemas where name = 'sys');\n");

	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	assert(pos < bufsize);

	{
		sql_schema *s;

		if ((s = mvc_bind_schema(sql, "sys")) != NULL) {
			sql_table *t;

			if ((t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
				t->system = 0;
		}
	}

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2014_sp3(Client c, mvc *sql)
{
	size_t bufsize = 8192, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"CREATE FUNCTION \"left_shift\"(i1 inet, i2 inet) RETURNS boolean EXTERNAL NAME inet.\"<<\";\n"
			"CREATE FUNCTION \"right_shift\"(i1 inet, i2 inet) RETURNS boolean EXTERNAL NAME inet.\">>\";\n"
			"CREATE FUNCTION \"left_shift_assign\"(i1 inet, i2 inet) RETURNS boolean EXTERNAL NAME inet.\"<<=\";\n"
			"CREATE FUNCTION \"right_shift_assign\"(i1 inet, i2 inet) RETURNS boolean EXTERNAL NAME inet.\">>=\";\n"
			"insert into sys.systemfunctions (select id from sys.functions where name in ('left_shift', 'right_shift', 'left_shift_assign', 'right_shift_assign') and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n");

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
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

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
			"create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns HUGEINT\n"
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
			"  sum(imprints) as imprints,\n"
			"  sum(case when sorted = false then 8 * count else 0 end) as auxiliary\n"
			"from sys.storagemodel() group by \"schema\",\"table\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select id from sys.functions where name in ('fuse', 'generate_series', 'stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'median', 'quantile', 'corr') and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n"
			"insert into sys.systemfunctions (select id from sys.functions where name = 'filter' and schema_id = (select id from sys.schemas where name = 'json') and id not in (select function_id from sys.systemfunctions));\n"
			"update sys._tables set system = true where name = 'tablestoragemodel' and schema_id = (select id from sys.schemas where name = 'sys');\n");

	{
		node *n;
		sql_type *t;

		for (n = types->h; n; n = n->next) {
			t = n->data;
			if (t->base.id < 2000 &&
			    strcmp(t->base.name, "hge") == 0)
				pos += snprintf(buf + pos, bufsize - pos, "insert into sys.types values (%d, '%s', '%s', %u, %u, %d, %d, %d);\n", t->base.id, t->base.name, t->sqlname, t->digits, t->scale, t->radix, t->eclass, t->s ? t->s->base.id : 0);
		}
	}

	{
		sql_schema *s;

		if ((s = mvc_bind_schema(sql, "sys")) != NULL) {
			sql_table *t;

			if ((t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
				t->system = 0;
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
#endif

static str
sql_update_jul2015(Client c, mvc *sql)
{
	size_t bufsize = 15360, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	/* change to 09_like */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop filter function sys.\"like\";\n"
			"create filter function sys.\"like\"(val string, pat string, esc string) external name algebra.\"like\";\n"
			"create filter function sys.\"like\"(val string, pat string) external name algebra.\"like\";\n"
			"drop filter function sys.\"ilike\";\n"
			"create filter function sys.\"ilike\"(val string, pat string, esc string) external name algebra.\"ilike\";\n"
			"create filter function sys.\"ilike\"(val string, pat string) external name algebra.\"ilike\";\n");

	/* change to 13_date */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.str_to_time(s string, format string) returns time external name mtime.\"str_to_time\";\n"
			"create function sys.time_to_str(d time, format string) returns string external name mtime.\"time_to_str\";\n"
			"create function sys.str_to_timestamp(s string, format string) returns timestamp external name mtime.\"str_to_timestamp\";\n"
			"create function sys.timestamp_to_str(d timestamp, format string) returns string external name mtime.\"timestamp_to_str\";\n");

	/* change to 15_querylog */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.querylog_history;\n"
			"drop view sys.querylog_calls;\n"
			"drop function sys.querylog_calls;\n"
			"drop view sys.querylog_catalog;\n"
			"drop function sys.querylog_catalog;\n"
			"create function sys.querylog_catalog()\n"
			"returns table(\n"
			"    id oid,\n"
			"    owner string,\n"
			"    defined timestamp,\n"
			"    query string,\n"
			"    pipe string,\n"
			"    \"plan\" string,\n"
			"    mal int,\n"
			"    optimize bigint\n"
			") external name sql.querylog_catalog;\n"
			"create function sys.querylog_calls()\n"
			"returns table(\n"
			"    id oid,\n"
			"    \"start\" timestamp,\n"
			"    \"stop\" timestamp,\n"
			"    arguments string,\n"
			"    tuples wrd,\n"
			"    run bigint,\n"
			"    ship bigint,\n"
			"    cpu int,\n"
			"    io int\n"
			") external name sql.querylog_calls;\n"
			"create view sys.querylog_catalog as select * from sys.querylog_catalog();\n"
			"create view sys.querylog_calls as select * from sys.querylog_calls();\n"
			"create view sys.querylog_history as\n"
			"select qd.*, ql.\"start\",ql.\"stop\", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.io\n"
			"from sys.querylog_catalog() qd, sys.querylog_calls() ql\n"
			"where qd.id = ql.id and qd.owner = user;\n");


	/* change to 16_tracelog */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tracelog;\n"
			"drop function sys.tracelog;\n"
			"create function sys.tracelog()\n"
			"returns table (\n"
			"    event integer,\n"
			"    clk varchar(20),\n"
			"    pc varchar(50),\n"
			"    thread int,\n"
			"    ticks bigint,\n"
			"    rrsMB bigint,\n"
			"    vmMB bigint,\n"
			"    reads bigint,\n"
			"    writes bigint,\n"
			"    minflt bigint,\n"
			"    majflt bigint,\n"
			"    nvcsw bigint,\n"
			"    stmt string\n"
			"    ) external name sql.dump_trace;\n"
			"create view sys.tracelog as select * from sys.tracelog();\n"
			"create procedure sys.profiler_openstream(host string, port int) external name profiler.\"openStream\";\n"
			"create procedure sys.profiler_stethoscope(ticks int) external name profiler.stethoscope;\n");

	/* change to 17_temporal */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.\"epoch\"(sec BIGINT) returns TIMESTAMP external name timestamp.\"epoch\";\n"
			"create function sys.\"epoch\"(ts TIMESTAMP WITH TIME ZONE) returns INT external name timestamp.\"epoch\";\n");

	/* removal of 19_cluster.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop procedure sys.cluster1;\n"
			"drop procedure sys.cluster2;\n");

	/* new file 27_rejects.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.rejects()\n"
			"returns table(\n"
			"    rowid bigint,\n"
			"    fldid int,\n"
			"    \"message\" string,\n"
			"    \"input\" string\n"
			") external name sql.copy_rejects;\n"
			"create view sys.rejects as select * from sys.rejects();\n"
			"create procedure sys.clearrejects()\n"
			"external name sql.copy_rejects_clear;\n");

	/* new file 51_sys_schema_extension.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"CREATE TABLE sys.keywords (\n"
			"    keyword VARCHAR(40) NOT NULL PRIMARY KEY);\n"

			"INSERT INTO sys.keywords (keyword) VALUES\n"
			"('ADMIN'), ('AFTER'), ('AGGREGATE'), ('ALWAYS'), ('ASYMMETRIC'), ('ATOMIC'), ('AUTO_INCREMENT'),\n"
			"('BEFORE'), ('BIGINT'), ('BIGSERIAL'), ('BINARY'), ('BLOB'),\n"
			"('CALL'), ('CHAIN'), ('CLOB'), ('COMMITTED'), ('COPY'), ('CORR'), ('CUME_DIST'), ('CURRENT_ROLE'), ('CYCLE'),\n"
			"('DATABASE'), ('DELIMITERS'), ('DENSE_RANK'), ('DO'),\n"
			"('EACH'), ('ELSEIF'), ('ENCRYPTED'), ('EVERY'), ('EXCLUDE'),\n"
			"('FOLLOWING'), ('FUNCTION'),\n"
			"('GENERATED'),\n"
			"('IF'), ('ILIKE'), ('INCREMENT'),\n"
			"('LAG'), ('LEAD'), ('LIMIT'), ('LOCALTIME'), ('LOCALTIMESTAMP'), ('LOCKED'),\n"
			"('MAXVALUE'), ('MEDIAN'), ('MEDIUMINT'), ('MERGE'), ('MINVALUE'),\n"
			"('NEW'), ('NOCYCLE'), ('NOMAXVALUE'), ('NOMINVALUE'), ('NOW'),\n"
			"('OFFSET'), ('OLD'), ('OTHERS'), ('OVER'),\n"
			"('PARTITION'), ('PERCENT_RANK'), ('PLAN'), ('PRECEDING'), ('PROD'),\n"
			"('QUANTILE'),\n"
			"('RANGE'), ('RANK'), ('RECORDS'), ('REFERENCING'), ('REMOTE'), ('RENAME'), ('REPEATABLE'), ('REPLICA'),\n"
			"('RESTART'), ('RETURN'), ('RETURNS'), ('ROWS'), ('ROW_NUMBER'),\n"
			"('SAMPLE'), ('SAVEPOINT'), ('SCHEMA'), ('SEQUENCE'), ('SERIAL'), ('SERIALIZABLE'), ('SIMPLE'),\n"
			"('START'), ('STATEMENT'), ('STDIN'), ('STDOUT'), ('STREAM'), ('STRING'), ('SYMMETRIC'),\n"
			"('TIES'), ('TINYINT'), ('TRIGGER'),\n"
			"('UNBOUNDED'), ('UNCOMMITTED'), ('UNENCRYPTED'),\n"
			"('WHILE'),\n"
			"('XMLAGG'), ('XMLATTRIBUTES'), ('XMLCOMMENT'), ('XMLCONCAT'), ('XMLDOCUMENT'), ('XMLELEMENT'), ('XMLFOREST'),\n"
			"('XMLNAMESPACES'), ('XMLPARSE'), ('XMLPI'), ('XMLQUERY'), ('XMLSCHEMA'), ('XMLTEXT'), ('XMLVALIDATE');\n"

			"CREATE TABLE sys.table_types (\n"
			"    table_type_id   SMALLINT NOT NULL PRIMARY KEY,\n"
			"    table_type_name VARCHAR(25) NOT NULL UNIQUE);\n"

			"INSERT INTO sys.table_types (table_type_id, table_type_name) VALUES\n"
			"-- values from sys._tables.type:  0=Table, 1=View, 2=Generated, 3=Merge, etc.\n"
			"  (0, 'TABLE'), (1, 'VIEW'), /* (2, 'GENERATED'), */ (3, 'MERGE TABLE'), (4, 'STREAM TABLE'), (5, 'REMOTE TABLE'), (6, 'REPLICA TABLE'),\n"
			"-- synthetically constructed system obj variants (added 10 to sys._tables.type value when sys._tables.system is true).\n"
			"  (10, 'SYSTEM TABLE'), (11, 'SYSTEM VIEW'),\n"
			"-- synthetically constructed temporary variants (added 20 or 30 to sys._tables.type value depending on values of temporary and commit_action).\n"
			"  (20, 'GLOBAL TEMPORARY TABLE'),\n"
			"  (30, 'LOCAL TEMPORARY TABLE');\n"

			"CREATE TABLE sys.dependency_types (\n"
			"    dependency_type_id   SMALLINT NOT NULL PRIMARY KEY,\n"
			"    dependency_type_name VARCHAR(15) NOT NULL UNIQUE);\n"

			"INSERT INTO sys.dependency_types (dependency_type_id, dependency_type_name) VALUES\n"
			"-- values taken from sql_catalog.h\n"
			"  (1, 'SCHEMA'), (2, 'TABLE'), (3, 'COLUMN'), (4, 'KEY'), (5, 'VIEW'), (6, 'USER'), (7, 'FUNCTION'), (8, 'TRIGGER'),\n"
			"  (9, 'OWNER'), (10, 'INDEX'), (11, 'FKEY'), (12, 'SEQUENCE'), (13, 'PROCEDURE'), (14, 'BE_DROPPED');\n");

	/* the attendant change to the sys.tables view */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tables;\n"
			"create view sys.tables as SELECT \"id\", \"name\", \"schema_id\", \"query\", CAST(CASE WHEN \"system\" THEN \"type\" + 10 /* system table/view */ ELSE (CASE WHEN \"commit_action\" = 0 THEN \"type\" /* table/view */ ELSE \"type\" + 20 /* global temp table */ END) END AS SMALLINT) AS \"type\", \"system\", \"commit_action\", \"access\", CASE WHEN (NOT \"system\" AND \"commit_action\" > 0) THEN 1 ELSE 0 END AS \"temporary\" FROM \"sys\".\"_tables\" WHERE \"type\" <> 2 UNION ALL SELECT \"id\", \"name\", \"schema_id\", \"query\", CAST(\"type\" + 30 /* local temp table */ AS SMALLINT) AS \"type\", \"system\", \"commit_action\", \"access\", 1 AS \"temporary\" FROM \"tmp\".\"_tables\";\n");

	/* change to 75_storagemodel */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tablestoragemodel;\n"
			"drop view sys.storagemodel;\n"
			"drop function sys.storagemodel;\n"
			"drop function sys.imprintsize;\n"
			"drop function sys.columnsize;\n"
			"drop procedure sys.storagemodelinit;\n"
			"drop view sys.storage;\n"
			"drop function sys.storage;\n"

			"create function sys.\"storage\"()\n"
			"returns table (\n"
			"  \"schema\" string,\n"
			"  \"table\" string,\n"
			"  \"column\" string,\n"
			"  \"type\" string,\n"
			"  \"mode\" string,\n"
			"  location string,\n"
			"  \"count\" bigint,\n"
			"  typewidth int,\n"
			"  columnsize bigint,\n"
			"  heapsize bigint,\n"
			"  hashes bigint,\n"
			"  phash boolean,\n"
			"  imprints bigint,\n"
			"  sorted boolean,\n"
			"  orderidx bigint\n"
			")\n"
			"external name sql.\"storage\";\n"

			"create view sys.\"storage\" as select * from sys.\"storage\"();\n"

			"create procedure sys.storagemodelinit()\n"
			"begin\n"
			"  delete from sys.storagemodelinput;\n"
			"  insert into sys.storagemodelinput\n"
			"  select X.\"schema\", X.\"table\", X.\"column\", X.\"type\", X.typewidth, X.count, 0, X.typewidth, false, X.sorted from sys.\"storage\"() X;\n"
			"  update sys.storagemodelinput\n"
			"  set reference = true\n"
			"  where concat(concat(\"schema\",\"table\"), \"column\") in (\n"
			"    SELECT concat( concat(\"fkschema\".\"name\", \"fktable\".\"name\"), \"fkkeycol\".\"name\" )\n"
			"    FROM \"sys\".\"keys\" AS    \"fkkey\",\n"
			"        \"sys\".\"objects\" AS \"fkkeycol\",\n"
			"        \"sys\".\"tables\" AS  \"fktable\",\n"
			"        \"sys\".\"schemas\" AS \"fkschema\"\n"
			"    WHERE \"fktable\".\"id\" = \"fkkey\".\"table_id\"\n"
			"      AND \"fkkey\".\"id\" = \"fkkeycol\".\"id\"\n"
			"      AND \"fkschema\".\"id\" = \"fktable\".\"schema_id\"\n"
			"      AND \"fkkey\".\"rkey\" > -1);\n"
			"  update sys.storagemodelinput\n"
			"  set \"distinct\" = \"count\" -- assume all distinct\n"
			"  where \"type\" = 'varchar' or \"type\"='clob';\n"
			"end;\n"

			"create function sys.columnsize(nme string, i bigint, d bigint)\n"
			"returns bigint\n"
			"begin\n"
			"  case\n"
			"  when nme = 'boolean' then return i;\n"
			"  when nme = 'char' then return 2*i;\n"
			"  when nme = 'smallint' then return 2 * i;\n"
			"  when nme = 'int' then return 4 * i;\n"
			"  when nme = 'bigint' then return 8 * i;\n"
			"  when nme = 'hugeint' then return 16 * i;\n"
			"  when nme = 'timestamp' then return 8 * i;\n"
			"  when  nme = 'varchar' then\n"
			"    case\n"
			"    when cast(d as bigint) << 8 then return i;\n"
			"    when cast(d as bigint) << 16 then return 2 * i;\n"
			"    when cast(d as bigint) << 32 then return 4 * i;\n"
			"    else return 8 * i;\n"
			"    end case;\n"
			"  else return 8 * i;\n"
			"  end case;\n"
			"end;\n"

			"create function sys.imprintsize(i bigint, nme string)\n"
			"returns bigint\n"
			"begin\n"
			"  if nme = 'boolean'\n"
			"    or nme = 'tinyint'\n"
			"    or nme = 'smallint'\n"
			"    or nme = 'int'\n"
			"    or nme = 'bigint'\n"
			"    or nme = 'hugeint'\n"
			"    or nme = 'decimal'\n"
			"    or nme = 'date'\n"
			"    or nme = 'timestamp'\n"
			"    or nme = 'real'\n"
			"    or nme = 'double'\n"
			"  then\n"
			"    return cast( i * 0.12 as bigint);\n"
			"  end if;\n"
			"  return 0;\n"
			"end;\n"

			"create function sys.storagemodel()\n"
			"returns table (\n"
			"  \"schema\" string,\n"
			"  \"table\" string,\n"
			"  \"column\" string,\n"
			"  \"type\" string,\n"
			"  \"count\" bigint,\n"
			"  columnsize bigint,\n"
			"  heapsize bigint,\n"
			"  hashes bigint,\n"
			"  imprints bigint,\n"
			"  sorted boolean,"
			"  orderidx bigint)\n"
			"begin\n"
			"  return select I.\"schema\", I.\"table\", I.\"column\", I.\"type\", I.\"count\",\n"
			"  columnsize(I.\"type\", I.count, I.\"distinct\"),\n"
			"  heapsize(I.\"type\", I.\"distinct\", I.\"atomwidth\"),\n"
			"  hashsize(I.\"reference\", I.\"count\"),\n"
			"  imprintsize(I.\"count\",I.\"type\"),\n"
			"  I.sorted, I.orderidx\n"
			"  from sys.storagemodelinput I;\n"
			"end;\n"

			"create view sys.storagemodel as select * from sys.storagemodel();\n"

			"create view sys.tablestoragemodel\n"
			"as select \"schema\",\"table\",max(count) as \"count\",\n"
			"  sum(columnsize) as columnsize,\n"
			"  sum(heapsize) as heapsize,\n"
			"  sum(hashes) as hashes,\n"
			"  sum(imprints) as imprints,\n"
			"  sum(case when sorted = false then 8 * count else 0 end) as auxiliary\n"
			"from sys.storagemodel() group by \"schema\",\"table\";\n");

	/* change to 80_statistics */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop all procedure sys.analyze;\n"
			"drop table sys.statistics;\n"
			"create table sys.statistics(\n"
			"    \"column_id\" integer,\n"
			"    \"type\" string,\n"
			"    width integer,\n"
			"    stamp timestamp,\n"
			"    \"sample\" bigint,\n"
			"    \"count\" bigint,\n"
			"    \"unique\" bigint,\n"
			"    \"nils\" bigint,\n"
			"    minval string,\n"
			"    maxval string,\n"
			"    sorted boolean);\n"
			"create procedure sys.analyze(minmax int, \"sample\" bigint)\n"
			"external name sql.analyze;\n"
			"create procedure sys.analyze(minmax int, \"sample\" bigint, sch string)\n"
			"external name sql.analyze;\n"
			"create procedure sys.analyze(minmax int, \"sample\" bigint, sch string, tbl string)\n"
			"external name sql.analyze;\n"
			"create procedure sys.analyze(minmax int, \"sample\" bigint, sch string, tbl string, col string)\n"
			"external name sql.analyze;\n");

	/* 15_querylog update the querylog table definition */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.querylog_history;\n"
			"drop view sys.querylog_calls;\n"
			"drop function sys.querylog_calls;\n"
			"create function sys.querylog_calls()\n"
			"returns table(\n"
			"    id oid,\n"
			"    \"start\" timestamp,\n"
			"    \"stop\" timestamp,\n"
			"    arguments string,\n"
			"    tuples wrd,\n"
			"    run bigint,\n"
			"    ship bigint,\n"
			"    cpu int,\n"
			"    io int\n"
			") external name sql.querylog_calls;\n"
			"create view sys.querylog_calls as select * from sys.querylog_calls();\n"
			"create view sys.querylog_history as\n"
			"select qd.*, ql.\"start\",ql.\"stop\", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.io\n"
			"from sys.querylog_catalog() qd, sys.querylog_calls() ql\n"
			"where qd.id = ql.id and qd.owner = user;\n");


	/* 16_tracelog update the tracelog table definition */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.tracelog;\n"
			"drop function sys.tracelog;\n"
			"create function sys.tracelog()\n"
			"returns table (\n"
			"  event integer,\n"
			"  clk varchar(20),\n"
			"  pc varchar(50),\n"
			"  thread int,\n"
			"  ticks bigint,\n"
			"  rrsMB bigint,\n"
			"  vmMB bigint,\n"
			"  reads bigint,\n"
			"  writes bigint,\n"
			"  minflt bigint,\n"
			"  majflt bigint,\n"
			"  nvcsw bigint,\n"
			"  stmt string\n"
			"  ) external name sql.dump_trace;\n"
			"create view sys.tracelog as select * from sys.tracelog();\n");


	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select id from sys.functions where name in ('analyze', 'clearrejects', 'columnsize', 'epoch', 'ilike', 'imprintsize', 'like', 'profiler_openstream', 'profiler_stethoscope', 'querylog_calls', 'querylog_catalog', 'rejects', 'storage', 'storagemodel', 'storagemodelinit', 'str_to_time', 'str_to_timestamp', 'timestamp_to_str', 'time_to_str', 'tracelog') and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n"
			"delete from systemfunctions where function_id not in (select id from functions);\n"
			"update sys._tables set system = true where name in ('dependency_types', 'keywords', 'querylog_calls', 'querylog_catalog', 'querylog_history', 'rejects', 'statistics', 'storage', 'storagemodel', 'tables', 'tablestoragemodel', 'table_types', 'tracelog') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	{
		sql_schema *s;

		if ((s = mvc_bind_schema(sql, "sys")) != NULL) {
			sql_table *t;

			if ((t = mvc_bind_table(sql, s, "querylog_calls")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "querylog_catalog")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "querylog_history")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "statistics")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "storagemodel")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "storage")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "tables")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
				t->system = 0;
			if ((t = mvc_bind_table(sql, s, "tracelog")) != NULL)
				t->system = 0;
		}
	}

	/* remove code from 19_cluster.sql script */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop procedure sys.cluster1;\n"
			"drop procedure sys.cluster2;\n");

	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_epoch(Client c, mvc *m)
{
	size_t bufsize = 1000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(m, "current_schema");
	sql_subtype tp;
	int n = 0;
	sql_schema *s = mvc_bind_schema(m, "sys");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	sql_find_subtype(&tp, "bigint", 0, 0);
	if (!sql_bind_func(m->sa, s, "epoch", &tp, NULL, F_FUNC)) {
		n++;
		pos += snprintf(buf + pos, bufsize - pos, "\
create function sys.\"epoch\"(sec BIGINT) returns TIMESTAMP external name timestamp.\"epoch\";\n");
	}
	sql_find_subtype(&tp, "int", 0, 0);
	if (!sql_bind_func(m->sa, s, "epoch", &tp, NULL, F_FUNC)) {
		n++;
		pos += snprintf(buf + pos, bufsize - pos, "\
create function sys.\"epoch\"(sec INT) returns TIMESTAMP external name timestamp.\"epoch\";\n");
	}
	sql_find_subtype(&tp, "timestamp", 0, 0);
	if (!sql_bind_func(m->sa, s, "epoch", &tp, NULL, F_FUNC)) {
		n++;
		pos += snprintf(buf + pos, bufsize - pos, "\
create function sys.\"epoch\"(ts TIMESTAMP) returns INT external name timestamp.\"epoch\";\n");
	}
	sql_find_subtype(&tp, "timestamptz", 0, 0);
	if (!sql_bind_func(m->sa, s, "epoch", &tp, NULL, F_FUNC)) {
		n++;
		pos += snprintf(buf + pos, bufsize - pos, "\
create function sys.\"epoch\"(ts TIMESTAMP WITH TIME ZONE) returns INT external name timestamp.\"epoch\";\n");
	}
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select id from sys.functions where name = 'epoch' and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n");

	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);

	assert(pos < bufsize);
	if (n) {
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	}
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jun2016(Client c, mvc *sql)
{
	size_t bufsize = 1000000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");
	node *n;
	sql_schema *s;

	s = mvc_bind_schema(sql, "sys");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	pos += snprintf(buf + pos, bufsize - pos, "delete from sys.dependencies where id < 2000;\n");
	pos += snprintf(buf + pos, bufsize - pos, "delete from sys.types where id < 2000;\n");
	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;

		if (t->base.id >= 2000)
			continue;

		pos += snprintf(buf + pos, bufsize - pos, "insert into sys.types values (%d, '%s', '%s', %u, %u, %d, %d, %d);\n", t->base.id, t->base.name, t->sqlname, t->digits, t->scale, t->radix, t->eclass, t->s ? t->s->base.id : s->base.id);
	}
	pos += snprintf(buf + pos, bufsize - pos, "delete from sys.functions where id < 2000;\n");
	pos += snprintf(buf + pos, bufsize - pos, "delete from sys.args where func_id not in (select id from sys.functions);\n");
	for (n = funcs->h; n; n = n->next) {
		sql_func *f = n->data;
		int number = 0;
		sql_arg *a;
		node *m;

		if (f->base.id >= 2000)
			continue;

		pos += snprintf(buf + pos, bufsize - pos, "insert into sys.functions values (%d, '%s', '%s', '%s', %d, %d, %s, %s, %s, %d);\n", f->base.id, f->base.name, f->imp, f->mod, FUNC_LANG_INT, f->type, f->side_effect ? "true" : "false", f->varres ? "true" : "false", f->vararg ? "true" : "false", f->s ? f->s->base.id : s->base.id);
		if (f->res) {
			for (m = f->res->h; m; m = m->next, number++) {
				a = m->data;
				pos += snprintf(buf + pos, bufsize - pos, "insert into sys.args values (%d, %d, 'res_%d', '%s', %u, %u, %d, %d);\n", store_next_oid(), f->base.id, number, a->type.type->sqlname, a->type.digits, a->type.scale, a->inout, number);
			}
		}
		for (m = f->ops->h; m; m = m->next, number++) {
			a = m->data;
			if (a->name)
				pos += snprintf(buf + pos, bufsize - pos, "insert into sys.args values (%d, %d, '%s', '%s', %u, %u, %d, %d);\n", store_next_oid(), f->base.id, a->name, a->type.type->sqlname, a->type.digits, a->type.scale, a->inout, number);
			else
				pos += snprintf(buf + pos, bufsize - pos, "insert into sys.args values (%d, %d, 'arg_%d', '%s', %u, %u, %d, %d);\n", store_next_oid(), f->base.id, number, a->type.type->sqlname, a->type.digits, a->type.scale, a->inout, number);
		}
	}
	for (n = aggrs->h; n; n = n->next) {
		sql_func *aggr = n->data;
		sql_arg *arg;

		if (aggr->base.id >= 2000)
			continue;

		pos += snprintf(buf + pos, bufsize - pos, "insert into sys.functions values (%d, '%s', '%s', '%s', %d, %d, false, %s, %s, %d);\n", aggr->base.id, aggr->base.name, aggr->imp, aggr->mod, FUNC_LANG_INT, aggr->type, aggr->varres ? "true" : "false", aggr->vararg ? "true" : "false", aggr->s ? aggr->s->base.id : s->base.id);
		arg = aggr->res->h->data;
		pos += snprintf(buf + pos, bufsize - pos, "insert into sys.args values (%d, %d, 'res', '%s', %u, %u, %d, 0);\n", store_next_oid(), aggr->base.id, arg->type.type->sqlname, arg->type.digits, arg->type.scale, arg->inout);
		if (aggr->ops->h) {
			arg = aggr->ops->h->data;

			pos += snprintf(buf + pos, bufsize - pos, "insert into sys.args values (%d, %d, 'arg', '%s', %u, %u, %d, 1);\n", store_next_oid(), aggr->base.id, arg->type.type->sqlname, arg->type.digits, arg->type.scale, arg->inout);
		}
	}
	pos += snprintf(buf + pos, bufsize - pos, "insert into sys.systemfunctions (select id from sys.functions where id < 2000 and id not in (select function_id from sys.systemfunctions));\n");

	pos += snprintf(buf + pos, bufsize - pos, "grant execute on filter function \"like\"(string, string, string) to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on filter function \"ilike\"(string, string, string) to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on filter function \"like\"(string, string) to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on filter function \"ilike\"(string, string) to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function degrees to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function radians to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on procedure times to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function str_to_date to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function date_to_str to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function str_to_time to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function time_to_str to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function str_to_timestamp to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function timestamp_to_str to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function sys.\"epoch\"(BIGINT) to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function sys.\"epoch\"(INT) to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function sys.\"epoch\"(TIMESTAMP) to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function sys.\"epoch\"(TIMESTAMP WITH TIME ZONE) to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function MS_STUFF to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function MS_TRUNC to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function MS_ROUND to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function MS_STR to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function alpha to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function zorder_encode to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function zorder_decode_x to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function zorder_decode_y to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function rejects to public;\n");
	pos += snprintf(buf + pos, bufsize - pos, "grant execute on function md5 to public;\n");

	/* 16_tracelog.sql */
	pos += snprintf(buf + pos, bufsize - pos, "drop procedure sys.profiler_openstream(string, int);\n");
	pos += snprintf(buf + pos, bufsize - pos, "drop procedure sys.profiler_stethoscope(int);\n");

	/* 25_debug.sql */
	pos += snprintf(buf + pos, bufsize - pos, "drop function sys.bbp();\n");
	pos += snprintf(buf + pos, bufsize - pos,
		"create function sys.bbp ()\n"
		"returns table (id int, name string,\n"
		"ttype string, count BIGINT, refcnt int, lrefcnt int,\n"
		"location string, heat int, dirty string,\n"
		"status string, kind string)\n"
		"external name bbp.get;\n");
	pos += snprintf(buf + pos, bufsize - pos,
		"create function sys.malfunctions()\n"
		"returns table(\"signature\" string, \"address\" string, \"comment\" string)\n"
		"external name \"manual\".\"functions\";\n");
	pos += snprintf(buf + pos, bufsize - pos,
		"create procedure sys.flush_log ()\n"
		"external name sql.\"flush_log\";\n");
	pos += snprintf(buf + pos, bufsize - pos,
		"create function sys.debug(debug int) returns integer\n"
		"external name mdb.\"setDebug\";\n");
	pos += snprintf(buf + pos, bufsize - pos,
		"insert into sys.systemfunctions (select id from sys.functions where name in ('bbp', 'malfunctions', 'flush_log', 'debug') and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n");

	/* 45_uuid.sql */
	{
		/* in previous updates, the functions
		 * sys.isauuid(string) was not created, so we can't
		 * always drop it here */
		sql_subtype tp;
		sql_find_subtype(&tp, "clob", 0, 0);
		if (sql_bind_func(sql->sa, s, "isauuid", &tp, NULL, F_FUNC))
			pos += snprintf(buf + pos, bufsize - pos,
					"drop function sys.isaUUID(string);\n");
	}
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function sys.isaUUID(uuid);\n"
			"create function sys.isaUUID(s string)\n"
			"returns boolean external name uuid.\"isaUUID\";\n"
			"insert into sys.systemfunctions (select id from sys.functions where name = 'isauuid' and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n");

	/* 46_profiler.sql */
	pos += snprintf(buf + pos, bufsize - pos,
		"create schema profiler;\n"
		"create procedure profiler.start() external name profiler.\"start\";\n"
		"create procedure profiler.stop() external name profiler.stop;\n"
		"create procedure profiler.setheartbeat(beat int) external name profiler.setheartbeat;\n"
		"create procedure profiler.setpoolsize(poolsize int) external name profiler.setpoolsize;\n"
		"create procedure profiler.setstream(host string, port int) external name profiler.setstream;\n");
	pos += snprintf(buf + pos, bufsize - pos,
		"update sys.schemas set system = true where name = 'profiler';\n"
		"insert into sys.systemfunctions (select id from sys.functions where name in ('start', 'stop', 'setheartbeat', 'setpoolsize', 'setstream') and schema_id = (select id from sys.schemas where name = 'profiler') and id not in (select function_id from sys.systemfunctions));\n");

	/* 51_sys_schema_extensions.sql */
	pos += snprintf(buf + pos, bufsize - pos,
		"delete from sys.keywords;\n"
		"insert into sys.keywords values\n"
		"('ADD'), ('ADMIN'), ('AFTER'), ('AGGREGATE'), ('ALL'), ('ALTER'), ('ALWAYS'), ('AND'), ('ANY'), ('ASC'), ('ASYMMETRIC'), ('ATOMIC'), ('AUTO_INCREMENT'),\n"
		"('BEFORE'), ('BEGIN'), ('BEST'), ('BETWEEN'), ('BIGINT'), ('BIGSERIAL'), ('BINARY'), ('BLOB'), ('BY'),\n"
		"('CALL'), ('CASCADE'), ('CASE'), ('CAST'), ('CHAIN'), ('CHAR'), ('CHARACTER'), ('CHECK'), ('CLOB'), ('COALESCE'), ('COMMIT'), ('COMMITTED'), ('CONSTRAINT'), ('CONVERT'), ('COPY'), ('CORRESPONDING'), ('CREATE'), ('CROSS'), ('CURRENT'), ('CURRENT_DATE'), ('CURRENT_ROLE'), ('CURRENT_TIME'), ('CURRENT_TIMESTAMP'), ('CURRENT_USER'),\n"
		"('DAY'), ('DEC'), ('DECIMAL'), ('DECLARE'), ('DEFAULT'), ('DELETE'), ('DELIMITERS'), ('DESC'), ('DO'), ('DOUBLE'), ('DROP'),\n"
		"('EACH'), ('EFFORT'), ('ELSE'), ('ELSEIF'), ('ENCRYPTED'), ('END'), ('ESCAPE'), ('EVERY'), ('EXCEPT'), ('EXCLUDE'), ('EXISTS'), ('EXTERNAL'), ('EXTRACT'),\n"
		"('FALSE'), ('FLOAT'), ('FOLLOWING'), ('FOR'), ('FOREIGN'), ('FROM'), ('FULL'), ('FUNCTION'),\n"
		"('GENERATED'), ('GLOBAL'), ('GRANT'), ('GROUP'),\n"
		"('HAVING'), ('HOUR'), ('HUGEINT'),\n"
		"('IDENTITY'), ('IF'), ('ILIKE'), ('IN'), ('INDEX'), ('INNER'), ('INSERT'), ('INT'), ('INTEGER'), ('INTERSECT'), ('INTO'), ('IS'), ('ISOLATION'),\n"
		"('JOIN'),\n"
		"('LEFT'), ('LIKE'), ('LIMIT'), ('LOCAL'), ('LOCALTIME'), ('LOCALTIMESTAMP'), ('LOCKED'),\n"
		"('MEDIUMINT'), ('MERGE'), ('MINUTE'), ('MONTH'),\n"
		"('NATURAL'), ('NEW'), ('NEXT'), ('NOCYCLE'), ('NOMAXVALUE'), ('NOMINVALUE'), ('NOT'), ('NOW'), ('NULL'), ('NULLIF'), ('NUMERIC'),\n"
		"('OF'), ('OFFSET'), ('OLD'), ('ON'), ('ONLY'), ('OPTION'), ('OR'), ('ORDER'), ('OTHERS'), ('OUTER'), ('OVER'),\n"
		"('PARTIAL'), ('PARTITION'), ('POSITION'), ('PRECEDING'), ('PRESERVE'), ('PRIMARY'), ('PRIVILEGES'), ('PROCEDURE'), ('PUBLIC'),\n"
		"('RANGE'), ('READ'), ('REAL'), ('RECORDS'), ('REFERENCES'), ('REFERENCING'), ('REMOTE'), ('RENAME'), ('REPEATABLE'), ('REPLICA'), ('RESTART'), ('RESTRICT'), ('RETURN'), ('RETURNS'), ('REVOKE'), ('RIGHT'), ('ROLLBACK'), ('ROWS'),\n"
		"('SAMPLE'), ('SAVEPOINT'), ('SECOND'), ('SELECT'), ('SEQUENCE'), ('SERIAL'), ('SERIALIZABLE'), ('SESSION_USER'), ('SET'), ('SIMPLE'), ('SMALLINT'), ('SOME'), ('SPLIT_PART'), ('STDIN'), ('STDOUT'), ('STORAGE'), ('STREAM'), ('STRING'), ('SUBSTRING'), ('SYMMETRIC'),\n"
		"('THEN'), ('TIES'), ('TINYINT'), ('TO'), ('TRANSACTION'), ('TRIGGER'), ('TRUE'),\n"
		"('UNBOUNDED'), ('UNCOMMITTED'), ('UNENCRYPTED'), ('UNION'), ('UNIQUE'), ('UPDATE'), ('USER'), ('USING'),\n"
		"('VALUES'), ('VARCHAR'), ('VARYING'), ('VIEW'),\n"
		"('WHEN'), ('WHERE'), ('WHILE'), ('WITH'), ('WORK'), ('WRITE'),\n"
		"('XMLAGG'), ('XMLATTRIBUTES'), ('XMLCOMMENT'), ('XMLCONCAT'), ('XMLDOCUMENT'), ('XMLELEMENT'), ('XMLFOREST'), ('XMLNAMESPACES'), ('XMLPARSE'), ('XMLPI'), ('XMLQUERY'), ('XMLSCHEMA'), ('XMLTEXT'), ('XMLVALIDATE');\n");

	// Add new dependency_type 15 to table sys.dependency_types
	pos += snprintf(buf + pos, bufsize - pos,
		"insert into sys.dependency_types (dependency_type_id, dependency_type_name)\n"
		" select 15 as id, 'TYPE' as name where 15 not in (select dependency_type_id from sys.dependency_types);\n");

	// Add 46 missing sys.dependencies rows for new dependency_type: 15
	pos += snprintf(buf + pos, bufsize - pos,
		"insert into sys.dependencies (id, depend_id, depend_type)\n"
		" select distinct types.id as type_id, args.func_id, 15 as depend_type from sys.args join sys.types on types.systemname = args.type where args.type in ('inet', 'json', 'url', 'uuid')\n"
		" except\n"
		" select distinct id, depend_id, depend_type from sys.dependencies where depend_type = 15;\n");

	// Add the new storage inspection functions.
	pos += snprintf(buf + pos, bufsize - pos,
		"create function sys.\"storage\"( sname string)\n"
		"returns table (\n"
		"    \"schema\" string,\n"
		"    \"table\" string,\n"
		"    \"column\" string,\n"
		"    \"type\" string,\n"
		"    \"mode\" string,\n"
		"    location string,\n"
		"    \"count\" bigint,\n"
		"    typewidth int,\n"
		"    columnsize bigint,\n"
		"    heapsize bigint,\n"
		"    hashes bigint,\n"
		"    phash boolean,\n"
		"    imprints bigint,\n"
		"    sorted boolean\n"
		")\n"
		"external name sql.\"storage\";\n"
		"\n"
		"create function sys.\"storage\"( sname string, tname string)\n"
		"returns table (\n"
		"    \"schema\" string,\n"
		"    \"table\" string,\n"
		"    \"column\" string,\n"
		"    \"type\" string,\n"
		"    \"mode\" string,\n"
		"    location string,\n"
		"    \"count\" bigint,\n"
		"    typewidth int,\n"
		"    columnsize bigint,\n"
		"    heapsize bigint,\n"
		"    hashes bigint,\n"
		"    phash boolean,\n"
		"    imprints bigint,\n"
		"    sorted boolean\n"
		")\n"
		"external name sql.\"storage\";\n"
		"\n"
		"create function sys.\"storage\"( sname string, tname string, cname string)\n"
		"returns table (\n"
		"    \"schema\" string,\n"
		"    \"table\" string,\n"
		"    \"column\" string,\n"
		"    \"type\" string,\n"
		"    \"mode\" string,\n"
		"    location string,\n"
		"    \"count\" bigint,\n"
		"    typewidth int,\n"
		"    columnsize bigint,\n"
		"    heapsize bigint,\n"
		"    hashes bigint,\n"
		"    phash boolean,\n"
		"    imprints bigint,\n"
		"    sorted boolean\n"
		")\n"
		"external name sql.\"storage\";\n"
	);
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select id from sys.functions where name = 'storage' and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n");

	/* change to 99_system.sql: correct invalid FK schema ids, set
	 * them to schema id 2000 (the "sys" schema) */
	pos += snprintf(buf + pos, bufsize - pos,
			"UPDATE sys.types SET schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys') WHERE schema_id = 0 AND schema_id NOT IN (SELECT id from sys.schemas);\n"
			"UPDATE sys.functions SET schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys') WHERE schema_id = 0 AND schema_id NOT IN (SELECT id from sys.schemas);\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"delete from sys.systemfunctions where function_id not in (select id from sys.functions);\n");

	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

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
		throw(SQL, "sql_update_geom", MAL_MALLOC_FAIL);
	bufsize = strlen(geomupgrade) + 512;
	buf = GDKmalloc(bufsize);
	if (buf == NULL) {
		GDKfree(geomupgrade);
		throw(SQL, "sql_update_geom", MAL_MALLOC_FAIL);
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
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('storage', 'storagemodel') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('createorderindex', 'droporderindex', 'storagemodelinit') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_PROC);
	pos += snprintf(buf + pos, bufsize - pos,
			"delete from systemfunctions where function_id not in (select id from functions);\n");

	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_nowrd(Client c, mvc *sql)
{
	size_t bufsize = 10240, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");
	sql_schema *s;

	s = mvc_bind_schema(sql, "sys");
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	{
		sql_table *t;

		if ((t = mvc_bind_table(sql, s, "querylog_calls")) != NULL)
			t->system = 0;
		if ((t = mvc_bind_table(sql, s, "querylog_history")) != NULL)
			t->system = 0;
	}

	/* 15_querylog.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop view sys.querylog_history;\n"
			"drop view sys.querylog_calls;\n"
			"drop function sys.querylog_calls();\n"
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
			"update _tables set system = true where name in ('querylog_calls', 'querylog_history') and schema_id = (select id from schemas where name = 'sys');\n");

	/* 39_analytics.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop aggregate sys.stddev_pop(wrd);\n"
			"drop aggregate sys.stddev_samp(wrd);\n"
			"drop aggregate sys.var_pop(wrd);\n"
			"drop aggregate sys.var_samp(wrd);\n"
			"drop aggregate sys.median(wrd);\n"
			"drop aggregate sys.quantile(wrd, double);\n"
			"drop aggregate sys.corr(wrd, wrd);\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('querylog_calls') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_UNION);
	pos += snprintf(buf + pos, bufsize - pos,
			"delete from systemfunctions where function_id not in (select id from functions);\n");

	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

/* older databases may have sys.median and sys.quantile aggregates on
 * decimal(1) which doesn't match plain decimal: fix those */
static str
sql_update_median(Client c, mvc *sql)
{
	char *q1 = "select id from sys.args where func_id in (select id from sys.functions where name = 'median' and schema_id = (select id from sys.schemas where name = 'sys')) and type = 'decimal' and type_digits = 1 and type_scale = 0 and number = 1;\n";
	char *q2 = "select id from sys.args where func_id in (select id from sys.functions where name = 'median' and schema_id = (select id from sys.schemas where name = 'sys')) and type = 'date' and number = 1;\n";
	size_t bufsize = 5000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");
	res_table *output;
	BAT *b;
	int needed = 0;

	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n");
	err = SQLstatementIntern(c, &q1, "update", 1, 0, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) > 0) {
			pos += snprintf(buf + pos, bufsize - pos,
					"drop aggregate median(decimal(1));\n"
					"create aggregate median(val DECIMAL) returns DECIMAL"
					" external name \"aggr\".\"median\";\n"
					"drop aggregate quantile(decimal(1), double);\n"
					"create aggregate quantile(val DECIMAL, q DOUBLE) returns DECIMAL"
					" external name \"aggr\".\"quantile\";\n");
			needed = 1;
		}
		BBPunfix(b->batCacheid);
	}
	res_tables_destroy(output);
	err = SQLstatementIntern(c, &q2, "update", 1, 0, &output);
	if (err) {
		GDKfree(buf);
		return err;
	}
	b = BATdescriptor(output->cols[0].b);
	if (b) {
		if (BATcount(b) == 0) {
			pos += snprintf(buf + pos, bufsize - pos,
					"create aggregate median(val DATE) returns DATE"
					" external name \"aggr\".\"median\";\n"
					"create aggregate median(val TIME) returns TIME"
					" external name \"aggr\".\"median\";\n"
					"create aggregate median(val TIMESTAMP) returns TIMESTAMP"
					" external name \"aggr\".\"median\";\n"
#if 0
					"create aggregate quantile(val DATE, q DOUBLE) returns DATE"
					" external name \"aggr\".\"quantile\";\n"
					"create aggregate quantile(val TIME, q DOUBLE) returns TIME"
					" external name \"aggr\".\"quantile\";\n"
					"create aggregate quantile(val TIMESTAMP, q DOUBLE) returns TIMESTAMP"
					" external name \"aggr\".\"quantile\";\n"
#endif
		);
			needed = 1;
		}
		BBPunfix(b->batCacheid);
	}
	res_tables_destroy(output);
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select id from sys.functions where name in ('median', 'quantile') and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n");
	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
	assert(pos < bufsize);
	if (needed) {
		printf("Running database upgrade commands:\n%s\n", buf);
		err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	}

	GDKfree(buf);

	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_geom_jun2016_sp2(Client c, mvc *sql)
{
	size_t bufsize = 1000000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"GRANT EXECUTE ON FUNCTION sys.Has_Z(integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.Has_M(integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.get_type(integer, integer) TO PUBLIC;\n"
			"GRANT SELECT ON sys.spatial_ref_sys TO PUBLIC;\n"
			"GRANT SELECT ON sys.geometry_columns TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.mbr(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Overlaps(mbr, mbr) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Contains(mbr, mbr) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Equals(mbr, mbr) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Distance(mbr, mbr) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_WKTToSQL(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_WKBToSQL(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_AsText(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_AsBinary(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Dimension(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_GeometryType(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_SRID(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_SetSRID(Geometry, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_IsEmpty(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_IsSimple(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Boundary(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Envelope(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Equals(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Disjoint(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Intersects(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Touches(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Crosses(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Within(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Contains(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Overlaps(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Relate(Geometry, Geometry, string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Distance(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Intersection(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Difference(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Union(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_SymDifference(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Buffer(Geometry, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_ConvexHull(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_X(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Y(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Z(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_StartPoint(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_EndPoint(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_IsRing(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Length(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_IsClosed(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_NumPoints(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_PointN(Geometry, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Centroid(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_PointOnSurface(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Area(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_ExteriorRing(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_SetExteriorRing(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_NumInteriorRing(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_InteriorRingN(Geometry, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_InteriorRings(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_NumGeometries(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_GeometryN(Geometry, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_NumPatches(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_PatchN(Geometry, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_GeomFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_PointFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_LineFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_PolygonFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MPointFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MLineFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MPolyFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_GeomCollFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_BdPolyFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_BdMPolyFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_GeometryFromText(string, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_GeomFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_GeometryFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_PointFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_LineFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_PolygonFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MPointFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MLineFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MPolyFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_GeomCollFromText(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakePoint(double, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Point(double, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakePoint(double, double, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakePoint(double, double, double, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakePointM(double, double, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakeLine(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakeEnvelope(double, double, double, double, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakeEnvelope(double, double, double, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakePolygon(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Polygon(Geometry, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_MakeBox2D(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.GeometryType(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_CoordDim(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_IsValid(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_IsValidReason(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_NPoints(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_NRings(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_NumInteriorRings(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_XMax(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_XMax(mbr) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_XMin(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_XMin(mbr) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_YMax(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_YMax(mbr) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_YMin(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_YMin(mbr) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Force2D(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Force3D(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Segmentize(Geometry, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getProj4(integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.InternalTransform(Geometry, integer, integer, string, string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Transform(Geometry, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Translate(Geometry, double, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Translate(Geometry, double, double, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_AsEWKT(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Covers(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_CoveredBy(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_DWithin(Geometry, Geometry, double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Length2D(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Collect(Geometry, Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_DelaunayTriangles(Geometry, double, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_Dump(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.ST_DumpPoints(Geometry) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.Contains(Geometry, double, double) TO PUBLIC;\n");

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);

	assert(pos < bufsize);
	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jun2016_sp2(Client c, mvc *sql)
{
	size_t bufsize = 1000000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	char *schema = stack_get_string(sql, "current_schema");

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"GRANT EXECUTE ON FUNCTION sys.getAnchor(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getBasename(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getContent(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getContext(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getDomain(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getExtension(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getFile(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getHost(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getPort(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getProtocol(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getQuery(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getUser(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.getRobotURL(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.isaURL(url) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.newurl(STRING, STRING, INT, STRING) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.newurl(STRING, STRING, STRING) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"broadcast\"(inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"host\"(inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"masklen\"(inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"setmasklen\"(inet, int) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"netmask\"(inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"hostmask\"(inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"network\"(inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"text\"(inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"abbrev\"(inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"left_shift\"(inet, inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"right_shift\"(inet, inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"left_shift_assign\"(inet, inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.\"right_shift_assign\"(inet, inet) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(TINYINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(INTEGER) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(BIGINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(REAL) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(DATE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(TIME) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(TIMESTAMP) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(TINYINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(INTEGER) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(BIGINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(REAL) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(DATE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(TIME) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(TIMESTAMP) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(TINYINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(INTEGER) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(BIGINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(REAL) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(DATE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(TIME) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_samp(TIMESTAMP) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(TINYINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(INTEGER) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(BIGINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(REAL) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(DATE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(TIME) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.var_pop(TIMESTAMP) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(TINYINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(INTEGER) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(BIGINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(DECIMAL) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(REAL) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(DATE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(TIME) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.median(TIMESTAMP) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(TINYINT, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(SMALLINT, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(INTEGER, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(BIGINT, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(DECIMAL, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(REAL, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(DOUBLE, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(DATE, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(TIME, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.quantile(TIMESTAMP, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.corr(TINYINT, TINYINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.corr(SMALLINT, SMALLINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.corr(INTEGER, INTEGER) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.corr(BIGINT, BIGINT) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.corr(REAL, REAL) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE sys.corr(DOUBLE, DOUBLE) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.filter(json, string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.filter(json, tinyint) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.filter(json, integer) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.filter(json, bigint) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.text(json, string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.number(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.\"integer\"(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.isvalid(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.isobject(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.isarray(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.isvalid(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.isobject(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.isarray(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.length(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.keyarray(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.valuearray(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.text(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.text(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION json.text(int) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE json.output(json) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE json.tojsonarray(string) TO PUBLIC;\n"
			"GRANT EXECUTE ON AGGREGATE json.tojsonarray(double) TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.uuid() TO PUBLIC;\n"
			"GRANT EXECUTE ON FUNCTION sys.isaUUID(string) TO PUBLIC;\n");
#ifdef HAVE_HGE
	if (have_hge) {
		pos += snprintf(buf + pos, bufsize - pos,
				"GRANT EXECUTE ON AGGREGATE sys.stddev_samp(HUGEINT) TO PUBLIC;\n"
				"GRANT EXECUTE ON AGGREGATE sys.stddev_pop(HUGEINT) TO PUBLIC;\n"
				"GRANT EXECUTE ON AGGREGATE sys.var_samp(HUGEINT) TO PUBLIC;\n"
				"GRANT EXECUTE ON AGGREGATE sys.var_pop(HUGEINT) TO PUBLIC;\n"
				"GRANT EXECUTE ON AGGREGATE sys.median(HUGEINT) TO PUBLIC;\n"
				"GRANT EXECUTE ON AGGREGATE sys.quantile(HUGEINT, DOUBLE) TO PUBLIC;\n"
				"GRANT EXECUTE ON AGGREGATE sys.corr(HUGEINT, HUGEINT) TO PUBLIC;\n"
				"GRANT EXECUTE ON FUNCTION json.filter(json, hugeint) TO PUBLIC;\n");
	}
#endif

	if (schema)
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);

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

	pos += snprintf(buf + pos, bufsize - pos, 
			"set schema \"sys\";\n"
			"drop procedure sys.settimeout(bigint);\n"
			"drop procedure sys.settimeout(bigint,bigint);\n"
			"drop procedure sys.setsession(bigint);\n"
			"create procedure sys.settimeout(\"query\" bigint) external name clients.settimeout;\n"
			"create procedure sys.settimeout(\"query\" bigint, \"session\" bigint) external name clients.settimeout;\n"
			"create procedure sys.setsession(\"timeout\" bigint) external name clients.setsession;\n"
			"insert into sys.systemfunctions (select id from sys.functions where name in ('settimeout', 'setsession') and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n"
			"delete from systemfunctions where function_id not in (select id from functions);\n");
	if (schema) 
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
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

	/* if function sys.md5(str) does not exist, we need to
	 * update */
	sql_find_subtype(&tp, "clob", 0, 0);
	if (!sql_bind_func(m->sa, s, "md5", &tp, NULL, F_FUNC)) {
		if ((err = sql_update_oct2014(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}
	/* if table returning function sys.environment() does not
	 * exist, we need to update from oct2014->sp1 */
	if (!sql_bind_func(m->sa, s, "environment", NULL, NULL, F_UNION)) {
		if ((err = sql_update_oct2014_sp1(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}
	/* if sys.tablestoragemodel.auxillary exists, we need
	 * to update (note, the proper spelling is auxiliary) */
	if (mvc_bind_column(m, mvc_bind_table(m, s, "tablestoragemodel"), "auxillary")) {
		if ((err = sql_update_oct2014_sp2(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	/* if function sys.<<(inet,inet) does not exist, we need to
	 * update */
	sql_init_subtype(&tp, find_sql_type(s, "inet"), 0, 0);
	if (!sql_bind_func(m->sa, s, "left_shift", &tp, &tp, F_FUNC)) {
		if ((err = sql_update_oct2014_sp3(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

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

	/* add missing features needed beyond Oct 2014 */
	sql_find_subtype(&tp, "clob", 0, 0);
	if (!sql_bind_func(m->sa, s, "like", &tp, &tp, F_FILT)) {
		if ((err = sql_update_jul2015(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	/* add missing epoch functions */
	if ((err = sql_update_epoch(c, m)) != NULL) {
		fprintf(stderr, "!%s\n", err);
		freeException(err);
	}

	sql_find_subtype(&tp, "clob", 0, 0);
	if (!sql_bind_func(m->sa, s, "storage", &tp, NULL, F_UNION)) {
		if ((err = sql_update_jun2016(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

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

	if ((err = sql_update_median(c, m)) != NULL) {
		fprintf(stderr, "!%s\n", err);
		freeException(err);
	}

	if (sql_find_subtype(&tp, "geometry", 0, 0) &&
	    (f = sql_bind_func(m->sa, s, "mbr", &tp, NULL, F_FUNC)) != NULL &&
	    sql_privilege(m, ROLE_PUBLIC, f->func->base.id, PRIV_EXECUTE, 0) != PRIV_EXECUTE) {
		if ((err = sql_update_geom_jun2016_sp2(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	if ((f = sql_bind_func(m->sa, s, "uuid", NULL, NULL, F_FUNC)) != NULL &&
	    sql_privilege(m, ROLE_PUBLIC, f->func->base.id, PRIV_EXECUTE, 0) != PRIV_EXECUTE) {
		if ((err = sql_update_jun2016_sp2(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	sql_find_subtype(&tp, "clob", 0, 0);
	if (!sql_bind_func3(m->sa, s, "createorderindex", &tp, &tp, &tp, F_PROC)) {
		if ((err = sql_update_dec2016(c, m)) != NULL) {
			fprintf(stderr, "!%s\n", err);
			freeException(err);
		}
	}

	sql_find_subtype(&tp, "wrd", 0, 0);
	if (sql_bind_func(m->sa, s, "median", &tp, NULL, F_AGGR)) {
		if ((err = sql_update_nowrd(c, m)) != NULL) {
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
}
