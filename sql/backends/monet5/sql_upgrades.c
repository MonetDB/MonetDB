/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

static str
sql_update_feb2013(Client c)
{
	char *buf = GDKmalloc(4096), *err = NULL;
	size_t bufsize = 4096, pos = 0;

	/* sys.stddev_samp functions */
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.stddev_samp(val TINYINT) returns DOUBLE external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_samp(val SMALLINT) returns DOUBLE external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_samp(val INTEGER) returns DOUBLE external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_samp(val BIGINT) returns DOUBLE external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_samp(val REAL) returns DOUBLE external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_samp(val DOUBLE) returns DOUBLE external name \"aggr\".\"stdev\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.stddev_samp(val DATE) returns DOUBLE external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_samp(val TIME) returns DOUBLE external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_samp(val TIMESTAMP) returns DOUBLE external name \"aggr\".\"stdev\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.stddev_pop(val TINYINT) returns DOUBLE external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.stddev_pop(val SMALLINT) returns DOUBLE external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.stddev_pop(val INTEGER) returns DOUBLE external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.stddev_pop(val BIGINT) returns DOUBLE external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.stddev_pop(val REAL) returns DOUBLE external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.stddev_pop(val DOUBLE) returns DOUBLE external name \"aggr\".\"stdevp\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.stddev_pop(val DATE) returns DOUBLE external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.stddev_pop(val TIME) returns DOUBLE external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.stddev_pop(val TIMESTAMP) returns DOUBLE external name \"aggr\".\"stdevp\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.var_samp(val TINYINT) returns DOUBLE external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_samp(val SMALLINT) returns DOUBLE external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_samp(val INTEGER) returns DOUBLE external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_samp(val BIGINT) returns DOUBLE external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_samp(val REAL) returns DOUBLE external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_samp(val DOUBLE) returns DOUBLE external name \"aggr\".\"variance\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.var_samp(val DATE) returns DOUBLE external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_samp(val TIME) returns DOUBLE external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_samp(val TIMESTAMP) returns DOUBLE external name \"aggr\".\"variance\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.var_pop(val TINYINT) returns DOUBLE external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.var_pop(val SMALLINT) returns DOUBLE external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.var_pop(val INTEGER) returns DOUBLE external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.var_pop(val BIGINT) returns DOUBLE external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.var_pop(val REAL) returns DOUBLE external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.var_pop(val DOUBLE) returns DOUBLE external name \"aggr\".\"variancep\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.var_pop(val DATE) returns DOUBLE external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.var_pop(val TIME) returns DOUBLE external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.var_pop(val TIMESTAMP) returns DOUBLE external name \"aggr\".\"variancep\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('stddev_samp', 'stddev_pop', 'var_samp', 'var_pop') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_AGGR);

	assert(pos < 4096);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}


static str
sql_update_feb2013_sp1(Client c)
{
	size_t bufsize = 10240, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;

	/* sys.stddev functions */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop filter function sys.\"like\"(string, string, string);\n"
			"drop filter function sys.\"ilike\"(string, string, string);\n"
			"create filter function sys.\"like\"(val string, pat string, esc string) external name algebra.likesubselect;\n"
			"create filter function sys.\"ilike\"(val string, pat string, esc string) external name algebra.ilikesubselect;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"drop function sys.storage;\n");

	/* 75_storagemodel.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.storage()\n"
			"returns table (\"schema\" string, \"table\" string, \"column\" string, \"type\" string, location string, \"count\" bigint, typewidth int, columnsize bigint, heapsize bigint, indices bigint, sorted boolean)\n"
			"external name sql.storage;\n"

			"create table sys.storagemodelinput(\n"
			"  \"schema\" string,\n"
			"  \"table\" string,\n"
			"  \"column\" string,\n"
			"  \"type\" string,\n"
			"  \"typewidth\" int,\n"
			"  \"count\" bigint,\n"
			"  \"distinct\" bigint,\n"
			"  \"atomwidth\" int,\n"
			"  \"reference\" boolean,\n"
			"  \"sorted\" boolean\n"
			");\n"
			"update sys._tables\n"
			"  set system = true\n"
			"  where name = 'storagemodelinput'\n"
			"    and schema_id = (select id from sys.schemas where name = 'sys');\n"

			"create procedure sys.storagemodelinit()\n"
			"begin\n"
			"  delete from sys.storagemodelinput;\n"
			"  insert into sys.storagemodelinput\n"
			"  select X.\"schema\", X.\"table\", X.\"column\", X.\"type\", X.typewidth, X.count, 0, X.typewidth, false, X.sorted from sys.storage() X;\n"
			"  update sys.storagemodelinput\n"
			"  set reference = true\n"
			"  where concat(concat(\"schema\",\"table\"), \"column\") in (\n"
			"    SELECT concat( concat(\"fkschema\".\"name\", \"fktable\".\"name\"), \"fkkeycol\".\"name\" )\n"
			"    FROM  \"sys\".\"keys\" AS \"fkkey\",\n"
			"        \"sys\".\"objects\" AS \"fkkeycol\",\n"
			"        \"sys\".\"tables\" AS \"fktable\",\n"
			"        \"sys\".\"schemas\" AS \"fkschema\"\n"
			"    WHERE   \"fktable\".\"id\" = \"fkkey\".\"table_id\"\n"
			"      AND \"fkkey\".\"id\" = \"fkkeycol\".\"id\"\n"
			"      AND \"fkschema\".\"id\" = \"fktable\".\"schema_id\"\n"
			"      AND \"fkkey\".\"rkey\" > -1);\n"
			"  update sys.storagemodelinput\n"
			"  set \"distinct\" = \"count\"\n"
			"  where \"type\" = 'varchar' or \"type\"='clob';\n"
			"end;\n"

			"create function sys.columnsize(nme string, i bigint, d bigint)\n"
			"returns bigint\n"
			"begin\n"
			"  case\n"
			"  when nme = 'boolean' then return i;\n"
			"  when nme = 'char' then return 2*i;\n"
			"  when nme = 'smallint' then return 2 * i;\n"
			"  when nme = 'int'	 then return 4 * i;\n"
			"  when nme = 'bigint'	 then return 8 * i;\n"
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

			"create function sys.heapsize(tpe string, i bigint, w int)\n"
			"returns bigint\n"
			"begin\n"
			"  if  tpe <> 'varchar' and tpe <> 'clob'\n"
			"  then\n"
			"    return 0;\n"
			"  end if;\n"
			"  return 10240 + i * w;\n"
			"end;\n"

			"create function sys.indexsize(b boolean, i bigint)\n"
			"returns bigint\n"
			"begin\n"

			"  if  b = true\n"
			"  then\n"
			"    return 8 * i;\n"
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
			"  indices bigint,\n"
			"  sorted boolean)\n"
			"begin\n"
			"  return select I.\"schema\", I.\"table\", I.\"column\", I.\"type\", I.\"count\",\n"
			"  columnsize(I.\"type\", I.count, I.\"distinct\"),\n"
			"  heapsize(I.\"type\", I.\"distinct\", I.\"atomwidth\"),\n"
			"  indexsize(I.\"reference\", I.\"count\"),\n"
			"  I.sorted\n"
			"  from sys.storagemodelinput I;\n"
			"end;\n"

			"create view sys.tablestoragemodel\n"
			"as select \"schema\",\"table\",max(count) as \"count\",\n"
			"  sum(columnsize) as columnsize,\n"
			"  sum(heapsize) as heapsize,\n"
			"  sum(indices) as indices,\n"
			"  sum(case when sorted = false then 8 * count else 0 end) as auxiliary\n"
			"from sys.storagemodel() group by \"schema\",\"table\";\n"
			"update sys._tables\n"
			"  set system = true\n"
			"  where name = 'tablestoragemodel'\n"
			"    and schema_id = (select id from sys.schemas where name = 'sys');\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('like', 'ilike') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_FILT);
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('storage', 'columnsize', 'heapsize', 'indexsize', 'storagemodel') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name = 'storagemodelinit' and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_PROC);

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_feb2013_sp3(Client c)
{
	size_t bufsize = 4096, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;

	/* aggregates on type WRD */
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate sys.stddev_samp(val WRD) returns DOUBLE external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_pop(val WRD) returns DOUBLE external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.var_samp(val WRD) returns DOUBLE external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_pop(val WRD) returns DOUBLE external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.median(val WRD) returns WRD external name \"aggr\".\"median\";\n"
			"create aggregate sys.corr(e1 WRD, e2 WRD) returns WRD external name \"aggr\".\"corr\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('stddev_samp', 'stddev_pop', 'var_samp', 'var_pop', 'median', 'corr') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_AGGR);

	assert(pos < 4096);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_jan2014(Client c)
{
	size_t bufsize = 25000, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	ValRecord *schvar = stack_get_var(((backend *) c->sqlcontext)->mvc, "current_schema");
	char *schema = NULL;

	if (schvar)
		schema = strdup(schvar->val.sval);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	/* replaced 15_history.sql by 15_querylog.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop procedure sys.resetHistory;\n"
			"drop procedure sys.keepCall;\n"
			"drop procedure sys.keepQuery;\n");
	{
		char *msg;
		mvc *sql = NULL;

		if ((msg = getSQLContext(c, c->curprg->def, &sql, NULL)) != MAL_SUCCEED) {
			GDKfree(msg);
		} else {
			sql_schema *s;

			if ((s = mvc_bind_schema(sql, "sys")) != NULL) {
				sql_table *t;

				if ((t = mvc_bind_table(sql, s, "querylog")) != NULL)
					t->system = 0;
				if ((t = mvc_bind_table(sql, s, "callhistory")) != NULL)
					t->system = 0;
				if ((t = mvc_bind_table(sql, s, "queryhistory")) != NULL)
					t->system = 0;
			}
		}
	}
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = false where name in ('callhistory','queryhistory','querylog') and schema_id = (select id from sys.schemas where name = 'sys');\n"
			"drop view sys.queryLog;\n"
			"drop table sys.callHistory;\n"
			"drop table sys.queryHistory;\n"
			"create function sys.querylog_catalog()\n"
			"returns table(\n"
			"  id oid,\n"
			"  owner string,\n"
			"  defined timestamp,\n"
			"  query string,\n"
			"  pipe string,\n"
			"  mal int,		-- size of MAL plan\n"
			"  optimize bigint	-- time in usec\n"
			")\n"
			"external name sql.querylog_catalog;\n"

			"create function sys.querylog_calls()\n"
			"returns table(\n"
			"  id oid,			-- references query plan\n"
			"  \"start\" timestamp,	-- time the statement was started\n"
			"  \"stop\" timestamp,	-- time the statement was completely finished\n"
			"  arguments string,	-- actual call structure\n"
			"  tuples wrd,		-- number of tuples in the result set\n"
			"  run bigint,		-- time spent (in usec)  until the result export\n"
			"  ship bigint,		-- time spent (in usec)  to ship the result set\n"
			"  cpu int,		-- average cpu load percentage during execution\n"
			"  io int,			-- percentage time waiting for IO to finish\n"
			"  space bigint		-- total storage size of intermediates created (in MB)\n"
			")\n"
			"external name sql.querylog_calls;\n"

			"create view sys.querylog_catalog as select * from sys.querylog_catalog();\n"
			"create view sys.querylog_calls as select * from sys.querylog_calls();\n"
			"create view sys.querylog_history as\n"
			"select qd.*, ql.\"start\",ql.\"stop\", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.space, ql.io\n"
			"from sys.querylog_catalog() qd, sys.querylog_calls() ql\n"
			"where qd.id = ql.id and qd.owner = user;\n"

			"update sys._tables\n"
			"    set system = true\n"
			"    where name in ('querylog_history', 'querylog_calls', 'querylog_catalog')\n"
			"        and schema_id = (select id from sys.schemas where name = 'sys');\n"

			"create procedure sys.querylog_empty()\n"
			"external name sql.querylog_empty;\n"

			"create procedure sys.querylog_enable()\n"
			"external name sql.querylog_enable;\n"
			"create procedure sys.querylog_enable(threshold smallint)\n"
			"external name sql.querylog_enable_threshold;\n"
			"create procedure sys.querylog_disable()\n"
			"external name sql.querylog_disable;\n");

	/* new entry in 16_tracelog.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create view sys.tracelog as select * from sys.tracelog();\n");

	/* deleted entry from and new entries in 22_clients.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop function sys.clients;\n"
			"create function sys.sessions() returns table(\"user\" string, \"login\" timestamp, \"sessiontimeout\" bigint, \"lastcommand\" timestamp, \"querytimeout\" bigint, \"active\" bool) external name sql.sessions;\n"
			"create view sys.sessions as select * from sys.sessions();\n"
			"create procedure sys.shutdown(delay tinyint) external name sql.shutdown;\n"
			"create procedure sys.shutdown(delay tinyint, force bool) external name sql.shutdown;\n"
			"create procedure sys.settimeout(\"query\" bigint) external name sql.settimeout;\n"
			"create procedure sys.settimeout(\"query\" bigint, \"session\" bigint) external name sql.settimeout;\n"
			"create procedure sys.setsession(\"timeout\" bigint) external name sql.setsession;\n");

	/* added entry in 25_debug.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create view sys.optimizers as select * from sys.optimizers();\n"
			"create view sys.environment as select * from sys.environment();\n");

	/* new file 26_sysmon.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.queue()\n"
			"returns table(\n"
			"  qtag bigint,\n"
			"  \"user\" string,\n"
			"  started timestamp,\n"
			"  estimate timestamp,\n"
			"  progress int,\n"
			"  status string,\n"
			"  tag oid,\n"
			"  query string\n"
			")\n"
			"external name sql.sysmon_queue;\n"

			"create view sys.queue as select * from sys.queue();\n"
			"update sys._tables\n"
			"    set system = true\n"
			"    where name = 'queue'\n"
			"        and schema_id = (select id from sys.schemas where name = 'sys');\n"

			"create procedure sys.pause(tag int)\n"
			"external name sql.sysmon_pause;\n"
			"create procedure sys.resume(tag int)\n"
			"external name sql.sysmon_resume;\n"
			"create procedure sys.stop(tag int)\n"
			"external name sql.sysmon_stop;\n"

			"create procedure sys.pause(tag bigint)\n"
			"external name sql.sysmon_pause;\n"
			"create procedure sys.resume(tag bigint)\n"
			"external name sql.sysmon_resume;\n"
			"create procedure sys.stop(tag bigint)\n"
			"external name sql.sysmon_stop;\n");

	/* new entries in 39_analytics.sql for quantiles and one
	 * previously missing median */
	pos += snprintf(buf + pos, bufsize - pos,
			"create aggregate quantile(val TINYINT, q DOUBLE) returns TINYINT external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val SMALLINT, q DOUBLE) returns SMALLINT external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val INTEGER, q DOUBLE) returns INTEGER external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val WRD, q DOUBLE) returns WRD external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val BIGINT, q DOUBLE) returns BIGINT external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val DECIMAL, q DOUBLE) returns DECIMAL external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val REAL, q DOUBLE) returns REAL external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val DOUBLE, q DOUBLE) returns DOUBLE external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val DATE, q DOUBLE) returns DATE external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val TIME, q DOUBLE) returns TIME external name \"aggr\".\"quantile\";\n"
			"create aggregate quantile(val TIMESTAMP, q DOUBLE) returns TIMESTAMP external name \"aggr\".\"quantile\";\n"
			"create aggregate median(val DECIMAL) returns DECIMAL external name \"aggr\".\"median\";\n");

	/* added entry in 75_storagemodel.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"create view sys.storage as select * from sys.storage();\n"
			"create view sys.storagemodel as select * from sys.storagemodel();\n");

	/* new file 80_statistics.sql */
	pos += snprintf(buf + pos, bufsize - pos,
			"CREATE TABLE sys.statistics(\n"
			"  \"schema\" string,\n"
			"  \"table\" string,\n"
			"  \"column\" string,\n"
			"  \"type\" string,\n"
			"  width integer,\n"
			"  stamp timestamp,\n"
			"  \"sample\" bigint,\n"
			"  \"count\" bigint,\n"
			"  \"unique\" bigint,\n"
			"  \"nils\" bigint,\n"
			"  minval string,\n"
			"  maxval string,\n"
			"  sorted boolean);\n"

			"update sys._tables\n"
			"  set system = true\n"
			"  where name = 'statistics'\n"
			"    and schema_id = (select id from sys.schemas where name = 'sys');\n"

			"create procedure analyze()\n"
			"external name sql.analyze;\n"

			"create procedure analyze(tbl string)\n"
			"external name sql.analyze;\n"

			"create procedure analyze(sch string, tbl string)\n"
			"external name sql.analyze;\n"

			"create procedure analyze(sch string, tbl string, col string)\n"
			"external name sql.analyze;\n"

			"create procedure analyze(\"sample\" bigint)\n"
			"external name sql.analyze;\n"

			"create procedure analyze(tbl string, \"sample\" bigint)\n"
			"external name sql.analyze;\n"

			"create procedure analyze(sch string, tbl string, \"sample\" bigint)\n"
			"external name sql.analyze;\n"

			"create procedure analyze(sch string, tbl string, col string, \"sample\" bigint)\n"
			"external name sql.analyze;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('json_filter', 'json_filter_all', 'json_isvalid', 'json_isvalidarray', 'json_isvalidobject', 'json_length', 'json_path', 'json_text', 'querylog_calls', 'querylog_catalog', 'queue', 'sessions') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_FUNC);
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('analyze', 'pause', 'querylog_disable', 'querylog_empty', 'querylog_enable', 'resume', 'setsession', 'settimeout', 'shutdown', 'stop', 'sysmon_resume') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n",
			F_PROC);
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select f.id from sys.functions f, sys.schemas s where f.name in ('quantile', 'median') and f.type = %d and f.schema_id = s.id and s.name = 'sys');\n", F_AGGR);

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys._tables set system = true where name in ('environment', 'optimizers', 'queue', 'sessions', 'statistics', 'storage', 'storagemodel', 'tracelog') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	/* 17_compress script has been removed */
	pos += snprintf(buf + pos, bufsize - pos,
			"drop procedure gzcompress;\n"
			"drop procedure gzdecompress;\n"
			"drop procedure gztruncate;\n"
			"drop procedure gzexpand;\n");

	if (schema) {
		pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"%s\";\n", schema);
		free(schema);
	}

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

/* Because of a difference of computing hash values for single vs bulk operators we need to drop and recreate all constraints/indices */
static str
sql_update_oct2014_2(Client c)
{
	mvc *sql = ((backend*) c->sqlcontext)->mvc;
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
sql_update_oct2014(Client c)
{
	size_t bufsize = 8192*2, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	mvc *sql = ((backend*) c->sqlcontext)->mvc;
	ValRecord *schvar = stack_get_var(sql, "current_schema");
	char *schema = NULL;
	sql_table *t;
	sql_schema *s;

	if (schvar)
		schema = strdup(schvar->val.sval);

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

	if (schema) {
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		free(schema);
	}

	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	if (err == MAL_SUCCEED)
		return sql_update_oct2014_2(c);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2014_sp1(Client c)
{
	size_t bufsize = 8192*2, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	mvc *sql = ((backend*) c->sqlcontext)->mvc;
	ValRecord *schvar = stack_get_var(sql, "current_schema");
	char *schema = NULL;

	if (schvar)
		schema = strdup(schvar->val.sval);

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

	if (schema) {
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		free(schema);
	}
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2014_sp2(Client c)
{
	size_t bufsize = 8192, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	mvc *sql = ((backend*) c->sqlcontext)->mvc;
	ValRecord *schvar = stack_get_var(sql, "current_schema");
	char *schema = NULL;

	if (schvar)
		schema = strdup(schvar->val.sval);

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

	if (schema) {
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		free(schema);
	}
	assert(pos < bufsize);

	{
		char *msg;
		mvc *sql = NULL;

		if ((msg = getSQLContext(c, c->curprg->def, &sql, NULL)) != MAL_SUCCEED) {
			GDKfree(msg);
		} else {
			sql_schema *s;

			if ((s = mvc_bind_schema(sql, "sys")) != NULL) {
				sql_table *t;

				if ((t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
					t->system = 0;
			}
		}
	}

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

static str
sql_update_oct2014_sp3(Client c)
{
	size_t bufsize = 8192, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	mvc *sql = ((backend*) c->sqlcontext)->mvc;
	ValRecord *schvar = stack_get_var(sql, "current_schema");
	char *schema = NULL;

	if (schvar)
		schema = strdup(schvar->val.sval);

	pos += snprintf(buf + pos, bufsize - pos,
			"set schema \"sys\";\n"
			"CREATE FUNCTION \"left_shift\"(i1 inet, i2 inet) RETURNS boolean EXTERNAL NAME inet.\"<<\";\n"
			"CREATE FUNCTION \"right_shift\"(i1 inet, i2 inet) RETURNS boolean EXTERNAL NAME inet.\">>\";\n"
			"CREATE FUNCTION \"left_shift_assign\"(i1 inet, i2 inet) RETURNS boolean EXTERNAL NAME inet.\"<<=\";\n"
			"CREATE FUNCTION \"right_shift_assign\"(i1 inet, i2 inet) RETURNS boolean EXTERNAL NAME inet.\">>=\";\n"
			"insert into sys.systemfunctions (select id from sys.functions where name in ('left_shift', 'right_shift', 'left_shift_assign', 'right_shift_assign') and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n");

	if (schema) {
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		free(schema);
	}
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#ifdef HAVE_HGE
static str
sql_update_hugeint(Client c)
{
	size_t bufsize = 8192, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	mvc *sql = ((backend*) c->sqlcontext)->mvc;
	ValRecord *schvar = stack_get_var(sql, "current_schema");
	char *schema = NULL;

	if (schvar)
		schema = strdup(schvar->val.sval);

	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"create function sys.fuse(one bigint, two bigint)\n"
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
			"create aggregate sys.stddev_samp(val HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"stdev\";\n"
			"create aggregate sys.stddev_pop(val HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"stdevp\";\n"
			"create aggregate sys.var_samp(val HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"variance\";\n"
			"create aggregate sys.var_pop(val HUGEINT) returns DOUBLE\n"
			"    external name \"aggr\".\"variancep\";\n"
			"create aggregate sys.median(val HUGEINT) returns HUGEINT\n"
			"    external name \"aggr\".\"median\";\n"
			"create aggregate sys.quantile(val HUGEINT, q DOUBLE) returns HUGEINT\n"
			"    external name \"aggr\".\"quantile\";\n"
			"create aggregate sys.corr(e1 HUGEINT, e2 HUGEINT) returns HUGEINT\n"
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

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.types values(%d, 'hge', 'hugeint', 128, 1, 2, 6, 0);\n", store_next_oid());
	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.types values(%d, 'hge', 'decimal', 39, 1, 10, 8, 0);\n", store_next_oid());
	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.types set digits = 18 where systemname = 'lng' and sqlname = 'decimal';\n");

	{
		char *msg;
		mvc *sql = NULL;

		if ((msg = getSQLContext(c, c->curprg->def, &sql, NULL)) != MAL_SUCCEED) {
			GDKfree(msg);
		} else {
			sql_schema *s;

			if ((s = mvc_bind_schema(sql, "sys")) != NULL) {
				sql_table *t;

				if ((t = mvc_bind_table(sql, s, "tablestoragemodel")) != NULL)
					t->system = 0;
			}
		}
	}

	if (schema) {
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		free(schema);
	}
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

static str
sql_update_jul2015(Client c)
{
	size_t bufsize = 15360, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	mvc *sql = ((backend*) c->sqlcontext)->mvc;
	ValRecord *schvar = stack_get_var(sql, "current_schema");
	char *schema = NULL;

	if (schvar)
		schema = strdup(schvar->val.sval);
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
			"  (0, 'TABLE'), (1, 'VIEW'), /* (2, 'GENERATED'), */ (3, 'MERGE TABLE'), (4, 'STREAM TABLE'), (5, 'REMOTE TABLE'), (6, 'REPLICA TABLE'),\n"
			"  (10, 'SYSTEM TABLE'), (11, 'SYSTEM VIEW'),\n"
			"  (20, 'GLOBAL TEMPORARY TABLE'),\n"
			"  (30, 'LOCAL TEMPORARY TABLE');\n"

			"CREATE TABLE sys.dependency_types (\n"
			"    dependency_type_id   SMALLINT NOT NULL PRIMARY KEY,\n"
			"    dependency_type_name VARCHAR(15) NOT NULL UNIQUE);\n"

			"INSERT INTO sys.dependency_types (dependency_type_id, dependency_type_name) VALUES\n"
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
			"  sorted boolean\n"
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
			"  sorted boolean)\n"
			"begin\n"
			"  return select I.\"schema\", I.\"table\", I.\"column\", I.\"type\", I.\"count\",\n"
			"  columnsize(I.\"type\", I.count, I.\"distinct\"),\n"
			"  heapsize(I.\"type\", I.\"distinct\", I.\"atomwidth\"),\n"
			"  hashsize(I.\"reference\", I.\"count\"),\n"
			"  imprintsize(I.\"count\",I.\"type\"),\n"
			"  I.sorted\n"
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

	pos += snprintf(buf + pos, bufsize - pos,
			"insert into sys.systemfunctions (select id from sys.functions where name in ('analyze', 'clearrejects', 'columnsize', 'epoch', 'ilike', 'imprintsize', 'like', 'profiler_openstream', 'profiler_stethoscope', 'querylog_calls', 'querylog_catalog', 'rejects', 'storage', 'storagemodel', 'storagemodelinit', 'str_to_time', 'str_to_timestamp', 'timestamp_to_str', 'time_to_str', 'tracelog') and schema_id = (select id from sys.schemas where name = 'sys') and id not in (select function_id from sys.systemfunctions));\n"
			"delete from systemfunctions where function_id not in (select id from functions);\n"
			"update sys._tables set system = true where name in ('dependency_types', 'keywords', 'querylog_calls', 'querylog_catalog', 'querylog_history', 'rejects', 'statistics', 'storage', 'storagemodel', 'tables', 'tablestoragemodel', 'table_types', 'tracelog') and schema_id = (select id from sys.schemas where name = 'sys');\n");

	{
		char *msg;
		mvc *sql = NULL;

		if ((msg = getSQLContext(c, c->curprg->def, &sql, NULL)) != MAL_SUCCEED) {
			GDKfree(msg);
		} else {
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
	}

	if (schema) {
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		free(schema);
	}
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}

#if 0
static str
sql_update_dec2015(Client c)
{
	size_t bufsize = 10240, pos = 0;
	char *buf = GDKmalloc(bufsize), *err = NULL;
	mvc *sql = ((backend*) c->sqlcontext)->mvc;
	ValRecord *schvar = stack_get_var(sql, "current_schema");
	char *schema = NULL;

	if (schvar)
		schema = strdup(schvar->val.sval);
	pos += snprintf(buf + pos, bufsize - pos, "set schema \"sys\";\n");

/* insert upgrade code here */
	pos += snprintf(buf + pos, bufsize - pos, "drop procedure profiler_openstream(host string, port int);");
	pos += snprintf(buf + pos, bufsize - pos, "drop procedure profiler_stethoscope(ticks int);");
	pos += snprintf(buf + pos, bufsize - pos, "create schema profiler;"
		"create procedure profiler.start() external name profiler.\"start\";"
		"create procedure profiler.stop() external name profiler.stop;"
		"create procedure profiler.setheartbeat(beat int) external name profiler.setheartbeat;"
		"create procedure profiler.setpoolsize(poolsize int) external name profiler.setpoolsize;"
		"create procedure profiler.setstream(host string, port int) external name profiler.setstream;");

			if (schema) {
		pos += snprintf(buf + pos, bufsize - pos, "set schema \"%s\";\n", schema);
		free(schema);
	}
	assert(pos < bufsize);

	printf("Running database upgrade commands:\n%s\n", buf);
	err = SQLstatementIntern(c, &buf, "update", 1, 0, NULL);
	GDKfree(buf);
	return err;		/* usually MAL_SUCCEED */
}
#endif

void
SQLupgrades(Client c, mvc *m)
{
	sql_subtype tp;
	char *err;

	/* if aggregate function sys.stddev_samp(int) does not
	 * exist, we need to update */
	sql_find_subtype(&tp, "int", 0, 0);
	if (!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "stddev_samp", &tp, NULL, F_AGGR)) {
		if ((err = sql_update_feb2013(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}
	/* if function sys.storagemodel() does not exist, we
	 * need to update */
	if (!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "storagemodel", NULL, NULL, F_FUNC) &&
		!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "storagemodel", NULL, NULL, F_UNION)) {
		if ((err = sql_update_feb2013_sp1(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}
	/* if aggregate function sys.stddev_samp(wrd) does not
	 * exist, we need to update */
	sql_find_subtype(&tp, "wrd", 0, 0);
	if (!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "stddev_samp", &tp, NULL, F_AGGR)) {
		if ((err = sql_update_feb2013_sp3(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}
	/* if function sys.querylog_catalog() does not exist, we
	 * need to update */
	if (!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "querylog_catalog", NULL, NULL, F_FUNC) &&
		!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "querylog_catalog", NULL, NULL, F_UNION)) {
		if ((err = sql_update_jan2014(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}
	/* if function sys.md5(str) does not exist, we need to
	 * update */
	sql_find_subtype(&tp, "clob", 0, 0);
	if (!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "md5", &tp, NULL, F_FUNC)) {
		if ((err = sql_update_oct2014(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}
	/* if table returning function sys.environment() does not
	 * exist, we need to update from oct2014->sp1 */
	if (!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "environment", NULL, NULL, F_UNION)) {
		if ((err = sql_update_oct2014_sp1(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}
	/* if sys.tablestoragemodel.auxillary exists, we need
	 * to update (note, the proper spelling is auxiliary) */
	if (mvc_bind_column(m, mvc_bind_table(m, mvc_bind_schema(m, "sys"), "tablestoragemodel"), "auxillary")) {
		if ((err = sql_update_oct2014_sp2(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}

	/* if function sys.<<(inet,inet) does not exist, we need to
	 * update */
	sql_init_subtype(&tp, find_sql_type(mvc_bind_schema(m, "sys"), "inet"), 0, 0);
	if (!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "left_shift", &tp, &tp, F_FUNC)) {
		if ((err = sql_update_oct2014_sp3(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}

#ifdef HAVE_HGE
	if (have_hge) {
		sql_find_subtype(&tp, "hugeint", 0, 0);
		if (!sql_bind_aggr(m->sa, mvc_bind_schema(m, "sys"), "var_pop", &tp)) {
			if ((err = sql_update_hugeint(c)) != NULL) {
				fprintf(stderr, "!%s\n", err);
				GDKfree(err);
			}
		}
	}
#endif

	/* add missing features needed beyond Oct 2014 */
	sql_find_subtype(&tp, "clob", 0, 0);
	if (!sql_bind_func(m->sa, mvc_bind_schema(m, "sys"), "like", &tp, &tp, F_FILT)) {
		if ((err = sql_update_jul2015(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}

#if 0
	/* test for upgrade condition */
	if (...) {
		if ((err = sql_update_dec2015(c)) !=NULL) {
			fprintf(stderr, "!%s\n", err);
			GDKfree(err);
		}
	}
#endif
}
