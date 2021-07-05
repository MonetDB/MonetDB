/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_strimps.h"

static str
sql_load_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, BAT **b)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	str sch,tbl,col;
	sql_schema *s;
	sql_table *t;
	sql_column *c;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);

	if (!(s = mvc_bind_schema(m, sch)))
		throw(SQL, "sql.createstrimps", SQLSTATE(3FOOO) "Unknown schema %s", sch);

	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.createstrimps", SQLSTATE(42000) "Access denied for %s to schema '%s'",
			  get_string_global_var(m, "current_user"), s->base.name);
	if (!(t = mvc_bind_table(m, s, tbl)) || !isTable(t))
		throw(SQL, "sql.createstrimps", SQLSTATE(42S02) "Unknown table %s.%s", sch, tbl);
	if (!(c = mvc_bind_column(m, t, col)))
		throw(SQL, "sql.createstrimps", SQLSTATE(38000) "Unknown column %s.%s.%s", sch, tbl, col);

	sqlstore *store = m->session->tr->store;
	*b = store->storage_api.bind_col(m->session->tr, c, 0);
	if (*b == 0)
		throw(SQL, "sql.createstrimps", SQLSTATE(HY005) "Cannot access column %s", col);

	return msg;

}

str
sql_createstrimps(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;

	if (sql_load_bat(cntxt, mb, stk, pci, &b) != MAL_SUCCEED)
		throw(SQL, "sql.createstrimps", SQLSTATE(HY002) OPERATION_FAILED);

	if (STRMPcreate(b) != GDK_SUCCEED)
		throw(SQL, "sql.createstrimps", SQLSTATE(HY002) OPERATION_FAILED);

	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/* str */
/* sql_strimpfilter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) */
/* { */
/* 	BAT *b; */
/* 	if (sql_load_bat(cntxt, mb, stk, pci, &b) != MAL_SUCCEED) */
/* 		throw(SQL, "sql.createstrimps", SQLSTATE(HY002) OPERATION_FAILED); */

/* 	return MAL_SUCCEED; */
/* } */
