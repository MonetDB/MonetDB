/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* (c) M.L. Kersten
 * The order index interface routines are defined here.
*/
#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_scenario.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_env.h"
#include "sql_mvc.h"
#include "sql_orderidx.h"
#include "orderidx.h"
#include "sql_scenario.h"

str
sql_createorderindex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	str sch,tbl,col;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *b;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);

	if (!(s = mvc_bind_schema(m, sch)))
		throw(SQL, "sql.createorderindex", SQLSTATE(3FOOO) "Unknown schema %s", sch);
	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.createorderindex", SQLSTATE(42000) "Access denied for %s to schema '%s'", stack_get_string(m, "current_user"), s->base.name);
	if (!(t = mvc_bind_table(m, s, tbl)) || !isTable(t))
		throw(SQL, "sql.createorderindex", SQLSTATE(42S02) "Unknown table %s.%s", sch, tbl);
	if (!(c = mvc_bind_column(m, t, col)))
		throw(SQL, "sql.createorderindex", SQLSTATE(38000) "Unknown column %s.%s.%s", sch, tbl, col);
	b = store_funcs.bind_col(m->session->tr, c, 0);
	if (b == 0)
		throw(SQL,"sql.createorderindex", SQLSTATE(HY005) "Column can not be accessed");
	/* create the ordered index on the column */
	msg = OIDXcreateImplementation(cntxt, newBatType(b->ttype), b, -1);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sql_droporderindex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	str sch,tbl,col;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *b;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);

	if (!(s = mvc_bind_schema(m, sch)))
		throw(SQL, "sql.droporderindex", SQLSTATE(3FOOO) "Unknown schema %s", sch);
	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.droporderindex", SQLSTATE(42000) "Access denied for %s to schema '%s'", stack_get_string(m, "current_user"), s->base.name);
	if (!(t = mvc_bind_table(m, s, tbl)) || !isTable(t))
		throw(SQL, "sql.droporderindex", SQLSTATE(42S02) "Unknown table %s.%s", sch, tbl);
	if (!(c = mvc_bind_column(m, t, col)))
		throw(SQL, "sql.droporderindex", SQLSTATE(38000) "Unknown column %s.%s.%s", sch, tbl, col);
	b = store_funcs.bind_col(m->session->tr, c, 0);
	if (b == 0)
		throw(SQL,"sql.droporderindex", SQLSTATE(38000) "Column can not be accessed");
	msg = OIDXdropImplementation(cntxt, b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}
