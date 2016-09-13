/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

#ifdef DEBUG_SQL_ORDERIDX
	mnstr_printf(cntxt->fdout, "#orderindex layout %s.%s.%s \n", sch, tbl, col);
#endif
	s = mvc_bind_schema(m, sch);
	if (s == NULL)
		throw(SQL, "sql.createorderindex", "unknown schema %s", sch);
	t = mvc_bind_table(m, s, tbl);
	if (t == NULL || !isTable(t))
		throw(SQL, "sql.createorderindex", "unknown table %s.%s",
		      sch, tbl);
	c = mvc_bind_column(m, t, col);
	if (c == NULL)
		throw(SQL, "sql.createorderindex", "unknown column %s.%s.%s",
		      sch, tbl, col);
	b = store_funcs.bind_col(m->session->tr, c, 0);
	if (b == 0)
		throw(SQL,"sql.createorderindex","Column can not be accessed");
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

#ifdef DEBUG_SQL_ORDERIDX
	mnstr_printf(cntxt->fdout, "#orderindex layout %s.%s.%s \n", sch, tbl, col);
#endif
	s = mvc_bind_schema(m, sch);
	if (s == NULL)
		throw(SQL, "sql.droporderindex", "unknown schema %s", sch);
	t = mvc_bind_table(m, s, tbl);
	if (t == NULL || !isTable(t))
		throw(SQL, "sql.droporderindex", "unknown table %s.%s",
		      sch, tbl);
	c = mvc_bind_column(m, t, col);
	if (c == NULL)
		throw(SQL, "sql.droporderindex", "unknown column %s.%s.%s",
		      sch, tbl, col);
	b = store_funcs.bind_col(m->session->tr, c, 0);
	if (b == 0)
		throw(SQL,"sql.droporderindex","Column can not be accessed");
	msg = OIDXdropImplementation(cntxt, b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

