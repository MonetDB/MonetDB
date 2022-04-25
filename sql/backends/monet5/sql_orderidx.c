/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
	BAT *b = NULL, *nb = NULL;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);
	if (strNil(sch))
		throw(SQL, "sql.createorderindex", SQLSTATE(42000) "Schema name cannot be NULL");
	if (strNil(tbl))
		throw(SQL, "sql.createorderindex", SQLSTATE(42000) "Table name cannot be NULL");
	if (strNil(col))
		throw(SQL, "sql.createorderindex", SQLSTATE(42000) "Column name cannot be NULL");

	if (!(s = mvc_bind_schema(m, sch)))
		throw(SQL, "sql.createorderindex", SQLSTATE(3FOOO) "Unknown schema %s", sch);
	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.createorderindex", SQLSTATE(42000) "Access denied for %s to schema '%s'", get_string_global_var(m, "current_user"), s->base.name);
	if (!(t = mvc_bind_table(m, s, tbl)))
		throw(SQL, "sql.createorderindex", SQLSTATE(42S02) "Unknown table %s.%s", sch, tbl);
	if (!isTable(t))
		throw(SQL, "sql.createorderindex", SQLSTATE(42000) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (!(c = mvc_bind_column(m, t, col)))
		throw(SQL, "sql.createorderindex", SQLSTATE(38000) "Unknown column %s.%s.%s", sch, tbl, col);
	sqlstore *store = m->session->tr->store;
	if (!(b = store->storage_api.bind_col(m->session->tr, c, RDONLY)))
		throw(SQL,"sql.createorderindex", SQLSTATE(HY005) "Column can not be accessed");
	if (VIEWtparent(b) && (nb = BBP_cache(VIEWtparent(b)))) {
		BBPunfix(b->batCacheid);
		if (!(b = BATdescriptor(nb->batCacheid)))
			throw(SQL,"sql.createorderindex", SQLSTATE(HY005) "Column can not be accessed");
	}
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
	BAT *b = NULL, *nb = NULL;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);
	if (strNil(sch))
		throw(SQL, "sql.droporderindex", SQLSTATE(42000) "Schema name cannot be NULL");
	if (strNil(tbl))
		throw(SQL, "sql.droporderindex", SQLSTATE(42000) "Table name cannot be NULL");
	if (strNil(col))
		throw(SQL, "sql.droporderindex", SQLSTATE(42000) "Column name cannot be NULL");

	if (!(s = mvc_bind_schema(m, sch)))
		throw(SQL, "sql.droporderindex", SQLSTATE(3FOOO) "Unknown schema %s", sch);
	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.droporderindex", SQLSTATE(42000) "Access denied for %s to schema '%s'", get_string_global_var(m, "current_user"), s->base.name);
	if (!(t = mvc_bind_table(m, s, tbl)))
		throw(SQL, "sql.droporderindex", SQLSTATE(42S02) "Unknown table %s.%s", sch, tbl);
	if (!isTable(t))
		throw(SQL, "sql.droporderindex", SQLSTATE(42000) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (!(c = mvc_bind_column(m, t, col)))
		throw(SQL, "sql.droporderindex", SQLSTATE(38000) "Unknown column %s.%s.%s", sch, tbl, col);
	sqlstore *store = m->session->tr->store;
	if (!(b = store->storage_api.bind_col(m->session->tr, c, RDONLY)))
		throw(SQL,"sql.droporderindex", SQLSTATE(HY005) "Column can not be accessed");
	if (VIEWtparent(b) && (nb = BBP_cache(VIEWtparent(b)))) {
		BBPunfix(b->batCacheid);
		if (!(b = BATdescriptor(nb->batCacheid)))
			throw(SQL,"sql.droporderindex", SQLSTATE(HY005) "Column can not be accessed");
	}
	OIDXdestroy(b);
	BBPunfix(b->batCacheid);
	return msg;
}
