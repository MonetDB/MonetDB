/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);

#ifdef DEBUG_SQL_ORDERIDX
	mnstr_printf(cntxt->fdout, "#orderindex layout %s.%s.%s \n", sch, tbl, col);
#endif
	s = mvc_bind_schema(m, sch);
	if (s) {
		sql_table *t = mvc_bind_table(m, s, tbl);
		if (t && isTable(t)) {
			sql_column *c = mvc_bind_column(m, t, col);
			BAT *bn = store_funcs.bind_col(m->session->tr, c, 0);

			if (bn == 0) {
				msg = createException(SQL,"sql","Column can not be accessed");
			} else { // create the ordered index on the column
				msg = OIDXcreateImplementation(cntxt, newBatType(TYPE_void,bn->ttype), bn, -1);
				BBPunfix(bn->batCacheid);
			}
		}
	}
	return msg;
}

str
sql_droporderindex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	str sch,tbl,col;
	sql_schema *s;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);

#ifdef DEBUG_SQL_ORDERIDX
	mnstr_printf(cntxt->fdout, "#orderindex layout %s.%s.%s \n", sch, tbl, col);
#endif
	s = mvc_bind_schema(m, sch);
	if (s) {
		sql_table *t = mvc_bind_table(m, s, tbl);
		if (t && isTable(t)) {
			sql_column *c = mvc_bind_column(m, t, col);
			BAT *bn = store_funcs.bind_col(m->session->tr, c, 0);

			if (bn == 0) {
				msg = createException(SQL,"sql","Column can not be accessed");
			} else {
				msg = OIDXdropImplementation(cntxt, bn);

				BBPunfix(bn->batCacheid);
			}
		}
	}
	return MAL_SUCCEED;
}

