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
	sql_trans *tr = m->session->tr;
	str sch,tbl,col;
	node *nsch, *ntab, *ncol;
	BAT *bn;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);

#ifdef DEBUG_SQL_ORDERIDX
	mnstr_printf(cntxt->fdout, "#orderindex layout %s.%s.%s \n", sch, tbl, col);
#endif
	for (nsch = tr->schemas.set->h; nsch; nsch = nsch->next) {
		sql_base *b = nsch->data;
		sql_schema *s = (sql_schema *) nsch->data;
		if (!isalpha((int) b->name[0]))
			continue;
		if (sch && strcmp(sch, b->name))
			continue;
		if (s->tables.set)
			for (ntab = (s)->tables.set->h; ntab; ntab = ntab->next) {
				sql_base *bt = ntab->data;
				sql_table *t = (sql_table *) bt;

				if (tbl && strcmp(bt->name, tbl))
					continue;
				if (isTable(t) && t->columns.set)
					for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
						sql_base *bc = ncol->data;
						sql_column *c = (sql_column *) ncol->data;
						if (col && strcmp(bc->name, col))
							continue;
						bn = store_funcs.bind_col(m->session->tr, c, 0);
						if ( bn == 0){
							msg = createException(SQL,"sql","Column can not be accessed");
							break;
						}
						// create the ordered index on the column
						msg = OIDXcreateImplementation(cntxt, newBatType(TYPE_void,bn->ttype), bn, -1);
						BBPunfix(bn->batCacheid);
						if( msg)
							break;
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
	sql_trans *tr = m->session->tr;
	node *nsch, *ntab, *ncol;
	BAT *bn;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);
	col = *getArgReference_str(stk, pci, 3);

#ifdef DEBUG_SQL_ORDERIDX
	mnstr_printf(cntxt->fdout, "#orderindex layout %s.%s.%s \n", sch, tbl, col);
#endif
	for (nsch = tr->schemas.set->h; nsch; nsch = nsch->next) {
		sql_base *b = nsch->data;
		sql_schema *s = (sql_schema *) nsch->data;
		if (!isalpha((int) b->name[0]))
			continue;
		if (sch && strcmp(sch, b->name))
			continue;
		if (s->tables.set)
			for (ntab = (s)->tables.set->h; ntab; ntab = ntab->next) {
				sql_base *bt = ntab->data;
				sql_table *t = (sql_table *) bt;

				if (tbl && strcmp(bt->name, tbl))
					continue;
				if (isTable(t) && t->columns.set)
					for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
						sql_base *bc = ncol->data;
						sql_column *c = (sql_column *) ncol->data;
						if (col && strcmp(bc->name, col))
							continue;
						bn = store_funcs.bind_col(m->session->tr, c, 0);
						// create the ordered index on the column
						msg = OIDXdropImplementation(cntxt, bn);

						BBPunfix(bn->batCacheid);
						(void) c;
					}
			}
	}
	return MAL_SUCCEED;
}

