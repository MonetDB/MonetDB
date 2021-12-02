/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* (c) M.L. Kersten
Most optimizers need easy access to key information
for proper plan generation. Amongst others, this
information consists of the tuple count, size,
min- and max-value, and the null-density.
They are kept around as persistent tables, modeled
directly as a collection of BATs.

We made need an directly accessible structure to speedup
analysis by optimizers.
*/
#include "monetdb_config.h"
#include "sql_statistics.h"
#include "sql_execute.h"

str
sql_analyze(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_trans *tr = m->session->tr;
	str sch = 0, tbl = 0, col = 0;
	int argc = pci->argc, sfnd = 0, tfnd = 0, cfnd = 0;
	sql_schema *sys;
	sql_table *sysstats;
	sql_column *statsid;

	if (msg != MAL_SUCCEED || (msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sys = mvc_bind_schema(m, "sys");
	if (sys == NULL)
		throw(SQL, "sql.analyze", SQLSTATE(3F000) "Internal error: No schema sys");
	sysstats = mvc_bind_table(m, sys, "statistics");
	if (sysstats == NULL)
		throw(SQL, "sql.analyze", SQLSTATE(3F000) "Internal error: No table sys.statistics");
	statsid = mvc_bind_column(m, sysstats, "column_id");
	if (statsid == NULL)
		throw(SQL, "sql.analyze", SQLSTATE(3F000) "Internal error: No table sys.statistics");

	switch (argc) {
	case 4:
		col = *getArgReference_str(stk, pci, 3);
		if (strNil(col))
			throw(SQL, "sql.analyze", SQLSTATE(42000) "Column name cannot be NULL");
		/* fall through */
	case 3:
		tbl = *getArgReference_str(stk, pci, 2);
		if (strNil(tbl))
			throw(SQL, "sql.analyze", SQLSTATE(42000) "Table name cannot be NULL");
		/* fall through */
	case 2:
		sch = *getArgReference_str(stk, pci, 1);
		if (strNil(sch))
			throw(SQL, "sql.analyze", SQLSTATE(42000) "Schema name cannot be NULL");
	}

	TRC_DEBUG(SQL_PARSER, "analyze %s.%s.%s\n", (sch ? sch : ""), (tbl ? tbl : " "), (col ? col : " "));

	/* Do all the validations before doing any analyze */
	struct os_iter si;
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for(sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema *)b;
		if (s->base.name[0] == '%')
			continue;

		if (sch && strcmp(s->base.name, sch))
			continue;
		sfnd = 1;
		struct os_iter oi;
		os_iterator(&oi, s->tables, tr, NULL);
		for(sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *t = (sql_table *)b;

			if (tbl && strcmp(t->base.name, tbl))
				continue;
			tfnd = 1;
			if (tbl && !isTable(t))
				throw(SQL, "sql.analyze", SQLSTATE(42S02) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
			if (!table_privs(m, t, PRIV_SELECT))
				throw(SQL, "sql.analyze", SQLSTATE(42000) "ANALYZE: access denied for %s to table '%s.%s'",
					  get_string_global_var(m, "current_user"), t->s->base.name, t->base.name);
			if (isTable(t) && ol_first_node(t->columns)) {
				for (node *ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
					sql_column *c = (sql_column *) ncol->data;

					if (col && strcmp(c->base.name, col))
						continue;
					cfnd = 1;
					if (!column_privs(m, c, PRIV_SELECT))
						throw(SQL, "sql.analyze", SQLSTATE(42000) "ANALYZE: access denied for %s to column '%s' on table '%s.%s'",
							  get_string_global_var(m, "current_user"), c->base.name, t->s->base.name, t->base.name);
				}
			}
		}
	}
	if (sch && !sfnd)
		throw(SQL, "sql.analyze", SQLSTATE(3F000) "Schema '%s' does not exist", sch);
	if (tbl && !tfnd)
		throw(SQL, "sql.analyze", SQLSTATE(42S02) "Table '%s' does not exist", tbl);
	if (col && !cfnd)
		throw(SQL, "sql.analyze", SQLSTATE(38000) "Column '%s' does not exist", col);

	sqlstore *store = tr->store;
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for(sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema *)b;
		if (b->name[0] == '%')
			continue;

		if (sch && strcmp(sch, b->name))
			continue;
		struct os_iter oi;
		os_iterator(&oi, s->tables, tr, NULL);
		for(sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *t = (sql_table *) b;

			if (tbl && strcmp(b->name, tbl))
				continue;
			if (isTable(t) && ol_first_node(t->columns)) {
				for (node *ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
					sql_column *c = (sql_column *) ncol->data;
					BAT *b, *unq;
					ptr mn, mx;

					if (col && strcmp(c->base.name, col))
						continue;
					if (!(b = store->storage_api.bind_col(tr, c, RDONLY)))
						continue; /* At the moment we ignore the error, but maybe we can change this */

					/* Collect new sorted and revsorted properties */
					(void) BATordered(b);
					(void) BATordered_rev(b);

					/* Check for nils existence */
					if (!c->null) {
						b->tnonil = true;
						b->tnil = false;
					} else {
						(void) BATcount_no_nil(b, NULL);
					}

					/* Test it column is unique */
					if (is_column_unique(c) && b->tnonil) {
						b->tkey = true;
					} else if ((unq = BATunique(b, NULL)))
						BBPunfix(unq->batCacheid);

					/* Guess number of uniques if not entirely unique */
					(void) BATguess_uniques(b, NULL);

					/* Collect min and max values */
					mn = BATmin(b, NULL);
					GDKfree(mn);
					mx = BATmax(b, NULL);
					GDKfree(mx);
					BBPunfix(b->batCacheid);
				}
			}
		}
	}
	return MAL_SUCCEED;
}
