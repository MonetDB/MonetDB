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
sql_drop_statistics(mvc *m, sql_table *t)
{
	node *ncol;
	sql_trans *tr;
	sql_schema *sys;
	sql_table *sysstats;
	sql_column *statsid;
	oid rid;

	tr = m->session->tr;
	sys = mvc_bind_schema(m, "sys");
	if (sys == NULL)
		throw(SQL, "sql_drop_statistics", SQLSTATE(3F000) "Internal error: No schema sys");
	sysstats = mvc_bind_table(m, sys, "statistics");
	if (sysstats == NULL)
		throw(SQL, "sql_drop_statistics", SQLSTATE(3F000) "No table sys.statistics");
	statsid = mvc_bind_column(m, sysstats, "column_id");
	if (statsid == NULL)
		throw(SQL, "sql_drop_statistics", SQLSTATE(3F000) "No table sys.statistics");

	/* Do all the validations before any drop */
	if (!isTable(t))
		throw(SQL, "sql_drop_statistics", SQLSTATE(42S02) "DROP STATISTICS: %s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (!table_privs(m, t, PRIV_SELECT))
		throw(SQL, "sql_drop_statistics", SQLSTATE(42000) "DROP STATISTICS: access denied for %s to table '%s.%s'",
			  get_string_global_var(m, "current_user"), t->s->base.name, t->base.name);
	if (isTable(t) && ol_first_node(t->columns)) {
		for (ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
			sql_column *c = (sql_column *) ncol->data;

			if (!column_privs(m, c, PRIV_SELECT))
				throw(SQL, "sql_drop_statistics", SQLSTATE(42000) "DROP STATISTICS: access denied for %s to column '%s' on table '%s.%s'",
					  get_string_global_var(m, "current_user"), c->base.name, t->s->base.name, t->base.name);
		}
	}

	sqlstore *store = tr->store;
	if (isTable(t) && ol_first_node(t->columns)) {
		for (ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
			sql_column *c = ncol->data;

			rid = store->table_api.column_find_row(tr, statsid, &c->base.id, NULL);
			if (!is_oid_nil(rid) &&
			    store->table_api.table_delete(tr, sysstats, rid) != LOG_OK)
				throw(SQL, "sql_drop_statistics", "delete failed");
		}
	}
	return MAL_SUCCEED;
}

str
sql_analyze(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_trans *tr = m->session->tr;
	node *ncol;
	char *maxval = NULL, *minval = NULL;
	size_t minlen = 0, maxlen = 0;
	str sch = 0, tbl = 0, col = 0;
	bit sorted, revsorted;	/* not bool since address is taken */
	lng nils = 0;
	lng uniq = 0;
	lng samplesize = *getArgReference_lng(stk, pci, 2);
	int argc = pci->argc;
	int width = 0;
	int minmax = *getArgReference_int(stk, pci, 1);
	int sfnd = 0, tfnd = 0, cfnd = 0;
	sql_schema *sys;
	sql_table *sysstats;
	sql_column *statsid;
	oid rid;
	timestamp ts;

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
	case 6:
		col = *getArgReference_str(stk, pci, 5);
		/* fall through */
	case 5:
		tbl = *getArgReference_str(stk, pci, 4);
		/* fall through */
	case 4:
		sch = *getArgReference_str(stk, pci, 3);
	}

	TRC_DEBUG(SQL_PARSER, "analyze %s.%s.%s sample " LLFMT "%s\n", (sch ? sch : ""), (tbl ? tbl : " "), (col ? col : " "), samplesize, (minmax)?"MinMax":"");

	/* Do all the validations before doing any analyze */
	struct os_iter si;
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for(sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema *)b;
		if (!isalpha((unsigned char) s->base.name[0]))
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
				throw(SQL, "analyze", SQLSTATE(42S02) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
			if (!table_privs(m, t, PRIV_SELECT))
				throw(SQL, "analyze", SQLSTATE(42000) "ANALYZE: access denied for %s to table '%s.%s'",
					  get_string_global_var(m, "current_user"), t->s->base.name, t->base.name);
			if (isTable(t) && ol_first_node(t->columns)) {
				for (ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
					sql_column *c = (sql_column *) ncol->data;

					if (col && strcmp(c->base.name, col))
						continue;
					cfnd = 1;
					if (!column_privs(m, c, PRIV_SELECT))
						throw(SQL, "analyze", SQLSTATE(42000) "ANALYZE: access denied for %s to column '%s' on table '%s.%s'",
							  get_string_global_var(m, "current_user"), c->base.name, t->s->base.name, t->base.name);
				}
			}
		}
	}
	if (sch && !sfnd)
		throw(SQL, "analyze", SQLSTATE(3F000) "Schema '%s' does not exist", sch);
	if (tbl && !tfnd)
		throw(SQL, "analyze", SQLSTATE(42S02) "Table '%s' does not exist", tbl);
	if (col && !cfnd)
		throw(SQL, "analyze", SQLSTATE(38000) "Column '%s' does not exist", col);

	sqlstore *store = tr->store;
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for(sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema *)b;
		if (!isalpha((unsigned char) b->name[0]))
			continue;

		if (sch && strcmp(sch, b->name))
			continue;
		struct os_iter oi;
		os_iterator(&oi, s->tables, tr, NULL);
		for(sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *t = (sql_table *) b;

			if (tbl && strcmp(b->name, tbl))
				continue;
			if (isTable(t) && ol_first_node(t->columns))
				for (ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
					sql_base *bc = ncol->data;
					sql_column *c = (sql_column *) ncol->data;
					BAT *bn, *br;
					BAT *bsample;
					lng sz;
					ssize_t (*tostr)(str*,size_t*,const void*,bool);
					void *val=0;

					if (col && strcmp(bc->name, col))
						continue;

					/* remove cached value */
					if (c->min)
						c->min = NULL;
					if (c->max)
						c->max = NULL;

					if ((bn = store->storage_api.bind_col(tr, c, RDONLY)) == NULL) {
						/* XXX throw error instead? */
						continue;
					}
					sz = BATcount(bn);
					tostr = BATatoms[bn->ttype].atomToStr;

					rid = store->table_api.column_find_row(tr, statsid, &c->base.id, NULL);
					if (samplesize > 0) {
						bsample = BATsample(bn, (BUN) samplesize);
					} else
						bsample = NULL;
					br = BATselect(bn, bsample, ATOMnilptr(bn->ttype), NULL, true, false, false);
					if (br == NULL) {
						BBPunfix(bn->batCacheid);
						/* XXX throw error instead? */
						continue;
					}
					nils = BATcount(br);
					BBPunfix(br->batCacheid);
					if (bn->tkey)
						uniq = sz;
					else if (!minmax) {
						BAT *en;
						if (bsample)
							br = BATproject(bsample, bn);
						else
							br = bn;
						if (br && (en = BATunique(br, NULL)) != NULL) {
							uniq = canditer_init(&(struct canditer){0}, NULL, en);
							BBPunfix(en->batCacheid);
						} else
							uniq = 0;
						if (bsample && br)
							BBPunfix(br->batCacheid);
					}
					if (bsample)
						BBPunfix(bsample->batCacheid);
					/* use BATordered(_rev)
					 * and not
					 * BATt(rev)ordered
					 * because we want to
					 * know for sure */
					sorted = BATordered(bn);
					revsorted = BATordered_rev(bn);

					// Gather the min/max value for builtin types
					width = bn->twidth;

					if (maxlen < 4) {
						GDKfree(maxval);
						maxval = GDKmalloc(4);
						if (maxval == NULL) {
							GDKfree(minval);
							BBPunfix(bn->batCacheid);
							throw(SQL, "analyze", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						}
						maxlen = 4;
					}
					if (minlen < 4) {
						GDKfree(minval);
						minval = GDKmalloc(4);
						if (minval == NULL){
							GDKfree(maxval);
							BBPunfix(bn->batCacheid);
							throw(SQL, "analyze", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						}
						minlen = 4;
					}
					if (tostr) {
						if ((val = BATmax(bn, NULL)) == NULL)
							strcpy(maxval, str_nil);
						else {
							if (tostr(&maxval, &maxlen, val, false) < 0) {
								GDKfree(val);
								GDKfree(minval);
								GDKfree(maxval);
								BBPunfix(bn->batCacheid);
								throw(SQL, "analyze", GDK_EXCEPTION);
							}
							GDKfree(val);
						}
						if ((val = BATmin(bn, NULL)) == NULL)
							strcpy(minval, str_nil);
						else {
							if (tostr(&minval, &minlen, val, false) < 0) {
								GDKfree(val);
								GDKfree(minval);
								GDKfree(maxval);
								BBPunfix(bn->batCacheid);
								throw(SQL, "analyze", GDK_EXCEPTION);
							}
							GDKfree(val);
						}
					} else {
						strcpy(maxval, str_nil);
						strcpy(minval, str_nil);
					}
					BBPunfix(bn->batCacheid);
					ts = timestamp_current();
					if (!is_oid_nil(rid) && store->table_api.table_delete(tr, sysstats, rid) != LOG_OK) {
						GDKfree(maxval);
						GDKfree(minval);
						throw(SQL, "analyze", "delete failed");
					}
					if (store->table_api.table_insert(tr, sysstats, &c->base.id, &c->type.type->sqlname, &width, &ts, samplesize ? &samplesize : &sz, &sz, &uniq, &nils, &minval, &maxval, &sorted, &revsorted) != LOG_OK) {
						GDKfree(maxval);
						GDKfree(minval);
						throw(SQL, "analyze", "insert failed");
					}
				}
			}
		}
	GDKfree(maxval);
	GDKfree(minval);
	return MAL_SUCCEED;
}
