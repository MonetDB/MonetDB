/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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
		throw(SQL, "sql_drop_statistics", SQLSTATE(3F000) "Internal error");
	sysstats = mvc_bind_table(m, sys, "statistics");
	if (sysstats == NULL)
		throw(SQL, "sql_drop_statistics", SQLSTATE(3F000) "No table sys.statistics");
	statsid = mvc_bind_column(m, sysstats, "column_id");
	if (statsid == NULL)
		throw(SQL, "sql_drop_statistics", SQLSTATE(3F000) "No table sys.statistics");

	if (isTable(t) && t->columns.set) {
		for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
			sql_column *c = ncol->data;

			rid = table_funcs.column_find_row(tr, statsid, &c->base.id, NULL);
			if (!is_oid_nil(rid) &&
			    table_funcs.table_delete(tr, sysstats, rid) != LOG_OK)
				throw(SQL, "analyze", "delete failed");
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
	node *nsch, *ntab, *ncol;
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
		throw(SQL, "sql.analyze", SQLSTATE(3F000) "Internal error");
	sysstats = mvc_bind_table(m, sys, "statistics");
	if (sysstats == NULL)
		throw(SQL, "sql.analyze", SQLSTATE(3F000) "No table sys.statistics");
	statsid = mvc_bind_column(m, sysstats, "column_id");
	if (statsid == NULL)
		throw(SQL, "sql.analyze", SQLSTATE(3F000) "No table sys.statistics");

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
#ifdef DEBUG_SQL_STATISTICS
	fprintf(stderr, "analyze %s.%s.%s sample " LLFMT "%s\n", (sch ? sch : ""), (tbl ? tbl : " "), (col ? col : " "), samplesize, (minmax)?"MinMax":"");
#endif
	for (nsch = tr->schemas.set->h; nsch; nsch = nsch->next) {
		sql_base *b = nsch->data;
		sql_schema *s = (sql_schema *) nsch->data;
		if (!isalpha((unsigned char) b->name[0]))
			continue;

		if (sch && strcmp(sch, b->name))
			continue;
		sfnd = 1;
		if (s->tables.set)
			for (ntab = (s)->tables.set->h; ntab; ntab = ntab->next) {
				sql_base *bt = ntab->data;
				sql_table *t = (sql_table *) bt;

				if (tbl && strcmp(bt->name, tbl))
					continue;
				if (t->persistence != SQL_PERSIST) {
					GDKfree(maxval);
					GDKfree(minval);
					throw(SQL, "analyze", SQLSTATE(42S02) "Table '%s' is not persistent", bt->name);
				}
				tfnd = 1;
				if (isTable(t) && t->columns.set)
					for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
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

						if ((bn = store_funcs.bind_col(tr, c, RDONLY)) == NULL) {
							/* XXX throw error instead? */
							continue;
						}
						sz = BATcount(bn);
						tostr = BATatoms[bn->ttype].atomToStr;

						rid = table_funcs.column_find_row(tr, statsid, &c->base.id, NULL);
						cfnd = 1;
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
								uniq = BATcount(en);
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
								throw(SQL, "analyze", SQLSTATE(HY001) MAL_MALLOC_FAIL);
							}
							maxlen = 4;
						}
						if (minlen < 4) {
							GDKfree(minval);
							minval = GDKmalloc(4);
							if (minval == NULL){
								GDKfree(maxval);
								throw(SQL, "analyze", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
						if (!is_oid_nil(rid) && table_funcs.table_delete(tr, sysstats, rid) != LOG_OK) {
							GDKfree(maxval);
							GDKfree(minval);
							throw(SQL, "analyze", "delete failed");
						}
						if (table_funcs.table_insert(tr, sysstats, &c->base.id, c->type.type->sqlname, &width, &ts, samplesize ? &samplesize : &sz, &sz, &uniq, &nils, minval, maxval, &sorted, &revsorted) != LOG_OK) {
							GDKfree(maxval);
							GDKfree(minval);
							throw(SQL, "analyze", "insert failed");
						}
					}
			}
	}
	GDKfree(maxval);
	GDKfree(minval);
	if (sch && !sfnd)
		throw(SQL, "analyze", SQLSTATE(3F000) "Schema '%s' does not exist", sch);
	if (tbl && !tfnd)
		throw(SQL, "analyze", SQLSTATE(42S02) "Table '%s' does not exist", tbl);
	if (col && !cfnd)
		throw(SQL, "analyze", SQLSTATE(38000) "Column '%s' does not exist", col);
	return MAL_SUCCEED;
}
