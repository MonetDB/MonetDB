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

We made need an directly accessible structure to speedup
analysis by optimizers.
*/
#include "monetdb_config.h"
#include "sql_statistics.h"

str
sql_analyze(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_trans *tr = NULL;
	str sch = NULL, tbl = NULL, col = NULL, msg = MAL_SUCCEED;
	int argc = pci->argc, sfnd = 0, tfnd = 0, cfnd = 0;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	tr = m->session->tr;
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
					(void) BATcount_no_nil(b, NULL);

					/* Test it column is unique */
					if ((unq = BATunique(b, NULL)))
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

str
sql_statistics(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *sch, *tab, *col, *type, *width, *count, *unique, *nils, *minval, *maxval, *sorted, *revsorted, *bs = NULL, *fb = NULL;
	mvc *m = NULL;
	sql_trans *tr = NULL;
	sqlstore *store = NULL;
	bat *rsch = getArgReference_bat(stk, pci, 0);
	bat *rtab = getArgReference_bat(stk, pci, 1);
	bat *rcol = getArgReference_bat(stk, pci, 2);
	bat *rtype = getArgReference_bat(stk, pci, 3);
	bat *rwidth = getArgReference_bat(stk, pci, 4);
	bat *rcount = getArgReference_bat(stk, pci, 5);
	bat *runique = getArgReference_bat(stk, pci, 6);
	bat *rnils = getArgReference_bat(stk, pci, 7);
	bat *rminval = getArgReference_bat(stk, pci, 8);
	bat *rmaxval = getArgReference_bat(stk, pci, 9);
	bat *rsorted = getArgReference_bat(stk, pci, 10);
	bat *rrevsorted = getArgReference_bat(stk, pci, 11);
	str sname = NULL, tname = NULL, cname = NULL, msg = MAL_SUCCEED;
	struct os_iter si = {0};
	int sfnd = 0, tfnd = 0, cfnd = 0;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (pci->argc - pci->retc >= 1) {
		sname = *getArgReference_str(stk, pci, pci->retc);
		if (strNil(sname))
			throw(SQL, "sql.statistics", SQLSTATE(42000) "Schema name cannot be NULL");
	}
	if (pci->argc - pci->retc >= 2) {
		tname = *getArgReference_str(stk, pci, pci->retc + 1);
		if (strNil(tname))
			throw(SQL, "sql.statistics", SQLSTATE(42000) "Table name cannot be NULL");
	}
	if (pci->argc - pci->retc >= 3) {
		cname = *getArgReference_str(stk, pci, pci->retc + 2);
		if (strNil(cname))
			throw(SQL, "sql.statistics", SQLSTATE(42000) "Column name cannot be NULL");
	}

	tr = m->session->tr;
	store = tr->store;
	/* Do all the validations before retrieving any statistics */
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for(sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema *)b;
		if (s->base.name[0] == '%')
			continue;

		if (sname && strcmp(s->base.name, sname))
			continue;
		sfnd = 1;
		struct os_iter oi;
		os_iterator(&oi, s->tables, tr, NULL);
		for(sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *t = (sql_table *)b;

			if (tname && strcmp(t->base.name, tname))
				continue;
			tfnd = 1;
			if (tname && !isTable(t))
				throw(SQL, "sql.statistics", SQLSTATE(42S02) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
			if (!table_privs(m, t, PRIV_SELECT))
				throw(SQL, "sql.statistics", SQLSTATE(42000) "STATISTICS: access denied for %s to table '%s.%s'",
					  get_string_global_var(m, "current_user"), t->s->base.name, t->base.name);
			if (isTable(t) && ol_first_node(t->columns)) {
				for (node *ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
					sql_column *c = (sql_column *) ncol->data;

					if (cname && strcmp(c->base.name, cname))
						continue;
					cfnd = 1;
					if (!column_privs(m, c, PRIV_SELECT))
						throw(SQL, "sql.statistics", SQLSTATE(42000) "STATISTICS: access denied for %s to column '%s' on table '%s.%s'",
							  get_string_global_var(m, "current_user"), c->base.name, t->s->base.name, t->base.name);
				}
			}
		}
	}
	if (sname && !sfnd)
		throw(SQL, "sql.statistics", SQLSTATE(3F000) "Schema '%s' does not exist", sname);
	if (tname && !tfnd)
		throw(SQL, "sql.statistics", SQLSTATE(42S02) "Table '%s' does not exist", tname);
	if (cname && !cfnd)
		throw(SQL, "sql.statistics", SQLSTATE(38000) "Column '%s' does not exist", cname);

	sch = COLnew(0, TYPE_str, 0, TRANSIENT);
	tab = COLnew(0, TYPE_str, 0, TRANSIENT);
	col = COLnew(0, TYPE_str, 0, TRANSIENT);
	type = COLnew(0, TYPE_str, 0, TRANSIENT);
	width = COLnew(0, TYPE_int, 0, TRANSIENT);
	count = COLnew(0, TYPE_lng, 0, TRANSIENT);
	unique = COLnew(0, TYPE_bit, 0, TRANSIENT);
	nils = COLnew(0, TYPE_bit, 0, TRANSIENT);
	minval = COLnew(0, TYPE_str, 0, TRANSIENT);
	maxval = COLnew(0, TYPE_str, 0, TRANSIENT);
	sorted = COLnew(0, TYPE_bit, 0, TRANSIENT);
	revsorted = COLnew(0, TYPE_bit, 0, TRANSIENT);

	if (!sch || !tab || !col || !type || !width || !count || !unique || !nils || !minval || !maxval || !sorted || !revsorted) {
		msg = createException(SQL, "sql.statistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	si = (struct os_iter) {0};
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema *) b;
		if ((sname && strcmp(b->name, sname)) || b->name[0] == '%')
			continue;
		if (s->tables) {
			struct os_iter oi;

			os_iterator(&oi, s->tables, tr, NULL);
			for (sql_base *bt = oi_next(&oi); bt; bt = oi_next(&oi)) {
				sql_table *t = (sql_table *) bt;
				if (tname && strcmp(bt->name, tname))
					continue;
				if (isTable(t) && ol_first_node(t->columns)) {
					for (node *ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
						sql_column *c = (sql_column *) ncol->data;
						int w;
						lng cnt;
						bit un, hnils, issorted, isrevsorted;

						if (cname && strcmp(c->base.name, cname))
							continue;
						if (!(bs = store->storage_api.bind_col(tr, c, QUICK))) {
							msg = createException(SQL, "sql.statistics", SQLSTATE(HY005) "Cannot access column descriptor");
							goto bailout;
						}
						w = bs->twidth;
						cnt = BATcount(bs);
						un = bs->tkey;
						hnils = !bs->tnonil;
						issorted = bs->tsorted;
						isrevsorted = bs->trevsorted;

						if (BUNappend(sch, b->name, false) != GDK_SUCCEED ||
							BUNappend(tab, bt->name, false) != GDK_SUCCEED ||
							BUNappend(col, c->base.name, false) != GDK_SUCCEED ||
							BUNappend(type, c->type.type->base.name, false) != GDK_SUCCEED ||
							BUNappend(width, &w, false) != GDK_SUCCEED ||
							BUNappend(count, &cnt, false) != GDK_SUCCEED ||
							BUNappend(unique, &un, false) != GDK_SUCCEED ||
							BUNappend(nils, &hnils, false) != GDK_SUCCEED ||
							BUNappend(sorted, &issorted, false) != GDK_SUCCEED ||
							BUNappend(revsorted, &isrevsorted, false) != GDK_SUCCEED)
							goto bailout;

						if (bs->tminpos != BUN_NONE || bs->tmaxpos != BUN_NONE) {
							ssize_t (*tostr)(str*,size_t*,const void*,bool) = BATatoms[bs->ttype].atomToStr;
							size_t minlen = 0, maxlen = 0;
							char *min = NULL, *max = NULL;
							gdk_return res = GDK_SUCCEED;

							if (!(fb = store->storage_api.bind_col(tr, c, RDONLY))) {
								msg = createException(SQL, "sql.statistics", SQLSTATE(HY005) "Cannot access column descriptor");
								goto bailout;
							}

							BATiter bi = bat_iterator(fb);
							if (fb->tminpos != BUN_NONE || fb->tmaxpos != BUN_NONE) {
								if (fb->tminpos != BUN_NONE) {
									if (tostr(&min, &minlen, BUNtail(bi, fb->tminpos), false) < 0) {
										bat_iterator_end(&bi);
										BBPunfix(fb->batCacheid);
										msg = createException(SQL, "sql.statistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
										goto bailout;
									}
								} else {
									min = (char *) str_nil;
								}
								res = BUNappend(minval, min, false);
								if (fb->tminpos != BUN_NONE)
									GDKfree(min);
								if (res != GDK_SUCCEED) {
									bat_iterator_end(&bi);
									BBPunfix(fb->batCacheid);
									goto bailout;
								}

								if (fb->tmaxpos != BUN_NONE) {
									if (tostr(&max, &maxlen, BUNtail(bi, fb->tmaxpos), false) < 0) {
										bat_iterator_end(&bi);
										BBPunfix(fb->batCacheid);
										msg = createException(SQL, "sql.statistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
										goto bailout;
									}
								} else {
									max = (char *) str_nil;
								}
								res = BUNappend(maxval, max, false);
								if (fb->tmaxpos != BUN_NONE)
									GDKfree(max);
								if (res != GDK_SUCCEED) {
									bat_iterator_end(&bi);
									BBPunfix(fb->batCacheid);
									goto bailout;
								}
							} else if (BUNappend(minval, str_nil, false) != GDK_SUCCEED || BUNappend(maxval, str_nil, false) != GDK_SUCCEED) {
								bat_iterator_end(&bi);
								BBPunfix(fb->batCacheid);
								goto bailout;
							}
							bat_iterator_end(&bi);
							BBPunfix(fb->batCacheid);
						} else if (BUNappend(minval, str_nil, false) != GDK_SUCCEED || BUNappend(maxval, str_nil, false) != GDK_SUCCEED) {
							goto bailout;
						}
					}
				}
			}
		}
	}

	BBPkeepref(*rsch = sch->batCacheid);
	BBPkeepref(*rtab = tab->batCacheid);
	BBPkeepref(*rcol = col->batCacheid);
	BBPkeepref(*rtype = type->batCacheid);
	BBPkeepref(*rwidth = width->batCacheid);
	BBPkeepref(*rcount = count->batCacheid);
	BBPkeepref(*runique = unique->batCacheid);
	BBPkeepref(*rnils = nils->batCacheid);
	BBPkeepref(*rminval = minval->batCacheid);
	BBPkeepref(*rmaxval = maxval->batCacheid);
	BBPkeepref(*rsorted = sorted->batCacheid);
	BBPkeepref(*rrevsorted = revsorted->batCacheid);
	return MAL_SUCCEED;
bailout:
	BBPreclaim(sch);
	BBPreclaim(tab);
	BBPreclaim(col);
	BBPreclaim(type);
	BBPreclaim(width);
	BBPreclaim(count);
	BBPreclaim(unique);
	BBPreclaim(nils);
	BBPreclaim(minval);
	BBPreclaim(maxval);
	BBPreclaim(sorted);
	BBPreclaim(revsorted);
	if (!msg)
		msg = createException(SQL, "sql.statistics", GDK_EXCEPTION);
	return msg;
}
