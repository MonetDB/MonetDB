/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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

					int access = c->storage_type && c->storage_type[0] == 'D' ? RD_EXT : RDONLY;
					if (!(b = store->storage_api.bind_col(tr, c, access)))
						continue; /* At the moment we ignore the error, but maybe we can change this */
					if (isVIEW(b)) { /* If it is a view get the parent BAT */
						BAT *nb = BBP_cache(VIEWtparent(b));
						BBPunfix(b->batCacheid);
						if (!(b = BATdescriptor(nb->batCacheid)))
							continue;
					}

					/* Collect new sorted and revsorted properties */
					(void) BATordered(b);
					(void) BATordered_rev(b);

					/* Check for nils existence */
					(void) BATcount_no_nil(b, NULL);

					/* Test if column is unique */
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
	BAT *cid, *sch, *tab, *col, *type, *width, *count, *unique, *nils, *minval, *maxval, *sorted, *revsorted;
	mvc *m = NULL;
	sql_trans *tr = NULL;
	sqlstore *store = NULL;
	bat *rcid = getArgReference_bat(stk, pci, 0);
	bat *rsch = getArgReference_bat(stk, pci, 1);
	bat *rtab = getArgReference_bat(stk, pci, 2);
	bat *rcol = getArgReference_bat(stk, pci, 3);
	bat *rtype = getArgReference_bat(stk, pci, 4);
	bat *rwidth = getArgReference_bat(stk, pci, 5);
	bat *rcount = getArgReference_bat(stk, pci, 6);
	bat *runique = getArgReference_bat(stk, pci, 7);
	bat *rnils = getArgReference_bat(stk, pci, 8);
	bat *rminval = getArgReference_bat(stk, pci, 9);
	bat *rmaxval = getArgReference_bat(stk, pci, 10);
	bat *rsorted = getArgReference_bat(stk, pci, 11);
	bat *rrevsorted = getArgReference_bat(stk, pci, 12);
	str sname = NULL, tname = NULL, cname = NULL, msg = MAL_SUCCEED;
	struct os_iter si = {0};
	BUN nrows = 0;
	int sfnd = 0, tfnd = 0, cfnd = 0;
	size_t buflen = 0;
	char *buf = NULL, *nval = NULL;

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
					nrows++;
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

	cid = COLnew(0, TYPE_int, nrows, TRANSIENT);
	sch = COLnew(0, TYPE_str, nrows, TRANSIENT);
	tab = COLnew(0, TYPE_str, nrows, TRANSIENT);
	col = COLnew(0, TYPE_str, nrows, TRANSIENT);
	type = COLnew(0, TYPE_str, nrows, TRANSIENT);
	width = COLnew(0, TYPE_int, nrows, TRANSIENT);
	count = COLnew(0, TYPE_lng, nrows, TRANSIENT);
	unique = COLnew(0, TYPE_bit, nrows, TRANSIENT);
	nils = COLnew(0, TYPE_bit, nrows, TRANSIENT);
	minval = COLnew(0, TYPE_str, nrows, TRANSIENT);
	maxval = COLnew(0, TYPE_str, nrows, TRANSIENT);
	sorted = COLnew(0, TYPE_bit, nrows, TRANSIENT);
	revsorted = COLnew(0, TYPE_bit, nrows, TRANSIENT);

	if (!cid || !sch || !tab || !col || !type || !width || !count || !unique || !nils || !minval || !maxval || !sorted || !revsorted) {
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
						bit un, hnils, issorted, isrevsorted, dict;
						BAT *qd = NULL, *fb = NULL, *re = NULL;

						if (cname && strcmp(c->base.name, cname))
							continue;

						if (!(qd = store->storage_api.bind_col(tr, c, QUICK))) {
							msg = createException(SQL, "sql.statistics", SQLSTATE(HY005) "Cannot access column descriptor");
							goto bailout;
						}
						BATiter qdi = bat_iterator(qd);
						BATiter posi;
						if ((dict = (c->storage_type && c->storage_type[0] == 'D'))) {
							if (!(re = store->storage_api.bind_col(tr, c, RD_EXT))) {
								bat_iterator_end(&qdi);
								msg = createException(SQL, "sql.statistics", SQLSTATE(HY005) "Cannot access column descriptor");
								goto bailout;
							}
							BATiter rei = bat_iterator(re);
							if (isVIEW(re)) { /* If it is a view get the parent BAT */
								BAT *nb = BBP_cache(VIEWtparent(re));
								BBPunfix(re->batCacheid);
								if (!(re = BATdescriptor(nb->batCacheid))) {
									bat_iterator_end(&qdi);
									bat_iterator_end(&rei);
									msg = createException(SQL, "sql.statistics", SQLSTATE(HY005) "Cannot access column descriptor");
									goto bailout;
								}
							}
							issorted = qdi.sorted && rei.sorted;
							isrevsorted = qdi.revsorted && rei.revsorted;
							hnils = !rei.nonil || rei.nil;
							posi = bat_iterator_copy(&rei);
							bat_iterator_end(&rei);
						} else {
							issorted = qdi.sorted;
							isrevsorted = qdi.revsorted;
							hnils = !qdi.nonil || qdi.nil;
							posi = bat_iterator_copy(&qdi);
						}

						w = qdi.width;
						cnt = qdi.count;
						un = qdi.key;
						bat_iterator_end(&qdi);

						if (BUNappend(cid, &c->base.id, false) != GDK_SUCCEED ||
							BUNappend(sch, b->name, false) != GDK_SUCCEED ||
							BUNappend(tab, bt->name, false) != GDK_SUCCEED ||
							BUNappend(col, c->base.name, false) != GDK_SUCCEED ||
							BUNappend(type, c->type.type->base.name, false) != GDK_SUCCEED ||
							BUNappend(width, &w, false) != GDK_SUCCEED ||
							BUNappend(count, &cnt, false) != GDK_SUCCEED ||
							BUNappend(unique, &un, false) != GDK_SUCCEED ||
							BUNappend(nils, &hnils, false) != GDK_SUCCEED ||
							BUNappend(sorted, &issorted, false) != GDK_SUCCEED ||
							BUNappend(revsorted, &isrevsorted, false) != GDK_SUCCEED) {
							bat_iterator_end(&posi);
							if (re)
								BBPunfix(re->batCacheid);
							goto bailout;
						}

						if (posi.minpos != BUN_NONE || posi.maxpos != BUN_NONE) {
							bat_iterator_end(&posi);
							if (dict) {
								fb = re;
							} else {
								if (!(fb = store->storage_api.bind_col(tr, c, RDONLY))) {
									msg = createException(SQL, "sql.statistics", SQLSTATE(HY005) "Cannot access column descriptor");
									goto bailout;
								}
								if (isVIEW(fb)) { /* If it is a view get the parent BAT */
									BAT *nb = BBP_cache(VIEWtparent(fb));
									BBPunfix(fb->batCacheid);
									if (!(fb = BATdescriptor(nb->batCacheid))) {
										msg = createException(SQL, "sql.statistics", SQLSTATE(HY005) "Cannot access column descriptor");
										goto bailout;
									}
								}
							}

							BATiter fbi = bat_iterator(fb);
							ssize_t (*tostr)(str*,size_t*,const void*,bool) = BATatoms[fbi.type].atomToStr;
							if (fbi.minpos != BUN_NONE) {
								if (tostr(&buf, &buflen, BUNtail(fbi, fbi.minpos), false) < 0) {
									bat_iterator_end(&fbi);
									BBPunfix(fb->batCacheid);
									msg = createException(SQL, "sql.statistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
									goto bailout;
								}
								nval = buf;
							} else {
								nval = (char *) str_nil;
							}
							if (BUNappend(minval, nval, false) != GDK_SUCCEED) {
								bat_iterator_end(&fbi);
								BBPunfix(fb->batCacheid);
								goto bailout;
							}

							if (fbi.maxpos != BUN_NONE) {
								if (tostr(&buf, &buflen, BUNtail(fbi, fbi.maxpos), false) < 0) {
									bat_iterator_end(&fbi);
									BBPunfix(fb->batCacheid);
									msg = createException(SQL, "sql.statistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
									goto bailout;
								}
								nval = buf;
							} else {
								nval = (char *) str_nil;
							}
							bat_iterator_end(&fbi);
							if (BUNappend(maxval, nval, false) != GDK_SUCCEED) {
								BBPunfix(fb->batCacheid);
								goto bailout;
							}
							BBPunfix(fb->batCacheid);
						} else if (BUNappend(minval, str_nil, false) != GDK_SUCCEED || BUNappend(maxval, str_nil, false) != GDK_SUCCEED) {
							bat_iterator_end(&posi);
							if (re)
								BBPunfix(re->batCacheid);
							goto bailout;
						} else if (re) {
							bat_iterator_end(&posi);
							BBPunfix(re->batCacheid);
						} else {
							bat_iterator_end(&posi);
						}
					}
				}
			}
		}
	}

	GDKfree(buf);
	*rcid = cid->batCacheid;
	BBPkeepref(cid);
	*rsch = sch->batCacheid;
	BBPkeepref(sch);
	*rtab = tab->batCacheid;
	BBPkeepref(tab);
	*rcol = col->batCacheid;
	BBPkeepref(col);
	*rtype = type->batCacheid;
	BBPkeepref(type);
	*rwidth = width->batCacheid;
	BBPkeepref(width);
	*rcount = count->batCacheid;
	BBPkeepref(count);
	*runique = unique->batCacheid;
	BBPkeepref(unique);
	*rnils = nils->batCacheid;
	BBPkeepref(nils);
	*rminval = minval->batCacheid;
	BBPkeepref(minval);
	*rmaxval = maxval->batCacheid;
	BBPkeepref(maxval);
	*rsorted = sorted->batCacheid;
	BBPkeepref(sorted);
	*rrevsorted = revsorted->batCacheid;
	BBPkeepref(revsorted);
	return MAL_SUCCEED;
bailout:
	GDKfree(buf);
	BBPreclaim(cid);
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
