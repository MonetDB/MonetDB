/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_rank.h"
#include "gdk_analytic.h"

static void
unfix_inputs(int nargs, ...)
{
	va_list valist;

	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		BAT *b = va_arg(valist, BAT *);
		if (b)
			BBPunfix(b->batCacheid);
	}
	va_end(valist);
}

static void
finalize_output(bat *res, BAT *r, str msg)
{
	if (res && r && !msg) {
		r->tsorted = BATcount(r) <= 1;
		r->trevsorted = BATcount(r) <= 1;
		r->tkey = BATcount(r) <= 1;
		BBPkeepref(*res = r->batCacheid);
	} else if (r)
		BBPreclaim(r);
}

str
SQLdiff(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *c = NULL;
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		gdk_return gdk_code = GDK_SUCCEED;

		res = getArgReference_bat(stk, pci, 0);
		if ((!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1))))) {
			msg = createException(SQL, "sql.diff", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (pci->argc > 2) {
			if (isaBatType(getArgType(mb, pci, 2))) {
				if (!(r = COLnew(b->hseqbase, TYPE_bit, BATcount(b), TRANSIENT))) {
					msg = createException(SQL, "sql.diff", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				c = b;
				if ((!(b = BATdescriptor(*getArgReference_bat(stk, pci, 2))))) {
					msg = createException(SQL, "sql.diff", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				gdk_code = GDKanalyticaldiff(r, b, c, NULL, b->ttype);
			} else { /* the input is a constant, so the output is the previous sql.diff output */
				assert(b->ttype == TYPE_bit);
				BBPkeepref(*res = b->batCacheid);
				return MAL_SUCCEED;
			}
		} else {
			if (!(r = COLnew(b->hseqbase, TYPE_bit, BATcount(b), TRANSIENT))) {
				msg = createException(SQL, "sql.diff", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			gdk_code = GDKanalyticaldiff(r, b, NULL, NULL, b->ttype);
		}
		if (gdk_code != GDK_SUCCEED)
			msg = createException(SQL, "sql.diff", GDK_EXCEPTION);
	} else if (pci->argc > 2 && isaBatType(getArgType(mb, pci, 2))) {
		bit *restrict prev = getArgReference_bit(stk, pci, 1);

		res = getArgReference_bat(stk, pci, 0);
		if ((!(b = BATdescriptor(*getArgReference_bat(stk, pci, 2))))) {
			msg = createException(SQL, "sql.diff", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, TYPE_bit, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.diff", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}

		if (GDKanalyticaldiff(r, b, NULL, prev, b->ttype) != GDK_SUCCEED)
			msg = createException(SQL, "sql.diff", GDK_EXCEPTION);
	} else {
		bit *res = getArgReference_bit(stk, pci, 0);
		*res = FALSE;
	}

bailout:
	unfix_inputs(2, b, c);
	finalize_output(res, r, msg);
	return msg;
}

str
SQLwindow_bound(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	bool preceding;
	oid second_half;
	int unit, bound, excl, part_offset = (pci->argc > 6);
	bat *res = NULL;
	BAT *r = NULL, *b = NULL, *p = NULL, *l = NULL;

	if ((pci->argc != 6 && pci->argc != 7) || getArgType(mb, pci, part_offset + 2) != TYPE_int ||
		getArgType(mb, pci, part_offset + 3) != TYPE_int || getArgType(mb, pci, part_offset + 4) != TYPE_int) {
		throw(SQL, "sql.window_bound", SQLSTATE(42000) "Invalid arguments");
	}

	unit = *getArgReference_int(stk, pci, part_offset + 2);
	bound = *getArgReference_int(stk, pci, part_offset + 3);
	excl = *getArgReference_int(stk, pci, part_offset + 4);

	assert(unit >= 0 && unit <= 3);
	assert(bound >= 0 && bound <= 5);
	assert(excl >= 0 && excl <= 2);
	preceding = (bound % 2 == 0);
	second_half = !(bound < 2 || bound == 4);

	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		int tp1, tp2 = getArgType(mb, pci, part_offset + 5);
		ptr limit = NULL;
		bool is_a_bat;

		res = getArgReference_bat(stk, pci, 0);
		if ((!(b = BATdescriptor(*getArgReference_bat(stk, pci, part_offset + 1))))) {
			msg = createException(SQL, "sql.window_bound", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		tp1 = b->ttype;

		if (excl != 0) {
			msg = createException(SQL, "sql.window_bound", SQLSTATE(42000) "Only EXCLUDE NO OTHERS exclusion is currently implemented");
			goto bailout;
		}

		is_a_bat = isaBatType(tp2);
		if (is_a_bat)
			tp2 = getBatType(tp2);

		if (!(r = COLnew(b->hseqbase, TYPE_oid, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.window_bound", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (is_a_bat) { //SQL_CURRENT_ROW shall never fall in limit validation
			if ((!(l = BATdescriptor(*getArgReference_bat(stk, pci, part_offset + 5))))) {
				msg = createException(SQL, "sql.window_bound", SQLSTATE(HY005) "Cannot access column descriptor");
				goto bailout;
			}
		} else {
			limit = getArgReference(stk, pci, part_offset + 5);
		}
		if (part_offset) {
			if ((!(p = BATdescriptor(*getArgReference_bat(stk, pci, 1))))) {
				msg = createException(SQL, "sql.window_bound", SQLSTATE(HY005) "Cannot access column descriptor");
				goto bailout;
			}
		}

		//On RANGE frame, when "CURRENT ROW" is not specified, the ranges are calculated with SQL intervals in mind
		if (GDKanalyticalwindowbounds(r, b, p, l, limit, tp1, tp2, unit, preceding, second_half) != GDK_SUCCEED)
			msg = createException(SQL, "sql.window_bound", GDK_EXCEPTION);
	} else {
		oid *res = getArgReference_oid(stk, pci, 0);

		*res = preceding ? 0 : 1;
	}

bailout:
	unfix_inputs(3, b, p, l);
	finalize_output(res, r, msg);
	return msg;
}

str
SQLrow_number(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *p = NULL;
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	if (pci->argc != 4 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.row_number", SQLSTATE(42000) "row_number(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		BUN cnt;
		int j, *rp, *end;
		bit *np;

		res = getArgReference_bat(stk, pci, 0);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "sql.row_number", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, TYPE_int, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.row_number", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r->tsorted = r->trevsorted = BATcount(b) <= 1;

		cnt = BATcount(b);
		rp = (int*)Tloc(r, 0);
		if (isaBatType(getArgType(mb, pci, 2))) {
			/* order info not used */
			if (!(p = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
				msg = createException(SQL, "sql.row_number", SQLSTATE(HY005) "Cannot access column descriptor");
				goto bailout;
			}
			np = (bit*)Tloc(p, 0);
			end = rp + cnt;
			for(j=1; rp<end; j++, np++, rp++) {
				if (*np)
					j=1;
				*rp = j;
			}
		} else { /* single value, ie no partitions, order info not used */
			int icnt = (int) cnt;
			for(j=1; j<=icnt; j++, rp++)
				*rp = j;
			r->tsorted = true;
			r->tkey = true;
		}
		BATsetcount(r, cnt);
		r->tnonil = true;
		r->tnil = false;
	} else {
		int *res = getArgReference_int(stk, pci, 0);

		*res = 1;
	}

bailout:
	unfix_inputs(2, b, p);
	if (res && r && !msg) {
		r->tkey = BATcount(r) <= 1;
		BBPkeepref(*res = r->batCacheid);
	} else if (r)
		BBPreclaim(r);
	return msg;
}

str
SQLrank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL;
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	if (pci->argc != 4 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.rank", SQLSTATE(42000) "rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		BUN cnt;
		int j, k, *rp, *end;
		bit *np, *no;

		res = getArgReference_bat(stk, pci, 0);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, TYPE_int, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.rank", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r->tsorted = r->trevsorted = BATcount(b) <= 1;

		cnt = BATcount(b);
		rp = (int*)Tloc(r, 0);
		end = rp + cnt;
		if (isaBatType(getArgType(mb, pci, 2))) {
			if (isaBatType(getArgType(mb, pci, 3))) {
				if (!(p = BATdescriptor(*getArgReference_bat(stk, pci, 2))) || !(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
					msg = createException(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				np = (bit*)Tloc(p, 0);
				no = (bit*)Tloc(o, 0);
				for(j=1,k=1; rp<end; k++, np++, no++, rp++) {
					if (*np)
						j=k=1;
					if (*no)
						j=k;
					*rp = j;
				}
			} else { /* single value, ie no ordering */
				if (!(p = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
					msg = createException(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				np = (bit*)Tloc(p, 0);
				for(j=1; rp<end; np++, rp++) {
					if (*np)
						j=1;
					*rp = j;
				}
			}
		} else { /* single value, ie no partitions */
			if (isaBatType(getArgType(mb, pci, 3))) {
				if (!(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
					msg = createException(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				no = (bit*)Tloc(o, 0);
				for(j=1,k=1; rp<end; k++, no++, rp++) {
					if (*no)
						j=k;
					*rp = j;
				}
			} else { /* single value, ie no ordering */
				for(; rp<end; rp++)
					*rp = 1;
				r->tsorted = true;
				r->trevsorted = true;
			}
		}
		BATsetcount(r, cnt);
		r->tnonil = true;
		r->tnil = false;
	} else {
		int *res = getArgReference_int(stk, pci, 0);

		*res = 1;
	}

bailout:
	unfix_inputs(3, b, p, o);
	if (res && r && !msg) {
		r->tkey = BATcount(r) <= 1;
		BBPkeepref(*res = r->batCacheid);
	} else if (r)
		BBPreclaim(r);
	return msg;
}

str
SQLdense_rank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL;
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	if (pci->argc != 4 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.dense_rank", SQLSTATE(42000) "dense_rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		BUN cnt;
		int j, *rp, *end;
		bit *np, *no;

		res = getArgReference_bat(stk, pci, 0);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, TYPE_int, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.dense_rank", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r->tsorted = r->trevsorted = BATcount(b) <= 1;

		cnt = BATcount(b);
		rp = (int*)Tloc(r, 0);
		end = rp + cnt;
		if (isaBatType(getArgType(mb, pci, 2))) {
			if (isaBatType(getArgType(mb, pci, 3))) {
				if (!(p = BATdescriptor(*getArgReference_bat(stk, pci, 2))) || !(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
					msg = createException(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				np = (bit*)Tloc(p, 0);
				no = (bit*)Tloc(o, 0);
				for(j=1; rp<end; np++, no++, rp++) {
					if (*np)
						j=1;
					else if (*no)
						j++;
					*rp = j;
				}
			} else { /* single value, ie no ordering */
				if (!(p = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
					msg = createException(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				np = (bit*)Tloc(p, 0);
				for(j=1; rp<end; np++, rp++) {
					if (*np)
						j=1;
					*rp = j;
				}
			}
		} else { /* single value, ie no partitions */
			if (isaBatType(getArgType(mb, pci, 3))) {
				if (!(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
					msg = createException(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				no = (bit*)Tloc(o, 0);
				for(j=1; rp<end; no++, rp++) {
					if (*no)
						j++;
					*rp = j;
				}
			} else { /* single value, ie no ordering */
				for(; rp<end; rp++)
					*rp = 1;
				r->tsorted = true;
				r->trevsorted = true;
			}
		}
		BATsetcount(r, cnt);
		r->tnonil = true;
		r->tnil = false;
	} else {
		int *res = getArgReference_int(stk, pci, 0);

		*res = 1;
	}

bailout:
	unfix_inputs(3, b, p, o);
	if (res && r && !msg) {
		r->tkey = BATcount(r) <= 1;
		BBPkeepref(*res = r->batCacheid);
	} else if (r)
		BBPreclaim(r);
	return msg;
}

str
SQLpercent_rank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL;
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	if (pci->argc != 4 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.percent_rank", SQLSTATE(42000) "percent_rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		BUN ncnt, cnt;
		int j, k;
		dbl *rp, *end, cnt_cast;
		bit *np, *np2, *no, *no2;

		res = getArgReference_bat(stk, pci, 0);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.percent_rank", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r->tsorted = r->trevsorted = BATcount(b) <= 1;

		cnt = BATcount(b);
		rp = (dbl*)Tloc(r, 0);
		end = rp + cnt;
		if (isaBatType(getArgType(mb, pci, 2))) {
			if (isaBatType(getArgType(mb, pci, 3))) {
				if (!(p = BATdescriptor(*getArgReference_bat(stk, pci, 2))) || !(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
					msg = createException(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				np = (bit*)Tloc(p, 0);
				np2 = np + BATcount(p);
				no2 = no = (bit*)Tloc(o, 0);

				for (; np<np2; np++, no++) {
					if (*np) {
						ncnt = no - no2;
						if (ncnt == 1) {
							for (; no2<no; no2++, rp++)
								*rp = 0.0;
						} else {
							cnt_cast = (dbl) (ncnt - 1);
							j = 0;
							k = 0;
							for (; no2<no; k++, no2++, rp++) {
								if (*no2)
									j=k;
								*rp = j / cnt_cast;
							}
						}
					}
				}
				ncnt = no - no2;
				if (ncnt == 1) {
					for (; no2<no; no2++, rp++)
						*rp = 0.0;
				} else {
					cnt_cast = (dbl) (ncnt - 1);
					j = 0;
					k = 0;
					for (; no2<no; k++, no2++, rp++) {
						if (*no2)
							j=k;
						*rp = j / cnt_cast;
					}
				}
			} else { /* single value, ie no ordering */
				for(; rp<end; rp++)
					*rp = 0.0;
				r->tsorted = true;
				r->trevsorted = true;
			}
		} else { /* single value, ie no partitions */
			if (isaBatType(getArgType(mb, pci, 3))) {
				if (!(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
					msg = createException(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				no = (bit*)Tloc(o, 0);

				if (cnt == 1) {
					for (; rp<end; rp++)
						*rp = 0.0;
					r->tsorted = true;
					r->trevsorted = true;
				} else {
					cnt_cast = (dbl) (cnt - 1);
					for(j=0,k=0; rp<end; k++, no++, rp++) {
						if (*no)
							j=k;
						*rp = j / cnt_cast;
					}
				}
			} else { /* single value, ie no ordering */
				for(; rp<end; rp++)
					*rp = 0.0;
				r->tsorted = true;
				r->trevsorted = true;
			}
		}
		BATsetcount(r, cnt);
		r->tnonil = true;
		r->tnil = false;
	} else {
		dbl *res = getArgReference_dbl(stk, pci, 0);

		*res = 0.0;
	}

bailout:
	unfix_inputs(3, b, p, o);
	if (res && r && !msg) {
		r->tkey = BATcount(r) <= 1;
		BBPkeepref(*res = r->batCacheid);
	} else if (r)
		BBPreclaim(r);
	return msg;
}

str
SQLcume_dist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL;
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	if (pci->argc != 4 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.cume_dist", SQLSTATE(42000) "cume_dist(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		BUN ncnt, j = 0;
		bit *np, *no, *bo1, *bo2, *end;
		dbl *rb, *rp, cnt_cast, nres;

		res = getArgReference_bat(stk, pci, 0);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "sql.cume_dist", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.cume_dist", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r->tsorted = r->trevsorted = BATcount(b) <= 1;

		rb = rp = (dbl*)Tloc(r, 0);
		if (isaBatType(getArgType(mb, pci, 2))) {
			if (isaBatType(getArgType(mb, pci, 3))) {
				if (!(p = BATdescriptor(*getArgReference_bat(stk, pci, 2))) || !(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
					msg = createException(SQL, "sql.cume_dist", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				np = (bit*)Tloc(p, 0);
				end = np + BATcount(p);
				bo1 = bo2 = no = (bit*)Tloc(o, 0);

				for (; np<end; np++, no++) {
					if (*np) {
						ncnt = no - bo2;
						cnt_cast = (dbl) ncnt;
						j = 0;
						for (; bo2<no; bo2++) {
							if (*bo2) {
								j += (bo2 - bo1);
								nres = j / cnt_cast;
								for (; bo1 < bo2; bo1++, rb++)
									*rb = nres;
							}
						}
						for (; bo1 < bo2; bo1++, rb++)
							*rb = 1.0;
					}
				}
				j = 0;
				ncnt = no - bo2;
				cnt_cast = (dbl) ncnt;
				for (; bo2<no; bo2++) {
					if (*bo2) {
						j += (bo2 - bo1);
						nres = j / cnt_cast;
						for (; bo1 < bo2; bo1++, rb++)
							*rb = nres;
					}
				}
				for (; bo1 < bo2; bo1++, rb++)
					*rb = 1.0;
			} else { /* single value, ie no ordering */
				rp = rb + BATcount(b);
				for (; rb<rp; rb++)
					*rb = 1.0;
				r->tsorted = true;
				r->trevsorted = true;
			}
		} else { /* single value, ie no partitions */
			if (isaBatType(getArgType(mb, pci, 3))) {
				if (!(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
					msg = createException(SQL, "sql.cume_dist", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
				bo1 = bo2 = (bit*)Tloc(o, 0);
				no = bo1 + BATcount(b);
				cnt_cast = (dbl) BATcount(b);
				for (; bo2<no; bo2++) {
					if (*bo2) {
						j += (bo2 - bo1);
						nres = j / cnt_cast;
						for (; bo1 < bo2; bo1++, rb++)
							*rb = nres;
					}
				}
				for (; bo1 < bo2; bo1++, rb++)
					*rb = 1.0;
			} else { /* single value, ie no ordering */
				rp = rb + BATcount(b);
				for (; rb<rp; rb++)
					*rb = 1.0;
				r->tsorted = true;
				r->trevsorted = true;
			}
		}
		BATsetcount(r, BATcount(b));
		r->tnonil = true;
		r->tnil = false;
	} else {
		dbl *res = getArgReference_dbl(stk, pci, 0);

		*res = 1.0;
	}

bailout:
	unfix_inputs(3, b, p, o);
	if (res && r && !msg) {
		r->tkey = BATcount(r) <= 1;
		BBPkeepref(*res = r->batCacheid);
	} else if (r)
		BBPreclaim(r);
	return msg;
}

#define NTILE_VALUE_SINGLE_IMP(TPE) \
	do { \
		TPE val = *(TPE*) VALget(ntile); \
		if (!is_##TPE##_nil(val) && val < 1) { \
			msg = createException(SQL, "sql.ntile", SQLSTATE(42000) "ntile must be greater than zero"); \
			goto bailout; 	\
		}	\
		if (!is_##TPE##_nil(val)) \
			val = 1; \
		VALset(res, tp2, &val); \
	} while(0)

str
SQLntile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *p = NULL, *n = NULL;
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	(void)cntxt;
	if (pci->argc != 5 || (getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit) ||
		(getArgType(mb, pci, 4) != TYPE_bit && getBatType(getArgType(mb, pci, 4)) != TYPE_bit)) {
		throw(SQL, "sql.ntile", SQLSTATE(42000) "ntile(:any_1,:number,:bit,:bit)");
	}

	if (isaBatType(getArgType(mb, pci, 1))) {
		int tp2 = 0;
		ptr ntile = NULL;
		res = getArgReference_bat(stk, pci, 0);

		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "sql.ntile", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (isaBatType(getArgType(mb, pci, 2))) {
			tp2 = getBatType(getArgType(mb, pci, 2));
			if (!(n = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
				msg = createException(SQL, "sql.ntile", SQLSTATE(HY005) "Cannot access column descriptor");
				goto bailout;
			}
		} else {
			tp2 = getArgType(mb, pci, 2);
			ntile = getArgReference(stk, pci, 2);
		}
		if (!(r = COLnew(b->hseqbase, tp2, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.ntile", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (isaBatType(getArgType(mb, pci, 3)) && !(p = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
			msg = createException(SQL, "sql.ntile", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((p && BATcount(b) != BATcount(p)) || (n && BATcount(b) != BATcount(n))) {
			msg = createException(SQL, "sql.ntile", ILLEGAL_ARGUMENT " Requires bats of identical size");
			goto bailout;
		}

		if (GDKanalyticalntile(r, b, p, n, tp2, ntile) != GDK_SUCCEED)
			msg = createException(SQL, "sql.ntile", GDK_EXCEPTION);
	} else {
		ValRecord *res = &(stk)->stk[(pci)->argv[0]];
		ValRecord *ntile = &(stk)->stk[(pci)->argv[2]];
		int tp2 = getArgType(mb, pci, 2);

		switch (tp2) {
			case TYPE_bte:
				NTILE_VALUE_SINGLE_IMP(bte);
				break;
			case TYPE_sht:
				NTILE_VALUE_SINGLE_IMP(sht);
				break;
			case TYPE_int:
				NTILE_VALUE_SINGLE_IMP(int);
				break;
			case TYPE_lng:
				NTILE_VALUE_SINGLE_IMP(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				NTILE_VALUE_SINGLE_IMP(hge);
				break;
#endif
			default:
				msg = createException(SQL, "sql.ntile", SQLSTATE(42000) "ntile not available for %s", ATOMname(tp2));
		}
	}

bailout:
	unfix_inputs(3, b, p, n);
	finalize_output(res, r, msg);
	return MAL_SUCCEED;
}

static str
SQLanalytics_args(BAT **r, BAT **b, int *frame_type, BAT **p, BAT **o, BAT **s, BAT **e, Client cntxt, MalBlkPtr mb,
				  MalStkPtr stk, InstrPtr pci, int rtype, const char *mod)
{
	(void) cntxt;
	if (pci->argc != 7)
		throw(SQL, mod, ILLEGAL_ARGUMENT "%s requires exactly 7 arguments", mod);

	*frame_type = *getArgReference_int(stk, pci, 4);
	assert(*frame_type >= 0 && *frame_type <= 6);

	if (isaBatType(getArgType(mb, pci, 1)) && !(*b = BATdescriptor(*getArgReference_bat(stk, pci, 1))))
		throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
	if (*b && !(*r = COLnew((*b)->hseqbase, rtype ? rtype : (*b)->ttype, BATcount(*b), TRANSIENT)))
		throw(MAL, mod, SQLSTATE(HY013) MAL_MALLOC_FAIL); 
	if (isaBatType(getArgType(mb, pci, 2)) && !(*p = BATdescriptor(*getArgReference_bat(stk, pci, 2))))
		throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
	if ((*frame_type == 3 || *frame_type == 4) && isaBatType(getArgType(mb, pci, 3)) && !(*o = BATdescriptor(*getArgReference_bat(stk, pci, 3))))
		throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
	if (*frame_type < 3 && isaBatType(getArgType(mb, pci, 5)) && !(*s = BATdescriptor(*getArgReference_bat(stk, pci, 5))))
		throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
	if (*frame_type < 3 && isaBatType(getArgType(mb, pci, 6)) && !(*e = BATdescriptor(*getArgReference_bat(stk, pci, 6))))
		throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
	if ((*s && BATcount(*b) != BATcount(*s)) || (*e && BATcount(*b) != BATcount(*e)) ||
		(*p && BATcount(*b) != BATcount(*p)) || (*o && BATcount(*b) != BATcount(*o)))
		throw(SQL, mod, ILLEGAL_ARGUMENT " Requires bats of identical size");
	if ((*p && (*p)->ttype != TYPE_bit) || (*o && (*o)->ttype != TYPE_bit) || (*s && (*s)->ttype != TYPE_oid) || (*e && (*e)->ttype != TYPE_oid))
		throw(SQL, mod, ILLEGAL_ARGUMENT " p and o must be bit type and s and e must be oid");

	return MAL_SUCCEED;
}

static str
SQLanalytical_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *op,
				   gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, BAT *, BAT *, int, int))
{
	int tpe = getArgType(mb, pci, 1), frame_type;
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL, *s = NULL, *e = NULL;
	str msg = SQLanalytics_args(&r, &b, &frame_type, &p, &o, &s, &e, cntxt, mb, stk, pci, 0, op);
	bat *res = NULL;

	if (msg)
		goto bailout;
	if (b) {
		res = getArgReference_bat(stk, pci, 0);

		if (func(r, p, o, b, s, e, getBatType(tpe), frame_type) != GDK_SUCCEED)
			msg = createException(SQL, op, GDK_EXCEPTION);
	} else {
		ValRecord *res = &(stk)->stk[(pci)->argv[0]];
		ValRecord *in = &(stk)->stk[(pci)->argv[1]];

		if (!VALcopy(res, in))
			msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

bailout:
	unfix_inputs(5, b, p, o, s, e);
	finalize_output(res, r, msg);
	return msg;
}

static str
do_limit_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *op,
			   gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, int))
{
	int tpe = getArgType(mb, pci, 1);
	BAT *r = NULL, *b = NULL, *s = NULL, *e = NULL; /* p and o are ignored for this one */
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	if (pci->argc != 7)
		throw(SQL, op, ILLEGAL_ARGUMENT "%s requires exactly 7 arguments", op);
	tpe = getArgType(mb, pci, 1);

	if (isaBatType(tpe))
		tpe = getBatType(tpe);
	if (isaBatType(getArgType(mb, pci, 1))) {
		res = getArgReference_bat(stk, pci, 0);

		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, b->ttype, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (!(s = BATdescriptor(*getArgReference_bat(stk, pci, 5)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(e = BATdescriptor(*getArgReference_bat(stk, pci, 6)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((s && BATcount(b) != BATcount(s)) || (e && BATcount(b) != BATcount(e))) {
			msg = createException(SQL, op, ILLEGAL_ARGUMENT " Requires bats of identical size");
			goto bailout;
		}
		if ((s && s->ttype != TYPE_oid) || (e && e->ttype != TYPE_oid)) {
			msg = createException(SQL, op, ILLEGAL_ARGUMENT " p and o must be bit type and s and e must be oid");
			goto bailout;
		}

		if (func(r, b, s, e, tpe) != GDK_SUCCEED)
			msg = createException(SQL, op, GDK_EXCEPTION);
	} else {
		ValRecord *res = &(stk)->stk[(pci)->argv[0]];
		ValRecord *in = &(stk)->stk[(pci)->argv[1]];

		if (!VALcopy(res, in))
			msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

bailout:
	unfix_inputs(3, b, s, e);
	finalize_output(res, r, msg);
	return MAL_SUCCEED;
}

str
SQLfirst_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_limit_value(cntxt, mb, stk, pci, "sql.first_value", GDKanalyticalfirst);
}

str
SQLlast_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_limit_value(cntxt, mb, stk, pci, "sql.last_value", GDKanalyticallast);
}

#define NTH_VALUE_SINGLE_IMP(TPE) \
	do { \
		TPE val = *(TPE*) VALget(nth); \
		if (!VALisnil(nth) && val < 1) \
			throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value must be greater than zero"); \
		if (VALisnil(nth) || val > 1) { \
			ValRecord def = (ValRecord) {.vtype = TYPE_void,}; \
			if (!VALinit(&def, tp1, ATOMnilptr(tp1)) || !VALcopy(res, &def)) { \
				VALclear(&def); \
				throw(SQL, "sql.nth_value", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			} \
			VALclear(&def); \
		} else { \
			if (!VALcopy(res, in)) \
				throw(SQL, "sql.nth_value", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
		} \
	} while(0)

str
SQLnth_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *l = NULL, *s = NULL, *e = NULL; /* p and o are ignored for this one */
	int tpe;
	bat *res = NULL;
	str msg = MAL_SUCCEED;
	bool is_a_bat;

	(void) cntxt;
	if (pci->argc != 8)
		throw(SQL, "sql.nth_value", ILLEGAL_ARGUMENT "sql.nth_value requires exactly 8 arguments");

	tpe = getArgType(mb, pci, 1);
	is_a_bat = isaBatType(getArgType(mb, pci, 2));

	if (isaBatType(tpe)) {
		lng *nth = NULL;
		res = getArgReference_bat(stk, pci, 0);

		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		tpe = getBatType(tpe);
		if (b && !(r = COLnew(b->hseqbase, tpe, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.nth_value", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (is_a_bat) {
			if (!(l = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
				msg = createException(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
				goto bailout;
			}
		} else {
			nth = getArgReference_lng(stk, pci, 2);
		}
		if (!(s = BATdescriptor(*getArgReference_bat(stk, pci, 6)))) {
			msg = createException(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(e = BATdescriptor(*getArgReference_bat(stk, pci, 7)))) {
			msg = createException(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((s && BATcount(b) != BATcount(s)) || (e && BATcount(b) != BATcount(e)) || (l && BATcount(b) != BATcount(l))) {
			msg = createException(SQL, "sql.nth_value", ILLEGAL_ARGUMENT " Requires bats of identical size");
			goto bailout;
		}
		if ((s && s->ttype != TYPE_oid) || (e && e->ttype != TYPE_oid)) {
			msg = createException(SQL, "sql.nth_value", ILLEGAL_ARGUMENT " p and o must be bit type and s and e must be oid");
			goto bailout;
		}

		if (GDKanalyticalnthvalue(r, b, s, e, l, nth, tpe) != GDK_SUCCEED)
			msg = createException(SQL, "sql.nth_value", GDK_EXCEPTION);
	} else {
		ValRecord *res = &(stk)->stk[(pci)->argv[0]];
		ValRecord *in = &(stk)->stk[(pci)->argv[1]];
		lng nth = 0;

		if (getArgType(mb, pci, 2) != TYPE_lng) {
			msg = createException(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value offset not available for type %s", ATOMname(getArgType(mb, pci, 2)));
			goto bailout;
		}
		nth = *getArgReference_lng(stk, pci, 2);
		if (!is_lng_nil(nth) && nth < 1) {
			msg = createException(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value must be greater than zero");
			goto bailout;
		}
		if (is_lng_nil(nth) || nth > 1) {
			ValRecord def = (ValRecord) {.vtype = TYPE_void,};
			if (!VALinit(&def, tpe, ATOMnilptr(tpe)) || !VALcopy(res, &def)) {
				VALclear(&def);
				msg = createException(SQL, "sql.nth_value", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			VALclear(&def);
		} else {
			if (!VALcopy(res, in))
				msg = createException(SQL, "sql.nth_value", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}

bailout:
	unfix_inputs(4, b, l, s, e);
	finalize_output(res, r, msg);
	return msg;
}

#define CHECK_L_VALUE(TPE) \
	do { \
		TPE rval; \
		if (tp2_is_a_bat) { \
			if (!(l = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) { \
				msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor"); \
				goto bailout; \
			} \
			rval = ((TPE*)Tloc(l, 0))[0]; \
		} else { \
			rval = *getArgReference_##TPE(stk, pci, 2); \
		} \
		if (!is_##TPE##_nil(rval) && rval < 0) { \
			gdk_call = dual; \
			rval *= -1; \
		} \
		l_value = is_##TPE##_nil(rval) ? BUN_NONE : (BUN)rval; \
	} while(0)

static str
do_lead_lag(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *op, const char* desc,
			gdk_return (*func)(BAT *, BAT *, BAT *, BUN, const void* restrict, int),
			gdk_return (*dual)(BAT *, BAT *, BAT *, BUN, const void* restrict, int))
{
	int tp1, tp2, tp3, base = 2;
	BUN l_value = 1;
	const void *restrict default_value;
	gdk_return (*gdk_call)(BAT *, BAT *, BAT *, BUN, const void* restrict, int) = func;
	BAT *b = NULL, *l = NULL, *d = NULL, *p = NULL, *r = NULL;
	bool tp2_is_a_bat;
	str msg = MAL_SUCCEED;
	bat *res = NULL;

	(void)cntxt;
	if (pci->argc < 4 || pci->argc > 6)
		throw(SQL, op, SQLSTATE(42000) "%s called with invalid number of arguments", desc);

	tp1 = getArgType(mb, pci, 1);

	if (pci->argc > 4) { //contains (lag or lead) value;
		tp2 = getArgType(mb, pci, 2);
		tp2_is_a_bat = isaBatType(tp2);
		if (tp2_is_a_bat)
			tp2 = getBatType(tp2);

		switch (tp2) {
			case TYPE_bte:
				CHECK_L_VALUE(bte);
				break;
			case TYPE_sht:
				CHECK_L_VALUE(sht);
				break;
			case TYPE_int:
				CHECK_L_VALUE(int);
				break;
			case TYPE_lng:
				CHECK_L_VALUE(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				CHECK_L_VALUE(hge);
				break;
#endif
			default:
				throw(SQL, op, SQLSTATE(42000) "%s value not available for %s", desc, ATOMname(tp2));
		}
		base = 3;
	}

	if (pci->argc > 5) { //contains default value;
		tp3 = getArgType(mb, pci, 3);
		if (isaBatType(tp3)) {
			BATiter bpi;

			tp3 = getBatType(tp3);
			if (!(d = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
				msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
				goto bailout;
			}
			bpi = bat_iterator(d);
			default_value = BUNtail(bpi, 0);
		} else {
			ValRecord *in = &(stk)->stk[(pci)->argv[3]];
			default_value = VALget(in);
		}
		base = 4;
	} else {
		int tpe = tp1;
		if (isaBatType(tpe))
			tpe = getBatType(tp1);
		default_value = ATOMnilptr(tpe);
	}

	assert(default_value); //default value must be set

	if (isaBatType(tp1)) {
		res = getArgReference_bat(stk, pci, 0);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}

		tp1 = getBatType(tp1);
		if (!(r = COLnew(b->hseqbase, tp1, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (isaBatType(getArgType(mb, pci, base))) {
			if (!(p = BATdescriptor(*getArgReference_bat(stk, pci, base)))) {
				msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
				goto bailout;
			}
		}
		if ((p && BATcount(b) != BATcount(p)) || (l && BATcount(b) != BATcount(l)) || (d && BATcount(b) != BATcount(d))) {
			msg = createException(SQL, op, ILLEGAL_ARGUMENT " Requires bats of identical size");
			goto bailout;
		}

		if (gdk_call(r, b, p, l_value, default_value, tp1) != GDK_SUCCEED)
			msg = createException(SQL, op, GDK_EXCEPTION);
	} else {
		ValRecord *res = &(stk)->stk[(pci)->argv[0]];
		ValRecord *in = &(stk)->stk[(pci)->argv[1]];

		if (l_value == 0) {
			if (!VALcopy(res, in))
				msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else {
			ValRecord def = (ValRecord) {.vtype = TYPE_void,};

			if (!VALinit(&def, tp1, default_value) || !VALcopy(res, &def))
				msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			VALclear(&def);
		}
	}

bailout:
	unfix_inputs(4, b, p, l, d);
	finalize_output(res, r, msg);
	return msg;
}

str
SQLlag(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_lead_lag(cntxt, mb, stk, pci, "sql.lag", "lag", GDKanalyticallag, GDKanalyticallead);
}

str
SQLlead(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_lead_lag(cntxt, mb, stk, pci, "sql.lead", "lead", GDKanalyticallead, GDKanalyticallag);
}

str
SQLmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SQLanalytical_func(cntxt, mb, stk, pci, "sql.min", GDKanalyticalmin);
}

str
SQLmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SQLanalytical_func(cntxt, mb, stk, pci, "sql.max", GDKanalyticalmax);
}

str
SQLcount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL, *s = NULL, *e = NULL;
	int tpe, frame_type;
	bit ignore_nils;
	bat *res = NULL;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	if (pci->argc != 8)
		throw(SQL, "sql.count", ILLEGAL_ARGUMENT "sql.count requires exactly 8 arguments");
	tpe = getArgType(mb, pci, 1);
	ignore_nils = *getArgReference_bit(stk, pci, 2);
	frame_type = *getArgReference_int(stk, pci, 5);
	assert(frame_type >= 0 && frame_type <= 6);

	if (isaBatType(tpe))
		tpe = getBatType(tpe);
	if (isaBatType(getArgType(mb, pci, 1)) && (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1))))) {
		msg = createException(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (b && !(r = COLnew(b->hseqbase, TYPE_lng, BATcount(b), TRANSIENT))) {
		msg = createException(SQL, "sql.count", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (isaBatType(getArgType(mb, pci, 3)) && !(p = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
		msg = createException(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if ((frame_type == 3 || frame_type == 4) && isaBatType(getArgType(mb, pci, 4)) && !(o = BATdescriptor(*getArgReference_bat(stk, pci, 4)))) {
		msg = createException(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (frame_type < 3 && isaBatType(getArgType(mb, pci, 6)) && !(s = BATdescriptor(*getArgReference_bat(stk, pci, 6)))) {
		msg = createException(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (frame_type < 3 && isaBatType(getArgType(mb, pci, 7)) && !(e = BATdescriptor(*getArgReference_bat(stk, pci, 7)))) {
		msg = createException(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if ((s && BATcount(b) != BATcount(s)) || (e && BATcount(b) != BATcount(e)) || (p && BATcount(b) != BATcount(p)) || (o && BATcount(b) != BATcount(o))) {
		msg = createException(SQL, "sql.count", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if ((p && p->ttype != TYPE_bit) || (o && o->ttype != TYPE_bit) || (s && s->ttype != TYPE_oid) || (e && e->ttype != TYPE_oid)) {
		msg = createException(SQL, "sql.count", ILLEGAL_ARGUMENT " p and o must be bit type and s and e must be oid");
		goto bailout;
	}

	if (b) {
		res = getArgReference_bat(stk, pci, 0);

		if (GDKanalyticalcount(r, p, o, b, s, e, ignore_nils, tpe, frame_type) != GDK_SUCCEED)
			msg = createException(SQL, "sql.count", GDK_EXCEPTION);
	} else {
		lng *res = getArgReference_lng(stk, pci, 0);
		ValRecord *in = &(stk)->stk[(pci)->argv[1]];

		*res = (VALisnil(in) && ignore_nils) ? 0 : 1;
	}

bailout:
	unfix_inputs(5, b, p, o, s, e);
	finalize_output(res, r, msg);
	return msg;
}

static str
do_analytical_sumprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *op,
					  gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, BAT *, BAT *, int, int, int))
{
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL, *s = NULL, *e = NULL;
	int tp1, tp2, frame_type;
	str msg = MAL_SUCCEED;
	bat *res = NULL;

	(void) cntxt;
	if (pci->argc != 7)
		throw(SQL, op, ILLEGAL_ARGUMENT "%s requires exactly 7 arguments", op);
	tp1 = getArgType(mb, pci, 1);
	frame_type = *getArgReference_int(stk, pci, 4);
	assert(frame_type >= 0 && frame_type <= 6);

	if (isaBatType(tp1)) {
		tp1 = getBatType(tp1);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
	}
	switch (tp1) {
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_int:
		case TYPE_lng:
#ifdef HAVE_HGE
		case TYPE_hge:
			tp2 = TYPE_hge;
#else
			tp2 = TYPE_lng;
#endif
			break;
		case TYPE_flt:
			tp2 = TYPE_flt;
			break;
		case TYPE_dbl:
			tp2 = TYPE_dbl;
			break;
		default: {
			msg = createException(SQL, op, SQLSTATE(42000) "%s not available for %s", op, ATOMname(tp1));
			goto bailout;
		}
	}
	if (b) {
		res = getArgReference_bat(stk, pci, 0);
		if (!(r = COLnew(b->hseqbase, tp2, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (isaBatType(getArgType(mb, pci, 2)) && !(p = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((frame_type == 3 || frame_type == 4) && isaBatType(getArgType(mb, pci, 3)) && !(o = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (frame_type < 3 && isaBatType(getArgType(mb, pci, 5)) && !(s = BATdescriptor(*getArgReference_bat(stk, pci, 5)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (frame_type < 3 && isaBatType(getArgType(mb, pci, 6)) && !(e = BATdescriptor(*getArgReference_bat(stk, pci, 6)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((s && BATcount(b) != BATcount(s)) || (e && BATcount(b) != BATcount(e)) || (p && BATcount(b) != BATcount(p)) || (o && BATcount(b) != BATcount(o))) {
			msg = createException(SQL, op, ILLEGAL_ARGUMENT " Requires bats of identical size");
			goto bailout;
		}
		if ((p && p->ttype != TYPE_bit) || (o && o->ttype != TYPE_bit) || (s && s->ttype != TYPE_oid) || (e && e->ttype != TYPE_oid)) {
			msg = createException(SQL, op, ILLEGAL_ARGUMENT " p and o must be bit type and s and e must be oid");
			goto bailout;
		}

		if (func(r, p, o, b, s, e, tp1, tp2, frame_type) != GDK_SUCCEED)
			msg = createException(SQL, op, GDK_EXCEPTION);
	} else {
		/* the pointers here will always point from bte to dbl, so no strings are handled here */
		ptr res = getArgReference(stk, pci, 0);
		ptr in = getArgReference(stk, pci, 1);
		int scale = 0;

		switch (tp1) {
#ifdef HAVE_HGE
			case TYPE_bte:
				msg = bte_dec2_hge((hge*)res, &scale, (bte*)in);
				break;
			case TYPE_sht:
				msg = sht_dec2_hge((hge*)res, &scale, (sht*)in);
				break;
			case TYPE_int:
				msg = int_dec2_hge((hge*)res, &scale, (int*)in);
				break;
			case TYPE_lng:
				msg = lng_dec2_hge((hge*)res, &scale, (lng*)in);
				break;
			case TYPE_hge:
				*(hge*)res = *((hge*)in);
				break;
#else
			case TYPE_bte:
				msg = bte_dec2_lng((lng*)res, &scale, (bte*)in);
				break;
			case TYPE_sht:
				msg = sht_dec2_lng((lng*)res, &scale, (sht*)in);
				break;
			case TYPE_int:
				msg = int_dec2_lng((lng*)res, &scale, (int*)in);
				break;
			case TYPE_lng:
				*(lng*)res = *((lng*)in);
				break;
#endif
			case TYPE_flt: {
				flt fp = *((flt*)in);
				*(dbl*)res = is_flt_nil(fp) ? dbl_nil : (dbl) fp;
			} break;
			case TYPE_dbl:
				*(dbl*)res = *((dbl*)in);
				break;
			default:
				msg = createException(SQL, op, SQLSTATE(42000) "%s not available for %s", op, ATOMname(tp1));
		}
	}

bailout:
	unfix_inputs(5, b, p, o, s, e);
	finalize_output(res, r, msg);
	return msg;
}

str
SQLsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_analytical_sumprod(cntxt, mb, stk, pci, "sql.sum", GDKanalyticalsum);
}

str
SQLprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_analytical_sumprod(cntxt, mb, stk, pci, "sql.prod", GDKanalyticalprod);
}

str
SQLavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1), frame_type;
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL, *s = NULL, *e = NULL;
	str msg = SQLanalytics_args(&r, &b, &frame_type, &p, &o, &s, &e, cntxt, mb, stk, pci, TYPE_dbl, "sql.avg");
	bat *res = NULL;

	if (msg)
		goto bailout;
	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (b) {
		res = getArgReference_bat(stk, pci, 0);

		if (GDKanalyticalavg(r, p, o, b, s, e, tpe, frame_type) != GDK_SUCCEED)
			msg = createException(SQL, "sql.avg", GDK_EXCEPTION);
	} else {
		/* the pointers here will always point from bte to dbl, so no strings are handled here */
		ptr res = getArgReference(stk, pci, 0);
		ptr in = getArgReference(stk, pci, 1);
		int scale = 0;

		switch (tpe) {
			case TYPE_bte:
				msg = bte_dec2_dbl((dbl*)res, &scale, (bte*)in);
				break;
			case TYPE_sht:
				msg = sht_dec2_dbl((dbl*)res, &scale, (sht*)in);
				break;
			case TYPE_int:
				msg = int_dec2_dbl((dbl*)res, &scale, (int*)in);
				break;
			case TYPE_lng:
				msg = lng_dec2_dbl((dbl*)res, &scale, (lng*)in);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				msg = hge_dec2_dbl((dbl*)res, &scale, (hge*)in);
				break;
#endif
			case TYPE_flt: {
				flt fp = *((flt*)in);
				*(dbl*)res = is_flt_nil(fp) ? dbl_nil : (dbl) fp;
			} break;
			case TYPE_dbl:
				*(dbl*)res = *((dbl*)in);
				break;
			default:
				msg = createException(SQL, "sql.avg", SQLSTATE(42000) "sql.avg not available for %s to dbl", ATOMname(tpe));
		}
	}

bailout:
	unfix_inputs(5, b, p, o, s, e);
	finalize_output(res, r, msg);
	return msg;
}

str
SQLavginteger(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1), frame_type;
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL, *s = NULL, *e = NULL;
	str msg = SQLanalytics_args(&r, &b, &frame_type, &p, &o, &s, &e, cntxt, mb, stk, pci, 0, "sql.avg");
	bat *res = NULL;

	if (msg)
		goto bailout;
	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (b) {
		res = getArgReference_bat(stk, pci, 0);

		if (GDKanalyticalavginteger(r, p, o, b, s, e, tpe, frame_type) != GDK_SUCCEED)
			msg = createException(SQL, "sql.avg", GDK_EXCEPTION);
	} else {
		ValRecord *res = &(stk)->stk[(pci)->argv[0]];
		ValRecord *in = &(stk)->stk[(pci)->argv[1]];

		switch (tpe) {
			case TYPE_bte:
			case TYPE_sht:
			case TYPE_int:
			case TYPE_lng:
#ifdef HAVE_HGE
			case TYPE_hge:
#endif
				if (!VALcopy(res, in))
					msg = createException(SQL, "sql.avg", SQLSTATE(HY013) MAL_MALLOC_FAIL); /* malloc failure should never happen, but let it be here */
				break;
			default:
				msg = createException(SQL, "sql.avg", SQLSTATE(42000) "sql.avg not available for %s to %s", ATOMname(tpe), ATOMname(tpe));
		}
	}

bailout:
	unfix_inputs(5, b, p, o, s, e);
	finalize_output(res, r, msg);
	return msg;
}

static str
do_stddev_and_variance(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *op,
					   gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, BAT *, BAT *, int, int))
{
	int tpe = getArgType(mb, pci, 1), frame_type;
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL, *s = NULL, *e = NULL;
	str msg = SQLanalytics_args(&r, &b, &frame_type, &p, &o, &s, &e, cntxt, mb, stk, pci, TYPE_dbl, op);
	bat *res = NULL;

	if (msg)
		goto bailout;
	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (b) {
		res = getArgReference_bat(stk, pci, 0);

		if (func(r, p, o, b, s, e, tpe, frame_type) != GDK_SUCCEED)
			msg = createException(SQL, op, GDK_EXCEPTION);
	} else {
		dbl *res = getArgReference_dbl(stk, pci, 0);
		ValRecord *input1 = &(stk)->stk[(pci)->argv[1]];

		switch (tpe) {
			case TYPE_bte:
			case TYPE_sht:
			case TYPE_int:
			case TYPE_lng:
#ifdef HAVE_HGE
			case TYPE_hge:
#endif
			case TYPE_flt:
			case TYPE_dbl:
				*res = VALisnil(input1) ? dbl_nil : 0;
				break;
			default:
				msg = createException(SQL, op, SQLSTATE(42000) "%s not available for %s", op, ATOMname(tpe));
		}
	}

bailout:
	unfix_inputs(5, b, p, o, s, e);
	finalize_output(res, r, msg);
	return msg;
}

str
SQLstddev_samp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_stddev_and_variance(cntxt, mb, stk, pci, "sql.stdev", GDKanalytical_stddev_samp);
}

str
SQLstddev_pop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_stddev_and_variance(cntxt, mb, stk, pci, "sql.stdevp", GDKanalytical_stddev_pop);
}

str
SQLvar_samp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_stddev_and_variance(cntxt, mb, stk, pci, "sql.variance", GDKanalytical_variance_samp);
}

str
SQLvar_pop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_stddev_and_variance(cntxt, mb, stk, pci, "sql.variancep", GDKanalytical_variance_pop);
}

#define COVARIANCE_AND_CORRELATION_ONE_SIDE_UNBOUNDED_TILL_CURRENT_ROW(TPE) \
	do { \
		TPE *restrict bp = (TPE*)Tloc(d, 0); \
		for (; k < i;) { \
			j = k; \
			do {	\
				n += !is_##TPE##_nil(bp[k]);	\
				k++; \
			} while (k < i && !opp[k]);	\
			if (n > minimum) { /* covariance_samp requires at least one value */ \
				rr = val; \
			} else { \
				rr = dbl_nil; \
				has_nils = true; \
			} \
			for (; j < k; j++) \
				rb[j] = rr; \
		} \
		n = 0;	\
		k = i; \
	} while (0)

#define COVARIANCE_AND_CORRELATION_ONE_SIDE_CURRENT_ROW_TILL_UNBOUNDED(TPE) \
	do { \
		TPE *restrict bp = (TPE*)Tloc(d, 0); \
		l = i - 1; \
		for (j = l; ; j--) { \
			n += !is_##TPE##_nil(bp[j]);	\
			if (opp[j] || j == k) {	\
				if (n > minimum) { /* covariance_samp requires at least one value */ \
					rr = val; \
				} else { \
					rr = dbl_nil; \
					has_nils = true; \
				} \
				for (; ; l--) { \
					rb[l] = rr; \
					if (l == j)	\
						break;	\
				} \
				if (j == k)	\
					break;	\
				l = j - 1;	\
			}	\
		}	\
		n = 0;	\
		k = i; \
	} while (0)

#define COVARIANCE_AND_CORRELATION_ONE_SIDE_ALL_ROWS(TPE) \
	do { \
		TPE *restrict bp = (TPE*)Tloc(d, 0); \
		for (; j < i; j++) \
			n += !is_##TPE##_nil(bp[j]);	\
		if (n > minimum) { /* covariance_samp requires at least one value */ \
			rr = val; \
		} else { \
			rr = dbl_nil; \
			has_nils = true; \
		} \
		for (; k < i; k++) \
			rb[k] = rr;	\
		n = 0; \
	} while (0)

#define COVARIANCE_AND_CORRELATION_ONE_SIDE_CURRENT_ROW(TPE) \
	do { \
		TPE *restrict bp = (TPE*)Tloc(d, 0); \
		for (; k < i; k++) { \
			n += !is_##TPE##_nil(bp[k]);	\
			if (n > minimum) { /* covariance_samp requires at least one value */ \
				rb[k] = val; \
			} else { \
				rb[k] = dbl_nil; \
				has_nils = true; \
			} \
			n = 0; \
		}	\
	} while (0)

#define INIT_AGGREGATE_COUNT(TPE, NOTHING1, NOTHING2) \
	do { \
		computed = 0; \
	} while (0)
#define COMPUTE_LEVEL0_COUNT_FIXED(X, TPE, NOTHING1, NOTHING2) \
	do { \
		computed = !is_##TPE##_nil(bp[j + X]); \
	} while (0)
#define COMPUTE_LEVELN_COUNT(VAL, NOTHING1, NOTHING2, NOTHING3) \
	do { \
		computed += VAL; \
	} while (0)
#define FINALIZE_AGGREGATE_COUNT(NOTHING1, NOTHING2, NOTHING3) \
	do { \
		if (computed > minimum) { /* covariance_samp requires at least one value */ \
			rb[k] = val; \
		} else { \
			rb[k] = dbl_nil; \
			has_nils = true; \
		} \
	} while (0)
#define COVARIANCE_AND_CORRELATION_ONE_SIDE_OTHERS(TPE)	\
	do { \
		TPE *restrict bp = (TPE*)Tloc(d, 0); \
		oid ncount = i - k; \
		if (GDKrebuild_segment_tree(ncount, sizeof(lng), &segment_tree, &tree_capacity, &levels_offset, &nlevels) != GDK_SUCCEED) { \
			msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			goto bailout; \
		} \
		populate_segment_tree(lng, ncount, INIT_AGGREGATE_COUNT, COMPUTE_LEVEL0_COUNT_FIXED, COMPUTE_LEVELN_COUNT, TPE, NOTHING, NOTHING); \
		for (; k < i; k++) \
			compute_on_segment_tree(lng, start[k] - j, end[k] - j, INIT_AGGREGATE_COUNT, COMPUTE_LEVELN_COUNT, FINALIZE_AGGREGATE_COUNT, TPE, NOTHING, NOTHING); \
		j = k; \
	} while (0)

#define COVARIANCE_AND_CORRELATION_ONE_SIDE_PARTITIONS(TPE, IMP)		\
	do {						\
		if (p) {					\
			for (; i < cnt; i++) {		\
				if (np[i]) 	{		\
covariance##TPE##IMP: \
					IMP(TPE);	\
				} \
			}						\
		}	\
		if (!last) { /* hack to reduce code explosion, there's no need to duplicate the code to iterate each partition */ \
			last = true; \
			i = cnt; \
			goto covariance##TPE##IMP; \
		} \
	} while (0)

#ifdef HAVE_HGE
#define COVARIANCE_AND_CORRELATION_ONE_SIDE_LIMIT(IMP) \
	case TYPE_hge: \
		COVARIANCE_AND_CORRELATION_ONE_SIDE_PARTITIONS(hge, COVARIANCE_AND_CORRELATION_ONE_SIDE_##IMP); \
	break;
#else
#define COVARIANCE_AND_CORRELATION_ONE_SIDE_LIMIT(IMP)
#endif

#define COVARIANCE_AND_CORRELATION_ONE_SIDE_BRANCHES(IMP)		\
	do { \
		switch (tp1) {	\
		case TYPE_bte:	\
			COVARIANCE_AND_CORRELATION_ONE_SIDE_PARTITIONS(bte, COVARIANCE_AND_CORRELATION_ONE_SIDE_##IMP);	\
			break;	\
		case TYPE_sht:	\
			COVARIANCE_AND_CORRELATION_ONE_SIDE_PARTITIONS(sht, COVARIANCE_AND_CORRELATION_ONE_SIDE_##IMP);	\
			break;	\
		case TYPE_int:	\
			COVARIANCE_AND_CORRELATION_ONE_SIDE_PARTITIONS(int, COVARIANCE_AND_CORRELATION_ONE_SIDE_##IMP);	\
			break;	\
		case TYPE_lng:	\
			COVARIANCE_AND_CORRELATION_ONE_SIDE_PARTITIONS(lng, COVARIANCE_AND_CORRELATION_ONE_SIDE_##IMP);	\
			break;	\
		case TYPE_flt:	\
			COVARIANCE_AND_CORRELATION_ONE_SIDE_PARTITIONS(flt, COVARIANCE_AND_CORRELATION_ONE_SIDE_##IMP);	\
			break;	\
		case TYPE_dbl:	\
			COVARIANCE_AND_CORRELATION_ONE_SIDE_PARTITIONS(dbl, COVARIANCE_AND_CORRELATION_ONE_SIDE_##IMP);	\
			break;	\
		COVARIANCE_AND_CORRELATION_ONE_SIDE_LIMIT(IMP)	\
		default: {	\
			msg = createException(SQL, op, SQLSTATE(42000) "%s not available for %s", op, ATOMname(tp1)); \
			goto bailout; \
		} \
		}	\
	} while (0)

static str
do_covariance_and_correlation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *op,
							  gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, BAT *, BAT *, BAT *, int, int), lng minimum, dbl defaultv, dbl single_case)
{
	BAT *r = NULL, *b = NULL, *c = NULL, *p = NULL, *o = NULL, *s = NULL, *e = NULL;
	int tp1, tp2, frame_type;
	bool is_a_bat1, is_a_bat2;
	str msg = MAL_SUCCEED;
	bat *res = NULL;
	void *segment_tree = NULL;
	oid *levels_offset = NULL;

	(void)cntxt;
	if (pci->argc != 8)
		throw(SQL, op, ILLEGAL_ARGUMENT "%s requires exactly 8 arguments", op);

	tp1 = getArgType(mb, pci, 1);
	tp2 = getArgType(mb, pci, 2);
	frame_type = *getArgReference_int(stk, pci, 5);
	assert(frame_type >= 0 && frame_type <= 6);
	is_a_bat1 = isaBatType(tp1);
	is_a_bat2 = isaBatType(tp2);

	if (is_a_bat1)
		tp1 = getBatType(tp1);
	if (is_a_bat2)
		tp2 = getBatType(tp2);
	if (tp1 != tp2)
		throw(SQL, op, SQLSTATE(42000) "The input arguments for %s must be from the same type", op);

	if (is_a_bat1 || is_a_bat2) {
		res = getArgReference_bat(stk, pci, 0);

		if (is_a_bat1 && !(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (is_a_bat2 && !(c = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, op, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (isaBatType(getArgType(mb, pci, 3)) && !(p = BATdescriptor(*getArgReference_bat(stk, pci, 3)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((frame_type == 3 || frame_type == 4) && isaBatType(getArgType(mb, pci, 4)) && !(o = BATdescriptor(*getArgReference_bat(stk, pci, 4)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (frame_type < 3 && isaBatType(getArgType(mb, pci, 6)) && !(s = BATdescriptor(*getArgReference_bat(stk, pci, 6)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (frame_type < 3 && isaBatType(getArgType(mb, pci, 7)) && !(e = BATdescriptor(*getArgReference_bat(stk, pci, 7)))) {
			msg = createException(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((s && BATcount(b) != BATcount(s)) || (e && BATcount(b) != BATcount(e)) || (p && BATcount(b) != BATcount(p)) || (o && BATcount(b) != BATcount(o)) || (c && BATcount(b) != BATcount(c))) {
			msg = createException(SQL, op, ILLEGAL_ARGUMENT " Requires bats of identical size");
			goto bailout;
		}
		if ((p && p->ttype != TYPE_bit) || (o && o->ttype != TYPE_bit) || (s && s->ttype != TYPE_oid) || (e && e->ttype != TYPE_oid)) {
			msg = createException(SQL, op, ILLEGAL_ARGUMENT " p and o must be bit type and s and e must be oid");
			goto bailout;
		}

		if (is_a_bat1 && is_a_bat2) {
			if (func(r, p, o, b, c, s, e, tp1, frame_type) != GDK_SUCCEED)
				msg = createException(SQL, op, GDK_EXCEPTION);
		} else {
			/* corner case, second column is a constant, calculate it this way... */
			BAT *d = b ? b : c;
			ValRecord *input2 = &(stk)->stk[(pci)->argv[b ? 2 : 1]];
			oid i = 0, j = 0, k = 0, l = 0, cnt = BATcount(d), *restrict start = s ? (oid*)Tloc(s, 0) : NULL, *restrict end = e ? (oid*)Tloc(e, 0) : NULL,
				tree_capacity = 0, nlevels = 0;
			lng n = 0;
			bit *np = p ? Tloc(p, 0) : NULL, *opp = o ? Tloc(o, 0) : NULL;
			dbl *restrict rb = (dbl *) Tloc(r, 0), val = VALisnil(input2) ? dbl_nil : defaultv, rr;
			bool has_nils = is_dbl_nil(val), last = false;

			if (cnt > 0) {
				switch (frame_type) {
				case 3: /* unbounded until current row */	{
					COVARIANCE_AND_CORRELATION_ONE_SIDE_BRANCHES(UNBOUNDED_TILL_CURRENT_ROW);
				} break;
				case 4: /* current row until unbounded */	{
					COVARIANCE_AND_CORRELATION_ONE_SIDE_BRANCHES(CURRENT_ROW_TILL_UNBOUNDED);
				} break;
				case 5: /* all rows */	{
					COVARIANCE_AND_CORRELATION_ONE_SIDE_BRANCHES(ALL_ROWS);
				} break;
				case 6: /* current row */ {
					COVARIANCE_AND_CORRELATION_ONE_SIDE_BRANCHES(CURRENT_ROW);
				} break;
				default: {
					COVARIANCE_AND_CORRELATION_ONE_SIDE_BRANCHES(OTHERS);
				}
				}
			}

			BATsetcount(r, cnt);
			r->tnonil = !has_nils;
			r->tnil = has_nils;
		}
	} else {
		dbl *res = getArgReference_dbl(stk, pci, 0);
		ValRecord *input1 = &(stk)->stk[(pci)->argv[1]];
		ValRecord *input2 = &(stk)->stk[(pci)->argv[2]];

		switch (tp1) {
			case TYPE_bte:
			case TYPE_sht:
			case TYPE_int:
			case TYPE_lng:
#ifdef HAVE_HGE
			case TYPE_hge:
#endif
			case TYPE_flt:
			case TYPE_dbl:
				*res = (VALisnil(input1) || VALisnil(input2)) ? dbl_nil : single_case;
				break;
			default:
				msg = createException(SQL, op, SQLSTATE(42000) "%s not available for %s", op, ATOMname(tp1));
		}
	}

bailout:
	GDKfree(segment_tree);
	unfix_inputs(6, b, c, p, o, s, e);
	finalize_output(res, r, msg);
	return msg;
}

str
SQLcovar_samp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_covariance_and_correlation(cntxt, mb, stk, pci, "sql.covariance", GDKanalytical_covariance_samp, 1, 0.0f, dbl_nil);
}

str
SQLcovar_pop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_covariance_and_correlation(cntxt, mb, stk, pci, "sql.covariancep", GDKanalytical_covariance_pop, 0, 0.0f, 0.0f);
}

str
SQLcorr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_covariance_and_correlation(cntxt, mb, stk, pci, "sql.corr", GDKanalytical_correlation, 0, dbl_nil, dbl_nil);
}

str
SQLstrgroup_concat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *sep = NULL, *p = NULL, *o = NULL, *s = NULL, *e = NULL;
	int separator_offset = 0, tpe, frame_type;
	str msg = MAL_SUCCEED, separator = NULL;
	bat *res = NULL;

	(void)cntxt;
	if (pci->argc != 7 && pci->argc != 8)
		throw(SQL, "sql.strgroup_concat", ILLEGAL_ARGUMENT "sql.strgroup_concat requires 7 or 8 arguments");

	tpe = getArgType(mb, pci, 2);
	if (isaBatType(tpe))
		tpe = getBatType(tpe);
	if (tpe == TYPE_str) /* there's a separator */
		separator_offset = 1;
	else
		assert(tpe == TYPE_bit); /* otherwise it must be the partition's type */

	frame_type = *getArgReference_int(stk, pci, 4 + separator_offset);
	assert(frame_type >= 0 && frame_type <= 6);

	if (isaBatType(getArgType(mb, pci, 1))) {
		res = getArgReference_bat(stk, pci, 0);

		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "sql.strgroup_concat", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (!(r = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT))) {
			msg = createException(SQL, "sql.strgroup_concat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (separator_offset) {
			if (isaBatType(getArgType(mb, pci, 2))) {
				if (!(sep = BATdescriptor(*getArgReference_bat(stk, pci, 2)))) {
					msg = createException(SQL, "sql.strgroup_concat", SQLSTATE(HY005) "Cannot access column descriptor");
					goto bailout;
				}
			} else
				separator = *getArgReference_str(stk, pci, 2);
		} else
			separator = ",";

		if (isaBatType(getArgType(mb, pci, 2 + separator_offset)) && !(p = BATdescriptor(*getArgReference_bat(stk, pci, 2 + separator_offset)))) {
			msg = createException(SQL, "sql.strgroup_concat", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((frame_type == 3 || frame_type == 4) && isaBatType(getArgType(mb, pci, 3 + separator_offset)) && !(o = BATdescriptor(*getArgReference_bat(stk, pci, 3 + separator_offset)))) {
			msg = createException(SQL, "sql.strgroup_concat", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (frame_type < 3 && isaBatType(getArgType(mb, pci, 5 + separator_offset)) && !(s = BATdescriptor(*getArgReference_bat(stk, pci, 5 + separator_offset)))) {
			msg = createException(SQL, "sql.strgroup_concat", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if (frame_type < 3 && isaBatType(getArgType(mb, pci, 6 + separator_offset)) && !(e = BATdescriptor(*getArgReference_bat(stk, pci, 6 + separator_offset)))) {
			msg = createException(SQL, "sql.strgroup_concat", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		if ((s && BATcount(b) != BATcount(s)) || (e && BATcount(b) != BATcount(e)) || (p && BATcount(b) != BATcount(p)) || (o && BATcount(b) != BATcount(o)) || (sep && BATcount(b) != BATcount(sep))) {
			msg = createException(SQL, "sql.strgroup_concat", ILLEGAL_ARGUMENT " Requires bats of identical size");
			goto bailout;
		}
		if ((p && p->ttype != TYPE_bit) || (o && o->ttype != TYPE_bit) || (s && s->ttype != TYPE_oid) || (e && e->ttype != TYPE_oid)) {
			msg = createException(SQL, "sql.strgroup_concat", ILLEGAL_ARGUMENT " p and o must be bit type and s and e must be oid");
			goto bailout;
		}

		assert((separator && !sep) || (!separator && sep)); /* only one of them must be set */
		if (GDKanalytical_str_group_concat(r, p, o, b, sep, s, e, separator, frame_type) != GDK_SUCCEED)
			msg = createException(SQL, "sql.strgroup_concat", GDK_EXCEPTION);
	} else {
		str *res = getArgReference_str(stk, pci, 0);
		str in = *getArgReference_str(stk, pci, 1);

		if (strNil(in)) {
			*res = GDKstrdup(str_nil);
		} else if (separator_offset) {
			str sep = *getArgReference_str(stk, pci, 2);
			size_t l1 = strlen(in), l2 = strNil(sep) ? 0 : strlen(sep);

			if ((*res = GDKmalloc(l1+l2+1))) {
				if (l1)
					memcpy(*res, in, l1);
				if (l2)
					memcpy((*res)+l1, sep, l2);
				(*res)[l1+l2] = '\0';
			}
		} else {
			*res = GDKstrdup(in);
		}
		if (!*res)
			msg = createException(SQL, "sql.strgroup_concat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

bailout:
	unfix_inputs(6, b, sep, p, o, s, e);
	finalize_output(res, r, msg);
	return msg;
}
