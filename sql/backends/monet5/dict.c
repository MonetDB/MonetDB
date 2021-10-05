
#include "monetdb_config.h"
#include "sql.h"
#include "mal.h"
#include "mal_client.h"

#include "dict.h"

str
DICTcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	/* always assume one result */
	str msg = MAL_SUCCEED;
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *tname = *getArgReference_str(stk, pci, 2);
	const char *cname = *getArgReference_str(stk, pci, 3);
	backend *be = NULL;
	sql_trans *tr = NULL;

	if (!sname || !tname || !cname)
		throw(SQL, "dict.compress", SQLSTATE(3F000) "dict compress: invalid column name");
	if ((msg = getBackendContext(cntxt, &be)) != MAL_SUCCEED)
		return msg;
	tr = be->mvc->session->tr;

	sql_schema *s = find_sql_schema(tr, sname);
	if (!s)
		throw(SQL, "dict.compress", SQLSTATE(3F000) "schema '%s' unknown", sname);
	sql_table *t = find_sql_table(tr, s, tname);
	if (!t)
		throw(SQL, "dict.compress", SQLSTATE(3F000) "table '%s.%s' unknown", sname, tname);
	sql_column *c = find_sql_column(t, cname);
	if (!c)
		throw(SQL, "dict.compress", SQLSTATE(3F000) "column '%s.%s.%s' unknown", sname, tname, cname);

	sqlstore *store = tr->store;
	BAT *b = store->storage_api.bind_col(tr, c, RDONLY);

	/* for now use all rows */
	BAT *u = BATunique(b, NULL);
	if (!u)
		throw(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BUN cnt = BATcount(u);
	/* create hash on u */
	int tt = (cnt<256)?TYPE_bte:(cnt<(64*1024))?TYPE_sht:TYPE_int;
	if (cnt > 2L*1024*1024*1024) {
		bat_destroy(u);
		bat_destroy(b);
		throw(SQL, "dict.compress", SQLSTATE(3F000) "dict compress: too many values");
	}
	BAT *uv = BATproject(u, b); /* get values */
	bat_destroy(u);
	if (!uv) {
		bat_destroy(b);
		throw(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
    BAT *uu = COLcopy(uv, uv->ttype, true, PERSISTENT);
	if (!uu) {
		bat_destroy(uv);
		throw(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bat_destroy(uv);
	u = uu;

	BAT *o = COLnew(b->hseqbase, tt, BATcount(b), PERSISTENT);
	if (!o || BAThash(u) != GDK_SUCCEED) {
		bat_destroy(u);
		throw(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BUN p, q;
	BATiter bi = bat_iterator(b);
	BATiter ui = bat_iterator_nolock(u);
	if (tt == TYPE_bte) {
		bte *op = (bte*)Tloc(o, 0);
		BATloop(b, p, q) {
			BUN up = 0;
			HASHloop(ui, ui.b->thash, up, BUNtail(bi, p)) {
				op[p] = up;
			}
		}
		BATsetcount(o, BATcount(b));
		o->tsorted = (u->tsorted && b->tsorted);
		o->trevsorted = false;
		o->tnil = b->tnil;
		o->tnonil = b->tnonil;
		o->tkey = b->tkey;
		if (sql_trans_alter_storage(tr, c, "DICT") != LOG_OK || store->storage_api.col_dict(tr, c, o, u) != LOG_OK) {
			bat_iterator_end(&bi);
			throw(SQL, "dict.compress", SQLSTATE(HY013) "alter_storage failed");
		}
	} else if (tt == TYPE_sht) {
		sht *op = (sht*)Tloc(o, 0);
		BATloop(b, p, q) {
			BUN up = 0;
			HASHloop(ui, ui.b->thash, up, BUNtail(bi, p)) {
				op[p] = up;
			}
		}
		BATsetcount(o, BATcount(b));
		o->tsorted = (u->tsorted && b->tsorted);
		o->trevsorted = false;
		o->tnil = b->tnil;
		o->tnonil = b->tnonil;
		o->tkey = b->tkey;
		if (sql_trans_alter_storage(tr, c, "DICT") != LOG_OK || store->storage_api.col_dict(tr, c, o, u) != LOG_OK) {
			bat_iterator_end(&bi);
			throw(SQL, "dict.compress", SQLSTATE(HY013) "alter_storage failed");
		}
	} else {
		printf("implement int cases \n");
	}
	bat_iterator_end(&bi);
	bat_destroy(b);
	bat_destroy(u);
	bat_destroy(o);
	return MAL_SUCCEED;
}


str
DICTdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* b = project(o:bat[:bte], u) */
	/* b = project(o:bat[:sht], u) */
	/* b = project(o:bat[:int], u) */
	(void)cntxt;
	(void)mb;
	bat *r = getArgReference_bat(stk, pci, 0);
	bat O = *getArgReference_bat(stk, pci, 1);
	bat U = *getArgReference_bat(stk, pci, 2);

	BAT *o = BATdescriptor(O);
	BAT *u = BATdescriptor(U);
	if (!o || !u) {
		bat_destroy(o);
		bat_destroy(u);
		throw(SQL, "dict.decompress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BAT *b = COLnew(o->hseqbase, u->ttype, BATcount(o), TRANSIENT);
	BUN p, q;
	BATiter oi = bat_iterator(o);
	BATiter ui = bat_iterator_nolock(u);
	if (o->ttype == TYPE_bte) {
		unsigned char *op = Tloc(o, 0);
		BATloop(o, p, q) {
			BUN up = op[p];
	        if (BUNappend(b, BUNtail(ui, up), false) != GDK_SUCCEED) {
				bat_iterator_end(&oi);
				bat_destroy(b);
				bat_destroy(o);
				bat_destroy(u);
				throw(SQL, "dict.decompress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	} else if (o->ttype == TYPE_sht) {
		unsigned short *op = Tloc(o, 0);
		BATloop(o, p, q) {
			BUN up = op[p];
	        if (BUNappend(b, BUNtail(ui, up), false) != GDK_SUCCEED) {
				bat_iterator_end(&oi);
				bat_destroy(b);
				bat_destroy(o);
				bat_destroy(u);
				throw(SQL, "dict.decompress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	} else if (o->ttype == TYPE_int) {
		assert(0);
	} else {
		bat_iterator_end(&oi);
		bat_destroy(b);
		bat_destroy(o);
		bat_destroy(u);
		throw(SQL, "dict.decompress", SQLSTATE(HY013) "unknown offset type");
	}
	bat_iterator_end(&oi);
	BBPkeepref(*r = b->batCacheid);
	bat_destroy(o);
	bat_destroy(u);
	return MAL_SUCCEED;
}

str
DICTconvert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* convert candidates into bte,sht,int offsets */
	(void)cntxt;
	bat *r = getArgReference_bat(stk, pci, 0);
	bat O = *getArgReference_bat(stk, pci, 1);
	int rt = getBatType(getArgType(mb, pci, 0));

	BAT *o = BATdescriptor(O);
	if (!o)
		throw(SQL, "dict.convert", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BAT *b = COLnew(o->hseqbase, rt, BATcount(o), TRANSIENT);

	BUN p, q;
	BATiter oi = bat_iterator(o);
	if (rt == TYPE_bte) {
		unsigned char *rp = Tloc(b, 0);
		oid *op = Tloc(o, 0);
		BATloop(o, p, q) {
			rp[p] = op[p];
		}
	} else if (rt == TYPE_sht) {
		unsigned short *rp = Tloc(b, 0);
		oid *op = Tloc(o, 0);
		BATloop(o, p, q) {
			rp[p] = op[p];
		}
	} else {
		assert(0);
	}
	bat_iterator_end(&oi);
	BATsetcount(b, BATcount(o));
	/* TODO correct props and set min/max offset */
	BBPkeepref(*r = b->batCacheid);
	bat_destroy(o);
	return MAL_SUCCEED;
}

static void
BATnegateprops(BAT *b)
{
	/* disable all properties here */
	b->tsorted = false;
	b->trevsorted = false;
	b->tnosorted = 0;
	b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
}

/* renumber lo iff rv0 is sorted and dense directly lookup in rv1
 *					if not dense (ie missing matches on the right side), first check (ie output
 *					too large values for a match ie BATcount(rv1))
 * else sort rv0 -> reorder (project) rv1, then lookup etc in rv1
* */
static BAT *
DICTrenumber( BAT *o, BAT *lc, BAT *rc, BUN offcnt)
{
	BAT *olc = lc, *orc = rc, *no = NULL;
	BUN cnt = BATcount(o);

	if (!lc->tsorted) {
		BAT *nlc = NULL, *order = NULL;
		int ret = BATsort(&nlc, &order, NULL, lc, NULL, NULL, false, false, false);
		if (ret != GDK_SUCCEED)
			return no;
		BAT *nrc = order;

		if (!nlc || !nrc) {
			bat_destroy(nlc);
			bat_destroy(nrc);
			return no;
		}
		lc = nlc;
		rc = nrc;
	}
	/* dense or cheap dense check */
	if (!BATtdense(lc) && !(lc->tsorted && lc->tkey && BATcount(lc) == offcnt && *(oid*)Tloc(lc, offcnt-1) == offcnt-1)) {
		BAT *nrc = COLnew(0, rc->ttype, offcnt, TRANSIENT);

		/* create map with holes filled in */
		if (o->ttype == TYPE_bte) {
			bte *op = Tloc(nrc, 0);
			unsigned char *ip = Tloc(rc, 0);
			unsigned char *lp = Tloc(lc, 0);
			for(BUN i = 0, j = 0; i<offcnt; i++) {
				if (lp[j] > i) {
					op[i] = offcnt;
				} else {
					op[i] = ip[j++];
				}
			}
		} else if (o->ttype == TYPE_sht) {
			sht *op = Tloc(nrc, 0);
			unsigned short *ip = Tloc(rc, 0);
			unsigned short *lp = Tloc(lc, 0);
			for(BUN i = 0, j = 0; i<offcnt; i++) {
				if (lp[j] > i) {
					op[i] = offcnt;
				} else {
					op[i] = ip[j++];
				}
			}
		} else {
			assert(0);
		}
		if (orc != rc)
			bat_destroy(rc);
		rc = nrc;
	}

	no = COLnew(o->hseqbase, o->ttype, cnt, TRANSIENT);
	if (o->ttype == TYPE_bte) {
		bte *op = Tloc(no, 0);
		unsigned char *ip = Tloc(o, 0);
		oid *c = Tloc(rc, 0);
		for(BUN i = 0; i<cnt; i++) {
			op[i] = ip[i]==offcnt?offcnt:c[ip[i]];
		}
		BATsetcount(no, cnt);
		BATnegateprops(no);
		no->tkey = o->tkey;
	} else if (o->ttype == TYPE_sht) {
		sht *op = Tloc(no, 0);
		unsigned short *ip = Tloc(o, 0);
		oid *c = Tloc(rc, 0);
		for(BUN i = 0; i<cnt; i++) {
			op[i] = ip[i]==offcnt?offcnt:c[ip[i]];
		}
		BATsetcount(no, cnt);
		BATnegateprops(no);
		no->tkey = o->tkey;
	} else {
		assert(0);
	}
	if (olc != lc)
		bat_destroy(lc);
	if (orc != rc)
		bat_destroy(rc);
	return no;
}

/* simple join operator with on both sides a (different) dictionary
 * (r0, r1) = dict.join(lo, lv, ro, rv, lcand, rcand, ... ) */
str
DICTjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *R0 = getArgReference_bat(stk, pci, 0);
	bat *R1 = getArgReference_bat(stk, pci, 1);
	bat LO = *getArgReference_bat(stk, pci, 2);
	bat LV = *getArgReference_bat(stk, pci, 3);
	bat RO = *getArgReference_bat(stk, pci, 4);
	bat RV = *getArgReference_bat(stk, pci, 5);
	bat LC = *getArgReference_bat(stk, pci, 6);
	bat RC = *getArgReference_bat(stk, pci, 7);
	BAT *lc = NULL, *rc = NULL, *r0 = NULL, *r1 = NULL;
	bit nil_matches = *getArgReference_bit(stk, pci, 8);
	lng estimate = *getArgReference_lng(stk, pci, 9);
	str res = NULL;
	int err = 0;

	BAT *lo = BATdescriptor(LO);
	BAT *lv = BATdescriptor(LV);
	BAT *ro = BATdescriptor(RO);
	BAT *rv = BATdescriptor(RV);

	if (!lo || !lv || !ro || !rv) {
		bat_destroy(lo);
		bat_destroy(lv);
		bat_destroy(ro);
		bat_destroy(rv);
		throw(SQL, "dict.join", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (!is_bat_nil(LC))
		lc = BATdescriptor(LC);
	if (!is_bat_nil(RC))
		rc = BATdescriptor(RC);
	/* if both are the same, continue with join on indices */
	if (lv->batCacheid != rv->batCacheid) {
		/* first join values of the dicts */
		BAT *rv0 = NULL, *rv1 = NULL;

		if (BATjoin(&rv0, &rv1, lv, rv, NULL, NULL, nil_matches, BATcount(lv)) != GDK_SUCCEED) {
			err = 1;
		} else {
			/* input was unique, ie we do expect at least one dense candidate list */
			if (!BATtdense(rv0) || !BATtdense(rv1)) { /* the same again */
				/* smallest offset needs renumbering */
				if (BATcount(lo) < BATcount(ro)) {
					BAT *nlo = DICTrenumber(lo, rv0, rv1, BATcount(lv));
					bat_destroy(lo);
					lo = nlo;
				} else {
					BAT *nro = DICTrenumber(ro, rv1, rv0, BATcount(rv));
					bat_destroy(ro);
					ro = nro;
				}
				if (!lo || !ro)
					err = 1;
			}
			bat_destroy(rv0);
			bat_destroy(rv1);
		}
	}
	if (!err) {
		if (BATjoin(&r0, &r1, lo, ro, lc, rc, TRUE /* nil offset should match */, estimate) != GDK_SUCCEED)
			err = 1;
	}
	bat_destroy(lo);
	bat_destroy(lv);
	bat_destroy(ro);
	bat_destroy(rv);
	bat_destroy(lc);
	bat_destroy(rc);
	if (r0)
		BBPkeepref(*R0 = r0->batCacheid);
	if (r1)
		BBPkeepref(*R1 = r1->batCacheid);
	if (err)
		throw(MAL, "BATjoin", GDK_EXCEPTION);
	return res;
}
