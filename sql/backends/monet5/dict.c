
#include "monetdb_config.h"
#include "sql.h"
#include "mal.h"
#include "mal_client.h"

#include "dict.h"

static sql_column *
get_newcolumn(sql_trans *tr, sql_column *c)
{
	sql_table *t = find_sql_table_id(tr, c->t->s, c->t->base.id);
	if (t)
		return find_sql_column(t, c->base.name);
	return NULL;
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

str
DICTcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	/* always assume one result */
	str msg = MAL_SUCCEED;
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *tname = *getArgReference_str(stk, pci, 2);
	const char *cname = *getArgReference_str(stk, pci, 3);
	const bit ordered = (pci->argc > 4)?*getArgReference_bit(stk, pci, 4):FALSE;
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
	assert(u->tkey);

	BUN cnt = BATcount(u);
	/* create hash on u */
	int tt = (cnt<256)?TYPE_bte:(cnt<(64*1024))?TYPE_sht:TYPE_int;
	if (cnt > (BUN)2*1024*1024*1024) {
		bat_destroy(u);
		bat_destroy(b);
		throw(SQL, "dict.compress", SQLSTATE(3F000) "dict compress: too many values");
	}
	BAT *uv = BATproject(u, b); /* get values */
	uv->tkey = true;
	bat_destroy(u);
	if (!uv) {
		bat_destroy(b);
		throw(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
    BAT *uu = NULL;
	if (ordered) {
			if (BATsort(&uu, NULL, NULL, uv, NULL, NULL, false, false, false) != GDK_SUCCEED) {
				bat_destroy(uv);
				bat_destroy(b);
				throw(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			bat_destroy(uv);
			uv = uu;
	}
	uu = COLcopy(uv, uv->ttype, true, PERSISTENT);
	bat_destroy(uv);
	assert(uu->tkey);
	if (!uu) {
		bat_destroy(b);
		throw(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
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
				op[p] = (bte)up;
			}
		}
		BATsetcount(o, BATcount(b));
		o->tsorted = (u->tsorted && b->tsorted);
		o->trevsorted = false;
		o->tnil = b->tnil;
		o->tnonil = b->tnonil;
		o->tkey = b->tkey;
		if (sql_trans_alter_storage(tr, c, "DICT") != LOG_OK || (c=get_newcolumn(tr, c)) == NULL || store->storage_api.col_dict(tr, c, o, u) != LOG_OK) {
			bat_iterator_end(&bi);
			bat_destroy(b);
			bat_destroy(u);
			bat_destroy(o);
			throw(SQL, "dict.compress", SQLSTATE(HY013) "alter_storage failed");
		}
		BUN minpos = BUN_NONE, maxpos = BUN_NONE;
		bte m = BATcount(u) - 1;
		bte minval = m<0?-128:0;
		bte maxval = m<0?127:m;
		o->tnil = m<0?true:false;
		o->tnonil = m<=0?false:true;
		BATloop(o, p, q) {
			if (op[p] == minval) {
				minpos = p;
				break;
			}
		}
		BATloop(o, p, q) {
			if (op[p] == maxval) {
				maxpos = p;
				break;
			}
		}
		o->tminpos = minpos;
		o->tmaxpos = maxpos;
	} else if (tt == TYPE_sht) {
		sht *op = (sht*)Tloc(o, 0);
		BATloop(b, p, q) {
			BUN up = 0;
			HASHloop(ui, ui.b->thash, up, BUNtail(bi, p)) {
				op[p] = (sht)up;
			}
		}
		BATsetcount(o, BATcount(b));
		o->tsorted = (u->tsorted && b->tsorted);
		o->trevsorted = false;
		o->tnil = b->tnil;
		o->tnonil = b->tnonil;
		o->tkey = b->tkey;
		if (sql_trans_alter_storage(tr, c, "DICT") != LOG_OK || (c=get_newcolumn(tr, c)) == NULL || store->storage_api.col_dict(tr, c, o, u) != LOG_OK) {
			bat_iterator_end(&bi);
			bat_destroy(b);
			bat_destroy(u);
			bat_destroy(o);
			throw(SQL, "dict.compress", SQLSTATE(HY013) "alter_storage failed");
		}
		BUN minpos = BUN_NONE, maxpos = BUN_NONE;
		sht m = BATcount(u) - 1;
		sht minval = m<0?-32768:0;
		sht maxval = m<0?32767:m;
		o->tnil = m<0?true:false;
		o->tnonil = m<=0?false:true;
		BATloop(o, p, q) {
			if (op[p] == minval) {
				minpos = p;
				break;
			}
		}
		BATloop(o, p, q) {
			if (op[p] == maxval) {
				maxpos = p;
				break;
			}
		}
		o->tminpos = minpos;
		o->tmaxpos = maxpos;
	} else {
		printf("implement int cases \n");
	}
	bat_iterator_end(&bi);
	bat_destroy(b);
	bat_destroy(u);
	bat_destroy(o);
	return MAL_SUCCEED;
}


/* improve decompress of int,lng,hge types */
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
	if (!b) {
		bat_destroy(o);
		bat_destroy(u);
		throw(SQL, "dict.decompress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BUN p, q;
	BATiter oi = bat_iterator(o);
	BATiter ui = bat_iterator_nolock(u);
	if (o->ttype == TYPE_bte) {
		unsigned char *op = Tloc(o, 0);

		if (ATOMstorage(u->ttype) == TYPE_int) {
			int *up = Tloc(u, 0);
			int *bp = Tloc(b, 0);

			BATloop(o, p, q) {
				bp[p] = up[op[p]];
			}
			BATsetcount(b, BATcount(o));
			BATnegateprops(b);
		} else if (ATOMstorage(u->ttype) == TYPE_lng) {
			lng *up = Tloc(u, 0);
			lng *bp = Tloc(b, 0);

			BATloop(o, p, q) {
				bp[p] = up[op[p]];
			}
			BATsetcount(b, BATcount(o));
			BATnegateprops(b);
		} else {
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
		}
	} else if (o->ttype == TYPE_sht) {
		unsigned short *op = Tloc(o, 0);

		if (ATOMstorage(u->ttype) == TYPE_int) {
			int *up = Tloc(u, 0);
			int *bp = Tloc(b, 0);

			BATloop(o, p, q) {
				bp[p] = up[op[p]];
			}
			BATsetcount(b, BATcount(o));
			BATnegateprops(b);
		} else if (ATOMstorage(u->ttype) == TYPE_lng) {
			lng *up = Tloc(u, 0);
			lng *bp = Tloc(b, 0);

			BATloop(o, p, q) {
				bp[p] = up[op[p]];
			}
			BATsetcount(b, BATcount(o));
			BATnegateprops(b);
		} else {
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

static BAT *
convert_oid( BAT *o, int rt)
{
	BUN p, q;
	BATiter oi = bat_iterator(o);
	BAT *b = COLnew(o->hseqbase, rt, BATcount(o), TRANSIENT);
	int brokenrange = 0, nil = 0;

	if (!b)
		return NULL;
	if (rt == TYPE_bte) {
		unsigned char *rp = Tloc(b, 0);
		if (o->ttype == TYPE_void) {
			BATloop(o, p, q) {
				rp[p] = (unsigned char) (p+o->tseqbase);
				brokenrange |= ((bte)rp[p] < 0);
				nil |= ((bte)rp[p] == bte_nil);
			}
		} else {
			oid *op = Tloc(o, 0);
			BATloop(o, p, q) {
				rp[p] = (unsigned char) op[p];
				brokenrange |= ((bte)rp[p] < 0);
				nil |= ((bte)rp[p] == bte_nil);
			}
		}
	} else if (rt == TYPE_sht) {
		unsigned short *rp = Tloc(b, 0);
		if (o->ttype == TYPE_void) {
			BATloop(o, p, q) {
				rp[p] = (unsigned short) (p+o->tseqbase);
				brokenrange |= ((short)rp[p] < 0);
				nil |= ((short)rp[p] == sht_nil);
			}
		} else {
			oid *op = Tloc(o, 0);
			BATloop(o, p, q) {
				rp[p] = (unsigned short) op[p];
				brokenrange |= ((short)rp[p] < 0);
				nil |= ((short)rp[p] == sht_nil);
			}
		}
	} else {
		assert(0);
	}
	bat_iterator_end(&oi);
	BATsetcount(b, BATcount(o));
	BATnegateprops(b);
	if (!brokenrange)
		b->tsorted = o->tsorted;
	b->tkey = o->tkey;
	if (nil) {
		b->tnil = true;
		b->tnonil = false;
	} else {
		b->tnil = false;
		b->tnonil = true;
	}
	return b;
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

	BAT *b = convert_oid(o, rt);
	if (!b) {
		bat_destroy(o);
		throw(SQL, "dict.convert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BBPkeepref(*r = b->batCacheid);
	bat_destroy(o);
	return MAL_SUCCEED;
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
		BAT *nlc = NULL, *nrc = NULL;
		int ret = BATsort(&nlc, &nrc, NULL, lc, NULL, NULL, false, false, false);

		if (ret != GDK_SUCCEED || !nlc || !nrc) {
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
		if (!nrc) {
			if (lc != olc)
				bat_destroy(lc);
			if (rc != orc)
				bat_destroy(rc);
			return no;
		}

		/* create map with holes filled in */
		oid *op = Tloc(nrc, 0);
		oid *ip = Tloc(rc, 0);
		unsigned char *lp = Tloc(lc, 0);
		for(BUN i = 0, j = 0; i<offcnt; i++) {
			if (lp[j] > i) {
				op[i] = offcnt;
			} else {
				op[i] = ip[j++];
			}
		}
		BATsetcount(nrc, offcnt);
		BATnegateprops(nrc);
		nrc->tkey = rc->tkey;
		if (orc != rc)
			bat_destroy(rc);
		rc = nrc;
	}

	no = COLnew(o->hseqbase, o->ttype, cnt, TRANSIENT);
	if (!no) {
		if (lc != olc)
			bat_destroy(lc);
		if (rc != orc)
			bat_destroy(rc);
		return no;
	}
	if (o->ttype == TYPE_bte) {
		bte *op = Tloc(no, 0);
		unsigned char *ip = Tloc(o, 0);
		oid *c = Tloc(rc, 0);
		for(BUN i = 0; i<cnt; i++) {
			op[i] = (bte) ((BUN)ip[i]==offcnt?offcnt:c[ip[i]]);
		}
		BATsetcount(no, cnt);
		BATnegateprops(no);
		no->tkey = o->tkey;
	} else if (o->ttype == TYPE_sht) {
		sht *op = Tloc(no, 0);
		unsigned short *ip = Tloc(o, 0);
		oid *c = Tloc(rc, 0);
		for(BUN i = 0; i<cnt; i++) {
			op[i] = (sht) ((BUN)ip[i]==offcnt?offcnt:c[ip[i]]);
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
		if (BATjoin(&r0, &r1, lo, ro, lc, rc, TRUE /* nil offset should match */, is_lng_nil(estimate) ? BUN_NONE : (BUN) estimate) != GDK_SUCCEED)
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

str
DICTselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *R0 = getArgReference_bat(stk, pci, 0);
	bat LO = *getArgReference_bat(stk, pci, 1);
	bat LC = *getArgReference_bat(stk, pci, 2);
	bat LV = *getArgReference_bat(stk, pci, 3);
	ptr v = getArgReference(stk, pci, 4);
	const char *op = *getArgReference_str(stk, pci, 5);

	BAT *lc = NULL, *bn = NULL;
	BAT *lo = BATdescriptor(LO);
	BAT *lv = BATdescriptor(LV);
	BUN p = BUN_NONE;

	if (!lo || !lv) {
		bat_destroy(lo);
		bat_destroy(lv);
		throw(SQL, "dict.thetaselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (!is_bat_nil(LC))
		lc = BATdescriptor(LC);
	if (op[0] == '=' || ((op[0] == '<' || op[0] == '>') && lv->tsorted)) {
		if (ATOMvarsized(lv->ttype))
			v = *(ptr*)v;
		if (op[0] == '=')
			p =  BUNfnd(lv, v);
		else if (op[0] == '<')
			p = SORTfndlast(lv, v);
		else if (op[0] == '>')
			p = SORTfndfirst(lv, v);
		if (p != BUN_NONE) {
			if (lo->ttype == TYPE_bte) {
				bte val = p;
				bn =  BATthetaselect(lo, lc, &val, op);
			} else if (lo->ttype == TYPE_sht) {
				sht val = p;
				bn =  BATthetaselect(lo, lc, &val, op);
			} else
				assert(0);
		} else {
			bn = BATdense(0, 0, 0);
		}
	} else { /* select + intersect */
		bn = BATthetaselect(lv, NULL, v, op);
		/* call dict convert */
		if (bn) {
			BAT *c = convert_oid(bn, lo->ttype);
			bat_destroy(bn);
			bn = c;
		}
		if (bn) {
			BAT *n = BATintersect(lo, bn, lc, NULL, true, true, BATcount(lo));
			bat_destroy(bn);
			bn = n;
		}
	}
	bat_destroy(lo);
	bat_destroy(lv);
	bat_destroy(lc);
	if (!bn)
		throw(SQL, "dict.thetaselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	BBPkeepref(*R0 = bn->batCacheid);
	return MAL_SUCCEED;
}
