/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

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
	b->tnonil = false;
	b->tnil = false;
	b->tsorted = false;
	b->trevsorted = false;
	b->tnosorted = 0;
	b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
}

static void
BATmaxminpos_bte(BAT *o, bte m)
{
	BUN minpos = BUN_NONE, maxpos = BUN_NONE, p, q;
	bte minval = m<0?GDK_bte_min:0; /* Later once nils use a bitmask we can include -128 in the range */
	bte maxval = m<0?GDK_bte_max:m;

	assert(o->ttype == TYPE_bte);
	o->tnil = m<0?true:false;
	o->tnonil = m<=0?false:true;
	bte *op = (bte*)Tloc(o, 0);
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
}

static void
BATmaxminpos_sht(BAT *o, sht m)
{
	BUN minpos = BUN_NONE, maxpos = BUN_NONE, p, q;
	sht minval = m<0?GDK_sht_min:0; /* Later once nils use a bitmask we can include -32768 in the range */
	sht maxval = m<0?GDK_sht_max:m;

	assert(o->ttype == TYPE_sht);
	o->tnil = m<0?true:false;
	o->tnonil = m<=0?false:true;
	sht *op = (sht*)Tloc(o, 0);
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
}

static str
DICTcompress_intern(BAT **O, BAT **U, BAT *b, bool ordered, bool persists, bool smallest_type)
{
	/* for now use all rows */
	BAT *u = BATunique(b, NULL), *uu = NULL;
	if (!u)
		throw(SQL, "dict.compress", GDK_EXCEPTION);
	assert(u->tkey);

	BUN cnt = BATcount(u);
	/* create hash on u */
	int tt = (cnt<256)?TYPE_bte:TYPE_sht;
	if (!smallest_type) {
		BUN cnt = BATcount(b);
		tt = (cnt<256)?TYPE_bte:TYPE_sht;
	}
	if (cnt >= 64*1024) {
		bat_destroy(u);
		throw(SQL, "dict.compress", SQLSTATE(3F000) "dict compress: too many values");
	}
	BAT *uv = BATproject(u, b); /* get values */
	bat_destroy(u);
	if (!uv)
		throw(SQL, "dict.compress", GDK_EXCEPTION);
	uv->tkey = true;

	if (ordered) {
		if (BATsort(&uu, NULL, NULL, uv, NULL, NULL, false, false, false) != GDK_SUCCEED) {
			bat_destroy(uv);
			throw(SQL, "dict.compress", GDK_EXCEPTION);
		}
		bat_destroy(uv);
		uv = uu;
	}
	u = uv;
	if (persists) {
		uu = COLcopy(uv, uv->ttype, true, PERSISTENT);
		bat_destroy(uv);
		if (!uu)
			throw(SQL, "dict.compress", GDK_EXCEPTION);
		assert(uu->tkey);
		u = uu;
	}

	BAT *o = COLnew(b->hseqbase, tt, BATcount(b), persists?PERSISTENT:TRANSIENT);
	if (!o || BAThash(u) != GDK_SUCCEED) {
		bat_destroy(o);
		bat_destroy(u);
		throw(SQL, "dict.compress", GDK_EXCEPTION);
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
		o->tsorted = (u->tsorted && bi.sorted);
		o->trevsorted = false;
		o->tnil = bi.nil;
		o->tnonil = bi.nonil;
		o->tkey = bi.key;

		BATmaxminpos_bte(o, (bte) (BATcount(u)-1));
	} else {
		sht *op = (sht*)Tloc(o, 0);
		BATloop(b, p, q) {
			BUN up = 0;
			HASHloop(ui, ui.b->thash, up, BUNtail(bi, p)) {
				op[p] = (sht)up;
			}
		}
		BATsetcount(o, BATcount(b));
		o->tsorted = (u->tsorted && bi.sorted);
		o->trevsorted = false;
		o->tnil = bi.nil;
		o->tnonil = bi.nonil;
		o->tkey = bi.key;

		BATmaxminpos_sht(o, (sht) (BATcount(u)-1));
	}
	bat_iterator_end(&bi);
	*O = o;
	*U = u;
	return MAL_SUCCEED;
}

str
DICTcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	/* (o,v) = dict.compress(c) */
	bat *RO = getArgReference_bat(stk, pci, 0);
	bat *RV = getArgReference_bat(stk, pci, 1);
	bat C = *getArgReference_bat(stk, pci, 2);

	BAT *c = BATdescriptor(C), *O, *V;

	if (!c)
		throw(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	str msg = DICTcompress_intern(&O, &V, c, false, false, false /*output type matches input*/);
	bat_destroy(c);
	if (msg == MAL_SUCCEED) {
		*RO = O->batCacheid;
		BBPkeepref(O);
		*RV = V->batCacheid;
		BBPkeepref(V);
	}
	return msg;
}

str
DICTcompress_col(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
	if (strNil(sname))
		throw(SQL, "dict.compress", SQLSTATE(42000) "Schema name cannot be NULL");
	if (strNil(tname))
		throw(SQL, "dict.compress", SQLSTATE(42000) "Table name cannot be NULL");
	if (strNil(cname))
		throw(SQL, "dict.compress", SQLSTATE(42000) "Column name cannot be NULL");
	if ((msg = getBackendContext(cntxt, &be)) != MAL_SUCCEED)
		return msg;
	tr = be->mvc->session->tr;

	sql_schema *s = find_sql_schema(tr, sname);
	if (!s)
		throw(SQL, "dict.compress", SQLSTATE(3F000) "schema '%s' unknown", sname);
	sql_table *t = find_sql_table(tr, s, tname);
	if (!t)
		throw(SQL, "dict.compress", SQLSTATE(3F000) "table '%s.%s' unknown", sname, tname);
	if (!isTable(t))
		throw(SQL, "dict.compress", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (isTempTable(t))
		throw(SQL, "dict.compress", SQLSTATE(42000) "columns from temporary tables cannot be compressed");
	if (t->system)
		throw(SQL, "dict.compress", SQLSTATE(42000) "columns from system tables cannot be compressed");
	sql_column *c = find_sql_column(t, cname);
	if (!c)
		throw(SQL, "dict.compress", SQLSTATE(3F000) "column '%s.%s.%s' unknown", sname, tname, cname);
	if (c->storage_type)
		throw(SQL, "dict.compress", SQLSTATE(3F000) "column '%s.%s.%s' already compressed", sname, tname, cname);

	sqlstore *store = tr->store;
	BAT *b = store->storage_api.bind_col(tr, c, RDONLY), *o, *u;
	if( b == NULL)
		throw(SQL,"dict.compress", SQLSTATE(HY005) "Cannot access column descriptor");

	msg = DICTcompress_intern(&o, &u, b, ordered, true, true);
	bat_destroy(b);
	if (msg == MAL_SUCCEED) {
		switch (sql_trans_alter_storage(tr, c, "DICT")) {
			case -1:
				msg = createException(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			case -2:
			case -3:
				msg = createException(SQL, "dict.compress", SQLSTATE(42000) "transaction conflict detected");
				break;
			default:
				break;
		}
		if (msg == MAL_SUCCEED && !(c = get_newcolumn(tr, c)))
			msg = createException(SQL, "dict.compress", SQLSTATE(HY013) "alter_storage failed");
		if (msg == MAL_SUCCEED) {
			switch (store->storage_api.col_compress(tr, c, ST_DICT, o, u)) {
				case -1:
					msg = createException(SQL, "dict.compress", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					break;
				case -2:
				case -3:
					msg = createException(SQL, "dict.compress", SQLSTATE(42000) "transaction conflict detected");
					break;
				default:
					break;
			}
		}
		bat_destroy(u);
		bat_destroy(o);
	}
	return msg;
}

#define decompress_loop(TPE) \
	do { \
		TPE *up = Tloc(u, 0); \
		TPE *restrict bp = Tloc(b, 0); \
		BATloop(o, p, q) { \
			TPE v = up[op[p]]; \
			nils |= is_##TPE##_nil(v); \
			bp[p] = v; \
		} \
		BATsetcount(b, BATcount(o)); \
		BATnegateprops(b); \
		b->tnil = nils; \
		b->tnonil = !nils; \
	} while (0)

BAT *
DICTdecompress_(BAT *o, BAT *u, role_t role)
{
	bool nils = false;
	BAT *b = COLnew(o->hseqbase, u->ttype, BATcount(o), role);

	if (!b)
		return NULL;
	BUN p, q;
	BATiter oi = bat_iterator(o);
	BATiter ui = bat_iterator_nolock(u);
	if (o->ttype == TYPE_bte) {
		unsigned char *op = Tloc(o, 0);

		switch (ATOMbasetype(u->ttype)) {
		case TYPE_int:
			decompress_loop(int);
			break;
		case TYPE_lng:
			decompress_loop(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			decompress_loop(hge);
			break;
#endif
		default:
			BATloop(o, p, q) {
				BUN up = op[p];
				if (BUNappend(b, BUNtail(ui, up), false) != GDK_SUCCEED) {
					bat_iterator_end(&oi);
					bat_destroy(b);
					return NULL;
				}
			}
		}
	} else {
		assert(o->ttype == TYPE_sht);
		unsigned short *op = Tloc(o, 0);

		switch (ATOMbasetype(u->ttype)) {
		case TYPE_int:
			decompress_loop(int);
			break;
		case TYPE_lng:
			decompress_loop(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			decompress_loop(hge);
			break;
#endif
		default:
			BATloop(o, p, q) {
				BUN up = op[p];
				if (BUNappend(b, BUNtail(ui, up), false) != GDK_SUCCEED) {
					bat_iterator_end(&oi);
					bat_destroy(b);
					return NULL;
				}
			}
		}
	}
	bat_iterator_end(&oi);
	return b;
}

str
DICTdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
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
	BAT *b = DICTdecompress_(o, u, TRANSIENT);
	bat_destroy(o);
	bat_destroy(u);
	if (!b)
		throw(SQL, "dict.decompress", GDK_EXCEPTION);
	*r = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

static BAT *
convert_oid( BAT *o, int rt)
{
	BUN p, q;
	BATiter oi = bat_iterator(o);
	BAT *b = COLnew(o->hseqbase, rt, oi.count, TRANSIENT);
	int brokenrange = 0, nil = 0;

	if (!b) {
		bat_iterator_end(&oi);
		return NULL;
	}
	if (rt == TYPE_bte) {
		unsigned char *rp = Tloc(b, 0);
		if (oi.type == TYPE_void) {
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
		if (oi.type == TYPE_void) {
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
	BATsetcount(b, oi.count);
	BATnegateprops(b);
	if (!brokenrange)
		b->tsorted = oi.sorted;
	b->tkey = oi.key;
	if (nil) {
		b->tnil = true;
		b->tnonil = false;
	} else {
		b->tnil = false;
		b->tnonil = true;
	}
	bat_iterator_end(&oi);
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

	*r = b->batCacheid;
	BBPkeepref(b);
	bat_destroy(o);
	return MAL_SUCCEED;
}

/* renumber lo iff rv0 is sorted and dense directly lookup in rv1
 *					if not dense (ie missing matches on the right side), first check (ie output
 *					too large values for a match ie BATcount(rv1))
 * else sort rv0 -> reorder (project) rv1, then lookup etc in rv1
* */
static BAT *
DICTrenumber_intern( BAT *o, BAT *lc, BAT *rc, BUN offcnt)
{
	BAT *olc = lc, *orc = rc, *no = NULL;
	BATiter oi = bat_iterator(o);
	BUN cnt = oi.count;

	if (!lc->tsorted) {
		BAT *nlc = NULL, *nrc = NULL;
		int ret = BATsort(&nlc, &nrc, NULL, lc, NULL, NULL, false, false, false);

		if (ret != GDK_SUCCEED || !nlc || !nrc) {
			bat_iterator_end(&oi);
			bat_destroy(nlc);
			bat_destroy(nrc);
			return no;
		}
		lc = nlc;
		rc = nrc;
	}
	/* dense or cheap dense check */
	if (!BATtdense(lc) && !(lc->tsorted && lc->tkey && BATcount(lc) == offcnt && *(oid*)Tloc(lc, offcnt-1) == offcnt-1)) {
		BAT *nrc = COLnew(0, ATOMtype(rc->ttype), offcnt, TRANSIENT);
		if (!nrc) {
			bat_iterator_end(&oi);
			if (lc != olc)
				bat_destroy(lc);
			if (rc != orc)
				bat_destroy(rc);
			return no;
		}

		/* create map with holes filled in */
		oid *restrict op = Tloc(nrc, 0);
		unsigned char *lp = Tloc(lc, 0);
		if (BATtvoid(rc)) {
			oid seq = rc->tseqbase, j = 0;
			for(BUN i = 0; i<offcnt; i++) {
				if (lp[j] > i) {
					op[i] = offcnt;
				} else {
					op[i] = seq + j;
					j++;
				}
			}
		} else {
			oid *ip = Tloc(rc, 0);
			for(BUN i = 0, j = 0; i<offcnt; i++) {
				if (lp[j] > i) {
					op[i] = offcnt;
				} else {
					op[i] = ip[j++];
				}
			}
		}
		BATsetcount(nrc, offcnt);
		BATnegateprops(nrc);
		nrc->tkey = rc->tkey;
		if (orc != rc)
			bat_destroy(rc);
		rc = nrc;
	}

	no = COLnew(o->hseqbase, oi.type, cnt, TRANSIENT);
	if (!no) {
		bat_iterator_end(&oi);
		if (lc != olc)
			bat_destroy(lc);
		if (rc != orc)
			bat_destroy(rc);
		return no;
	}
	if (oi.type == TYPE_bte) {
		bte *op = Tloc(no, 0);
		oid *c = Tloc(rc, 0);
		unsigned char *ip = (unsigned char *) oi.base;

		for(BUN i = 0; i<cnt; i++) {
			op[i] = (bte) ((BUN)ip[i]==offcnt?offcnt:c[ip[i]]);
		}
		BATsetcount(no, cnt);
		BATnegateprops(no);
		no->tkey = oi.key;
	} else if (oi.type == TYPE_sht) {
		sht *op = Tloc(no, 0);
		oid *c = Tloc(rc, 0);
		unsigned short *ip = (unsigned short *) oi.base;

		for(BUN i = 0; i<cnt; i++) {
			op[i] = (sht) ((BUN)ip[i]==offcnt?offcnt:c[ip[i]]);
		}
		BATsetcount(no, cnt);
		BATnegateprops(no);
		no->tkey = oi.key;
	} else {
		assert(0);
	}
	bat_iterator_end(&oi);
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

	if (!is_bat_nil(LC))
		lc = BATdescriptor(LC);
	if (!is_bat_nil(RC))
		rc = BATdescriptor(RC);
	if (!lo || !lv || !ro || !rv || (!is_bat_nil(LC) && !lc) || (!is_bat_nil(RC) && !rc)) {
		bat_destroy(lo);
		bat_destroy(lv);
		bat_destroy(ro);
		bat_destroy(rv);
		bat_destroy(lc);
		bat_destroy(rc);
		throw(SQL, "dict.join", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

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
					BAT *nlo = DICTrenumber_intern(lo, rv0, rv1, BATcount(lv));
					bat_destroy(lo);
					lo = nlo;
				} else {
					BAT *nro = DICTrenumber_intern(ro, rv1, rv0, BATcount(rv));
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
	if (r0) {
		*R0 = r0->batCacheid;
		BBPkeepref(r0);
	}
	if (r1) {
		*R1 = r1->batCacheid;
		BBPkeepref(r1);
	}
	if (err)
		throw(MAL, "BATjoin", GDK_EXCEPTION);
	return res;
}

str
DICTthetaselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
	BATiter loi = bat_iterator(lo);
	BATiter lvi = bat_iterator(lv);

	if (!is_bat_nil(LC))
		lc = BATdescriptor(LC);
	if (!lo || !lv || (!is_bat_nil(LC) && !lc)) {
		bat_iterator_end(&loi);
		bat_iterator_end(&lvi);
		bat_destroy(lo);
		bat_destroy(lv);
		bat_destroy(lc);
		throw(SQL, "dict.thetaselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BUN max_cnt = lvi.type == TYPE_bte?256:(64*1024);
	if ((lvi.key && (op[0] == '=' || op[0] == '!')) || ((op[0] == '<' || op[0] == '>') && lvi.sorted && BATcount(lv) < (max_cnt/2))) {
		BUN p = BUN_NONE;
		if (ATOMextern(lvi.type))
			v = *(ptr*)v;
		if (ATOMcmp(lvi.type, v,  ATOMnilptr(lvi.type)) == 0) {
			/* corner case, if v is NULL skip any calculations */
			bn = BATdense(0, 0, 0);
		} else {
			if (op[0] == '=' || op[0] == '!') {
				p =  BUNfnd(lv, v);
			} else if (op[0] == '<' || op[0] == '>') {
				p = SORTfndfirst(lv, v);
				if (p != BUN_NONE && op[0] == '<' && op[1] == '=') {
					if (ATOMcmp(lvi.type, v, BUNtail(lvi, p)) != 0)
						p--;
				} else if (p != BUN_NONE && op[0] == '>' && !op[1]) {
                    if (ATOMcmp(lvi.type, v, BUNtail(lvi, p)) != 0)
                        op = ">=";
				}
			}
			if (p != BUN_NONE) {
				if (loi.type == TYPE_bte) {
					bte val = (bte)p;
					bn =  BATthetaselect(lo, lc, &val, op);
				} else if (loi.type == TYPE_sht) {
					sht val = (sht)p;
					bn =  BATthetaselect(lo, lc, &val, op);
				} else
					assert(0);
				if (bn && (op[0] == '<' || op[0] == '>' || op[0] == '!') && (!lvi.nonil || lvi.nil)) { /* filter the NULL value out */
					p = BUNfnd(lv, ATOMnilptr(lvi.type));
					if (p != BUN_NONE) {
						BAT *nbn = NULL;
						if (loi.type == TYPE_bte) {
							bte val = (bte)p;
							nbn =  BATthetaselect(lo, bn, &val, "<>");
						} else if (loi.type == TYPE_sht) {
							sht val = (sht)p;
							nbn =  BATthetaselect(lo, bn, &val, "<>");
						} else
							assert(0);
						BBPreclaim(bn);
						bn = nbn;
					}
				}
			} else if (op[0] == '!') {
				if (!lvi.nonil || lvi.nil) { /* find a possible NULL value */
					p = BUNfnd(lv, ATOMnilptr(lvi.type));
				} else {
					p = BUN_NONE;
				}

				if (p != BUN_NONE) { /* filter the NULL value out */
					if (loi.type == TYPE_bte) {
						bte val = (bte)p;
						bn =  BATthetaselect(lo, lc, &val, op);
					} else if (loi.type == TYPE_sht) {
						sht val = (sht)p;
						bn =  BATthetaselect(lo, lc, &val, op);
					} else
						assert(0);
				} else if (lc) { /* all rows pass, use input candidate list */
					bn = lc;
					BBPfix(lc->batCacheid); /* give one extra physical reference to keep the count in the end */
				} else { /* otherwise return all rows */
					bn = BATdense(0, 0, BATcount(lo));
				}
			} else {
				bn = BATdense(0, 0, 0);
			}
		}
	} else { /* select + intersect */
		if (ATOMextern(lvi.type))
			v = *(ptr*)v;
		bn = BATthetaselect(lv, NULL, v, op);
		/* call dict convert */
		if (bn) {
			BAT *c = convert_oid(bn, loi.type);
			bat_destroy(bn);
			bn = c;
		}
		if (bn) {
			BAT *n = BATintersect(lo, bn, lc, NULL, true, true, BATcount(lo));
			bat_destroy(bn);
			bn = n;
		}
	}
	bat_iterator_end(&loi);
	bat_iterator_end(&lvi);
	bat_destroy(lo);
	bat_destroy(lv);
	bat_destroy(lc);
	if (!bn)
		throw(SQL, "dict.thetaselect", GDK_EXCEPTION);
	*R0 = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
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
	ptr l = getArgReference(stk, pci, 4);
	ptr h = getArgReference(stk, pci, 5);
	bit li = *getArgReference_bit(stk, pci, 6);
	bit hi = *getArgReference_bit(stk, pci, 7);
	bit anti = *getArgReference_bit(stk, pci, 8);
	bit unknown = *getArgReference_bit(stk, pci, 9);

	if (!unknown ||
		(li != 0 && li != 1) ||
		(hi != 0 && hi != 1) ||
		(anti != 0 && anti != 1)) {
		throw(MAL, "algebra.select", ILLEGAL_ARGUMENT);
	}

	BAT *lc = NULL, *bn = NULL;
	BAT *lo = BATdescriptor(LO);
	BAT *lv = BATdescriptor(LV);
	BATiter loi = bat_iterator(lo);
	BATiter lvi = bat_iterator(lv);

	if (!is_bat_nil(LC))
		lc = BATdescriptor(LC);
	if (!lo || !lv || (!is_bat_nil(LC) && !lc)) {
		bat_iterator_end(&loi);
		bat_iterator_end(&lvi);
		bat_destroy(lo);
		bat_destroy(lv);
		bat_destroy(lc);
		throw(SQL, "dict.select", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if (ATOMextern(lvi.type)) {
		l = *(ptr*)l;
		h = *(ptr*)h;
	}

	/* here we don't need open ended parts with nil */
	if (!anti) {
		const void *nilptr = ATOMnilptr(lvi.type);
		if (li == 1 && ATOMcmp(lvi.type, l, nilptr) == 0) {
			l = h;
			li = 0;
		}
		if (hi == 1 && ATOMcmp(lvi.type, h, nilptr) == 0) {
			h = l;
			hi = 0;
		}
		if (ATOMcmp(lvi.type, l, h) == 0 && ATOMcmp(lvi.type, h, nilptr) == 0) /* ugh sql nil != nil */
			anti = 1;
	}
	BUN max_cnt = lvi.type == TYPE_bte?256:(64*1024);
	if (!anti && lvi.key && lvi.sorted && BATcount(lv) < (max_cnt/2)) { /* ie select(lo, lc, find(lv, l), find(lv, h), ...) */
		BUN p = li?SORTfndfirst(lv, l):SORTfndlast(lv, l);
		BUN q = SORTfnd(lv, h);

		if (q == BUN_NONE) {
			q = SORTfndfirst(lv, h);
			q--;
		}

		if (p != BUN_NONE) {
			if (loi.type == TYPE_bte) {
				bte lpos = (bte)p;
				bte hpos = (bte)q;
				bn =  BATselect(lo, lc, &lpos, &hpos, 1, hi, anti);
			} else if (loi.type == TYPE_sht) {
				sht lpos = (sht)p;
				sht hpos = (sht)q;
				bn =  BATselect(lo, lc, &lpos, &hpos, 1, hi, anti);
			} else
				assert(0);
		} else {
			bn = BATdense(0, 0, 0);
		}
	} else {
		bn = BATselect(lv, NULL, l, h, li, hi, anti);

		/* call dict convert */
		if (bn) {
			BAT *c = convert_oid(bn, loi.type);
			bat_destroy(bn);
			bn = c;
		}
		if (bn) {
			BAT *n = BATintersect(lo, bn, lc, NULL, true, true, BATcount(lo));
			bat_destroy(bn);
			bn = n;
		}
	}
	bat_iterator_end(&loi);
	bat_iterator_end(&lvi);
	bat_destroy(lo);
	bat_destroy(lv);
	bat_destroy(lc);
	if (!bn)
		throw(SQL, "dict.select", GDK_EXCEPTION);
	*R0 = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}


BAT *
DICTenlarge(BAT *offsets, BUN cnt, BUN sz, role_t role)
{
	BAT *n = COLnew(offsets->hseqbase, TYPE_sht, sz, role);

	if (!n)
		return NULL;
	unsigned char *o = Tloc(offsets, 0);
	unsigned short *no = Tloc(n, 0);
	for(BUN i = 0; i<cnt; i++) {
		no[i] = o[i];
	}
	BATnegateprops(n);
	n->tnil = offsets->tnil;
	n->tnonil = offsets->tnonil;
	n->tkey = offsets->tkey;
	n->tsorted = offsets->tsorted;
	n->trevsorted = offsets->trevsorted;
	return n;
}

str
DICTrenumber(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* r[p] = n[o[p]] */
	(void)cntxt;
	(void)mb;
	bat *R = getArgReference_bat(stk, pci, 0);
	bat O = *getArgReference_bat(stk, pci, 1);
	bat M = *getArgReference_bat(stk, pci, 2);

	BAT *o = BATdescriptor(O);
	BAT *m = BATdescriptor(M);

	if (!o || !m) {
		bat_destroy(o);
		bat_destroy(m);
		throw(SQL, "dict.renumber", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BUN cnt = BATcount(o);
	BAT *n = COLnew(o->hseqbase, o->ttype, cnt, TRANSIENT);
	if (!n) {
		bat_destroy(o);
		bat_destroy(m);
		throw(SQL, "dict.renumber", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	assert(o->ttype == TYPE_bte || o->ttype == TYPE_sht);
	if (o->ttype == TYPE_bte) {
		unsigned char *np = Tloc(n, 0);
		unsigned char *op = Tloc(o, 0);
		unsigned char *mp = Tloc(m, 0);
		for(BUN i = 0; i<cnt; i++) {
			np[i] = mp[op[i]];
		}
	} else {
		unsigned short *np = Tloc(n, 0);
		unsigned short *op = Tloc(o, 0);
		unsigned short *mp = Tloc(m, 0);
		for(BUN i = 0; i<cnt; i++) {
			np[i] = mp[op[i]];
		}
	}
	BATsetcount(n, cnt);
	BATnegateprops(n);
	n->tnil = false;
	n->tnonil = true;
	if (o->ttype == TYPE_bte) {
		unsigned char *mp = Tloc(m, 0);
		unsigned char mm = 0;
		for(BUN i = 0; i<BATcount(m); i++)
			if (mp[i] > mm)
				mm = mp[i];
		BATmaxminpos_bte(n, mm);
	} else {
		unsigned short *mp = Tloc(m, 0);
		unsigned short mm = 0;
		for(BUN i = 0; i<BATcount(m); i++)
			if (mp[i] > mm)
				mm = mp[i];
		BATmaxminpos_sht(n, mm);
	}
	bat_destroy(o);
	bat_destroy(m);
	*R = n->batCacheid;
	BBPkeepref(n);
	return MAL_SUCCEED;
}

/* for each val in vals compute its offset in dict (return via noffsets),
 * any missing value in dict will be added to the dict.
 * Possible side-effects:
 *	dict is nolonger sorted
 *	increase of the dict could mean the offset type overflows, then the output is
 *	an offset bat with a larger type, unless the larger type is int then abort.
 *
 *	Returns < 0 on error.
 */
int
DICTprepare4append(BAT **noffsets, BAT *vals, BAT *dict)
{
	int tt = BATcount(dict)>=256?TYPE_sht:TYPE_bte;
	BUN sz = BATcount(vals), nf = 0;
	BAT *n = COLnew(0, tt, sz, TRANSIENT);

	if (!n || BAThash(dict) != GDK_SUCCEED) {
		bat_destroy(n);
		return -1;
	}

	BATiter bi = bat_iterator(vals);
	BATiter ui = bat_iterator_nolock(dict);

	if (tt == TYPE_bte) {
		bte *op = (bte*)Tloc(n, 0);
		for(BUN i = 0; i<sz; i++) {
			BUN up = 0;
			int f = 0;
			HASHloop(ui, ui.b->thash, up, BUNtail(bi, i)) {
				op[i] = (bte)up;
				f = 1;
			}
			if (!f) {
				if (BATcount(dict) >= 255) {
					BAT *nn = DICTenlarge(n, i, sz, TRANSIENT);
					bat_destroy(n);
					if (!nn) {
						bat_iterator_end(&bi);
						return -1;
					}
					n = nn;
					nf = i;
					tt = TYPE_sht;
					break;
				} else {
					if (BUNappend(dict, BUNtail(bi, i), true) != GDK_SUCCEED ||
					   (!dict->thash && BAThash(dict) != GDK_SUCCEED)) {
						assert(0);
						bat_destroy(n);
						bat_iterator_end(&bi);
						return -1;
					}
					/* reinitialize */
					ui = bat_iterator_nolock(dict);
					op[i] = (bte) (BATcount(dict)-1);
				}
			}
		}
	}
	if (tt == TYPE_sht) {
		sht *op = (sht*)Tloc(n, 0);
		for(BUN i = nf; i<sz; i++) {
			BUN up = 0;
			int f = 0;
			HASHloop(ui, ui.b->thash, up, BUNtail(bi, i)) {
				op[i] = (sht)up;
				f = 1;
			}
			if (!f) {
				if (BATcount(dict) >= (64*1024)-1) {
					assert(0);
					bat_destroy(n);
					bat_iterator_end(&bi);
					return -2;
				} else {
					if (BUNappend(dict, BUNtail(bi, i), true) != GDK_SUCCEED ||
					   (!dict->thash && BAThash(dict) != GDK_SUCCEED)) {
						assert(0);
						bat_destroy(n);
						bat_iterator_end(&bi);
						return -1;
					}
					/* reinitialize */
					ui = bat_iterator_nolock(dict);
					op[i] = (sht) (BATcount(dict)-1);
				}
			}
		}
	}
	bat_iterator_end(&bi);
	BATsetcount(n, sz);
	BATnegateprops(n);
	n->tnil = false;
	n->tnonil = true;
	*noffsets = n;
	return 0;
}

static sht *
DICTenlarge_vals(bte *offsets, BUN cnt, BUN sz)
{
	sht *n = GDKmalloc(sizeof(sht) * sz);

	if (!n)
		return NULL;
	unsigned char *o = (unsigned char*)offsets;
	unsigned short *no = (unsigned short*)n;
	for(BUN i = 0; i<cnt; i++) {
		no[i] = o[i];
	}
	return n;
}

int
DICTprepare4append_vals(void **noffsets, void *vals, BUN cnt, BAT *dict)
{
	int tt = BATcount(dict)>=256?TYPE_sht:TYPE_bte;
	BUN sz = cnt, nf = 0;
	void *n = GDKmalloc((tt==TYPE_bte?sizeof(bte):sizeof(sht)) * sz);

	if (!n || BAThash(dict) != GDK_SUCCEED) {
		GDKfree(n);
		return -1;
	}

	int varsized = ATOMvarsized(dict->ttype);
	int wd = (varsized?sizeof(char*):dict->twidth);
	char *vp = vals;
	BATiter ui = bat_iterator_nolock(dict);
	if (tt == TYPE_bte) {
		bte *op = (bte*)n;
		for(BUN i = 0; i<sz; i++, vp += wd) {
			BUN up = 0;
			int f = 0;
			void *val = (void*)vp;
			if (varsized)
				val = *(void**)vp;
			HASHloop(ui, ui.b->thash, up, val) {
				op[i] = (bte)up;
				f = 1;
			}
			if (!f) {
				if (BATcount(dict) >= 255) {
					sht *nn = DICTenlarge_vals(n, i, sz);
					GDKfree(n);
					if (!nn)
						return -1;
					n = nn;
					nf = i;
					tt = TYPE_sht;
					break;
				} else {
					if (BUNappend(dict, val, true) != GDK_SUCCEED ||
					   (!dict->thash && BAThash(dict) != GDK_SUCCEED)) {
						assert(0);
						GDKfree(n);
						return -1;
					}
					/* reinitialize */
					ui = bat_iterator_nolock(dict);
					op[i] = (bte) (BATcount(dict)-1);
				}
			}
		}
	}
	if (tt == TYPE_sht) {
		sht *op = (sht*)n;
		for(BUN i = nf; i<sz; i++) {
			BUN up = 0;
			int f = 0;
			void *val = (void*)vp;
			if (varsized)
				val = *(void**)vp;
			HASHloop(ui, ui.b->thash, up, val) {
				op[i] = (sht)up;
				f = 1;
			}
			if (!f) {
				if (BATcount(dict) >= (64*1024)-1) {
					assert(0);
					GDKfree(n);
					return -2;
				} else {
					if (BUNappend(dict, val, true) != GDK_SUCCEED ||
					   (!dict->thash && BAThash(dict) != GDK_SUCCEED)) {
						assert(0);
						GDKfree(n);
						return -1;
					}
					/* reinitialize */
					ui = bat_iterator_nolock(dict);
					op[i] = (sht) (BATcount(dict)-1);
				}
			}
		}
	}
	*noffsets = n;
	return 0;
}
