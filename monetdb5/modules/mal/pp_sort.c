/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal_interpreter.h"
#include "mal_instruction.h"
#include "mal_exception.h"
#include "mal_pipelines.h"
#include "pipeline.h"

static int
var_atom_cmp(int type, const void *l, const void *r, bool nilsmallest)
{
	void *nil = (void*)ATOMnilptr(type);
	int (*cmp)(const void *v1,const void *v2) = ATOMcompare(type);

	bool lnil = cmp(l, nil) == 0;
	bool rnil = cmp(r, nil) == 0;

	if (lnil && rnil)
		return 0;
	if (lnil)
		return nilsmallest?-1:1;
	if (rnil)
		return nilsmallest?1:-1;
	return cmp(l, r);
}

static str
PPsubmerge_any( bat *Rzzl, bat *Rzzb, bat *Rzza, BAT *lcol, BAT *rcol, BAT *zzl, BAT *zzb, BAT *zza, bit desc, bit nlast)
{
	BAT *rzzl = NULL, *rzzb = NULL, *rzza = NULL;
	str err = MAL_SUCCEED;
	bool nilsmallest = desc == nlast; /* (desc && nlast) || (!desc && !nlast); */

	/* sofar the ANY type case, later add optimized versions */
	/* need new output bats */
	BUN rp = 0, lp = 0, o = 0, p = lp, cur = rp, sz = BATcount(zzl), e = 0;
	rzzl = COLnew(0, TYPE_int, 1024, TRANSIENT);
	rzzb = COLnew(0, TYPE_int, 1024, TRANSIENT);
	rzza = COLnew(0, TYPE_int, 1024, TRANSIENT);

	if (!rzzl || !rzzb || !rzza) {
		err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}
	if (BATcount(lcol) && BATcount(rcol) == 0) {
		int v1 = (int)BATcount(lcol), v2 = (int)0;
		if (BUNappend(rzzl, &v1, TRUE) != GDK_SUCCEED ||
			BUNappend(rzzb, &v2, TRUE) != GDK_SUCCEED ||
			BUNappend(rzza, &v2, TRUE) != GDK_SUCCEED) {
				err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
		}
		goto done;
	}
	BATiter ri = bat_iterator(rcol);
	BATiter li = bat_iterator(lcol);
	int (*cmp)(const void *v1,const void *v2) = ATOMcompare(rcol->ttype);

	if (nlast && ri.nonil && li.nonil)
		nilsmallest = true;

	int side = 0;
	int *gp = Tloc(zzl, 0);
	int *bp = Tloc(zzb, 0);
	int *ap = Tloc(zza, 0);
	int pa = 0;
	lng ilen = 0, rlen = 0, tsz = BATcount(lcol) + BATcount(rcol);
	(void)tsz;
	for(BUN i = 0; i<sz; i++) {
		int len = gp[i], ob = bp[i], oa = ap[i];
		bool oside = side;

		ilen += len;
		o = p;
		len -= pa;
		p += len;
		pa = 0;
		if (ob || oa) { /* split ? */
			BUN re = 0, le = 0;
			if (side) {
				re = p;
				le = lp+oa;
				cur = lp;
			} else {
				le = p;
				re = rp+oa;
				cur = rp;
			}
			e = p;
			p -= ob;
			lng b = 0, a = 0;
			for(;p < e; ) {
				const void *vc = !side?BUNtail(&ri, cur):BUNtail(&li, cur);
				const void *vp =  side?BUNtail(&ri, p):BUNtail(&li, p);
				int c = nilsmallest?cmp(vp,vc):var_atom_cmp(rcol->ttype, vp, vc, nilsmallest);
				if (desc)
					c = -c;
				if (c <= 0) {
					if (c == 0)
						b--;
					p++;
				} else { /* c > 0 flip */
					BUN l = p - o;
					if (side)
						rp = p;
					else
						lp = p;
					side = !side;
					o = cur;
					cur = p;
					p = o;
					e = side?re:le;
					if (b) {
						a = -1;
						const void *vc = side?BUNtail(&ri, p):BUNtail(&li, p);
						for (p++; p<e; p++, a--) {
							const void *vp = side?BUNtail(&ri, p):BUNtail(&li, p);
							if (cmp(vp,vc) != 0)
								break;
						}
					}
					int v1 = (int)l, v2 = (int)-b, v3 = (int)-a;
					rlen += v1;
					//printf("v1 -3  %d\n", v1);
					if (BUNappend(rzzl, &v1, TRUE) != GDK_SUCCEED ||
							BUNappend(rzzb, &v2, TRUE) != GDK_SUCCEED ||
							BUNappend(rzza, &v3, TRUE) != GDK_SUCCEED) {
						err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto error_iter;
					}
					b = 0;
					a = 0;
				}
			}
			BUN l = p - o;
			if (side)
				rp = p;
			else
				lp = p;
			side = !side;
			o = cur;
			cur = p;
			p = o;
			e = side?re:le;
			if (b) {
				a = -1;
				const void *vc = side?BUNtail(&ri, p):BUNtail(&li, p);
				BUN np = p;
				for (np++; np<e; np++, a--) {
					const void *vp = side?BUNtail(&ri, np):BUNtail(&li, np);
					if (cmp(vp,vc) != 0)
						break;
				}
			}
			int v1 = (int)l, v2 = (int)-b, v3 = (int)-a;
			rlen += v1;
			//printf("v1 -2  %d\n", v1);
			if (BUNappend(rzzl, &v1, TRUE) != GDK_SUCCEED ||
					BUNappend(rzzb, &v2, TRUE) != GDK_SUCCEED ||
					BUNappend(rzza, &v3, TRUE) != GDK_SUCCEED) {
				err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error_iter;
			}
			if (oside == side && p < e) {
				BUN l = e - p;
				int v1 = (int)l, v2 = (int)0, v3 = (int)0;
				rlen += v1;
				//printf("v1 -1  %d\n", v1);
				if (BUNappend(rzzl, &v1, TRUE) != GDK_SUCCEED ||
					BUNappend(rzzb, &v2, TRUE) != GDK_SUCCEED ||
					BUNappend(rzza, &v3, TRUE) != GDK_SUCCEED) {
					err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto error_iter;
				}
				p = e;
				if (side) {
					rp = p;
				} else {
					lp = p;
				}
				side = !side;
				o = cur;
				cur = p;
				p = o;
				pa = oa;
			} else {
				pa = oa - (int)(side?(re-p):(le-p));
			}
		} else {
			rlen += len;
			//printf("len %d\n", len);
			if (BUNappend(rzzl, &len, TRUE) != GDK_SUCCEED ||
			    BUNappend(rzzb, &ob, TRUE) != GDK_SUCCEED ||
			    BUNappend(rzza, &oa, TRUE) != GDK_SUCCEED) {
				err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error_iter;
			}
			if (side) {
				rp = p;
			} else {
				lp = p;
			}
			side = !side;
			o = cur;
			cur = p;
			p = o;
		}
	}
	(void)ilen; (void)rlen;
	assert (ilen == rlen && ilen == tsz);
	/*
	printf("sub\n");
	BATprint(GDKstdout, zzl);
	BATprint(GDKstdout, zzb);
	BATprint(GDKstdout, zza);
	BATprint(GDKstdout, lcol);
	BATprint(GDKstdout, rcol);
	BATprint(GDKstdout, rzzl);
	printf("sub done\n");
	fflush(stdout);
	*/

	bat_iterator_end(&li);
	bat_iterator_end(&ri);
done:
	BBPreclaim(zzl);
	BBPreclaim(zzb);
	BBPreclaim(zza);
	BBPreclaim(lcol);
	BBPreclaim(rcol);
	*Rzzl = rzzl->batCacheid;
	BBPkeepref(rzzl);
	*Rzzb = rzzb->batCacheid;
	BBPkeepref(rzzb);
	*Rzza = rzza->batCacheid;
	BBPkeepref(rzza);
	return MAL_SUCCEED;
error_iter:
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
error:
	BBPreclaim(zzl);
	BBPreclaim(zzb);
	BBPreclaim(zza);
	BBPreclaim(lcol);
	BBPreclaim(rcol);
	BBPreclaim(rzzl);
	BBPreclaim(rzzb);
	BBPreclaim(rzza);
	return err;
}

static str
PPmerge_any( bat *Rzzl, bat *Rzzb, bat *Rzza, BAT *lcol, BAT *rcol, bit desc, bit nlast)
{
	BAT *rzzl = NULL, *rzzb = NULL, *rzza = NULL;
	str err = MAL_SUCCEED;
	bool nilsmallest = desc == nlast; /* (desc && nlast) || (!desc && !nlast); */

	/* sofar the ANY type case, later add optimized versions */
	/* need new output bats */
	BUN rp = 0, lp = 0, re = BATcount(rcol), le = BATcount(lcol), o = 0, p = lp, e = le, cur = rp;
	lng b = 0, a = 0;
	rzzl = COLnew(0, TYPE_int, 1024, TRANSIENT);
	rzzb = COLnew(0, TYPE_int, 1024, TRANSIENT);
	rzza = COLnew(0, TYPE_int, 1024, TRANSIENT);

	if (!rzzl || !rzzb || !rzza) {
		err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}
	if (BATcount(lcol) == 0 && BATcount(rcol) == 0) {
		goto done;
	}
	if (BATcount(lcol) && BATcount(rcol) == 0) {
		int v1 = (int)BATcount(lcol), v2 = (int)0;
		if (BUNappend(rzzl, &v1, TRUE) != GDK_SUCCEED ||
			BUNappend(rzzb, &v2, TRUE) != GDK_SUCCEED ||
			BUNappend(rzza, &v2, TRUE) != GDK_SUCCEED) {
				err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
		}
		goto done;
	}
	BATiter ri = bat_iterator(rcol);
	BATiter li = bat_iterator(lcol);
	int (*cmp)(const void *v1,const void *v2) = ATOMcompare(rcol->ttype);

	if (nlast && ri.nonil && li.nonil)
		nilsmallest = false;

	int side = 0;
	for(; p<e; ) {
		const void *vc = !side?BUNtail(&ri, cur):BUNtail(&li, cur);
		const void *vp =  side?BUNtail(&ri, p):BUNtail(&li, p);
		int c = nilsmallest?cmp(vp,vc):var_atom_cmp(rcol->ttype, vp, vc, nilsmallest);
		if (desc)
			c = -c;
		if (c <= 0) {
			if (c == 0)
				b--;
			p++;
		} else { /* c > 0 flip */
			BUN l = p - o;
			if (side)
				rp = p;
			else
				lp = p;
			side = !side;
			o = cur;
			cur = p;
			p = o;
			e = side?re:le;
			if (b) {
				a = -1;
				const void *vc = side?BUNtail(&ri, p):BUNtail(&li, p);
				for (p++; p<e; p++, a--) {
					const void *vp = side?BUNtail(&ri, p):BUNtail(&li, p);
					if (cmp(vp,vc) != 0)
						break;
				}
			}
			int v1 = (int)l, v2 = (int)-b, v3 = (int)-a;
			if (BUNappend(rzzl, &v1, TRUE) != GDK_SUCCEED ||
				BUNappend(rzzb, &v2, TRUE) != GDK_SUCCEED ||
				BUNappend(rzza, &v3, TRUE) != GDK_SUCCEED) {
				err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error_iter;
			}
			b = 0;
			a = 0;
		}
	}
	BUN l = p - o;
	if (side)
		rp = p;
	else
		lp = p;
	side = !side;
	o = cur;
	cur = p;
	p = o;
	e = side?re:le;
	if (b) {
		a = -1;
		const void *vc = side?BUNtail(&ri, p):BUNtail(&li, p);
		BUN np = p;
		for (np++; np<e; np++, a--) {
			const void *vp = side?BUNtail(&ri, np):BUNtail(&li, np);
			if (cmp(vp,vc) != 0)
				break;
		}
	}
	int v1 = (int)l, v2 = (int)-b, v3 = (int)-a;
	assert( (!v2 && !v3) || (v2 && v3));
	if (BUNappend(rzzl, &v1, TRUE) != GDK_SUCCEED ||
		BUNappend(rzzb, &v2, TRUE) != GDK_SUCCEED ||
		BUNappend(rzza, &v3, TRUE) != GDK_SUCCEED) {
		err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error_iter;
	}
	b = 0;
	a = 0;
	if (p < e) {
		BUN l = e - p;
		int v1 = (int)l, v2 = (int)0, v3 = (int)0;
		if (BUNappend(rzzl, &v1, TRUE) != GDK_SUCCEED ||
			BUNappend(rzzb, &v2, TRUE) != GDK_SUCCEED ||
			BUNappend(rzza, &v3, TRUE) != GDK_SUCCEED) {
			err = createException(MAL, "sort.merge", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error_iter;
		}
	}
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
done:
	BBPreclaim(lcol);
	BBPreclaim(rcol);
	*Rzzl = rzzl->batCacheid;
	BBPkeepref(rzzl);
	*Rzzb = rzzb->batCacheid;
	BBPkeepref(rzzb);
	*Rzza = rzza->batCacheid;
	BBPkeepref(rzza);
	return MAL_SUCCEED;
error_iter:
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
error:
	BBPreclaim(lcol);
	BBPreclaim(rcol);
	BBPreclaim(rzzl);
	BBPreclaim(rzzb);
	BBPreclaim(rzza);
	return err;
}

/* later optimize last merge, to only return rzzl */
static str
PPmerge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	int extra = (pci->argc == 10)?3:0;
	bat *Rzzl = getArgReference_bat(stk, pci, 0);
	bat *Rzzb = getArgReference_bat(stk, pci, 1);
	bat *Rzza = getArgReference_bat(stk, pci, 2);
	bat *Lcol = getArgReference_bat(stk, pci, 3);
	bat *Rcol = getArgReference_bat(stk, pci, 4);
	bit desc = *(getArgReference_bit(stk, pci, 5+extra));
	bit nlast = *(getArgReference_bit(stk, pci, 6+extra));

	BAT *lcol = BATdescriptor(*Lcol);
	BAT *rcol = BATdescriptor(*Rcol);
	if (!lcol || !rcol) {
		BBPreclaim(lcol);
		BBPreclaim(rcol);
		throw(MAL, "sort.merge", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (pci->argc == 10) {
		bat *Zzl = getArgReference_bat(stk, pci, 5);
		bat *Zzb = getArgReference_bat(stk, pci, 6);
		bat *Zza = getArgReference_bat(stk, pci, 7);
		BAT *zzl = BATdescriptor(*Zzl);
		BAT *zzb = BATdescriptor(*Zzb);
		BAT *zza = BATdescriptor(*Zza);
		if (!zzl || !zzb || !zza) {
			BBPreclaim(lcol);
			BBPreclaim(rcol);
			throw(MAL, "sort.merge", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		return PPsubmerge_any(Rzzl, Rzzb, Rzza, lcol, rcol, zzl, zzb, zza, desc, nlast);
	} else {
		return PPmerge_any(Rzzl, Rzzb, Rzza, lcol, rcol, desc, nlast);
	}
}

#define zzl_vvproject(T, res, zzl, lcol, rcol) { \
	oid *o = (oid*)Tloc(res, 0); \
	oid l = lcol->tseqbase; \
	oid r = rcol->tseqbase; \
	oid v = side?r:l; \
	BUN p = side?rp:lp; \
	for (BUN k = 0; k<sz; k++) { \
		int nr = z[k]; \
		for (int j = 0; j<nr; j++, p++, op++) \
			o[op] = v+p; \
		if (side) { \
			rp = p; \
			p = lp; \
		} else { \
			lp = p; \
			p = rp; \
		} \
		side = !side; \
		v = side?r:l; \
	} \
}

#define zzl_vproject(T, res, zzl, lcol, rcol) { \
	oid *o = (oid*)Tloc(res, 0); \
	oid l = lcol->tseqbase; \
	oid *r = Tloc(rcol, 0); \
	BUN p = side?rp:lp; \
	for (BUN k = 0; k<sz; k++) { \
		int nr = z[k]; \
		for (int j = 0; j<nr; j++, p++, op++) \
			o[op] = side?r[p]:l+p; \
		if (side) { \
			rp = p; \
			p = lp; \
		} else { \
			lp = p; \
			p = rp; \
		} \
		side = !side; \
	} \
}

#define zzl_project(T, res, zzl, lcol, rcol) { \
	T *o = Tloc(res, 0); \
	T *l = Tloc(lcol, 0); \
	T *r = Tloc(rcol, 0); \
	T *v = side?r:l; \
	BUN p = side?rp:lp; \
	for (BUN k = 0; k<sz; k++) { \
		int nr = z[k]; \
		for (int j = 0; j<nr; j++, p++, op++) \
			o[op] = v[p]; \
		if (side) { \
			rp = p; \
			p = lp; \
		} else { \
			lp = p; \
			p = rp; \
		} \
		side = !side; \
		v = side?r:l; \
	} \
}


/*later optimize by starting at end, moving tuples */
static str
PPmproject_any( BAT *res, BAT *zzl, BAT *lcol, BAT *rcol)
{
	bool side = 0;
	int *z = (int*)Tloc(zzl, 0);
	BUN sz = BATcount(zzl), tsz = BATcount(lcol) + BATcount(rcol);
	BUN rp = 0, lp = 0, op = 0, cnt = tsz;
	int tt = lcol->ttype;

	if(!ATOMvarsized(tt)) {
		int width = lcol->twidth;
		if(width == 0) {
			if (lcol->ttype == TYPE_void && rcol->ttype == TYPE_void) {
				zzl_vvproject(bte, res, zzl, lcol, rcol);
			} else {
				zzl_vproject(bte, res, zzl, lcol, rcol);
			}
		} else if(width == sizeof(bte)) {
			zzl_project(bte, res, zzl, lcol, rcol);
		} else if(width == sizeof(sht)) {
			zzl_project(sht, res, zzl, lcol, rcol);
		} else if(width == sizeof(int)) {
			zzl_project(int, res, zzl, lcol, rcol);
		} else if(width == sizeof(lng)) {
			zzl_project(lng, res, zzl, lcol, rcol);
#ifdef HAVE_HGE
		} else if(width == sizeof(hge)) {
			zzl_project(hge, res, zzl, lcol, rcol);
#endif
		} else {
			printf("width %d\n", width);
			assert(0);
		}
		/* zap props */
		BATnegateprops(res);
		BATsetcount(res, cnt);
		return MAL_SUCCEED;
	} else if (tt == TYPE_str && res->tvheap->parentid == lcol->tvheap->parentid) {
		if(lcol->twidth == 1) {
			zzl_project(bte, res, zzl, lcol, rcol);
		} else if(lcol->twidth == 2) {
			zzl_project(sht, res, zzl, lcol, rcol);
		} else if(lcol->twidth == 4) {
			zzl_project(int, res, zzl, lcol, rcol);
		} else if(lcol->twidth == 8) {
			zzl_project(lng, res, zzl, lcol, rcol);
		}
		/* zap props */
		BATnegateprops(res);
		BATsetcount(res, cnt);
		return MAL_SUCCEED;
	}
	BATiter ri = bat_iterator(rcol);
	BATiter li = bat_iterator(lcol);

	// create result
	BUN p = lp;
	for(BUN i = 0; i < sz; i++) {
		BUN o = z[i];
		for (BUN c = 0; c < o; c++, p++) {
			const void *v = side ? BUNtail(&ri, p) : BUNtail(&li, p);
			if (BUNappend(res, v, TRUE) != GDK_SUCCEED) {
				bat_iterator_end(&li);
				bat_iterator_end(&ri);
				throw(MAL, "sort.mproject", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		/* switch */
		if (side) {
			rp = p;
			p = lp;
		} else {
			lp = p;
			p = rp;
		}
		side = !side;
	}
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	return MAL_SUCCEED;
}

static str
PPmproject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *R = getArgReference_bat(stk, pci, 0);
	bat *z = getArgReference_bat(stk, pci, 1);
	bat *l = getArgReference_bat(stk, pci, 2);
	bat *r = getArgReference_bat(stk, pci, 3);
	Pipeline *p = NULL;

	if (pci->argc == 5) /* optional argument for combine phase */
		p = (Pipeline*)*getArgReference_ptr(stk, pci, 4);
	(void)p;

	BAT *cl = BATdescriptor(*l), *cr = BATdescriptor(*r), *res = NULL;
	BAT *zzl = BATdescriptor(*z);
	if (!cl || !cr || !zzl) {
		BBPreclaim(cl);
		BBPreclaim(cr);
		BBPreclaim(zzl);
		throw(MAL, "sort.mproject", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	BUN sz = BATcount(cl) + BATcount(cr);
	if (cl->ttype == TYPE_void && cr->ttype == TYPE_void &&
			(!BATtdense(cl) || !BATcount(cl)) && (!BATtdense(cr) || !BATcount(cr))) { /* const void */
		res = COLnew(0, TYPE_void, sz, TRANSIENT);
		BBPreclaim(cl);
		BBPreclaim(cr);
		BBPreclaim(zzl);
		if (!res)
			throw(MAL, "sort.mproject", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		res->tseqbase = oid_nil;
		BATsetcount(res, sz);
		*R = res->batCacheid;
		BBPkeepref(res);
		return MAL_SUCCEED;
	}
	if (ATOMvarsized(cl->ttype)) {
		res = COLnew2(0, cl->ttype, sz, TRANSIENT, cl->ttype==TYPE_str?cl->twidth:0);
		if (res == NULL) {
			BBPreclaim(cl);
			BBPreclaim(cr);
			BBPreclaim(zzl);
			throw(MAL, "sort.mproject", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (res->ttype == TYPE_str && cl->tvheap->parentid == cr->tvheap->parentid && (BATcount(cl) || BATcount(cr)))
			BATswap_heaps(res, cl, NULL);
		else if (res->tvheap != NULL && res->tvheap->base == NULL) {
			/* this combination can happen since the last
			 * argument of COLnew2 not being zero triggers a
			 * skip in the allocation of the tvheap */
			if (ATOMheap(res->ttype, res->tvheap, res->batCapacity) != GDK_SUCCEED) {
				BBPreclaim(cl);
				BBPreclaim(cr);
				BBPreclaim(zzl);
				throw(MAL, "sort.mproject", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	} else if (cl->ttype == TYPE_void) {
		res = COLnew(0, TYPE_oid, sz, TRANSIENT);
	} else
		res = COLnew(0, cl->ttype, sz, TRANSIENT);

	if (!res) {
		BBPreclaim(cl);
		BBPreclaim(cr);
		BBPreclaim(zzl);
		throw(MAL, "sort.mproject", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	str msg = PPmproject_any(res, zzl, cl, cr);
	if (!msg) {
		*R = res->batCacheid;
		BBPretain(*R);
	}
	BBPreclaim(zzl);
	BBPreclaim(cl);
	BBPreclaim(cr);
	BBPreclaim(res);
	return msg;
}

/* set of (ordered) parts */
#define SOP_SINK 43

typedef struct part_t {
	struct part_t *next;
	int nr;
	bat bats[];
} part_t;

typedef struct sop_t {
	Sink s;
	int nr;
	int nr_workers;
	MT_Lock l;

	part_t *h, *t;
	part_t *workers[];
} sop_t;

static void
sop_destroy( sop_t *q )
{
	for(part_t *e = q->h, *n; e; e = n) {
		n = e->next;
		for(int i = 0; i < e->nr; i++)
			BBPrelease(e->bats[i]);
		GDKfree(e);
	}
	GDKfree(q);
}

static int
sop_done(sop_t *q, int wid, int nr_workers, bool redo)
{
	(void)redo;
	(void)nr_workers;
	int res = 0;
    assert(q->s.type == SOP_SINK);

	MT_lock_set(&q->l);
	assert(q->workers[wid] == 0);
	if (q->h && q->h->next) {
		q->workers[wid] = q->h;
		q->h = q->h->next->next;
		q->workers[wid]->next->next = NULL;
		if (!q->h)
			q->t = NULL;
	} else {
		res = 1;
	}
	MT_lock_unset(&q->l);
	return res;
}

static str
SOPnew(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void)cntxt;
	(void)mb;
	bat *sop = getArgReference_bat(stk, p, 0);
	int nr_workers = *getArgReference_int(stk, p, 1);

	sop_t *q = (sop_t*)GDKzalloc(sizeof(sop_t) + nr_workers*sizeof(part_t*));
	if (!q)
		throw(MAL, "sop.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	q->nr = 0;
	q->h = NULL;
	q->t = NULL;
	MT_lock_init(&q->l, "sop");
	q->s.destroy = (sink_destroy)&sop_destroy;
	q->s.done = (sink_done)&sop_done;
	q->s.type = SOP_SINK;

	BAT *qb = COLnew(0, TYPE_oid, 0 /* need estimate? */, TRANSIENT);
	if (!qb) {
		GDKfree(q);
		throw(MAL, "sop.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	qb->tsink = (Sink*)q;
	*sop = qb->batCacheid;
	BBPkeepref(qb);
	return MAL_SUCCEED;
}

static str
SOPadd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void)cntxt;
	(void)mb;
	int nr = p->argc - p->retc - 1;
	bat *rqbat = getArgReference_bat(stk, p, 0);
	bat *qbat = getArgReference_bat(stk, p, 1);

	*rqbat = *qbat;
	BAT *qb = BATdescriptor(*qbat);
	if (!qb)
		throw(MAL, "sop.add", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	sop_t *q = (sop_t*)qb->tsink;
	assert(q->s.type == SOP_SINK);

	part_t *e = (part_t*)GDKmalloc(sizeof(part_t) + sizeof(bat) * nr);
	if (!e) {
		BBPreclaim(qb);
		throw(MAL, "sop.add", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	e->next = NULL;
	e->nr = nr;
	for (int i = 0; i < nr; i++) {
		e->bats[i] = *getArgReference_bat(stk, p, p->retc + 1 + i);
		BBPretain(e->bats[i]);
	}
	MT_lock_set(&q->l);
	if (q->t)
		q->t->next = e;
	q->t = e;
	if (!q->h)
		q->h = e;
	q->nr++;
	MT_lock_unset(&q->l);
	BBPkeepref(qb);
	return MAL_SUCCEED;
}

static str
SOPfetch(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void)cntxt;
	(void)mb;
	int nr = p->retc;
	bat *qbat = getArgReference_bat(stk, p, p->retc);
	Pipeline *pp = NULL;
	int wid = 0;
	if (p->argc - p->retc == 2) {
		pp = (Pipeline *) *getArgReference_ptr(stk, p, p->retc + 1);
		wid = pp->wid;
	}

	BAT *qb = BATdescriptor(*qbat);
	if (!qb)
		throw(MAL, "sop.dequeue", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	sop_t *q = (sop_t*)qb->tsink;
	assert(q->s.type == SOP_SINK);

	part_t *e = q->workers[wid];
	if (e) {
		q->workers[wid] = e->next;
	} else {
		MT_lock_set(&q->l);
		e = q->h;
		if (e)
			q->h = e->next;
		if (!q->h)
			q->t = NULL;
		q->nr--;
		MT_lock_unset(&q->l);
	}
	if (e) {
		assert(e->nr == nr);
		for(int i = 0; i < nr; i++){
			bat *b = getArgReference_bat(stk, p, i);
			*b = e->bats[i];
		}
		GDKfree(e);
	} else {
		for(int i = 0; i < nr; i++){
			bat *b = getArgReference_bat(stk, p, i);
			int type = getBatType(getArgType(mb, p, i));
			BAT *bp = COLnew(0, type, 0, TRANSIENT);
			*b = bp->batCacheid;
			BBPkeepref(bp);
		}
	}
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func pp_sort_init_funcs[] = {
 pattern("sort", "merge", PPmerge, false, "", args(3,7,
			batarg("rzzl", int), batarg("rzzb", int), batarg("rzza", int),
			batargany("lc", 1), batargany("rc", 1),
			arg("desc", bit), arg("nlast", bit))),
 pattern("sort", "merge", PPmerge, false, "", args(3,10,
			batarg("rzzl", int), batarg("rzzb", int), batarg("rzza", int),
			batargany("lc", 1), batargany("rc", 1),
			batarg("zzl", int), batarg("zzb", int), batarg("zza", int),
			arg("desc", bit), arg("nlast", bit))),
 pattern("sort", "mproject", PPmproject, false, "", args(1,4,
			 batargany("r", 1), batarg("zzl", int), batargany("lc", 1), batargany("rc", 1))),
 pattern("sort", "mproject", PPmproject, false, "", args(1,5,
			 batargany("r", 1), batarg("zzl", int), batargany("lc", 1), batargany("rc", 1), arg("pipeline", ptr))),
 pattern("sop", "new", SOPnew, false, "Create set of (ordered) parts", args(1,2, batarg("sop", oid), arg("nrworkers", int))),
 pattern("sop", "add", SOPadd, false, "add set of bats to the set of (ordered) parts", args(1,3, batarg("sop", oid), batarg("sop", oid), batvarargany("b",0))),
 pattern("sop", "fetch", SOPfetch, false, "fetch a set of bats", args(1,3, batvarargany("res",0), batarg("tree", oid), arg("pipeline", ptr))),
 pattern("sop", "fetch", SOPfetch, false, "fetch a set of bats", args(1,2, batvarargany("res",0), batarg("tree", oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pp_sort", NULL, pp_sort_init_funcs); }
