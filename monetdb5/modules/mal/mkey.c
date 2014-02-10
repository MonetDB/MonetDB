/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) Peter Boncz, Stefan Manegold, Niels Nes
 *
 * new functionality for the low-resource-consumption. It will
 * first one by one create a hash value out of the multiple attributes.
 * This hash value is computed by xoring and rotating individual hash
 * values together. We create a hash and rotate command to do this.
 */
#include "monetdb_config.h"
#include "mkey.h"

/* TODO: nil handling. however; we do not want to lose time in bulk_rotate_xor_hash with that */
static int
CMDrotate(wrd *res, wrd *val, int *n)
{
	*res = GDK_ROTATE(*val, *n, (sizeof(wrd)*8) - *n, (((wrd)1) << *n) - 1);
	return GDK_SUCCEED;
}

str
MKEYhash_bte(wrd *ret, bte *val)
{
	*ret = *val;
	return MAL_SUCCEED;
}

str
MKEYhash_sht(wrd *res, sht *val)
{
	*res = *val;
	return MAL_SUCCEED;
}

str
MKEYhash_int(wrd *res, int *val)
{
	*res = *val;
	return MAL_SUCCEED;
}

str
MKEYhash_flt(wrd *res, flt *val)
{
	*res = *(int*) val;
	return MAL_SUCCEED;
}

str
MKEYhash_wrd(wrd *res, wrd *val)
{
	*res = *val;
	return MAL_SUCCEED;
}

str
MKEYhash_lng(wrd *res, lng *val)
{
#if SIZEOF_WRD == SIZEOF_LNG
	*res = (wrd) *val;
#else
	*res = ((wrd *) val)[0] ^ ((wrd *) val)[1];
#endif
	return MAL_SUCCEED;
}

str
MKEYhash_dbl(wrd *res, dbl *val)
{
#if SIZEOF_WRD == SIZEOF_LNG
	*res = (wrd) *val;
#else
	*res = ((wrd *) val)[0] ^ ((wrd *) val)[1];
#endif
	return MAL_SUCCEED;
}

str
MKEYhash_str(wrd *res, str *val)
{
	BUN h = strHash(*val);
	*res = h;
	return MAL_SUCCEED;
}

static str
voidbathash(BAT **res, BAT *b )
{
	BAT *dst;
	BUN (*hash)(const void *v);
	wrd *r, *f;
	BATiter bi, dsti;

	dst = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (!dst)
		throw(SQL, "mkey.bathash", MAL_MALLOC_FAIL);
	BATseqbase(dst, b->hseqbase);

	dsti = bat_iterator(dst);
	bi = bat_iterator(b);
 	r = (wrd*)BUNtail(dsti, BUNfirst(dst));
	f = r;
	hash = BATatoms[b->ttype].atomHash;
	if (ATOMvarsized(b->ttype)) { /* ugh */
		BUN p,q;

		BATloop(b,p,q) {
			ptr v = BUNtail(bi,p);
			*r++ = hash(v);
		}
	} else {
		char *v = BUNtail(bi,BUNfirst(b)), *e = BUNtail(bi,BUNlast(b));
		int sz = Tsize(b), tpe = b->ttype;

		switch (ATOMstorage(tpe)) {
		case TYPE_bte:
			for(; v < e; v+=sz)
				*r++ = *(bte*)v;
			break;
		case TYPE_sht:
			for(; v < e; v+=sz)
				*r++ = *(sht*)v;
			break;
		case TYPE_int:
		case TYPE_flt:
			for(; v < e; v+=sz)
				*r++ = *(int*)v;
			break;
		case TYPE_lng:
		case TYPE_dbl:
			for(; v < e; v+=sz)
#if SIZEOF_WRD == SIZEOF_LNG
				*r++ = *(wrd*)v;
#else
				*r++ = ((wrd *) v)[0] ^ ((wrd *) v)[1];
#endif
			break;
		default:
			for(; v < e; v+=sz)
				*r++ = hash(v);
		}
	}
	BATsetcount(dst, (BUN) (r-f));
	BATkey(BATmirror(dst), 0);
	dst->hrevsorted = dst->batCount <= 1;
	dst->tsorted = !ATOMvarsized(b->ttype) && Tsize(b) <= sizeof(wrd) && b->tsorted;
	dst->trevsorted = !ATOMvarsized(b->ttype) && Tsize(b) <= sizeof(wrd) && b->trevsorted;
	dst->T->nonil = b->T->nonil;

	*res = dst;
	return MAL_SUCCEED;
}

str
MKEYbathash(bat *res, bat *bid )
{
	str msg;
	BAT *b, *dst = 0;

	if( (b = BATdescriptor(*bid)) == NULL )
		throw(SQL, "mkey.bathash", RUNTIME_OBJECT_MISSING);
	
	assert(BAThvoid(b) || BAThrestricted(b));

	msg = voidbathash(&dst, b);
	if (dst->htype != b->htype) {
		BAT *x = VIEWcreate(b, dst);
		BBPreleaseref(dst->batCacheid);
		dst = x;
	}
	BBPkeepref( *res = dst->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
MKEYrotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	wrd *dst = (wrd*) getArgReference(stk,p,0);
	wrd *h = (wrd*) getArgReference(stk,p,1);
	int *rotate = (int*) getArgReference(stk,p,2);
	int tpe = getArgType(mb,p,3);
	ptr *pval = (ptr) getArgReference(stk,p,3);
	int lbit = *rotate;
	int rbit = (sizeof(wrd)*8) - *rotate;
	wrd mask = (((wrd)1) << lbit) - 1;

	(void) cntxt;
	if (tpe == TYPE_bte) {
		bte *cur = (bte*) pval;
		*dst = GDK_ROTATE(*h, lbit, rbit, mask) ^ *cur;
	} else if (tpe == TYPE_sht) {
		sht *cur = (sht*) pval;
		*dst = GDK_ROTATE(*h, lbit, rbit, mask) ^ *cur;
	} else if (tpe == TYPE_int || tpe == TYPE_flt) {
		int *cur = (int*) pval;
		*dst = GDK_ROTATE(*h, lbit, rbit, mask) ^ *cur;
	} else if (tpe == TYPE_lng || tpe == TYPE_dbl) {
		lng *cur = (lng*) pval;
#if SIZEOF_WRD == SIZEOF_LNG
		wrd val = *(wrd *) cur;
#else
		wrd val = ((wrd *) cur)[0] ^ ((wrd *) cur)[1];
#endif
		*dst = GDK_ROTATE(*h, lbit, rbit, mask) ^ val;
	} else if (tpe == TYPE_str) {	/* TYPE_str */
		str cur = *(str*) pval;
		BUN val = strHash(cur);
		*dst = GDK_ROTATE(*h, lbit, rbit, mask) ^ val;
	} else {
		BUN val = (*BATatoms[tpe].atomHash)(pval);
		*dst = GDK_ROTATE(*h, lbit, rbit, mask) ^ val;
	}
	return MAL_SUCCEED;
}

static str
CMDconstbulk_rotate_xor_hash(BAT **res, wrd *hsh, int *rotate, BAT *b)
{
	BAT* br = NULL;
	BATiter bi = bat_iterator(b);
	wrd *dst = NULL;
	int tpe = ATOMstorage(b->ttype);
	int lbit = *rotate;
	int rbit = (sizeof(wrd)*8) - *rotate;
	wrd mask = (((wrd)1) << lbit) - 1;

	if (*rotate < 0 || *rotate >= (int)(sizeof(wrd)*8)) {
#ifdef _DEBUG_MKEY_
		mnstr_printf(GDKout,"CMDbulk_rotate_xor_hash: (" SZFMT ",%d,%s): illegal number of rotate bits.\n", *hsh, *rotate, BATgetId(b));
#endif
		return " Illegal number of rotate bits";
	}

	br = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (br == NULL) {
#ifdef _DEBUG_MKEY_
		mnstr_printf(GDKout,"CMDbulk_rotate_xor_hash: fail to allocate result BAT[void,wrd] of "SZFMT" tuples.\n", BATcount(b));
#endif
		return MAL_MALLOC_FAIL;
	}
	BATseqbase(br, b->hseqbase);
	dst = (wrd *) Tloc(br, BUNfirst(br));

#define COL_ROTATE(TYPE) \
TYPE *cur = (TYPE *) BUNtloc(bi, BUNfirst(b));\
TYPE *end = (TYPE *) BUNtloc(bi, BUNlast(b));\
\
while (cur < end) {\
	*dst = GDK_ROTATE(*hsh, lbit, rbit, mask) ^ *cur;\
	cur++;\
	dst++;\
}
	if (tpe == TYPE_bte){
		COL_ROTATE(bte);
	} else if (tpe == TYPE_sht){
		COL_ROTATE(sht);
	} else if (tpe == TYPE_int || tpe == TYPE_flt) {
		COL_ROTATE(int);
	} else if (tpe == TYPE_lng || tpe == TYPE_dbl) {
		lng *cur = (lng *) BUNtloc(bi, BUNfirst(b));
		lng *end = (lng *) BUNtloc(bi, BUNlast(b));

		while (cur < end) {
			wrd *t = (wrd*)cur;
#if SIZEOF_WRD == SIZEOF_LNG
			wrd val = *t;
#else
			wrd val = t[0] ^ t[1];
#endif

			*dst = GDK_ROTATE(*hsh, lbit, rbit, mask) ^ val;
			cur++;
			dst++;
		}
	} else if (tpe == TYPE_str) {	/* TYPE_str */
		BUN p, q;

		if (b->T->vheap->hashash) {
			BATloop(b, p, q) {
				str val_p = (str) BUNtvar(bi, p);
				BUN h;
				wrd val;
				h = ((BUN *) val_p)[-1];
				val = (wrd) h;
				*dst = GDK_ROTATE(*hsh, lbit, rbit, mask) ^ val;
				dst++;
			}
		} else {
			BATloop(b, p, q) {
				str val_p = (str) BUNtvar(bi, p);
				BUN h;
				wrd val;
				GDK_STRHASH(val_p, h);
				val = (wrd) h;
				*dst = GDK_ROTATE(*hsh, lbit, rbit, mask) ^ val;
				dst++;
			}
		}
	} else if (b->ttype == TYPE_void) {
		BUN p, q;

		BATloop(b, p, q) {
			*dst = GDK_ROTATE(*hsh, lbit, rbit, mask) ^ *(wrd *) BUNtail(bi, p);
			dst++;
		}
	} else {
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;
		BUN p, q;

		BATloop(b, p, q) {
			BUN val = (*hash)(BUNtail(bi, p));
			*dst = GDK_ROTATE(*hsh, lbit, rbit, mask) ^ val;
			dst++;
		}
	}

	BATsetcount(br, BATcount(b));
	br->hrevsorted = br->batCount <= 1;
	br->tsorted = 0;
	br->trevsorted = 0;
	if (br->tkey)
		BATkey(BATmirror(br), FALSE);
	if (br->htype != b->htype) {
		*res = VIEWcreate(b,br);

		BBPreleaseref(br->batCacheid);
		br = NULL;
	} else {
		*res = br;
	}

	return MAL_SUCCEED;
}

str
MKEYbulkconst_rotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int *res = (int*) getArgReference(stk,p,0);
	int *hid = (int*) getArgReference(stk,p,1);
	int *rotate = (int*) getArgReference(stk,p,2);
	int tpe = getArgType(mb,p,3);
	ptr *pval = (ptr) getArgReference(stk,p,3);
	wrd *dst, *src, *end;
	int lbit = *rotate;
	int rbit = (sizeof(wrd)*8) - *rotate;
	wrd mask = (((wrd)1) << lbit) - 1;
	wrd cur = 0;
	BAT *br, *hn;

	if ((hn = BATdescriptor(*hid)) == NULL)
        	throw(MAL, "mkey.bulk_rotate_xor_hash", RUNTIME_OBJECT_MISSING);

	(void) cntxt;
	if (tpe == TYPE_bte) {
		cur = *(bte*) pval;
	} else if (tpe == TYPE_sht) {
		cur = *(sht*) pval;
	} else if (tpe == TYPE_int || tpe == TYPE_flt) {
		cur = *(int*) pval;
	} else if (tpe == TYPE_lng || tpe == TYPE_dbl) {
		lng *pcur = (lng*) pval;
#if SIZEOF_WRD == SIZEOF_LNG
		cur = *(wrd *) pcur;
#else
		cur = ((wrd *) pcur)[0] ^ ((wrd *) pcur)[1];
#endif
	} else if (tpe == TYPE_str) {	/* TYPE_str */
		str pcur = *(str*) pval;
		cur = strHash(pcur);
	} else {
		cur = (*BATatoms[tpe].atomHash)(pval);
	}
	br = BATnew(TYPE_void, TYPE_wrd, BATcount(hn));
	if (br == NULL) {
#ifdef _DEBUG_MKEY_
		mnstr_printf(GDKout,"CMDbulk_rotate_xor_hash: fail to allocate result BAT[void,wrd] of "SZFMT" tuples.\n", BATcount(hn));
#endif
		return MAL_MALLOC_FAIL;
	}
	BATseqbase(br, hn->hseqbase);
	dst = (wrd *) Tloc(br, BUNfirst(br));
	src = (wrd *) Tloc(hn, BUNfirst(hn));
	end = (wrd *) Tloc(hn, BUNlast(hn));

	while (src < end) {
		*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ cur;
		dst++;
		src++;
	}

	BATsetcount(br, BATcount(hn));
	br->hrevsorted = br->batCount <= 1;
	br->tsorted = 0;
	br->trevsorted = 0;
	if (br->tkey)
		BATkey(BATmirror(br), FALSE);
	if (br->htype != hn->htype) {
		BAT *tmp = VIEWcreate(hn,br);

		BBPreleaseref(br->batCacheid);
		br = tmp;
	}
	BBPkeepref(*res = br->batCacheid);
	BBPreleaseref(hn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbulk_rotate_xor_hash(BAT **res, BAT *bn, int *rotate, BAT *b)
{
	BAT* br = NULL;
	BATiter bi = bat_iterator(b);
	wrd *src = (wrd *) Tloc(bn, BUNfirst(bn));
	wrd *dst = NULL;
	int tpe = ATOMstorage(b->ttype);
	int lbit = *rotate;
	int rbit = (sizeof(wrd)*8) - *rotate;
	wrd mask = (((wrd)1) << lbit) - 1;

	if (!ALIGNsynced(bn, b) && (BATcount(b) || BATcount(bn))) {
#ifdef _DEBUG_MKEY_
		mnstr_printf(GDKout,"#CMDbulk_rotate_xor_hash: (%s,%d,%s): not synced on head.\n", BATgetId(bn), *rotate, BATgetId(b));
#endif
		return " Operands not synced on head";
	} else if (*rotate < 0 || *rotate >= (int)(sizeof(wrd)*8)) {
#ifdef _DEBUG_MKEY_
		mnstr_printf(GDKout,"CMDbulk_rotate_xor_hash: (%s,%d,%s): illegal number of rotate bits.\n", BATgetId(bn), *rotate, BATgetId(b));
#endif
		return " Illegal number of rotate bits";
	}

	br = BATnew(TYPE_void, TYPE_wrd, BATcount(bn));
	if (br == NULL) {
#ifdef _DEBUG_MKEY_
		mnstr_printf(GDKout,"CMDbulk_rotate_xor_hash: fail to allocate result BAT[void,wrd] of "SZFMT" tuples.\n", BATcount(bn));
#endif
		return MAL_MALLOC_FAIL;
	}
	BATseqbase(br, bn->hseqbase);
	dst = (wrd *) Tloc(br, BUNfirst(br));

	if (tpe == TYPE_bte) {
		bte *cur = (bte *) BUNtloc(bi, BUNfirst(b));
		bte *end = (bte *) BUNtloc(bi, BUNlast(b));

		while (cur < end) {
			*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ *cur;
			cur++;
			dst++;
			src++;
		}
	} else if (tpe == TYPE_sht) {
		sht *cur = (sht *) BUNtloc(bi, BUNfirst(b));
		sht *end = (sht *) BUNtloc(bi, BUNlast(b));

		while (cur < end) {
			*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ *cur;
			cur++;
			dst++;
			src++;
		}
	} else if (tpe == TYPE_int || tpe == TYPE_flt) {
		int *cur = (int *) BUNtloc(bi, BUNfirst(b));
		int *end = (int *) BUNtloc(bi, BUNlast(b));

		while (cur < end) {
			*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ *cur;
			cur++;
			dst++;
			src++;
		}
	} else if (tpe == TYPE_lng || tpe == TYPE_dbl) {
		lng *cur = (lng *) BUNtloc(bi, BUNfirst(b));
		lng *end = (lng *) BUNtloc(bi, BUNlast(b));

		while (cur < end) {
			wrd *t = (wrd*)cur;
#if SIZEOF_WRD == SIZEOF_LNG
			wrd val = *t;
#else
			wrd val = t[0] ^ t[1];
#endif

			*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ val;
			cur++;
			dst++;
			src++;
		}
	} else if (tpe == TYPE_str) {	/* TYPE_str */
		BUN p, q;

		if (b->T->vheap->hashash) {
			BATloop(b, p, q) {
				str val_p = (str) BUNtvar(bi, p);
				BUN h;
				wrd val;
				h = ((BUN *) val_p)[-1];
				val = (wrd) h;
				*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ val;
				dst++;
				src++;
			}
		} else {
			BATloop(b, p, q) {
				str val_p = (str) BUNtvar(bi, p);
				BUN h;
				wrd val;
				GDK_STRHASH(val_p, h);
				val = (wrd) h;
				*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ val;
				dst++;
				src++;
			}
		}
	} else if (b->ttype == TYPE_void) {
		BUN p, q;

		BATloop(b, p, q) {
			*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ *(wrd *) BUNtail(bi, p);
			dst++;
			src++;
		}
	} else {
		BUN (*hash)(const void *) = BATatoms[b->ttype].atomHash;
		BUN p, q;

		BATloop(b, p, q) {
			BUN val = (*hash)(BUNtail(bi, p));
			*dst = GDK_ROTATE(*src, lbit, rbit, mask) ^ val;
			dst++;
			src++;
		}
	}

	BATsetcount(br, BATcount(bn));
	br->hrevsorted = br->batCount <= 1;
	br->tsorted = 0;
	br->trevsorted = 0;
	if (br->tkey)
		BATkey(BATmirror(br), FALSE);
	if (br->htype != bn->htype) {
		*res = VIEWcreate(bn,br);

		BBPreleaseref(br->batCacheid);
		br = NULL;
	} else {
		*res = br;
	}

	return MAL_SUCCEED;
}
/*
 * The remainder contains the wrapping needed for M5
 */


str
MKEYrotate(wrd *res, wrd *val, int *n){
	CMDrotate(res,val,n);
	return MAL_SUCCEED;
}

str
MKEYhash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	wrd *ret;
	ptr val;
	int tpe = getArgType(mb,p,1);

	(void) cntxt;
	ret= (wrd*) getArgReference(stk,p,0);
	val= (ptr) getArgReference(stk,p,1);
	if (ATOMextern(tpe))
		val = *(ptr*)val;
	else if (tpe == TYPE_str)
		val = *(str*)val;
	*ret  = (*BATatoms[tpe].atomHash)(val);
	return MAL_SUCCEED;
}

str
MKEYconstbulk_rotate_xor_hash(int *ret, wrd *h, int *nbits, int *bid){
	BAT *b, *bn=0;
	str msg;

	if ((b = BATdescriptor(*bid)) == NULL)
       	    throw(MAL, "mkey.bulk_rotate_xor_hash",  RUNTIME_OBJECT_MISSING);

	if( (msg= CMDconstbulk_rotate_xor_hash(&bn,h,nbits,b)) != MAL_SUCCEED){
	    BBPreleaseref(b->batCacheid);
		throw(MAL, "mkey.bulk_rotate_xor_hash", OPERATION_FAILED "%s", msg);
	}
	BBPreleaseref(b->batCacheid);
	*ret= bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

str
MKEYbulk_rotate_xor_hash(int *ret, int *hid, int *nbits, int *bid){
	BAT *hn, *b, *bn=0;
	str msg;

	if ((hn = BATdescriptor(*hid)) == NULL)
        throw(MAL, "mkey.bulk_rotate_xor_hash", RUNTIME_OBJECT_MISSING);

	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPreleaseref(hn->batCacheid);
        throw(MAL, "mkey.bulk_rotate_xor_hash",  RUNTIME_OBJECT_MISSING);
    }

	if( (msg= CMDbulk_rotate_xor_hash(&bn,hn,nbits,b)) != MAL_SUCCEED){
		BBPreleaseref(hn->batCacheid);
		BBPreleaseref(b->batCacheid);
        throw(MAL, "mkey.bulk_rotate_xor_hash", OPERATION_FAILED "%s", msg);
	}
	BBPreleaseref(hn->batCacheid);
	BBPreleaseref(b->batCacheid);
	*ret= bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}
