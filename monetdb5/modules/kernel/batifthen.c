/*
 *The contents of this file are subject to the MonetDB Public License
 *Version 1.1 (the "License"); you may not use this file except in
 *compliance with the License. You may obtain a copy of the License at
 *http://www.monetdb.org/Legal/MonetDBLicense
 *
 *Software distributed under the License is distributed on an "AS IS"
 *basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 *License for the specific language governing rights and limitations
 *under the License.
 *
 *The Original Code is the MonetDB Database System.
 *
 *The Initial Developer of the Original Code is CWI.
 *Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 *Copyright August 2008-2012 MonetDB B.V.
 *All Rights Reserved.
*/

/*
 *  M.L. Kersten
 * BAT if-then-else multiplex expressions.
 * The assembled code for IF-THEN-ELSE multiplex operations.
 * Again we assume that the BAT arguments are aligned.
 */

#define resBAT2(X1,X2)\
	bn = BATnew(ATOMtype(b->htype), X1, BATcount(b));\
	if (bn == NULL)\
		throw(MAL, X2, MAL_MALLOC_FAIL);\
	bn->hsorted = b->hsorted;\
	bn->hrevsorted = b->hrevsorted;\
	bn->tsorted = b->tsorted;\
	bn->trevsorted = b->trevsorted;\
	bn->H->nonil = b->H->nonil;\
	bn->T->nonil = b->T->nonil;\
	BATkey(bn, BAThkey(b));

#define resBAT(X1,X2)\
	bn = BATnew(ATOMtype(b->htype), TYPE_##X1, BATcount(b));\
	if (bn == NULL)\
		throw(MAL, X2, MAL_MALLOC_FAIL);\
	bn->hsorted = b->hsorted;\
	bn->hrevsorted = b->hrevsorted;\
	bn->tsorted = b->tsorted;\
	bn->trevsorted = b->trevsorted;\
	bn->H->nonil = b->H->nonil;\
	bn->T->nonil = b->T->nonil;\
	BATkey(bn, BAThkey(b));

#define resultBAT(X1,X2)\
	if (BAThvoid(b)) {\
		bn = BATnew(TYPE_void, TYPE_##X1, BATcount(b));\
		BATseqbase(bn, b->hseqbase);\
	} else {\
		bn = BATnew(b->htype, TYPE_##X1, BATcount(b));\
	}\
	if (bn == NULL) {\
		throw(MAL, X2, MAL_MALLOC_FAIL);\
	}\
	bn->hsorted = b->hsorted;\
	bn->hrevsorted = b->hrevsorted;\
	bn->tsorted = b->tsorted;\
	bn->trevsorted = b->trevsorted;\
	if (!BAThvoid(b))\
		bn->H->nonil = b->H->nonil;\
	bn->T->nonil = b->T->nonil;\
	BATkey(bn, BAThkey(b));

#define voidresultBAT(X1,X2)\
	bn = BATnew(TYPE_void, X1, BATcount(b));\
	BATseqbase(bn, b->hseqbase);\
	if (bn == NULL) {\
		throw(MAL, X2, MAL_MALLOC_FAIL);\
	}\
	bn->hsorted = b->hsorted;\
	bn->hrevsorted = b->hrevsorted;\
	bn->tsorted = b->tsorted;\
	bn->trevsorted = b->trevsorted;\
	bn->H->nonil = 1;\
	bn->T->nonil = b->T->nonil;

#include "monetdb_config.h"
#include "batifthen.h"
/*
 * A general assumption in all cases is the bats are synchronized on their
 * head column. This is not checked and may be mis-used to deploy the
 * implementation for shifted window arithmetic as well.
 */
#define wrapup\
    if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);\
    *ret= bn->batCacheid;\
    BBPkeepref(*ret);\
    BBPreleaseref(b->batCacheid);\
    return MAL_SUCCEED;

#define void_wrapup\
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);\
	if (b->htype != bn->htype) {\
		BAT *r = VIEWcreate(b,bn);\
		BBPreleaseref(bn->batCacheid);\
		bn = r;\
	}\
	BBPkeepref(*ret = bn->batCacheid);\
	BBPreleaseref(b->batCacheid);\
	return MAL_SUCCEED;

/* The constant versions are typed by the parser
* String arguments call for an extra type casting. In combination
* with type resolution and runtime checks it provides a dense definition.
*/
#define ifthenImpl(X1,X2,X3)\
		{ X1 nilval=  (X1) X3, *val;\
			resBAT2(X2,"batcalc.ifThen")\
			bn->T->nonil = (b->T->nonil && tb->T->nonil);\
			BATloop(b, p, q) {\
				if (*t == bit_nil) {\
					BUNfastins(bn, BUNhead(bi,p), (ptr) & nilval);\
					bn->T->nonil = 0;\
					bn->T->nil = 1;\
				} else if (*t) {\
					val = (X1*) BUNtail(tbi,p);\
					BUNfastins(bn, BUNhead(bi,p), val);\
				}\
				t++;\
			}\
		}
str
CMDifThen(int *ret, int *bid, int *tid) 
{
	BATiter bi, tbi;
	BAT *b, *tb, *bn;
	BUN p,q;
	bit *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.ifthen", RUNTIME_OBJECT_MISSING);
	if ((tb = BATdescriptor(*tid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "batcalc.ifthen", RUNTIME_OBJECT_MISSING);
	}
	if( BATcount(b) != BATcount(tb) )
		throw(MAL, "batcalc.ifthen", ILLEGAL_ARGUMENT " Requires bats of identical size");

	bi = bat_iterator(b);
	tbi = bat_iterator(tb);
	t = (bit*)Tloc(b,BUNfirst(b));

	BATaccessBegin(b,USE_HEAD, MMAP_SEQUENTIAL);
	BATaccessBegin(tb,USE_TAIL, MMAP_SEQUENTIAL);
	switch(tb->ttype){
	case TYPE_bit: ifthenImpl(bit,TYPE_bit,bit_nil); break;
	case TYPE_bte: ifthenImpl(bte,TYPE_bte,bte_nil); break;
	case TYPE_sht: ifthenImpl(sht,TYPE_sht,sht_nil); break;
	case TYPE_int: ifthenImpl(int,TYPE_int,int_nil); break;
	case TYPE_lng: ifthenImpl(lng,TYPE_lng,lng_nil); break;
	case TYPE_oid: ifthenImpl(oid,TYPE_oid,oid_nil); break;
	case TYPE_flt: ifthenImpl(flt,TYPE_flt,flt_nil); break;
	case TYPE_dbl: ifthenImpl(dbl,TYPE_dbl,dbl_nil); break;
	case TYPE_str:
		{ str nilval= (str) str_nil;

			resBAT(str,"batcalc.ifThen")
			bn->T->nonil = (b->T->nonil && tb->T->nonil);
			BATloop(b, p, q) {
				if (*t == bit_nil)  {
					BUNfastins(bn, BUNhead(bi,p), (ptr) nilval);
					bn->T->nonil = 0;
				} else if (*t ) {
					str *val = (str*) BUNtail(tbi,p);
					BUNfastins(bn, BUNhead(bi,p), val);
				}
				t++;
			}
		}
		break;
	throw(MAL,"batcalc.ifthen",ILLEGAL_ARGUMENT);
	}
	BATaccessEnd(tb,USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(b,USE_HEAD, MMAP_SEQUENTIAL);
	BBPreleaseref(tb->batCacheid);
	wrapup
}

#define ifThenCst(X1,X2,X3)\
		{ X1 nilval= (X1) X3;\
			resBAT2(X2,"batcalc.ifThen")\
			BATloop(b, p, q) {\
				if (*t == bit_nil) \
					BUNfastins(bn, BUNhead(bi,p), (ptr) & nilval);\
				else if (*t) \
					BUNfastins(bn, BUNhead(bi,p), (ptr) tid);\
				t++;\
			}\
			bn->T->nonil = (b->T->nonil && *(X1*)tid != nilval);\
		}

static str CMDifThenCstImpl(int *ret, int *bid, ptr *tid, int type)
{
	BATiter bi;
	BAT *b, *bn;
	BUN p,q;
	bit *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.ifthen", RUNTIME_OBJECT_MISSING);

	bi = bat_iterator(b);
	t = (bit*)Tloc(b,BUNfirst(b));

	BATaccessBegin(b,USE_HEAD, MMAP_SEQUENTIAL);
	switch(type){
	case TYPE_bit: ifThenCst(bit,TYPE_bit,bit_nil); break;
	case TYPE_bte: ifThenCst(bte,TYPE_bte,bte_nil); break;
	case TYPE_sht: ifThenCst(sht,TYPE_sht,sht_nil); break;
	case TYPE_int: ifThenCst(int,TYPE_int,int_nil); break;
	case TYPE_lng: ifThenCst(lng,TYPE_lng,lng_nil); break;
	case TYPE_oid: ifThenCst(oid,TYPE_oid,oid_nil); break;
	case TYPE_flt: ifThenCst(flt,TYPE_flt,flt_nil); break;
	case TYPE_dbl: ifThenCst(dbl,TYPE_dbl,dbl_nil); break;
	case TYPE_str:
		{ str nilval= (str) str_nil;
			resBAT(str,"batcalc.ifThen")
			bn->T->nonil = (b->T->nonil);
			BATloop(b, p, q) {
				if (*t == bit_nil)  {
					BUNfastins(bn, BUNhead(bi,p), (ptr) nilval);
					bn->T->nonil = 0;
				} else if (*t ) 
					BUNfastins(bn, BUNhead(bi,p), *(str*) tid);
				t++;
			}
		}
		break;
	default:
		throw(MAL,"batcalc.ifthen",ILLEGAL_ARGUMENT);
	}
	BATaccessEnd(b,USE_HEAD, MMAP_SEQUENTIAL);
	wrapup
}
str CMDifThenCst_bit(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_bit); }

str CMDifThenCst_bte(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_bte); }

str CMDifThenCst_sht(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_sht); }

str CMDifThenCst_int(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_int); }

str CMDifThenCst_lng(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_lng); }

str CMDifThenCst_oid(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_oid); }

str CMDifThenCst_flt(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_flt); }

str CMDifThenCst_dbl(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_dbl); }

str CMDifThenCst_str(int *ret, int *bid, ptr *tid)
{ return CMDifThenCstImpl(ret, bid, tid, TYPE_str); }

#define ifthenelsecstImpl(Type)\
	{ Type nilval= (Type) Type##_nil, *dst;\
		BUN o;\
		voidresultBAT(TYPE_##Type,"batcalc.ifThenElse")\
		bn->tsorted = FALSE;\
		bn->trevsorted = FALSE;\
		BATkey(BATmirror(bn), FALSE);\
		dst = (Type*)Tloc(bn, BUNfirst(bn));\
		bn->T->nonil = (b->T->nonil && *(Type*)tid != nilval && *(Type*)eid != nilval);\
		for (o=0; o<cnt; o++) {\
			if (t[o] == bit_nil) {\
				dst[o] = nilval;\
				bn->T->nil =1 ;\
				bn->T->nonil = 0;\
			} else if (t[o]) \
				dst[o] = *(Type*)tid;\
			else\
				dst[o] = *(Type*)eid;\
		}\
	}

static str CMDifThenElseCstImpl(int *ret, int *bid, ptr *tid, ptr *eid, int type)
{
	BAT *b, *bn;
	bit *t;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);

	cnt = BATcount(b);
	t = (bit*)Tloc(b,BUNfirst(b));

	BATaccessBegin(b,USE_TAIL, MMAP_SEQUENTIAL);
	switch(type){
	case TYPE_bit: ifthenelsecstImpl(bit); break;
	case TYPE_bte: ifthenelsecstImpl(bte); break;
	case TYPE_sht: ifthenelsecstImpl(sht); break;
	case TYPE_int: ifthenelsecstImpl(int); break;
	case TYPE_lng: ifthenelsecstImpl(lng); break;
	case TYPE_oid: ifthenelsecstImpl(oid); break;
	case TYPE_flt: ifthenelsecstImpl(flt); break;
	case TYPE_dbl: ifthenelsecstImpl(dbl); break;
	case TYPE_str:
		{ str nilval= (str) str_nil;
		  BATiter bi = bat_iterator(b);
		  BUN p,q;

			resBAT(str,"batcalc.ifThen")
			bn->T->nonil = b->T->nonil;
			BATloop(b, p, q) {
				if (*t == bit_nil)  {
					BUNfastins(bn, BUNhead(bi,p), (ptr) nilval);
					bn->T->nonil = 0;
				} else if (*t ) 
					BUNfastins(bn, BUNhead(bi,p), (str)tid);
				else
					BUNfastins(bn, BUNhead(bi,p), (str)eid);

				t++;
			}
		}
		break;
	default:
		throw(MAL,"batcalc.ifthenelse",ILLEGAL_ARGUMENT);
	}
	BATaccessEnd(b,USE_TAIL, MMAP_SEQUENTIAL);
	BATsetcount(bn, cnt);
	void_wrapup
}
str CMDifThenElseCst_bit(int *ret, int *bid, ptr *tid, ptr *eid){
	return CMDifThenElseCstImpl(ret, bid, tid, eid, TYPE_bit);
}
str CMDifThenElseCst_bte(int *ret, int *bid, ptr *tid, ptr *eid){
	return CMDifThenElseCstImpl(ret, bid, tid, eid, TYPE_bte);
}
str CMDifThenElseCst_sht(int *ret, int *bid, ptr *tid, ptr *eid){
	return CMDifThenElseCstImpl(ret, bid, tid, eid, TYPE_sht);
}
str CMDifThenElseCst_int(int *ret, int *bid, ptr *tid, ptr *eid){
	return CMDifThenElseCstImpl(ret, bid, tid, eid, TYPE_int);
}
str CMDifThenElseCst_lng(int *ret, int *bid, ptr *tid, ptr *eid){
	return CMDifThenElseCstImpl(ret, bid, tid, eid, TYPE_lng);
}
str CMDifThenElseCst_flt(int *ret, int *bid, ptr *tid, ptr *eid){
	return CMDifThenElseCstImpl(ret, bid, tid, eid, TYPE_flt);
}
str CMDifThenElseCst_dbl(int *ret, int *bid, ptr *tid, ptr *eid){
	return CMDifThenElseCstImpl(ret, bid, tid, eid, TYPE_dbl);
}
str CMDifThenElseCst_str(int *ret, int *bid, ptr *tid, ptr *eid){
	return CMDifThenElseCstImpl(ret, bid, tid, eid, TYPE_str);
}

#define ifthenelseImpl(Type)\
{	Type nilval= (Type) Type##_nil, *dst, *tbv, *ebv;\
	BUN o;\
	voidresultBAT(TYPE_##Type,"batcalc.ifThenElse")\
	bn->tsorted = FALSE;\
	bn->trevsorted = FALSE;\
	BATkey(BATmirror(bn), FALSE);\
	dst = (Type*)Tloc(bn, BUNfirst(bn));\
	tbv = (Type*)Tloc(tb, BUNfirst(tb));\
	ebv = (Type*)Tloc(eb, BUNfirst(eb));\
	for (o=0; o<cnt; o++) {\
		if (t[o] == bit_nil) \
			dst[o] = nilval;\
		else if (t[o]) \
			dst[o] = tbv[o];\
		else\
			dst[o] = ebv[o];\
	}\
	bn->T->nonil = (b->T->nonil && tb->T->nonil && eb->T->nonil);\
}

str
CMDifThenElse(int *ret, int *bid, int *tid, int *eid)
{
	BAT *b, *tb, *eb, *bn;
	bit *t;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
	if ((tb = BATdescriptor(*tid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
	}
	if ((eb = BATdescriptor(*eid)) == NULL) {
		BBPreleaseref(b->batCacheid); BBPreleaseref(tb->batCacheid);
		throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
	}
	if( BATcount(b) != BATcount(tb) )
		throw(MAL, "batcalc.ifThenElse", ILLEGAL_ARGUMENT " Requires bats of identical size");
	if( BATcount(b) != BATcount(eb) )
		throw(MAL, "batcalc.ifThenElse", ILLEGAL_ARGUMENT " Requires bats of identical size");

	cnt = BATcount(b);
	t = (bit*) Tloc(b, BUNfirst(b));
	BATaccessBegin(b,USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(tb,USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessBegin(eb,USE_TAIL, MMAP_SEQUENTIAL);
	switch(tb->ttype){
	case TYPE_bit: ifthenelseImpl(bit); break;
	case TYPE_bte: ifthenelseImpl(bte); break;
	case TYPE_sht: ifthenelseImpl(sht); break;
	case TYPE_int: ifthenelseImpl(int); break;
	case TYPE_lng: ifthenelseImpl(lng); break;
	case TYPE_oid: ifthenelseImpl(oid); break;
	case TYPE_flt: ifthenelseImpl(flt); break;
	case TYPE_dbl: ifthenelseImpl(dbl); break;
	case TYPE_str:
		{ str nilval= (str) str_nil;
		  BATiter tbi = bat_iterator(tb);
		  BATiter ebi = bat_iterator(eb);
		  BATiter bi = bat_iterator(b);
		  BUN p,q;

			resBAT(str,"batcalc.ifThen")
			bn->T->nonil = (b->T->nonil && tb->T->nonil && eb->T->nonil);
			BATloop(b, p, q) {
				if (*t == bit_nil)  {
					BUNfastins(bn, BUNhead(bi,p), (ptr) nilval);
					bn->T->nonil = 0;
				} else if (*t ) {
					str *val = (str*) BUNtail(tbi,p);
					BUNfastins(bn, BUNhead(bi,p), val);
				} else{
					str *val = (str*) BUNtail(ebi,p);
					BUNfastins(bn, BUNhead(bi,p), val);
				}
				t++;
			}
		}
		break;
	default:
		throw(MAL,"batcalc.ifthenelse",ILLEGAL_ARGUMENT);
	}
	BATaccessEnd(b,USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(tb,USE_TAIL, MMAP_SEQUENTIAL);
	BATaccessEnd(eb,USE_TAIL, MMAP_SEQUENTIAL);
	BATsetcount(bn, cnt);
	BBPreleaseref(tb->batCacheid);
	BBPreleaseref(eb->batCacheid);
	void_wrapup
}

#define ifthenelsecst1(Type)\
{ Type nilval= (Type) Type##_nil, *dst, *ebv;\
	BUN o;\
	voidresultBAT(TYPE_##Type,"batcalc.ifThenElse")\
	bn->tsorted = FALSE;\
	bn->trevsorted = FALSE;\
	BATkey(BATmirror(bn), FALSE);\
	dst = (Type*)Tloc(bn, BUNfirst(bn));\
	ebv = (Type*)Tloc(eb, BUNfirst(eb));\
	for (o=0; o<cnt; o++) {\
		if (t[o] == bit_nil) \
			dst[o] = nilval;\
		else if (t[o]) \
			dst[o] = *(Type*)val;\
		else\
			dst[o] = ebv[o];\
	}\
	bn->T->nonil = (b->T->nonil && eb->T->nonil && *(Type*)val != nilval);\
}

str
CMDifThenElseCst1(int *ret, int *bid, ptr *val, int *eid)
{
	BAT *b, *eb, *bn;
	bit *t;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL) 
		throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
	if ((eb = BATdescriptor(*eid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
	}
	if( BATcount(b) != BATcount(eb) )
		throw(MAL, "batcalc.ifThenElse", ILLEGAL_ARGUMENT " Requires bats of identical size");

	cnt = BATcount(b);
	t = (bit*) Tloc(b,BUNfirst(b));

	switch(eb->ttype){
	case TYPE_bit: ifthenelsecst1(bit); break;
	case TYPE_bte: ifthenelsecst1(bte); break;
	case TYPE_sht: ifthenelsecst1(sht); break;
	case TYPE_int: ifthenelsecst1(int); break;
	case TYPE_lng: ifthenelsecst1(lng); break;
	case TYPE_oid: ifthenelsecst1(oid); break;
	case TYPE_flt: ifthenelsecst1(flt); break;
	case TYPE_dbl: ifthenelsecst1(dbl); break;
	case TYPE_str:
		{ str nilval= (str) str_nil;
		  BATiter ebi = bat_iterator(eb);
		  BATiter bi = bat_iterator(b);
		  BUN p,q;

			resBAT(str,"batcalc.ifThen")
			bn->T->nonil = (b->T->nonil && eb->T->nonil);
			BATloop(b, p, q) {
				if (*t == bit_nil)  {
					BUNfastins(bn, BUNhead(bi,p), (ptr) nilval);
					bn->T->nonil = 0;
				} else if (*t ) 
					BUNfastins(bn, BUNhead(bi,p), *(str*)val);
				else {
					str *v = (str*) BUNtail(ebi,p);
					BUNfastins(bn, BUNhead(bi,p), v);
				}

				t++;
			}
		}
		break;
	default:
		throw(MAL,"batcalc.ifthenelse",ILLEGAL_ARGUMENT);
	}
	cnt = BATcount(b);
	t = (bit*) Tloc(b,BUNfirst(b));
	BATsetcount(bn, cnt);
	BBPreleaseref(eb->batCacheid);
	void_wrapup
}

#define ifthenelsecst2(Type)\
{ Type nilval= (Type) Type##_nil, *dst, *tbv;\
	BUN o;\
	voidresultBAT(TYPE_##Type,"batcalc.ifThenElse")\
	bn->tsorted = FALSE;\
	bn->trevsorted = FALSE;\
	BATkey(BATmirror(bn), FALSE);\
	dst = (Type*)Tloc(bn, BUNfirst(bn));\
	tbv = (Type*)Tloc(tb, BUNfirst(tb));\
	for (o=0; o<cnt; o++) {\
		if (t[o] == bit_nil) \
			dst[o] = nilval;\
		else if (t[o]) \
			dst[o] = tbv[o];\
		else \
			dst[o] = *(Type*)val;\
	}\
	bn->T->nonil = (b->T->nonil && tb->T->nonil && *(Type*)val != nilval);\
}

str
CMDifThenElseCst2(int *ret, int *bid, int *tid, ptr *val)
{
	BAT *b, *tb, *bn;
	bit *t;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
	if ((tb = BATdescriptor(*tid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
	}
	if( BATcount(b) != BATcount(tb) )
		throw(MAL, "batcalc.ifThenElse", ILLEGAL_ARGUMENT " Requires bats of identical size");

	cnt = BATcount(b);
	t = (bit*) Tloc(b,BUNfirst(b));
	switch(tb->ttype){
	case TYPE_bit: ifthenelsecst2(bit); break;
	case TYPE_bte: ifthenelsecst2(bte); break;
	case TYPE_sht: ifthenelsecst2(sht); break;
	case TYPE_int: ifthenelsecst2(int); break;
	case TYPE_lng: ifthenelsecst2(lng); break;
	case TYPE_oid: ifthenelsecst2(oid); break;
	case TYPE_flt: ifthenelsecst2(flt); break;
	case TYPE_dbl: ifthenelsecst2(dbl); break;
	case TYPE_str:
		{ str nilval= (str) str_nil;
		  BATiter tbi = bat_iterator(tb);
		  BATiter bi = bat_iterator(b);
		  BUN p,q;

			resBAT(str,"batcalc.ifThen")
			bn->T->nonil = (b->T->nonil && tb->T->nonil);
			BATloop(b, p, q) {
				if (*t == bit_nil)  {
					BUNfastins(bn, BUNhead(bi,p), (ptr) nilval);
					bn->T->nonil = 0;
				} else if (*t ) {
					str *v = (str*) BUNtail(tbi,p);
					BUNfastins(bn, BUNhead(bi,p), v);
				} else
					BUNfastins(bn, BUNhead(bi,p), *(str*)val);
				t++;
			}
		}
		break;
	default:
		throw(MAL,"batcalc.ifthenelse",ILLEGAL_ARGUMENT);
	}
	BATsetcount(bn, cnt);
	BBPreleaseref(tb->batCacheid);
	void_wrapup
}
