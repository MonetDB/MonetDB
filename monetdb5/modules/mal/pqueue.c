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

#include "monetdb_config.h"
#include "pqueue.h"

#define QTOPN_shuffle(TYPE,OPER,LAB)\
{	TYPE *val = (TYPE *) Tloc(b,BUNfirst(b)), v;\
	for(o = 0; o < lim; o++){\
		v = val[o];\
		oo = o;\
		if( top == size &&  !((TYPE) v OPER (TYPE) val[idx[top-1]]) )\
			continue;\
		for (i= 0; i<top; i++)\
		if ( (TYPE) v OPER (TYPE) val[idx[i]]) {\
			v= val[idx[i]];\
			tmp = idx[i];\
			idx[i]= oo;\
			oo = tmp;\
		} \
		if( top < size)\
			idx[top++] = oo;\
	}\
}

str PQtopn_minmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe, *ret;
	BAT *b,*bn;
	BUN i, size,top = 0;
	oid *idx, lim, o, oo, tmp, off;
	int max = 0;

	(void) cntxt;
	ret = (int*) getArgReference(stk, pci, 0);
	tpe = ATOMstorage(getColumnType(getArgType(mb, pci, 1)));
	size = (BUN) *(wrd*) getArgReference(stk,pci,2);

	max = strstr(getFunctionId(pci),"max") != 0;

	max = (max)?0:1;
	b = BATdescriptor(*(bat *) getArgReference(stk, pci, 1));
	if (!b)
		throw(MAL, "topn_min", RUNTIME_OBJECT_MISSING);
	lim = BATcount(b);
	off = b->hseqbase;	

	if( b->tsorted || b->trevsorted){
		BAT *r;

		if ((b->tsorted && max) || (b->trevsorted && !max)) {
			size_t l = BATcount(b) < size ? 0 : BATcount(b)-size;
			bn = BATslice(b, l, size);
		} else
			bn = BATslice(b, 0, size);
		r = BATmirror(BATmark(bn, 0));
		BBPkeepref(*ret = r->batCacheid);
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bn->batCacheid);
		return MAL_SUCCEED;
	} 

	bn = BATnew(TYPE_void, TYPE_oid, size+1);
	BATseqbase(bn,0);
	idx = (oid*) Tloc(bn,BUNfirst(bn));

	if (!bn){
		BBPreleaseref(b->batCacheid);
		throw(MAL, getFunctionId(pci), RUNTIME_OBJECT_MISSING);
	}
	// shuffle insert new values, keep it simple!
	if( size){
		if ( max )
		switch(tpe){
		case TYPE_bte: QTOPN_shuffle(bte,>,GTR) break;
		case TYPE_sht: QTOPN_shuffle(sht,>,GTR) break;
		case TYPE_int: QTOPN_shuffle(int,>,GTR) break;
		case TYPE_wrd: QTOPN_shuffle(wrd,>,GTR) break;
		case TYPE_lng: QTOPN_shuffle(lng,>,GTR) break;
		case TYPE_flt: QTOPN_shuffle(flt,>,GTR) break;
		case TYPE_dbl: QTOPN_shuffle(dbl,>,GTR) break;
		default:
		{	void  *v;

			for(o = 0; o < lim; o++){
				v = (void*) Tloc(b,o);
				oo = o;
				for (i= 0; i<top; i++)
				if (  atom_CMP( v, Tloc(b,idx[i]), tpe) > 0) {
					v = Tloc(b,idx[i]);
					tmp = idx[i];
					idx[i]= oo;
					oo = tmp;
				} 
				if( top < size)
					idx[top++] = oo;
			}
		}
		}
		if ( max == 0 )
		switch(tpe){
		case TYPE_bte: QTOPN_shuffle(bte,<,LESS) break;
		case TYPE_sht: QTOPN_shuffle(sht,<,LESS) break;
		case TYPE_int: QTOPN_shuffle(int,<,LESS) break;
		case TYPE_wrd: QTOPN_shuffle(wrd,<,LESS) break;
		case TYPE_lng: QTOPN_shuffle(lng,<,LESS) break;
		case TYPE_flt: QTOPN_shuffle(flt,<,LESS) break;
		case TYPE_dbl: QTOPN_shuffle(dbl,<,LESS) break;
		default:
		{	void  *v;
			for(o = 0; o < lim; o++){
				v = (void*) Tloc(b,o);
				oo = o;
				for (i= 0; i<top; i++)
				if ( atom_CMP( v, Tloc(b,idx[i]), tpe) < 0) {
					v = Tloc(b,idx[i]);
					tmp = idx[i];
					idx[i]= oo;
					oo = tmp;
				} 
				if( top < size)
					idx[top++] = oo;
			}
		}
		}
	}
	
	BATsetcount(bn, (BUN)  top);
	BATderiveProps(bn, TRUE);
	if (off) {
		for (i=0;i<top; i++)
			idx[i] += off;
		BATseqbase(BATmirror(bn), off);
	}

	BBPkeepref(*ret = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

/* some new code for headless */
#define QTOPN_shuffle2(TYPE,OPER)\
{	TYPE *val = (TYPE *) Tloc(b,BUNfirst(b));\
	uniq = 0;\
	for(o = 0; o < lim; o++){\
		if(uniq >= size &&  !((TYPE) val[o] OPER##= (TYPE) val[idx[top-1]]) )\
			continue;\
		idx[top] = gdx[top] = o;\
		uniq++;\
		for (i= top; i>0; i--){\
			if( (TYPE) val[idx[i]] OPER (TYPE) val[idx[i-1]]){\
				tmp= idx[i]; idx[i] = idx[i-1]; idx[i-1] = tmp;\
				tmp= gdx[i]; gdx[i] = gdx[i-1]; gdx[i-1] = tmp;\
			} else if( (TYPE) val[idx[i]] == (TYPE) val[idx[i-1]]){\
				uniq--; gdx[i] = gdx[i-1];\
				break;\
			} else break;\
		}\
		if( uniq <= size) top++;\
	}\
}

str PQtopn2_minmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe, *ret, *ret1;
	BAT *b,*bpiv, *bgid;
	BUN i, size, top = 0, uniq;
	oid *idx, *gdx, lim, o, tmp, off;
	int max = 0;

	(void) cntxt;
	ret = (int*) getArgReference(stk, pci, 0);
	ret1 = (int*) getArgReference(stk, pci, 1);
	tpe = ATOMstorage(getColumnType(getArgType(mb, pci, 2)));
	size = (BUN) *(wrd*) getArgReference(stk,pci,3);
	max = strstr(getFunctionId(pci),"max") != 0;

	max = (max)?0:1;
	b = BATdescriptor(*(bat *) getArgReference(stk, pci, 2));
	if (!b)
		throw(MAL, "topn_min", RUNTIME_OBJECT_MISSING);
	off = b->hseqbase;	

	bpiv = BATnew(TYPE_void, TYPE_oid, BATcount(b));
	if (!bpiv){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "topn_min", RUNTIME_OBJECT_MISSING);
	}
	bgid = BATnew(TYPE_void, TYPE_oid, BATcount(b));
	if (!bgid){
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bpiv->batCacheid);
		throw(MAL, "topn_min", RUNTIME_OBJECT_MISSING);
	}
	lim = BATcount(b);

	BATseqbase(bpiv,0);
	BATseqbase(bgid,0);
	idx = (oid*) Tloc(bpiv,BUNfirst(bpiv));
	gdx = (oid*) Tloc(bgid,BUNfirst(bgid));

	// shuffle insert new values, keep it simple!
	if( size){
		if ( max ==0)
		switch(tpe){
		case TYPE_bte: QTOPN_shuffle2(bte,<) break;
		case TYPE_sht: QTOPN_shuffle2(sht,<) break;
		case TYPE_int: QTOPN_shuffle2(int,<) break;
		case TYPE_wrd: QTOPN_shuffle2(wrd,<) break;
		case TYPE_lng: QTOPN_shuffle2(lng,<) break;
		case TYPE_flt: QTOPN_shuffle2(flt,<) break;
		case TYPE_dbl: QTOPN_shuffle2(dbl,<) break;
		default:
		{	int k;
			uniq = 0;
			for(o = 0; o < lim; o++){
				k = atom_CMP( Tloc(b,o), Tloc(b,idx[top-1]), tpe) >= 0;
				if( uniq >= size &&  k) 
					continue;
				uniq++;
				idx[top] = gdx[top] = o;
				for (i= top; i>0; i--){
					if ( (k = atom_CMP( Tloc(b,idx[i]), Tloc(b,idx[i-1]), tpe)) < 0) {
						tmp= idx[i]; idx[i] = idx[i-1]; idx[i-1] = tmp;
						tmp= gdx[i]; gdx[i] = gdx[i-1]; gdx[i-1] = tmp;
					} else if ( atom_CMP( Tloc(b,idx[i]), Tloc(b,idx[i-1]), tpe) == 0) {
						gdx[i] = gdx[i-1];
						uniq--;
						break;
					} else break;
				}
				if( uniq < size) top++;
			}
		}
		}
		if ( max )
		switch(tpe){
		case TYPE_bte: QTOPN_shuffle2(bte,>) break;
		case TYPE_sht: QTOPN_shuffle2(sht,>) break;
		case TYPE_int: QTOPN_shuffle2(int,>) break;
		case TYPE_wrd: QTOPN_shuffle2(wrd,>) break;
		case TYPE_lng: QTOPN_shuffle2(lng,>) break;
		case TYPE_flt: QTOPN_shuffle2(flt,>) break;
		case TYPE_dbl: QTOPN_shuffle2(dbl,>) break;
		default:
		{	int k;
			uniq=0;
			for(o = 0; o < lim; o++){
				k = atom_CMP( Tloc(b,o), Tloc(b,idx[top-1]), tpe) <= 0;
				if( uniq >= size &&  k) 
					continue;
				idx[top] = gdx[top] = o;
				uniq++;
				for (i= top; i>0; i--){
					if ( (k = atom_CMP( Tloc(b,idx[i]), Tloc(b,idx[i-1]), tpe)) > 0) {
						tmp= idx[i]; idx[i] = idx[i-1]; idx[i-1] = tmp;
						tmp= gdx[i]; gdx[i] = gdx[i-1]; gdx[i-1] = tmp;
					} else if ( atom_CMP( Tloc(b,idx[i]), Tloc(b,idx[i-1]), tpe) == 0) {
						gdx[i] = gdx[i-1];
						uniq--;
						break;
					} else break;
				}
				if( uniq < size) top++;
			}
		}
		}
	}
	
	BATsetcount(bpiv, (BUN)  top);
	BATsetcount(bgid, (BUN)  top);
	BATderiveProps(bpiv, TRUE);
	BATderiveProps(bgid, TRUE);
	if (off) {
		for (i=0;i<top; i++)
			idx[i] += off;
		BATseqbase(BATmirror(bpiv), off);
	}

	BBPkeepref(*ret = bpiv->batCacheid);
	BBPkeepref(*ret1 = bgid->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

/* some new code for headless */
#define QTOPN_shuffle3(TYPE,OPER)\
{	TYPE *val = (TYPE *) Tloc(a,BUNfirst(a));\
	uniq = 0;\
	gid = BUN_MAX;\
	for(o = 0; uniq <= size && o < lim; o++){\
		cpx[top] = bpx[o];\
		cgx[top] = bgx[o];\
		if ( cgx[top] != gid){\
			gid = cgx[o];\
			top++;\
			uniq++;\
			continue;\
		}\
		if(uniq >= size &&  (TYPE) val[cpx[o]] OPER##= (TYPE) val[cpx[top-1]]) \
			continue;\
		for (i= top; i>0; i--){\
			if ( cgx[i-1] != gid)\
				break;\
			if( (TYPE) val[cpx[i]] OPER (TYPE) val[cpx[i-1]]){\
				tmp= cpx[i]; cpx[i] = cpx[i-1]; cpx[i-1] = tmp;\
				tmp= cgx[i]; cgx[i] = cgx[i-1]; cgx[i-1] = tmp;\
			} else\
				break;\
		}\
		top++; \
	}\
}

str PQtopn3_minmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	
	int tpe, *retcp,*retcg;
	BAT *bp,*bg, *a, *cp, *cg;
	BUN i, size, top = 0, uniq, gid;
	oid *bpx, *bgx, *cpx, *cgx, lim, o, tmp;
	int k,max = 0;

	(void) cntxt;
	(void) k;
	retcp = (int*) getArgReference(stk, pci, 0);
	retcg = (int*) getArgReference(stk, pci, 1);
	tpe = ATOMstorage(getColumnType(getArgType(mb, pci, 2)));
	size = (BUN) *(wrd*) getArgReference(stk,pci,5);
	max = strstr(getFunctionId(pci),"max") != 0;

	max = (max)?0:1;
	a = BATdescriptor(*(bat *) getArgReference(stk, pci, 2));
	if (!a)
		throw(MAL, "topn_min", RUNTIME_OBJECT_MISSING);

	bp = BATdescriptor(*(bat *) getArgReference(stk, pci, 3));
	if (!bp){
		BBPreleaseref(a->batCacheid);
		throw(MAL, "topn_min", RUNTIME_OBJECT_MISSING);
	}

	bg = BATdescriptor(*(bat *) getArgReference(stk, pci, 4));
	if (!bg){
		BBPreleaseref(a->batCacheid);
		BBPreleaseref(bp->batCacheid);
		throw(MAL, "topn_min", RUNTIME_OBJECT_MISSING);
	}

	cp = BATnew(TYPE_void, TYPE_oid, BATcount(bp));
	if (!cp){
		BBPreleaseref(a->batCacheid);
		BBPreleaseref(bg->batCacheid);
		BBPreleaseref(bp->batCacheid);
		throw(MAL, "topn_min", MAL_MALLOC_FAIL);
	}
	cg = BATnew(TYPE_void, TYPE_oid, BATcount(bp));
	if (!cg){
		BBPreleaseref(a->batCacheid);
		BBPreleaseref(bg->batCacheid);
		BBPreleaseref(bp->batCacheid);
		BBPreleaseref(cp->batCacheid);
		throw(MAL, "topn_min", MAL_MALLOC_FAIL);
	}

	lim = BATcount(bp);
	BATseqbase(cp,0);
	BATseqbase(cg,0);

	bgx = (oid*) Tloc(bg,BUNfirst(bg));
	bpx = (oid*) Tloc(bp,BUNfirst(bp));

	cpx = (oid*) Tloc(cp,BUNfirst(cp));
	cgx = (oid*) Tloc(cg,BUNfirst(cg));

	// shuffle insert new values, keep it simple!
	if( size){
		if ( max ==0)
		switch(tpe){
		case TYPE_bte: QTOPN_shuffle3(bte,<) break;
		case TYPE_sht: QTOPN_shuffle3(sht,<) break;
		case TYPE_int: QTOPN_shuffle3(int,<) break;
		case TYPE_wrd: QTOPN_shuffle3(wrd,<) break;
		case TYPE_lng: QTOPN_shuffle3(lng,<) break;
		case TYPE_flt: QTOPN_shuffle3(flt,<) break;
		case TYPE_dbl: QTOPN_shuffle3(dbl,<) break;
		default:
		{	uniq = 0;
			gid = BUN_MAX;
			for(o = 0; uniq<=size && o < lim; o++){
				cpx[top] = bpx[o];
				cgx[top] = bgx[o];
				if ( cgx[top] != gid){
					gid = cgx[o];\
					top++;
					uniq++;
					continue;
				}
				k = atom_CMP( Tloc(bp,cpx[o]), Tloc(bp,cpx[top-1]), tpe) <= 0;
				if( uniq >= size &&  k) 
					continue;
				for (i= top; i>0; i--){
					if ( cgx[i-1] != gid)
						break;
					if ( (k = atom_CMP( Tloc(bp,cpx[i]), Tloc(bp,cpx[i-1]), tpe)) < 0) {
						tmp= cpx[i]; cpx[i] = cpx[i-1]; cpx[i-1] = tmp;
						tmp= cgx[i]; cgx[i] = cgx[i-1]; cgx[i-1] = tmp;
					} else
						break;
				}
				top++; 
			}
		}

		}
		if ( max )
		switch(tpe){
		case TYPE_bte: QTOPN_shuffle3(bte,>) break;
		case TYPE_sht: QTOPN_shuffle3(sht,>) break;
		case TYPE_int: QTOPN_shuffle3(int,>) break;
		case TYPE_wrd: QTOPN_shuffle3(wrd,>) break;
		case TYPE_lng: QTOPN_shuffle3(lng,>) break;
		case TYPE_flt: QTOPN_shuffle3(flt,>) break;
		case TYPE_dbl: QTOPN_shuffle3(dbl,>) break;
		default:
		{	uniq = 0;
			gid = BUN_MAX;
			for(o = 0; uniq<=size && o < lim; o++){
				cpx[top] = bpx[o];
				cgx[top] = bgx[o];
				if ( cgx[top] != gid){
					gid = cgx[o];\
					top++;
					uniq++;
					continue;
				}
				k = atom_CMP( Tloc(bp,cpx[o]), Tloc(bp,cpx[top-1]), tpe) <= 0;
				if( uniq >= size &&  k) 
					continue;
				for (i= top; i>0; i--){
					if ( cgx[i-1] != gid)
						break;
					if ( (k = atom_CMP( Tloc(bp,cpx[i]), Tloc(bp,cpx[i-1]), tpe)) > 0) {
						tmp= cpx[i]; cpx[i] = cpx[i-1]; cpx[i-1] = tmp;
						tmp= cgx[i]; cgx[i] = cgx[i-1]; cgx[i-1] = tmp;
					} else
						break;
				}
				top++; 
			}
		}
		}
	}
	
	BATsetcount(cg, (BUN)  top);
	BATderiveProps(cg, TRUE);
	BATsetcount(cp, (BUN)  top);
	BATderiveProps(cp, TRUE);

	BBPkeepref(*retcp = cp->batCacheid);
	BBPkeepref(*retcg = cg->batCacheid);
	BBPreleaseref(bp->batCacheid);
	BBPreleaseref(bg->batCacheid);
	BBPreleaseref(a->batCacheid);
	return MAL_SUCCEED;
}
