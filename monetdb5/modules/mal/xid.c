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
 * author Martin Kersten
 * Light-weight compress oid columns to reduce temporary storage footprint.
*/
#include "xid.h"

static long
XIDencode(XIDcolumn col, oid *p, oid *q)
{
	lng v, prev=0;
	long i=0,scnt =0; 
	//long point=0, range=0,set=0;

	col[++i].tag = XIDBASE;
	for ( v= *(oid*)p ; p<q; p++, v= *(oid*) p)
		switch ( (unsigned int) col[i].tag & XIDMASK ){
		case XIDBASE:
			col[i].tag = XIDPOINT;
			col[i].value = v;
			//mnstr_printf(GDKout,"xidpoint %d %ld\n",i,v);
			break;
		case XIDSET:
			/* watch out for duplicates */
			if ( v > col[i-1].value && v <= col[i-1].value + 61 &&
				(col[i].value &  ((long)1)<< (v - col[i-1].value)) == 0){
				col[i].value |=  ( ((long)1)<< (v - col[i-1].value));
				scnt++;
				prev= v;
				//mnstr_printf(GDKout,"xidset %d %ld\n",i,(v - col[i-1].value));
				break;
			}
			if (scnt == 1) { 
				col[i].tag = XIDPOINT;
				col[i].value = prev;
				scnt =0;
			}
			i++;
			col[i].tag = XIDPOINT;
			col[i].value = v;
			//mnstr_printf(GDKout,"xidpoint %d %ld\n",i,v);
			//point++;
			break;
		case XIDRANGE:
			if ( col[i].value + 1 == v){
				col[i].value = v;
				//mnstr_printf(GDKout,"xidrange %d %ld %ld\n",i,(long) col[i-1].value, v);
				break;
			} 
			/* fall back to point if spread to large */
			i++;
			col[i].tag = XIDPOINT;
			col[i].value = v;
			//mnstr_printf(GDKout,"xidpoint %d %ld\n",i,v);
			//point++;
			break;
		case XIDPOINT:
			if ( col[i].value + 1 == v){
				/* make a new range */
				col[i].tag = XIDRANGE;
				col[++i].tag = XIDRANGE;
				col[i].value = v;
				//mnstr_printf(GDKout,"xidrange %d %ld %ld\n",i,(long) col[i-1].value, v);
				//range++;
				break;
			} 
			if ( v > col[i].value && v <= col[i].value + 61 ){
				/* make a new set */
				i++;
				col[i].tag = XIDSET;
				scnt = 1;
				prev = v;
				col[i].value =  (1 << (v -col[i-1].value));
				//mnstr_printf(GDKout,"xidset %d %ld\n",i,(long) (v- col[i-1].value));
				//set++;
				break;
			} 
			i++;
			col[i].tag = XIDPOINT;
			col[i].value = v;
			//mnstr_printf(GDKout,"xidpoint %d %ld\n",i,v);
		}
	//mnstr_printf(GDKout,"stats point %ld range %ld set %ld\n",point,range,set);
	return i;
}
str
XIDcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	int *ret = (int*) getArgReference(stk,pci,0);
	int *bid = (int*) getArgReference(stk,pci,1);
	BAT *b, *bn;
	oid *p,*q;
	XIDcolumn col;
	long clk, i=0;

	(void) mb;
	(void) cntxt;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "xid.compress", INTERNAL_BAT_ACCESS);
	if ( isVIEW(b) || BATcount(b) == 0){
		mnstr_printf(cntxt->fdout,"#xid %s view %d\n",(BATcount(b)==0 ? "empty":""), getArg(pci,1));
		b->T->heap.xidcompressed = 0;
		b->H->heap.xidcompressed = 0;
		BBPkeepref(*ret = b->batCacheid);
		return MAL_SUCCEED;
	}
	bn = BATnew(b->htype,b->ttype, BATcount(b)+1);
	if (bn == NULL)
		throw(MAL,"xid.compress", MAL_MALLOC_FAIL);

	if ( b->ttype == TYPE_oid){
		p = (oid*)Tloc(b,BUNfirst(b));
		q = (oid*)Tloc(b,BUNlast(b));
		col = (XIDcolumn) bn->T->heap.base;

		clk = GDKusec();
		i= XIDencode(col,p,q);
		col[0].tag  = XIDBASE;
		col[0].value = i+1; /* keep compression size */
		mnstr_printf(cntxt->fdout,"#xid, %d, tail compress, %ld,%ld,  clk %ld ms %ld usec\n",
			getArg(pci,0), BATcount(b), i, (long) GDKusec()/1000,(long) GDKusec()-clk);

		bn->T->heap.xidcompressed = 1;
		bn->batDirty =1;
	}
	bn->T->sorted = b->T->sorted;
	bn->T->revsorted = b->T->revsorted;
	bn->T->dense = b->T->dense;
	bn->T->key = b->T->key;
	bn->T->nonil = b->T->nonil;
	bn->T->nil = b->T->nil;
	bn->T->seq = b->T->seq;

	if ( b->htype == TYPE_oid){
		p = (oid*)Hloc(b,BUNfirst(b));
		q = (oid*)Hloc(b,BUNlast(b));
		col = (XIDcolumn) bn->H->heap.base;

		clk = GDKusec();
		i= XIDencode(col,p,q);
		col[0].tag  = XIDBASE;
		col[0].value = i+1; /* keep compression size */
		mnstr_printf(cntxt->fdout,"#xid, %d, head compress, %ld,%ld,  clk %ld ms %ld usec\n",
			getArg(pci,0), BATcount(b), i, (long) GDKusec()/1000,(long) GDKusec()-clk);
		bn->H->heap.xidcompressed = 1;
		bn->batDirty =1;
	}

	bn->H->sorted = b->H->sorted;
	bn->H->revsorted = b->H->revsorted;
	bn->H->dense = b->H->dense;
	bn->H->key = b->H->key;
	bn->H->nonil = b->H->nonil;
	bn->H->nil = b->H->nil;
	bn->H->seq = b->H->seq;

	/* don't set the BATcount of bn to avoid property checks */
	BBPreleaseref(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static long XIDdecode(XIDcolumn col, oid *o, long lim)
{
	oid v,w;
	long cnt=0, i,j;
	
	for ( i=1, v= col[i].value ; i<lim; i++, v= col[i].value)
		switch ( (unsigned int) col[i].tag & XIDMASK ){
		case XIDSET:
			v= col[i-1].value;
			for ( j=0; j< 61; j++) 
			if ( col[i].value & (((long)1)<<j)){
				w= v+j;
				*o++ = w;
				cnt++;
			}
			break;
		case XIDRANGE:
			for( j = col[i].value, i++; j <= col[i].value; j++, v++){
				*o++ = v;
				cnt++;
			}
			break;
		case XIDPOINT:
			*o++ = v;
			cnt++;
		}
	return cnt;
}

str
XIDdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	int *ret = (int*) getArgReference(stk,pci,0);
	int *bid = (int*) getArgReference(stk,pci,1);
	BAT *b, *bn;
	long cnt, lim;
	XIDcolumn col;
	oid *o;
	long clk;

	(void) mb;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "xid.decompress", INTERNAL_BAT_ACCESS);
	if ( isVIEW(b)){
		BBPkeepref(*ret = b->batCacheid);
		return MAL_SUCCEED;
	}
	if ( b->T->heap.xidcompressed == 0 && b->H->heap.xidcompressed == 0){
		BBPkeepref(*ret = b->batCacheid);
		return MAL_SUCCEED;
	}

	bn = BATnew(b->htype, b->ttype, BATcapacity(b));
	if ( bn == NULL)
		throw(MAL, "xid.decompress", MAL_MALLOC_FAIL);

	if ( b->T->heap.xidcompressed )
	{
		col = (XIDcolumn) b->T->heap.base;
		lim = col[0].value;
		o = (oid*)Tloc(bn,BUNfirst(bn));

		clk = GDKusec();
		cnt = XIDdecode(col,o,lim);
		mnstr_printf(cntxt->fdout,"#xid, %d, decompress, %ld, %ld, clk %ld %ld usec\n",
			getArg(pci,1), lim, cnt, (long) GDKusec()/1000, (long) GDKusec()-clk);

		BATsetcount(bn, cnt);
		bn->batDirty =1;
	}
	bn->T->heap.xidcompressed = 0;
	bn->T->sorted = b->T->sorted;
	bn->T->revsorted = b->T->revsorted;
	bn->T->dense = b->T->dense;
	bn->T->key = b->T->key;
	bn->T->nonil = b->T->nonil;
	bn->T->nil = b->T->nil;
	bn->T->seq = b->T->seq;

	if ( b->H->heap.xidcompressed )
	{
		col = (XIDcolumn) b->H->heap.base;
		lim = col[0].value;
		o = (oid*)Hloc(bn,BUNfirst(bn));

		clk = GDKusec();
		cnt = XIDdecode(col,o,lim);
		mnstr_printf(cntxt->fdout,"#xid, %d, decompress, %ld, %ld , clk %ld %ld usec\n",
			getArg(pci,1), lim, cnt, (long) GDKusec()/1000, (long) GDKusec()-clk);

		BATsetcount(bn, cnt);
		bn->batDirty =1;
	}
	bn->H->heap.xidcompressed = 0;
	bn->H->sorted = b->H->sorted;
	bn->H->revsorted = b->H->revsorted;
	bn->H->dense = b->H->dense;
	bn->H->key = b->H->key;
	bn->H->nonil = b->H->nonil;
	bn->H->nil = b->H->nil;
	bn->H->seq = b->H->seq;

	BBPreleaseref(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static void XIDcolumndump(stream *f, XIDcolumn col, long lim)
{
	oid v;
	long i;

	for ( i=1, v= col[i].value ; i<lim; i++, v= col[i].value){
        switch ( (unsigned int) (col[i].tag & XIDMASK) ){
        case XIDBASE:
			mnstr_printf(f,"x:%ld", (long) v);
			break;
        case XIDSET:
			mnstr_printf(f,"s:[%ld] 0%lo",(long) col[i-1].value,v);
			break;
        case XIDRANGE:
			mnstr_printf(f,"r:%ld %ld", (long) v, (long) col[i+1].value);
			i++;
			break;
        case XIDPOINT:
			mnstr_printf(f,"p:%ld", (long) v);
        }
		mnstr_printf(f,"\n");
	}
}
str
XIDdump(int *ret, int *bid)
{	
	BAT *b;
	XIDcolumn col;
	long lim;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "xid.dump", INTERNAL_BAT_ACCESS);
	if ( b->T->heap.xidcompressed){
		col = (XIDcolumn) b->T->heap.base;
		lim = col[0].value;

		mnstr_printf(GDKout,"column first %ld, size "BUNFMT", \n", lim, BATcount(b));
		XIDcolumndump(GDKout,col,lim);
	}
	if ( b->H->heap.xidcompressed){
		col = (XIDcolumn) b->H->heap.base;
		lim = col[0].value;

		mnstr_printf(GDKout,"column first %ld, size "BUNFMT", \n", lim, BATcount(b));
		XIDcolumndump(GDKout,col,lim);
	}
	BBPreleaseref(*bid);
	(void) ret;
	return MAL_SUCCEED;
}
