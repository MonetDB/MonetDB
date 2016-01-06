/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * author Martin Kersten
 * Z-order
 * This module provides the primitives to implement 2-dim Z-order functionality.
 * Arrays stored in Z-order have a better locality of reference for many
 * operations. Slicing part of the array amounts to deriving a BAT with
 * the z-order indices required, whereafter a simple semijoin would be
 * sufficient to fetch the values.
 *
 * The encoding produces OIDs, which makes it easy to align
 * any void headed BAT as a sorted Z-ordered representation.
 * This gives both fast point access and clustered slicing.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "zorder.h"

static inline oid Zencode_int_oid(int x, int y)
{
	oid v = 0;
	int i,mask=1;
	for ( i = 0; i < (int) (8 * sizeof(oid)/2) ; i++) {
		v |= ((x & 1) * mask);
		x>>=1;
		mask <<= 1;
		v |= ((y & 1) * mask);
		y>>=1;
		mask <<= 1;
	}
	return v;
}

static inline void Zdecode_int_oid(int *x, int *y, oid *z)
{
	int xv = 0, yv=0, mask =1;
	oid zv = *z;
	int i;
	for ( i = 0; i < (int) (8 * sizeof(oid)); i+= 2) {
		xv |= ((zv & 1) * mask);
		zv >>= 1;
		yv |= ((zv & 1) * mask);
		zv >>= 1;
		mask <<=1;
	}
	*x = xv;
	*y = yv;
}
static inline void Zdecode_int_oid_x(int *x, oid *z)
{
	int xv = 0, mask =1;
	oid zv = *z;
	int i;
	for ( i = 0; i < (int) (8 * sizeof(oid)); i+= 2) {
		xv |= ((zv & 1) * mask);
		zv >>= 2;
		mask <<=1;
	}
	*x = xv;
}
static inline void Zdecode_int_oid_y(int *y, oid *z)
{
	int yv=0, mask =1;
	oid zv = *z;
	int i;
	for ( i = 0; i < (int) (8 * sizeof(oid)); i+= 2) {
		zv >>= 1;
		yv |= ((zv & 1) * mask);
		zv >>= 1;
		mask <<=1;
	}
	*y = yv;
}

str 
ZORDencode_int_oid(oid *z, int *x, int *y)
{
	*z = Zencode_int_oid(*x,*y);
	return MAL_SUCCEED;
}

str 
ZORDdecode_int_oid(int *x, int *y, oid *z)
{
	Zdecode_int_oid(x,y,z);
	return MAL_SUCCEED;
}

str 
ZORDdecode_int_oid_x(int *x, oid *z)
{
	Zdecode_int_oid_x(x,z);
	return MAL_SUCCEED;
}

str 
ZORDdecode_int_oid_y(int *y, oid *z)
{
	Zdecode_int_oid_y(y,z);
	return MAL_SUCCEED;
}

str
ZORDbatencode_int_oid(bat *zbid, bat *xbid, bat *ybid)
{
	BAT *bx, *by,*bz;
	int *p, *q, *r;
	oid *z;

	bx = BATdescriptor(*xbid);
	by = BATdescriptor(*ybid);
	if ( bx == 0 || by == 0){
		if ( bx ) BBPunfix(bx->batCacheid);
		if ( by ) BBPunfix(by->batCacheid);
		throw(OPTIMIZER, "zorder.encode", RUNTIME_OBJECT_MISSING);
	}
	if ( BATcount(bx) != BATcount(by)){
		BBPunfix(bx->batCacheid);
		BBPunfix(by->batCacheid);
		throw(OPTIMIZER, "zorder.encode", ILLEGAL_ARGUMENT);
	}
	
	bz = BATnew(TYPE_void, TYPE_oid, BATcount(bx), TRANSIENT);
	if (bz == 0){
		BBPunfix(bx->batCacheid);
		BBPunfix(by->batCacheid);
		throw(OPTIMIZER, "zorder.encode", MAL_MALLOC_FAIL);
	}
	p = (int *) Tloc(bx, BUNfirst(bx));
	q = (int *) Tloc(bx, BUNlast(bx));
	r = (int *) Tloc(by, BUNfirst(by));
	z = (oid *) Tloc(bz, BUNfirst(bz));

	if ( bx->T->nonil && by->T->nonil){
		for ( ; p<q; z++,p++,r++)
			*z = Zencode_int_oid( *p, *r );
	} else
	if ( bx->T->nonil ){
		for ( ; p<q; z++,p++,r++)
		if ( *r == int_nil)
			*z = oid_nil;
		else
			*z = Zencode_int_oid( *p, *r );
	} else
	if ( by->T->nonil ){
		for ( ; p<q; z++,p++,r++)
		if ( *p == int_nil)
			*z = oid_nil;
		else
			*z = Zencode_int_oid( *p, *r );
	} else {
		for ( ; p<q; z++,p++,r++)
		if ( *r == int_nil)
			*z = oid_nil;
		else
		if ( *p == int_nil)
			*z = oid_nil;
		else
			*z = Zencode_int_oid( *p, *r );
	}

	BBPunfix(bx->batCacheid);
	BBPunfix(by->batCacheid);

	if (!(bz->batDirty&2)) 
		BATsetaccess(bz, BAT_READ);
	BATsetcount(bz, BATcount(bx));
	BATseqbase(bz, bx->hseqbase);
	bz->hsorted = 1;
	bz->hrevsorted = 0;
	bz->tsorted = 0;
	bz->trevsorted = 0;
	bz->H->nonil = 1;
	bz->T->nonil = bx->T->nonil && by->T->nonil;

	BBPkeepref(*zbid = bz->batCacheid);
	return MAL_SUCCEED;
}

str
ZORDbatdecode_int_oid(bat *xbid, bat *ybid, bat *zbid)
{
	BAT *bx, *by,*bz;
	oid *z, *q;
	int *x, *y;

	bz = BATdescriptor(*zbid);
	if ( bz == 0 )
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	
	bx = BATnew(TYPE_void, TYPE_int, BATcount(bz), TRANSIENT);
	by = BATnew(TYPE_void, TYPE_int, BATcount(bz), TRANSIENT);
	if ( bx == 0 || by == 0 ){
		if ( bx ) BBPunfix(bx->batCacheid);
		if ( by ) BBPunfix(by->batCacheid);
		BBPunfix(bz->batCacheid);
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	}
	
	z = (oid *) Tloc(bz, BUNfirst(bz));
	q = (oid *) Tloc(bz, BUNlast(bz));
	x = (int *) Tloc(bx, BUNfirst(bx));
	y = (int *) Tloc(by, BUNfirst(by));

	if ( bz->T->nonil ){
		for ( ; z<q; z++,x++,y++)
			Zdecode_int_oid(x,y,z);
	} else {
		for ( ; z<q; z++,x++,y++)
		if ( *z == oid_nil) {
			*x = int_nil;
			*y = int_nil;
		} else
			Zdecode_int_oid( x,y,z);
	} 

	if (!(bx->batDirty&2)) 
		BATsetaccess(bx, BAT_READ);
	BATsetcount(bx, BATcount(bz));
	BATseqbase(bx, bz->hseqbase);
	bx->hsorted = 1;
	bx->hrevsorted = 0;
	bx->tsorted = 0;
	bx->trevsorted = 0;
	bx->H->nonil = 1;
	bx->T->nonil = bz->T->nonil;

	if (!(by->batDirty&2)) 
		BATsetaccess(by, BAT_READ);
	BATsetcount(by, BATcount(bz));
	BATseqbase(by, bz->hseqbase);
	by->hsorted = 1;
	by->hrevsorted = 0;
	by->tsorted = 0;
	by->trevsorted = 0;
	by->H->nonil = 1;
	by->T->nonil = bz->T->nonil;

	BBPunfix(bz->batCacheid);
	BBPkeepref(*xbid = bx->batCacheid);
	BBPkeepref(*ybid = by->batCacheid);
	return MAL_SUCCEED;
}

str
ZORDbatdecode_int_oid_x(bat *xbid, bat *zbid)
{
	BAT *bx,*bz;
	oid *z, *q;
	int *x;

	bz = BATdescriptor(*zbid);
	if ( bz == 0 )
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	
	bx = BATnew(TYPE_void, TYPE_int, BATcount(bz), TRANSIENT);
	if ( bx == 0 ){
		BBPunfix(bz->batCacheid);
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	}
	
	z = (oid *) Tloc(bz, BUNfirst(bz));
	q = (oid *) Tloc(bz, BUNlast(bz));
	x = (int *) Tloc(bx, BUNfirst(bx));

	if ( bz->T->nonil ){
		for ( ; z<q; z++,x++)
			Zdecode_int_oid_x(x,z);
	} else {
		for ( ; z<q; z++,x++)
		if ( *z == oid_nil) {
			*x = int_nil;
		} else
			Zdecode_int_oid_x(x,z);
	} 

	if (!(bx->batDirty&2)) 
		BATsetaccess(bx, BAT_READ);
	BATsetcount(bx, BATcount(bz));
	BATseqbase(bx, bz->hseqbase);
	bx->hsorted = 1;
	bx->hrevsorted = 0;
	bx->tsorted = 0;
	bx->trevsorted = 0;
	bx->H->nonil = 1;
	bx->T->nonil = bz->T->nonil;

	BBPunfix(bz->batCacheid);
	BBPkeepref(*xbid = bx->batCacheid);
	return MAL_SUCCEED;
}

str
ZORDbatdecode_int_oid_y(bat *ybid, bat *zbid)
{
	BAT *by,*bz;
	oid *z, *q;
	int *y;

	bz = BATdescriptor(*zbid);
	if ( bz == 0 )
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	
	by = BATnew(TYPE_void, TYPE_int, BATcount(bz), TRANSIENT);
	if ( by == 0 ){
		BBPunfix(bz->batCacheid);
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	}
	
	z = (oid *) Tloc(bz, BUNfirst(bz));
	q = (oid *) Tloc(bz, BUNlast(bz));
	y = (int *) Tloc(by, BUNfirst(by));

	if ( bz->T->nonil ){
		for ( ; z<q; z++,y++)
			Zdecode_int_oid_y(y,z);
	} else {
		for ( ; z<q; z++,y++)
		if ( *z == oid_nil) {
			*y = int_nil;
		} else
			Zdecode_int_oid_y(y,z);
	} 

	if (!(by->batDirty&2)) 
		BATsetaccess(by, BAT_READ);
	BATsetcount(by, BATcount(bz));
	BATseqbase(by, bz->hseqbase);
	by->hsorted = 1;
	by->hrevsorted = 0;
	by->tsorted = 0;
	by->trevsorted = 0;
	by->H->nonil = 1;
	by->T->nonil = bz->T->nonil;

	BBPunfix(bz->batCacheid);
	BBPkeepref(*ybid = by->batCacheid);
	return MAL_SUCCEED;
}

str ZORDslice_int(bat *r, int *xb, int *yb, int *xt, int *yt)
{
	BAT *bn;
	int i,j;
	oid zv;

	bn = BATnew(TYPE_void, TYPE_oid, 0, TRANSIENT);
	if( bn == 0)
		throw(OPTIMIZER, "zorder.slice", MAL_MALLOC_FAIL);
	BATseqbase(bn, 0);
	/* use the expensive road, could be improved by bit masking */
	for ( i= *xb; i < *xt; i++)
	{
		for (j= *yb; j < *yt; j++){
			zv= Zencode_int_oid(i,j);
			BUNappend(bn, &zv, FALSE);
		}
	}

	if (!(bn->batDirty&2)) 
		BATsetaccess(bn, BAT_READ);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}
