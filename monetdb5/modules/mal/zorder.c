/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
	
	bz = COLnew(bx->hseqbase, TYPE_oid, BATcount(bx), TRANSIENT);
	if (bz == 0){
		BBPunfix(bx->batCacheid);
		BBPunfix(by->batCacheid);
		throw(OPTIMIZER, "zorder.encode", MAL_MALLOC_FAIL);
	}
	p = (int *) Tloc(bx, 0);
	q = (int *) Tloc(bx, BUNlast(bx));
	r = (int *) Tloc(by, 0);
	z = (oid *) Tloc(bz, 0);

	if ( bx->tnonil && by->tnonil){
		for ( ; p<q; z++,p++,r++)
			*z = Zencode_int_oid( *p, *r );
	} else
	if ( bx->tnonil ){
		for ( ; p<q; z++,p++,r++)
		if ( *r == int_nil)
			*z = oid_nil;
		else
			*z = Zencode_int_oid( *p, *r );
	} else
	if ( by->tnonil ){
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

	BATsetcount(bz, BATcount(bx));
	bz->tsorted = 0;
	bz->trevsorted = 0;
	bz->tnonil = bx->tnonil && by->tnonil;

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
	
	bx = COLnew(bz->hseqbase, TYPE_int, BATcount(bz), TRANSIENT);
	by = COLnew(bz->hseqbase, TYPE_int, BATcount(bz), TRANSIENT);
	if ( bx == 0 || by == 0 ){
		if ( bx ) BBPunfix(bx->batCacheid);
		if ( by ) BBPunfix(by->batCacheid);
		BBPunfix(bz->batCacheid);
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	}
	
	z = (oid *) Tloc(bz, 0);
	q = (oid *) Tloc(bz, BUNlast(bz));
	x = (int *) Tloc(bx, 0);
	y = (int *) Tloc(by, 0);

	if ( bz->tnonil ){
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

	BATsetcount(bx, BATcount(bz));
	bx->tsorted = 0;
	bx->trevsorted = 0;
	bx->tnonil = bz->tnonil;

	BATsetcount(by, BATcount(bz));
	by->tsorted = 0;
	by->trevsorted = 0;
	by->tnonil = bz->tnonil;

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
	
	bx = COLnew(bz->hseqbase, TYPE_int, BATcount(bz), TRANSIENT);
	if ( bx == 0 ){
		BBPunfix(bz->batCacheid);
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	}
	
	z = (oid *) Tloc(bz, 0);
	q = (oid *) Tloc(bz, BUNlast(bz));
	x = (int *) Tloc(bx, 0);

	if ( bz->tnonil ){
		for ( ; z<q; z++,x++)
			Zdecode_int_oid_x(x,z);
	} else {
		for ( ; z<q; z++,x++)
		if ( *z == oid_nil) {
			*x = int_nil;
		} else
			Zdecode_int_oid_x(x,z);
	} 

	BATsetcount(bx, BATcount(bz));
	bx->tsorted = 0;
	bx->trevsorted = 0;
	bx->tnonil = bz->tnonil;

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
	
	by = COLnew(bz->hseqbase, TYPE_int, BATcount(bz), TRANSIENT);
	if ( by == 0 ){
		BBPunfix(bz->batCacheid);
		throw(OPTIMIZER, "zorder.decode", RUNTIME_OBJECT_MISSING);
	}
	
	z = (oid *) Tloc(bz, 0);
	q = (oid *) Tloc(bz, BUNlast(bz));
	y = (int *) Tloc(by, 0);

	if ( bz->tnonil ){
		for ( ; z<q; z++,y++)
			Zdecode_int_oid_y(y,z);
	} else {
		for ( ; z<q; z++,y++)
		if ( *z == oid_nil) {
			*y = int_nil;
		} else
			Zdecode_int_oid_y(y,z);
	} 

	BATsetcount(by, BATcount(bz));
	by->tsorted = 0;
	by->trevsorted = 0;
	by->tnonil = bz->tnonil;

	BBPunfix(bz->batCacheid);
	BBPkeepref(*ybid = by->batCacheid);
	return MAL_SUCCEED;
}

str ZORDslice_int(bat *r, int *xb, int *yb, int *xt, int *yt)
{
	BAT *bn;
	int i,j;
	oid zv;

	bn = COLnew(0, TYPE_oid, 0, TRANSIENT);
	if( bn == 0)
		throw(OPTIMIZER, "zorder.slice", MAL_MALLOC_FAIL);
	/* use the expensive road, could be improved by bit masking */
	for ( i= *xb; i < *xt; i++)
	{
		for (j= *yb; j < *yt; j++){
			zv= Zencode_int_oid(i,j);
			BUNappend(bn, &zv, FALSE);
		}
	}

	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}
