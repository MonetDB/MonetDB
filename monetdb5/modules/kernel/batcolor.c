/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * M.L. Kersten
 * Color multiplexes
 * [TODO: property propagations and general testing]
 * The collection of routines provided here are map operations
 * for the color string primitives.
 *
 * In line with the batcalc module, we assume that
 * if two bat operands are provided that they are already
 * aligned on the head. Moreover, the head of the BATs
 * are limited to :oid, which can be cheaply realized using
 * the GRPsplit operation.
 */

#include "monetdb_config.h"
#include "batcolor.h"

#define BATwalk(NAME,FUNC,TYPE1,TYPE2)									\
str CLRbat##NAME(bat *ret, const bat *l)								\
{																		\
	BATiter bi;															\
	BAT *bn, *b;														\
	BUN p,q;															\
	TYPE1 *x;															\
	TYPE2 y, *yp = &y;													\
																		\
	if( (b= BATdescriptor(*l)) == NULL )								\
		throw(MAL, "batcolor." #NAME, RUNTIME_OBJECT_MISSING);			\
	bn= COLnew(b->hseqbase,getAtomIndex(#TYPE2,-1,TYPE_int),BATcount(b), TRANSIENT); \
	if( bn == NULL){													\
		BBPunfix(b->batCacheid);										\
		throw(MAL, "batcolor." #NAME, MAL_MALLOC_FAIL);					\
	}																	\
	bn->tsorted=0;														\
	bn->trevsorted=0;													\
	bn->tnil = 0;														\
	bn->tnonil = 1;														\
																		\
	bi = bat_iterator(b);												\
																		\
	BATloop(b, p, q) {													\
		x= (TYPE1 *) BUNtail(bi,p);										\
		if (x== 0 || *x == TYPE1##_nil) {								\
			y = (TYPE2) TYPE2##_nil;									\
			bn->tnonil = 0;												\
			bn->tnil = 1;												\
		} else															\
			FUNC(yp,x);													\
		bunfastapp(bn, yp);												\
	}																	\
	*ret = bn->batCacheid;												\
	BBPkeepref(*ret);													\
	BBPunfix(b->batCacheid);											\
	return MAL_SUCCEED;													\
bunins_failed:															\
	BBPunfix(b->batCacheid);											\
	BBPunfix(bn->batCacheid);											\
	throw(MAL, "batcolor." #NAME, OPERATION_FAILED " During bulk operation"); \
}

BATwalk(Color,CLRcolor,str,color)
BATwalk(Str,CLRstr,color,str)

BATwalk(Red,CLRred,color,int)
BATwalk(Green,CLRgreen,color,int)
BATwalk(Blue,CLRblue,color,int)

BATwalk(Hue,CLRhue,color,flt)
BATwalk(Saturation,CLRsaturation,color,flt)
BATwalk(Value,CLRvalue,color,flt)

BATwalk(HueInt,CLRhueInt,color,int)
BATwalk(SaturationInt,CLRsaturationInt,color,int)
BATwalk(ValueInt,CLRvalueInt,color,int)

BATwalk(Luminance,CLRluminance,color,int)
BATwalk(Cr,CLRcr,color,int)
BATwalk(Cb,CLRcb,color,int)

#define BATwalk3(NAME,FUNC,TYPE)										\
str CLRbat##NAME(bat *ret, const bat *l, const bat *bid2, const bat *bid3) \
{																		\
	BATiter bi, b2i, b3i;												\
	BAT *bn, *b2,*b3, *b;												\
	BUN p,q;															\
	TYPE *x, *x2, *x3;													\
	color y, *yp = &y;													\
																		\
	b= BATdescriptor(*l);												\
	b2= BATdescriptor(*bid2);											\
	b3= BATdescriptor(*bid3);											\
	if (b == NULL || b2 == NULL || b3 == NULL) {						\
		if (b)															\
			BBPunfix(b->batCacheid);									\
		if (b2)															\
			BBPunfix(b2->batCacheid);									\
		if (b3)															\
			BBPunfix(b3->batCacheid);									\
		throw(MAL, "batcolor." #NAME, RUNTIME_OBJECT_MISSING);			\
	}																	\
	bn= COLnew(b->hseqbase,getAtomIndex("color",5,TYPE_int),BATcount(b), TRANSIENT); \
	if( bn == NULL){													\
		BBPunfix(b->batCacheid);										\
		BBPunfix(b2->batCacheid);										\
		BBPunfix(b3->batCacheid);										\
		throw(MAL, "batcolor." #NAME, MAL_MALLOC_FAIL);					\
	}																	\
	bn->tsorted=0;														\
	bn->trevsorted=0;													\
	bn->tnil = 0;														\
	bn->tnonil = 1;														\
																		\
	bi = bat_iterator(b);												\
	b2i = bat_iterator(b2);												\
	b3i = bat_iterator(b3);												\
																		\
	BATloop(b, p, q) {													\
		x= (TYPE *) BUNtail(bi,p);										\
		x2= (TYPE *) BUNtail(b2i,p);									\
		x3= (TYPE *) BUNtail(b3i,p);									\
		if (x== 0 || *x == TYPE##_nil ||								\
			x2== 0 || *x2 == TYPE##_nil ||								\
			x3== 0 || *x3 == TYPE##_nil) {								\
			y = color_nil;												\
			bn->tnonil = 0;												\
			bn->tnil = 1;												\
		} else															\
			FUNC(yp,x,x2,x3);											\
		bunfastapp(bn, yp);												\
	}																	\
	*ret = bn->batCacheid;												\
	BBPkeepref(*ret);													\
	BBPunfix(b->batCacheid);											\
	BBPunfix(b2->batCacheid);											\
	BBPunfix(b3->batCacheid);											\
	return MAL_SUCCEED;													\
bunins_failed:															\
	BBPunfix(b->batCacheid);											\
	BBPunfix(b2->batCacheid);											\
	BBPunfix(b3->batCacheid);											\
	BBPunfix(bn->batCacheid);											\
	throw(MAL, "batcolor." #NAME, OPERATION_FAILED " During bulk operation"); \
}

BATwalk3(Hsv,CLRhsv,flt)
BATwalk3(Rgb,CLRrgb,int)
BATwalk3(ycc,CLRycc,int)
