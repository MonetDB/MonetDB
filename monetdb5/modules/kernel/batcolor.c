/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * M.L. Kersten
 * Color multiplexes
 * [TODO: property propagations and general testing]
 * The collection of routines provided here are map operations
 * for the color string primitives.
 *
 * In line with the batcalc module, we assume that if two bat operands
 * are provided that they are aligned.
 */

#include "monetdb_config.h"
#include "batcolor.h"

#define BATwalk(NAME,FUNC,TYPE1,ISNIL,TYPE2)							\
str CLRbat##NAME(bat *ret, const bat *l)								\
{																		\
	BATiter bi;															\
	BAT *bn, *b;														\
	BUN p,q;															\
	const TYPE1 *x;														\
	TYPE2 y, *yp = &y;													\
																		\
	if( (b= BATdescriptor(*l)) == NULL )								\
		throw(MAL, "batcolor." #NAME, RUNTIME_OBJECT_MISSING);			\
	bn= COLnew(b->hseqbase,getAtomIndex(#TYPE2,-1,TYPE_int),BATcount(b), TRANSIENT); \
	if( bn == NULL){													\
		BBPunfix(b->batCacheid);										\
		throw(MAL, "batcolor." #NAME, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
	}																	\
	bn->tsorted=0;														\
	bn->trevsorted=0;													\
	bn->tnil = 0;														\
	bn->tnonil = 1;														\
																		\
	bi = bat_iterator(b);												\
																		\
	BATloop(b, p, q) {													\
		x= (const TYPE1 *) BUNtail(bi,p);								\
		if (x== 0 || ISNIL(*x)) {										\
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

BATwalk(Color,CLRcolor,char *,GDK_STRNIL,color)
BATwalk(Str,CLRstr,color,is_color_nil,str)

BATwalk(Red,CLRred,color,is_color_nil,int)
BATwalk(Green,CLRgreen,color,is_color_nil,int)
BATwalk(Blue,CLRblue,color,is_color_nil,int)

BATwalk(Hue,CLRhue,color,is_color_nil,flt)
BATwalk(Saturation,CLRsaturation,color,is_color_nil,flt)
BATwalk(Value,CLRvalue,color,is_color_nil,flt)

BATwalk(HueInt,CLRhueInt,color,is_color_nil,int)
BATwalk(SaturationInt,CLRsaturationInt,color,is_color_nil,int)
BATwalk(ValueInt,CLRvalueInt,color,is_color_nil,int)

BATwalk(Luminance,CLRluminance,color,is_color_nil,int)
BATwalk(Cr,CLRcr,color,is_color_nil,int)
BATwalk(Cb,CLRcb,color,is_color_nil,int)

#define BATwalk3(NAME,FUNC,TYPE)										\
str CLRbat##NAME(bat *ret, const bat *l, const bat *bid2, const bat *bid3) \
{																		\
	BATiter bi, b2i, b3i;												\
	BAT *bn, *b2,*b3, *b;												\
	BUN p,q;															\
	const TYPE *x, *x2, *x3;											\
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
		throw(MAL, "batcolor." #NAME, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
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
		x= (const TYPE *) BUNtail(bi,p);								\
		x2= (const TYPE *) BUNtail(b2i,p);								\
		x3= (const TYPE *) BUNtail(b3i,p);								\
		if (x== 0 || is_##TYPE##_nil(*x) ||								\
			x2== 0 || is_##TYPE##_nil(*x2) ||							\
			x3== 0 || is_##TYPE##_nil(*x3)) {							\
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
