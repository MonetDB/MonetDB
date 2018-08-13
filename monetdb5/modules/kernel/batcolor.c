/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

#define BATwalk(NAME,FUNC,TYPE1,ISNIL,TYPE2,TPE,APP)					\
str CLRbat##NAME(bat *ret, const bat *l)								\
{																		\
	BATiter bi;															\
	BAT *bn, *b;														\
	BUN p,q;															\
	const TYPE1 *x;														\
	TYPE2 y;															\
	char *msg = MAL_SUCCEED;											\
																		\
	if( (b= BATdescriptor(*l)) == NULL )								\
		throw(MAL, "batcolor." #NAME, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	bn= COLnew(b->hseqbase,TPE,BATcount(b), TRANSIENT);					\
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
		} else if ((msg = FUNC(&y,x)) != MAL_SUCCEED)					\
			goto bunins_failed;											\
		APP;															\
	}																	\
	bn->theap.dirty |= BATcount(bn) > 0;								\
	*ret = bn->batCacheid;												\
	BBPkeepref(*ret);													\
	BBPunfix(b->batCacheid);											\
	return MAL_SUCCEED;													\
bunins_failed:															\
	BBPunfix(b->batCacheid);											\
	BBPunfix(bn->batCacheid);											\
	if (msg)															\
		return msg;														\
	throw(MAL, "batcolor." #NAME, OPERATION_FAILED " During bulk operation"); \
}

BATwalk(Color,CLRcolor,char *,GDK_STRNIL,color,getAtomIndex("color",5,TYPE_int),bunfastappTYPE(color, bn, &y))
BATwalk(Str,CLRstr,color,is_color_nil,str,TYPE_str,bunfastappVAR(bn, &y))

BATwalk(Red,CLRred,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))
BATwalk(Green,CLRgreen,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))
BATwalk(Blue,CLRblue,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))

BATwalk(Hue,CLRhue,color,is_color_nil,flt,TYPE_flt,bunfastappTYPE(flt, bn, &y))
BATwalk(Saturation,CLRsaturation,color,is_color_nil,flt,TYPE_flt,bunfastappTYPE(flt, bn, &y))
BATwalk(Value,CLRvalue,color,is_color_nil,flt,TYPE_flt,bunfastappTYPE(flt, bn, &y))

BATwalk(HueInt,CLRhueInt,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))
BATwalk(SaturationInt,CLRsaturationInt,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))
BATwalk(ValueInt,CLRvalueInt,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))

BATwalk(Luminance,CLRluminance,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))
BATwalk(Cr,CLRcr,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))
BATwalk(Cb,CLRcb,color,is_color_nil,int,TYPE_int,bunfastappTYPE(int, bn, &y))

#define BATwalk3(NAME,FUNC,TYPE)										\
str CLRbat##NAME(bat *ret, const bat *l, const bat *bid2, const bat *bid3) \
{																		\
	BATiter bi, b2i, b3i;												\
	BAT *bn, *b2,*b3, *b;												\
	BUN p,q;															\
	const TYPE *x, *x2, *x3;											\
	color y;															\
	char *msg = MAL_SUCCEED;											\
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
		throw(MAL, "batcolor." #NAME, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
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
		} else if ((msg = FUNC(&y,x,x2,x3)) != MAL_SUCCEED)				\
			goto bunins_failed;											\
		bunfastappTYPE(color, bn, &y);									\
	}																	\
	bn->theap.dirty |= BATcount(bn) > 0;								\
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
	if (msg)															\
		return msg;														\
	throw(MAL, "batcolor." #NAME, OPERATION_FAILED " During bulk operation"); \
}

BATwalk3(Hsv,CLRhsv,flt)
BATwalk3(Rgb,CLRrgb,int)
BATwalk3(ycc,CLRycc,int)
