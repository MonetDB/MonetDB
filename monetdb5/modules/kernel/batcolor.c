/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "gdk.h"
#include <string.h>
#include "mal.h"
#include "color.h"
#include "mal_exception.h"

#include "mel.h"

#define BATwalk(NAME,FUNC,TYPE1,ISNIL,TYPE2,TPE,APP)					\
static str CLRbat##NAME(bat *ret, const bat *l)							\
{																		\
	BATiter bi;															\
	BAT *bn, *b;														\
	BUN p,q;															\
	const TYPE1 *x;														\
	TYPE2 y;															\
	char *msg = MAL_SUCCEED;											\
																		\
	if( (b= BATdescriptor(*l)) == NULL )								\
		throw(MAL, "batcolor." #NAME, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING); \
	bn= COLnew(b->hseqbase,TPE,BATcount(b), TRANSIENT);					\
	if( bn == NULL){													\
		BBPunfix(b->batCacheid);										\
		throw(MAL, "batcolor." #NAME, SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
	}																	\
	bn->tsorted=false;													\
	bn->trevsorted=false;												\
	bn->tnil = false;													\
	bn->tnonil = true;													\
																		\
	bi = bat_iterator(b);												\
																		\
	BATloop(b, p, q) {													\
		x= (const TYPE1 *) BUNtail(bi,p);								\
		if (x== 0 || ISNIL(*x)) {										\
			y = (TYPE2) TYPE2##_nil;									\
			bn->tnonil = false;											\
			bn->tnil = true;											\
		} else if ((msg = FUNC(&y,x)) != MAL_SUCCEED)					\
			goto bunins_failed;											\
		if ((APP) != GDK_SUCCEED)										\
			goto bunins_failed;											\
	}																	\
	bn->theap->dirty |= BATcount(bn) > 0;								\
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

BATwalk(Color,CLRcolor,char *,strNil,color,getAtomIndex("color",5,TYPE_int),bunfastappTYPE(color, bn, &y))
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
static str CLRbat##NAME(bat *ret, const bat *l, const bat *bid2, const bat *bid3) \
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
		throw(MAL, "batcolor." #NAME, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING); \
	}																	\
	bn= COLnew(b->hseqbase,getAtomIndex("color",5,TYPE_int),BATcount(b), TRANSIENT); \
	if( bn == NULL){													\
		BBPunfix(b->batCacheid);										\
		BBPunfix(b2->batCacheid);										\
		BBPunfix(b3->batCacheid);										\
		throw(MAL, "batcolor." #NAME, SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
	}																	\
	bn->tsorted=false;													\
	bn->trevsorted=false;												\
	bn->tnil = false;													\
	bn->tnonil = true;													\
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
			bn->tnonil = false;											\
			bn->tnil = true;											\
		} else if ((msg = FUNC(&y,x,x2,x3)) != MAL_SUCCEED)				\
			goto bunins_failed;											\
		if (bunfastappTYPE(color, bn, &y) != GDK_SUCCEED)				\
			goto bunins_failed;											\
	}																	\
	bn->theap->dirty |= BATcount(bn) > 0;								\
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

#include "mel.h"
mel_func batcolor_init_funcs[] = {
 command("batcolor", "str", CLRbatStr, false, "Identity mapping for string bats", args(1,2, batarg("",str),batarg("b",color))),
 command("batcolor", "color", CLRbatColor, false, "Converts string to color", args(1,2, batarg("",color),batarg("s",str))),
 command("batcolor", "rgb", CLRbatRgb, false, "Converts an RGB triplets to a color atom", args(1,4, batarg("",color),batarg("r",int),batarg("g",int),batarg("b",int))),
 command("batcolor", "red", CLRbatRed, false, "Extracts red component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 command("batcolor", "green", CLRbatGreen, false, "Extracts green component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 command("batcolor", "blue", CLRbatBlue, false, "Extracts blue component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 command("batcolor", "hue", CLRbatHueInt, false, "Extracts hue component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 command("batcolor", "saturation", CLRbatSaturationInt, false, "Extracts saturation component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 command("batcolor", "value", CLRbatValueInt, false, "Extracts value component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 command("batcolor", "hsv", CLRbatHsv, false, "Converts an HSV triplets to a color atom", args(1,4, batarg("",color),batarg("h",flt),batarg("s",flt),batarg("v",flt))),
 command("batcolor", "hue", CLRbatHue, false, "Extracts hue component from a color atom", args(1,2, batarg("",flt),batarg("c",color))),
 command("batcolor", "saturation", CLRbatSaturation, false, "Extracts saturation component from a color atom", args(1,2, batarg("",flt),batarg("c",color))),
 command("batcolor", "value", CLRbatValue, false, "Extracts value component from a color atom", args(1,2, batarg("",flt),batarg("c",color))),
 command("batcolor", "ycc", CLRbatycc, false, "Converts an YCC triplets to a color atom", args(1,4, batarg("",color),batarg("y",flt),batarg("cr",flt),batarg("cb",flt))),
 command("batcolor", "luminance", CLRbatLuminance, false, "Extracts Y(luminance) component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 command("batcolor", "cr", CLRbatCr, false, "Extracts Cr(red color) component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 command("batcolor", "cb", CLRbatCb, false, "Extracts Cb(blue color) component from a color atom", args(1,2, batarg("",int),batarg("c",color))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batcolor_mal)
{ mal_module("batcolor", NULL, batcolor_init_funcs); }
