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

#define prepareOperand(X,Y,Z)								\
	if( (X= BATdescriptor(*Y)) == NULL )					\
		throw(MAL, "batstr." Z, RUNTIME_OBJECT_MISSING);
#define prepareOperand2(X,Y,A,B,Z)							\
	if( (X= BATdescriptor(*Y)) == NULL )					\
		throw(MAL, "batstr." Z, RUNTIME_OBJECT_MISSING);	\
	if( (A= BATdescriptor(*B)) == NULL ){					\
		BBPreleaseref(X->batCacheid);						\
		throw(MAL, "batstr."Z, RUNTIME_OBJECT_MISSING);		\
	}
#define prepareResult(X,Y,T,Z)						\
	X= BATnew(Y->htype,T,BATcount(Y));				\
	if( X == NULL){									\
		BBPreleaseref(Y->batCacheid);				\
		throw(MAL, "batstr." Z, MAL_MALLOC_FAIL);	\
	}												\
	if( Y->htype== TYPE_void)						\
		BATseqbase(X, Y->hseqbase);					\
	X->hsorted=Y->hsorted;							\
	X->hrevsorted=Y->hrevsorted;					\
	X->tsorted=0;									\
	X->trevsorted=0;
#define finalizeResult(X,Y,Z)									\
	if (!((Y)->batDirty&2)) (Y) = BATsetaccess((Y), BAT_READ);	\
	*X = (Y)->batCacheid;										\
	BBPkeepref(*(X));											\
	BBPreleaseref(Z->batCacheid);

#define BATwalk(X1,X2,X3,X4)											\
str CLRbat##X1(int *ret, int *l)										\
{																		\
	BATiter bi;															\
	BAT *bn, *b;														\
	BUN p,q;															\
	X3 *x;																\
	X4 y, *yp = &y;														\
																		\
	prepareOperand(b,l,#X1);											\
	prepareResult(bn,b,getTypeIndex(#X4,-1,TYPE_int),#X1);				\
																		\
	bi = bat_iterator(b);												\
																		\
	BATloop(b, p, q) {													\
		ptr h = BUNhead(bi,p);											\
		x= (X3 *) BUNtail(bi,p);										\
		if (x== 0 || *x == X3##_nil) {									\
			y = (X4)X4##_nil;											\
			bn->T->nonil = 0;											\
		} else															\
			X2(yp,x);													\
		bunfastins(bn, h, yp);											\
	}																	\
	bn->H->nonil = b->H->nonil;											\
	finalizeResult(ret,bn,b);											\
	return MAL_SUCCEED;													\
bunins_failed:															\
	BBPreleaseref(b->batCacheid);										\
	BBPreleaseref(bn->batCacheid);										\
	throw(MAL, "batstr.==", OPERATION_FAILED " During bulk operation");	\
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

#define BATwalk3(X1,X2,X3,X4)											\
str CLRbat##X1(int *ret, int *l, int *bid2, int *bid3)					\
{																		\
	BATiter bi, b2i, b3i;												\
	BAT *bn, *b2,*b3, *b;												\
	BUN p,q,p2,p3;														\
	X3 *x, *x2, *x3;													\
	X4 y, *yp = &y;														\
																		\
	prepareOperand(b,l,#X1);											\
	b2= BATdescriptor(*bid2);											\
	if(b2== NULL)														\
		throw(MAL, "batcolor." #X1,RUNTIME_OBJECT_MISSING);				\
	b3= BATdescriptor(*bid3);											\
	if(b3== NULL)														\
		throw(MAL, "batcolor." #X1,RUNTIME_OBJECT_MISSING);				\
	prepareResult(bn,b,getTypeIndex(#X4,-1,TYPE_int),#X1);				\
																		\
	bi = bat_iterator(b);												\
	b2i = bat_iterator(b2);												\
	b3i = bat_iterator(b3);												\
																		\
	p2= BUNfirst(b2);													\
	p3= BUNfirst(b3);													\
	BATloop(b, p, q) {													\
		ptr h = BUNhead(bi,p);											\
		x= (X3 *) BUNtail(bi,p);										\
		x2= (X3 *) BUNtail(b2i,p);										\
		x3= (X3 *) BUNtail(b3i,p);										\
		if (x== 0 || *x == X3##_nil ||									\
		   x2== 0 || *x2 == X3##_nil ||									\
		   x3== 0 || *x3 == X3##_nil) {									\
			y = X4##_nil;												\
			bn->T->nonil = 0;											\
		} else															\
			X2(yp,x,x2,x3);												\
		bunfastins(bn, h, yp);											\
		p2++;															\
		p3++;															\
	}																	\
	bn->H->nonil = b->H->nonil;											\
	finalizeResult(ret,bn,b);											\
	return MAL_SUCCEED;													\
bunins_failed:															\
	BBPreleaseref(b->batCacheid);										\
	BBPreleaseref(bn->batCacheid);										\
	throw(MAL, "batstr.==", OPERATION_FAILED " During bulk operation");	\
}

BATwalk3(Hsv,CLRhsv,flt,color)
BATwalk3(Rgb,CLRrgb,int,color)
BATwalk3(ycc,CLRycc,int,color)
