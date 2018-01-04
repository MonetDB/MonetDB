/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_CAST_H_
#define _SQL_CAST_H_

#define CAST_INTEGER_2_NUMERIC_2(T1,T2)					\
sql5_export str T1##_dec2_##T2(T2 *res, const int *s1, const T1 *v);	\
sql5_export str bat##T1##_dec2_##T2(bat *res, const int *s1, const bat *v); \
sql5_export str T1##_dec2dec_##T2(T2 *res, const int *S1, const T1 *v, const int *d2, const int *S2); \
sql5_export str bat##T1##_dec2dec_##T2(bat *res, const int *S1, const bat *v, const int *d2, const int *S2); \
sql5_export str T1##_num2dec_##T2(T2 *res, const T1 *v, const int *d2, const int *s2); \
sql5_export str bat##T1##_num2dec_##T2(bat *res, const bat *v, const int *d2, const int *s2);

#ifdef HAVE_HGE
#define CAST_INTEGER_2_NUMERIC_2_hge(T1) CAST_INTEGER_2_NUMERIC_2(T1,hge)
#else
#define CAST_INTEGER_2_NUMERIC_2_hge(T1)
#endif

#define CAST_INTEGER_2_NUMERIC_1(T1)	\
CAST_INTEGER_2_NUMERIC_2(T1,bte)	\
CAST_INTEGER_2_NUMERIC_2(T1,sht)	\
CAST_INTEGER_2_NUMERIC_2(T1,int)	\
CAST_INTEGER_2_NUMERIC_2(T1,lng)	\
CAST_INTEGER_2_NUMERIC_2_hge(T1)	\
CAST_INTEGER_2_NUMERIC_2(T1,flt)	\
CAST_INTEGER_2_NUMERIC_2(T1,dbl)

CAST_INTEGER_2_NUMERIC_1(bte)
CAST_INTEGER_2_NUMERIC_1(sht)
CAST_INTEGER_2_NUMERIC_1(int)
CAST_INTEGER_2_NUMERIC_1(lng)
#ifdef HAVE_HGE
CAST_INTEGER_2_NUMERIC_1(hge)
#endif


#define CAST_FLOATINGPOINT_2_INTEGER_2(T1,T2)				\
sql5_export str T1##_num2dec_##T2(T2 *res, const T1 *v, const int *d2, const int *s2); \
sql5_export str bat##T1##_num2dec_##T2(bat *res, const bat *v, const int *d2, const int *s2);

#ifdef HAVE_HGE
#define CAST_FLOATINGPOINT_2_INTEGER_2_hge(T1) CAST_FLOATINGPOINT_2_INTEGER_2(T1,hge)
#else
#define CAST_FLOATINGPOINT_2_INTEGER_2_hge(T1)
#endif

#define CAST_FLOATINGPOINT_2_INTEGER_1(T1)	\
CAST_FLOATINGPOINT_2_INTEGER_2(T1,bte)		\
CAST_FLOATINGPOINT_2_INTEGER_2(T1,sht)		\
CAST_FLOATINGPOINT_2_INTEGER_2(T1,int)		\
CAST_FLOATINGPOINT_2_INTEGER_2(T1,lng)		\
CAST_FLOATINGPOINT_2_INTEGER_2_hge(T1)

CAST_FLOATINGPOINT_2_INTEGER_1(flt)
CAST_FLOATINGPOINT_2_INTEGER_1(dbl)

#endif	/* _SQL_CAST_H_ */
