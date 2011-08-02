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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @-
 * @+ Implementation
 */
#ifndef _HISTOGRAM_H
#define _HISTOGRAM_H
#include <stdarg.h>
#include <gdk.h>
#include <mmath.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define histogram_export extern __declspec(dllimport)
#else
#define histogram_export extern __declspec(dllexport)
#endif
#else
#define histogram_export extern
#endif

typedef struct {
	int bins;
	dbl total;
	ValRecord low, hgh, inc;
	dbl cnt[];
} *Histogram;

#define HSTinc_sht(H,X) H->cnt[(X - H->low.val.shval) / H->inc.val.shval]++
#define HSTinc_bte(H,X) H->cnt[(X - H->low.val.btval) / H->inc.val.btval]++
#define HSTinc_int(H,X) H->cnt[(X - H->low.val.ival) / H->inc.val.ival]++
#define HSTinc_lng(H,X) H->cnt[(X - H->low.val.lval) / H->inc.val.lval]++
#define HSTinc_flt(H,X) H->cnt[(int)((X - H->low.val.fval) / H->inc.val.fval)]++
#define HSTinc_dbl(H,X) H->cnt[(int)((X - H->low.val.dval) / H->inc.val.dval)]++

#define HSTdec_sht(H,X) H->cnt[(X - H->low.val.shval) / H->inc.val.shval]--
#define HSTdec_bte(H,X) H->cnt[(X - H->low.val.btval) / H->inc.val.btval]--
#define HSTdec_int(H,X) H->cnt[(X - H->low.val.ival) / H->inc.val.ival]--
#define HSTdec_lng(H,X) H->cnt[(X - H->low.val.lval) / H->inc.val.lval]--
#define HSTdec_flt(H,X) H->cnt[(int)((X - H->low.val.fval) / H->inc.val.fval)]--
#define HSTdec_dbl(H,X) H->cnt[(int)((X - H->low.val.dval) / H->inc.val.dval)]--

histogram_export Histogram HSTnew(int bins, ValPtr minval, ValPtr maxval);
histogram_export int  HSTincrement(Histogram h, ValPtr val);
histogram_export void HSTdecrement(Histogram h, ValPtr val);
histogram_export void HSTprint(stream *s, Histogram h);
histogram_export void HSTprintf(Histogram h);
histogram_export int HSTgetIndex(Histogram h, ValPtr val);

histogram_export dbl HSTeuclidian(Histogram h1, Histogram h2);
histogram_export int HSTeuclidianWhatIf(Histogram h1, Histogram h2, ValPtr val);
histogram_export int HSTeuclidianWhatIfMove(Histogram h1, Histogram h2, ValPtr val);
histogram_export dbl HSTcityblock(Histogram h1, Histogram h2);
histogram_export dbl HSTchebyshev(Histogram h1, Histogram h2);
histogram_export dbl HSTbinrelative(Histogram h1, Histogram h2);
histogram_export dbl HSTbinrelativeWhatIf(Histogram h1, Histogram h2, ValPtr val, dbl *whatIfDistance);
histogram_export int HSTbinrelativeWhatIfMove(Histogram h1, Histogram h2, ValPtr val, dbl *whatIfDistance);
histogram_export dbl HSTeuclidianNorm(Histogram h1, Histogram h2);
histogram_export dbl HSTeuclidianNormWhatIf(Histogram h1, Histogram h2, ValPtr val, dbl *whatIfDistance);
histogram_export int HSTeuclidianNormWhatIfMove(Histogram h1, Histogram h2, ValPtr val, dbl *whatIfDistance);
histogram_export dbl HSTeuclidianDelta(Histogram h1, Histogram h2, dbl d, int idx, int cnt);
histogram_export dbl HSTcityblockDelta(Histogram h1, Histogram h2, dbl d, int idx, int cnt);
histogram_export dbl HSTchebyshevDelta(Histogram h1, Histogram h2, dbl d, int idx, int cnt);
histogram_export int HSTwhichbin(Histogram h1, Histogram h2);
histogram_export int HSTwhichbinNorm(Histogram h1, Histogram h2);
#endif /* _HISTOGRAM_H */
