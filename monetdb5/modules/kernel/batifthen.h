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
#ifndef _BATIFTHEN_
#define _BATIFTHEN_
#include "gdk.h"
#include "math.h"
#include "mal_exception.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define batifthen_export extern __declspec(dllimport)
#else
#define batifthen_export extern __declspec(dllexport)
#endif
#else
#define batifthen_export extern
#endif

batifthen_export str CMDifThen(int *ret, int *bid, int *tid);
batifthen_export str CMDifThenElse(int *ret, int *bid, int *tid, int *eid);
batifthen_export str CMDifThenElseCst1(int *ret, int *bid, ptr *val, int *eid);
batifthen_export str CMDifThenElseCst2(int *ret, int *bid, int *tid, ptr *val);

batifthen_export str CMDifThenCst_bit(int *ret, int *bid, ptr *tid);
batifthen_export str CMDifThenCst_bte(int *ret, int *bid, ptr *tid);
batifthen_export str CMDifThenCst_sht(int *ret, int *bid, ptr *tid);
batifthen_export str CMDifThenCst_int(int *ret, int *bid, ptr *tid);
batifthen_export str CMDifThenCst_lng(int *ret, int *bid, ptr *tid);
batifthen_export str CMDifThenCst_oid(int *ret, int *bid, ptr *tid);
batifthen_export str CMDifThenCst_flt(int *ret, int *bid, ptr *tid);
batifthen_export str CMDifThenCst_dbl(int *ret, int *bid, ptr *tid);
batifthen_export str CMDifThenCst_str(int *ret, int *bid, ptr *tid);

batifthen_export str CMDifThenElseCst_bit(int *ret, int *bid, ptr *tid, ptr *eid);
batifthen_export str CMDifThenElseCst_bte(int *ret, int *bid, ptr *tid, ptr *eid);
batifthen_export str CMDifThenElseCst_sht(int *ret, int *bid, ptr *tid, ptr *eid);
batifthen_export str CMDifThenElseCst_int(int *ret, int *bid, ptr *tid, ptr *eid);
batifthen_export str CMDifThenElseCst_lng(int *ret, int *bid, ptr *tid, ptr *eid);
batifthen_export str CMDifThenElseCst_oid(int *ret, int *bid, ptr *tid, ptr *eid);
batifthen_export str CMDifThenElseCst_flt(int *ret, int *bid, ptr *tid, ptr *eid);
batifthen_export str CMDifThenElseCst_dbl(int *ret, int *bid, ptr *tid, ptr *eid);
batifthen_export str CMDifThenElseCst_str(int *ret, int *bid, ptr *tid, ptr *eid);
#endif /* _BATIFTHEN_ */
