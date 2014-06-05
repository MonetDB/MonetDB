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

#ifndef _PQUEUE_
#define _PQUEUE_
#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define pqueue_export extern __declspec(dllimport)
#else
#define pqueue_export extern __declspec(dllexport)
#endif
#else
#define pqueue_export extern
#endif

pqueue_export str PQinit(int *ret, int *bid, wrd *maxsize);

pqueue_export str PQenqueue_btemin(int *ret, int *bid, oid *idx, bte *el);
pqueue_export str PQtopreplace_btemin(int *ret, int *bid, oid *idx, bte *el);
pqueue_export str PQmovedowntop_btemin(int *ret, int *bid);
pqueue_export str PQdequeue_btemin(int *ret, int *bid);

pqueue_export str PQtopn_btemin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_btemin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_btemin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_btemin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_btemax(int *ret, int *bid, oid *idx, bte *el);
pqueue_export str PQtopreplace_btemax(int *ret, int *bid, oid *idx, bte *el);
pqueue_export str PQmovedowntop_btemax(int *ret, int *bid);
pqueue_export str PQdequeue_btemax(int *ret, int *bid);

pqueue_export str PQtopn_btemax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_btemax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_btemax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_btemax(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_shtmin(int *ret, int *bid, oid *idx, sht *el);
pqueue_export str PQtopreplace_shtmin(int *ret, int *bid, oid *idx, sht *el);
pqueue_export str PQmovedowntop_shtmin(int *ret, int *bid);
pqueue_export str PQdequeue_shtmin(int *ret, int *bid);

pqueue_export str PQtopn_shtmin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_shtmin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_shtmin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_shtmin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_shtmax(int *ret, int *bid, oid *idx, sht *el);
pqueue_export str PQtopreplace_shtmax(int *ret, int *bid, oid *idx, sht *el);
pqueue_export str PQmovedowntop_shtmax(int *ret, int *bid);
pqueue_export str PQdequeue_shtmax(int *ret, int *bid);

pqueue_export str PQtopn_shtmax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_shtmax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_shtmax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_shtmax(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_intmin(int *ret, int *bid, oid *idx, int *el);
pqueue_export str PQtopreplace_intmin(int *ret, int *bid, oid *idx, int *el);
pqueue_export str PQmovedowntop_intmin(int *ret, int *bid);
pqueue_export str PQdequeue_intmin(int *ret, int *bid);

pqueue_export str PQtopn_intmin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_intmin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_intmin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_intmin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_intmax(int *ret, int *bid, oid *idx, int *el);
pqueue_export str PQtopreplace_intmax(int *ret, int *bid, oid *idx, int *el);
pqueue_export str PQmovedowntop_intmax(int *ret, int *bid);
pqueue_export str PQdequeue_intmax(int *ret, int *bid);

pqueue_export str PQtopn_intmax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_intmax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_intmax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_intmax(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_oidmin(int *ret, int *bid, oid *idx, oid *el);
pqueue_export str PQtopreplace_oidmin(int *ret, int *bid, oid *idx, oid *el);
pqueue_export str PQmovedowntop_oidmin(int *ret, int *bid);
pqueue_export str PQdequeue_oidmin(int *ret, int *bid);

pqueue_export str PQtopn_oidmin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_oidmin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_oidmin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_oidmin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_oidmax(int *ret, int *bid, oid *idx, oid *el);
pqueue_export str PQtopreplace_oidmax(int *ret, int *bid, oid *idx, oid *el);
pqueue_export str PQmovedowntop_oidmax(int *ret, int *bid);
pqueue_export str PQdequeue_oidmax(int *ret, int *bid);

pqueue_export str PQtopn_oidmax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_oidmax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_oidmax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_oidmax(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_wrdmin(int *ret, int *bid, oid *idx, wrd *el);
pqueue_export str PQtopreplace_wrdmin(int *ret, int *bid, oid *idx, wrd *el);
pqueue_export str PQmovedowntop_wrdmin(int *ret, int *bid);
pqueue_export str PQdequeue_wrdmin(int *ret, int *bid);

pqueue_export str PQtopn_wrdmin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_wrdmin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_wrdmin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_wrdmin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_wrdmax(int *ret, int *bid, oid *idx, wrd *el);
pqueue_export str PQtopreplace_wrdmax(int *ret, int *bid, oid *idx, wrd *el);
pqueue_export str PQmovedowntop_wrdmax(int *ret, int *bid);
pqueue_export str PQdequeue_wrdmax(int *ret, int *bid);

pqueue_export str PQtopn_wrdmax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_wrdmax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_wrdmax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_wrdmax(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_lngmin(int *ret, int *bid, oid *idx, lng *el);
pqueue_export str PQtopreplace_lngmin(int *ret, int *bid, oid *idx, lng *el);
pqueue_export str PQmovedowntop_lngmin(int *ret, int *bid);
pqueue_export str PQdequeue_lngmin(int *ret, int *bid);

pqueue_export str PQtopn_lngmin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_lngmin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_lngmin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_lngmin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_lngmax(int *ret, int *bid, oid *idx, lng *el);
pqueue_export str PQtopreplace_lngmax(int *ret, int *bid, oid *idx, lng *el);
pqueue_export str PQmovedowntop_lngmax(int *ret, int *bid);
pqueue_export str PQdequeue_lngmax(int *ret, int *bid);

pqueue_export str PQtopn_lngmax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_lngmax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_lngmax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_lngmax(int *ret, int *aid, int *bid, wrd *N);

#ifdef HAVE_HGE
pqueue_export str PQenqueue_hgemin(int *ret, int *bid, oid *idx, hge *el);
pqueue_export str PQtopreplace_hgemin(int *ret, int *bid, oid *idx, hge *el);
pqueue_export str PQmovedowntop_hgemin(int *ret, int *bid);
pqueue_export str PQdequeue_hgemin(int *ret, int *bid);

pqueue_export str PQtopn_hgemin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_hgemin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_hgemin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_hgemin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_hgemax(int *ret, int *bid, oid *idx, hge *el);
pqueue_export str PQtopreplace_hgemax(int *ret, int *bid, oid *idx, hge *el);
pqueue_export str PQmovedowntop_hgemax(int *ret, int *bid);
pqueue_export str PQdequeue_hgemax(int *ret, int *bid);

pqueue_export str PQtopn_hgemax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_hgemax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_hgemax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_hgemax(int *ret, int *aid, int *bid, wrd *N);
#endif

pqueue_export str PQenqueue_fltmin(int *ret, int *bid, oid *idx, flt *el);
pqueue_export str PQtopreplace_fltmin(int *ret, int *bid, oid *idx, flt *el);
pqueue_export str PQmovedowntop_fltmin(int *ret, int *bid);
pqueue_export str PQdequeue_fltmin(int *ret, int *bid);

pqueue_export str PQtopn_fltmin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_fltmin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_fltmin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_fltmin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_fltmax(int *ret, int *bid, oid *idx, flt *el);
pqueue_export str PQtopreplace_fltmax(int *ret, int *bid, oid *idx, flt *el);
pqueue_export str PQmovedowntop_fltmax(int *ret, int *bid);
pqueue_export str PQdequeue_fltmax(int *ret, int *bid);

pqueue_export str PQtopn_fltmax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_fltmax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_fltmax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_fltmax(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_dblmin(int *ret, int *bid, oid *idx, dbl *el);
pqueue_export str PQtopreplace_dblmin(int *ret, int *bid, oid *idx, dbl *el);
pqueue_export str PQmovedowntop_dblmin(int *ret, int *bid);
pqueue_export str PQdequeue_dblmin(int *ret, int *bid);

pqueue_export str PQtopn_dblmin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_dblmin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_dblmin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_dblmin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_dblmax(int *ret, int *bid, oid *idx, dbl *el);
pqueue_export str PQtopreplace_dblmax(int *ret, int *bid, oid *idx, dbl *el);
pqueue_export str PQmovedowntop_dblmax(int *ret, int *bid);
pqueue_export str PQdequeue_dblmax(int *ret, int *bid);

pqueue_export str PQtopn_dblmax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_dblmax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_dblmax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_dblmax(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_anymax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
pqueue_export str PQtopreplace_anymax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

pqueue_export str PQmovedowntop_anymax(int *ret, int *bid);
pqueue_export str PQdequeue_anymax(int *ret, int *bid);

pqueue_export str PQtopn_anymax(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_anymax(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_anymax(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_anymax(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQenqueue_anymin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
pqueue_export str PQtopreplace_anymin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

pqueue_export str PQmovedowntop_anymin(int *ret, int *bid);
pqueue_export str PQdequeue_anymin(int *ret, int *bid);

pqueue_export str PQtopn_anymin(int *ret, int *bid, wrd *N);
pqueue_export str PQutopn_anymin(int *ret, int *bid, wrd *N);
pqueue_export str PQtopn2_anymin(int *ret, int *aid, int *bid, wrd *N);
pqueue_export str PQutopn2_anymin(int *ret, int *aid, int *bid, wrd *N);

pqueue_export str PQtopn_minmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pqueue_export str PQtopn2_minmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pqueue_export str PQtopn3_minmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _PQUEUE */
