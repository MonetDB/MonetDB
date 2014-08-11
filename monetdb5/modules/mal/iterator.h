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

#ifndef _ITERATOR_
#define _ITERATOR_

#include "mal.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define iterator_export extern __declspec(dllimport)
#else
#define iterator_export extern __declspec(dllexport)
#endif
#else
#define iterator_export extern
#endif

iterator_export str ITRnewChunk(lng *res, int *vid, int *bid, lng *granule);
iterator_export str ITRnextChunk(lng *res, int *vid, int *bid, lng *granule);
iterator_export str ITRbunIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iterator_export str ITRbunNext(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

iterator_export str ITRnext_oid(oid *i, oid *step, oid *last);
iterator_export str ITRnext_lng(lng *i, lng *step, lng *last);
#ifdef HAVE_HGE
iterator_export str ITRnext_hge(hge *i, hge *step, hge *last);
#endif
iterator_export str ITRnext_int(int *i, int *step, int *last);
iterator_export str ITRnext_sht(sht *i, sht *step, sht *last);
iterator_export str ITRnext_flt(flt *i, flt *step, flt *last);
iterator_export str ITRnext_dbl(dbl *i, dbl *step, dbl *last);
#endif
