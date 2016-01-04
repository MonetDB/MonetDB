/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

iterator_export str ITRnewChunk(lng *res, bat *vid, bat *bid, lng *granule);
iterator_export str ITRnextChunk(lng *res, bat *vid, bat *bid, lng *granule);
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
