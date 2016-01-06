/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _TKNZR_H
#define _TKNZR_H
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define tokenizer_export extern __declspec(dllimport)
#else
#define tokenizer_export extern __declspec(dllexport)
#endif
#else
#define tokenizer_export extern
#endif

tokenizer_export str TKNZRopen             (void *r, str *name);
tokenizer_export str TKNZRclose            (void *r);
tokenizer_export str TKNZRappend          (oid *pos, str *tuple);
tokenizer_export str TKNZRlocate           (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tokenizer_export str TKNZRtakeOid          (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tokenizer_export str TKNZRdepositFile      (void *r, str *fnme);
tokenizer_export str TKNZRgetLevel         (bat *r, int *level);
tokenizer_export str TKNZRgetIndex         (bat *r);
tokenizer_export str TKNZRgetCount         (bat *r);
tokenizer_export str TKNZRgetCardinality   (bat *r);
tokenizer_export str takeOid	 	   (oid id, str *val); 			

#endif /* _TKNZR_H */
