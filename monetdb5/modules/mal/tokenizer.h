/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _TKNZR_H
#define _TKNZR_H
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"

mal_export str TKNZRopen             (void *r, str *name);
mal_export str TKNZRclose            (void *r);
mal_export str TKNZRappend          (oid *pos, str *tuple);
mal_export str TKNZRlocate           (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str TKNZRtakeOid          (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str TKNZRdepositFile      (void *r, str *fnme);
mal_export str TKNZRgetLevel         (bat *r, int *level);
mal_export str TKNZRgetIndex         (bat *r);
mal_export str TKNZRgetCount         (bat *r);
mal_export str TKNZRgetCardinality   (bat *r);
mal_export str takeOid	 	   (oid id, str *val); 			

#endif /* _TKNZR_H */
