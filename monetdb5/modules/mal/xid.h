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
#ifndef _XIDLIST_H
#define _XIDLIST_H
#include "monetdb_config.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define xid_export extern __declspec(dllimport)
#else
#define xid_export extern __declspec(dllexport)
#endif
#else
#define xid_export extern
#endif

#define XIDBASE  0
#define XIDSET   1
#define XIDRANGE 2
#define XIDPOINT 3
#define XIDMASK  7	/* we keep some reserve for runlength compression */

typedef struct XIDCOLUMN{
        long tag:3, value:61;
} *XIDcolumn;

xid_export str XIDcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
xid_export str XIDdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
xid_export str  XIDdump(int *ret, int *bid);
#endif /* _XIDLIST_H */
