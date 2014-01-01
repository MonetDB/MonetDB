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
#ifndef _CLUSTER_H
#define _CLUSTER_H

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"

/*#define _CLUSTER_DEBUG	for local debugging */

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define cluster_export extern __declspec(dllimport)
#else
#define cluster_export extern __declspec(dllexport)
#endif
#else
#define cluster_export extern
#endif

cluster_export str CLUSTER_key( bat *M, bat *B);
cluster_export str CLUSTER_map(bat *RB, bat *B);
cluster_export str CLUSTER_apply(bat *bid, BAT *nb, BAT *cmap);
cluster_export str CLUSTER_column( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
cluster_export str CLUSTER_table( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


cluster_export str  CLS_create_bte(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset);
cluster_export str  CLS_create2_bte(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset, bit *order);

cluster_export str  CLS_create_sht(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset);
cluster_export str  CLS_create2_sht(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset, bit *order);

cluster_export str  CLS_create_int(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset);
cluster_export str  CLS_create2_int(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset, bit *order);

cluster_export str  CLS_create_wrd(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset);
cluster_export str  CLS_create2_wrd(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset, bit *order);

cluster_export str  CLS_create_lng(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset);
cluster_export str  CLS_create2_lng(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset, bit *order);

cluster_export str  CLS_create_flt(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset);
cluster_export str  CLS_create2_flt(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset, bit *order);

cluster_export str  CLS_create_dbl(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset);
cluster_export str  CLS_create2_dbl(bat *rpsum, bat *rcmap, bat *b, unsigned int *bits, unsigned int *offset, bit *order);

cluster_export str  CLS_map(bat *rb, bat *cmap, bat *b);
cluster_export str  CLS_map2(bat *rb, bat *psum, bat *cmap, bat *b);
cluster_export str CLS_split( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _CLUSTER_H */
