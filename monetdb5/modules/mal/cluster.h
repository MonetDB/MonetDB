/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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

cluster_export str CLUSTER_key( bat *M, const bat *B);
cluster_export str CLUSTER_map(bat *RB, const bat *B);
cluster_export str CLUSTER_apply(bat *bid, BAT *nb, BAT *cmap);
cluster_export str CLUSTER_column( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
cluster_export str CLUSTER_table( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


cluster_export str  CLS_create_bte(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset);
cluster_export str  CLS_create2_bte(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset, bit *order);

cluster_export str  CLS_create_sht(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset);
cluster_export str  CLS_create2_sht(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset, bit *order);

cluster_export str  CLS_create_int(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset);
cluster_export str  CLS_create2_int(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset, bit *order);

cluster_export str  CLS_create_wrd(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset);
cluster_export str  CLS_create2_wrd(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset, bit *order);

cluster_export str  CLS_create_lng(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset);
cluster_export str  CLS_create2_lng(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset, bit *order);

#ifdef HAVE_HGE
cluster_export str  CLS_create_hge(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset);
cluster_export str  CLS_create2_hge(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset, bit *order);
#endif

cluster_export str  CLS_create_flt(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset);
cluster_export str  CLS_create2_flt(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset, bit *order);

cluster_export str  CLS_create_dbl(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset);
cluster_export str  CLS_create2_dbl(bat *rpsum, bat *rcmap, bat *b, int *bits, int *offset, bit *order);

cluster_export str  CLS_map(bat *rb, bat *cmap, bat *b);
cluster_export str  CLS_map2(bat *rb, bat *psum, bat *cmap, bat *b);
cluster_export str CLS_split( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _CLUSTER_H */
