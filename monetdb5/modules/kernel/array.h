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

#ifndef _ARRAY_H_
#define _ARRAY_H_
#include "gdk.h"
#include "mal.h"
#include "algebra.h"
#include <math.h>
#include <time.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define array_export extern __declspec(dllimport)
#else
#define array_export extern __declspec(dllexport)
#endif
#else
#define array_export extern
#endif

array_export str ARRAYproduct(int *ret, int *ret2, int *bid, int *rid);
array_export str ARRAYproject(int *ret, int *bid, int *cst);

array_export str ARRAYgrid_int(int *ret, int *groups, int *groupsize, int *clustersize, int *offset);
array_export str ARRAYgridShift_int(int *ret, int *groups, int *groupsize, int *clustersize, int *offset, int *shift);
array_export str ARRAYgridBAT_int(int *ret, int *bid, int *groups, int *groupsize, int *clustersize, int *offset);
array_export str ARRAYgridBATshift_int(int *ret, int *bid, int *groups, int *groupsize, int *clustersize, int *offset, int *shift);

array_export str ARRAYgrid_lng(lng *ret, lng *groups, lng *groupsize, lng *clustersize, lng *offset);
array_export str ARRAYgridShift_lng(lng *ret, lng *groups, lng *groupsize, lng *clustersize, lng *offset, lng *shift);
array_export str ARRAYgridBAT_lng(lng *ret, lng *bid, lng *groups, lng *groupsize, lng *clustersize, lng *offset);
array_export str ARRAYgridBATshift_lng(lng *ret, lng *bid, lng *groups, lng *groupsize, lng *clustersize, lng *offset, lng *shift);
#endif
