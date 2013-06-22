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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _GDAL_
#define _GDAL_
#include "mal.h"
#include "mal_client.h"

#ifdef WIN32
#ifndef LIBGDAL
#define gdal_export extern __declspec(dllimport)
#else
#define gdal_export extern __declspec(dllexport)
#endif
#else
#define gdal_export extern
#endif

gdal_export str GDALtest(int *wid, int *len, str *fname);
gdal_export str GDALattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
gdal_export str GDALloadGreyscaleImage(bat *x, bat *y, bat *intensity, str *fname);
gdal_export str GDALimportImage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

