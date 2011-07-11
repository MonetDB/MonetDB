/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _FITS_
#define _FITS_
#include "mal.h"
#include "mal_client.h"

#ifdef WIN32
#ifndef LIBFITS
#define fits_export extern __declspec(dllimport)
#else
#define fits_export extern __declspec(dllexport)
#endif
#else
#define fits_export extern
#endif

fits_export str FITStest(int *res, str *fname);
fits_export str FITSdir(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
fits_export str FITSdirpat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
fits_export str FITSattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
fits_export str FITSloadTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

