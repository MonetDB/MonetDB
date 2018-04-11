/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
fits_export str FITSexportTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

