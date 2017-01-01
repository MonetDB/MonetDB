/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _LIDAR_
#define _LIDAR_
#include "mal.h"
#include "mal_client.h"

#ifdef WIN32
#ifndef LIBLIDAR
#define lidar_export extern __declspec(dllimport)
#else
#define lidar_export extern __declspec(dllexport)
#endif
#else
#define lidar_export extern
#endif

lidar_export str LIDARtest(int *res, str *fname);
lidar_export str LIDARdir(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
lidar_export str LIDARdirpat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
lidar_export str LIDARattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
lidar_export str LIDARloadTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
lidar_export str LIDARexportTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
lidar_export str LIDARprelude(void *ret);
#endif

