/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _VLT_SEQUENCE_
#define _VLT_SEQUENCE_
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_function.h"

//#define VLT_DEBUG 

#ifdef WIN32
#ifndef LIBGENERATOR
#define generator_export extern __declspec(dllimport)
#else
#define generator_export extern __declspec(dllexport)
#endif
#else
#define generator_export extern
#endif

generator_export str VLTgenerator_noop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
generator_export str VLTgenerator_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
generator_export str VLTgenerator_subselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
generator_export str VLTgenerator_thetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
generator_export str VLTgenerator_projection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
generator_export str VLTgenerator_join(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
generator_export str VLTgenerator_rangejoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
