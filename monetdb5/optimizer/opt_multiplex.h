/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _OPT_MULTIPLEX_H_
#define _OPT_MULTIPLEX_H_
#include "mal.h"
#include "mal_builder.h"
#include "opt_prelude.h"
#include "opt_support.h"

opt_export int OPTmultiplexImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
opt_export str OPTmultiplexSimple(Client cntxt, MalBlkPtr mb);

#define OPTDEBUGmultiplex  if ( optDebug & ((lng)1 <<DEBUG_OPT_MULTIPLEX) )

#endif
