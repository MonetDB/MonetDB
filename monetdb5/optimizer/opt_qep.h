/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _OPT_QEP_
#define _OPT_QEP_
#include "mal.h"
#include "mal_interpreter.h"
#include "opt_prelude.h"
#include "opt_support.h"

typedef struct QEPrecord {
	MalBlkPtr mb;
	InstrPtr p;
	int plimit, climit;		/* capacities */
	struct QEPrecord **parents;	/* at least one link to parent */
	struct QEPrecord **children;
} *QEP;

#define MAXPARENT 4
#define MAXCHILD 8

opt_export int OPTdumpQEPImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#define OPTDEBUGdumpQEP  if ( optDebug & ((lng) 1 <<DEBUG_OPT_QEP) )

#endif /* _OPT_QEP_ */
