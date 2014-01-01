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

#define OPTDEBUGdumpQEP  if ( optDebug & (1 <<DEBUG_OPT_QEP) )

#endif /* _OPT_QEP_ */
