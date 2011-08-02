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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @+ Implementation
 */
#include "gdk.h"

typedef struct {
	lng status;		/* counter status */
	lng generation;		/* IRIX                 : counter id
				   SunOS      + Perfmon : file descriptor
				   Linux-i?86 + Perfctr : struct vperfctr*
				   Linux-ia64 + Pfm     : pfmInfo_t*
				 */
	lng usec;		/* microseconds of elapsed time */
	lng clocks;		/* elapsed clock ticks */
	lng event0;		/* event counters */
	lng count0;
	lng event1;
	lng count1;
} counter;

