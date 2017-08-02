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
 * Copyright August 2008-2016 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _BASKETS_
#define _BASKETS_

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "sql.h"

#define INTIAL_BSKT 128

typedef struct{
	sql_table *table;	/* The basket stream table */
	sql_column **cols;	/* the table columns */
	BAT **bats;			/* the bats comprising the basket */
	int ncols;			/* number of columns of the table */
	BUN count;			/* number of events available in basket */
	int window;			/* consumption size */
	int stride;			/* stride forward after consumption  */

	/* statistics */
	timestamp seen;
	BUN events; /* total number of events grabbed */
	str error;
	/* concurrency control between petrinet/{receptor,emitter} */
	MT_Lock lock;
	MT_Id pid;
} *Basket, BasketRec;

sql5_export BasketRec *baskets;   /* the global timetrails catalog */
sql5_export int bsktTop, bsktLimit;

sql5_export int BSKTlocate(str sch, str tbl);
sql5_export void BSKTclean(int idx);
sql5_export str BSKTregisterInternal(Client cntxt, MalBlkPtr mb, str sch, str tbl, int* res);

sql5_export str BSKTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTtid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str BSKTkeep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTwindow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTtumble(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTreset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTlock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTunlock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str BSKTstatus( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str BSKTdrop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str BSKTappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTlock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str BSKTunlock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str BSKTdump(void *ret);
sql5_export void BSKTshutdown(void);
sql5_export str BSKTprelude(void *ret);
#endif
