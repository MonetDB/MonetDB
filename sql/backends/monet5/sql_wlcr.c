/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "wlcr.h"
#include "sql_wlcr.h"

static str wlcr_master;
static int wlcr_replaythreshold;

static str
WLCRinit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    int i = 1;

	(void) cntxt;

    wlcr_master = GDKgetenv("gdk_master");
    if (i< pci->argc+1 && getArgType(mb, pci, i) == TYPE_str){
        wlcr_master =  *getArgReference_str(stk,pci,i);
        wlcr_master = GDKfilepath(0,wlcr_master,"batch",0);
        i++;
    }
	if( wlcr_master == NULL){
		throw(SQL,"wlcr.init","Can not access the wlcr directory");
	}
    if ( i < pci->argc+1 && getArgType(mb, pci, i) == TYPE_int){
        wlcr_replaythreshold = *getArgReference_int(stk,pci,i);
	}
	return MAL_SUCCEED;
}

str
WLCRreplay(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	msg = WLCRinit(cntxt, mb, stk, pci);
	mnstr_printf(cntxt->fdout,"#Ready to start the replay against '%s' threshold %d", wlcr_master, wlcr_replaythreshold);
	return msg;
}

str
WLCRsynchronize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	msg = WLCRinit(cntxt, mb, stk, pci);
	mnstr_printf(cntxt->fdout,"#Ready to start the synchronization against '%s' threshold %d", wlcr_master, wlcr_replaythreshold);
	return msg;
}

