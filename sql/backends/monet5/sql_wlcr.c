/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * A master can be replicated by taking a binary copy of the 'bat' directory.
 * This should be done under control of the program monetdb, e.g.
 * monetdb replica <masterlocation> <dbname>+
 * 
 * After restart of a mserver against the newly created image,
 * the log files from the master are processed.
 * Alternatively you start with an empty database.
 *
 * Since the wlcr files can be stored anywhere, the full path should be given.
 */
#include "monetdb_config.h"
#include "sql.h"
#include "wlcr.h"
#include "sql_wlcr.h"

static str wlcr_master;
static int wlcr_replaythreshold;
static int wlcr_replaybatch;

static str
WLCRinit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    int i = 1, j,k,l;
	char path[PATHLENGTH];
	FILE *fd;

	(void) cntxt;
	(void) k;
	(void) l;

    wlcr_master = GDKgetenv("gdk_master");
    if (i< pci->argc+1 && getArgType(mb, pci, i) == TYPE_str){
        wlcr_master =  *getArgReference_str(stk,pci,i);
        i++;
    }
	if( wlcr_master == NULL){
		throw(SQL,"wlcr.init","Can not access the wlcr directory");
	}
	snprintf(path,PATHLENGTH,"%s%cwlcr", wlcr_master, DIR_SEP);
	mnstr_printf(cntxt->fdout,"#Testing '%s'\n", path);
	fd = fopen(path,"r");
	if( fd == NULL){
		throw(SQL,"wlcr.init","Can not access '%s'\n",path);
	}
	if( fscanf(fd,"%d %d %d", &j,&k,&l) != 3){
		throw(SQL,"wlcr.init","'%s' does not have proper number of arguments\n",path);
	}
	wlcr_replaybatch = j;

    if ( i < pci->argc+1 && getArgType(mb, pci, i) == TYPE_int){
        wlcr_replaythreshold = *getArgReference_int(stk,pci,i);
	}
	return MAL_SUCCEED;
}

str
WLCRreplay(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	int i;
	char path[PATHLENGTH];
	stream *fd;

	msg = WLCRinit(cntxt, mb, stk, pci);
	mnstr_printf(cntxt->fdout,"#Ready to start the replay against '%s' threshold %d", wlcr_master, wlcr_replaythreshold);

	for( i= 0; i < wlcr_replaybatch; i++){
		snprintf(path,PATHLENGTH,"%s%cwlcr_%06d", wlcr_master, DIR_SEP,i);
		fd= open_rstream(path);
		if( fd == NULL){
			throw(SQL,"wlcr.replay","'%s' can not be accessed \n",path);
		}
		close_stream(fd);
	}
	return msg;
}

str
WLCRsynchronize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	int i;
	char path[PATHLENGTH];
	stream *fd;

	msg = WLCRinit(cntxt, mb, stk, pci);
	mnstr_printf(cntxt->fdout,"#Ready to start the synchronization against '%s' threshold %d", wlcr_master, wlcr_replaythreshold);

	for( i= 0; i < wlcr_replaybatch; i++){
		snprintf(path,PATHLENGTH,"%s%cwlcr_%06d", wlcr_master, DIR_SEP,i);
		fd= open_rstream(path);
		if( fd == NULL){
			throw(SQL,"wlcr.synchronize","'%s' can not be accessed \n",path);
		}
		close_stream(fd);
	}
	return msg;
}

