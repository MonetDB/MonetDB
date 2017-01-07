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
 *
 * At any time there can only be on choice to interpret the files.
 */
#include "monetdb_config.h"
#include "sql.h"
#include "wlcr.h"
#include "sql_wlcr.h"
#include "mal_parser.h"
#include "mal_client.h"

#define WLCR_REPLAY 1
#define WLCR_SYNC 2
static int wlcr_mode;

static str wlcr_master;
static int wlcr_replaythreshold;
static int wlcr_replaybatches;

static MT_Id wlcr_thread;

static str
WLCRreplayinit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    int i = 1, j,k;
	char path[PATHLENGTH];
	str dbname,dir;
	FILE *fd;

	(void) cntxt;
	(void) k;

    if (getArgType(mb, pci, i) == TYPE_str){
        dbname =  *getArgReference_str(stk,pci,i);
        i++;
    }
	if( dbname == NULL){
		throw(SQL,"wlcr.init","Master database name missing.");
	}
	snprintf(path,PATHLENGTH,"..%c%s",DIR_SEP,dbname);
	dir = GDKfilepath(0,path,"master",0);
	wlcr_master = GDKstrdup(dir);
	mnstr_printf(cntxt->fdout,"#WLCR master '%s'\n", wlcr_master);
	snprintf(path,PATHLENGTH,"%s%cwlcr", dir, DIR_SEP);
	mnstr_printf(cntxt->fdout,"#Testing access to master '%s'\n", path);
	fd = fopen(path,"r");
	if( fd == NULL){
		throw(SQL,"wlcr.init","Can not access master control file '%s'\n",path);
	}
	if( fscanf(fd,"%d %d", &j,&k) != 2){
		throw(SQL,"wlcr.init","'%s' does not have proper number of arguments\n",path);
	}
	wlcr_replaybatches = j;

    if ( i < pci->argc && getArgType(mb, pci, i) == TYPE_int){
        wlcr_replaythreshold = *getArgReference_int(stk,pci,i);
	}
	return MAL_SUCCEED;
}

void
WLCRprocess(void *arg)
{	
	Client cntxt = (Client) arg;
	int i;
	char path[PATHLENGTH];
	stream *fd;
	Client c;

	c =MCforkClient(cntxt);
	if( c == 0){
		GDKerror("Could not create user for WLCR process\n");
		return;
	}
    c->prompt = GDKstrdup("");  /* do not produce visible prompts */
    c->promptlength = 0;
    c->listing = 0;
	c->curprg = newFunction(putName("user"), putName("wlcr"), FUNCTIONsymbol);


	mnstr_printf(cntxt->fdout,"#Ready to start the replayagainst '%s' batches %d threshold %d", wlcr_master, wlcr_replaybatches, wlcr_replaythreshold);
	for( i= 0; i < wlcr_replaybatches; i++){
		snprintf(path,PATHLENGTH,"%s%cwlcr_%06d", wlcr_master, DIR_SEP,i);
		fd= open_rstream(path);
		if( fd == NULL){
			mnstr_printf(cntxt->fdout,"#wlcr.process:'%s' can not be accessed \n",path);
			continue;
		}
		if( MCpushClientInput(c, bstream_create(fd, 128 * BLOCK), 0, "") < 0){
			mnstr_printf(cntxt->fdout,"#wlcr.process: client can not be initialized \n");
		}
		mnstr_printf(cntxt->fdout,"#wlcr.process:start processing log file '%s'\n",path);
		c->yycur = 0;
		if( parseMAL(c, c->curprg, 1, 1)  || c->curprg->def->errors){
			mnstr_printf(cntxt->fdout,"#wlcr.process:parsing failed '%s'\n",path);
		}
		// preload the complete file
		// now parse the file line by line
		close_stream(fd);
	}
	(void) mnstr_flush(cntxt->fdout);
}

str
WLCRreplay(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	char path[PATHLENGTH];
	stream *fd;

	if( wlcr_mode == WLCR_SYNC){
		throw(SQL,"wlcr.replay","System already in synchronization mode");
	}
	if( wlcr_mode == WLCR_REPLAY){
		throw(SQL,"wlcr.replay","System already in replay mode");
	}
	wlcr_mode = WLCR_REPLAY;
	msg = WLCRreplayinit(cntxt, mb, stk, pci);
	if( msg)
		return msg;

	snprintf(path,PATHLENGTH,"%s%cwlcr", wlcr_master, DIR_SEP);
	fd= open_rstream(path);
	if( fd == NULL){
		throw(SQL,"wlcr.replay","'%s' can not be accessed \n",path);
	}
	close_stream(fd);

    if (MT_create_thread(&wlcr_thread, WLCRprocess, (void*) cntxt, MT_THR_JOINABLE) < 0) {
			throw(SQL,"wlcr.replay","replay process can not be started\n");
	}
	return MAL_SUCCEED;
}

str
WLCRsynchronize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	char path[PATHLENGTH];
	stream *fd;

	if( wlcr_mode == WLCR_SYNC || wlcr_mode == WLCR_REPLAY){
		throw(SQL,"wlcr.synchronize","System already in synchronization mode");
	}
	snprintf(path,PATHLENGTH,"%s%cwlcr", wlcr_master, DIR_SEP);
	fd= open_rstream(path);
	if( fd == NULL){
		throw(SQL,"wlcr.synchronize","'%s' can not be accessed \n",path);
	}
	close_stream(fd);

	wlcr_mode = WLCR_SYNC;
	msg = WLCRreplayinit(cntxt, mb, stk, pci);
	if( msg)
		return msg;
    if (MT_create_thread(&wlcr_thread, WLCRprocess, (void*) cntxt, MT_THR_JOINABLE) < 0) {
			throw(SQL,"wlcr.synchronize","replay process can not be started\n");
	}
	return MAL_SUCCEED;
}

