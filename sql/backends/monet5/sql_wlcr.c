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
#include "sql_scenario.h"
#include "opt_prelude.h"
#include "mal_parser.h"
#include "mal_client.h"

#define WLCR_REPLAY 1
#define WLCR_CLONE 2

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

/*
 * Run the clone actions under a new client
 * and safe debugging in a tmp file.
 */
void
WLCRprocess(void *arg)
{	
	Client cntxt = (Client) arg;
	int i, pc;
	char path[PATHLENGTH];
	stream *fd;
	Client c;
	size_t sz;
	MalBlkPtr mb;
	InstrPtr q;
	str msg;
	mvc *sql;

	c =MCforkClient(cntxt);
	if( c == 0){
		GDKerror("Could not create user for WLCR process\n");
		return;
	}
    c->prompt = GDKstrdup("");  /* do not produce visible prompts */
    c->promptlength = 0;
    c->listing = 0;
	c->fdout = open_wastream("/tmp/wlcr");
	c->curprg = newFunction(putName("user"), putName("sql_wlcr"), FUNCTIONsymbol);
	mb = c->curprg->def;
	setVarType(mb, 0, TYPE_void);

	msg = SQLinitClient(c);
	if( msg != MAL_SUCCEED)
		mnstr_printf(c->fdout,"#Failed to initialize the client\n");
	msg = getSQLContext(c, mb, &sql, NULL);
	if( msg)
		mnstr_printf(c->fdout,"#Failed to access the transaction context: %s\n",msg);
    if ((msg = checkSQLContext(c)) != NULL)
		mnstr_printf(c->fdout,"#Inconsitent SQL contex : %s\n",msg);

	mnstr_printf(c->fdout,"#Ready to start the replayagainst '%s' batches %d threshold %d\n", 
		wlcr_master, wlcr_replaybatches, wlcr_replaythreshold);
	for( i= 0; i < wlcr_replaybatches; i++){
		snprintf(path,PATHLENGTH,"%s%cwlcr_%06d", wlcr_master, DIR_SEP,i);
		fd= open_rstream(path);
		if( fd == NULL){
			mnstr_printf(c->fdout,"#wlcr.process:'%s' can not be accessed \n",path);
			continue;
		}
		sz = getFileSize(fd);
		if (sz > (size_t) 1 << 29) {
			mnstr_destroy(fd);
			mnstr_printf(c->fdout, "wlcr.process File %s too large to process", path);
			continue;
		}
		c->fdin = bstream_create(fd, sz == 0 ? (size_t) (2 * 128 * BLOCK) : sz);
		if (bstream_next(c->fdin) < 0)
			mnstr_printf(c->fdout, "!WARNING: could not read %s\n", path);

		mnstr_printf(c->fdout,"#wlcr.process:start processing log file '%s'\n",path);
		c->yycur = 0;
		//
		// now parse the file line by line to reconstruct the WLCR blocks
		do{
			pc = mb->stop;
			if( parseMAL(c, c->curprg, 1, 1)  || mb->errors){
				mnstr_printf(c->fdout,"#wlcr.process:parsing failed '%s'\n",path);
			}
			q= getInstrPtr(mb, mb->stop-1);
			if ( getModuleId(q) == cloneRef && getFunctionId(q) ==execRef){
				printFunction(c->fdout, mb, 0, LIST_MAL_DEBUG );
				// execute this block
				chkTypes(c->fdout,c->nspace, mb, FALSE);
				chkFlow(c->fdout,mb);
				chkDeclarations(c->fdout,mb);
				sql->session->auto_commit = 0;
				sql->session->ac_on_commit = 1;
				sql->session->level = 0;
				(void) mvc_trans(sql);
				msg= runMAL(c,mb,0,0);
				if( msg != MAL_SUCCEED) // they should succeed
					break;
				if( mvc_commit(sql, 0, 0) < 0)
					mnstr_printf(c->fdout,"#wlcr.process transaction commit failed");

				// cleanup
				trimMalVariables(mb, NULL);
				resetMalBlk(mb, 1);
				pc = 0;
			}
		} while( mb->errors == 0 && pc != mb->stop);
		close_stream(fd);
	}
	(void) mnstr_flush(c->fdout);
	MCcloseClient(c);
	cntxt->wlcr_mode = 0;
}

str
WLCRreplay(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	char path[PATHLENGTH];
	stream *fd;

	if( cntxt->wlcr_mode == WLCR_CLONE || cntxt->wlcr_mode == WLCR_REPLAY){
		throw(SQL,"wlcr.replay","System already in replay mode");
	}
	cntxt->wlcr_mode = WLCR_REPLAY;
	msg = WLCRreplayinit(cntxt, mb, stk, pci);
	if( msg)
		return msg;

	snprintf(path,PATHLENGTH,"%s%cwlcr", wlcr_master, DIR_SEP);
	fd= open_rstream(path);
	if( fd == NULL){
		throw(SQL,"wlcr.replay","'%s' can not be accessed \n",path);
	}
	close_stream(fd);

/*
	WLCRprocess((void*) cntxt);
*/
    if (MT_create_thread(&wlcr_thread, WLCRprocess, (void*) cntxt, MT_THR_JOINABLE) < 0) {
			throw(SQL,"wlcr.replay","replay process can not be started\n");
	}
	return MAL_SUCCEED;
}

str
WLCRclone(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	char path[PATHLENGTH];
	stream *fd;

	if( cntxt->wlcr_mode == WLCR_CLONE || cntxt->wlcr_mode == WLCR_REPLAY){
		throw(SQL,"wlcr.clone","System already in synchronization mode");
	}
	msg = WLCRreplayinit(cntxt, mb, stk, pci);
	snprintf(path,PATHLENGTH,"%s%cwlcr", wlcr_master, DIR_SEP);
	fd= open_rstream(path);
	if( fd == NULL){
		throw(SQL,"wlcr.clone","'%s' can not be accessed \n",path);
	}
	close_stream(fd);

	cntxt->wlcr_mode = WLCR_CLONE;
	msg = WLCRreplayinit(cntxt, mb, stk, pci);
	if( msg)
		return msg;
/*
	WLCRprocess((void*) cntxt);
*/
    if (MT_create_thread(&wlcr_thread, WLCRprocess, (void*) cntxt, MT_THR_JOINABLE) < 0) {
			throw(SQL,"wlcr.clone","replay process can not be started\n");
	}
	return MAL_SUCCEED;
}
str
CLONEjob(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str kind = *getArgReference_str(stk,pci,5);
	(void) cntxt;
	(void) mb;
	if( strcmp(kind,"catalog") == 0)
		cntxt->wlcr_kind = WLCR_CATALOG;
	if( strcmp(kind,"update") == 0)
		cntxt->wlcr_kind = WLCR_UPDATE;
	if( strcmp(kind,"query") == 0)
		cntxt->wlcr_kind = WLCR_QUERY;
	return MAL_SUCCEED;
}


str
CLONEexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	// perform any cleanup
	cntxt->wlcr_mode = 0;
	return MAL_SUCCEED;
}

str
CLONEquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str qry =  *getArgReference_str(stk,pci,1);

	(void) mb;
	// execute the query in replay mode
	if( cntxt->wlcr_kind == WLCR_CATALOG || cntxt->wlcr_kind == WLCR_QUERY){
		mnstr_printf(cntxt->fdout,"#execute %s",qry);
		return SQLstatementIntern(cntxt, &qry, "SQLstatement", TRUE, TRUE, NULL);
	}
	return MAL_SUCCEED;
}

str
CLONEgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	// currently they are informative only
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
CLONEappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str sname, tname, cname;
    int tpe,i;
	mvc *m=NULL;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *ins = 0;
	str msg;

	sname = *getArgReference_str(stk,pci,2);
	tname = *getArgReference_str(stk,pci,3);
	cname = *getArgReference_str(stk,pci,4);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.append", "Schema missing");
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.append", "Table missing");
	// get the data into local BAT

	tpe= getArgType(mb,pci,5);
	ins = COLnew(0, tpe, 0, TRANSIENT);
	if( ins == NULL){
		throw(SQL,"CLONEappend",MAL_MALLOC_FAIL);
	}

	for( i = 5; i < pci->argc; i++)
		BATappend(ins, NULL, (void*) getArgReference(stk,pci,i), FALSE);

	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		store_funcs.append_col(m->session->tr, c, ins, tpe);
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i)
			store_funcs.append_idx(m->session->tr, i, ins, tpe);
	}
	BBPunfix(((BAT *) ins)->batCacheid);

	return MAL_SUCCEED;
}

str
CLONEdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}
str
CLONEupdateOID(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}
str
CLONEupdateValue(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}
str
CLONEclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}
