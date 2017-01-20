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

/* The current status of the replica  */
/* TODO actually move these to the WLRprocess client record */
static str wlr_dbname;
static str wlr_snapshot;
static str wlr_archive;
static int wlr_firstbatch; // the next log file to handle
static int wlr_lastbatch; // the last log file report by the master
static int wlr_tag;

/* The master configuration file is a simple key=value table.
 * It is analysed here. 
 * We assume that there always only one master from which we collect the logs */
static
str WLRgetConfig(void){
    char path[PATHLENGTH];
    FILE *fd;

    snprintf(path,PATHLENGTH,"%s%cwlcr.config", wlr_archive, DIR_SEP);
    fd = fopen(path,"r");
    if( fd == NULL)
        throw(MAL,"wlr.getConfig","Could not access %s\n",path);
    while( fgets(path, PATHLENGTH, fd) ){
		path[strlen(path)-1]= 0;
        if( strncmp("archive=", path,8) == 0)
            wlr_archive = GDKstrdup(path + 8);
        if( strncmp("snapshot=", path,9) == 0)
            wlr_snapshot = GDKstrdup(path + 9);
        if( strncmp("start=", path,6) == 0)
            wlr_firstbatch = atoi(path+ 6);
        if( strncmp("last=", path, 5) == 0)
            wlr_lastbatch = atoi(path+ 5);
    }
    fclose(fd);
    return MAL_SUCCEED;
}

static
str WLRsetConfig(void){
    char path[PATHLENGTH];
    FILE *fd;

    snprintf(path,PATHLENGTH,"%s%cwlcr.config", wlr_archive, DIR_SEP);
    fd = fopen(path,"w");
    if( fd == NULL)
        throw(MAL,"wlr.setConfig","Could not access %s\n",path);
    while( fgets(path, PATHLENGTH, fd) ){
		path[strlen(path)-1]= 0;
        if( strncmp("archive=", path,8) == 0)
            wlr_archive = GDKstrdup(path + 8);
        if( strncmp("snapshot=", path,9) == 0)
            wlr_snapshot = GDKstrdup(path + 9);
        if( strncmp("start=", path,6) == 0)
            wlr_firstbatch = atoi(path+ 6);
        if( strncmp("last=", path, 5) == 0)
            wlr_lastbatch = atoi(path+ 5);
    }
    fclose(fd);
    return MAL_SUCCEED;
}

/*
static str
WLRgetlogfile( Client cntxt, MalBlkPtr mb)
{
	mvc *m = NULL;
	str msg;
	atom *a;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	a = stack_get_var(m, "replaylog");
	if (!a) {
		throw(SQL, "sql.getVariable", "variable 'replaylog' unknown");
	}
	cntxt->wlcr_replaylog = GDKstrdup(a->data.val.sval);
	return MAL_SUCCEED;
}
*/

/*
static str
WLRgetThreshold( Client cntxt, MalBlkPtr mb)
{
	mvc *m = NULL;
	str msg;
	atom *a;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	a = stack_get_var(m, "replaythreshold");
	if (!a) {
		throw(SQL, "sql.getVariable", "variable 'replaylog' unknown");
	}
	cntxt->wlcr_threshold = atoi(a->data.val.sval);
	return MAL_SUCCEED;
}
*/

/*
 * The log files are identified by a range. It starts with 0 when an empty
 * database was used to bootstrap. Otherwise it is the range of the dbmaster.
 * At any time we should be able to restart the synchronization
 * process by grabbing a new set of log files.
 * This calls for keeping track in the replica what log files have been applied.
 * It is stored in a separate directory for two purposes. It allows us to fuse
 * different masters and even to consider bulk copying the logfiles into a local
 * cache structure before applying them.
 */
static str
WLRinit(str dbname)
{
	char path[PATHLENGTH];
	str dir;

	wlr_dbname = GDKstrdup(dbname);
	snprintf(path,PATHLENGTH,"..%c%s",DIR_SEP,dbname);
	dir = GDKfilepath(0,path,"master",0);
	wlr_archive = GDKstrdup(dir);
	WLRgetConfig();
	return MAL_SUCCEED;
}


/*
static void
WLRstatus(int idx, int tag)
{
	FILE *fd;
	char path[PATHLENGTH];
	snprintf(path,PATHLENGTH,"status_%s",wlr_dbname);
	fd = fopen(path,"w");
	if( fd == NULL)
		GDKerror("Could not create clone status file");
	else {
		fprintf(fd,"%d %d\n", idx, tag);
		fclose(fd);
	}
}
*/
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
		GDKerror("Could not create user for WLR process\n");
		return;
	}
    c->prompt = GDKstrdup("");  /* do not produce visible prompts */
    c->promptlength = 0;
    c->listing = 0;
	c->fdout = open_wastream(".wlcr");
	c->curprg = newFunction(putName("user"), putName("sql_wlcr"), FUNCTIONsymbol);
	mb = c->curprg->def;
	setVarType(mb, 0, TYPE_void);

	msg = SQLinitClient(c);
	if( msg != MAL_SUCCEED)
		mnstr_printf(GDKerr,"#Failed to initialize the client\n");
	msg = getSQLContext(c, mb, &sql, NULL);
	if( msg)
		mnstr_printf(GDKerr,"#Failed to access the transaction context: %s\n",msg);
    if ((msg = checkSQLContext(c)) != NULL)
		mnstr_printf(GDKerr,"#Inconsitent SQL contex : %s\n",msg);

#ifdef _WLR_DEBUG_
	mnstr_printf(c->fdout,"#Ready to start the replayagainst '%s' batches %d:%d \n", 
		wlr_archive, wlr_firstbatch, wlr_lastbatch);
#endif
	wlr_tag = 0;
	for( i= wlr_firstbatch; i < wlr_lastbatch; i++){
		snprintf(path,PATHLENGTH,"%s%c%s_%012d", wlr_archive, DIR_SEP, wlr_dbname, i);
		fd= open_rstream(path);
		if( fd == NULL){
			mnstr_printf(GDKerr,"#wlcr.process:'%s' can not be accessed \n",path);
			continue;
		}
		sz = getFileSize(fd);
		if (sz > (size_t) 1 << 29) {
			mnstr_destroy(fd);
			mnstr_printf(GDKerr, "wlcr.process File %s too large to process", path);
			continue;
		}
		c->fdin = bstream_create(fd, sz == 0 ? (size_t) (2 * 128 * BLOCK) : sz);
		if (bstream_next(c->fdin) < 0)
			mnstr_printf(GDKerr, "!WARNING: could not read %s\n", path);

#ifdef _WLR_DEBUG_
		mnstr_printf(c->fdout,"#wlcr.process:start processing log file '%s'\n",path);
#endif
		c->yycur = 0;
		//
		// now parse the file line by line to reconstruct the WLR blocks
		do{
			pc = mb->stop;
			if( parseMAL(c, c->curprg, 1, 1)  || mb->errors){
				mnstr_printf(GDKerr,"#wlcr.process:parsing failed '%s'\n",path);
			}
			mb = c->curprg->def; // needed
			q= getInstrPtr(mb, mb->stop-1);
			if ( getModuleId(q) == wlrRef && getFunctionId(q) ==execRef){
				pushEndInstruction(mb);
				// execute this block
				chkTypes(c->fdout,c->nspace, mb, FALSE);
				chkFlow(c->fdout,mb);
				chkDeclarations(c->fdout,mb);
				//printFunction(GDKerr, mb, 0, LIST_MAL_DEBUG );

				sql->session->auto_commit = 0;
				sql->session->ac_on_commit = 1;
				sql->session->level = 0;
				(void) mvc_trans(sql);
				msg= runMAL(c,mb,0,0);
				wlr_tag++;
				WLRsetConfig();
				if( msg != MAL_SUCCEED){
					// they should always succeed
					mnstr_printf(GDKerr,"ERROR in processing batch %d \n", i);
					printFunction(GDKerr, mb, 0, LIST_MAL_DEBUG );
					mvc_rollback(sql,0,NULL);
					break;
				}
				if( mvc_commit(sql, 0, 0) < 0)
					mnstr_printf(GDKerr,"#wlcr.process transaction commit failed");

				// cleanup
				resetMalBlk(mb, 1);
				trimMalVariables(mb, NULL);
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
	(void) mb;

	if( cntxt->wlcr_mode == WLCR_CLONE || cntxt->wlcr_mode == WLCR_REPLAY){
		throw(SQL,"wlcr.replay","System already in replay mode");
	}
	cntxt->wlcr_mode = WLCR_REPLAY;
	msg = WLRinit( *getArgReference_str(stk,pci,1));
	if( msg)
		return msg;

	WLCRprocess((void*) cntxt);
	return MAL_SUCCEED;
}

str
WLCRclone(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	MT_Id wlcr_thread;
	(void) mb;

	if( cntxt->wlcr_mode == WLCR_CLONE || cntxt->wlcr_mode == WLCR_REPLAY){
		throw(SQL,"wlcr.clone","System already in synchronization mode");
	}
	msg = WLRinit( *getArgReference_str(stk,pci,1));
	if( msg)
		return msg;

	cntxt->wlcr_mode = WLCR_CLONE;
    if (MT_create_thread(&wlcr_thread, WLCRprocess, (void*) cntxt, MT_THR_JOINABLE) < 0) {
			throw(SQL,"wlcr.clone","replay process can not be started\n");
	}
	return MAL_SUCCEED;
}

str
WLRjob(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
WLRexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	// perform any cleanup
	cntxt->wlcr_mode = 0;
	return MAL_SUCCEED;
}

str
WLRquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str qry =  *getArgReference_str(stk,pci,1);
	str msg = MAL_SUCCEED;
	lng clk = GDKms();

	(void) mb;
	// execute the query in replay mode
	if( cntxt->wlcr_kind == WLCR_CATALOG || cntxt->wlcr_kind == WLCR_QUERY){
		msg =  SQLstatementIntern(cntxt, &qry, "SQLstatement", TRUE, TRUE, NULL);
		mnstr_printf(cntxt->fdout,"# "LLFMT"ms\n",GDKms() - clk);
	}
	return msg;
}

str
WLRgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	// currently they are informative only
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

#define WLRcolumn(TPE) \
		for( i = 4; i < pci->argc; i++){\
			TPE val = *getArgReference_##TPE(stk,pci,i);\
			BUNappend(ins, (void*) &val, FALSE);\
		}

str
WLRappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str sname, tname, cname;
    int tpe,i;
	mvc *m=NULL;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *ins = 0;
	str msg;

	sname = *getArgReference_str(stk,pci,1);
	tname = *getArgReference_str(stk,pci,2);
	cname = *getArgReference_str(stk,pci,3);

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

	tpe= getArgType(mb,pci,4);
	ins = COLnew(0, tpe, 0, TRANSIENT);
	if( ins == NULL){
		throw(SQL,"WLRappend",MAL_MALLOC_FAIL);
	}

	switch(ATOMstorage(tpe)){
	case TYPE_bit: WLRcolumn(bit); break;
	case TYPE_bte: WLRcolumn(bte); break;
	case TYPE_sht: WLRcolumn(sht); break;
	case TYPE_int: WLRcolumn(int); break;
	case TYPE_lng: WLRcolumn(lng); break;
	case TYPE_oid: WLRcolumn(oid); break;
	case TYPE_flt: WLRcolumn(flt); break;
	case TYPE_dbl: WLRcolumn(dbl); break;
#ifdef HAVE
	case TYPE_hge: WLRcolumn(hge); break;
#endif
	case TYPE_str:
		for( i = 4; i < pci->argc; i++){
			str val = *getArgReference_str(stk,pci,i);
			BUNappend(ins, (void*) val, FALSE);
		}
		break;
	}

	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		store_funcs.append_col(m->session->tr, c, ins, TYPE_bat);
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i)
			store_funcs.append_idx(m->session->tr, i, ins, tpe);
	}
	BBPunfix(((BAT *) ins)->batCacheid);

	return MAL_SUCCEED;
}

str
WLRdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	str sname, tname;
    int i;
	mvc *m=NULL;
	sql_schema *s;
	sql_table *t;
	BAT *ins = 0;
	str msg;

	sname = *getArgReference_str(stk,pci,1);
	tname = *getArgReference_str(stk,pci,2);

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

	ins = COLnew(0, TYPE_oid, 0, TRANSIENT);
	if( ins == NULL){
		throw(SQL,"WLRappend",MAL_MALLOC_FAIL);
	}

	WLRcolumn(oid); 

    store_funcs.delete_tab(m->session->tr, t, ins, TYPE_bat);
	BBPunfix(((BAT *) ins)->batCacheid);

	return MAL_SUCCEED;
}

#define WLRvalue(TPE) \
{	TPE val = *getArgReference_##TPE(stk,pci,5);\
	BUNappend(upd, (void*) &val, FALSE);\
}


str
WLRupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sname, tname, cname;
	mvc *m=NULL;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *upd = 0, *tids=0;
	str msg;
	oid o;
	int tpe = getArgType(mb,pci,5);

	sname = *getArgReference_str(stk,pci,1);
	tname = *getArgReference_str(stk,pci,2);
	cname = *getArgReference_str(stk,pci,3);
	o = *getArgReference_oid(stk,pci,4);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.update", "Schema missing");
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.update", "Table missing");
	// get the data into local BAT

	tids = COLnew(0, TYPE_oid, 0, TRANSIENT);
	if( tids == NULL){
		throw(SQL,"WLRupdate",MAL_MALLOC_FAIL);
	}
	upd = COLnew(0, tpe, 0, TRANSIENT);
	if( upd == NULL){
		BBPunfix(((BAT *) tids)->batCacheid);
		throw(SQL,"WLRupdate",MAL_MALLOC_FAIL);
	}
	BUNappend(tids, &o, FALSE);

	switch(ATOMstorage(tpe)){
	case TYPE_bit: WLRvalue(bit); break;
	case TYPE_bte: WLRvalue(bte); break;
	case TYPE_sht: WLRvalue(sht); break;
	case TYPE_int: WLRvalue(int); break;
	case TYPE_lng: WLRvalue(lng); break;
	case TYPE_oid: WLRvalue(oid); break;
	case TYPE_flt: WLRvalue(flt); break;
	case TYPE_dbl: WLRvalue(dbl); break;
#ifdef HAVE
	case TYPE_hge: WLRvalue(hge); break;
#endif
	case TYPE_str:
		{ 	str val = *getArgReference_str(stk,pci,5);
			BUNappend(upd, (void*) val, FALSE);
		}
		break;
	default:
		GDKerror("Missing type in WLRupdate");
	}

	BATmsync(tids);
	BATmsync(upd);
    if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
        store_funcs.update_col(m->session->tr, c, tids, upd, tpe);
    } else if (cname[0] == '%') {
        sql_idx *i = mvc_bind_idx(m, s, cname + 1);
        if (i)
            store_funcs.update_idx(m->session->tr, i, tids, upd, tpe);
    }
	BBPunfix(((BAT *) tids)->batCacheid);
	BBPunfix(((BAT *) upd)->batCacheid);
	return MAL_SUCCEED;
}

str
WLRclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	sql_schema *s;
	sql_table *t;
	mvc *m = NULL;
	str msg;
	str *sname = getArgReference_str(stk, pci, 1);
	str *tname = getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sname);
	if (s == NULL)
		throw(SQL, "sql.clear_table", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tname);
	if (t == NULL)
		throw(SQL, "sql.clear_table", "42S02!Table missing");
	(void) mvc_clear_table(m, t);
	return MAL_SUCCEED;
}
