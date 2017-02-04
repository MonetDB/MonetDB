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
 * Alternatively you start with an empty database.
 * 
 * After restart of a mserver against the newly created image,
 * the log files from the master are processed.
 *
 * In replay mode also all queries are executed if they surpass
 * the latest threshold set for by the master.
 */
#include "monetdb_config.h"
#include "sql.h"
#include "wlcr.h"
#include "sql_wlcr.h"
#include "sql_scenario.h"
#include "opt_prelude.h"
#include "mal_parser.h"
#include "mal_client.h"
#include "querylog.h"

#define WLCR_REPLAY 1
#define WLCR_REPLICATE 2

#define WLCR_COMMIT 4
#define WLCR_ROLLBACK 5

/* The current status of the replica  processing */
static str wlr_logs;
static str wlr_master;
static int wlr_nextbatch; 	// the next file to be processed
static int wlr_tag;			// the next transaction to be processeds
static int wlr_threshold;	// replay threshold set by user.

#define MAXLINE 2048

static
str WLRgetConfig(void){
    char *path;
	char line[MAXLINE];
    FILE *fd;

	path = GDKfilepath(0,0,"wlr.config",0);
    fd = fopen(path,"r");
	GDKfree(path);
    if( fd == NULL)
        throw(MAL,"wlr.getConfig","Could not access configuration file\n");
    while( fgets(line, MAXLINE, fd) ){
		line[strlen(line)-1]= 0;
        if( strncmp("master=", line,7) == 0)
            wlr_master = GDKstrdup(line + 7);
        if( strncmp("logs=", line,5) == 0)
            wlr_logs = GDKstrdup(line + 5);
        if( strncmp("nextbatch=", line, 10) == 0)
            wlr_nextbatch = atoi(line+ 10);
        if( strncmp("tag=", line, 4) == 0)
            wlr_tag = atoi(line+ 4);
    }
    fclose(fd);
    return MAL_SUCCEED;
}

static
str WLRsetConfig(void){
    char *path;
    FILE *fd;

	path = GDKfilepath(0,0,"wlr.config",0);
    fd = fopen(path,"w");
	GDKfree(path);
    if( fd == NULL)
        throw(MAL,"wlr.setConfig","Could not access configuration file\n");
	fprintf(fd,"master=%s\n", wlr_master);
	fprintf(fd,"logs=%s\n", wlr_logs);
	fprintf(fd,"nextbatch=%d\n", wlr_nextbatch);
	fprintf(fd,"tag=%d\n", wlr_tag);
    fclose(fd);
    return MAL_SUCCEED;
}

str
WLRreplaythreshold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    (void) mb;
    (void) cntxt;
    wlr_threshold = * getArgReference_int(stk,pci,1);
	if( (wlr_threshold >=0 && wlr_threshold < wlcr_threshold) || wlcr_threshold < 0)
		throw(SQL,"wlr.replaythreshold","Warning: threshold is smaller then those currently reported");
    return MAL_SUCCEED;
}

/*
 * When the master database exist, we should set the replica administration.
 * But only once.
 *
 * The log files are identified by a range. It starts with 0 when an empty
 * database was used to bootstrap. Otherwise it is the range of the dbmaster.
 * At any time we should be able to restart the synchronization
 * process by grabbing a new set of log files.
 * This calls for keeping track in the replica what log files have been applied.
 */
static str
WLRgetMaster(str dbname)
{
	char path[PATHLENGTH];
	str dir;
	FILE *fd;

	if( dbname == 0)
		return MAL_SUCCEED;

	/* collect master properties */
	snprintf(path,PATHLENGTH,"..%c%s",DIR_SEP,dbname);
	dir = GDKfilepath(0,path,"master",0);

	fd = fopen(dir,"r");
	if( fd == NULL){
		GDKfree(dir);
		throw(SQL,"getMaster","Database '%s' not acting as a master",dbname);
	}
	(void) fclose(fd);

	wlr_logs = dir;
	snprintf(path,PATHLENGTH,"..%c%s%cmaster",DIR_SEP,dbname,DIR_SEP);
	dir = GDKfilepath(0,path,"wlc.config",0);
	fd = fopen(dir,"r");
	GDKfree(dir);
	if( fd == NULL){
		GDKfree(wlr_logs);
		wlr_logs = 0;
		throw(SQL,"getMaster","missing configuration file");
	}

	wlr_master = GDKstrdup(dbname);
    while( fgets(path, PATHLENGTH, fd) ){
		path[strlen(path)-1]= 0;
        if( strncmp("batches=", path, 8) == 0)
            wlcr_batches = atoi(path+ 8);
        if( strncmp("drift=", path, 6) == 0)
            wlcr_drift = atoi(path+ 6);
    }
	(void) fclose(fd);
	return MAL_SUCCEED;
}

static str
WLRinitReplica(str dbname)
{
	str dir, msg;
	FILE *fd;

	/* The replica mode can be set only once */
	dir = GDKfilepath(0,0,"wlr.config",0);
	fd = fopen(dir,"r");
	GDKfree(dir);
	if( fd ){
		(void) fclose(fd);
		throw(SQL,"setreplica","Already in replica mode for '%s'",dbname);
	}

	msg = WLRgetMaster(dbname);
	if( msg)
		return msg;

	wlr_master = GDKstrdup(dbname);
	wlr_nextbatch = 0;
	wlr_tag = 0;

	WLRsetConfig();	// initialize the replica configuration setting
	return MAL_SUCCEED;
}

/* 
 * Run once through the list of pending WLC logs
 * Continuing where you left off the previous time.
 */
static void
WLRprocess(void *arg)
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
	mnstr_printf(c->fdout,"#Ready to start the replay against '%s' batches %d:%d  threshold %d\n", 
		wlcr_archive, wlr_firstbatch, wlr_nextbatch, wlr_threshold);
#endif
	wlr_tag = 0;
	for( i= wlr_nextbatch; i < wlcr_batches && ! GDKexiting(); i++){
		snprintf(path,PATHLENGTH,"%s%c%s_%012d", wlr_logs, DIR_SEP, wlr_master, i);
		fd= open_rstream(path);
		if( fd == NULL){
			mnstr_printf(GDKerr,"#wlcr.process:'%s' can not be accessed \n",path);
			// Be careful not to miss log files.
			// In the future wait for more files becoming available.
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
		mnstr_printf(cntxt->fdout,"#replay log file:%s\n",path);
		//
		// now parse the file line by line to reconstruct the WLR blocks
		do{
			pc = mb->stop;
			if( parseMAL(c, c->curprg, 1, 1)  || mb->errors){
				mnstr_printf(GDKerr,"#wlcr.process:parsing failed '%s'\n",path);
			}
			mb = c->curprg->def; // needed
			q= getInstrPtr(mb, mb->stop-1);
			// only re-execute successful transactions.
			if ( getModuleId(q) == wlrRef && getFunctionId(q) ==commitRef ){
				pushEndInstruction(mb);
				// execute this block if no errors are found
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
					mnstr_printf(GDKerr,"ERROR in processing batch %d :%s\n", i, msg);
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
		wlr_nextbatch++;
		WLRsetConfig();
		close_stream(fd);
	}
	(void) mnstr_flush(c->fdout);
	MCcloseClient(c);
	cntxt->wlcr_mode = 0;
}

static void
WLRprocessScheduler(void *arg)
{
	Client cntxt = (Client) arg;

	while(1){
		if( wlr_master)
			WLRgetMaster(wlr_master);
		if( wlr_nextbatch < wlcr_batches)
			WLRprocess(cntxt);
		// wait at most for the drift period
		MT_sleep_ms( (wlcr_drift? wlcr_drift:1) * 1000 );
	}
}

void
WLRinit(Client cntxt)
{
	MT_Id wlcr_thread;
	
	WLRgetConfig();
	(void) WLRgetMaster(wlr_master);
	// time to continue the consolidation process in the background
	if( wlr_logs){
		// Always try to roll forward before you continue
		cntxt->wlcr_mode = WLCR_REPLICATE;
		// The client has to wait initially for all logs known to be processed.
		WLRprocess(cntxt);
		if (MT_create_thread(&wlcr_thread, WLRprocessScheduler, (void*) cntxt, MT_THR_JOINABLE) < 0) {
				GDKerror("wlcr.replicate:replay scheduling process can not be started");
		}
	}
}

str
WLCRreplicate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg;
	MT_Id wlcr_thread;
	(void) mb;

	if( cntxt->wlcr_mode == WLCR_REPLICATE || cntxt->wlcr_mode == WLCR_REPLAY){
		throw(SQL,"wlr.replicate","System already in synchronization mode");
	}
	msg = WLRinitReplica( *getArgReference_str(stk,pci,1));
	if( msg)
		return msg;

	cntxt->wlcr_mode = WLCR_REPLICATE;
	// The client has to wait initially for all logs known to be processed.
	WLRprocess(cntxt);
	// start the process for continual integration in the background
    if (MT_create_thread(&wlcr_thread, WLRprocessScheduler, (void*) cntxt, MT_THR_JOINABLE) < 0) {
			throw(SQL,"wlr.replicate","replay scheduling process can not be started\n");
	}
	return MAL_SUCCEED;
}

str
WLRtransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;
	int i;

	(void) cntxt;
	(void) pci;
	(void) stk;
	cntxt->wlcr_kind = 0;
	for( i = mb->stop-1; cntxt->wlcr_kind == 0 && i > 1; i--){
		p = getInstrPtr(mb,i);
		if( getModuleId(p) == wlrRef && getFunctionId(p)== commitRef) 
			cntxt->wlcr_kind = WLCR_COMMIT;
		if( getModuleId(p) == wlrRef && getFunctionId(p)== rollbackRef)
			cntxt->wlcr_kind = WLCR_ROLLBACK;
	}
	return MAL_SUCCEED;
}


str
WLRfinish(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
	char *x, *y, *qtxt;

	(void) mb;
	if( cntxt->wlcr_kind == WLCR_ROLLBACK)
		return msg;
	// execute the query in replay mode when required.
	// check the old timings
	if( wlr_threshold >= 0){
		// we need to get rid of the escaped quote.
		x = qtxt= (char*) GDKmalloc(strlen(qry) +1);
		for(y = qry; *y; y++){
			if( *y == '\\' ){
				if( *(y+1) ==  '\'')
				y += 1;
			}
			*x++ = *y;
		}
		*x = 0;
		msg =  SQLstatementIntern(cntxt, &qtxt, "SQLstatement", TRUE, TRUE, NULL);
		GDKfree(qtxt);
	}
	return msg;
}

str
WLRchange(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str qry =  *getArgReference_str(stk,pci,1);
	str msg = MAL_SUCCEED;
	char *x, *y, *qtxt;

	(void) mb;
	if( cntxt->wlcr_kind == WLCR_ROLLBACK)
		return msg;
	// we need to get rid of the escaped quote.
	x = qtxt= (char*) GDKmalloc(strlen(qry) +1);
	for(y = qry; *y; y++){
		if( *y == '\\' ){
			if( *(y+1) ==  '\'')
			y += 1;
		}
		*x++ = *y;
	}
	*x = 0;
	msg =  SQLstatementIntern(cntxt, &qtxt, "SQLstatement", TRUE, TRUE, NULL);
	GDKfree(qtxt);
	return msg;
}

str
WLRcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	return WLRchange(cntxt,mb,stk,pci);
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
	str msg= MAL_SUCCEED;

	if( cntxt->wlcr_kind == WLCR_ROLLBACK)
		return msg;
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
	str msg= MAL_SUCCEED;

	if( cntxt->wlcr_kind == WLCR_ROLLBACK)
		return msg;
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
	str msg= MAL_SUCCEED;
	oid o;
	int tpe = getArgType(mb,pci,5);

	if( cntxt->wlcr_kind == WLCR_ROLLBACK)
		return msg;
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
        store_funcs.update_col(m->session->tr, c, tids, upd, TYPE_bat);
    } else if (cname[0] == '%') {
        sql_idx *i = mvc_bind_idx(m, s, cname + 1);
        if (i)
            store_funcs.update_idx(m->session->tr, i, tids, upd, TYPE_bat);
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
	str msg= MAL_SUCCEED;
	str *sname = getArgReference_str(stk, pci, 1);
	str *tname = getArgReference_str(stk, pci, 2);

	if( cntxt->wlcr_kind == WLCR_ROLLBACK)
		return msg;
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
