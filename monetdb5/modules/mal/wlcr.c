/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) 2017 Martin Kersten
 * This module collects the workload-capture-replay statements during transaction execution,
 * also known as asynchronous logical replication log management.
 * It is used primarilly for recovery and replication management. 
 *
 * The goal is to easily clone a master database.  Accidental corruption of the  replicated
 * data should be avoided by setting ownership and access properties at the SQL level.
 *
 *
 * IMPLEMENTATION
 * The underlying assumption of the techniques deployed is that the database
 * resides on a proper (global) file system to guarantees recovery from most
 * storage system related failures. Such as RAID disks.
 * Furthermore, when deployed in a Cloud setting, the database recides in the
 * global file system.
 *
 * A database can be set once into 'master' mode only once using the SQL command:
 * CALL master()
 *
 * It creates a directory .../dbfarm/dbname/master to hold all necessary information
 * for the creation and maintenance of replicas.
 * A configuration file is added to keep track on the status of the master.
 * It contains the following key=value pairs:
 * 		snapshot=<path to a binary snapshot>
 * 		logs=<path to the wlcr log directory>
 * 		start=<first batch file to be applied>
 * 		last=<last batch file to be applied>
 * 		drift=<maximal delay before transactions are seen globally, in seconds>
 *
 * Every replica should start off with a copy of binary snapshot identified by 'snapshot'
 * by default stored in .../dbfarm/dbname/master/bat. An alternative path can be given
 * to reduce the storage cost at the expense of slower recovery time (e.g. AWS glacier).
 * A missing path to the snapshot denotes that we can start rebuilding with an empty database instead.
 * The log files are stored as master/<dbname>_<batchnumber>.
 * 
 * Each wlcr log file contains a serial log of committed transactions.
 * The log records are represented as ordinary MAL statement blocks, which
 * are executed in serial mode. (parallelism can be considered for large updates)
 * Each transaction job is identified by the owner of the query, 
 * commit/rollback status, its starting time and runtime (in ms).
 * The end of the transaction is marked as exec()
 *
 * Logging of queries can be limited to those that satisfy an minimum execution threshold.
 * SET replaythreshold= <number>
 * The threshold is given in milliseconds. A negative threshold leads to ignoring all queries.
 * The threshold setting is not saved because it is a client session specific action.
 * The default for a production system version is set to -1
 *
 * A transaction log is owned by the master. He decides when the log may be globally
 * used. There are several triggers for this. A new transaction log is created when
 * the system has been collecting logs for some time (drift).
 * The problem here is that we should ensure that the log file is closed even if there
 * are no transactions running. After closing, the replicas can see from the
 * master configuration file that a new batch is available.
 * The maximum drift can be set using a SQL command. Setting it to zero leads to a
 * log file per transaction.
 *
 * A more secure way to set a database into master mode is to use the command
 *	 monetdb master <dbname> [ <optional snapshot path>]
 * which locks the database, takes a save copy, initializes the state chance. 
 *
 * A replica can be constructed as follows:
 * 	monetdb replica <dbname> <mastername>
 *
 * Instead of using the monetdb command line we can use the SQL calls directly
 * master and replica, provided we start with a fresh database.
 *
 * Processing the log files starts in the background using the call.
 * CALL replicate("mastername")
 * It will iterate through the log files, applying all transactions.
 * Queries are simply ignored unless needed as replacement for catalog actions.
 *
 * The alternative is to replay only the query log
 * CALL replicate("dbname",threshold)
 * In this mode all pure queries are executed under the credentials of the query owner
 * for which the reported threshold exceeds the argument[TODO].
 * It excludes catalog and update queries.
 *
 * Any failure encountered during a log replay terminates the process,
 * leaving a message in the merovingian log.
 *
 * The wlcr files purposely have a textual format derived from the MAL statements.
 * Simplicity and ease of control has been the driving argument here.
 *
 * [TODO]The progress can be inspected using the timestamp function wlcr.drift(),
 * which indicates how far apart the current replica is compared to the master transaction log.
 * [TODO] the user might want to indicate a time-stamped, ignoring all actions afterwards.
 *
 */
#include "monetdb_config.h"
#include <time.h>
#include "mal_builder.h"
#include "wlcr.h"

static MT_Lock     wlcr_lock MT_LOCK_INITIALIZER("wlcr_lock");

static char *wlcr_name[]= {"","query","update","catalog","ignore"};

static str wlcr_snapshot= 0; // The name of the snapshot against the logs work
static str wlcr_logs = 0; 	// The location in the global file store for the logs
static char wlcr_time[26];	// The timestamp of the last committed transaction.
static stream *wlcr_fd = 0;
static int wlcr_start = 0;	// time stamp of first transaction in log file
int wlcr_threshold;

// These properties are needed by the replica to direct the roll-forward.
str wlcr_dbname = 0;  		// The master database name
int wlcr_firstbatch = 0;	// first log file  associated with the snapshot
int wlcr_batches = 0;		// identifier of next batch
int wlcr_drift = 10;	// maximal period covered by a single log file in seconds
int wlcr_tid = 0;			// transaction id of next to be processed

/* The database snapshots are binary copies of the dbfarm/database/bat
 * New snapshots are created currently using the 'monetdb snapshot <db>' command
 * or a SQL procedure.
 *
 * The wlcr logs are stored in the snapshot directory as a time-stamped list
 */

int
WLCused(void)
{
	return wlcr_logs != NULL;
}

/* The master configuration file is a simple key=value table */
str WLCgetConfig(void){
	char path[PATHLENGTH];
	FILE *fd;

	snprintf(path,PATHLENGTH,"%s%cwlc.config", wlcr_logs, DIR_SEP);
	fd = fopen(path,"r");
	if( fd == NULL)
		throw(MAL,"wlcr.getConfig","Could not access %s\n",path);
	while( fgets(path, PATHLENGTH, fd) ){
		path[strlen(path)-1] = 0;
		if( strncmp("logs=", path,5) == 0)
			wlcr_logs = GDKstrdup(path + 5);
		if( strncmp("snapshot=", path,9) == 0)
			wlcr_snapshot = GDKstrdup(path + 9);
		if( strncmp("firstbatch=", path,11) == 0)
			wlcr_firstbatch = atoi(path+ 11);
		if( strncmp("batches=", path, 8) == 0)
			wlcr_batches = atoi(path+ 8);
		if( strncmp("drift=", path, 6) == 0)
			wlcr_drift = atoi(path+ 6);
	}
	fclose(fd);
	return MAL_SUCCEED;
}

static 
str WLCsetConfig(void){
	char path[PATHLENGTH];
	FILE *fd;

	// default setting for the archive directory is the db itself
	if( wlcr_logs == NULL){
		snprintf(path,PATHLENGTH,"%s%c", wlcr_logs, DIR_SEP);
		wlcr_logs = GDKstrdup(path);
	}

	snprintf(path,PATHLENGTH,"%s%cwlc.config", wlcr_logs, DIR_SEP);
	fd = fopen(path,"w");
	if( fd == NULL)
		throw(MAL,"wlcr.setConfig","Could not access %s\n",path);
	if( wlcr_snapshot)
		fprintf(fd,"snapshot=%s\n", wlcr_snapshot);
	if( wlcr_logs)
		fprintf(fd,"logs=%s\n", wlcr_logs);
	fprintf(fd,"firstbatch=%d\n", wlcr_firstbatch);
	fprintf(fd,"batches=%d\n", wlcr_batches );
	fprintf(fd,"drift=%d\n", wlcr_drift );
	fclose(fd);
	return MAL_SUCCEED;
}

// creation of the logger file and updating the configuration file should be atomic !!!
// The log files are marked with the database name. This allows for easy recognition later on.
static str
WLCsetlogger(void)
{
	char path[PATHLENGTH];

	if( wlcr_logs == NULL)
		throw(MAL,"wlcr.setlogger","Path not initalized");
	MT_lock_set(&wlcr_lock);
	snprintf(path,PATHLENGTH,"%s%c%s_%012d", wlcr_logs, DIR_SEP, wlcr_dbname, wlcr_batches);
	wlcr_fd = open_wastream(path);
	if( wlcr_fd == 0){
		MT_lock_unset(&wlcr_lock);
		GDKerror("wlcr.logger:Could not create %s\n",path);
		throw(MAL,"wlcr.logger","Could not create %s\n",path);
	}

	wlcr_batches++;
	wlcr_tid = 0;
	wlcr_start = GDKms()/1000;
	WLCsetConfig();
	MT_lock_unset(&wlcr_lock);
	return MAL_SUCCEED;
}

static void
WLCcloselogger(void)
{
	if( wlcr_fd == NULL)
		return ;
	close_stream(wlcr_fd);
	wlcr_fd= NULL;
	wlcr_tid = 0;
	wlcr_start = 0;
	WLCsetConfig();
}

/*
 * The WLCRlogger process ensures that log files are properly closed
 * and released when their drift time window has expired.
 */

static MT_Id wlcr_logger;

static void
WLCRlogger(void *arg)
{
	(void) arg;
	while(1){
		if( wlcr_logs && wlcr_fd ){
			if (wlcr_start + wlcr_drift < GDKms() / 1000){
				MT_lock_set(&wlcr_lock);
				WLCcloselogger();
				MT_lock_unset(&wlcr_lock);
			}
			MT_sleep_ms( (wlcr_drift? wlcr_drift:1 ) * 1000);
		} else
		if( wlcr_drift)
				MT_sleep_ms( wlcr_drift * 1000);
		else
				MT_sleep_ms(  10  * 1000);
	}
}
/*
 * The existence of the master directory should be checked upon server restart.
 * Then the master record information should be set and the WLClogger started.
 */
str 
WLCinit(Client cntxt)
{
	char path[PATHLENGTH];
	str pathname, msg= MAL_SUCCEED;
	FILE *fd;

	if( wlcr_logs){
#ifdef _WLC_DEBUG_
		mnstr_printf(cntxt->fdout,"#WLC already running\n");
#else
	(void) cntxt;
#endif
	} else{
		// use default location for archive
		pathname = GDKfilepath(0,0,"master",0);
		snprintf(path, PATHLENGTH,"%s%cwlc.config", pathname, DIR_SEP);

		fd = fopen(path,"r");
		if( fd == NULL){	// not in master mode
			GDKfree(pathname);
			return MAL_SUCCEED;
		}
		fclose(fd);
		// we are in master mode
		wlcr_dbname = GDKgetenv("gdk_dbname");
		wlcr_logs = pathname;
		msg =  WLCgetConfig();
		if( msg)
			GDKerror("%s",msg);
		if (MT_create_thread(&wlcr_logger, WLCRlogger , (void*) 0, MT_THR_JOINABLE) < 0) {
                GDKerror("wlcr.logger thread could not be spawned");
        }
	}
	return MAL_SUCCEED;
}

str 
WLCexit(void)
{
 	// only keep non-empty log files 
	return MAL_SUCCEED;
}

str
WLCstopmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	if( wlcr_logs == NULL)
		throw(MAL,"wlcr.stopmaster","Replica control not active");
	wlcr_batches = - wlcr_batches;
	WLCsetConfig();

	return MAL_SUCCEED;
}

str 
WLCinitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCinit(cntxt);
}

str 
WLCthreshold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) cntxt;
	wlcr_threshold = * getArgReference_int(stk,pci,1);
	return MAL_SUCCEED;
}

str 
WLCdrift(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) cntxt;
	wlcr_drift = * getArgReference_int(stk,pci,1);
	return MAL_SUCCEED;
}

str 
WLCmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char path[PATHLENGTH];
	(void) stk;
	(void) pci;
	(void) cntxt;
	(void) mb;

	if ( wlcr_logs == NULL){
		wlcr_dbname = GDKgetenv("gdk_dbname");
		wlcr_logs = GDKfilepath(0,0,"master",0);
		snprintf(path, PATHLENGTH,"%s%c%s_wlcr", wlcr_logs, DIR_SEP, wlcr_dbname);
		if( GDKcreatedir(path) == GDK_FAIL){
			wlcr_dbname = NULL;
			GDKfree(wlcr_logs);
			wlcr_logs = NULL;
			throw(SQL,"wlcr.master","Could not create %s\n", wlcr_logs);
		}
		WLCsetConfig();
	} else
		WLCgetConfig();
#ifdef _WLC_DEBUG_
	mnstr_printf(cntxt->fdout,"#master batches %d file open %d\n", wlcr_batches,  wlcr_fd != NULL);
#endif
	return MAL_SUCCEED;
}

static InstrPtr
WLCsettime(Client cntxt, InstrPtr pci, InstrPtr p)
{
	struct timeval clock;
	time_t clk ;
	struct tm ctm;

	(void) pci;
	gettimeofday(&clock,NULL);
	clk = clock.tv_sec;
	ctm = *localtime(&clk);
	strftime(wlcr_time, 26, "%Y-%m-%dT%H:%M:%S",&ctm);
	return pushStr(cntxt->wlcr, p, wlcr_time);
}

#define WLCstart(P)\
{ Symbol s; \
	if( cntxt->wlcr == NULL){\
		s = newSymbol("wlrc", FUNCTIONsymbol);\
		cntxt->wlcr_kind = WLCR_QUERY;\
		cntxt->wlcr = s->def;\
		s->def = NULL;\
	} \
	if( cntxt->wlcr->stop == 0){\
		P = newStmt(cntxt->wlcr,"wlr","job");\
		P = pushStr(cntxt->wlcr, P, cntxt->username);\
		P = pushInt(cntxt->wlcr, P, wlcr_tid);\
		P = WLCsettime(cntxt,pci, P); \
		P->ticks = GDKms();\
}	}

str
WLCjob(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tid = *getArgReference_int(stk,pci,1);

	(void) cntxt;
	(void) mb;

	if ( tid < wlcr_tid)
		throw(MAL,"wlcr.job","Work unit identifier is before last one executed");
	return MAL_SUCCEED;
}

str
WLCexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
WLCquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;

	(void) stk;
	if ( strcmp("-- no query",getVarConstant(mb, getArg(pci,1)).val.sval) == 0)
		return MAL_SUCCEED;	// ignore system internal queries.
	WLCstart(p);
	p = newStmt(cntxt->wlcr, "wlr","query");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p->ticks = GDKms();
	return MAL_SUCCEED;
}

str
WLCgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;
	int i, tpe, varid;
	(void) stk;
	
	WLCstart(p);
	p = newStmt(cntxt->wlcr, "wlr",getFunctionId(pci));
	for( i = pci->retc; i< pci->argc; i++){
		tpe =getArgType(mb, pci, i);
		switch(tpe){
		case TYPE_str:
			p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci, i)).val.sval);
			break;
		default:
			varid = defConstant(cntxt->wlcr, tpe, getArgReference(stk, pci, i));
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}
	p->ticks = GDKms();
	cntxt->wlcr_kind = WLCR_CATALOG;
	return MAL_SUCCEED;
}

#define bulk(TPE1, TPE2)\
{	TPE1 *p = (TPE1 *) Tloc(b,0);\
	TPE1 *q = (TPE1 *) Tloc(b, BUNlast(b));\
	int k=0; \
	for( ; p < q; p++, k++){\
		if( k % 32 == 31){\
			pci = newStmt(cntxt->wlcr, "wlr",getFunctionId(pci));\
			pci = pushStr(cntxt->wlcr, pci, sch);\
			pci = pushStr(cntxt->wlcr, pci, tbl);\
			pci = pushStr(cntxt->wlcr, pci, col);\
			pci->ticks = GDKms();\
		}\
		pci = push##TPE2(cntxt->wlcr, pci ,*p);\
} }

#define updateBatch(TPE1,TPE2)\
{	TPE1 *x = (TPE1 *) Tloc(bval,0);\
	TPE1 *y = (TPE1 *) Tloc(bval, BUNlast(b));\
	int k=0; \
	for( ; x < y; x++, k++){\
		p = newStmt(cntxt->wlcr, "wlr","update");\
		p = pushStr(cntxt->wlcr, p, sch);\
		p = pushStr(cntxt->wlcr, p, tbl);\
		p = pushStr(cntxt->wlcr, p, col);\
		p = pushOid(cntxt->wlcr, p,  (ol? *ol++: o++));\
		p = push##TPE2(cntxt->wlcr, p ,*x);\
} }

static void
WLCdatashipping(Client cntxt, MalBlkPtr mb, InstrPtr pci, int bid)
{	BAT *b;
	str sch,tbl,col;
	(void) mb;
	b= BATdescriptor(bid);
	assert(b);

// large BATs can also be re-created using the query.
// Copy into should always be expanded, because the source may not
// be accessible in the replica. TODO

	sch = GDKstrdup(getVarConstant(cntxt->wlcr, getArg(pci,1)).val.sval);
	tbl = GDKstrdup(getVarConstant(cntxt->wlcr, getArg(pci,2)).val.sval);
	col = GDKstrdup(getVarConstant(cntxt->wlcr, getArg(pci,3)).val.sval);
	if (cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;
	switch( ATOMstorage(b->ttype)){
	case TYPE_bit: bulk(bit,Bit); break;
	case TYPE_bte: bulk(bte,Bte); break;
	case TYPE_sht: bulk(sht,Sht); break;
	case TYPE_int: bulk(int,Int); break;
	case TYPE_lng: bulk(lng,Lng); break;
	case TYPE_flt: bulk(flt,Flt); break;
	case TYPE_dbl: bulk(dbl,Dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: bulk(hge,Hge); break;
#endif
	case TYPE_str: 
		{	BATiter bi;
			BUN p,q;
			int k=0; 
			bi= bat_iterator(b);
			BATloop(b,p,q){
				if( k % 32 == 31){
					pci = newStmt(cntxt->wlcr, "wlr",getFunctionId(pci));
					pci = pushStr(cntxt->wlcr, pci, sch);
					pci = pushStr(cntxt->wlcr, pci, tbl);
					pci = pushStr(cntxt->wlcr, pci, col);
				}
				k++;
				pci = pushStr(cntxt->wlcr, pci ,(str) BUNtail(bi,p));
		} }
		break;
	default:
		cntxt->wlcr_kind = WLCR_CATALOG;
	}
	GDKfree(sch);
	GDKfree(tbl);
	GDKfree(col);
}

str
WLCappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	 InstrPtr p;
	int tpe, varid;
	(void) stk;
	(void) mb;
	
	WLCstart(p);
	p = newStmt(cntxt->wlcr, "wlr","append");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,3)).val.sval);

	// extend the instructions with all values. 
	// If this become too large we can always switch to a "catalog" mode
	// forcing re-execution instead
	tpe= getArgType(mb,pci,4);
	if (isaBatType(tpe) ){
		// actually check the size of the BAT first, most have few elements
		WLCdatashipping(cntxt, mb, p, stk->stk[getArg(pci,4)].val.bval);
	} else {
		ValRecord cst;
		if (VALcopy(&cst, getArgReference(stk,pci,4)) != NULL){
			varid = defConstant(cntxt->wlcr, tpe, &cst);
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}
	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;
	
	return MAL_SUCCEED;
}

str
WLCdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;	
	int tpe, varid;
	(void) stk;
	(void) mb;
	
	WLCstart(p);
	p = newStmt(cntxt->wlcr, "wlr","delete");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,3)).val.sval);
	
	tpe= getArgType(mb,pci,4);
	if (isaBatType(tpe) ){
		WLCdatashipping(cntxt, mb, p, stk->stk[getArg(pci,4)].val.bval);
	} else {
		ValRecord cst;
		if (VALcopy(&cst, getArgReference(stk,pci,4)) != NULL){
			varid = defConstant(cntxt->wlcr, tpe, &cst);
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}
	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;

	return MAL_SUCCEED;
}

str
WLCupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;
	str sch,tbl,col;
	ValRecord cst;
	int tpe, varid;
	oid o = 0, *ol = 0;
	
	sch = *getArgReference_str(stk,pci,1);
	tbl = *getArgReference_str(stk,pci,2);
	col = *getArgReference_str(stk,pci,3);
	WLCstart(p);
	tpe= getArgType(mb,pci,5);
	if (isaBatType(tpe) ){
		BAT *b, *bval;
		b= BATdescriptor(stk->stk[getArg(pci,4)].val.bval);
		assert(b);
		bval= BATdescriptor(stk->stk[getArg(pci,5)].val.bval);
		assert(bval);
		if( b->ttype == TYPE_void)
			o = b->tseqbase;
		else
			ol = (oid*) Tloc(b,0);
		switch( ATOMstorage(bval->ttype)){
		case TYPE_bit: updateBatch(bit,Bit); break;
		case TYPE_bte: updateBatch(bte,Bte); break;
		case TYPE_sht: updateBatch(sht,Sht); break;
		case TYPE_int: updateBatch(int,Int); break;
		case TYPE_lng: updateBatch(lng,Lng); break;
		case TYPE_flt: updateBatch(flt,Flt); break;
		case TYPE_dbl: updateBatch(dbl,Dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: updateBatch(hge,Hge); break;
#endif
		case TYPE_str:
		{	BATiter bi;
			int k=0; 
			BUN x,y;
			bi = bat_iterator(bval);
			BATloop(bval,x,y){
				p = newStmt(cntxt->wlcr, "wlr","update");
				p = pushStr(cntxt->wlcr, p, sch);
				p = pushStr(cntxt->wlcr, p, tbl);
				p = pushStr(cntxt->wlcr, p, col);
				p = pushOid(cntxt->wlcr, p, (ol? *ol++ : o++));
				p = pushStr(cntxt->wlcr, p , BUNtail(bi,x));
				k++;
		} }
		default:
			cntxt->wlcr_kind = WLCR_CATALOG;
		}
	} else {
		p = newStmt(cntxt->wlcr, "wlr","update");
		p = pushStr(cntxt->wlcr, p, sch);
		p = pushStr(cntxt->wlcr, p, tbl);
		p = pushStr(cntxt->wlcr, p, col);
		o = *getArgReference_oid(stk,pci,4);
		p = pushOid(cntxt->wlcr,p, o);
		if (VALcopy(&cst, getArgReference(stk,pci,5)) != NULL){
			varid = defConstant(cntxt->wlcr, tpe, &cst);
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}

	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;

	return MAL_SUCCEED;
}

str
WLCclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	 InstrPtr p;
	(void) stk;
	
	WLCstart(p);
	p = newStmt(cntxt->wlcr, "wlr","clear_table");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;

	return MAL_SUCCEED;
}

static str
WLCwrite(Client cntxt, str kind)
{	str msg = MAL_SUCCEED;
	InstrPtr p;
	// save the wlcr record on a file 
	if( cntxt->wlcr == 0 || cntxt->wlcr->stop == 0)
		return MAL_SUCCEED;

	if( wlcr_logs ){	
		if( wlcr_start + wlcr_drift < GDKms()/1000)
			WLCcloselogger();

		if (wlcr_fd == NULL){
			msg = WLCsetlogger();
			if( msg) 
				return msg;
		}
		
		if ( wlcr_fd == NULL)
			throw(MAL,"wlcr.write","WLC log file not accessible");
		else {


			newStmt(cntxt->wlcr,"wlr","exec");
			wlcr_tid++;
			MT_lock_set(&wlcr_lock);
			p = getInstrPtr(cntxt->wlcr,0);
			p = pushStr(cntxt->wlcr,p,kind);
			p = pushStr(cntxt->wlcr, p, wlcr_name[cntxt->wlcr_kind]);
			p = pushLng(cntxt->wlcr,p, GDKms() - p->ticks);
			printFunction(wlcr_fd, cntxt->wlcr, 0, LIST_MAL_DEBUG );
			(void) mnstr_flush(wlcr_fd);
			if( wlcr_drift == 0 || wlcr_start + wlcr_drift < GDKms()/1000)
				WLCcloselogger();

			MT_lock_unset(&wlcr_lock);
			trimMalVariables(cntxt->wlcr, NULL);
			resetMalBlk(cntxt->wlcr, 0);
			cntxt->wlcr_kind = WLCR_QUERY;
		}
	} else
			throw(MAL,"wlcr.write","WLC log path missing ");

#ifdef _WLC_DEBUG_
	printFunction(cntxt->fdout, cntxt->wlcr, 0, LIST_MAL_ALL );
#endif
	return msg;
}

str
WLCcommit(int clientid)
{
		return WLCwrite( &mal_clients[clientid], "commit");
}

str
WLCcommitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCcommit(cntxt->idx);
}

str
WLCrollback(int clientid)
{
	return WLCwrite( &mal_clients[clientid], "rollback");
}
str
WLCrollbackCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCrollback(cntxt->idx);
}
