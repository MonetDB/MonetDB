/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * A master can be replicated by taking a binary copy of the 'bat' directory
 * when in quiescent mode or a more formal snapshot..
 * Alternatively you start with an empty database.
 * 
 * After restart of a mserver against the newly created image,
 * the log files from the master are processed by calling....
 *
 * In replay mode also all queries are executed if they surpass
 * the latest threshold set for by the master.
 */
#include "monetdb_config.h"
#include "sql.h"
#include "wlc.h"
#include "wlr.h"
#include "sql_scenario.h"
#include "sql_execute.h"
#include "opt_prelude.h"
#include "mal_parser.h"
#include "mal_client.h"
#include "mal_authorize.h"
#include "querylog.h"

#define WLR_WAIT 0
#define WLR_RUN   101
#define WLR_PAUSE 201
#define WLR_STOP 301

#define WLC_COMMIT 40
#define WLC_ROLLBACK 50
#define WLC_ERROR 60

#define _WLR_DEBUG_
  
MT_Lock     wlr_lock = MT_LOCK_INITIALIZER("wlr_lock");

/* The current status of the replica processing.
 * It is based on the assumption that at most one replica thread is running
 * importing data from a single master.
 */
static char wlr_master[IDLENGTH];
static char wlr_error[FILENAME_MAX];	// errors should stop the process
static int	wlr_batches; 				// the next file to be processed
static lng 	wlr_tag;					// the next transaction id to be processed
static lng 	wlr_limit = -1;				// stop re-processing transactions when limit is reached
static char wlr_timelimit[26];			// stop re-processing transactions when time limit is reached
static char wlr_read[26];				// stop re-processing transactions when time limit is reached
static int 	wlr_state = WLR_WAIT;		// which state WAIT/PAUSE/RUN
static int 	wlr_beat;					// period between successive synchronisations with master
static MT_Id wlr_thread = 0;			// The single replicator thread

#define MAXLINE 2048

/* Simple read the replica configuration status file */
static int
WLRgetConfig(void){
	char *path;
	char line[MAXLINE];
	FILE *fd;
	int len;

	if((path = GDKfilepath(0, 0, "wlr.config", 0)) == NULL){
		fprintf(stderr, "wlr.getConfig:Could not create wlr.config file path\n");
		return -1;
	}
	fd = fopen(path,"r");
	GDKfree(path);
	if( fd == NULL){
		// during start of the replicator it need not be there
		return 1;
	}
	while( fgets(line, MAXLINE, fd) ){
		line[strlen(line)-1]= 0;
#ifdef _WLR_DEBUG_
		fprintf(stderr,"#WLRgetConfig %s\n", line);
#endif
		if( strncmp("master=", line,7) == 0) {
			len = snprintf(wlr_master, IDLENGTH, "%s", line + 7);
			if (len == -1 || len >= IDLENGTH) {
				fprintf(stderr, "wlr.getConfig:master config value is too large");
				goto bailout;
			} else
			if (len  == 0) {
				fprintf(stderr, "wlr.getConfig:master config path missing");
				goto bailout;
			}
		} else
		if( strncmp("batches=", line, 8) == 0)
			wlr_batches = atoi(line+ 8);
		else
		if( strncmp("tag=", line, 4) == 0)
			wlr_tag = atoi(line+ 4);
		else
		if( strncmp("beat=", line, 5) == 0)
			wlr_beat = atoi(line+ 5);
		else
		if( strncmp("limit=", line, 6) == 0)
			wlr_limit = atol(line+ 6);
		else
		if( strncmp("timelimit=", line, 10) == 0)
			strcpy(wlr_timelimit, line + 10);
		else
		if( strncmp("error=", line, 6) == 0) {
			len = snprintf(wlr_error, FILENAME_MAX, "%s", line + 6);
			if (len == -1 || len >= FILENAME_MAX) {
				fprintf(stderr, "wlr.getConfig:error config value is too large");
				goto bailout;
			}
		} else{
				fprintf(stderr, "wlr.getConfig:unknown configuration item %s", line);
				goto bailout;
		}
	}
	return 0;
bailout:
	fclose(fd);
	return -1;
}

/* Keep the current status in the configuration status file */
static void
WLRputConfig(void){
	char *path;
	stream *fd;

	if((path = GDKfilepath(0,0,"wlr.config",0)) == NULL){
		fprintf(stderr,"wlr.setMaster:Could not access wlr.config file\n");
		return ;
	}
	fd = open_wastream(path);
	GDKfree(path);
	if( fd == NULL){
		fprintf(stderr,"wlr.setMaster:Could not create wlr.config file\n");
		return;
	}

	mnstr_printf(fd,"master=%s\n", wlr_master);
	mnstr_printf(fd,"batches=%d\n", wlr_batches);
	mnstr_printf(fd,"tag="LLFMT"\n", wlr_tag);
	mnstr_printf(fd,"limit="LLFMT"\n", wlr_limit);
	mnstr_printf(fd,"beat=%d\n", wlr_beat);
	if( wlr_timelimit[0])
		mnstr_printf(fd,"timelimit=%s\n", wlr_timelimit);
	if( wlr_error[0])
		mnstr_printf(fd,"error=%s\n", wlr_error);
	close_stream(fd);
}

/*
 * When the master database exist, we should set the replica administration.
 * But only once.
 *
 * The log files are identified by a range. It starts with 0 when an empty database
 * was used to bootstrap. Otherwise it is the range received from the dbmaster.
 * At any time we should be able to restart the synchronization
 * process by grabbing a new set of log files.
 * This calls for keeping track in the replica what log files have been applied
 * and what the last completed transaction was.
 *
 * Given that the replication thread runs independently, all errors encountered
 * should be sent to the system logging system.
 */
static str
WLRgetMaster(void)
{
	char path[FILENAME_MAX];
	int len;
	str dir;
	FILE *fd;

	if( wlr_master[0] == 0 )
		return MAL_SUCCEED;

	/* collect master properties */
	len = snprintf(path, FILENAME_MAX, "..%c%s", DIR_SEP, wlr_master);
	if (len == -1 || len >= FILENAME_MAX)
		throw(MAL, "wlr.getMaster", "wlc.config filename path is too large");
	if((dir = GDKfilepath(0,path,"wlc.config",0)) == NULL)
		throw(MAL,"wlr.getMaster","Could not access wlc.config file\n");

	fd = fopen(dir,"r");
	GDKfree(dir);
	if( fd ){
		WLCreadConfig(fd);
		if( ! wlr_master[0] )
			throw(MAL,"wlr.getMaster","Master not identified\n");
		wlc_state = WLC_CLONE; // not used as master
	} else
		throw(MAL,"wlr.getMaster","Could not access wlc.config file\n");
	return MAL_SUCCEED;
}

/* each WLR block is turned into a separate MAL block and executed
 * This block is re-used as we consider the complete file.
 */

#define cleanup(){\
	resetMalBlkAndFreeInstructions(mb, 1);\
	trimMalVariables(mb, NULL);\
	pc = 0;}

static void
WLRprocessBatch(void *arg)
{
	Client cntxt = (Client) arg;
	int i, pc, len;
	char path[FILENAME_MAX];
	stream *fd = NULL;
	Client c;
	size_t sz;
	MalBlkPtr mb;
	InstrPtr q;
	str msg, other;
	mvc *sql;
	lng currid =0;
	Symbol prev = NULL;

	c =MCforkClient(cntxt);
	if( c == 0){
		fprintf(stderr, "#Could not create user for WLR process\n");
		return;
	}
	c->promptlength = 0;
	c->listing = 0;
	c->fdout = open_wastream(".wlr");
	if(c->fdout == NULL) {
		MCcloseClient(c);
		fprintf(stderr, "#Could not create user for WLR process\n");
		return;
	}

	/* Cook a log file into a concreate MAL function for multiple transactions */
	prev = newFunction(putName("user"), putName("wlr"), FUNCTIONsymbol);
	if(prev == NULL) {
		MCcloseClient(c);
		fprintf(stderr, "#Could not create user for WLR process\n");
		return;
	}
	c->curprg = prev;
	mb = c->curprg->def;
	setVarType(mb, 0, TYPE_void);

	msg = SQLinitClient(c);
	if( msg != MAL_SUCCEED)
		fprintf(stderr,"#Failed to initialize the client\n");
	msg = getSQLContext(c, mb, &sql, NULL);
	if( msg)
		fprintf(stderr,"#Failed to access the transaction context: %s\n",msg);
	if ((msg = checkSQLContext(c)) != NULL)
		fprintf(stderr,"#Inconsistent SQL context: %s\n",msg);

#ifdef _WLR_DEBUG_
	mnstr_printf(c->fdout,"#Ready to start the replay against batches %d:%d\n", wlr_batches, wlc_batches );
#endif
	path[0]=0;
	for( i= wlr_batches; wlr_state == WLR_RUN && i < wlc_batches && ! GDKexiting(); i++){
		len = snprintf(path,FILENAME_MAX,"%s%c%s_%012d", wlc_dir, DIR_SEP, wlr_master, i);
		if (len == -1 || len >= FILENAME_MAX) {
			fprintf(stderr,"#wlr.process: filename path is too large\n");
			continue;
		}
		fd= open_rastream(path);
		if( fd == NULL){
			fprintf(stderr,"#wlr.process:'%s' can not be accessed \n",path);
			// Be careful not to miss log files.
			continue;
		}
		sz = getFileSize(fd);
		if (sz > (size_t) 1 << 29) {
			close_stream(fd);
			fprintf(stderr, "#wlr.process File %s too large to process", path);
			continue;
		}
		if((c->fdin = bstream_create(fd, sz == 0 ? (size_t) (2 * 128 * BLOCK) : sz)) == NULL) {
			close_stream(fd);
			fprintf(stderr, "#wlr.process Failed to open stream for file %s", path);
			continue;
		}
		if (bstream_next(c->fdin) < 0)
			fprintf(stderr, "!WARNING: could not read %s\n", path);

		c->yycur = 0;
#ifdef _WLR_DEBUG_
		mnstr_printf(cntxt->fdout,"#replay log file:%s\n",path);
#endif

		// now parse the file line by line to reconstruct the WLR blocks
		do{
			pc = mb->stop;
			parseMAL(c, c->curprg, 1, 1);
			mb = c->curprg->def; // needed
			if( mb->errors){
				char line[FILENAME_MAX];
				snprintf(line, FILENAME_MAX,"#wlr.process:failed further parsing '%s':\n",path);
				snprintf(wlr_error, FILENAME_MAX, "%.*s", FILENAME_MAX, line);
				fprintf(stderr,"%s",line);
				printFunction(GDKerr, mb, 0, LIST_MAL_DEBUG );
			}
			q= getInstrPtr(mb, mb->stop-1);
#ifdef _WLR_DEBUG_
			mnstr_printf(cntxt->fdout,"#redo wlr_tag %ld wlr_limit %ld\n", getVarConstant(mb, getArg(q,1)).val.lval, wlr_limit);
#endif
			if( getModuleId(q) == wlrRef && getFunctionId(q) == transactionRef && (currid = getVarConstant(mb, getArg(q,1)).val.lval) < wlr_tag){
				/* skip already executed transaction log */
			} else
			if( getModuleId(q) == wlrRef && getFunctionId(q) == transactionRef &&
				( ( (currid = getVarConstant(mb, getArg(q,1)).val.lval) >= wlr_limit && wlr_limit != -1) ||
				  ( wlr_timelimit[0] && strcmp(getVarConstant(mb, getArg(q,2)).val.sval, wlr_timelimit) >= 0))
					){
				/* stop execution of the transactions if your reached the limit */
				resetMalBlkAndFreeInstructions(mb, 1);
				trimMalVariables(mb, NULL);
				bstream_destroy(c->fdin);
#ifdef _WLR_DEBUG_
				mnstr_printf(cntxt->fdout,"Found final transaction\n");
#endif
				goto wrapup;
			} else
			if( getModuleId(q) == wlrRef && getFunctionId(q) == transactionRef ){
				snprintf(wlr_read, sizeof(wlr_read), "%s", getVarConstant(mb, getArg(q,2)).val.sval);
				wlr_tag = getVarConstant(mb, getArg(q,1)).val.lval;
#ifdef _WLR_DEBUG_
				mnstr_printf(cntxt->fdout,"#run against tlimit %s  tag %s\n", wlr_timelimit, wlr_read);
#endif
			}
			// only re-execute successful transactions.
			if ( getModuleId(q) == wlrRef && getFunctionId(q) ==commitRef ){
				pushEndInstruction(mb);
				// execute this block if no errors are found
				chkTypes(c->usermodule, mb, FALSE);
				chkFlow(mb);
				chkDeclarations(mb);

				if( mb->errors == 0){
					sql->session->auto_commit = 0;
					sql->session->ac_on_commit = 1;
					sql->session->level = 0;
					if(mvc_trans(sql) < 0) {
						fprintf(stderr,"#Allocation failure while starting the transaction \n");
					} else {
#ifdef _WLR_DEBUG_
						fprintf(stderr,"#process a transaction\n");
						printFunction(GDKerr, mb, 0, LIST_MAL_DEBUG );
#endif
						msg= runMAL(c,mb,0,0);
						wlr_tag++;
						if( msg == MAL_SUCCEED)
							/* at this point we have updated the replica, but the configuration has not been changed.
							 * If at this point an error occurs, we could redo the same transaction twice later on.
							 * The solution is to make sure that we recognize that a transaction has started and is completed successfully
							 */
							WLRputConfig();
						// ignore warnings
						if (msg && strstr(msg,"WARNING"))
							msg = MAL_SUCCEED;
						if( msg != MAL_SUCCEED){
							// they should always succeed
							fprintf(stderr,"#ERROR in wlr processing batch %d :%s\n", i, msg);
							printFunction(GDKerr, mb, 0, LIST_MAL_DEBUG );
							if((other = mvc_rollback(sql,0,NULL, false)) != MAL_SUCCEED) //an error was already established
								GDKfree(other);
							cleanup();
						} else
						if((msg = mvc_commit(sql, 0, 0, false)) != MAL_SUCCEED) {
							fprintf(stderr,"#wlr.process transaction commit failed: %s\n", msg);
							freeException(msg);
						}
					}
				} else {
					char line[FILENAME_MAX];
					snprintf(line, FILENAME_MAX,"#wlr.process:typechecking failed '%s':\n",path);
					snprintf(wlr_error, FILENAME_MAX, "%s", line);
					fprintf(stderr,"%s",line);
					printFunction(GDKerr, mb, 0, LIST_MAL_DEBUG );
				}
				cleanup();
			} else
			if ( getModuleId(q) == wlrRef && getFunctionId(q) == rollbackRef ){
				cleanup();
			}
		} while( mb->errors == 0 && pc != mb->stop);
#ifdef _WLR_DEBUG_
		mnstr_printf(c->fdout,"#wlr.process:processed log file '%s'\n",path);
#endif
		// skip to next file when all is read
		wlr_batches++;
		WLRputConfig();
		// stop when we are about to read beyond the limited transaction (timestamp)
		if( (wlr_limit != -1 || (wlr_timelimit[0] && wlr_read[0] && strncmp(wlr_read,wlr_timelimit,sizeof(wlr_timelimit))>= 0) )  && wlr_limit <= wlr_tag) {
			bstream_destroy(c->fdin);
			break;
		}
		bstream_destroy(c->fdin);
	}
wrapup:
	(void) mnstr_flush(c->fdout);
	close_stream(c->fdout);
	SQLexitClient(c);
	MCcloseClient(c);
	if(prev)
		freeSymbol(prev);
}

/*
 *  A single WLR thread is allowed to run in the background.
 *  If it happens to crash then replication roll forward is suspended.
 *  Moreover, the background job can only leave error messages in the merovingian log.
 *
 * A timing issue.
 * The WLRprocess can only start after an SQL environment has been initialized.
 * It is therefore initialized when a SQLclient() is issued.
 */
static void
WLRprocessScheduler(void *arg)
{	Client cntxt = (Client) arg;
	int i, duration;
	struct timeval clock;
	time_t clk;
	struct tm ctm;
	char clktxt[26];

	if( ( i = WLRgetConfig()) ){
		if ( i> 0) 
			WLRputConfig();
		else return;
	}
	assert(wlr_master[0]);
	cntxt = MCinitClient(MAL_ADMIN, NULL,NULL);
	wlr_state = WLR_PAUSE;	// ready to accept commands
#ifdef _WLR_DEBUG_
		mnstr_printf(cntxt->fdout, "#Run the replicator %d %d\n", GDKexiting(),  wlr_state);
#endif
	while( wlr_state != WLR_STOP ){
		// wait at most for the cycle period, also at start
		if ( wlr_state != WLR_RUN){
			MT_thread_setworking("sleeping");
			duration = (wlc_beat? wlc_beat:1) * 1000 ;
			if( wlr_timelimit[0]){
				gettimeofday(&clock, NULL);
				clk = clock.tv_sec;
#ifdef HAVE_LOCALTIME_R
				(void) localtime_r(&clk, &ctm);
#else
				ctm = *localtime(&clk);
#endif

				strftime(clktxt, sizeof(clktxt), "%Y-%m-%dT%H:%M:%S.000",&ctm);
#ifdef _WLR_DEBUG_
				fprintf(stderr,"#now %s tlimit %s\n",clktxt, wlr_timelimit);
#endif
				// actually never wait longer then the timelimit requires
				// preference is given to the beat.
				if(strncmp(clktxt, wlr_timelimit,sizeof(wlr_timelimit)) >= 0) 
					MT_sleep_ms(duration);
			} else
				for( ; duration > 0  && wlr_state == WLR_PAUSE; duration -= 20){
					MT_sleep_ms( 20);
				}
		}
		if( ( (wlr_batches == wlc_batches && wlr_tag < wlr_limit) || wlr_limit > wlr_tag  ||
			  (wlr_limit == -1 && wlr_timelimit[0] == 0 && wlr_batches < wlc_batches) ||
			  (wlr_timelimit[0]  && strncmp(clktxt, wlr_timelimit, sizeof(wlr_timelimit))> 0)  ) ) {
			MT_thread_setworking("processing");
			WLRprocessBatch(cntxt);
			wlr_state =  WLR_PAUSE;
		}
		/* Can not use GDKexiting(), because a test may already reach that point before it did anything.
		 * Instead wait for the explicit WLR_STOP
		 */
		if( GDKexiting()){
#ifdef _WLR_DEBUG_
				mnstr_printf(cntxt->fdout, "#Replicator thread stopped due to GDKexiting()\n");
#endif
			break;
		}
	}
	wlr_state = WLR_WAIT;
	wlr_thread = 0;
#ifdef _WLR_DEBUG_
	mnstr_printf(cntxt->fdout, "#Replicator thread is accidentally stopped\n");
#endif
}

// The replicate() command can be issued at the SQL console
// which can accept exceptions
str
WLRreplicate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str timelimit  = wlr_timelimit;
	size_t size = sizeof(wlr_timelimit);
	struct timeval clock;
	time_t clk;
	struct tm ctm;
	char clktxt[26];
	int duration = 2000;
	str msg = MAL_SUCCEED;

#ifdef _WLR_DEBUG_
	mnstr_printf(cntxt->fdout, "#replicate: start call wlr_limit = "LLFMT" (tag "LLFMT") time %s\n", wlr_limit, wlr_tag, wlr_timelimit);
#endif

	// first stop the background process
	if( WLRgetConfig())
		throw(MAL, "sql.replicate", "No replication configuration");

	if( getArgType(mb, pci, 1) == TYPE_str)
		msg = WLRstartreplicate(cntxt, mb, stk, pci);
	
	if( getArgType(mb, pci, 1) == TYPE_timestamp){
		if (timestamp_precision_tostr(&timelimit, &size, *getArgReference_TYPE(stk, pci, 1, timestamp), 3, true) < 0)
			throw(SQL, "wlr.replicate", GDK_EXCEPTION);
		mnstr_printf(cntxt->fdout,"#time limit %s\n",timelimit);
	} else
	if( getArgType(mb, pci, 1) == TYPE_bte)
		wlr_limit = getVarConstant(mb,getArg(pci,1)).val.btval;
	else
	if( getArgType(mb, pci, 1) == TYPE_sht)
		wlr_limit = getVarConstant(mb,getArg(pci,1)).val.shval;
	else
	if( getArgType(mb, pci, 1) == TYPE_int)
		wlr_limit = getVarConstant(mb,getArg(pci,1)).val.ival;
	else
	if( getArgType(mb, pci, 1) == TYPE_lng)
		wlr_limit = getVarConstant(mb,getArg(pci,1)).val.lval;
	WLRputConfig();
#ifdef _WLR_DEBUG_
	mnstr_printf(cntxt->fdout, "#replicate: wait until wlr_limit = "LLFMT" (tag "LLFMT") time %s\n", wlr_limit, wlr_tag, wlr_timelimit);
#endif
	if ( wlr_limit == -1 ){
#ifdef _WLR_DEBUG_
		mnstr_printf(cntxt->fdout, "#replicate terminated \n");
#endif
		return MAL_SUCCEED;
	}

	// the client thread should wait for the replicator to its job
	gettimeofday(&clock, NULL);
	clk = clock.tv_sec;
#ifdef HAVE_LOCALTIME_R
	(void) localtime_r(&clk, &ctm);
#else
	ctm = *localtime(&clk);
#endif

	strftime(clktxt, sizeof(clktxt), "%Y-%m-%dT%H:%M:%S.000",&ctm);
	while ( (wlr_batches == wlc_batches && wlr_tag < wlr_limit) || wlr_limit > wlr_tag  ||
			  (wlr_timelimit[0]  && strncmp(clktxt, wlr_timelimit, sizeof(wlr_timelimit))> 0)  ) {
		if ( !wlr_thread )
			throw(SQL,"wlr.startreplicate",SQLSTATE(42000) "Replicator terminated prematurely ");
		MT_sleep_ms( 20);
		duration -= 20;
		if ( duration < 0)
			throw(SQL,"wlr.startreplicate",SQLSTATE(42000) "Timeout to wait for replicator ");
	}
#ifdef _WLR_DEBUG_
	mnstr_printf(cntxt->fdout, "#replicate finished "LLFMT" (tag "LLFMT")\n", wlr_limit, wlr_tag);
#endif
	return msg;
}

/* watch out, each log record can contain multiple transaction COMMIT/ROLLBACKs
 * This means the wlc_kind can not be set to the last one.
 */
str
WLRtransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;
	int i;

	(void) cntxt;
	(void) pci;
	(void) stk;
	cntxt->wlc_kind = 0;
	if( wlr_error[0]){
		cntxt->wlc_kind = WLC_ERROR;
		return MAL_SUCCEED;
	}
	for( i = mb->stop-1; cntxt->wlc_kind == 0 && i > 1; i--){
		p = getInstrPtr(mb,i);
		if( getModuleId(p) == wlrRef && getFunctionId(p)== commitRef) 
			cntxt->wlc_kind = WLC_COMMIT;
		if( getModuleId(p) == wlrRef && getFunctionId(p)== rollbackRef)
			cntxt->wlc_kind = WLC_ROLLBACK;
	}
	return MAL_SUCCEED;
}


/* the Configuration is shared with the Logger thread, so protect its access */
str
WLRstartreplicate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int len;

	(void) mb;
	if( wlr_thread)
		throw(MAL, "wlr.replicate", SQLSTATE(42000) "replicator thread already running for %s ", wlr_master);

	len = snprintf(wlr_master, IDLENGTH, "%s", *getArgReference_str(stk, pci, 1));
	if (len == -1 || len >= IDLENGTH)
		throw(MAL, "wlr.replicate", SQLSTATE(42000) "Input value is too large for wlr_master buffer");
	if( WLRgetMaster())
		throw(MAL, "wlr.replicate", SQLSTATE(42000) "Can not access the master configuration record ");

	MT_lock_set(&wlr_lock);	 // avoid creation of multiple replicators in parallel.
	
	// time the consolidation process in the background
	if (MT_create_thread(&wlr_thread, WLRprocessScheduler, (void*) NULL,
			     MT_THR_DETACHED, "WLRprocSched") < 0) {
			MT_lock_unset(&wlr_lock);
			throw(SQL,"wlr.init",SQLSTATE(42000) "Starting wlr manager failed");
	}
#ifdef _WLR_DEBUG_
	mnstr_printf(cntxt->fdout,"#WLR scheduler forked\n");
#else
	(void) cntxt;
#endif
	MT_lock_unset(&wlr_lock);
	return MAL_SUCCEED;
}

str
WLRstopreplicate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	// kill the replicator thread and reset for a new one
	wlr_state =  WLR_STOP;
	return MAL_SUCCEED;
}

str
WLRgetreplicaclock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = getArgReference_str(stk,pci,0);
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	if( WLRgetConfig())
		return msg;
	if( wlr_read[0])
		*ret= GDKstrdup(wlr_read);
	else *ret= GDKstrdup(str_nil);
	if (*ret == NULL)
		throw(MAL, "wlr.getreplicaclock", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return msg;
}

str
WLRgetreplicatick(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *ret = getArgReference_lng(stk,pci,0);
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	if( WLRgetConfig())
		return msg;
	*ret = wlr_tag;
	return msg;
}

/* the replica cycle can be set to fixed interval.
 * This allows for ensuring an up to date version every N seconds
 */
str
WLRsetreplicabeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int new;
	(void) cntxt;
	(void) mb;
	new = *getArgReference_int(stk,pci,1);
	if ( new < wlc_beat || new < 1)
		throw(SQL,"replicatebeat",SQLSTATE(42000) "Cycle time should be larger then master or >= 1 second");
	wlr_beat = new;
	return MAL_SUCCEED;
}

static str
WLRquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str qry =  *getArgReference_str(stk,pci,1);
	str msg = MAL_SUCCEED;
	char *x, *y, *qtxt;

	(void) mb;
	if( cntxt->wlc_kind == WLC_ROLLBACK || cntxt->wlc_kind == WLC_ERROR)
		return msg;
	// execute the query in replay mode when required.
	// we need to get rid of the escaped quote.
	x = qtxt= (char*) GDKmalloc(strlen(qry) +1);
	if( qtxt == NULL)
		throw(SQL,"wlr.query",SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

/* A change event need not be executed, because it is already captured
 * in the update/append/delete
 */
str
WLRcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) cntxt;
	(void) pci;
	(void) stk;
	(void) mb;
	return MAL_SUCCEED;
}

str
WLRrollback(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) cntxt;
	(void) pci;
	(void) stk;
	(void) mb;
	return MAL_SUCCEED;
}

str
WLRaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) cntxt;
	(void) pci;
	(void) stk;
	(void) mb;
	return MAL_SUCCEED;
}

str
WLRcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return WLRquery(cntxt,mb,stk,pci);
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

/* TODO: Martin take a look at this.
 *
 * PSA: DO NOT USE THIS OUT OF WLRappend or very bad things will happen!
 * (variable msg and tag cleanup will not be defined).
 */
#define WLRcolumn(TPE) \
	for( i = 4; i < pci->argc; i++){                                \
		TPE val = *getArgReference_##TPE(stk,pci,i);            \
		if (BUNappend(ins, (void*) &val, false) != GDK_SUCCEED) { \
			msg = createException(MAL, "WLRappend", "BUNappend failed"); \
			goto cleanup;                                   \
		}                                                       \
	}

str
WLRappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sname, tname, cname;
	int tpe,i;
	mvc *m=NULL;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *ins = 0;
	str msg = MAL_SUCCEED;

	if( cntxt->wlc_kind == WLC_ROLLBACK || cntxt->wlc_kind == WLC_ERROR)
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
		throw(SQL, "sql.append", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.append", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	// get the data into local BAT

	tpe= getArgType(mb,pci,4);
	ins = COLnew(0, tpe, 0, TRANSIENT);
	if( ins == NULL){
		throw(SQL,"WLRappend",SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
#ifdef HAVE_HGE
	case TYPE_hge: WLRcolumn(hge); break;
#endif
	case TYPE_str:
		for( i = 4; i < pci->argc; i++){
			str val = *getArgReference_str(stk,pci,i);
			if (BUNappend(ins, (void*) val, false) != GDK_SUCCEED) {
				msg = createException(MAL, "WLRappend", "BUNappend failed");
				goto cleanup;
			}
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
cleanup:
	BBPunfix(((BAT *) ins)->batCacheid);
	return msg;
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
	oid o;
	str msg= MAL_SUCCEED;

	if( cntxt->wlc_kind == WLC_ROLLBACK || cntxt->wlc_kind == WLC_ERROR)
		return msg;
	sname = *getArgReference_str(stk,pci,1);
	tname = *getArgReference_str(stk,pci,2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.append", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.append", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	// get the data into local BAT

	ins = COLnew(0, TYPE_oid, 0, TRANSIENT);
	if( ins == NULL){
		throw(SQL,"WLRappend",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	for( i = 3; i < pci->argc; i++){
		o = *getArgReference_oid(stk,pci,i);
		if (BUNappend(ins, (void*) &o, false) != GDK_SUCCEED) {
			msg = createException(MAL, "WLRdelete", "BUNappend failed");
			goto cleanup;
		}
	}

	store_funcs.delete_tab(m->session->tr, t, ins, TYPE_bat);
cleanup:
	BBPunfix(((BAT *) ins)->batCacheid);
	return msg;
}

/* TODO: Martin take a look at this.
 *
 * PSA: DO NOT USE THIS OUT OF WLRupdate or very bad things will happen!
 * (variable msg and tag cleanup will not be defined).
 */
#define WLRvalue(TPE)                                                   \
	{	TPE val = *getArgReference_##TPE(stk,pci,5);            \
			if (BUNappend(upd, (void*) &val, false) != GDK_SUCCEED) { \
				fprintf(stderr, "WLRupdate:BUNappend failed"); \
				goto cleanup;                                   \
			}                                                       \
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

	if( cntxt->wlc_kind == WLC_ROLLBACK || cntxt->wlc_kind == WLC_ERROR)
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
		throw(SQL, "sql.update", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.update", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	// get the data into local BAT

	tids = COLnew(0, TYPE_oid, 0, TRANSIENT);
	if( tids == NULL){
		throw(SQL,"WLRupdate",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	upd = COLnew(0, tpe, 0, TRANSIENT);
	if( upd == NULL){
		BBPunfix(((BAT *) tids)->batCacheid);
		throw(SQL,"WLRupdate",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	if (BUNappend(tids, &o, false) != GDK_SUCCEED) {
		msg = createException(MAL, "WLRupdate", "BUNappend failed");
		goto cleanup;
	}

	switch(ATOMstorage(tpe)){
	case TYPE_bit: WLRvalue(bit); break;
	case TYPE_bte: WLRvalue(bte); break;
	case TYPE_sht: WLRvalue(sht); break;
	case TYPE_int: WLRvalue(int); break;
	case TYPE_lng: WLRvalue(lng); break;
	case TYPE_oid: WLRvalue(oid); break;
	case TYPE_flt: WLRvalue(flt); break;
	case TYPE_dbl: WLRvalue(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: WLRvalue(hge); break;
#endif
	case TYPE_str:
		{
			str val = *getArgReference_str(stk,pci,5);
			if (BUNappend(upd, (void*) val, false) != GDK_SUCCEED) {
				msg = createException(MAL, "WLRupdate", "BUNappend failed");
				goto cleanup;
			}
		}
		break;
	default:
		fprintf(stderr, "Missing type in WLRupdate");
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

cleanup:
	BBPunfix(((BAT *) tids)->batCacheid);
	BBPunfix(((BAT *) upd)->batCacheid);
	return msg;
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

	if( cntxt->wlc_kind == WLC_ROLLBACK || cntxt->wlc_kind == WLC_ERROR)
		return msg;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sname);
	if (s == NULL)
		throw(SQL, "sql.clear_table", SQLSTATE(3F000) "Schema missing %s",*sname);
	t = mvc_bind_table(m, s, *tname);
	if (t == NULL)
		throw(SQL, "sql.clear_table", SQLSTATE(42S02) "Table missing %s.%s",*sname,*tname);
	(void) mvc_clear_table(m, t);
	return MAL_SUCCEED;
}
