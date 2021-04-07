/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * A master can be replicated by taking a binary copy of the 'bat' directory
 * when in quiescent mode or a more formal snapshot..
 * Alternatively you start with an empty database.
 *
 * The wlc log records written are numbered 0.. wlc_tag - 1
 * The replicator copies all of them unto and including wlc_limit.
 * This leads to the wlr_tag from -1 .. wlc_limit, wlr_tag,..., INT64_MAX
 *
 * Replication start after setting the master id and giving an (optional) wlr_limit.
 * Any error encountered in replaying the log stops the process, because then
 * no guarantee can be given on the consistency with the master database.
 * A manual fix for an exceptional case is allowed, whereafter a call
 * to CALL wlrclear() accepts the failing transaction and prepares
 * to the next CALL replicate(),
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
#include "mutils.h"

#define WLR_WAIT 0
#define WLR_RUN   101
#define WLR_STOP 201

#define WLC_COMMIT 40
#define WLC_ROLLBACK 50
#define WLC_ERROR 60

MT_Lock     wlr_lock = MT_LOCK_INITIALIZER(wlr_lock);

/* The current status of the replica processing.
 * It is based on the assumption that at most one replica thread is running
 * importing data from a single master.
 */
static char wlr_master[IDLENGTH];
static int	wlr_batches; 				// the next file to be processed
static lng 	wlr_tag = -1;				// the last transaction id being processed
static char wlr_read[26];				// last record read
static char wlr_timelimit[128];			// stop re-processing transactions when time limit is reached
static int 	wlr_beat;					// period between successive synchronisations with master
static char wlr_error[BUFSIZ];	// error that stopped the replication process

static MT_Id wlr_thread = 0;			// The single replicator thread is active
static int 	wlr_state = WLR_WAIT;		// which state WAIT/RUN
static lng 	wlr_limit = -1;				// stop re-processing after transaction id 'wlr_limit' is processed

#define MAXLINE 2048

/* Simple read the replica configuration status file */
static str
WLRgetConfig(void){
	char *path;
	char line[MAXLINE];
	FILE *fd;
	int len;
	str msg= MAL_SUCCEED;

	if((path = GDKfilepath(0, 0, "wlr.config", 0)) == NULL)
		throw(MAL,"wlr.getConfig", "Could not create wlr.config file path\n");
	fd = MT_fopen(path,"r");
	GDKfree(path);
	if( fd == NULL)
		throw(MAL,"wlr.getConfig", "Could not access wlr.config file \n");
	while( fgets(line, MAXLINE, fd) ){
		line[strlen(line)-1]= 0;
		if( strncmp("master=", line,7) == 0) {
			len = snprintf(wlr_master, IDLENGTH, "%s", line + 7);
			if (len == -1 || len >= IDLENGTH) {
				msg= createException(SQL,"wlr.getConfig", "Master config value is too large\n");
				goto bailout;
			} else
			if (len  == 0) {
				msg = createException(SQL,"wlr.getConfig", "Master config path is missing\n");
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
		if( strncmp("read=", line, 5) == 0)
			strcpy(wlr_read, line + 5);
		else
		if( strncmp("error=", line, 6) == 0) {
			char *s;
			len = snprintf(wlr_error, BUFSIZ, "%s", line + 6);
			if (len == -1 || len >= BUFSIZ) {
				msg = createException(SQL, "wlr.getConfig", "Config value is too large\n");
				goto bailout;
			}
			s = strchr(wlr_error, (int) '\n');
			if ( s) *s = 0;
		}
	}
bailout:
	fclose(fd);
	return msg;
}

/* Keep the current status in the configuration status file */
static str
WLRputConfig(void){
	char *path;
	stream *fd;
	str msg = MAL_SUCCEED;

	if((path = GDKfilepath(0,0,"wlr.config",0)) == NULL)
		throw(SQL, "wlr.putConfig", "Could not access wlr.config file\n");
	fd = open_wastream(path);
	GDKfree(path);
	if( fd == NULL)
		throw(SQL,"wlr.putConfig", "Could not create wlr.config file: %s\n", mnstr_peek_error(NULL));

	mnstr_printf(fd,"master=%s\n", wlr_master);
	mnstr_printf(fd,"batches=%d\n", wlr_batches);
	mnstr_printf(fd,"tag="LLFMT"\n", wlr_tag);
	mnstr_printf(fd,"beat=%d\n", wlr_beat);
	if( wlr_timelimit[0])
		mnstr_printf(fd,"read=%s\n", wlr_read);
	if( wlr_error[0])
		mnstr_printf(fd,"error=%s", wlr_error);
	close_stream(fd);
	return msg;
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
	str dir, msg = MAL_SUCCEED;
	FILE *fd;

	if( wlr_master[0] == 0 )
		return MAL_SUCCEED;

	/* collect master properties */
	len = snprintf(path, FILENAME_MAX, "..%c%s", DIR_SEP, wlr_master);
	if (len == -1 || len >= FILENAME_MAX)
		throw(MAL, "wlr.getMaster", "wlc.config filename path is too large");
	if ((dir = GDKfilepath(0, path, "wlc.config", 0)) == NULL)
		throw(MAL,"wlr.getMaster","Could not access wlc.config file %s/wlc.config\n", path);

	fd = MT_fopen(dir,"r");
	GDKfree(dir);
	if (fd == NULL)
		throw(MAL,"wlr.getMaster","Could not get read access to '%s'config file\n", wlr_master);
	msg = WLCreadConfig(fd);
	if( msg != MAL_SUCCEED)
		return msg;
	if( ! wlr_master[0] )
		throw(MAL,"wlr.getMaster","Master not identified\n");
	wlc_state = WLC_CLONE; // not used as master
	if( !wlr_master[0] )
		throw(MAL,"wlr.getMaster","Master not identified\n");
	wlc_state = WLC_CLONE; // not used as master
	return MAL_SUCCEED;
}

/* each WLR block is turned into a separate MAL block and executed
 * This block is re-used as we consider the complete file.
 */

#define cleanup(){\
	resetMalBlkAndFreeInstructions(mb, 1);\
	trimMalVariables(mb, NULL);\
	}

static str
WLRprocessBatch(Client cntxt)
{
	int i, len;
	char path[FILENAME_MAX];
	stream *fd = NULL;
	Client c;
	size_t sz;
	MalBlkPtr mb;
	InstrPtr q;
	str other;
	mvc *sql;
	Symbol prev = NULL;
	lng tag;
	char tag_read[26];			// stop re-processing transactions when time limit is reached
	str action= NULL;
	str msg= MAL_SUCCEED, msg2= MAL_SUCCEED;

	msg = WLRgetConfig();
	tag = wlr_tag;
	if( msg != MAL_SUCCEED){
		snprintf(wlr_error, BUFSIZ, "%s", msg);
		freeException(msg);
		return MAL_SUCCEED;
	}
	if( wlr_error[0]) {
		if (!(msg = GDKstrdup(wlr_error)))
			throw(MAL, "wlr.batch", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}

	c = MCforkClient(cntxt);
	if( c == 0)
		throw(MAL, "wlr.batch", "Could not create user for WLR process\n");
	c->promptlength = 0;
	c->listing = 0;
	c->fdout = open_wastream(".wlr");
	if(c->fdout == NULL) {
		MCcloseClient(c);
		throw(MAL,"wlr.batch", "Could not create user for WLR process: %s\n", mnstr_peek_error(NULL));
	}

	/* Cook a log file into a concreate MAL function for multiple transactions */
	prev = newFunction(putName(sql_private_module_name), putName("wlr"), FUNCTIONsymbol);
	if(prev == NULL) {
		MCcloseClient(c);
		throw(MAL, "wlr.batch", "Could not create user for WLR process\n");
	}
	c->curprg = prev;
	mb = c->curprg->def;
	setVarType(mb, 0, TYPE_void);

	msg = SQLinitClient(c);
	if( msg != MAL_SUCCEED) {
		MCcloseClient(c);
		freeSymbol(prev);
		return msg;
	}
	if ((msg = getSQLContext(c, mb, &sql, NULL))) {
		SQLexitClient(c);
		MCcloseClient(c);
		freeSymbol(prev);
		return msg;
	}
	if ((msg = checkSQLContext(c)) != NULL) {
		SQLexitClient(c);
		MCcloseClient(c);
		freeSymbol(prev);
		return msg;
	}

	path[0]=0;
	for( i= wlr_batches; i < wlc_batches && !GDKexiting() && wlr_state != WLR_STOP && wlr_tag <= wlr_limit && msg == MAL_SUCCEED; i++){
		len = snprintf(path,FILENAME_MAX,"%s%c%s_%012d", wlc_dir, DIR_SEP, wlr_master, i);
		if (len == -1 || len >= FILENAME_MAX) {
			msg = createException(MAL, "wlr.batch", "Filename path is too large\n");
			break;
		}
		fd= open_rastream(path);
		if( fd == NULL) {
			msg = createException(MAL, "wlr.batch", "Cannot access path '%s': %s\n", path, mnstr_peek_error(NULL));
			break;
		}
		sz = getFileSize(fd);
		if (sz > (size_t) 1 << 29) {
			close_stream(fd);
			msg = createException(MAL, "wlr.batch", "File %s is too large to process\n", path);
			break;
		}
		if ((c->fdin = bstream_create(fd, sz == 0 ? (size_t) (2 * 128 * BLOCK) : sz)) == NULL) {
			close_stream(fd);
			msg = createException(MAL, "wlr.batch", "Failed to open stream for file %s\n", path);
			break;
		}
		if (bstream_next(c->fdin) < 0){
			msg = createException(MAL, "wlr.batch", "Could not read %s\n", path);
			break;
		}

		c->yycur = 0;

		// now parse the file line by line to reconstruct the WLR blocks
		do{
			parseMAL(c, c->curprg, 1, 1, NULL);

			mb = c->curprg->def;
			if( mb->errors){
				msg = mb->errors;
				mb->errors = NULL;
				cleanup();
				break;
			}
			if( mb->stop == 1){
				cleanup();
				break;
			}
			q= getInstrPtr(mb, mb->stop - 1);
			if( getModuleId(q) != wlrRef){
				msg =createException(MAL,"wlr.process", "batch %d:improper wlr instruction: %s\n", i, instruction2str(mb,0, q, LIST_MAL_CALL));
				cleanup();
				break;
			}
			if ( getModuleId(q) == wlrRef && getFunctionId(q) == actionRef ){
				action = getVarConstant(mb, getArg(q,1)).val.sval;
			}
			if ( getModuleId(q) == wlrRef && getFunctionId(q) == catalogRef ){
				action = getVarConstant(mb, getArg(q,1)).val.sval;
			}
			if( getModuleId(q) == wlrRef && getFunctionId(q) == transactionRef){
				tag = getVarConstant(mb, getArg(q,1)).val.lval;
				snprintf(tag_read, sizeof(tag_read), "%s", getVarConstant(mb, getArg(q,2)).val.sval);

				// break loop if we don't see a the next expected transaction
				if ( tag <= wlr_tag){
					/* skip already executed transaction log */
					continue;
				} else
				if(  ( tag > wlr_limit) ||
					  ( wlr_timelimit[0] && strcmp(tag_read, wlr_timelimit) > 0)){
					/* stop execution of the transactions if your reached the limit */
					cleanup();
					break;
				}
			}
			// only re-execute successful transactions.
			if ( getModuleId(q) == wlrRef && getFunctionId(q) ==commitRef  && (tag > wlr_tag || ( wlr_timelimit[0] && strcmp(tag_read, wlr_timelimit) > 0))){
				pushEndInstruction(mb);
				// execute this block if no errors are found
				msg = chkTypes(c->usermodule, mb, FALSE);
				if (!msg)
					msg = chkFlow(mb);
				if (!msg)
					msg = chkDeclarations(mb);
				wlr_tag =  tag; // remember which transaction we executed
				snprintf(wlr_read, sizeof(wlr_read), "%s", tag_read);
				if(!msg && mb->errors == 0){
					sql->session->auto_commit = 0;
					sql->session->ac_on_commit = 1;
					sql->session->level = 0;
					if(mvc_trans(sql) < 0) {
						TRC_ERROR(SQL_TRANS, "Allocation failure while starting the transaction\n");
					} else {
						msg= runMAL(c,mb,0,0);
						if( msg == MAL_SUCCEED){
							/* at this point we have updated the replica, but the configuration has not been changed.
							 * If at this point an error occurs, we could redo the same transaction twice later on.
							 * The solution is to make sure that we recognize that a transaction has started and is completed successfully
							 */
							msg = WLRputConfig();
							if( msg)
								break;
						}
						// ignore warnings
						if (msg && strstr(msg,"WARNING"))
							msg = MAL_SUCCEED;
						if( msg != MAL_SUCCEED){
							// they should always succeed
							msg =createException(MAL,"wlr.process", "Replication error in batch %d:"LLFMT" :%s:%s\n", i, wlr_tag, msg, action);
							if((other = mvc_rollback(sql,0,NULL, false)) != MAL_SUCCEED) //an error was already established
								GDKfree(other);
							break;
						} else
						if((other = mvc_commit(sql, 0, 0, false)) != MAL_SUCCEED) {
							msg = createException(MAL,"wlr.process", "transaction %d:"LLFMT" commit failed: %s\n", i, tag, other);
							freeException(other);
							break;
						}
					}
				} else {
					if( msg == MAL_SUCCEED)
						msg = createException(SQL, "wlr.replicate", "typechecking failed '%s':'%s':\n",path, mb->errors);
					cleanup();
					break;
				}
				cleanup();
				if ( wlr_tag + 1 == wlc_tag || tag == wlr_limit)
						break;
			} else
			if ( getModuleId(q) == wlrRef && (getFunctionId(q) == rollbackRef || getFunctionId(q) == commitRef)){
				cleanup();
				if ( wlr_tag + 1 == wlc_tag || tag == wlr_limit || ( wlr_timelimit[0] && strcmp(tag_read, wlr_timelimit) > 0))
						break;
			}
		} while(wlr_state != WLR_STOP &&  mb->errors == 0 && msg == MAL_SUCCEED);

		// skip to next file when all is read correctly
		if (msg == MAL_SUCCEED && tag <= wlr_limit)
			wlr_batches++;
		if( msg != MAL_SUCCEED)
			snprintf(wlr_error, BUFSIZ, "%s", msg);
		msg2 = WLRputConfig();
		bstream_destroy(c->fdin);
		if(msg2)
			break;
		if ( wlr_tag == wlr_limit)
			break;
	}

	close_stream(c->fdout);
	SQLexitClient(c);
	MCcloseClient(c);
	if (prev)
		freeSymbol(prev);
	if (msg2) { /* throw msg2, if msg is not set */
		if (!msg)
			msg = msg2;
		else
			freeException(msg2);
	}
	return msg;
}

/*
 *  A single WLR thread is allowed to run in the background.
 *  If it happens to crash then replication roll forward is suspended.
 *  The background job can only leave error messages in the merovingian log.
 *
 * A timing issue.
 * The WLRprocess can only start after an SQL environment has been initialized.
 * It is therefore initialized when a SQLclient() is issued.
 */
static void
WLRprocessScheduler(void *arg)
{
	Client cntxt = (Client) arg;
	int duration = 0;
	str msg = MAL_SUCCEED;

	msg = WLRgetConfig();
	if ( msg ){
		snprintf(wlr_error, BUFSIZ, "%s", msg);
		freeException(msg);
		return;
	}

	assert(wlr_master[0]);
	if (!(cntxt = MCinitClient(MAL_ADMIN, NULL,NULL))) {
		snprintf(wlr_error, BUFSIZ, "Failed to init WLR scheduler client");
		return;
	}

	MT_lock_set(&wlr_lock);
	if ( wlr_state != WLR_STOP)
		wlr_state = WLR_RUN;
	MT_lock_unset(&wlr_lock);

	while( wlr_state != WLR_STOP  && !wlr_error[0]){
		// wait at most for the cycle period, also at start
		duration = (wlc_beat > 0 ? wlc_beat:1) * 1000 ;
		if( wlr_timelimit[0]){
			timestamp ts = timestamp_current();
			str wlc_time = NULL;
			size_t wlc_limit = 0;
			int compare;

			assert(!is_timestamp_nil(ts));
			if (timestamp_tostr(&wlc_time, &wlc_limit, &ts, true) < 0) {
				snprintf(wlr_error, BUFSIZ, "Unable to retrieve current time");
				return;
			}
			// actually never wait longer then the timelimit requires
			// preference is given to the beat.
			compare = strncmp(wlc_time, wlr_timelimit, sizeof(wlr_timelimit));
			GDKfree(wlc_time);
			MT_thread_setworking("sleeping");
			if (compare >= 0 && duration >100)
				MT_sleep_ms(duration);
		}
		for( ; duration > 0  && wlr_state != WLR_STOP; duration -= 200){
			if ( wlr_tag + 1 == wlc_tag || wlr_tag >= wlr_limit || wlr_limit == -1){
				MT_thread_setworking("sleeping");
				MT_sleep_ms(200);
			}
		}
		MT_thread_setworking("processing wlr");
		if ((msg = WLRprocessBatch(cntxt)))
			freeException(msg);

		/* Can not use GDKexiting(), because a test may already reach that point before it did anything.
		 * Instead wait for the explicit WLR_STOP
		 */
		if( GDKexiting()){
			MT_lock_set(&wlr_lock);
			wlr_state = WLR_STOP;
			MT_lock_unset(&wlr_lock);
			break;
		}
	}
	wlr_thread = 0;
	MT_lock_set(&wlr_lock);
	if( wlr_state == WLR_RUN)
		wlr_state = WLR_WAIT;
	MT_lock_unset(&wlr_lock);
	MCcloseClient(cntxt);
}

// The replicate() command can be issued at the SQL console
// which can accept exceptions
str
WLRmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int len;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	len = snprintf(wlr_master, IDLENGTH, "%s", *getArgReference_str(stk, pci, 1));
	if (len == -1 || len >= IDLENGTH)
		throw(MAL, "wlr.master", SQLSTATE(42000) "Input value is too large for wlr_master buffer");
	if ((msg = WLRgetMaster()))
		freeException(msg);
	if ((msg = WLRgetConfig())) {
		freeException(msg);
		if ((msg = WLRputConfig()))
			freeException(msg);
	}
	return MAL_SUCCEED;
}

str
WLRreplicate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg, timelimit = NULL;
	size_t size = 0;
	lng limit = INT64_MAX;

	if( wlr_thread)
		throw(MAL, "sql.replicate", "WLR thread already running, stop it before continueing");

	msg = WLRgetConfig();
	if( msg != MAL_SUCCEED)
		return msg;
	if( wlr_error[0]) {
		if (!(msg = GDKstrdup(wlr_error)))
			throw(MAL, "sql.replicate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}

	if( pci->argc == 0)
		wlr_limit = INT64_MAX;
	else
	if( getArgType(mb, pci, 1) == TYPE_timestamp){
		if (timestamp_precision_tostr(&timelimit, &size, *getArgReference_TYPE(stk, pci, 1, timestamp), 3, true) < 0)
			throw(SQL, "wlr.replicate", GDK_EXCEPTION);
		fprintf(stderr,"#time limit %s\n",timelimit);
	} else
	if( getArgType(mb, pci, 1) == TYPE_bte)
		limit = getVarConstant(mb,getArg(pci,1)).val.btval;
	else
	if( getArgType(mb, pci, 1) == TYPE_sht)
		limit = getVarConstant(mb,getArg(pci,1)).val.shval;
	else
	if( getArgType(mb, pci, 1) == TYPE_int)
		limit = getVarConstant(mb,getArg(pci,1)).val.ival;
	else
	if( getArgType(mb, pci, 1) == TYPE_lng)
		limit = getVarConstant(mb,getArg(pci,1)).val.lval;

	if (timelimit) {
		if (size > sizeof(wlr_timelimit)) {
			GDKfree(timelimit);
			throw(MAL, "sql.replicate", "Limit timestamp size is too large");
		}
		strcpy(wlr_timelimit, timelimit);
		GDKfree(timelimit);
	}
	if ( limit < 0 && wlr_timelimit[0] == 0)
		throw(MAL, "sql.replicate", "Stop tag limit should be positive or timestamp should be set");
	if( wlc_tag == 0) {
		if ((msg = WLRgetMaster()))
			freeException(msg);
		if( wlc_tag == 0)
			throw(MAL, "sql.replicate", "Perhaps a missing wlr.master() call. ");
	}
	if (limit < INT64_MAX && limit >= wlc_tag)
		throw(MAL, "sql.replicate", "Stop tag limit "LLFMT" be less than wlc_tag "LLFMT, limit, wlc_tag);
	if (limit >= 0)
		wlr_limit = limit;

	if (wlc_state != WLC_CLONE)
		throw(MAL, "sql.replicate", "No replication master set");
	if ((msg = WLRputConfig()))
		return msg;
	return WLRprocessBatch(cntxt);
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


/* Start a separate thread to continue merging the log record */
str
WLRstart(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	// time the consolidation process in the background
	if (MT_create_thread(&wlr_thread, WLRprocessScheduler, (void*) NULL,
						 MT_THR_DETACHED, "WLRprocessSched") < 0) {
		throw(SQL,"wlr.init",SQLSTATE(42000) "Starting wlr manager failed");
	}

	// Wait until the replicator is properly initialized
	while( wlr_state != WLR_RUN && wlr_error[0] == 0){
		MT_sleep_ms( 50);
	}
	return MAL_SUCCEED;
}

str
WLRstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	// kill the replicator thread and reset for a new one
	MT_lock_set(&wlr_lock);
	if( wlr_state == WLR_RUN)
		wlr_state =  WLR_STOP;
	MT_lock_unset(&wlr_lock);

	return MAL_SUCCEED;
}

str
WLRgetmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = getArgReference_str(stk,pci,0);
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	msg = WLRgetConfig();
	if( msg)
		return msg;
	if( wlr_master[0]) {
		if (!(*ret= GDKstrdup(wlr_master)))
			throw(MAL, "wlr.getmaster", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else
		throw(MAL, "wlr.getmaster", "Master not found");
	return msg;
}

str
WLRgetclock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = getArgReference_str(stk,pci,0);
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	msg = WLRgetConfig();
	if( msg)
		return msg;
	if( wlr_read[0])
		*ret = GDKstrdup(wlr_read);
	else
		*ret = GDKstrdup(str_nil);
	if (*ret == NULL)
		throw(MAL, "wlr.getclock", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
WLRgettick(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *ret = getArgReference_lng(stk,pci,0);
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	msg = WLRgetConfig();
	if( msg)
		return msg;
	*ret = wlr_tag;
	return msg;
}

/* the replica cycle can be set to fixed interval.
 * This allows for ensuring an up to date version every N seconds
 */
str
WLRsetbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int new;
	(void) cntxt;
	(void) mb;
	new = *getArgReference_int(stk,pci,1);
	if ( new < wlc_beat || new < 1)
		throw(SQL,"setbeat",SQLSTATE(42000) "Cycle time should be larger then master or >= 1 second");
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
		throw(SQL,"wlr.query",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	for(y = qry; *y; y++){
		if( *y == '\\' ){
			if( *(y+1) ==  '\'')
			y += 1;
		}
		*x++ = *y;
	}
	*x = 0;
	msg =  SQLstatementIntern(cntxt, qtxt, "SQLstatement", TRUE, TRUE, NULL);
	GDKfree(qtxt);
	return msg;
}

/* An error was reported and manually dealt with.
 */
str
WLRaccept(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) pci;
	(void) stk;
	(void) mb;
	wlr_error[0]= 0;
	return WLRputConfig();
}

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
	lng pos = *(lng*)getArgReference_lng(stk,pci,4);

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

	tpe= getArgType(mb,pci,5);
	ins = COLnew(0, tpe, 0, TRANSIENT);
	if( ins == NULL){
		throw(SQL,"WLRappend",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	sqlstore *store = m->session->tr->store;
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
		for( i = 5; i < pci->argc; i++){
			str val = *getArgReference_str(stk,pci,i);
			if (BUNappend(ins, (void*) val, false) != GDK_SUCCEED) {
				msg = createException(MAL, "WLRappend", "BUNappend failed");
				goto cleanup;
			}
		}
		break;
	}

	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		store->storage_api.append_col(m->session->tr, c, (size_t)pos, ins, TYPE_bat, 1);
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i)
			store->storage_api.append_idx(m->session->tr, i, (size_t)pos, ins, tpe, 1);
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
	sqlstore *store = m->session->tr->store;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.append", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.append", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	// get the data into local BAT

	ins = COLnew(0, TYPE_oid, 0, TRANSIENT);
	if( ins == NULL){
		throw(SQL,"WLRappend",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for( i = 3; i < pci->argc; i++){
		o = *getArgReference_oid(stk,pci,i);
		if (BUNappend(ins, (void*) &o, false) != GDK_SUCCEED) {
			msg = createException(MAL, "WLRdelete", "BUNappend failed");
			goto cleanup;
		}
	}

	store->storage_api.delete_tab(m->session->tr, t, ins, TYPE_bat);
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
	{	TPE val = *getArgReference_##TPE(stk,pci,5);                    \
			if (BUNappend(upd, (void*) &val, false) != GDK_SUCCEED) {   \
				goto cleanup;                                           \
		}                                                               \
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

	sqlstore *store = m->session->tr->store;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.update", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.update", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	// get the data into local BAT

	tids = COLnew(0, TYPE_oid, 0, TRANSIENT);
	if( tids == NULL){
		throw(SQL,"WLRupdate",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	upd = COLnew(0, tpe, 0, TRANSIENT);
	if( upd == NULL){
		BBPunfix(((BAT *) tids)->batCacheid);
		throw(SQL,"WLRupdate",SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		TRC_ERROR(SQL_TRANS, "Missing type in WLRupdate\n");
	}

	BATmsync(tids);
	BATmsync(upd);
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		store->storage_api.update_col(m->session->tr, c, tids, upd, TYPE_bat);
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i)
			store->storage_api.update_idx(m->session->tr, i, tids, upd, TYPE_bat);
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
