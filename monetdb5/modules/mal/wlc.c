/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) 2017 Martin Kersten
 * This module collects the workload-capture-replay statements during transaction execution,
 * also known as asynchronous logical replication management. It can be used for
 * multiple purposes: BACKUP, REPLICATION, and REPLAY
 *
 * For a BACKUP we need either a complete update log from the beginning, or
 * a binary snapshot with a collection of logs recording its changes since.
 * To ensure transaction ACID properties, the log record should be stored on
 * disk within the transaction brackets, which may cause a serious IO load.
 * (Tip, store these logs files on an SSD or NVM)
 *
 * For REPLICATION, also called a database clone or slave, we take a snapshot and the
 * log files that reflect the recent changes. The log updates are replayed against
 * the snapshot until a specific time point or transaction id is reached.
 *
 * Some systems also use the logical logs to REPLAY all (expensive) queries
 * against the database. We skip this for the time being, as those queries
 * can be captured already in the server.
 * [A flag should be added to at least capture them]
 *
 * The goal of this module is to ease BACKUP and REPLICATION of a master database
 * with a time-bounded delay. This means that both master and replica run at a certain beat
 * (in seconds) by which information is made available or read by the replica.
 *
 * Such a replica is used in query workload sharing, database versioning, and (re-)partitioning.
 * Tables taken from the master are not protected against local updates in the replica.
 * However, any replay transaction that fails stops the cloning process.
 * Furthermore, only persistent tables are considered for replication.
 * Updates under the 'tmp' schema, i.e. temporary tables, are ignored.
 *
 * Simplicity and ease of end-user control has been the driving argument here.
 *
 * IMPLEMENTATION
 * The underlying assumption of the techniques deployed is that the database
 * resides on a proper (global/distributed) file system to guarantees recovery
 * from most storage system related failures, e.g. using RAID disks or LSF systems.
 *
 * A database can be set into 'master' mode only once using the SQL command:
 * CALL wrc_master.master() whose access permission is limited to the 'monetdb' user.[CHECK]
 * An optional path to the log record directory can be given to reduce the IO latency,
 * e.g. using a nearby SSD, or where there is ample of space to keep a long history,
 * e.g. a HDD or cold storage location.
 *
 * By default, the command creates a directory .../dbfarm/dbname/wlc_logs to hold all logs
 * and a configuration file .../dbfarm/dbname/wlc.config to hold the state of the transaction logs.
 * It contains the following key=value pairs:
 * 		snapshot=<path to a snapshot directory>
 * 		logs=<path to the wlc log directory>
 * 		state=<started, stopped>
 * 		batches=<next available batch file to be applied>
 * 		beat=<maximal delay between log files, in seconds>
 * 		write=<timestamp of the last transaction recorded>
 *
 * A missing path to the snapshot denotes that we can start the clone with an empty database.
 * The log files are stored as master/<dbname>_<batchnumber>. They belong to the snapshot.
 *
 * Each wlc log file contains a serial log of a number of committed compound transactions.
 * The log records are represented as ordinary MAL statement blocks, which
 * are executed in serial mode. (parallelism can be considered for large updates later)
 * Each transaction job is identified by a unique id, its starting time, and the original responsible user.
 * Each log-record should end with a commit to be allowed for re-execution.
 * Log records with a rollback tag are merely for analysis by the DBA, their statements are ignored.
 *
 * A transaction log file is created by the master using a heartbeat (in seconds).
 * A new transaction log file is published when the system has been collecting transaction records for some time.
 * The beat can be set using a SQL command, e.g.
 * CALL wcr_master.beat(duration)
 * Setting it to zero leads to a log file per transaction and may cause a large log directory
 * with thousands of small files.
 * The default of 5 minutes should balance polling overhead in most practical situations.
 * Intermittent flush() during this period ensures the committed log records survive
 * a crash.
 *
 * A minor problem here is that we should ensure that the log file is closed even if there
 * are no transactions running. It is solved with a separate monitor thread, which ensures
 * that the a new log file is created at least after 'beat' seconds since the first logrecord was created.
 * After closing, the replicas can see from the master configuration file that a new log batch is available.
 *
 * The final step is to close stop transaction logging with the command
 * CALL wcr_master.stop().
 * It typically is the end-of-life-time for a snapshot. For example, when planning to do
 * a large bulk load of the database, stopping logging avoids a double write into the
 * database. The database can only be brought back into master mode using a fresh snapshot.
 *
 * [It is not advicable to temporarily stop logging and continue afterwards, because then there
 * is no guarantee the user will see a consistent database.]
 *
 * One of the key challenges for a DBA is to keep the log directory manageable, because it grows
 * with the speed up updates being applied to the database. This calls for regularly checking
 * for their disk footprint and taking a new snapshot as a frame of reference.
 *
 * [TODO A trigger should be added to stop logging and call for a fresh snapshot first]
 * [TODO the batch files might include the snapshot id for ease of rebuild]
 *
 * The DBA tool 'monetdb' provides options to create a master and its replicas.
 * It will also maintain the list of replicas for inspection and managing their drift.
 * For example,
 *	 monetdb master <dbname> [ <optional snapshot path>]
 * which locks the database, takes a save copy, initializes the state chance to master.
 *
 * A fresh replica can be constructed as follows:
 * 	monetdb replicate <dbname> <mastername>
 *
 * Instead of using the monetdb command line we can use the SQL calls directly
 * sys.master() and sys.replicate(), provided we start with a fresh database.
 *
 * CLONE
 *
 * Every clone should start off with a copy of the binary snapshot identified by 'snapshot'.
 * A fresh database can be turned into a clone using the call
 *     CALL wcr_replica.master('mastername')
 * It will grab the latest snapshot of the master and applies all
 * available log files before releasing the system.
 * The master has no knowledge about the number of clones and their whereabouts.
 *
 * The clone process will iterate in the background through the log files,
 * applying all update transactions.
 *
 * An optional timestamp or transaction id can be added to the replicate() command to
 * apply the logs until a given moment. This is particularly handy when an unexpected
 * desastrous user action (drop persistent table) has to be recovered from.
 *
 * CALL wcr_replica.master('mastername');  -- get logs from a specific master
 * ...
 * CALL wcr_replicate.replicate(tag); -- stops after we are in sink with tag
 * ...
 * CALL wcr_replicate.replicate(NOW()); -- stop after we sinked all transactions
 * ...
 * CALL wcr_replicate.replicate(); -- synchronize in background continuously
 * ...
 * CALL wcr_replicate.stop(); -- stop the synchroniation thread
 *
 * SELECT wcr_replica.clock();
 * returns the timestamp of the last replicated transaction.
 * SELECT wcr_replica.tick();
 * returns the transaction id of the last replicated transaction.
 * SELECT wcr_master.clock();
 * return the timestamp of the last committed transaction in the master.
 * SELECT wcr_master.tick();
 * return the transaction id of the last committed transaction in the master.
 *
 * Any failure encountered during a log replay terminates the replication process,
 * leaving a message in the merovingian log configuration.
 *
 * The wlc files purposely have a textual format derived from the MAL statements.
 * This provides a stepping stone for remote execution later.
 *
 * [TODO consider the roll logging of SQL session variables, i.e. optimizer_pipe
 * as part of the log record]
 * For updates we don't need special care for this.
 */
#include "monetdb_config.h"
#include <time.h>
#include "mal_builder.h"
#include "wlc.h"
#include "gdk_time.h"
#include "mutils.h"

static MT_Lock     wlc_lock = MT_LOCK_INITIALIZER(wlc_lock);

static char wlc_snapshot[FILENAME_MAX]; // The location of the snapshot against which the logs work
static stream *wlc_fd = 0;

// These properties are needed by the replica to direct the roll-forward.
char wlc_dir[FILENAME_MAX]; 	// The location in the global file store for the logs
char wlc_name[IDLENGTH];  	// The master database name
lng  wlc_tag = 0;			// next transaction id
int  wlc_state = 0;			// The current status of the logger in the life cycle
static char wlc_write[26];			// The timestamp of the last committed transaction
int  wlc_batches = 0;		// identifier of next batch
int  wlc_beat = 10;		// maximal period covered by a single log file in seconds

/* The database snapshots are binary copies of the dbfarm/database/bat
 * New snapshots are created currently using the 'monetdb snapshot <db>' command
 * or a SQL procedure.
 *
 * The wlc logs are stored in the snapshot directory as a time-stamped list
 */

int
WLCused(void)
{
	return wlc_dir[0] != 0;
}

/* The master configuration file is a simple key=value table */
str
WLCreadConfig(FILE *fd)
{
	str msg = MAL_SUCCEED;
	char path[FILENAME_MAX];
	int len;

	while( fgets(path, FILENAME_MAX, fd) ){
		path[strlen(path)-1] = 0;
		if( strncmp("logs=", path,5) == 0) {
			len = snprintf(wlc_dir, FILENAME_MAX, "%s", path + 5);
			if (len == -1 || len >= FILENAME_MAX) {
				msg = createException(MAL, "wlc.readConfig", "logs config value is too large");
				goto bailout;
			}
		}
		if( strncmp("snapshot=", path,9) == 0) {
			len = snprintf(wlc_snapshot, FILENAME_MAX, "%s", path + 9);
			if (len == -1 || len >= FILENAME_MAX) {
				msg = createException(MAL, "wlc.readConfig", "snapshot config value is too large");
				goto bailout;
			}
		}
		if( strncmp("tag=", path,4) == 0)
			wlc_tag = atol(path+ 4);
		if( strncmp("write=", path,6) == 0) {
			len = snprintf(wlc_write, 26, "%s", path + 6);
			if (len == -1 || len >= 26) {
				msg = createException(MAL, "wlc.readConfig", "write config value is too large");
				goto bailout;
			}
		}
		if( strncmp("batches=", path, 8) == 0)
			wlc_batches = atoi(path+ 8);
		if( strncmp("beat=", path, 5) == 0)
			wlc_beat = atoi(path+ 5);
		if( strncmp("state=", path, 6) == 0)
			wlc_state = atoi(path+ 6);
	}
bailout:
	fclose(fd);
	return msg;
}

static str
WLCgetConfig(void){
	str l;
	FILE *fd;

	if((l = GDKfilepath(0,0,"wlc.config",0)) == NULL)
		throw(MAL,"wlc.getConfig","Could not access wlc.config file\n");
	fd = MT_fopen(l,"r");
	GDKfree(l);
	if( fd == NULL)
		throw(MAL,"wlc.getConfig","Could not access wlc.config file\n");
	return WLCreadConfig(fd);
}

static
str WLCsetConfig(void){
	str path;
	stream *fd;

	/* be aware to be safe, on a failing fopen */
	if((path = GDKfilepath(0,0,"wlc.config",0)) == NULL)
		throw(MAL,"wlc.setConfig","Could not access wlc.config\n");
	fd = open_wastream(path);
	GDKfree(path);
	if( fd == NULL)
		throw(MAL,"wlc.setConfig","Could not access wlc.config: %s\n", mnstr_peek_error(NULL));
	if( wlc_snapshot[0] )
		mnstr_printf(fd,"snapshot=%s\n", wlc_snapshot);
	mnstr_printf(fd,"logs=%s\n", wlc_dir);
	mnstr_printf(fd,"tag="LLFMT"\n", wlc_tag );
	mnstr_printf(fd,"write=%s\n", wlc_write );
	mnstr_printf(fd,"state=%d\n", wlc_state );
	mnstr_printf(fd,"batches=%d\n", wlc_batches );
	mnstr_printf(fd,"beat=%d\n", wlc_beat );
	(void) mnstr_flush(wlc_fd, MNSTR_FLUSH_DATA);
	(void) mnstr_fsync(wlc_fd);
	close_stream(fd);
	return MAL_SUCCEED;
}

// creation of the logger file and updating the configuration file should be atomic !!!
// The log files are marked with the database name. This allows for easy recognition later on.
static str
WLCsetlogger(void)
{
	int len;
	char path[FILENAME_MAX];
	str msg = MAL_SUCCEED;

	if( wlc_dir[0] == 0)
		throw(MAL,"wlc.setlogger","Path not initalized");
	MT_lock_set(&wlc_lock);
	len = snprintf(path,FILENAME_MAX,"%s%c%s_%012d", wlc_dir, DIR_SEP, wlc_name, wlc_batches);
	if (len == -1 || len >= FILENAME_MAX) {
		MT_lock_unset(&wlc_lock);
		throw(MAL, "wlc.setlogger", "Logger filename path is too large");
	}
	wlc_fd = open_wastream(path);
	if( wlc_fd == 0){
		MT_lock_unset(&wlc_lock);
		throw(MAL,"wlc.logger","Could not create %s\n",path);
	}

	wlc_batches++;
	msg = WLCsetConfig();
	MT_lock_unset(&wlc_lock);
	return msg;
}

/* force the current log file to its storage container */
static str
WLCcloselogger(void)
{
	if( wlc_fd == NULL)
		return MAL_SUCCEED;
	mnstr_flush(wlc_fd, MNSTR_FLUSH_DATA);
	mnstr_fsync(wlc_fd);
	close_stream(wlc_fd);
	wlc_fd= NULL;
	return WLCsetConfig();
}

/* force the current log file to its storage container, but dont create a new one yet */
static str
WLCflush(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	if( wlc_fd == NULL)
		return MAL_SUCCEED;
	mnstr_flush(wlc_fd, MNSTR_FLUSH_DATA);
	mnstr_fsync(wlc_fd);
	return WLCsetConfig();
}

static str
WLCepilogue(void *ret)
{
	str msg = MAL_SUCCEED;

	(void)ret;

	MT_lock_set(&wlc_lock);
	msg = WLCcloselogger();
	wlc_snapshot[0]=0;
	wlc_dir[0]= 0;
	wlc_name[0]= 0;
	wlc_write[0] =0;
	MT_lock_unset(&wlc_lock);
       	//TODO we have to return a possible error message somehow
	return(msg);
}

/*
 * The WLClogger process ensures that log files are properly closed
 * and released when their cycle time window has expired.
 */

static MT_Id wlc_logger;

static void
WLClogger(void *arg)
{
	int seconds;
	str msg = MAL_SUCCEED;

	(void) arg;
	while(!GDKexiting()){
		if( wlc_dir[0] && wlc_fd ){
			MT_lock_set(&wlc_lock);
			if((msg = WLCcloselogger()) != MAL_SUCCEED) {
				TRC_ERROR(MAL_WLC, "%s\n", msg);
				freeException(msg);
			}
			MT_lock_unset(&wlc_lock);
		}
		for( seconds = 0; (wlc_beat == 0 || seconds < wlc_beat) && ! GDKexiting(); seconds++)
			MT_sleep_ms( 1000);
	}
}
/*
 * The existence of the master directory should be checked upon server restart.
 * Then the master record information should be set and the WLClogger started.
 */

#ifndef F_OK
#define F_OK 0
#endif

str
WLCinit(void)
{
	str conf, msg;
	int len;

	if( wlc_state == WLC_STARTUP){
		// use default location for master configuration file
		if((conf = GDKfilepath(0,0,"wlc.config",0)) == NULL)
			throw(MAL,"wlc.init","Could not access wlc.config\n");

		if (MT_access(conf, F_OK) ){
			GDKfree(conf);
			return MAL_SUCCEED;
		}
		GDKfree(conf);
		// we are in master mode
		len = snprintf(wlc_name, IDLENGTH, "%s", GDKgetenv("gdk_dbname"));
		if (len == -1 || len >= IDLENGTH)
			throw(MAL, "wlc.init", "gdk_dbname variable is too large");

		if ((msg = WLCgetConfig()) != MAL_SUCCEED)
			return msg;
		if (MT_create_thread(&wlc_logger, WLClogger , (void*) 0,
							 MT_THR_DETACHED, "WLClogger") < 0) {
			TRC_ERROR(MAL_WLC, "Thread could not be spawned\n");
		}
	}
	return MAL_SUCCEED;
}

static str
WLCinitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCinit();
}

static str
WLCgetclock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str *ret = getArgReference_str(stk,pci,0);
	(void) cntxt;
	(void) mb;
	if( wlc_write[0])
		*ret = GDKstrdup(wlc_write);
	else
		*ret = GDKstrdup(str_nil);
	if(*ret == NULL)
		throw(MAL,"wlc.getclock", MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
WLCgettick(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	lng *ret = getArgReference_lng(stk,pci,0);
	(void) cntxt;
	(void) mb;
	*ret = wlc_tag;
	return MAL_SUCCEED;
}

/* Changing the beat should have immediate effect
 * It forces a new log file
 */
static str
WLCsetbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int beat;
	(void) mb;
	(void) cntxt;
	beat = * getArgReference_int(stk,pci,1);
	if ( beat < 0)
		throw(MAL, "wlc.setbeat", "beat should be a positive number");
	wlc_beat = beat;
	return WLCcloselogger();
}

static str
WLCgetbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int *ret = getArgReference_int(stk,pci,0);
	(void) mb;
	(void) cntxt;
	*ret = wlc_beat;
	return MAL_SUCCEED;
}

static str
WLCmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int len;
	char path[FILENAME_MAX];
	str l;

	(void) cntxt;
	(void) mb;
	if( wlc_state == WLC_STOP)
		throw(MAL,"master","WARNING: logging has been stopped. Use new snapshot");
	if( wlc_state == WLC_RUN)
		throw(MAL,"master","WARNING: already in master mode, call ignored");
	if( pci->argc == 2) {
		len = snprintf(path, FILENAME_MAX, "%s", *getArgReference_str(stk, pci,1));
		if (len == -1 || len >= FILENAME_MAX)
			throw(MAL, "wlc.master", "wlc master filename path is too large");
	} else {
		if((l = GDKfilepath(0,0,"wlc_logs",0)) == NULL)
			throw(SQL,"wlc.master", MAL_MALLOC_FAIL);
		len = snprintf(path,FILENAME_MAX,"%s%c",l, DIR_SEP);
		GDKfree(l);
		if (len == -1 || len >= FILENAME_MAX)
			throw(MAL, "wlc.master", "wlc master filename path is too large");
	}
	// set location for logs
	if( GDKcreatedir(path) != GDK_SUCCEED)
		throw(SQL,"wlc.master","Could not create %s\n", path);
	len = snprintf(wlc_name, IDLENGTH, "%s", GDKgetenv("gdk_dbname"));
	if (len == -1 || len >= IDLENGTH)
		throw(SQL,"wlc.master","gdk_dbname is too large");
	len = snprintf(wlc_dir, FILENAME_MAX, "%s", path);
	if (len == -1 || len >= FILENAME_MAX)
		throw(SQL,"wlc.master","wlc_dir directory name is too large");
	wlc_state= WLC_RUN;
	return WLCsetConfig();
}

static str
WLCstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	if( wlc_state != WLC_RUN )
		throw(MAL,"wlc.stop","WARNING: master role not active");
	wlc_state = WLC_STOP;
	return WLCsetConfig();
}

static str
WLCsettime(Client cntxt, InstrPtr pci, InstrPtr p, str fcn)
{
	timestamp ts = timestamp_current();
	str wlc_time = NULL;
	size_t wlc_limit = 0;
	InstrPtr ins;

	(void) pci;
	assert(!is_timestamp_nil(ts));
	if (timestamp_tostr(&wlc_time, &wlc_limit, &ts, true) < 0)
		throw(MAL, fcn, "Unable to retrieve current time");
	ins = pushStr(cntxt->wlc, p, wlc_time);
	GDKfree(wlc_time);
	if (ins == NULL)
		throw(MAL, fcn, MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* Beware that a client context can be used in parallel and
 * that we don't want transaction interference caused by merging
 * the MAL instructions accidentally.
 * The effectively means that the SQL transaction record should
 * collect the MAL instructions and flush them.
 */
static str
WLCpreparewrite(Client cntxt)
{	str msg = MAL_SUCCEED;
	// save the wlc record on a file

	if( cntxt->wlc == 0 || cntxt->wlc->stop <= 1 ||  cntxt->wlc_kind == WLC_QUERY )
		return MAL_SUCCEED;

	if( wlc_state != WLC_RUN){
		trimMalVariables(cntxt->wlc, NULL);
		resetMalBlk(cntxt->wlc, 0);
		cntxt->wlc_kind = WLC_QUERY;
		return MAL_SUCCEED;
	}
	if( wlc_dir[0] ){
		if (wlc_fd == NULL){
			msg = WLCsetlogger();
			if( msg) {
				return msg;
			}
		}

		MT_lock_set(&wlc_lock);
		printFunction(wlc_fd, cntxt->wlc, 0, LIST_MAL_CALL );
		(void) mnstr_flush(wlc_fd, MNSTR_FLUSH_DATA);
		(void) mnstr_fsync(wlc_fd);
		// close file if no delay is allowed
		if( wlc_beat == 0 )
			msg = WLCcloselogger();

		MT_lock_unset(&wlc_lock);
		trimMalVariables(cntxt->wlc, NULL);
		resetMalBlk(cntxt->wlc, 0);
		cntxt->wlc_kind = WLC_QUERY;
	} else
		throw(MAL,"wlc.write","WLC log path missing ");

	if( wlc_state == WLC_STOP)
		throw(MAL,"wlc.write","Logging for this snapshot has been stopped. Use a new snapshot to continue logging.");
	return msg;
}

static str
WLCstart(Client cntxt, str fcn)
{
	InstrPtr pci;
	str msg = MAL_SUCCEED;
	MalBlkPtr mb = cntxt->wlc;
	lng tag;

	if( cntxt->wlc == NULL){
		if((cntxt->wlc = newMalBlk(STMT_INCREMENT)) == NULL)
			throw(MAL, fcn, MAL_MALLOC_FAIL);
		mb = cntxt->wlc;
	}
	/* Find a single transaction sequence ending with COMMIT or ROLLBACK */
	if( mb->stop > 1 ){
		pci = getInstrPtr(mb, mb->stop -1 );
		if (!(strcmp( getFunctionId(pci), "commit") == 0 || strcmp( getFunctionId(pci), "rollback") == 0))
			return MAL_SUCCEED;
	}

	/* create the start of a new transaction block */
	MT_lock_set(&wlc_lock);
	tag = wlc_tag;
	wlc_tag++; // Update wlc administration

	pci = newStmt(mb,"wlr", "transaction");
	pci = pushLng(mb, pci, tag);
	if((msg = WLCsettime(cntxt,pci, pci, fcn)) == MAL_SUCCEED) {
		snprintf(wlc_write, 26, "%s", getVarConstant(cntxt->wlc, getArg(pci, 2)).val.sval);
		pci = pushStr(mb, pci, cntxt->username);
		pci->ticks = GDKms();
	}
	MT_lock_unset(&wlc_lock);

	return msg;
}

static str
WLCquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p;
	str msg = MAL_SUCCEED;

	(void) stk;
	if ( strcmp("-- no query",getVarConstant(mb, getArg(pci,1)).val.sval) == 0)
		return MAL_SUCCEED;	// ignore system internal queries.
	msg = WLCstart(cntxt, "wlr.query");
	if(msg)
		return msg;
	cntxt->wlc_kind = WLC_QUERY;
	p = newStmt(cntxt->wlc, "wlr","query");
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	return msg;
}

static str
WLCcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p;
	str msg = MAL_SUCCEED;

	(void) stk;
	msg = WLCstart(cntxt, "wlr.catalog");
	if(msg)
		return msg;
	cntxt->wlc_kind = WLC_CATALOG;
	p = newStmt(cntxt->wlc, "wlr","catalog");
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	return msg;
}

static str
WLCaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p;
	str msg = MAL_SUCCEED;

	(void) stk;
	msg = WLCstart(cntxt, "wlr.action");
	if(msg)
		return msg;
	cntxt->wlc_kind = WLC_UPDATE;
	p = newStmt(cntxt->wlc, "wlr","action");
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	return msg;
}

/*
 * We actually don't need the catalog operations in the log.
 * It is sufficient to upgrade the replay block to WLR_CATALOG.
 */
static str
WLCgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p;
	int i, k,  tpe, varid;
	str msg = MAL_SUCCEED;

	(void) stk;
	msg = WLCstart(cntxt, "wlr.generic");
	if(msg)
		return msg;
	cntxt->wlc_kind = WLC_IGNORE;
	p = newInstruction(cntxt->wlc, "wlr",getFunctionId(pci));
	k = newTmpVariable(mb,TYPE_any);
	if( k >= 0)
		getArg(p,0) =  k;
	for( i = pci->retc; i< pci->argc; i++){
		tpe =getArgType(mb, pci, i);
		switch(tpe){
		case TYPE_str:
			k = defConstant(mb,TYPE_str,&getVarConstant(mb, getArg(pci, i)));
			if( k >= 0)
				p = addArgument(cntxt->wlc, p, k);
			break;
		default:
			varid = defConstant(cntxt->wlc, tpe, getArgReference(stk, pci, i));
			if( varid >= 0)
				p = addArgument(cntxt->wlc, p, varid);
		}
	}
	p->ticks = GDKms();
	pushInstruction(mb,p);
	cntxt->wlc_kind = WLC_CATALOG;
	return 	msg;
}

#define bulk(TPE1, TPE2)\
{	TPE1 *p = (TPE1 *) Tloc(b,0);\
	TPE1 *q = (TPE1 *) Tloc(b, BUNlast(b));\
	int k=0; \
	for( ; p < q; p++, k++){\
		if( k % 32 == 31){\
			pci = newStmt(cntxt->wlc, "wlr",getFunctionId(pci));\
			pci = pushStr(cntxt->wlc, pci, sch);\
			pci = pushStr(cntxt->wlc, pci, tbl);\
			pci = pushStr(cntxt->wlc, pci, col);\
			pci->ticks = GDKms();\
		}\
		pci = push##TPE2(cntxt->wlc, pci ,*p);\
} }

#define updateBatch(TPE1,TPE2)\
{	TPE1 *x = (TPE1 *) Tloc(bval,0);\
	TPE1 *y = (TPE1 *) Tloc(bval, BUNlast(b));\
	int k=0; \
	for( ; x < y; x++, k++){\
		p = newStmt(cntxt->wlc, "wlr","update");\
		p = pushStr(cntxt->wlc, p, sch);\
		p = pushStr(cntxt->wlc, p, tbl);\
		p = pushStr(cntxt->wlc, p, col);\
		p = pushOid(cntxt->wlc, p,  (ol? *ol++: o++));\
		p = push##TPE2(cntxt->wlc, p ,*x);\
} }

static str
WLCdatashipping(Client cntxt, MalBlkPtr mb, InstrPtr pci, int bid)
{	BAT *b;
	str sch, tbl, col;
	str msg = MAL_SUCCEED;
	(void) mb;

	b = BATdescriptor(bid);
	if (b == NULL) {
		throw(MAL, "wlc.datashipping", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

// large BATs can also be re-created using the query.
// Copy into should always be expanded, because the source may not
// be accessible in the replica. TODO

	sch = GDKstrdup(getVarConstant(cntxt->wlc, getArg(pci,1)).val.sval);
	tbl = GDKstrdup(getVarConstant(cntxt->wlc, getArg(pci,2)).val.sval);
	col = GDKstrdup(getVarConstant(cntxt->wlc, getArg(pci,3)).val.sval);
	if(!sch || !tbl || !col) {
		msg = createException(MAL, "wlc.datashipping", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto finish;
	}
	if (cntxt->wlc_kind < WLC_UPDATE)
		cntxt->wlc_kind = WLC_UPDATE;
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
					pci = newStmt(cntxt->wlc, "wlr",getFunctionId(pci));
					pci = pushStr(cntxt->wlc, pci, sch);
					pci = pushStr(cntxt->wlc, pci, tbl);
					pci = pushStr(cntxt->wlc, pci, col);
				}
				k++;
				pci = pushStr(cntxt->wlc, pci ,(str) BUNtvar(bi,p));
		} }
		break;
	default:
		TRC_ERROR(MAL_WLC, "Non-supported type: %d\n", ATOMstorage(b->ttype));
		cntxt->wlc_kind = WLC_CATALOG;
	}
finish:
	BBPunfix(b->batCacheid);
	if (sch)
		GDKfree(sch);
	if (tbl)
		GDKfree(tbl);
	if (col)
		GDKfree(col);
	return msg;
}

static str
WLCappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p;
	int tpe, varid;
	str msg = MAL_SUCCEED;

	(void) stk;
	(void) mb;
	msg = WLCstart(cntxt, "wlr.append");
	if(msg)
		return msg;
	p = newStmt(cntxt->wlc, "wlr","append");
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,3)).val.sval);

	// extend the instructions with all values.
	// If this become too large we can always switch to a "catalog" mode
	// forcing re-execution instead
	tpe= getArgType(mb,pci,4);
	if (isaBatType(tpe) ){
		// actually check the size of the BAT first, most have few elements
		msg = WLCdatashipping(cntxt, mb, p, stk->stk[getArg(pci,4)].val.bval);
	} else {
		ValRecord cst;
		if (VALcopy(&cst, getArgReference(stk,pci,4)) != NULL){
			varid = defConstant(cntxt->wlc, tpe, &cst);
			if( varid >=0)
				p = pushArgument(cntxt->wlc, p, varid);
		}
	}
	if( cntxt->wlc_kind < WLC_UPDATE)
		cntxt->wlc_kind = WLC_UPDATE;

	return msg;
}

/* check for empty BATs first */
static str
WLCdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;
	int tpe, k = 0;
	int bid =  stk->stk[getArg(pci,3)].val.bval;
	oid o=0, last, *ol;
	BAT *b;
	str msg = MAL_SUCCEED;

	(void) stk;
	(void) mb;
	b= BBPquickdesc(bid, false);
	if( BATcount(b) == 0)
		return MAL_SUCCEED;
	msg = WLCstart(cntxt, "wlr.delete");
	if(msg) {
		BBPunfix(b->batCacheid);
		return msg;
	}
	cntxt->wlc_kind = WLC_UPDATE;
	p = newStmt(cntxt->wlc, "wlr","delete");
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,2)).val.sval);

	tpe= getArgType(mb,pci,3);
	if (isaBatType(tpe) ){
		b= BATdescriptor(bid);
		if (b == NULL)
			throw(MAL, "wlc.delete", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		o = b->tseqbase;
		last = o + BATcount(b);
		if( b->ttype == TYPE_void){
			for( ; o < last; o++, k++){
				if( k % 32 == 31){
					p = newStmt(cntxt->wlc, "wlr","delete");
					p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,1)).val.sval);
					p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,2)).val.sval);
				}
				p = pushOid(cntxt->wlc,p, o);
			}
		} else {
			ol = (oid*) Tloc(b,0);
			for( ; o < last; o++, k++, ol++){
				if( k % 32 == 31){
					p = newStmt(cntxt->wlc, "wlr","delete");
					p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,1)).val.sval);
					p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,2)).val.sval);
				}
				p = pushOid(cntxt->wlc,p, *ol);
			}
		}
		BBPunfix(b->batCacheid);
	} else
		throw(MAL,"wlc.delete","BAT expected");
	if( cntxt->wlc_kind < WLC_UPDATE)
		cntxt->wlc_kind = WLC_UPDATE;

	return msg;
}

static str
WLCupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;
	str sch,tbl,col, msg = MAL_SUCCEED;
	ValRecord cst;
	int tpe, varid;
	oid o = 0, *ol = 0;

	sch = *getArgReference_str(stk,pci,1);
	tbl = *getArgReference_str(stk,pci,2);
	col = *getArgReference_str(stk,pci,3);
	msg = WLCstart(cntxt, "wlr.update");
	if(msg)
		return msg;
	cntxt->wlc_kind = WLC_UPDATE;
	tpe= getArgType(mb,pci,5);
	if (isaBatType(tpe) ){
		BAT *b, *bval;
		b= BATdescriptor(stk->stk[getArg(pci,4)].val.bval);
		bval= BATdescriptor(stk->stk[getArg(pci,5)].val.bval);
		if(b == NULL || bval == NULL) {
			if(b)
				BBPunfix(b->batCacheid);
			if(bval)
				BBPunfix(bval->batCacheid);
			throw(MAL, "wlr.update", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
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
				p = newStmt(cntxt->wlc, "wlr","update");
				p = pushStr(cntxt->wlc, p, sch);
				p = pushStr(cntxt->wlc, p, tbl);
				p = pushStr(cntxt->wlc, p, col);
				p = pushOid(cntxt->wlc, p, (ol? *ol++ : o++));
				p = pushStr(cntxt->wlc, p , BUNtvar(bi,x));
				k++;
		} }
		/* fall through */
		default:
			cntxt->wlc_kind = WLC_CATALOG;
		}
		BBPunfix(b->batCacheid);
	} else {
		p = newStmt(cntxt->wlc, "wlr","update");
		p = pushStr(cntxt->wlc, p, sch);
		p = pushStr(cntxt->wlc, p, tbl);
		p = pushStr(cntxt->wlc, p, col);
		o = *getArgReference_oid(stk,pci,4);
		p = pushOid(cntxt->wlc,p, o);
		if (VALcopy(&cst, getArgReference(stk,pci,5)) != NULL){
			varid = defConstant(cntxt->wlc, tpe, &cst);
			if( varid >= 0)
				p = pushArgument(cntxt->wlc, p, varid);
		}
	}

	if( cntxt->wlc_kind < WLC_UPDATE)
		cntxt->wlc_kind = WLC_UPDATE;
	return msg;
}

static str
WLCclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p;
	str msg = MAL_SUCCEED;
	(void) stk;
	msg = WLCstart(cntxt, "wlr.clear_table");
	if(msg)
		return msg;
	cntxt->wlc_kind = WLC_UPDATE;
	p = newStmt(cntxt->wlc, "wlr","clear_table");
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlc, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	if( cntxt->wlc_kind < WLC_UPDATE)
		cntxt->wlc_kind = WLC_UPDATE;

	return msg;
}

str
WLCcommit(int clientid)
{
	if( mal_clients[clientid].wlc && mal_clients[clientid].wlc->stop > 1){
		newStmt(mal_clients[clientid].wlc,"wlr","commit");
		return WLCpreparewrite( &mal_clients[clientid]);
	}
	return MAL_SUCCEED;
}

static str
WLCcommitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg = MAL_SUCCEED;
	msg = WLCstart(cntxt, "wlr.commit");
	if(msg)
		return msg;
	(void) mb;
	(void) stk;
	(void) pci;
	cntxt->wlc_kind = WLC_UPDATE;
	return WLCcommit(cntxt->idx);
}

str
WLCrollback(int clientid)
{
	if( mal_clients[clientid].wlc){
		newStmt(mal_clients[clientid].wlc,"wlr","rollback");
		return WLCpreparewrite( &mal_clients[clientid]);
	}
	return MAL_SUCCEED;
}

static str
WLCrollbackCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	str msg = MAL_SUCCEED;
	msg = WLCstart(cntxt, "wlr.rollback");
	if(msg)
		return msg;
	(void) mb;
	(void) stk;
	(void) pci;
	cntxt->wlc_kind = WLC_UPDATE;
	return WLCrollback(cntxt->idx);
}

#include "mel.h"
mel_func wlc_init_funcs[] = {
 pattern("wlc", "init", WLCinitCmd, false, "Test for running as master", noargs),
 command("wlc", "epilogue", WLCepilogue, false, "release the resources held by the wlc module", args(1,1, arg("",void))),
 pattern("wlc", "master", WLCmaster, false, "Activate the workload-capture-replay process", noargs),
 pattern("wlc", "master", WLCmaster, false, "Activate the workload-capture-replay process. Use a different location for the logs.", args(0,1, arg("path",str))),
 pattern("wlc", "stop", WLCstop, false, "Stop capturing the logs", noargs),
 pattern("wlc", "flush", WLCflush, false, "Flush current log buffer", noargs),
 pattern("wlc", "setbeat", WLCsetbeat, false, "Maximal delay for transaction log flushing", args(0,1, arg("duration",int))),
 pattern("wlc", "getbeat", WLCgetbeat, false, "Maximal delay for transaction log flushing", args(1,2, arg("",str),arg("duration",int))),
 pattern("wlc", "getclock", WLCgetclock, false, "Timestamp of last update transaction", args(1,1, arg("",str))),
 pattern("wlc", "gettick", WLCgettick, false, "Transaction identifier of the last committed transaction", args(1,1, arg("",lng))),
 pattern("wlc", "rollback", WLCrollbackCmd, false, "Mark the end of the work unit", noargs),
 pattern("wlc", "commit", WLCcommitCmd, false, "Mark the end of the work unit", noargs),
 pattern("wlc", "query", WLCquery, false, "Keep the queries for replay.", args(0,1, arg("q",str))),
 pattern("wlc", "catalog", WLCcatalog, false, "Keep the catalog changing queries for replay. ", args(0,1, arg("q",str))),
 pattern("wlc", "action", WLCaction, false, "Keep the database changing queries for replay. ", args(0,1, arg("q",str))),
 pattern("wlc", "append", WLCappend, false, "Keep the insertions in the workload-capture-replay list", args(1,5, arg("",int),arg("sname",str),arg("tname",str),arg("cname",str),argany("ins",0))),
 pattern("wlc", "update", WLCupdate, false, "Keep the update in the workload-capture-replay list", args(1,6, arg("",int),arg("sname",str),arg("tname",str),arg("cname",str),argany("tid",0),argany("val",0))),
 pattern("wlc", "delete", WLCdelete, false, "Keep the deletions in the workload-capture-replay list", args(1,4, arg("",int),arg("sname",str),arg("tname",str),argany("b",0))),
 pattern("wlc", "clear_table", WLCclear_table, false, "Keep the deletions in the workload-capture-replay list", args(1,3, arg("",int),arg("sname",str),arg("tname",str))),
 pattern("wlc", "commit", WLCcommitCmd, false, "Commit the workload-capture-replay record", noargs),
 pattern("wlc", "rollback", WLCcommitCmd, false, "Rollback the workload-capture-replay record", noargs),
 pattern("wlc", "create_seq", WLCgeneric, false, "Catalog operation create_seq", args(0,3, arg("sname",str),arg("seqname",str),arg("action",int))),
 pattern("wlc", "alter_seq", WLCgeneric, false, "Catalog operation alter_seq", args(0,3, arg("sname",str),arg("seqname",str),arg("val",lng))),
 pattern("wlc", "alter_seq", WLCgeneric, false, "Catalog operation alter_seq", args(0,4, arg("sname",str),arg("seqname",str),arg("seq",ptr),batarg("val",lng))),
 pattern("wlc", "drop_seq", WLCgeneric, false, "Catalog operation drop_seq", args(0,3, arg("sname",str),arg("nme",str),arg("action",int))),
 pattern("wlc", "create_schema", WLCgeneric, false, "Catalog operation create_schema", args(0,3, arg("sname",str),arg("auth",str),arg("action",int))),
 pattern("wlc", "drop_schema", WLCgeneric, false, "Catalog operation drop_schema", args(0,3, arg("sname",str),arg("ifexists",int),arg("action",int))),
 pattern("wlc", "create_table", WLCgeneric, false, "Catalog operation create_table", args(0,3, arg("sname",str),arg("tname",str),arg("temp",int))),
 pattern("wlc", "create_view", WLCgeneric, false, "Catalog operation create_view", args(0,3, arg("sname",str),arg("tname",str),arg("temp",int))),
 pattern("wlc", "drop_table", WLCgeneric, false, "Catalog operation drop_table", args(0,4, arg("sname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("wlc", "drop_view", WLCgeneric, false, "Catalog operation drop_view", args(0,4, arg("sname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("wlc", "drop_constraint", WLCgeneric, false, "Catalog operation drop_constraint", args(0,5, arg("sname",str),arg("tname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("wlc", "alter_table", WLCgeneric, false, "Catalog operation alter_table", args(0,3, arg("sname",str),arg("tname",str),arg("action",int))),
 pattern("wlc", "create_type", WLCgeneric, false, "Catalog operation create_type", args(0,3, arg("sname",str),arg("nme",str),arg("impl",str))),
 pattern("wlc", "drop_type", WLCgeneric, false, "Catalog operation drop_type", args(0,3, arg("sname",str),arg("nme",str),arg("action",int))),
 pattern("wlc", "grant_roles", WLCgeneric, false, "Catalog operation grant_roles", args(0,4, arg("sname",str),arg("auth",str),arg("grantor",int),arg("admin",int))),
 pattern("wlc", "revoke_roles", WLCgeneric, false, "Catalog operation revoke_roles", args(0,4, arg("sname",str),arg("auth",str),arg("grantor",int),arg("admin",int))),
 pattern("wlc", "grant", WLCgeneric, false, "Catalog operation grant", args(0,7, arg("sname",str),arg("tbl",str),arg("grantee",str),arg("privs",int),arg("cname",str),arg("gr",int),arg("grantor",int))),
 pattern("wlc", "revoke", WLCgeneric, false, "Catalog operation revoke", args(0,7, arg("sname",str),arg("tbl",str),arg("grantee",str),arg("privs",int),arg("cname",str),arg("grant",int),arg("grantor",int))),
 pattern("wlc", "grant_function", WLCgeneric, false, "Catalog operation grant_function", args(0,6, arg("sname",str),arg("fcnid",int),arg("grantee",str),arg("privs",int),arg("grant",int),arg("grantor",int))),
 pattern("wlc", "revoke_function", WLCgeneric, false, "Catalog operation revoke_function", args(0,6, arg("sname",str),arg("fcnid",int),arg("grantee",str),arg("privs",int),arg("grant",int),arg("grantor",int))),
 pattern("wlc", "create_user", WLCgeneric, false, "Catalog operation create_user", args(0,5, arg("sname",str),arg("passwrd",str),arg("enc",int),arg("schema",str),arg("fullname",str))),
 pattern("wlc", "drop_user", WLCgeneric, false, "Catalog operation drop_user", args(0,2, arg("sname",str),arg("action",int))),
 pattern("wlc", "drop_user", WLCgeneric, false, "Catalog operation drop_user", args(0,3, arg("sname",str),arg("auth",str),arg("action",int))),
 pattern("wlc", "alter_user", WLCgeneric, false, "Catalog operation alter_user", args(0,5, arg("sname",str),arg("passwrd",str),arg("enc",int),arg("schema",str),arg("oldpasswrd",str))),
 pattern("wlc", "rename_user", WLCgeneric, false, "Catalog operation rename_user", args(0,3, arg("sname",str),arg("newnme",str),arg("action",int))),
 pattern("wlc", "create_role", WLCgeneric, false, "Catalog operation create_role", args(0,3, arg("sname",str),arg("role",str),arg("grator",int))),
 pattern("wlc", "drop_role", WLCgeneric, false, "Catalog operation drop_role", args(0,3, arg("auth",str),arg("role",str),arg("action",int))),
 pattern("wlc", "drop_role", WLCgeneric, false, "Catalog operation drop_role", args(0,2, arg("role",str),arg("action",int))),
 pattern("wlc", "drop_index", WLCgeneric, false, "Catalog operation drop_index", args(0,3, arg("sname",str),arg("iname",str),arg("action",int))),
 pattern("wlc", "drop_function", WLCgeneric, false, "Catalog operation drop_function", args(0,5, arg("sname",str),arg("fname",str),arg("fid",int),arg("type",int),arg("action",int))),
 pattern("wlc", "create_function", WLCgeneric, false, "Catalog operation create_function", args(0,2, arg("sname",str),arg("fname",str))),
 pattern("wlc", "create_trigger", WLCgeneric, false, "Catalog operation create_trigger", args(0,10, arg("sname",str),arg("tname",str),arg("triggername",str),arg("time",int),arg("orientation",int),arg("event",int),arg("old",str),arg("new",str),arg("cond",str),arg("qry",str))),
 pattern("wlc", "drop_trigger", WLCgeneric, false, "Catalog operation drop_trigger", args(0,3, arg("sname",str),arg("nme",str),arg("ifexists",int))),
 pattern("wlc", "alter_add_table", WLCgeneric, false, "Catalog operation alter_add_table", args(0,5, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("action",int))),
 pattern("wlc", "alter_del_table", WLCgeneric, false, "Catalog operation alter_del_table", args(0,5, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("action",int))),
 pattern("wlc", "alter_set_table", WLCgeneric, false, "Catalog operation alter_set_table", args(0,3, arg("sname",str),arg("tnme",str),arg("access",int))),
 pattern("wlc", "alter_add_range_partition", WLCgeneric, false, "Catalog operation alter_add_range_partition", args(0,8, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("min",str),arg("max",str),arg("nills",bit),arg("update",int))),
 pattern("wlc", "comment_on", WLCgeneric, false, "Catalog operation comment_on", args(0,2, arg("objid",int),arg("remark",str))),
 pattern("wlc", "rename_schema", WLCgeneric, false, "Catalog operation rename_schema", args(0,2, arg("sname",str),arg("newnme",str))),
 pattern("wlc", "rename_table", WLCgeneric, false, "Catalog operation rename_table", args(0,4, arg("osname",str),arg("nsname",str),arg("otname",str),arg("ntname",str))),
 pattern("wlc", "rename_column", WLCgeneric, false, "Catalog operation rename_column", args(0,4, arg("sname",str),arg("tname",str),arg("cname",str),arg("newnme",str))),
 pattern("wlc", "transaction_release", WLCgeneric, false, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("wlc", "transaction_commit", WLCgeneric, false, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("wlc", "transaction_rollback", WLCgeneric, false, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("wlc", "transaction_begin", WLCgeneric, false, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("wlc", "transaction", WLCgeneric, true, "Start an autocommit transaction", noargs),
 pattern("wlc", "alter_add_value_partition", WLCgeneric, false, "Catalog operation alter_add_value_partition", args(0,6, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int))),
 pattern("wlc", "alter_add_value_partition", WLCgeneric, false, "Catalog operation alter_add_value_partition", args(0,7, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int),vararg("arg",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_wlc_mal)
{ mal_module("wlc", NULL, wlc_init_funcs); }
