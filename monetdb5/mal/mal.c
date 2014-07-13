/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/* (author) M. Kersten */
#include <monetdb_config.h>
#include <mal.h>

char monet_cwd[PATHLENGTH] = { 0 };
size_t monet_memory;
char *mal_trace;		/* enable profile events on console */

#include "mal_stack.h"
#include "mal_linker.h"
#include "mal_session.h"
#include "mal_parser.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"  /* for initNamespace() */
#include "mal_client.h"
#include "mal_sabaoth.h"
#include "mal_recycle.h"
#include "mal_dataflow.h"
#include "mal_profiler.h"
#include "mal_http_daemon.h"
#include "mal_private.h"

MT_Lock     mal_contextLock MT_LOCK_INITIALIZER("mal_contextLock");
MT_Lock     mal_namespaceLock MT_LOCK_INITIALIZER("mal_namespaceLock");
MT_Lock     mal_remoteLock MT_LOCK_INITIALIZER("mal_remoteLock");
MT_Lock  	mal_profileLock MT_LOCK_INITIALIZER("mal_profileLock");
MT_Lock     mal_copyLock MT_LOCK_INITIALIZER("mal_copyLock");
MT_Lock     mal_delayLock MT_LOCK_INITIALIZER("mal_delayLock");
MT_Sema		mal_parallelism;
/*
 * Initialization of the MAL context
 * The compiler directive STRUCT_ALIGNED tells that the
 * fields in the VALrecord all start at the same offset.
 * This knowledge avoids low-level type decodings, but should
 * be assured at least once for each platform.
 */

static
void tstAligned(void)
{
#ifdef STRUCT_ALIGNED
	int allAligned=0;
	ValRecord v;
	ptr val, base;
	base = (ptr) & v.val.ival;
	val= (ptr) & v.val.bval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.btval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.shval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.bval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.ival; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.oval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.pval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.fval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.dval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.lval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.sval; if(val != base){ allAligned = -1; }
	if(allAligned<0)
	    GDKfatal("Recompile with STRUCT_ALIGNED flag disabled\n");
#endif
}
int mal_init(void){
#ifdef NEED_MT_LOCK_INIT
	MT_lock_init( &mal_contextLock, "mal_contextLock");
	MT_lock_init( &mal_namespaceLock, "mal_namespaceLock");
	MT_lock_init( &mal_remoteLock, "mal_remoteLock");
	MT_lock_init( &mal_profileLock, "mal_profileLock");
	MT_lock_init( &mal_copyLock, "mal_copyLock");
	MT_lock_init( &mal_delayLock, "mal_delayLock");
#endif
	/* "/2" is arbitrarily used / chosen, as on systems with
	 * hyper-threading enabled, using all hardware threads rather than
	 * "only" all physical cores does not necessarily yield a linear
	 * performance benefit */
	MT_sema_init( &mal_parallelism, (GDKnr_threads > 1 ? GDKnr_threads/2: 1), "mal_parallelism");

	tstAligned();
	MCinit();
	if (mdbInit()) 
		return -1;
	if (monet_memory == 0)
		monet_memory = MT_npages() * MT_pagesize();
	initNamespace();
	initParser();
	initHeartbeat();
	initResource();
#ifdef HAVE_JSONSTORE
	startHttpdaemon();
#endif
	RECYCLEinit();
	if( malBootstrap() == 0)
		return -1;
	/* set up the profiler if needed, output sent to console */
	/* Use the same shortcuts as stethoscope */
	if ( mal_trace && *mal_trace) {
		char *s;
		setFilterAll();
		openProfilerStream(mal_clients[0].fdout);
		for ( s= mal_trace; *s; s++)
		switch(*s){
		case 'a': activateCounter("aggregate");break;
		case 'b': activateCounter("rbytes");
				activateCounter("wbytes");break;
		case 'c': activateCounter("cpu");break;
		case 'e': activateCounter("event");break;
		case 'f': activateCounter("function");break;
		case 'i': activateCounter("pc");break;
		case 'm': activateCounter("memory");break;
		case 'p': activateCounter("process");break;
		case 'r': activateCounter("reads");break;
		case 's': activateCounter("stmt");break;
		case 't': activateCounter("ticks");break;
		case 'u': activateCounter("user");break;
		case 'w': activateCounter("writes");break;
		case 'y': activateCounter("type");break;
		case 'D': activateCounter("dot");break;
		case 'I': activateCounter("thread");break; 
		case 'T': activateCounter("time");break;
		case 'S': activateCounter("start");
		}
		startProfiling();
	} else mal_trace =0;
	return 0;
}
/*
 * Upon exit we should attempt to remove all allocated memory explicitly.
 * This seemingly superflous action is necessary to simplify analyis of
 * memory leakage problems later on.
 */

/* stopping clients should be done with care, as they may be in the mids of
 * transactions. One safe place is between MAL instructions, which would
 * abort the transaction by raising an exception. All non-console sessions are
 * terminate this way.
 * We should also ensure that no new client enters the scene while shutting down.
 * For this we mark the client records as BLOCKCLIENT.
 *
 * Beware, mal_exit is also called during a SIGTERM from the monetdb tool
 */

void mal_exit(void){
	str err;

	/*
	 * Before continuing we should make sure that all clients
	 * (except the console) have left the scene.
	 */
	MCstopClients(0);
#if 0
{
	int reruns=0, go_on;
	do{
		if ( (go_on = MCactiveClients()) )
			MT_sleep_ms(1000);
		mnstr_printf(mal_clients->fdout,"#MALexit: %d clients still active\n", go_on);
	} while (++reruns < SERVERSHUTDOWNDELAY && go_on > 1);
}
#endif
	stopHeartbeat();
#ifdef HAVE_JSONSTORE
	stopHttpdaemon();
#endif
	stopMALdataflow();
	stopProfiling();
	RECYCLEdrop(mal_clients); /* remove any left over intermediates */
	unloadLibraries();
#if 0
	/* skip this to solve random crashes, needs work */
	freeModuleList(mal_clients->nspace);

	finishNamespace();
	if( mal_clients->prompt)
		GDKfree(mal_clients->prompt);
	if( mal_clients->errbuf)
		GDKfree(mal_clients->errbuf);
	if( mal_clients->bak)
		GDKfree(mal_clients->bak);
	if( mal_clients->fdin){
		/* missing protection against closing stdin stream */
		(void) mnstr_close(mal_clients->fdin->s);
		(void) bstream_destroy(mal_clients->fdin);
	}
	if( mal_clients->fdout && mal_clients->fdout != GDKstdout) {
		(void) mnstr_close(mal_clients->fdout);
		(void) mnstr_destroy(mal_clients->fdout);
	}
#endif
	/* deregister everything that was registered, ignore errors */
	if ((err = msab_wildRetreat()) != NULL) {
		fprintf(stderr, "!%s", err);
		free(err);
	}
	/* the server will now be shut down */
	if ((err = msab_registerStop()) != NULL) {
		fprintf(stderr, "!%s", err);
		free(err);
	}
	GDKexit(0); 	/* properly end GDK */
}
