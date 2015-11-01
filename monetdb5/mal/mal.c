/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* (author) M. Kersten */
#include <monetdb_config.h>
#include <mal.h>

char monet_cwd[PATHLENGTH] = { 0 };
size_t monet_memory;
char 	monet_characteristics[PATHLENGTH];
int mal_trace;		/* enable profile events on console */
#ifdef HAVE_HGE
int have_hge;
#endif

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
#include "mal_private.h"

MT_Lock     mal_contextLock MT_LOCK_INITIALIZER("mal_contextLock");
MT_Lock     mal_namespaceLock MT_LOCK_INITIALIZER("mal_namespaceLock");
MT_Lock     mal_remoteLock MT_LOCK_INITIALIZER("mal_remoteLock");
MT_Lock  	mal_profileLock MT_LOCK_INITIALIZER("mal_profileLock");
MT_Lock     mal_copyLock MT_LOCK_INITIALIZER("mal_copyLock");
MT_Lock     mal_delayLock MT_LOCK_INITIALIZER("mal_delayLock");
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
	val= (ptr) & v.val.ival; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.oval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.pval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.fval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.dval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.lval; if(val != base){ allAligned = -1; }
#ifdef HAVE_HGE
	val= (ptr) & v.val.hval; if(val != base){ allAligned = -1; }
#endif
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
	RECYCLEinit();
	if( malBootstrap() == 0)
		return -1;
	/* set up the profiler if needed, output sent to console */
	/* Use the same shortcuts as stethoscope */
	if ( mal_trace ) {
		openProfilerStream(mal_clients[0].fdout);
		startProfiler(mal_clients[0].user,1,0);
	} 
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
	setHeartbeat(0);
	stopMALdataflow();
	stopProfiler();
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
