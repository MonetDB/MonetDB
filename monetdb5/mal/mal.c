/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/* (author) M. Kersten */
#include "monetdb_config.h"
#include "mal.h"

char 	monet_cwd[FILENAME_MAX] = { 0 };
size_t 	monet_memory = 0;
char 	monet_characteristics[4096];
int		mal_trace;		/* enable profile events on console */
str     mal_session_uuid;   /* unique marker for the session */

#ifdef HAVE_HGE
int have_hge;
#endif

#include "mal_stack.h"
#include "mal_linker.h"
#include "mal_authorize.h"
#include "mal_session.h"
#include "mal_scenario.h"
#include "mal_parser.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"  /* for initNamespace() */
#include "mal_client.h"
#include "mal_sabaoth.h"
#include "mal_dataflow.h"
#include "mal_profiler.h"
#include "mal_private.h"
#include "mal_runtime.h"
#include "mal_resource.h"
#include "wlc.h"
#include "mal_atom.h"
#include "opt_pipes.h"
#include "tablet.h"

MT_Lock     mal_contextLock MT_LOCK_INITIALIZER("mal_contextLock");
MT_Lock     mal_namespaceLock MT_LOCK_INITIALIZER("mal_namespaceLk");
MT_Lock     mal_remoteLock MT_LOCK_INITIALIZER("mal_remoteLock");
MT_Lock  	mal_profileLock MT_LOCK_INITIALIZER("mal_profileLock");
MT_Lock     mal_copyLock MT_LOCK_INITIALIZER("mal_copyLock");
MT_Lock     mal_delayLock MT_LOCK_INITIALIZER("mal_delayLock");
MT_Lock     mal_beatLock MT_LOCK_INITIALIZER("mal_beatLock");
MT_Lock     mal_oltpLock MT_LOCK_INITIALIZER("mal_oltpLock");

/*
 * Initialization of the MAL context
 */

int mal_init(void){
#ifdef NEED_MT_LOCK_INIT
	static bool initialized = false;
	if (!initialized) {
		MT_lock_init( &mal_contextLock, "mal_contextLock");
		MT_lock_init( &mal_namespaceLock, "mal_namespaceLock");
		MT_lock_init( &mal_remoteLock, "mal_remoteLock");
		MT_lock_init( &mal_profileLock, "mal_profileLock");
		MT_lock_init( &mal_copyLock, "mal_copyLock");
		MT_lock_init( &mal_delayLock, "mal_delayLock");
		MT_lock_init( &mal_beatLock, "mal_beatLock");
		MT_lock_init( &mal_oltpLock, "mal_oltpLock");
		initialized = true;
	}
#endif

/* Any error encountered here terminates the process
 * with a message sent to stderr
 */
	MCinit();
	mdbInit();
	monet_memory = MT_npages() * MT_pagesize();
	initNamespace();
	initParser();
#ifndef HAVE_EMBEDDED
	initHeartbeat();
#endif
	initResource();
	malBootstrap();
	initProfiler();
	initTablet();
	return 0;
}

/*
 * Upon exit we should attempt to remove all allocated memory explicitly.
 * This seemingly superflous action is necessary to simplify analyis of
 * memory leakage problems later ons and to allow an embedded server to
 * restart the server properly.
 * 
 * It is the responsibility of the enclosing application to finish/cease all
 * activity first.
 * This function should be called after you have issued sql_reset();
 */
void cleanOptimizerPipe(void);

void mserver_reset(void)
{
	str err = 0;

	GDKprepareExit();
	WLCreset();
	MCstopClients(0);
	setHeartbeat(-1);
	stopProfiler();
	AUTHreset(); 
	if ((err = msab_wildRetreat()) != NULL) {
		fprintf(stderr, "!%s", err);
		free(err);
	}
	if ((err = msab_registerStop()) != NULL) {
		fprintf(stderr, "!%s", err);
		free(err);
	}
	/* TODO: make sure this is still required
#ifdef HAVE_EMBEDDED
	MTIMEreset();
#endif
*/
	mal_factory_reset();
	mal_dataflow_reset();
	THRdel(mal_clients->mythread);
	GDKfree(mal_clients->errbuf);
	mal_clients->fdin->s = NULL;
	bstream_destroy(mal_clients->fdin);
	GDKfree(mal_clients->prompt);
	GDKfree(mal_clients->username);
	freeStack(mal_clients->glb);
	if (mal_clients->usermodule/* && strcmp(mal_clients->usermodule->name,"user")==0*/)
		freeModule(mal_clients->usermodule);

	mal_clients->fdin = 0;
	mal_clients->prompt = 0;
	mal_clients->username = 0;
	mal_clients->curprg = 0;
	mal_clients->usermodule = 0;

	mal_client_reset();
  	mal_linker_reset();
	mal_resource_reset();
	mal_runtime_reset();
	mal_module_reset();
	mal_atom_reset();
	opt_pipes_reset();
	mdbExit();
	GDKfree(mal_session_uuid);
	mal_session_uuid = NULL;

	memset((char*)monet_cwd,0, sizeof(monet_cwd));
	monet_memory = 0;
	memset((char*)monet_characteristics,0, sizeof(monet_characteristics));
	mal_trace = 0;
	mal_namespace_reset();
	/* No need to clean up the namespace, it will simply be extended
	 * upon restart mal_namespace_reset(); */
	GDKreset(0);	// terminate all other threads
}


/* stopping clients should be done with care, as they may be in the mids of
 * transactions. One safe place is between MAL instructions, which would
 * abort the transaction by raising an exception. All non-console sessions are
 * terminate this way.
 * We should also ensure that no new client enters the scene while shutting down.
 * For this we mark the client records as BLOCKCLIENT.
 *
 * Beware, mal_exit is also called during a SIGTERM from the monetdb tool
 */

void mal_exit(int status)
{
	mserver_reset();
	exit(status);				/* properly end GDK */
}
