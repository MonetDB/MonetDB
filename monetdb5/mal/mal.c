/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* (author) M. Kersten */
#include "monetdb_config.h"
#include "mal.h"

char 	monet_cwd[FILENAME_MAX] = { 0 };
char 	monet_characteristics[4096];
stream *maleventstream = 0;

/* The compile time debugging flags are turned into bit masks, akin to GDK */
lng MALdebug;

#include "mal_stack.h"
#include "mal_linker.h"
#include "mal_authorize.h"
#include "mal_session.h"
#include "mal_scenario.h"
#include "mal_parser.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"  /* for initNamespace() */
#include "mal_profiler.h"
#include "mal_client.h"
#include "msabaoth.h"
#include "mal_dataflow.h"
#include "mal_private.h"
#include "mal_runtime.h"
#include "mal_resource.h"
#include "mal_atom.h"

MT_Lock     mal_contextLock = MT_LOCK_INITIALIZER(mal_contextLock);
MT_Lock     mal_remoteLock = MT_LOCK_INITIALIZER(mal_remoteLock);
MT_Lock     mal_profileLock = MT_LOCK_INITIALIZER(mal_profileLock);
MT_Lock     mal_copyLock = MT_LOCK_INITIALIZER(mal_copyLock);
MT_Lock     mal_delayLock = MT_LOCK_INITIALIZER(mal_delayLock);
MT_Lock     mal_oltpLock = MT_LOCK_INITIALIZER(mal_oltpLock);

const char *
mal_version(void)
{
	return MONETDB5_VERSION;
}

/*
 * Initialization of the MAL context
 */

int
mal_init(char *modules[], int embedded)
{
/* Any error encountered here terminates the process
 * with a message sent to stderr
 */
	str err;

	/* check that library that we're linked against is compatible with
	 * the one we were compiled with */
	int maj, min, patch;
	const char *version = GDKlibversion();
	sscanf(version, "%d.%d.%d", &maj, &min, &patch);
	if (maj != GDK_VERSION_MAJOR || min < GDK_VERSION_MINOR) {
		TRC_CRITICAL(MAL_SERVER, "Linked GDK library not compatible with the one this was compiled with\n");
		TRC_CRITICAL(MAL_SERVER, "Linked version: %s, compiled version: %s\n",
					 version, GDK_VERSION);
		return -1;
	}

	if ((err = AUTHinitTables(NULL)) != MAL_SUCCEED) {
		freeException(err);
		return -1;
	}

	if (!MCinit())
		return -1;
#ifndef NDEBUG
	if (!mdbInit()) {
		mal_client_reset();
		return -1;
	}
#endif
	initNamespace();
	initParser();
	initHeartbeat();

	err = malBootstrap(modules, embedded);
	if (err != MAL_SUCCEED) {
		mal_client_reset();
#ifndef NDEBUG
		mdbExit();
#endif
		TRC_CRITICAL(MAL_SERVER, "%s\n", err);
		freeException(err);
		return -1;
	}
	initProfiler();
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
void mal_reset(void)
{
	GDKprepareExit();
	MCstopClients(0);
	setHeartbeat(-1);
	stopProfiler(0);
	AUTHreset();
	if (!GDKinmemory(0) && !GDKembedded()) {
		str err = 0;

		if ((err = msab_wildRetreat()) != NULL) {
			TRC_ERROR(MAL_SERVER, "%s\n", err);
			free(err);
		}
		if ((err = msab_registerStop()) != NULL) {
			TRC_ERROR(MAL_SERVER, "%s\n", err);
			free(err);
		}
	}
	mal_factory_reset();
	mal_dataflow_reset();
	mal_client_reset();
  	mal_linker_reset();
	mal_resource_reset();
	mal_runtime_reset();
	mal_module_reset();
	mal_atom_reset();
#ifndef NDEBUG
	mdbExit();
#endif

	memset((char*)monet_cwd, 0, sizeof(monet_cwd));
	memset((char*)monet_characteristics,0, sizeof(monet_characteristics));
	mal_namespace_reset();
	/* No need to clean up the namespace, it will simply be extended
	 * upon restart mal_namespace_reset(); */
	GDKreset(0);	// terminate all other threads
}


/* stopping clients should be done with care, as they may be in the mids of
 * transactions. One safe place is between MAL instructions, which would
 * abort the transaction by raising an exception. All sessions are
 * terminate this way.
 * We should also ensure that no new client enters the scene while shutting down.
 * For this we mark the client records as BLOCKCLIENT.
 *
 * Beware, mal_exit is also called during a SIGTERM from the monetdb tool
 */

void mal_exit(int status)
{
	mal_reset();
	exit(status);				/* properly end GDK */
}
