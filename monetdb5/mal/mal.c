/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/* (author) M. Kersten */
#include "monetdb_config.h"
#include "mal.h"

char monet_cwd[FILENAME_MAX] = { 0 };

char monet_characteristics[4096];
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
#include "mal_namespace.h"		/* for initNamespace() */
#include "mal_profiler.h"
#include "mal_client.h"
#include "msabaoth.h"
#include "mal_dataflow.h"
#include "mal_private.h"
#include "mal_internal.h"
#include "mal_runtime.h"
#include "mal_resource.h"
#include "mal_atom.h"
#include "mutils.h"

MT_Lock mal_contextLock = MT_LOCK_INITIALIZER(mal_contextLock);
MT_Lock mal_profileLock = MT_LOCK_INITIALIZER(mal_profileLock);
MT_Lock mal_copyLock = MT_LOCK_INITIALIZER(mal_copyLock);
MT_Lock mal_delayLock = MT_LOCK_INITIALIZER(mal_delayLock);


const char *
mal_version(void)
{
	return MONETDB5_VERSION;
}

/*
 * Initialization of the MAL context
 */

int
mal_init(char *modules[], bool embedded, const char *initpasswd,
		 const char *caller_revision)
{
/* Any error encountered here terminates the process
 * with a message sent to stderr
 */
	str err;

	mal_startup();
	/* check that library that we're linked against is compatible with
	 * the one we were compiled with */
	int maj, min, patch;
	const char *version = GDKlibversion();
	sscanf(version, "%d.%d.%d", &maj, &min, &patch);
	if (maj != GDK_VERSION_MAJOR || min < GDK_VERSION_MINOR) {
		TRC_CRITICAL(MAL_SERVER,
					 "Linked GDK library not compatible with the one this was compiled with\n");
		TRC_CRITICAL(MAL_SERVER, "Linked version: %s, compiled version: %s\n",
					 version, GDK_VERSION);
		return -1;
	}

	if (caller_revision) {
		const char *p = mercurial_revision();
		if (p && strcmp(p, caller_revision) != 0) {
			TRC_CRITICAL(MAL_SERVER,
						 "incompatible versions: caller is %s, MAL is %s\n",
						 caller_revision, p);
			return -1;
		}
	}

	if (!MCinit())
		return -1;
	initNamespace();

	err = malBootstrap(modules, embedded, initpasswd);
	if (err != MAL_SUCCEED) {
		mal_client_reset();
		TRC_CRITICAL(MAL_SERVER, "%s\n", err);
		freeException(err);
		return -1;
	}
	initProfiler();
	initHeartbeat();
	return 0;
}

/*
 * Upon exit we should attempt to remove all allocated memory explicitly.
 * This seemingly superfluous action is necessary to simplify analysis of
 * memory leakage problems later on and to allow an embedded server to
 * restart the server properly.
 *
 * It is the responsibility of the enclosing application to finish/cease all
 * activity first.
 * This function should be called after you have issued sql_reset();
 */
void
mal_reset(void)
{
	GDKprepareExit();
	MCstopClients(0);
	setHeartbeat(-1);
	stopProfiler(0);
	AUTHreset();
	if (!GDKinmemory(0) && !GDKembedded()) {
		str err = 0;

		if ((err = msab_wildRetreat()) !=NULL) {
			TRC_ERROR(MAL_SERVER, "%s\n", err);
			free(err);
		}
		if ((err = msab_registerStop()) !=NULL) {
			TRC_ERROR(MAL_SERVER, "%s\n", err);
			free(err);
		}
	}
	mal_dataflow_reset();
	mal_client_reset();
	mal_linker_reset();
	mal_resource_reset();
	mal_runtime_reset();
	mal_module_reset();
	mal_atom_reset();

	memset((char *) monet_cwd, 0, sizeof(monet_cwd));
	memset((char *) monet_characteristics, 0, sizeof(monet_characteristics));
	mal_namespace_reset();
	GDKreset(0);				// terminate all other threads
}


/* stopping clients should be done with care, as they may be in the mids of
 * transactions. One safe place is between MAL instructions, which would
 * abort the transaction by raising an exception. All sessions are
 * terminate this way.
 * We should also ensure that no new client enters the scene while shutting down.
 * For this we mark the client records as BLOCKCLIENT.
 */

void
mal_exit(int status)
{
	mal_reset();
	printf("# mserver5 exiting\n");
	exit(status);				/* properly end GDK */
}
