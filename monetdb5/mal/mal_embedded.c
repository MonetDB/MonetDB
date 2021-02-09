/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (author) M.L. Kersten
 * These routines assume that the signatures for all MAL files are defined as text in mal_embedded.h
 * They are parsed upon system restart without access to their source files.
 * This way the definitions are part of the library upon compilation.
 * It assumes that all necessary libraries are already loaded.
 * A failure to bind the address in the context of an embedded version is not considered an error.
 */

#include "monetdb_config.h"

#include "mal_embedded.h"
#include "mal_builder.h"
#include "mal_stack.h"
#include "mal_linker.h"
#include "mal_session.h"
#include "mal_scenario.h"
#include "mal_parser.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"  /* for initNamespace() */
#include "mal_client.h"
#include "mal_dataflow.h"
#include "mal_private.h"
#include "mal_runtime.h"
#include "mal_atom.h"
#include "mal_resource.h"
#include "mal_atom.h"
#include "msabaoth.h"
#include "mal_authorize.h"
#include "mal_profiler.h"
#include "mutils.h"

static bool embeddedinitialized = false;

str
malEmbeddedBoot(int workerlimit, int memorylimit, int querytimeout, int sessiontimeout, int with_mapi_server)
{
	Client c;
	str msg = MAL_SUCCEED;

	if( embeddedinitialized )
		return MAL_SUCCEED;

	{
		/* unlock the vault, first see if we can find the file which
		 * holds the secret */
		char secret[1024];
		char *secretp = secret;
		FILE *secretf;
		size_t len;

		if (GDKinmemory(0) || GDKgetenv("monet_vault_key") == NULL) {
			/* use a default (hard coded, non safe) key */
			snprintf(secret, sizeof(secret), "%s", "Xas632jsi2whjds8");
		} else {
			if ((secretf = MT_fopen(GDKgetenv("monet_vault_key"), "r")) == NULL) {
				throw(MAL, "malEmbeddedBoot",
					"unable to open vault_key_file %s: %s\n",
					GDKgetenv("monet_vault_key"), strerror(errno));
			}
			len = fread(secret, 1, sizeof(secret), secretf);
			secret[len] = '\0';
			len = strlen(secret); /* secret can contain null-bytes */
			if (len == 0) {
				throw(MAL, "malEmbeddedBoot", "vault key has zero-length!\n");
			} else if (len < 5) {
				throw(MAL, "malEmbeddedBoot",
					"#warning: your vault key is too short "
					"(%zu), enlarge your vault key!\n", len);
			}
			fclose(secretf);
		}
		if ((msg = AUTHunlockVault(secretp)) != MAL_SUCCEED) {
			/* don't show this as a crash */
			return msg;
		}
	}
	if ((msg = AUTHinitTables(NULL)) != MAL_SUCCEED)
		return msg;

	if (!MCinit())
		throw(MAL, "malEmbeddedBoot", "MAL debugger failed to start");
#ifndef NDEBUG
	if (!mdbInit()) {
		mal_client_reset();
		throw(MAL, "malEmbeddedBoot", "MAL debugger failed to start");
	}
#endif
	// monet_memory = MT_npages() * MT_pagesize();
	initNamespace();
	initParser();
	initHeartbeat();
	// initResource();

	c = MCinitClient((oid) 0, 0, 0);
	if(c == NULL)
		throw(MAL, "malEmbeddedBoot", "Failed to initialize client");
	c->workerlimit = workerlimit;
	c->memorylimit = memorylimit;
	c->querytimeout = querytimeout * 1000000;	// from sec to usec
	c->sessiontimeout = sessiontimeout * 1000000;
	c->curmodule = c->usermodule = userModule();
	if(c->usermodule == NULL) {
		MCcloseClient(c);
		throw(MAL, "malEmbeddedBoot", "Failed to initialize client MAL module");
	}
	if ( (msg = defaultScenario(c)) ) {
		MCcloseClient(c);
		return msg;
	}
	if ((msg = MSinitClientPrg(c, "user", "main")) != MAL_SUCCEED) {
		MCcloseClient(c);
		return msg;
	}
	char *modules[5] = { "embedded", "sql", "generator", "udf" };
	if ((msg = malIncludeModules(c, modules, 0, !with_mapi_server)) != MAL_SUCCEED) {
		MCcloseClient(c);
		return msg;
	}
	pushEndInstruction(c->curprg->def);
	msg = chkProgram(c->usermodule, c->curprg->def);
	if ( msg != MAL_SUCCEED || (msg= c->curprg->def->errors) != MAL_SUCCEED ) {
		MCcloseClient(c);
		return msg;
	}
	msg = MALengine(c);
	if (msg == MAL_SUCCEED)
		embeddedinitialized = true;
	MCcloseClient(c);
	initProfiler();
	return msg;
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

void
malEmbeddedReset(void) //remove extra modules and set to non-initialized again
{
	if (!embeddedinitialized)
		return;

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
	embeddedinitialized = false;
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

void
malEmbeddedStop(int status)
{
	malEmbeddedReset();
	exit(status); /* properly end GDK */
}
