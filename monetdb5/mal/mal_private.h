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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

/* This file should not be included in any file outside of this directory */

#ifndef LIBMAL
#error this file should not be included outside its source directory
#endif

#ifdef FREECLIENT
/* FREECLIENT is defined in the same file as Client */
__hidden void MCexitClient(Client c)
	__attribute__((__visibility__("hidden")));
__hidden Client MCforkClient(Client c)
	__attribute__((__visibility__("hidden")));
__hidden int MCreadClient(Client c)
	__attribute__((__visibility__("hidden")));
__hidden void MCpopClientInput(Client c)
	__attribute__((__visibility__("hidden")));
__hidden str defaultScenario(Client c)	/* used in src/mal/mal_session.c */
	__attribute__((__visibility__("hidden")));
__hidden void exitScenario(Client c)		/* used in src/mal/mal_session.c */
	__attribute__((__visibility__("hidden")));
__hidden str AUTHrequireAdminOrUser(Client *c, str *username)
	__attribute__((__visibility__("hidden")));
__hidden void mdbStep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc)
	__attribute__((__visibility__("hidden")));

__hidden str runFactory(Client cntxt, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr stk, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
__hidden int yieldResult(MalBlkPtr mb, InstrPtr p, int pc)
	__attribute__((__visibility__("hidden")));
__hidden str yieldFactory(MalBlkPtr mb, InstrPtr p, int pc)
	__attribute__((__visibility__("hidden")));
__hidden str callFactory(Client cntxt, MalBlkPtr mb, ValPtr argv[],char flag)
	__attribute__((__visibility__("hidden")));

__hidden str malInclude(Client c, str name, int listing)
	__attribute__((__visibility__("hidden")));
#endif

__hidden void initResource(void)
	__attribute__((__visibility__("hidden")));
__hidden int moreClients(int reruns)
	__attribute__((__visibility__("hidden")));
__hidden void stopMALdataflow(void)
	__attribute__((__visibility__("hidden")));

__hidden void malAtomDefinition(stream *out, str name,int tpe)
	__attribute__((__visibility__("hidden")));
__hidden int malAtomProperty(MalBlkPtr mb, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
__hidden void showAtoms(stream *fd)		/* used in src/mal/mal_debugger.c */
	__attribute__((__visibility__("hidden")));

__hidden MT_Lock mal_namespaceLock;
__hidden MT_Sema mal_parallelism;

__hidden int mdbInit(void)
	__attribute__((__visibility__("hidden")));

__hidden str createScriptException(MalBlkPtr, int, enum malexception,
	const char *, _In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 5, 6)))
	__attribute__((__visibility__("hidden")));

#ifdef MAXSCOPE
/* MAXSCOPE is defined in the same file as Module */
__hidden Symbol cloneFunction(stream *out, Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p)
	__attribute__((__visibility__("hidden")));
#endif
__hidden int getBarrierEnvelop(MalBlkPtr mb)
	__attribute__((__visibility__("hidden")));
__hidden void malGarbageCollector(MalBlkPtr mb)
	__attribute__((__visibility__("hidden")));
__hidden void listFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int step)
	__attribute__((__visibility__("hidden")));

/* mal_http_daemon.h */
__hidden void startHttpdaemon(void)
	__attribute__((__visibility__("hidden")));
__hidden void stopHttpdaemon(void)
	__attribute__((__visibility__("hidden")));

/* mal_linker.h */
__hidden char *MSP_locate_script(const char *mod_name)
	__attribute__((__visibility__("hidden")));
