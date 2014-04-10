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

/* This file should not be included in any file outside of this directory */

#ifndef LIBMAL
#error this file should not be included outside its source directory
#endif

#ifdef FREECLIENT
/* FREECLIENT is defined in the same file as Client */
extern void MCexitClient(Client c)
	__attribute__((__visibility__("hidden")));
extern Client MCforkClient(Client c)
	__attribute__((__visibility__("hidden")));
extern int MCreadClient(Client c)
	__attribute__((__visibility__("hidden")));
extern void MCpopClientInput(Client c)
	__attribute__((__visibility__("hidden")));
extern str defaultScenario(Client c)	/* used in src/mal/mal_session.c */
	__attribute__((__visibility__("hidden")));
extern void exitScenario(Client c)		/* used in src/mal/mal_session.c */
	__attribute__((__visibility__("hidden")));
extern str AUTHrequireAdminOrUser(Client *c, str *username)
	__attribute__((__visibility__("hidden")));
extern void mdbStep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc)
	__attribute__((__visibility__("hidden")));

extern str runFactory(Client cntxt, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr stk, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
extern int yieldResult(MalBlkPtr mb, InstrPtr p, int pc)
	__attribute__((__visibility__("hidden")));
extern str yieldFactory(MalBlkPtr mb, InstrPtr p, int pc)
	__attribute__((__visibility__("hidden")));
extern str callFactory(Client cntxt, MalBlkPtr mb, ValPtr argv[],char flag)
	__attribute__((__visibility__("hidden")));

extern str malInclude(Client c, str name, int listing)
	__attribute__((__visibility__("hidden")));
#endif

extern void initResource(void)
	__attribute__((__visibility__("hidden")));
extern int moreClients(int reruns)
	__attribute__((__visibility__("hidden")));
extern void stopMALdataflow(void)
	__attribute__((__visibility__("hidden")));

extern void malAtomDefinition(stream *out, str name,int tpe)
	__attribute__((__visibility__("hidden")));
extern int malAtomProperty(MalBlkPtr mb, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
extern void showAtoms(stream *fd)		/* used in src/mal/mal_debugger.c */
	__attribute__((__visibility__("hidden")));

extern MT_Lock mal_namespaceLock;
extern MT_Sema mal_parallelism;

extern int mdbInit(void)
	__attribute__((__visibility__("hidden")));

extern str createScriptException(MalBlkPtr, int, enum malexception,
	const char *, _In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 5, 6)))
	__attribute__((__visibility__("hidden")));

#ifdef MAXSCOPE
/* MAXSCOPE is defined in the same file as Module */
extern Symbol cloneFunction(stream *out, Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p)
	__attribute__((__visibility__("hidden")));
#endif
extern int getBarrierEnvelop(MalBlkPtr mb)
	__attribute__((__visibility__("hidden")));
extern void malGarbageCollector(MalBlkPtr mb)
	__attribute__((__visibility__("hidden")));
extern void listFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int step)
	__attribute__((__visibility__("hidden")));

extern void startHttpdaemon(void)
	__attribute__((__visibility__("hidden")));
extern void stopHttpdaemon(void)
	__attribute__((__visibility__("hidden")));
