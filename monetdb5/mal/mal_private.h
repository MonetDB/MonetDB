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

#ifdef FREECLIENT
/* FREECLIENT is defined in the same file as Client */
extern void MCexitClient(Client c);
extern Client MCforkClient(Client c);
extern int MCreadClient(Client c);
extern void MCpopClientInput(Client c);
extern str defaultScenario(Client c);	/* used in src/mal/mal_session.c */
extern void exitScenario(Client c);	/* used in src/mal/mal_session.c */
extern str AUTHrequireAdminOrUser(Client *c, str *username);
extern void mdbStep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc);

extern str runFactory(Client cntxt, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr stk, InstrPtr pci);
extern int yieldResult(MalBlkPtr mb, InstrPtr p, int pc);
extern str yieldFactory(MalBlkPtr mb, InstrPtr p, int pc);
extern str callFactory(Client cntxt, MalBlkPtr mb, ValPtr argv[],char flag);

extern str malInclude(Client c, str name, int listing);
#endif

extern void initResource(void);
extern int moreClients(int reruns);
extern void stopMALdataflow(void);

extern void malAtomDefinition(stream *out, str name,int tpe);
extern int malAtomProperty(MalBlkPtr mb, InstrPtr pci);
extern void showAtoms(stream *fd);  /* used in src/mal/mal_debugger.c */

extern MT_Lock mal_namespaceLock;
extern MT_Sema mal_parallelism;

extern int mdbInit(void);

extern str createScriptException(MalBlkPtr, int, enum malexception,
	const char *, _In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 5, 6)));

#ifdef MAXSCOPE
/* MAXSCOPE is defined in the same file as Module */
extern Symbol cloneFunction(stream *out, Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p);
#endif
extern int getBarrierEnvelop(MalBlkPtr mb);
extern void malGarbageCollector(MalBlkPtr mb);
extern void listFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int step);

extern void startHttpdaemon(void);
extern void stopHttpdaemon(void);
