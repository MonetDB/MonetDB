/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* This file should not be included in any file outside of this directory */

#ifndef LIBMAL
#error this file should not be included outside its source directory
#endif

#ifdef _MAL_CLIENT_H_
/* _MAL_CLIENT_H_ is defined in the same file as Client */
void MCexitClient(Client c)
	__attribute__((__visibility__("hidden")));
void MCfreeClient(Client c)
	__attribute__((__visibility__("hidden")));
bool MCinit(void)
	__attribute__((__visibility__("hidden")));
int MCinitClientThread(Client c)
	__attribute__((__visibility__("hidden")));
void MCpopClientInput(Client c)
	__attribute__((__visibility__("hidden")));
int MCreadClient(Client c)
	__attribute__((__visibility__("hidden")));
int MCshutdowninprogress(void)
	__attribute__((__visibility__("hidden")));
str defaultScenario(Client c)	/* used in src/mal/mal_session.c */
	__attribute__((__visibility__("hidden")));
void mdbStep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc)
	__attribute__((__visibility__("hidden")));

str runFactory(Client cntxt, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr stk, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
int yieldResult(MalBlkPtr mb, InstrPtr p, int pc)
	__attribute__((__visibility__("hidden")));
str yieldFactory(MalBlkPtr mb, InstrPtr p, int pc)
	__attribute__((__visibility__("hidden")));
str callFactory(Client cntxt, MalBlkPtr mb, ValPtr argv[],char flag)
	__attribute__((__visibility__("hidden")));
#endif

str malAtomDefinition(const char *name,int tpe)
	__attribute__((__visibility__("hidden")));
str malAtomProperty(MalBlkPtr mb, InstrPtr pci)
	__attribute__((__visibility__("hidden")));

bool mdbInit(void)
	__attribute__((__visibility__("hidden")));
void mdbExit(void)
	__attribute__((__visibility__("hidden")));

#ifdef MAXSCOPE
/* MAXSCOPE is defined in the same file as Module */
Symbol cloneFunction(Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p)
	__attribute__((__visibility__("hidden")));
#endif
int getBarrierEnvelop(MalBlkPtr mb)
	__attribute__((__visibility__("hidden")));
void listFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int step)
	__attribute__((__visibility__("hidden")));
MALfcn findFunctionImplementation(const char *cname)
	__attribute__((__visibility__("hidden")));

/* mal_linker.h */
char *MSP_locate_script(const char *mod_name)
	__attribute__((__visibility__("hidden")));

/* Reset primitives */
void AUTHreset(void)
	__attribute__((__visibility__("hidden")));

void mal_client_reset(void)
	__attribute__((__visibility__("hidden")));

void mal_dataflow_reset(void)
	__attribute__((__visibility__("hidden")));

void mal_factory_reset(void)
	__attribute__((__visibility__("hidden")));

void mal_linker_reset(void)
	__attribute__((__visibility__("hidden")));

void mal_module_reset(void)
	__attribute__((__visibility__("hidden")));

void mal_namespace_reset(void)
	__attribute__((__visibility__("hidden")));

void mal_resource_reset(void)
	__attribute__((__visibility__("hidden")));

void mal_runtime_reset(void)
	__attribute__((__visibility__("hidden")));

char *dupError(const char *err)
	__attribute__((__visibility__("hidden"), __returns_nonnull__));

char *concatErrors(char *err1, const char *err2)
	__attribute__((__visibility__("hidden")))
	__attribute__((__nonnull__(1, 2)))
	__attribute__((__returns_nonnull__));
