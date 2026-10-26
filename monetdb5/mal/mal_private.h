/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/* This file should not be included in any file outside of this directory */

#ifndef LIBMAL
#error this file should not be included outside its source directory
#endif

#ifdef _MAL_CLIENT_H_
/* _MAL_CLIENT_H_ is defined in the same file as Client */
bool MCinit(void)
	ATTR_VISIBILITY_HIDDEN;
void MCpopClientInput(Client c)
	ATTR_VISIBILITY_HIDDEN;
int MCreadClient(Client c)
	ATTR_VISIBILITY_HIDDEN;
#endif

str malAtomDefinition(const char *name,int tpe)
	ATTR_VISIBILITY_HIDDEN;
str malAtomProperty(mel_func *f)
	ATTR_VISIBILITY_HIDDEN;

#ifdef MAXSCOPE
/* MAXSCOPE is defined in the same file as Module */
Symbol cloneFunction(Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p)
	ATTR_VISIBILITY_HIDDEN;
#endif
int getBarrierEnvelop(MalBlkPtr mb)
	ATTR_VISIBILITY_HIDDEN;
void listFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int step)
	ATTR_VISIBILITY_HIDDEN;
MALfcn findFunctionImplementation(const char *cname)
	ATTR_VISIBILITY_HIDDEN;

/* mal_linker.h */
char *MSP_locate_script(allocator *ma, const char *mod_name)
	ATTR_VISIBILITY_HIDDEN;

/* Reset primitives */
void AUTHreset(void)
	ATTR_VISIBILITY_HIDDEN;

void mal_client_reset(void)
	ATTR_VISIBILITY_HIDDEN;

void mal_dataflow_reset(void)
	ATTR_VISIBILITY_HIDDEN;

void mal_pipelines_reset(void)
	ATTR_VISIBILITY_HIDDEN;

void mal_linker_reset(void)
	ATTR_VISIBILITY_HIDDEN;

void mal_module_reset(void)
	ATTR_VISIBILITY_HIDDEN;

void mal_namespace_reset(void)
	ATTR_VISIBILITY_HIDDEN;

void mal_resource_reset(void)
	ATTR_VISIBILITY_HIDDEN;

void mal_runtime_reset(void)
	ATTR_VISIBILITY_HIDDEN;

char *dupError(const char *err)
	ATTR_VISIBILITY_HIDDEN
	ATTR_RETURNS_NONNULL;

void
setPoly(mel_func *f, malType tpe)
	ATTR_VISIBILITY_HIDDEN;
