/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _MAL_SCOPE_H_
#define _MAL_SCOPE_H_

#include "mal.h"
/* #define MAL_SCOPE_DEBUG  */

#define MAXSCOPE 256

typedef struct SCOPEDEF {
	struct SCOPEDEF   *link; /* module with same index value */
	const char *name;			/* index in namespace */
	Symbol *space; 			/* type dispatcher table */
	int isAtomModule; 		/* atom module definition ? */
	str help;   			/* short description of module functionality*/
} *Module, ModuleRecord;

mal_export Module   userModule(void);
mal_export Module   globalModule(const char *nme);
mal_export Module   fixModule(const char *nme);
mal_export Module   getModule(const char *nme);
mal_export void     freeModule(Module cur);
mal_export void     insertSymbol(Module scope, Symbol prg);
mal_export void     deleteSymbol(Module scope, Symbol prg);
mal_export Module   findModule(Module scope, const char *name);
mal_export Symbol   findSymbol(Module usermodule, const char *mod, const char *fcn);
mal_export Symbol   findSymbolInModule(Module v, const char *fcn);
mal_export void     getModuleList(Module** out, int* length);
mal_export void     freeModuleList(Module* list);
mal_export void     dumpModules(stream *out);
mal_export BAT     *getModules(void);

#define getSymbolIndex(N)  (int)(*(unsigned char*)(N))

#endif /* _MAL_SCOPE_H_ */
