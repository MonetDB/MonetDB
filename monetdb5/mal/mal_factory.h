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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAL_FACTORY_H
#define _MAL_FACTORY_H

/* #define DEBUG_MAL_FACTORY  */

#include "mal.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "mal_exception.h"
#include "mal_session.h"
#include "mal_debugger.h"

#define POLICYimmediate 1	/* let user wait for reply */
#define POLICYprivate   2	/* each client its own plants */

mal_export str runFactory(Client cntxt, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr stk, InstrPtr pci);
mal_export int yieldResult(MalBlkPtr mb, InstrPtr p, int pc);
mal_export str yieldFactory(MalBlkPtr mb, InstrPtr p, int pc);
mal_export str finishFactory(Client cntxt, MalBlkPtr mb, InstrPtr pp, int pc);
mal_export str shutdownFactory(Client cntxt, MalBlkPtr mb, bit force);
mal_export str shutdownFactoryByName(Client cntxt, Module m,str nme);
mal_export str callFactory(Client cntxt, MalBlkPtr mb, ValPtr argv[],char flag);
mal_export int factoryHasFreeSpace(void);
#endif /*  _MAL_FACTORY_H */
