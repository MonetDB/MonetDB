
/*
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

#ifndef _VLT_SEQUENCE_
#define _VLT_SEQUENCE_
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_function.h"

//#define VLT_DEBUG 

#ifdef WIN32
#ifndef LIBGENERATOR
#define vault_export extern __declspec(dllimport)
#else
#define vault_export extern __declspec(dllexport)
#endif
#else
#define vault_export extern
#endif

vault_export str VLTgenerator_noop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
vault_export str VLTgenerator_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
vault_export str VLTgenerator_subselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
vault_export str VLTgenerator_thetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
vault_export str VLTgenerator_leftfetchjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
