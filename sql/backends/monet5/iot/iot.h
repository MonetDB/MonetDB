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
 * Copyright August 2008-2016 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _IOT_
#define _IOT_

#include "mal.h"
#include "mal_interpreter.h"
#include "sql.h"
#include "sql_scenario.h"
#include "basket.h"
#include "petrinet.h"

#ifdef WIN32
#ifndef LIBIOT
#define iot_export extern __declspec(dllimport)
#else
#define iot_export extern __declspec(dllexport)
#endif
#else
#define iot_export extern
#endif

#define _DEBUG_IOT_ if(1)
iot_export MT_Lock iotLock;

iot_export str IOTquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str IOTpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str IOTresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str IOTstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str IOTdump(void *ret);

#endif
