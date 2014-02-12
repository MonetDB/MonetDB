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

#ifndef JAQL_H
#define JAQL_H 1

#include "stream.h"
#include "mal_client.h"
#include "jaqltree.h"

#ifdef WIN32
#ifndef LIBJAQL
#define jaql_export extern __declspec(dllimport)
#else
#define jaql_export extern __declspec(dllexport)
#endif
#else
#define jaql_export extern
#endif

str getJAQLContext(Client cntxt, jc **c);
void printtree(stream *out, tree *t, int level, char op);


jaql_export str JAQLexecute(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
jaql_export str JAQLgetVar(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
jaql_export str JAQLsetVar(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
jaql_export str JAQLcast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
jaql_export str JAQLbatconcat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
jaql_export str JAQLprintTimings(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif

