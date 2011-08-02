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

#ifndef _MAL_SESSION_H
#define _MAL_SESSION_H

#include "mal_scenario.h"

mal_export int malBootstrap(void);
mal_export void MSserveClient(void *dummy);
mal_export void MSinitClientPrg(Client cntxt, str mod, str nme);
mal_export void MSresetClientPrg(Client cntxt);
mal_export void MSscheduleClient(str command, str challenge, bstream *fin, stream *fout);

mal_export str MALreader(Client c);
mal_export str MALinitClient(Client c);
mal_export str MALexitClient(Client c);
mal_export str MALparser(Client c);
mal_export str MALengine(Client c);
mal_export void MSresetInstructions(MalBlkPtr mb, int start);
mal_export void MSresetVariables(Client cntxt, MalBlkPtr mb, MalStkPtr glb, int start);
mal_export int MALcommentsOnly(MalBlkPtr mb);

#endif /*  _MAL_SESSION_H */

