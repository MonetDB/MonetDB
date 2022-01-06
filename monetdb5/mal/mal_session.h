/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _MAL_SESSION_H
#define _MAL_SESSION_H

#include "mal_scenario.h"
#include "mal_resolve.h"

mal_export str malBootstrap(char *modules[], int embedded);
mal_export str MSserveClient(Client cntxt);
mal_export str MSinitClientPrg(Client cntxt, const char *mod, const char *nme);
mal_export void MSscheduleClient(str command, str challenge, bstream *fin, stream *fout, protocol_version protocol, size_t blocksize);

mal_export str MALreader(Client c);
mal_export str MALinitClient(Client c);
mal_export str MALexitClient(Client c);
mal_export str MALparser(Client c);
mal_export str MALengine(Client c);
mal_export str MALcallback(Client c, str msg);
mal_export void MSresetInstructions(MalBlkPtr mb, int start);
mal_export void MSresetVariables(MalBlkPtr mb);
mal_export void MSresetStack(Client cntxt, MalBlkPtr mb, MalStkPtr glb);
mal_export int MALcommentsOnly(MalBlkPtr mb);

#endif /*  _MAL_SESSION_H */
