/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAL_SESSION_H
#define _MAL_SESSION_H

#include "mal_scenario.h"

mal_export int malBootstrap(void);
mal_export void MSserveClient(void *dummy);
mal_export void MSinitClientPrg(Client cntxt, str mod, str nme);
mal_export void MSscheduleClient(str command, str challenge, bstream *fin, stream *fout, protocol_version protocol, size_t blocksize, int compute_column_widths);

mal_export str MALreader(Client c);
mal_export str MALinitClient(Client c);
mal_export str MALexitClient(Client c);
mal_export str MALparser(Client c);
mal_export str MALengine(Client c);
mal_export void MSresetInstructions(MalBlkPtr mb, int start);
mal_export void MSresetVariables(Client cntxt, MalBlkPtr mb, MalStkPtr glb, int start);
mal_export int MALcommentsOnly(MalBlkPtr mb);

#endif /*  _MAL_SESSION_H */

