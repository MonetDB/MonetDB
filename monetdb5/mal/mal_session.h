/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAL_SESSION_H
#define _MAL_SESSION_H

#include "mal_client.h"
#include "mal_resolve.h"

mal_export str malBootstrap(char *modules[], bool embedded,
							const char *initpasswd);
mal_export str MSinitClientPrg(Client cntxt, const char *mod, const char *nme);
mal_export void MSscheduleClient(str command, str peer, str challenge, bstream *fin,
								 stream *fout, protocol_version protocol,
								 size_t blocksize);

mal_export str MALinitClient(Client c);
mal_export str MALexitClient(Client c);
mal_export str MALparser(Client c);
mal_export void MALengine(Client c);
mal_export void MSresetInstructions(MalBlkPtr mb, int start);
mal_export void MSresetVariables(MalBlkPtr mb);
mal_export void MSresetStack(Client cntxt, MalBlkPtr mb, MalStkPtr glb);
mal_export int MALcommentsOnly(MalBlkPtr mb);

mal_export str optimizeMALBlock(Client cntxt, MalBlkPtr mb);

#endif /*  _MAL_SESSION_H */
