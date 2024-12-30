/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _OPT_PIPES_
#define _OPT_PIPES_
#include "opt_support.h"

mal_export str getPipeCatalog(bat *nme, bat *def, bat *stat);
mal_export str addPipeDefinition(Client cntxt, const char *name,
								 const char *pipe);
mal_export bool isOptimizerPipe(const char *name);
mal_export str addOptimizerPipe(Client cntxt, MalBlkPtr mb, const char *name);
mal_export void opt_pipes_reset(void);

#endif
