/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _OPT_PIPES_
#define _OPT_PIPES_
#include "opt_support.h"

mal_export str getPipeCatalog(Client ctx, bat *nme, bat *def, bat *stat);
mal_export str addPipeDefinition(Client cntxt, const char *name,
								 const char *pipe);
mal_export bool isOptimizerPipe(const char *name);
mal_export str addOptimizerPipe(Client cntxt, MalBlkPtr mb, const char *name);
mal_export void opt_pipes_reset(void);

#endif
