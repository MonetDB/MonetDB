/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _OPT_PIPES_
#define _OPT_PIPES_
#include "opt_prelude.h"
#include "opt_support.h"

opt_export str getPipeDefinition(str name);
opt_export str getPipeCatalog(bat *nme, bat *def, bat *stat);
opt_export str addPipeDefinition(Client cntxt, str name, str pipe);
opt_export int isOptimizerPipe(str name);
opt_export str addOptimizerPipe(Client cntxt, MalBlkPtr mb, str name);
opt_export str compileOptimizer(Client cntxt, str name);

#endif
