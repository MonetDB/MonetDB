/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _SQL_OPTIMIZER_H_
#define _SQL_OPTIMIZER_H_
#include "sql.h"

//#define _SQL_OPTIMIZER_DEBUG

sql5_export void addQueryToCache(Client c);
sql5_export str SQLoptimizer(Client c);
sql5_export void SQLsetAccessMode(Client c);
sql5_export str getSQLoptimizer(mvc *m);
sql5_export void addOptimizers(Client c, MalBlkPtr mb, char *pipe);

#endif /* _SQL_OPTIMIZER_H_ */
