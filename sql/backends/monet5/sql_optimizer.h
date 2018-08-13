/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_OPTIMIZER_H_
#define _SQL_OPTIMIZER_H_
#include "sql.h"

//#define _SQL_OPTIMIZER_DEBUG
sql5_export str SQLoptimizeQuery(Client c, MalBlkPtr mb);
sql5_export str SQLoptimizeFunction(Client c, MalBlkPtr mb);
sql5_export void SQLaddQueryToCache(Client c);
sql5_export str SQLoptimizer(Client c);
sql5_export void SQLsetAccessMode(Client c);
sql5_export str getSQLoptimizer(mvc *m);

#endif /* _SQL_OPTIMIZER_H_ */
