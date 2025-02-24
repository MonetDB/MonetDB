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

#ifndef _SQL_OPTIMIZER_H_
#define _SQL_OPTIMIZER_H_
#include "sql.h"

//#define _SQL_OPTIMIZER_DEBUG
extern str SQLoptimizeQuery(Client c, MalBlkPtr mb);
extern str SQLoptimizeFunction(Client c, MalBlkPtr mb);
extern void SQLaddQueryToCache(Client c);
extern void SQLremoveQueryFromCache(Client c);
extern str getSQLoptimizer(mvc *m);

#endif /* _SQL_OPTIMIZER_H_ */
