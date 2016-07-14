/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @+ Dummy code
 */
#ifndef _Q_STATISTICS_H
#define _Q_STATISTICS_H
/* #define _Q_STATISTICS_DEBUG*/

#include "mal_interpreter.h"
#include "mal_scenario.h"
#include "mal_namespace.h"
#include "opt_support.h"
#include "opt_prelude.h"

mal_export str QOTgetStatistics(bat *ret, str *nme);
mal_export void QOTupdateStatistics(str nme, int prop, lng val);
mal_export void QOTstatisticsExit(void);
#endif /* _Q_STATISTICS_H */
