/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _QLOG_H
#define _QLOG_H
#include "mal.h"
#include "mal_interpreter.h"

mal_export str QLOGcatalog(BAT **r);
mal_export str QLOGcalls(BAT **r);
mal_export str QLOGenable(void *ret);
mal_export str QLOGenableThreshold(Client ctx, void *ret, const int *threshold);
mal_export str QLOGdisable(void *ret);
mal_export int QLOGisset(void);
mal_export str QLOGempty(void *ret);

#endif /* _QLOG_H */
