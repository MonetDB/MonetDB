/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _FORKMSERVER_H
#define _FORKMSERVER_H 1

#include "msabaoth.h" /* sabdb */
#include "merovingian.h" /* err */

err forkMserver(const char* database, sabdb** stats, bool force);

#endif
