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

#ifndef _CONNECTIONS_H
#define _CONNECTIONS_H 1

#include "merovingian.h"

err openConnectionIP(int *socks, bool udp, const char *bindaddr, unsigned short port, FILE *log);
err openConnectionUNIX(int *ret, const char *path, int mode, FILE *log);

#endif
