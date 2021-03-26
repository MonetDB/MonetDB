/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _CONNECTIONS_H
#define _CONNECTIONS_H 1

#include "merovingian.h"

err openConnectionIP(int *socks, bool udp, const char *bindaddr, unsigned short port, FILE *log);
err openConnectionUNIX(int *ret, const char *path, int mode, FILE *log);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
