/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _PROXY_H
#define _PROXY_H 1

#include "merovingian.h"

err startProxy(int psock, stream *cfdin, stream *cfout, char *url, char *client);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
