/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _CONTROLRUNNER_H
#define _CONTROLRUNNER_H 1

#include "stream.h"

char control_authorise(const char *host, const char *chal, const char *algo, const char *passwd, stream *fout);
void control_handleclient(const char *host, int sock, stream *fdin, stream *fdout);
void *controlRunner(void *d);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
