/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _HANDLERS_H
#define _HANDLERS_H 1

#include <signal.h>

void handler(int sig);
void huphandler(int sig);
void childhandler(void);
void segvhandler(int sig);
void reinitialize(void);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
