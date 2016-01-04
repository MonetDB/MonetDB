/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef READLINETOOLS_H_INCLUDED
#define READLINETOOLS_H_INCLUDED

#include "mal_client.h"
mal_export int readConsole(Client cntxt);
mal_export char * getConsoleInput(Client c, const char *prompt, int linemode, int exit_on_error);

#endif /* READLINETOOLS_H_INCLUDED */
