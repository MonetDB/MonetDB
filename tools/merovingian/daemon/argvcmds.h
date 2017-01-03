/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _ARGVCMDS_H
#define _ARGVCMDS_H 1

#include <utils/utils.h>

int command_help(int argc, char *argv[]);
int command_version(void);
int command_create(int argc, char *argv[]);
int command_get(confkeyval *ckv, int argc, char *argv[]);
int command_set(confkeyval *ckv, int argc, char *argv[]);
int command_stop(confkeyval *ckv, int argc, char *argv[]);

#endif
