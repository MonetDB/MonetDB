/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SEEN_PROPERTIES_H
#define _SEEN_PROPERTIES_H 1

#include "utils.h"

#define MEROPROPFILE ".merovingian_properties"
#define PROFILERLOGPROPERTY "profilerlogpath"

confkeyval *getDefaultProps(void);
int writeProps(confkeyval *ckv, const char *path);
void writePropsBuf(confkeyval *ckv, char **buf);
int readProps(confkeyval *ckv, const char *path);
int readAllProps(confkeyval *ckv, const char *path);
void readPropsBuf(confkeyval *ckv, char *buf);
char *setProp(char *path, char *key, char *val);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
