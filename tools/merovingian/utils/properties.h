/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _SEEN_PROPERTIES_H
#define _SEEN_PROPERTIES_H 1

#include "utils.h"

#define MEROPROPFILE ".merovingian_properties"

confkeyval *getDefaultProps(void);
int writeProps(confkeyval *ckv, const char *path);
void writePropsBuf(confkeyval *ckv, char **buf);
int readProps(confkeyval *ckv, const char *path);
int readAllProps(confkeyval *ckv, const char *path);
void readPropsBuf(confkeyval *ckv, char *buf);
char *setProp(const char *path, const char *key, const char *val);

#endif
