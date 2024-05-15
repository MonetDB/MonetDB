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

/*
 * Provide a mapping between attribute names as found in data sources and
 * connection strings.
*/

#ifndef ODBCATTRS
#define ODBCATTRS

#include "msettings.h"

struct attr_setting {
	const char *name;        // canonical attribute name
	const char *alt_name;    // user-facing attribute name
	mparm parm;
};

// defined in SQLConnect.c, also used in SQLBrowseConnect.c
extern const struct attr_setting attr_settings[];
extern const int attr_setting_count;

// look up attr_setting entry by name or alt_name, -1 if not found
int attr_setting_lookup(const char *attr_name);




#endif
