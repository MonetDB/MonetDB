/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "monetdb_hgversion.h"
#include <string.h>
#include "mutils.h"

static const char revision[] =
#ifdef MERCURIAL_ID
	MERCURIAL_ID
#else
	"Unknown"
#endif
	;

const char *
mercurial_revision(void)
{
	return revision;
}
