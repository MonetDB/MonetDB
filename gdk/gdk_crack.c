/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * fake(?) crack implementation
 * L.Sidirourgos
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_crack.h"

#define CRACK_VERSION	0

gdk_return
BATcrack(BAT *b)
{
	(void) b;
	return GDK_SUCCEED;
}

lng
CRCKsize(BAT *b)
{
	lng sz = 0;
	(void) b;
	return sz;
}

void
CRCKdestroy(BAT *b)
{
	(void) b;
}
