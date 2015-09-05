/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

BAT *
BATkdiff(BAT *l, BAT *r)
{
	BAT *l1, *l2, *r1, *d;

	l1 = BATmirror(BATmark(l, 0));
	l2 = BATmirror(BATmark(BATmirror(l), 0));
	r1 = BATmirror(BATmark(r, 0));
	d = BATsubdiff(l1, r1, NULL, NULL, 0, BUN_NONE);
	BBPunfix(r1->batCacheid);
	r1 = BATproject(d, l1);
	BBPunfix(l1->batCacheid);
	l1 = BATproject(d, l2);
	BBPunfix(d->batCacheid);
	BBPunfix(l2->batCacheid);
	d = VIEWcreate(BATmirror(r1), l1);
	BBPunfix(r1->batCacheid);
	BBPunfix(l1->batCacheid);
	return d;
}
