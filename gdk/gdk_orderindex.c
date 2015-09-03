/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * Implementation for the oid order index.
 * Martin Kersten, Lefteris Sidirourgos.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/*
 * Keep the oid list of *order as the OID order index of *b, and also
 * make it persistent if *b is persistent. BAT *order can then be unfixed by
 * the caller function.
 */

gdk_return
ORDERkeepidx(BAT *b, BAT *order) {

	assert(BAThdense(b));	/* assert void headed */
	assert(BAThdense(order));	/* assert void headed */

	switch (ATOMstorage(b->ttype)) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_flt:
	case TYPE_dbl:
		break;
	case TYPE_void:
	case TYPE_str:
	case TYPE_ptr:
	default:
		/* TODO: support strings, date, timestamps etc. */
		GDKerror("TYPE not supported.\n");
		return GDK_FAIL;
	}

	if (ATOMtype(order->ttype) != TYPE_oid) {
		GDKerror("BAT order=%s not correct type.\n", BATgetId(order));
		return GDK_FAIL;
	}

	BATcheck(b, "ORDERkeepidx", GDK_FAIL);
	BATcheck(order, "ORDERkeepidx", GDK_FAIL);

	if (VIEWtparent(b)) {
		/* the Order Index is a slice of the parent Order Index.
		 * Functions on modules MAL should take care of that */
		GDKerror("b=%s is a VIEW.\n", BATgetId(b));
		return GDK_FAIL;
	}
	if (VIEWtparent(order)) {
		GDKerror("order=%s is a VIEW.\n", BATgetId(order));
		return GDK_FAIL;
	}

	MT_lock_set(&GDKorderIdxLock(abs(b->batCacheid)), "ORDERkeepidx");
	if (!b->torderidx.flags) {
		b->torderidx.o = BBPcacheid(order);
		//BBPkeepref(order->batCacheid);
		b->torderidx.flags = 1;
	} else {
		/* take care if other index already exists, should not happen though
		 * because it is a waste of resources.
		 */
		MT_lock_unset(&GDKorderIdxLock(abs(b->batCacheid)), "ORDERkeepidx");
		return GDK_FAIL;
	}
	MT_lock_unset(&GDKorderIdxLock(abs(b->batCacheid)), "ORDERkeepidx");

	return GDK_SUCCEED;
}

gdk_export
bat ORDERgetidx(BAT *b) {
	BATcheck(b, "ORDERgetidx", GDK_FAIL);

	if (b->torderidx.flags != 0) {
		return b->torderidx.o;
	}
	return 0;
}
