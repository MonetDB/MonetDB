/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* Calculate a cross product between bats l and r with optional
 * candidate lists sl for l and sr for r.
 * The result is two bats r1 and r2 which contain the OID (head
 * values) of the input bats l and r.
 * If max_one is set, r can have at most one row. */
gdk_return
BATsubcross(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool max_one)
{
	BAT *bn1, *bn2 = NULL;
	struct canditer ci1, ci2;
	BUN cnt1, cnt2;
	oid *restrict p;
	BUN i, j;

	cnt1 = canditer_init(&ci1, l, sl);
	cnt2 = canditer_init(&ci2, r, sr);

	if (max_one && cnt1 > 0 && cnt2 > 1) {
		GDKerror("more than one match");
		return GDK_FAIL;
	}

	bn1 = COLnew(0, TYPE_oid, cnt1 * cnt2, TRANSIENT);
	if (r2p)
		bn2 = COLnew(0, TYPE_oid, cnt1 * cnt2, TRANSIENT);
	if (!bn1 || (r2p && !bn2)) {
		BBPreclaim(bn1);
		if (bn2)
			BBPreclaim(bn2);
		return GDK_FAIL;
	}
	if (cnt1 > 0 && cnt2 > 0) {
		BATsetcount(bn1, cnt1 * cnt2);
		bn1->tsorted = true;
		bn1->trevsorted = cnt1 <= 1;
		bn1->tkey = cnt2 <= 1;
		bn1->tnil = false;
		bn1->tnonil = true;
		p = (oid *) Tloc(bn1, 0);
		for (i = 0; i < cnt1; i++) {
			oid x = canditer_next(&ci1);
			for (j = 0; j < cnt2; j++) {
				*p++ = x;
			}
		}
		BATtseqbase(bn1, cnt2 == 1 ? *(oid *) Tloc(bn1, 0) : oid_nil);

		if (bn2) {
			BATsetcount(bn2, cnt1 * cnt2);
			bn2->tsorted = cnt1 <= 1 || cnt2 <= 1;
			bn2->trevsorted = cnt2 <= 1;
			bn2->tkey = cnt1 <= 1;
			bn2->tnil = false;
			bn2->tnonil = true;
			p = (oid *) Tloc(bn2, 0);
			for (i = 0; i < cnt1; i++) {
				for (j = 0; j < cnt2; j++) {
					*p++ = canditer_next(&ci2);
				}
				canditer_reset(&ci2);
			}
			BATtseqbase(bn2, cnt1 == 1 ? *(oid *) Tloc(bn2, 0) : oid_nil);
		}
	}
	*r1p = bn1;
	if (r2p)
		*r2p = bn2;
	if (r2p)
		TRC_DEBUG(ALGO, "BATsubcross()=(" ALGOBATFMT "," ALGOBATFMT ")\n", ALGOBATPAR(bn1), ALGOBATPAR(bn2));
	else
		TRC_DEBUG(ALGO, "BATsubcross()=(" ALGOBATFMT ")\n", ALGOBATPAR(bn1));
	return GDK_SUCCEED;
}
