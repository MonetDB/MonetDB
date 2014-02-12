/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

gdk_return
BATcross1(BAT **r1p, BAT **r2p, BAT *l, BAT *r)
{
	BAT *bn1, *bn2;
	BUN i, j;
	oid *p1, *p2;

	assert(BAThdense(l));
	assert(BAThdense(r));
	bn1 = BATnew(TYPE_void, TYPE_oid, BATcount(l) * BATcount(r));
	bn2 = BATnew(TYPE_void, TYPE_oid, BATcount(l) * BATcount(r));
	if (bn1 == NULL || bn2 == NULL) {
		if (bn1 != NULL)
			BBPreclaim(bn1);
		if (bn2 != NULL)
			BBPreclaim(bn2);
		return GDK_FAIL;
	}
	BATseqbase(bn1, 0);
	BATseqbase(bn2, 0);
	p1 = (oid *) Tloc(bn1, BUNfirst(bn1));
	p2 = (oid *) Tloc(bn2, BUNfirst(bn2));
	for (i = 0; i < BATcount(l); i++) {
		for (j = 0; j < BATcount(r); j++) {
			*p1++ = i + l->hseqbase;
			*p2++ = j + r->hseqbase;
		}
	}
	BATsetcount(bn1, BATcount(l) * BATcount(r));
	BATsetcount(bn2, BATcount(l) * BATcount(r));
	bn1->tsorted = 1;
	bn1->trevsorted = BATcount(l) <= 1;
	bn1->tkey = BATcount(r) <= 1;
	bn1->tdense = bn1->tkey != 0;
	bn1->T->nil = 0;
	bn1->T->nonil = 1;
	bn2->tsorted = BATcount(l) <= 1;
	bn2->trevsorted = BATcount(bn2) <= 1;
	bn2->tkey = BATcount(l) <= 1;
	bn2->tdense = bn2->tkey != 0;
	bn2->T->nil = 0;
	bn2->T->nonil = 1;
	*r1p = bn1;
	*r2p = bn2;
	return GDK_SUCCEED;
}

gdk_return
BATsubcross(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr)
{
	BAT *bn1, *bn2, *t;

	if (BATcross1(&bn1, &bn2, sl ? sl : l, sr ? sr : r) == GDK_FAIL)
		return GDK_FAIL;
	if (sl) {
		t = BATproject(bn1, sl);
		BBPunfix(bn1->batCacheid);
		if (t == NULL) {
			BBPunfix(bn2->batCacheid);
			return GDK_FAIL;
		}
		bn1 = t;
	}
	if (sr) {
		t = BATproject(bn2, sr);
		BBPunfix(bn2->batCacheid);
		if (t == NULL) {
			BBPunfix(bn1->batCacheid);
			return GDK_FAIL;
		}
		bn2 = t;
	}
	*r1p = bn1;
	*r2p = bn2;
	return GDK_SUCCEED;
}

BAT *
BATcross(BAT *l, BAT *r)
{
	BAT *bn1, *bn2, *t;

	l = BATmirror(BATmark(l, 0));
	if (l == NULL)
		return NULL;
	r = BATmirror(BATmark(BATmirror(r), 0));
	if (r == NULL) {
		BBPunfix(l->batCacheid);
		return NULL;
	}

	if (BATcross1(&bn1, &bn2, l, r) == GDK_FAIL)
		goto bailout;
	t = BATproject(bn1, l);
	BBPunfix(bn1->batCacheid);
	if (t == NULL) {
		BBPunfix(bn2->batCacheid);
		goto bailout;
	}
	bn1 = t;
	t = BATproject(bn2, r);
	BBPunfix(bn2->batCacheid);
	if (t == NULL) {
		BBPunfix(bn1->batCacheid);
		goto bailout;
	}
	bn2 = t;
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	t = VIEWcreate(BATmirror(bn1), bn2);
	BBPunfix(bn1->batCacheid);
	BBPunfix(bn2->batCacheid);
	return t;

  bailout:
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	return NULL;
}
