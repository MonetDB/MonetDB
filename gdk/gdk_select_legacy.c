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

/* This file contains the legacy interface to the select functions */

static BAT *
BAT_select_(BAT *b, const void *tl, const void *th,
	    bit li, bit hi, bit tail, bit anti, const char *name)
{
	BAT *bn;
	BAT *bn1 = NULL;
	BAT *map;
	BAT *b1;

	ALGODEBUG fprintf(stderr, "#Legacy %s(b=%s#" BUNFMT "[%s,%s]%s%s%s,"
			  "li=%s,hi=%s,tail=%s,anti=%s)\n", name,
			  BATgetId(b), BATcount(b), ATOMname(b->htype), ATOMname(b->ttype),
			  BAThdense(b) ? "-hdense" : "",
			  b->tsorted ? "-sorted" : "",
			  b->trevsorted ? "-revsorted" : "",
			  li ? "true" : "false",
			  hi ? "true" : "false",
			  tail ? "true" : "false",
			  anti ? "true" : "false");
	BATcheck(b, "BAT_select_");
	/* b is a [any_1,any_2] BAT */
	if (!BAThdense(b)) {
		ALGODEBUG fprintf(stderr, "#BAT_select_(b=%s#" BUNFMT
				  ",tail=%d): make map\n",
				  BATgetId(b), BATcount(b), tail);
		map = BATmirror(BATmark(b, 0)); /* [dense1,any_1] */
		b1 = BATmirror(BATmark(BATmirror(b), 0)); /* dense1,any_2] */
	} else {
		ALGODEBUG fprintf(stderr, "#BAT_select_(b=%s#" BUNFMT
				  ",tail=%d): dense head\n",
				  BATgetId(b), BATcount(b), tail);
		map = NULL;
		b1 = b;		/* [dense1,any_2] (any_1==dense1) */
	}
	/* b1 is a [dense1,any_2] BAT, map (if set) is a [dense1,any_1] BAT */
	bn = BATsubselect(b1, NULL, tl, th, li, hi, anti);
	if (bn == NULL)
		goto error;
	/* bn is a [dense2,oid] BAT, if b was hdense, oid==any_1 */
	if (tail) {
		/* we want to return a [any_1,any_2] subset of b */
		if (map) {
			bn1 = BATproject(bn, map);
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense2,any_1] */
			BBPunfix(map->batCacheid);
			map = BATmirror(bn1);
			/* map is [any_1,dense2] */
			bn1 = BATproject(bn, b1);
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense2,any_2] */
			BBPunfix(b1->batCacheid);
			b1 = NULL;
			BBPunfix(bn->batCacheid);
			bn = VIEWcreate(map, bn1);
			if (bn == NULL)
				goto error;
			/* bn is [any_1,any_2] */
			BBPunfix(bn1->batCacheid);
			BBPunfix(map->batCacheid);
			bn1 = map = NULL;
		} else {
			bn1 = BATproject(bn, b);
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense2,any_2] */
			/* bn was [dense2,any_1] since b was hdense */
			b1 = VIEWcreate(BATmirror(bn), bn1);
			if (b1 == NULL)
				goto error;
			/* b1 is [any_1,any_2] */
			BBPunfix(bn->batCacheid);
			bn = b1;
			b1 = NULL;
			BBPunfix(bn1->batCacheid);
			bn1 = NULL;
		}
		if (th == NULL && !anti && BATcount(bn) > 0 &&
		    ATOMcmp(b->ttype, tl, ATOMnilptr(b->ttype)) == 0) {
			/* this was the only way to get nils, so we
			 * have nils if there are any values at all */
			bn->T->nil = 1;
		} else {
			/* we can't have nils */
			bn->T->nonil = 1;
		}
	} else {
		/* we want to return a [any_1,nil] BAT */
		if (map) {
			BBPunfix(b1->batCacheid);
			b1 = NULL;
			bn1 = BATproject(bn, map);
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense2,any_1] */
			BBPunfix(map->batCacheid);
			BBPunfix(bn->batCacheid);
			bn = bn1;
			map = bn1 = NULL;
		}
		BATseqbase(bn, oid_nil);
		/* bn is [nil,any_1] */
		bn = BATmirror(bn);
		/* bn is [any_1,nil] */
	}
	return bn;

  error:
	if (map)
		BBPunfix(map->batCacheid);
	if (b1 && b1 != b)
		BBPunfix(b1->batCacheid);
	if (bn1)
		BBPunfix(bn1->batCacheid);
	if (bn)
		BBPunfix(bn->batCacheid);
	return NULL;
}

BAT *
BATselect_(BAT *b, const void *h, const void *t, bit li, bit hi)
{
	return BAT_select_(b, h, t, li, hi, TRUE, FALSE, "BATselect_");
}

BAT *
BATuselect_(BAT *b, const void *h, const void *t, bit li, bit hi)
{
	return BAT_select_(b, h, t, li, hi, FALSE, FALSE, "BATuselect_");
}

BAT *
BATantiuselect_(BAT *b, const void *h, const void *t, bit li, bit hi)
{
	return BAT_select_(b, h, t, li, hi, FALSE, TRUE, "BATantiuselect");
}

/* Return a BAT which is a subset of b with just the qualifying
 * tuples. */
BAT *
BATselect(BAT *b, const void *h, const void *t)
{
	return BATselect_(b, h, t, TRUE, TRUE);
}

/* Return a BAT with in its head a subset of qualifying head values
 * from b, and void-nil in its tail. */
BAT *
BATuselect(BAT *b, const void *h, const void *t)
{
	return BATuselect_(b, h, t, TRUE, TRUE);
}
