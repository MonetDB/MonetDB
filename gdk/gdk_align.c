/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * @a Peter Boncz, Niels Nes
 * @* BAT Alignment
 * For BATs that result from a n-ary relational scheme it may help to
 * align the BATs on their head value. In particular, it permits
 * replacing a hash-join by a merge-join, which is significantly
 * faster on large tables. Especially if the BATs involved cause page
 * activity or when you can not afford the large hash structures to
 * speed-up processing.
 *
 * For orthogonality, we support alignment between arbitrary columns
 * (head or tail).
 *
 * All standard GDK set-calls update the alignment info in their
 * respective ways. For example, the routine @emph{BUNclustercopy}
 * shuffles the first argument, such that the BUNs are in the same
 * order as those in the second argument.  This operation will mark
 * both columns of the first @emph{BAT} as synced with the second
 * (likewise, @emph{Colcopy()}, which makes a copy, instead of
 * in-place shuffling, has the same alignment effect.
 *
 * Each alignment sequence is given a unique identifier, so as to
 * easily detect this situation. It is retained in the @emph{BAT
 * descriptor}.
 * @+ Alignment Design Considerations
 * Alignment primitives require the right hooks to be inserted in
 * several places in the GDK, apart form this file:
 * @itemize
 * @item @emph{ BUN update operations}.
 * The updated BATs have to be marked as un-aligned.
 * @item @emph{ set operations}.
 * For most relational operations some statements can be made about
 * the size and order of the BATs they produce. This information can
 * be formalized by indicating alignment information automatically.
 * @item @emph{ transaction operations}.
 * Alignment statuses must be kept consistent under database commits
 * and aborts.
 * @end itemize
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* Return TRUE if the two BATs are aligned (same size, same
 * hseqbase). */
int
ALIGNsynced(BAT *b1, BAT *b2)
{
	if (b1 == NULL || b2 == NULL)
		return 0;

	assert(!is_oid_nil(b1->hseqbase));
	assert(!is_oid_nil(b2->hseqbase));

	return BATcount(b1) == BATcount(b2) && b1->hseqbase == b2->hseqbase;
}

gdk_return
BATmaterialize(BAT *b)
{
	int tt;
	BUN cnt;
	Heap tail;
	BUN p, q;
	oid t, *x;

	BATcheck(b, "BATmaterialize", GDK_FAIL);
	tt = b->ttype;
	cnt = BATcapacity(b);
	tail = b->theap;
	p = 0;
	q = BUNlast(b);
	assert(cnt >= q - p);
	ALGODEBUG fprintf(stderr, "#BATmaterialize(" ALGOBATFMT ")\n",
			  ALGOBATPAR(b));

	if (tt != TYPE_void) {
		/* no voids */
		return GDK_SUCCEED;
	}
	tt = TYPE_oid;

	/* cleanup possible ACC's */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);

	snprintf(b->theap.filename, sizeof(b->theap.filename), "%s.tail", BBP_physical(b->batCacheid));
	if (HEAPalloc(&b->theap, cnt, sizeof(oid)) != GDK_SUCCEED) {
		b->theap = tail;
		return GDK_FAIL;
	}

	/* point of no return */
	b->ttype = tt;
	BATsetdims(b);
	b->batDirtydesc = TRUE;
	b->theap.dirty = TRUE;

	/* So now generate [t..t+cnt-1] */
	t = b->tseqbase;
	x = (oid *) b->theap.base;
	if (is_oid_nil(t)) {
		while (p < q)
			x[p++] = oid_nil;
	} else {
		while (p < q)
			x[p++] = t++;
	}
	BATsetcount(b, b->batCount);

	/* cleanup the old heaps */
	HEAPfree(&tail, false);
	return GDK_SUCCEED;
}

