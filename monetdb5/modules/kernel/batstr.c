/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 *  M.L. Kersten
 * String multiplexes
 * [TODO: property propagations]
 * The collection of routines provided here are map operations
 * for the atom string primitives.
 *
 * In line with the batcalc module, we assume that if two bat operands
 * are provided that they are aligned.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include <ctype.h>
#include <string.h>
#include "mal_exception.h"
#include "str.h"

static inline str
str_prefix(str *buf, size_t *buflen, str s, int l)
{
	return str_Sub_String(buf, buflen, s, 0, l);
}

static str
do_batstr_int(bat *res, const bat *l, const char *name, int (*func)(str))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	int *restrict vals;
	str x, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = func(x);
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatLength(bat *ret, const bat *l)
{
	return do_batstr_int(ret, l, "batstr.length", str_utf8_length);
}

static str
STRbatBytes(bat *ret, const bat *l)
{
	return do_batstr_int(ret, l, "batstr.bytes", str_nbytes);
}

static str
STRbatAscii(bat *res, const bat *l)
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL;
	BUN p, q;
	int *restrict vals, next;
	str x, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(left = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);

		if ((msg = str_wchr_at(&next, x, 0)) != MAL_SUCCEED)
			goto bailout;
		vals[p] = next;
		nils |= is_int_nil(next);
	}

bailout:
	if (left)
		BBPunfix(left->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatFromWChr(bat *res, const bat *l)
{
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = MAX(strlen(str_nil) + 1, 8);
	int *restrict vals, x;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	vals = Tloc(b, 0);
	for (p = 0; p < q ; p++) {
		x = vals[p];

		if (is_int_nil(x)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_from_wchr(&buf, &buflen, vals[p])) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatSpace(bat *res, const bat *l)
{
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict vals, x;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	char space[]= " ", *s = space;

	if (!buf) {
		msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.space", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	vals = Tloc(b, 0);
	for (p = 0; p < q ; p++) {
		x = vals[p];

		if (is_int_nil(x) || x < 0) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_repeat(&buf, &buflen, s, x)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
do_batstr_str(bat *res, const bat *l, const char *name, str (*func)(str *, size_t *, str))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: a BAT of strings 'l' and a constant string 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_conststr_str(bat *res, const bat *l, const str *s2, const char *name, size_t buflen, str (*func)(str*, size_t*, str, str))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	str x, y = *s2, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || strNil(y)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: two BATs of strings 'l' and 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batstr_str(bat *res, const bat *l, const bat *l2, const char *name, size_t buflen, str (*func)(str*, size_t*, str, str))
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	str x, y, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*l2))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = (str) BUNtail(righti, p);

		if (strNil(x) || strNil(y)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: a BAT of strings 'l' and a constant int 'n'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_str(bat *res, const bat *l, const int *n, const char *name, str (*func)(str*, size_t*, str, int))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	int y = *n;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || is_int_nil(y)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: a BAT of strings 'l' and a BAT of integers 'n'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_str(bat *res, const bat *l, const bat *n, const char *name, str (*func)(str*, size_t*, str, int))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	int *restrict righti, y;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*n))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = Tloc(right, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = righti[p];

		if (strNil(x) || is_int_nil(y)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: a BAT of strings 'l', a constant int 'n' and a constant str 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_conststr_str(bat *res, const bat *l, const int *n, const str *s2, const char *name, str (*func)(str*, size_t*, str, int, str))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, z = *s2, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	int y = *n;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || is_int_nil(y) || strNil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: a BAT of strings 'l', a BAT of integers 'n' and a constant str 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_conststr_str(bat *res, const bat *l, const bat *n, const str *s2, const char *name, str (*func)(str*, size_t*, str, int, str))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, z = *s2, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	int *restrict righti, y;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*n))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = Tloc(right, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = righti[p];

		if (strNil(x) || is_int_nil(y) || strNil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: a BAT of strings 'l', a constant int 'n' and a BAT of strings 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_batstr_str(bat *res, const bat *l, const int *n, const bat *l2, const char *name, str (*func)(str*, size_t*, str, int, str))
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, z, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	int y = *n;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*l2))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		z = (str) BUNtail(righti, p);

		if (strNil(x) || is_int_nil(y) || strNil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: a BAT of strings 'l', a BAT of int 'n' and a BAT of strings 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_batstr_str(bat *res, const bat *l, const bat *n, const bat *l2, const char *name, str (*func)(str*, size_t*, str, int, str))
{
	BATiter arg1i, arg3i;
	BAT *bn = NULL, *arg1 = NULL, *arg2 = NULL, *arg3 = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, z, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	int *restrict arg2i, y;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(arg1 = BATdescriptor(*l)) || !(arg2 = BATdescriptor(*n)) || !(arg3 = BATdescriptor(*l2))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(arg1) != BATcount(arg2) || BATcount(arg2) != BATcount(arg3)) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(arg1);
	if (!(bn = COLnew(arg1->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	arg1i = bat_iterator(arg1);
	arg2i = Tloc(arg2, 0);
	arg3i = bat_iterator(arg3);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(arg1i, p);
		y = arg2i[p];
		z = (str) BUNtail(arg3i, p);

		if (strNil(x) || is_int_nil(y) || strNil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (arg1)
		BBPunfix(arg1->batCacheid);
	if (arg2)
		BBPunfix(arg2->batCacheid);
	if (arg3)
		BBPunfix(arg3->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatLower(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.lower", str_lower);
}

static str
STRbatUpper(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.upper", str_upper);
}

static str
STRbatStrip(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.strip", str_strip);
}

static str
STRbatLtrim(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.ltrim", str_ltrim);
}

static str
STRbatRtrim(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.rtrim", str_rtrim);
}

static str
STRbatStrip2_const(bat *ret, const bat *l, const str *s2)
{
	return do_batstr_conststr_str(ret, l, s2, "batstr.strip", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_strip2);
}

static str
STRbatLtrim2_const(bat *ret, const bat *l, const str *s2)
{
	return do_batstr_conststr_str(ret, l, s2, "batstr.ltrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_ltrim2);
}

static str
STRbatRtrim2_const(bat *ret, const bat *l, const str *s2)
{
	return do_batstr_conststr_str(ret, l, s2, "batstr.rtrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_rtrim2);
}

static str
STRbatStrip2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.strip", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_strip2);
}

static str
STRbatLtrim2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.ltrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_ltrim2);
}

static str
STRbatRtrim2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.rtrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_rtrim2);
}

static str
STRbatLpad_const(bat *ret, const bat *l, const int *n)
{
	return do_batstr_constint_str(ret, l, n, "batstr.lpad", str_lpad);
}

static str
STRbatRpad_const(bat *ret, const bat *l, const int *n)
{
	return do_batstr_constint_str(ret, l, n, "batstr.rpad", str_rpad);
}

static str
STRbatLpad_bat(bat *ret, const bat *l, const bat *n)
{
	return do_batstr_batint_str(ret, l, n, "batstr.lpad", str_lpad);
}

static str
STRbatRpad_bat(bat *ret, const bat *l, const bat *n)
{
	return do_batstr_batint_str(ret, l, n, "batstr.rpad", str_rpad);
}

static str
STRbatLpad2_const_const(bat *ret, const bat *l, const int *n, const str *s2)
{
	return do_batstr_constint_conststr_str(ret, l, n, s2, "batstr.lpad", str_lpad2);
}

static str
STRbatRpad2_const_const(bat *ret, const bat *l, const int *n, const str *s2)
{
	return do_batstr_constint_conststr_str(ret, l, n, s2, "batstr.rpad", str_rpad2);
}

static str
STRbatLpad2_bat_const(bat *ret, const bat *l, const bat *n, const str *s2)
{
	return do_batstr_batint_conststr_str(ret, l, n, s2, "batstr.lpad", str_lpad2);
}

static str
STRbatRpad2_bat_const(bat *ret, const bat *l, const bat *n, const str *s2)
{
	return do_batstr_batint_conststr_str(ret, l, n, s2, "batstr.rpad", str_rpad2);
}

static str
STRbatLpad2_const_bat(bat *ret, const bat *l, const int *n, const bat *l2)
{
	return do_batstr_constint_batstr_str(ret, l, n, l2, "batstr.lpad", str_lpad2);
}

static str
STRbatRpad2_const_bat(bat *ret, const bat *l, const int *n, const bat *l2)
{
	return do_batstr_constint_batstr_str(ret, l, n, l2, "batstr.rpad", str_rpad2);
}

static str
STRbatLpad2_bat_bat(bat *ret, const bat *l, const bat *n, const bat *l2)
{
	return do_batstr_batint_batstr_str(ret, l, n, l2, "batstr.lpad", str_lpad2);
}

static str
STRbatRpad2_bat_bat(bat *ret, const bat *l, const bat *n, const bat *l2)
{
	return do_batstr_batint_batstr_str(ret, l, n, l2, "batstr.rpad", str_rpad2);
}

/*
 * A general assumption in all cases is the bats are synchronized on their
 * head column. This is not checked and may be mis-used to deploy the
 * implementation for shifted window arithmetic as well.
 */

static str
prefix_or_suffix(bat *res, const bat *l, const bat *r, const char *name, bit (*func)(str, str))
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	bit *restrict vals;
	str x, y, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_bit, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = (str) BUNtail(righti, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = bit_nil;
			nils = true;
		} else {
			vals[p] = func(x, y);
		}
	}

bailout:
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatPrefix(bat *res, const bat *l, const bat *r) 
{
	return prefix_or_suffix(res, l, r, "batstr.startsWith", str_is_prefix);
}

static str
STRbatSuffix(bat *res, const bat *l, const bat *r) 
{
	return prefix_or_suffix(res, l, r, "batstr.endsWith", str_is_suffix);
}

static str
prefix_or_suffix_cst(bat *res, const bat *l, str y, const char *name, bit (*func)(str, str))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL;
	BUN p, q;
	bit *restrict vals;
	str x, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(left = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_bit, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = bit_nil;
			nils = true;
		} else {
			vals[p] = func(x, y);
		}
	}

bailout:
	if (left)
		BBPunfix(left->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatPrefixcst(bat *res, const bat *l, const str *cst)
{
	return prefix_or_suffix_cst(res, l, *cst, "batstr.startsWith", str_is_prefix);
}

static str
STRbatSuffixcst(bat *res, const bat *l, const str *cst)
{
	return prefix_or_suffix_cst(res, l, *cst, "batstr.endsWith", str_is_suffix);
}

static str
STRbatstrSearch(bat *res, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	int *restrict vals;
	str x, y, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "batstr.search", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, "batstr.search", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.search", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = (str) BUNtail(righti, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_search(x, y);
		}
	}

bailout:
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatstrSearchcst(bat *res, const bat *l, const str *cst)
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL;
	BUN p, q;
	int *restrict vals;
	str x, y = *cst, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(left = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.search", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.search", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_search(x, y);
		}
	}

bailout:
	if (left)
		BBPunfix(left->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatRstrSearch(bat *res, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	int *restrict vals;
	str x, y, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "batstr.r_search", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, "batstr.r_search", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.r_search", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = (str) BUNtail(righti, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_reverse_str_search(x, y);
		}
	}

bailout:
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatRstrSearchcst(bat *res, const bat *l, const str *cst)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	int *restrict vals;
	str x, y = *cst, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.r_search", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.r_search", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_reverse_str_search(x, y);
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatWChrAt(bat *res, const bat *l, const bat *r)
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict righti, *restrict vals, next, y;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, "batstr.unicodeAt", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = Tloc(right, 0);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = righti[p];

		if (strNil(x) || is_int_nil(y) || y < 0) {
			vals[p] = int_nil;
			nils = true;
		} else {
			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout;
			vals[p] = next;
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatWChrAtcst(bat *res, const bat *l, const int *cst)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *cst, *restrict vals, next;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || is_int_nil(y) || y < 0) {
			vals[p] = int_nil;
			nils = true;
		} else {
			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout;
			vals[p] = next;
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
do_batstr_str_int_cst(bat *res, const bat *l, const int *cst, const char *name, str (*func)(str*, size_t*, str, int))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *cst;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || is_int_nil(y)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatprefixcst(bat *ret, const bat *l, const int *cst)
{
	return do_batstr_str_int_cst(ret, l, cst, "batstr.prefix", str_prefix);
}

static str
STRbatsuffixcst(bat *ret, const bat *l, const int *cst)
{
	return do_batstr_str_int_cst(ret, l, cst, "batstr.suffix", str_suffix);
}

static str
STRbatTailcst(bat *ret, const bat *l, const int *cst)
{
	return do_batstr_str_int_cst(ret, l, cst, "batstr.tail", str_tail);
}

static str
STRbatsubstringTailcst(bat *ret, const bat *l, const int *cst)
{
	return do_batstr_str_int_cst(ret, l, cst, "batstr.substring", str_substring_tail);
}

static str
STRbatrepeatcst(bat *res, const bat *l, const int *cst)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *cst;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || is_int_nil(y) || y < 0) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
do_batstr_strcst(bat *res, const str *cst, const bat *l, const char *name, str (*func)(str*, size_t*, str, int))
{
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict vals, y;
	str x = *cst, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	vals = Tloc(b, 0);
	for (p = 0; p < q ; p++) {
		y = vals[p];

		if (strNil(x) || is_int_nil(y)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatprefix_strcst(bat *ret, const str *cst, const bat *l)
{
	return do_batstr_strcst(ret, cst, l, "batstr.prefix", str_prefix);
}

static str
STRbatsuffix_strcst(bat *ret, const str *cst, const bat *l)
{
	return do_batstr_strcst(ret, cst, l, "batstr.suffix", str_suffix);
}

static str
STRbatTail_strcst(bat *ret, const str *cst, const bat *l)
{
	return do_batstr_strcst(ret, cst, l, "batstr.tail", str_tail);
}

static str
STRbatsubstringTail_strcst(bat *ret, const str *cst, const bat *l)
{
	return do_batstr_strcst(ret, cst, l, "batstr.substring", str_substring_tail);
}

static str
STRbatrepeat_strcst(bat *res, const str *cst, const bat *l)
{
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict vals, y;
	str x = *cst, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	vals = Tloc(b, 0);
	for (p = 0; p < q ; p++) {
		y = vals[p];

		if (strNil(x) || is_int_nil(y) || y < 0) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
do_batstr_str_int(bat *res, const bat *l, const bat *r, const char *name, str (*func)(str*, size_t*, str, int))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict righti, y;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, name, SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = Tloc(right, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = righti[p];

		if (strNil(x) || is_int_nil(y)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatprefix(bat *ret, const bat *l, const bat *r)
{
	return do_batstr_str_int(ret, l, r, "batstr.prefix", str_prefix);
}

static str
STRbatsuffix(bat *ret, const bat *l, const bat *r)
{
	return do_batstr_str_int(ret, l, r, "batstr.suffix", str_suffix);
}

static str
STRbatTail(bat *ret, const bat *l, const bat *r)
{
	return do_batstr_str_int(ret, l, r, "batstr.tail", str_tail);
}

static str
STRbatsubstringTail(bat *ret, const bat *l, const bat *r)
{
	return do_batstr_str_int(ret, l, r, "batstr.substring", str_substring_tail);
}

static str
STRbatrepeat(bat *res, const bat *l, const bat *r)
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict righti, y;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, "batstr.repeat", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = Tloc(right, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = righti[p];

		if (strNil(x) || is_int_nil(y) || y < 0) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatSubstitutecst(bat *res, const bat *bid, const str *arg2, const str *arg3, const bit *rep)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, y = *arg2, z = *arg3, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	bit w = *rep;

	if (!buf) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || strNil(y) || strNil(z) || is_bit_nil(w)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_substitute(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatSubstitute(bat *res, const bat *l, const bat *r, const bat *s, const bat *rep)
{
	BATiter arg1i, arg2i, arg3i;
	BAT *bn = NULL, *arg1 = NULL, *arg2 = NULL, *arg3 = NULL, *arg4 = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, y, z, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	bit *restrict arg4i, w;

	if (!buf) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(arg1 = BATdescriptor(*l)) || !(arg2 = BATdescriptor(*r)) || !(arg3 = BATdescriptor(*s)) || !(arg4 = BATdescriptor(*rep))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(arg1) != BATcount(arg2) || BATcount(arg2) != BATcount(arg3) || BATcount(arg3) != BATcount(arg4)) {
		msg = createException(MAL, "batstr.substritute", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(arg1);
	if (!(bn = COLnew(arg1->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	arg1i = bat_iterator(arg1);
	arg2i = bat_iterator(arg2);
	arg3i = bat_iterator(arg3);
	arg4i = Tloc(arg4, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(arg1i, p);
		y = (str) BUNtail(arg2i, p);
		z = (str) BUNtail(arg3i, p);
		w = arg4i[p];

		if (strNil(x) || strNil(y) || strNil(z) || is_bit_nil(w)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_substitute(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (arg1)
		BBPunfix(arg1->batCacheid);
	if (arg2)
		BBPunfix(arg2->batCacheid);
	if (arg3)
		BBPunfix(arg3->batCacheid);
	if (arg4)
		BBPunfix(arg4->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatsplitpartcst(bat *res, const bat *bid, const str *needle, const int *field)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int z = *field;
	str x, y = *needle, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || strNil(y) || is_int_nil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatsplitpart_needlecst(bat *res, const bat *bid, const str *needle, const bat *fid)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *f = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict field, z;
	str x, y = *needle, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(f = BATdescriptor(*fid))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(b) != BATcount(f)) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	field = Tloc(f, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);
		z = field[p];

		if (strNil(x) || strNil(y) || is_int_nil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (f)
		BBPunfix(f->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatsplitpart_fieldcst(bat *res, const bat *bid, const bat *nid, const int *field)
{
	BATiter bi, ni;
	BAT *bn = NULL, *b = NULL, *n = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int z = *field;
	str x, y, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(n = BATdescriptor(*nid))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(b) != BATcount(n)) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	ni = bat_iterator(n);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);
		y = (str) BUNtail(ni, p);

		if (strNil(x) || strNil(y) || is_int_nil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (n)
		BBPunfix(n->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatsplitpart(bat *res, const bat *l, const bat *r, const bat *t)
{
	BATiter arg1i, arg2i;
	BAT *bn = NULL, *arg1 = NULL, *arg2 = NULL, *arg3 = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict arg3i, z;
	str x, y, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(arg1 = BATdescriptor(*l)) || !(arg2 = BATdescriptor(*r)) || !(arg3 = BATdescriptor(*t))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(arg1) != BATcount(arg2) || BATcount(arg2) != BATcount(arg3)) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(arg1);
	if (!(bn = COLnew(arg1->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	arg1i = bat_iterator(arg1);
	arg2i = bat_iterator(arg2);
	arg3i = Tloc(arg3, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(arg1i, p);
		y = (str) BUNtail(arg2i, p);
		z = arg3i[p];

		if (strNil(x) || strNil(y) || is_int_nil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (arg1)
		BBPunfix(arg1->batCacheid);
	if (arg2)
		BBPunfix(arg2->batCacheid);
	if (arg3)
		BBPunfix(arg3->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatReplacecst(bat *res, const bat *bid, const str *pat, const str *s2)
{
	bit rep = TRUE;
	return STRbatSubstitutecst(res, bid, pat, s2, &rep);
}

static str
STRbatReplace(bat *res, const bat *l, const bat *s, const bat *s2)
{
	BATiter arg1i, arg2i, arg3i;
	BAT *bn = NULL, *arg1 = NULL, *arg2 = NULL, *arg3 = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, y, z, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(arg1 = BATdescriptor(*l)) || !(arg2 = BATdescriptor(*s)) || !(arg3 = BATdescriptor(*s2))) {
		msg = createException(MAL, "batstr.replace", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(arg1) != BATcount(arg2) || BATcount(arg2) != BATcount(arg3)) {
		msg = createException(MAL, "batstr.replace", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(arg1);
	if (!(bn = COLnew(arg1->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	arg1i = bat_iterator(arg1);
	arg2i = bat_iterator(arg2);
	arg3i = bat_iterator(arg3);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(arg1i, p);
		y = (str) BUNtail(arg2i, p);
		z = (str) BUNtail(arg3i, p);

		if (strNil(x) || strNil(y) || strNil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_substitute(&buf, &buflen, x, y, z, TRUE)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (arg1)
		BBPunfix(arg1->batCacheid);
	if (arg2)
		BBPunfix(arg2->batCacheid);
	if (arg3)
		BBPunfix(arg3->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatInsert(bat *res, const bat *l, const bat *s, const bat *chars, const bat *s2)
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL, *start = NULL, *nchars = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *starti, *ncharsi, y, z;
	str x, w, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(start = BATdescriptor(*s)) || !(nchars = BATdescriptor(*chars)) || !(right = BATdescriptor(*s2))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(start) || BATcount(start) != BATcount(nchars) || BATcount(nchars) != BATcount(right)) {
		msg = createException(MAL, "batstr.insert", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	starti = Tloc(start, 0);
	ncharsi = Tloc(nchars, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = starti[p];
		z = ncharsi[p];
		w = (str) BUNtail(righti, p);

		if (strNil(x) || is_int_nil(y) || is_int_nil(z) || strNil(w)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_insert(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (start)
		BBPunfix(start->batCacheid);
	if (nchars)
		BBPunfix(nchars->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatInsertcst(bat *res, const bat *bid, const int *start, const int *nchars, const str *input2)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *start, z = *nchars;
	str x, w = *input2, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || is_int_nil(y) || is_int_nil(z) || strNil(w)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_insert(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/*
 * The substring functions require slightly different arguments
 */
static str
STRbatsubstringcst(bat *res, const bat *bid, const int *start, const int *length)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *start, z = *length;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatsubstringcst_startcst(bat *res, const bat *bid, const int *start, const bat *l)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *lb = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *start, *len, z;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(lb = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(b) != BATcount(lb)) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	len = Tloc(lb, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);
		z = len[p];

		if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (lb)
		BBPunfix(lb->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatsubstringcst_indexcst(bat *res, const bat *bid, const bat *l, const int *index)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *lb = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *start, y, z = *index;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(lb = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(b) != BATcount(lb)) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	start = Tloc(lb, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);
		y = start[p];

		if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (b)
		BBPunfix(b->batCacheid);
	if (lb)
		BBPunfix(lb->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatsubstring(bat *res, const bat *l, const bat *r, const bat *t)
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *start = NULL, *length = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *starti, *lengthi, y, z;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(start = BATdescriptor(*r)) || !(length = BATdescriptor(*t))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(start) || BATcount(start) != BATcount(length)) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	starti = Tloc(start, 0);
	lengthi = Tloc(length, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = starti[p];
		z = lengthi[p];

		if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (left)
		BBPunfix(left->batCacheid);
	if (start)
		BBPunfix(start->batCacheid);
	if (length)
		BBPunfix(length->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatstrLocatecst(bat *res, const bat *l, const str *s2)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	int *restrict vals;
	str x, y = *s2, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_locate2(x, y, 1);
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatstrLocate_strcst(bat *res, const str *s, const bat *l)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	int *restrict vals;
	str x = *s, y, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		y = (str) BUNtail(bi, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_locate2(x, y, 1);
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatstrLocate(bat *res, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	int *restrict vals;
	str x, y, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, "batstr.locate", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = (str) BUNtail(righti, p);

		if (strNil(x) || strNil(y)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_locate2(x, y, 1);
		}
	}

bailout:
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatstrLocate2cst(bat *res, const bat *l, const str *s2, const int *start)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	int *restrict vals, z = *start;
	str x, y = *s2, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if (!(bn = COLnew(b->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(bi, p);

		if (strNil(x) || strNil(y) || is_int_nil(z)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_locate2(x, y, z);
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatstrLocate2(bat *res, const bat *l, const bat *r, const bat *s)
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL, *start = NULL;
	BUN p, q;
	int *restrict vals, *restrict starti, z;
	str x, y, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r)) || !(start = BATdescriptor(*s))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (BATcount(left) != BATcount(right) || BATcount(right) != BATcount(start)) {
		msg = createException(MAL, "batstr.locate2", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	starti = Tloc(start, 0);
	vals = Tloc(bn, 0);
	for (p = 0; p < q ; p++) {
		x = (str) BUNtail(lefti, p);
		y = (str) BUNtail(righti, p);
		z = starti[p];

		if (strNil(x) || strNil(y) || is_int_nil(z)) {
			vals[p] = int_nil;
			nils = true;
		} else {
			vals[p] = str_locate2(x, y, z);
		}
	}

bailout:
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (start)
		BBPunfix(start->batCacheid);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap.dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

#include "mel.h"
mel_func batstr_init_funcs[] = {
 command("batstr", "length", STRbatLength, false, "Return the length of a string.", args(1,2, batarg("",int),batarg("s",str))),
 command("batstr", "nbytes", STRbatBytes, false, "Return the string length in bytes.", args(1,2, batarg("",int),batarg("s",str))),
 command("batstr", "toLower", STRbatLower, false, "Convert a string to lower case.", args(1,2, batarg("",str),batarg("s",str))),
 command("batstr", "toUpper", STRbatUpper, false, "Convert a string to upper case.", args(1,2, batarg("",str),batarg("s",str))),
 command("batstr", "trim", STRbatStrip, false, "Strip whitespaces around a string.", args(1,2, batarg("",str),batarg("s",str))),
 command("batstr", "ltrim", STRbatLtrim, false, "Strip whitespaces from start of a string.", args(1,2, batarg("",str),batarg("s",str))),
 command("batstr", "rtrim", STRbatRtrim, false, "Strip whitespaces from end of a string.", args(1,2, batarg("",str),batarg("s",str))),
 command("batstr", "trim", STRbatStrip2_const, false, "Strip characters in the second string around the first strings.", args(1,3, batarg("",str),batarg("s",str),arg("s2",str))),
 command("batstr", "ltrim", STRbatLtrim2_const, false, "Strip characters in the second string from start of the first strings.", args(1,3, batarg("",str),batarg("s",str),arg("s2",str))),
 command("batstr", "rtrim", STRbatRtrim2_const, false, "Strip characters in the second string from end of the first strings.", args(1,3, batarg("",str),batarg("s",str),arg("s2",str))),
 command("batstr", "trim", STRbatStrip2_bat, false, "Strip characters in the second strings around the first strings.", args(1,3, batarg("",str),batarg("s",str),batarg("s2",str))),
 command("batstr", "ltrim", STRbatLtrim2_bat, false, "Strip characters in the second strings from start of the first strings.", args(1,3, batarg("",str),batarg("s",str),batarg("s2",str))),
 command("batstr", "rtrim", STRbatRtrim2_bat, false, "Strip characters in the second strings from end of the first strings.", args(1,3, batarg("",str),batarg("s",str),batarg("s2",str))),
 command("batstr", "lpad", STRbatLpad_const, false, "Prepend whitespaces to the strings to reach the given length. Truncate the strings on the right if their lengths is larger than the given length.", args(1,3, batarg("",str),batarg("s",str),arg("n",int))),
 command("batstr", "rpad", STRbatRpad_const, false, "Append whitespaces to the strings to reach the given length. Truncate the strings on the right if their lengths is larger than the given length.", args(1,3, batarg("",str),batarg("s",str),arg("n",int))),
 command("batstr", "lpad", STRbatLpad_bat, false, "Prepend whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,3, batarg("",str),batarg("s",str),batarg("n",int))),
 command("batstr", "rpad", STRbatRpad_bat, false, "Append whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,3, batarg("",str),batarg("s",str),batarg("n",int))),
 command("batstr", "lpad", STRbatLpad2_const_const, false, "Prepend the second string to the first strings to reach the given length. Truncate the first strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),arg("s2",str))),
 command("batstr", "rpad", STRbatRpad2_const_const, false, "Append the second string to the first strings to reach the given length. Truncate the first strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),arg("s2",str))),
 command("batstr", "lpad", STRbatLpad2_bat_const, false, "Prepend the second string to the first strings to reach the given lengths. Truncate the first strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),batarg("s",str),batarg("n",int),arg("s2",str))),
 command("batstr", "rpad", STRbatRpad2_bat_const, false, "Append the second string to the first strings to reach the given lengths. Truncate the first strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),batarg("s",str),batarg("n",int),arg("s2",str))),
 command("batstr", "lpad", STRbatLpad2_const_bat, false, "Prepend the second strings to the first strings to reach the given length. Truncate the first strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),batarg("s2",str))),
 command("batstr", "rpad", STRbatRpad2_const_bat, false, "Append the second strings to the first strings to reach the given length. Truncate the first strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),batarg("s2",str))),
 command("batstr", "lpad", STRbatLpad2_bat_bat, false, "Prepend the second strings to the first strings to reach the given lengths. Truncate the first strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),batarg("s",str),batarg("n",int),batarg("s2",str))),
 command("batstr", "rpad", STRbatRpad2_bat_bat, false, "Append the second strings to the first strings to reach the given lengths. Truncate the first strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),batarg("s",str),batarg("n",int),batarg("s2",str))),
 command("batstr", "startsWith", STRbatPrefix, false, "Prefix check.", args(1,3, batarg("",bit),batarg("s",str),batarg("prefix",str))),
 command("batstr", "startsWith", STRbatPrefixcst, false, "Prefix check.", args(1,3, batarg("",bit),batarg("s",str),arg("prefix",str))),
 command("batstr", "endsWith", STRbatSuffix, false, "Suffix check.", args(1,3, batarg("",bit),batarg("s",str),batarg("suffix",str))),
 command("batstr", "endsWith", STRbatSuffixcst, false, "Suffix check.", args(1,3, batarg("",bit),batarg("s",str),arg("suffix",str))),
 command("batstr", "splitpart", STRbatsplitpart, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),batarg("needle",str),batarg("field",int))),
 command("batstr", "splitpart", STRbatsplitpartcst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),arg("needle",str),arg("field",int))),
 command("batstr", "splitpart", STRbatsplitpart_needlecst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),arg("needle",str),batarg("field",int))),
 command("batstr", "splitpart", STRbatsplitpart_fieldcst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),batarg("needle",str),arg("field",int))),
 command("batstr", "search", STRbatstrSearch, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),batarg("c",str))),
 command("batstr", "search", STRbatstrSearchcst, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),arg("c",str))),
 command("batstr", "r_search", STRbatRstrSearch, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),batarg("c",str))),
 command("batstr", "r_search", STRbatRstrSearchcst, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),arg("c",str))),
 command("batstr", "string", STRbatTail, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,3, batarg("",str),batarg("b",str),batarg("offset",int))),
 command("batstr", "string", STRbatTailcst, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,3, batarg("",str),batarg("b",str),arg("offset",int))),
 command("batstr", "string", STRbatTail_strcst, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,3, batarg("",str),arg("b",str),batarg("offset",int))),
 command("batstr", "ascii", STRbatAscii, false, "Return unicode of head of string", args(1,2, batarg("",int),batarg("s",str))),
 command("batstr", "substring", STRbatsubstringTail, false, "Extract the tail of a string", args(1,3, batarg("",str),batarg("s",str),batarg("start",int))),
 command("batstr", "substring", STRbatsubstringTailcst, false, "Extract the tail of a string", args(1,3, batarg("",str),batarg("s",str),arg("start",int))),
 command("batstr", "substring", STRbatsubstringTail_strcst, false, "Extract the tail of a string", args(1,3, batarg("",str),arg("s",str),batarg("start",int))),
 command("batstr", "substring", STRbatsubstring, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),batarg("start",int),batarg("index",int))),
 command("batstr", "substring", STRbatsubstringcst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),arg("start",int),arg("index",int))),
 command("batstr", "substring", STRbatsubstringcst_startcst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),arg("start",int),batarg("index",int))),
 command("batstr", "substring", STRbatsubstringcst_indexcst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),batarg("start",int),arg("index",int))),
 command("batstr", "unicode", STRbatFromWChr, false, "convert a unicode to a character.", args(1,2, batarg("",str),batarg("wchar",int))),
 command("batstr", "unicodeAt", STRbatWChrAt, false, "get a unicode character (as an int) from a string position.", args(1,3, batarg("",int),batarg("s",str),batarg("index",int))),
 command("batstr", "unicodeAt", STRbatWChrAtcst, false, "get a unicode character (as an int) from a string position.", args(1,3, batarg("",int),batarg("s",str),arg("index",int))),
 command("batstr", "substitute", STRbatSubstitute, false, "Substitute first occurrence of 'src' by\n'dst'. Iff repeated = true this is\nrepeated while 'src' can be found in the\nresult string. In order to prevent\nrecursion and result strings of unlimited\nsize, repeating is only done iff src is\nnot a substring of dst.", args(1,5, batarg("",str),batarg("s",str),batarg("src",str),batarg("dst",str),batarg("rep",bit))),
 command("batstr", "substitute", STRbatSubstitutecst, false, "Substitute first occurrence of 'src' by\n'dst'. Iff repeated = true this is\nrepeated while 'src' can be found in the\nresult string. In order to prevent\nrecursion and result strings of unlimited\nsize, repeating is only done iff src is\nnot a substring of dst.", args(1,5, batarg("",str),batarg("s",str),arg("src",str),arg("dst",str),arg("rep",bit))),
 command("batstr", "stringleft", STRbatprefix, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("l",int))),
 command("batstr", "stringleft", STRbatprefixcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("l",int))),
 command("batstr", "stringleft", STRbatprefix_strcst, false, "", args(1,3, batarg("",str),arg("s",str),batarg("l",int))),
 command("batstr", "stringright", STRbatsuffix, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("l",int))),
 command("batstr", "stringright", STRbatsuffixcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("l",int))),
 command("batstr", "stringright", STRbatsuffix_strcst, false, "", args(1,3, batarg("",str),arg("s",str),batarg("l",int))),
 command("batstr", "locate", STRbatstrLocate, false, "Locate the start position of a string", args(1,3, batarg("",int),batarg("s1",str),batarg("s2",str))),
 command("batstr", "locate", STRbatstrLocatecst, false, "Locate the start position of a string", args(1,3, batarg("",int),batarg("s1",str),arg("s2",str))),
 command("batstr", "locate", STRbatstrLocate_strcst, false, "Locate the start position of a string", args(1,3, batarg("",int),arg("s1",str),batarg("s2",str))),
 command("batstr", "locate", STRbatstrLocate2, false, "Locate the start position of a string", args(1,4, batarg("",int),batarg("s1",str),batarg("s2",str),batarg("start",int))),
 command("batstr", "locate", STRbatstrLocate2cst, false, "Locate the start position of a string", args(1,4, batarg("",int),batarg("s1",str),arg("s2",str),arg("start",int))),
 command("batstr", "insert", STRbatInsert, false, "Insert a string into another", args(1,5, batarg("",str),batarg("s",str),batarg("start",int),batarg("l",int),batarg("s2",str))),
 command("batstr", "insert", STRbatInsertcst, false, "Insert a string into another", args(1,5, batarg("",str),batarg("s",str),arg("start",int),arg("l",int),arg("s2",str))),
 command("batstr", "replace", STRbatReplace, false, "Insert a string into another", args(1,4, batarg("",str),batarg("s",str),batarg("pat",str),batarg("s2",str))),
 command("batstr", "replace", STRbatReplacecst, false, "Insert a string into another", args(1,4, batarg("",str),batarg("s",str),arg("pat",str),arg("s2",str))),
 command("batstr", "repeat", STRbatrepeat, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("c",int))),
 command("batstr", "repeat", STRbatrepeatcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("c",int))),
 command("batstr", "repeat", STRbatrepeat_strcst, false, "", args(1,3, batarg("",str),arg("s",str),batarg("c",int))),
 command("batstr", "space", STRbatSpace, false, "", args(1,2, batarg("",str),batarg("l",int))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batstr_mal)
{ mal_module("batstr", NULL, batstr_init_funcs); }
