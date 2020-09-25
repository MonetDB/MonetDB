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

#define prepareOperand(X,Y,Z)									\
	if( (X= BATdescriptor(*Y)) == NULL )						\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
#define prepareOperand2(X,Y,A,B,Z)								\
	if( (X= BATdescriptor(*Y)) == NULL )						\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	if( (A= BATdescriptor(*B)) == NULL ){						\
		BBPunfix(X->batCacheid);								\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	}
#define prepareOperand3(X,Y,A,B,I,J,Z)							\
	if( (X= BATdescriptor(*Y)) == NULL )						\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	if( (A= BATdescriptor(*B)) == NULL ){						\
		BBPunfix(X->batCacheid);								\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	}															\
	if( (I= BATdescriptor(*J)) == NULL ){						\
		BBPunfix(X->batCacheid);								\
		BBPunfix(A->batCacheid);								\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	}
#define prepareResult(X,Y,T,Z)							\
	X= COLnew((Y)->hseqbase,T,BATcount(Y), TRANSIENT);	\
	if( X == NULL){										\
		BBPunfix(Y->batCacheid);						\
		throw(MAL, Z, SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
	}													\
	X->tsorted=false;									\
	X->trevsorted=false;
#define prepareResult2(X,Y,A,T,Z)						\
	X= COLnew((Y)->hseqbase,T,BATcount(Y), TRANSIENT);	\
	if( X == NULL){										\
		BBPunfix(Y->batCacheid);						\
		BBPunfix(A->batCacheid);						\
		throw(MAL, Z, SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
	}													\
	X->tsorted=false;									\
	X->trevsorted=false;
#define finalizeResult(X,Y,Z)								\
	(Y)->theap.dirty |= BATcount(Y) > 0;					\
	*X = (Y)->batCacheid;									\
	BBPkeepref(*(X));										\
	BBPunfix(Z->batCacheid);

static inline str
str_prefix(str *buf, size_t *buflen, const char *s, int l)
{
	return str_Sub_String(buf, buflen, s, 0, l);
}

static str
do_batstr_int(bat *res, const bat *l, const char *name, int (*func)(const char *))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	int *restrict vals, next;
	str x, msg = MAL_SUCCEED;
	bool nils = false;

	if ((b = BATdescriptor(*l)) == NULL) {
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
		next = func(x);
		vals[p] = next;
		nils |= is_int_nil(next);
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
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRbatLength(bat *ret, const bat *l)
{
	return do_batstr_int(ret, l, "batstr.length", str_length);
}

static str
STRbatBytes(bat *ret, const bat *l)
{
	return do_batstr_int(ret, l, "batstr.bytes", str_bytes);
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
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict vals;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if ((b = BATdescriptor(*l)) == NULL) {
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
		if ((msg = str_from_wchr(&buf, &buflen, vals[p])) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	int *restrict vals;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if ((b = BATdescriptor(*l)) == NULL) {
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
		char space[]= " ", *s = space;

		if ((msg = str_repeat(&buf, &buflen, s, vals[p])) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
do_batstr_str(bat *res, const bat *l, const char *name, str (*func)(str *, size_t *, const char *))
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

		if ((msg = func(&buf, &buflen, x)) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

/* Input: a BAT of strings 'l' and a constant string 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_conststr_str(bat *ret, const bat *l, const str *s2, const char *name, str (*func)(str *, const str *, const str *))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtvar(bi, p);
		if (!strNil(x) &&
			(msg = (*func)(&y, &x, s2)) != MAL_SUCCEED)
			goto bailout;
		if (y == NULL)
			y = (str) str_nil;
		if (bunfastappVAR(bn, y) != GDK_SUCCEED) {
			if (y != str_nil)
				GDKfree(y);
			goto bailout;
		}
		if (y == str_nil) {
			bn->tnonil = false;
			bn->tnil = true;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: two BATs of strings 'l' and 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batstr_str(bat *ret, const bat *l, const bat *l2, const char *name, str (*func)(str *, const str *, const str *))
{
	BATiter bi, bi2;
	BAT *bn, *b, *b2;
	BUN p, q;
	str x, x2, y;
	str msg = MAL_SUCCEED;

	prepareOperand2(b, l, b2, l2, name);
	if(BATcount(b) != BATcount(b2)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn, b, b2, TYPE_str, name);

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtvar(bi, p);
		x2 = (str) BUNtvar(bi2, p);
		if (!strNil(x) &&
			!strNil(x2) &&
			(msg = (*func)(&y, &x, &x2)) != MAL_SUCCEED)
			goto bailout;
		if (y == NULL)
			y = (str) str_nil;
		if (bunfastappVAR(bn, y) != GDK_SUCCEED) {
			if (y != str_nil)
				GDKfree(y);
			goto bailout;
		}
		if (y == str_nil) {
			bn->tnonil = false;
			bn->tnil = true;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l' and a constant int 'n'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_str(bat *ret, const bat *l, const int *n, const char *name, str (*func)(str *, const str *, const int *))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtvar(bi, p);
		if (!strNil(x) &&
			(msg = (*func)(&y, &x, n)) != MAL_SUCCEED)
			goto bailout;
		if (y == NULL)
			y = (str) str_nil;
		if (bunfastappVAR(bn, y) != GDK_SUCCEED) {
			if (y != str_nil)
				GDKfree(y);
			goto bailout;
		}
		if (y == str_nil) {
			bn->tnonil = false;
			bn->tnil = true;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l' and a BAT of integers 'n'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_str(bat *ret, const bat *l, const bat *n, const char *name, str (*func)(str *, const str *, const int *))
{
	BATiter bi, bi2;
	BAT *bn, *b, *b2;
	BUN p, q;
	int nn;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand2(b, l, b2, n, name);
	if(BATcount(b) != BATcount(b2)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn, b, b2, TYPE_str, name);

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtvar(bi, p);
		nn = *(int *)BUNtloc(bi2, p);
		if (!strNil(x) &&
			(msg = (*func)(&y, &x, &nn)) != MAL_SUCCEED)
			goto bailout;
		if (y == NULL)
			y = (str) str_nil;
		if (bunfastappVAR(bn, y) != GDK_SUCCEED) {
			if (y != str_nil)
				GDKfree(y);
			goto bailout;
		}
		if (y == str_nil) {
			bn->tnonil = false;
			bn->tnil = true;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l', a constant int 'n' and a constant str 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_conststr_str(bat *ret, const bat *l, const int *n, const str *s2, const char *name, str (*func)(str *, const str *, const int *, const str *))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtvar(bi, p);
		if (!strNil(x) &&
			(msg = (*func)(&y, &x, n, s2)) != MAL_SUCCEED)
			goto bailout;
		if (y == NULL)
			y = (str) str_nil;
		if (bunfastappVAR(bn, y) != GDK_SUCCEED) {
			if (y != str_nil)
				GDKfree(y);
			goto bailout;
		}
		if (y == str_nil) {
			bn->tnonil = false;
			bn->tnil = true;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l', a BAT of integers 'n' and a constant str 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_conststr_str(bat *ret, const bat *l, const bat *n, const str *s2, const char *name, str (*func)(str *, const str *, const int *, const str *))
{
	BATiter bi, bi2;
	BAT *bn, *b, *b2;
	BUN p, q;
	int nn;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand2(b, l, b2, n, name);
	if(BATcount(b) != BATcount(b2)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtvar(bi, p);
		nn = *(int *)BUNtloc(bi2, p);
		if (!strNil(x) &&
			(msg = (*func)(&y, &x, &nn, s2)) != MAL_SUCCEED)
			goto bailout;
		if (y == NULL)
			y = (str) str_nil;
		if (bunfastappVAR(bn, y) != GDK_SUCCEED) {
			if (y != str_nil)
				GDKfree(y);
			goto bailout;
		}
		if (y == str_nil) {
			bn->tnonil = false;
			bn->tnil = true;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l', a constant int 'n' and a BAT of strings 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_batstr_str(bat *ret, const bat *l, const int *n, const bat *l2, const char *name, str (*func)(str *, const str *, const int *, const str *))
{
	BATiter bi, bi2;
	BAT *bn, *b, *b2;
	BUN p, q;
	str x, x2, y;
	str msg = MAL_SUCCEED;

	prepareOperand2(b, l, b2, l2, name);
	if(BATcount(b) != BATcount(b2)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtvar(bi, p);
		x2 = (str) BUNtvar(bi2, p);
		if (!strNil(x) &&
			!strNil(x2) &&
			(msg = (*func)(&y, &x, n, &x2)) != MAL_SUCCEED)
			goto bailout;
		if (y == NULL)
			y = (str) str_nil;
		if (bunfastappVAR(bn, y) != GDK_SUCCEED) {
			if (y != str_nil)
				GDKfree(y);
			goto bailout;
		}
		if (y == str_nil) {
			bn->tnonil = false;
			bn->tnil = true;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l', a BAT of int 'n' and a BAT of strings 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_batstr_str(bat *ret, const bat *l, const bat *n, const bat *l2, const char *name, str (*func)(str *, const str *, const int *, const str *))
{
	BATiter bi, bi2, bi3;
	BAT *bn, *b, *b2, *b3;
	BUN p, q;
	int nn;
	str x, x2, y;
	str msg = MAL_SUCCEED;


	prepareOperand3(b, l, b2, n, b3, l2, name);
	if (BATcount(b) != BATcount(b2) || BATcount(b) != BATcount(b3)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		BBPunfix(b3->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	bn = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		BBPunfix(b3->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bn->tsorted=false;
	bn->trevsorted=false;

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);
	bi3 = bat_iterator(b3);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtvar(bi, p);
		nn = *(int *)BUNtloc(bi2, p);
		x2 = (str) BUNtvar(bi3, p);
		if (!strNil(x) &&
			!strNil(x2) &&
			(msg = (*func)(&y, &x, &nn, &x2)) != MAL_SUCCEED)
			goto bailout;
		if (y == NULL)
			y = (str) str_nil;
		if (bunfastappVAR(bn, y) != GDK_SUCCEED) {
			if (y != str_nil)
				GDKfree(y);
			goto bailout;
		}
		if (y == str_nil) {
			bn->tnonil = false;
			bn->tnil = true;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(b3->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
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
	return do_batstr_conststr_str(ret, l, s2, "batstr.Strip", STRStrip2);
}

static str
STRbatLtrim2_const(bat *ret, const bat *l, const str *s2)
{
	return do_batstr_conststr_str(ret, l, s2, "batstr.Ltrim", STRLtrim2);
}

static str
STRbatRtrim2_const(bat *ret, const bat *l, const str *s2)
{
	return do_batstr_conststr_str(ret, l, s2, "batstr.Rtrim", STRRtrim2);
}

static str
STRbatStrip2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.Strip", STRStrip2);
}

static str
STRbatLtrim2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.Ltrim", STRLtrim2);
}

static str
STRbatRtrim2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.Rtrim", STRRtrim2);
}

static str
STRbatLpad_const(bat *ret, const bat *l, const int *n)
{
	return do_batstr_constint_str(ret, l, n, "batstr.Lpad", STRLpad);
}

static str
STRbatRpad_const(bat *ret, const bat *l, const int *n)
{
	return do_batstr_constint_str(ret, l, n, "batstr.Rpad", STRRpad);
}

static str
STRbatLpad_bat(bat *ret, const bat *l, const bat *n)
{
	return do_batstr_batint_str(ret, l, n, "batstr.Lpad", STRLpad);
}

static str
STRbatRpad_bat(bat *ret, const bat *l, const bat *n)
{
	return do_batstr_batint_str(ret, l, n, "batstr.Rpad", STRRpad);
}

static str
STRbatLpad2_const_const(bat *ret, const bat *l, const int *n, const str *s2)
{
	return do_batstr_constint_conststr_str(ret, l, n, s2, "batstr.Lpad", STRLpad2);
}

static str
STRbatRpad2_const_const(bat *ret, const bat *l, const int *n, const str *s2)
{
	return do_batstr_constint_conststr_str(ret, l, n, s2, "batstr.Rpad", STRRpad2);
}

static str
STRbatLpad2_bat_const(bat *ret, const bat *l, const bat *n, const str *s2)
{
	return do_batstr_batint_conststr_str(ret, l, n, s2, "batstr.Lpad", STRLpad2);
}

static str
STRbatRpad2_bat_const(bat *ret, const bat *l, const bat *n, const str *s2)
{
	return do_batstr_batint_conststr_str(ret, l, n, s2, "batstr.Rpad", STRRpad2);
}

static str
STRbatLpad2_const_bat(bat *ret, const bat *l, const int *n, const bat *l2)
{
	return do_batstr_constint_batstr_str(ret, l, n, l2, "batstr.Lpad", STRLpad2);
}

static str
STRbatRpad2_const_bat(bat *ret, const bat *l, const int *n, const bat *l2)
{
	return do_batstr_constint_batstr_str(ret, l, n, l2, "batstr.Rpad", STRRpad2);
}

static str
STRbatLpad2_bat_bat(bat *ret, const bat *l, const bat *n, const bat *l2)
{
	return do_batstr_batint_batstr_str(ret, l, n, l2, "batstr.Lpad", STRLpad2);
}

static str
STRbatRpad2_bat_bat(bat *ret, const bat *l, const bat *n, const bat *l2)
{
	return do_batstr_batint_batstr_str(ret, l, n, l2, "batstr.Rpad", STRRpad2);
}

/*
 * A general assumption in all cases is the bats are synchronized on their
 * head column. This is not checked and may be mis-used to deploy the
 * implementation for shifted window arithmetic as well.
 */

static str
prefix_or_suffix(bat *res, const bat *l, const bat *r, const char *name, bit (*func)(const char *, const char *))
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	bit *restrict vals, next;
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

		next = func(x, y);
		vals[p] = next;
		nils |= is_bit_nil(next);
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
prefix_or_suffix_cst(bat *res, const bat *l, const char *y, const char *name, bit (*func)(const char *, const char *))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL;
	BUN p, q;
	bit *restrict vals, next;
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

		next = func(x, y);
		vals[p] = next;
		nils |= is_bit_nil(next);
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
	int *restrict vals, next;
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

		next = str_search(x, y);
		vals[p] = next;
		nils |= is_int_nil(next);
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
	int *restrict vals, next;
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

		next = str_search(x, y);
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
	int *restrict vals, next;
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

		next = str_reverse_str_search(x, y);
		vals[p] = next;
		nils |= is_int_nil(next);
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
	int *restrict vals, next;
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

		next = str_reverse_str_search(x, y);
		vals[p] = next;
		nils |= is_int_nil(next);
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
	int *restrict righti, *restrict vals, next;
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

		if ((msg = str_wchr_at(&next, x, righti[p])) != MAL_SUCCEED)
			goto bailout;
		vals[p] = next;
		nils |= is_int_nil(next);
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
	int c = *cst, *restrict vals, next;
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

		if ((msg = str_wchr_at(&next, x, c)) != MAL_SUCCEED)
			goto bailout;
		vals[p] = next;
		nils |= is_int_nil(next);
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
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
do_batstr_str_int_cst(bat *res, const bat *l, const int *cst, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int c = *cst;
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

		if ((msg = (*func)(&buf, &buflen, x, c)) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
STRbatrepeatcst(bat *ret, const bat *l, const int *cst)
{
	return do_batstr_str_int_cst(ret, l, cst, "batstr.repeat", str_repeat);
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
do_batstr_str_int(bat *res, const bat *l, const bat *r, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict righti;
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

		if ((msg = (*func)(&buf, &buflen, x, righti[p])) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
STRbatrepeat(bat *ret, const bat *l, const bat *r)
{
	return do_batstr_str_int(ret, l, r, "batstr.repeat", str_repeat);
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
STRbatSubstitutecst(bat *res, const bat *bid, const str *arg2, const str *arg3, const bit *rep)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL;
	BUN p, q;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str x, y = *arg2, z = *arg3, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	bit r = *rep;

	if (!buf) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
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

		if ((msg = str_substitute(&buf, &buflen, x, y, z, r)) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	bit *restrict arg4i;

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

		if ((msg = str_substitute(&buf, &buflen, x, y, z, arg4i[p])) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	int f = *field;
	str x, y = *needle, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
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

		if ((msg = str_splitpart(&buf, &buflen, x, y, f)) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	int *restrict arg3i;
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

		if ((msg = str_splitpart(&buf, &buflen, x, y, arg3i[p])) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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

		if ((msg = str_substitute(&buf, &buflen, x, y, z, TRUE)) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	int *starti, *ncharsi;
	str x, y, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
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
		y = (str) BUNtail(righti, p);

		if ((msg = str_insert(&buf, &buflen, x, starti[p], ncharsi[p], y)) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	int strt = *start, l = *nchars;
	str x, y = *input2, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
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

		if ((msg = str_insert(&buf, &buflen, x, strt, l, y)) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	int s = *start, len = *length;
	str x, buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;

	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
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

		if ((msg = str_sub_string(&buf, &buflen, x, s, len)) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	int *starti, *lengthi;
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

		if ((msg = str_sub_string(&buf, &buflen, x, starti[p], lengthi[p])) != MAL_SUCCEED)
			goto bailout;
		if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
			msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		nils |= strNil(buf);
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
	int *restrict vals, next;
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

		next = str_locate2(x, y, 1);
		vals[p] = next;
		nils |= is_int_nil(next);
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
	int *restrict vals, next;
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

		next = str_locate2(x, y, 1);
		vals[p] = next;
		nils |= is_int_nil(next);
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
	int *restrict vals, next, s = *start;
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

		next = str_locate2(x, y, s);
		vals[p] = next;
		nils |= is_int_nil(next);
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
	int *restrict vals, next, *restrict starti;
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

		next = str_locate2(x, y, starti[p]);
		vals[p] = next;
		nils |= is_int_nil(next);
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
 command("batstr", "search", STRbatstrSearch, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),batarg("c",str))),
 command("batstr", "search", STRbatstrSearchcst, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),arg("c",str))),
 command("batstr", "r_search", STRbatRstrSearch, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),batarg("c",str))),
 command("batstr", "r_search", STRbatRstrSearchcst, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),arg("c",str))),
 command("batstr", "string", STRbatTail, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,3, batarg("",str),batarg("b",str),batarg("offset",int))),
 command("batstr", "string", STRbatTailcst, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,3, batarg("",str),batarg("b",str),arg("offset",int))),
 command("batstr", "ascii", STRbatAscii, false, "Return unicode of head of string", args(1,2, batarg("",int),batarg("s",str))),
 command("batstr", "substring", STRbatsubstringTail, false, "Extract the tail of a string", args(1,3, batarg("",str),batarg("s",str),batarg("start",int))),
 command("batstr", "substring", STRbatsubstringTailcst, false, "Extract the tail of a string", args(1,3, batarg("",str),batarg("s",str),arg("start",int))),
 command("batstr", "substring", STRbatsubstring, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),batarg("start",int),batarg("index",int))),
 command("batstr", "substring", STRbatsubstringcst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),arg("start",int),arg("index",int))),
 command("batstr", "unicode", STRbatFromWChr, false, "convert a unicode to a character.", args(1,2, batarg("",str),batarg("wchar",int))),
 command("batstr", "unicodeAt", STRbatWChrAt, false, "get a unicode character (as an int) from a string position.", args(1,3, batarg("",int),batarg("s",str),batarg("index",int))),
 command("batstr", "unicodeAt", STRbatWChrAtcst, false, "get a unicode character (as an int) from a string position.", args(1,3, batarg("",int),batarg("s",str),arg("index",int))),
 command("batstr", "substitute", STRbatSubstitute, false, "Substitute first occurrence of 'src' by\n'dst'. Iff repeated = true this is\nrepeated while 'src' can be found in the\nresult string. In order to prevent\nrecursion and result strings of unlimited\nsize, repeating is only done iff src is\nnot a substring of dst.", args(1,5, batarg("",str),batarg("s",str),batarg("src",str),batarg("dst",str),batarg("rep",bit))),
 command("batstr", "substitute", STRbatSubstitutecst, false, "Substitute first occurrence of 'src' by\n'dst'. Iff repeated = true this is\nrepeated while 'src' can be found in the\nresult string. In order to prevent\nrecursion and result strings of unlimited\nsize, repeating is only done iff src is\nnot a substring of dst.", args(1,5, batarg("",str),batarg("s",str),arg("src",str),arg("dst",str),arg("rep",bit))),
 command("batstr", "stringleft", STRbatprefix, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("l",int))),
 command("batstr", "stringleft", STRbatprefixcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("l",int))),
 command("batstr", "stringright", STRbatsuffix, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("l",int))),
 command("batstr", "stringright", STRbatsuffixcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("l",int))),
 command("batstr", "locate", STRbatstrLocate, false, "Locate the start position of a string", args(1,3, batarg("",int),batarg("s1",str),batarg("s2",str))),
 command("batstr", "locate", STRbatstrLocatecst, false, "Locate the start position of a string", args(1,3, batarg("",int),batarg("s1",str),arg("s2",str))),
 command("batstr", "locate", STRbatstrLocate2, false, "Locate the start position of a string", args(1,4, batarg("",int),batarg("s1",str),batarg("s2",str),batarg("start",int))),
 command("batstr", "locate", STRbatstrLocate2cst, false, "Locate the start position of a string", args(1,4, batarg("",int),batarg("s1",str),arg("s2",str),arg("start",int))),
 command("batstr", "insert", STRbatInsert, false, "Insert a string into another", args(1,5, batarg("",str),batarg("s",str),batarg("start",int),batarg("l",int),batarg("s2",str))),
 command("batstr", "insert", STRbatInsertcst, false, "Insert a string into another", args(1,5, batarg("",str),batarg("s",str),arg("start",int),arg("l",int),arg("s2",str))),
 command("batstr", "replace", STRbatReplace, false, "Insert a string into another", args(1,4, batarg("",str),batarg("s",str),batarg("pat",str),batarg("s2",str))),
 command("batstr", "replace", STRbatReplacecst, false, "Insert a string into another", args(1,4, batarg("",str),batarg("s",str),arg("pat",str),arg("s2",str))),
 command("batstr", "repeat", STRbatrepeat, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("c",int))),
 command("batstr", "repeat", STRbatrepeatcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("c",int))),
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
