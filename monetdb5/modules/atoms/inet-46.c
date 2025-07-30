/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

static str
INETstr2inet4(inet4 *ret, const char *const *s)
{
	size_t l = sizeof(inet4);

	if (BATatoms[TYPE_inet4].atomFromStr(*s, &l, (void **) &ret, false) > 0) {
		return MAL_SUCCEED;
	}
	throw(MAL, "inet46.inet4", "Not an IPv4 address");
}

static str
INETinet42str(char **ret, const inet4 *val)
{
	size_t l = 0;
	*ret = NULL;
	if (BATatoms[TYPE_inet4].atomToStr(ret, &l, val, false) < 0)
		throw(MAL, "inet46.str", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
INETinet42inet4(inet4 *ret, const inet4 *val)
{
	*ret = *val;
	return MAL_SUCCEED;
}

static str
INETstr2inet4_bulk(bat *ret, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	str msg = NULL;
	inet4 *restrict vals;
	struct canditer ci;
	oid off;
	bool nils = false, btkey = false;
	size_t l = sizeof(inet4);
	ssize_t (*conv)(const char *, size_t *, void **, bool) = BATatoms[TYPE_inet4].atomFromStr;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.inet4",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.inet4",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_inet4, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.inet4",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			const char *v = BUNtvar(bi, p);
			inet4 *up = &vals[i], **pp = &up;

			if (conv(v, &l, (void **) pp, false) <= 0) {
				msg = createException(SQL, "batcalc.inet4",
									  SQLSTATE(42000) "Not an IPv4 address");
				goto bailout1;
			}
			nils |= strNil(v);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			const char *v = BUNtvar(bi, p);
			inet4 *up = &vals[i], **pp = &up;

			if (conv(v, &l, (void **) pp, false) <= 0) {
				msg = createException(SQL, "batcalc.inet4",
									  SQLSTATE(42000) "Not an IPv4 address");
				goto bailout1;
			}
			nils |= strNil(v);
		}
	}
	btkey = bi.key;
  bailout1:
	bat_iterator_end(&bi);

  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*ret = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

static str
INETinet42inet4_bulk(bat *ret, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	inet4 *restrict bv, *restrict dv;
	str msg = NULL;
	struct canditer ci;
	oid off;
	bool nils = false, btsorted = false, btrevsorted = false, btkey = false;
	BATiter bi;

	if (sid && !is_bat_nil(*sid)) {
		if ((s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.inet4",
								  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	} else {
		BBPretain(*ret = *bid);	/* nothing to convert, return */
		return MAL_SUCCEED;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.inet4",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_inet4, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.inet4",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	bv = bi.base;
	dv = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			inet4 v = bv[p];

			dv[i] = v;
			nils |= is_inet4_nil(v);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			inet4 v = bv[p];

			dv[i] = v;
			nils |= is_inet4_nil(v);
		}
	}
	btkey = bi.key;
	btsorted = bi.sorted;
	btrevsorted = bi.revsorted;
	bat_iterator_end(&bi);

  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	if (dst) {					/* implies msg==MAL_SUCCEED */
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = btsorted;
		dst->trevsorted = btrevsorted;
		*ret = dst->batCacheid;
		BBPkeepref(dst);
	}
	return msg;
}

static str
INETinet42str_bulk(bat *ret, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	str msg = NULL;
	inet4 *restrict vals;
	struct canditer ci;
	oid off;
	bool nils = false, btkey = false;
	char buf[16], *pbuf = buf;
	size_t l = sizeof(buf);
	ssize_t (*conv)(char **, size_t *, const void *, bool) = BATatoms[TYPE_inet4].atomToStr;
	BATiter bi;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.str",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.str",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_str, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.str",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = bi.base;
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			inet4 v = vals[p];

			if (conv(&pbuf, &l, &v, false) < 0) {	/* it should never be reallocated */
				msg = createException(MAL, "batcalc.str",
									  GDK_EXCEPTION);
				goto bailout1;
			}
			if (tfastins_nocheckVAR(dst, i, buf) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.str",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout1;
			}
			nils |= strNil(buf);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			inet4 v = vals[p];

			if (conv(&pbuf, &l, &v, false) < 0) {	/* it should never be reallocated */
				msg = createException(MAL, "batcalc.str",
									  GDK_EXCEPTION);
				goto bailout1;
			}
			if (tfastins_nocheckVAR(dst, i, buf) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.str",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout1;
			}
			nils |= strNil(buf);
		}
	}
	btkey = bi.key;
  bailout1:
	bat_iterator_end(&bi);

  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*ret = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

static str
INETstr2inet6(inet6 *ret, const char *const *s)
{
	size_t l = sizeof(inet6);

	if (BATatoms[TYPE_inet6].atomFromStr(*s, &l, (void **) &ret, false) > 0) {
		return MAL_SUCCEED;
	}
	throw(MAL, "inet46.inet6", "Not an IPv6 address");
}

static str
INETinet62str(char **ret, const inet6 *val)
{
	size_t l = 0;
	*ret = NULL;
	if (BATatoms[TYPE_inet6].atomToStr(ret, &l, val, false) < 0)
		throw(MAL, "inet46.str", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
INETinet42inet6(inet6 *ret, const inet4 *s)
{
	if (is_inet4_nil(*s))
		*ret = inet6_nil;
	else
		*ret = (inet6) {
			.oct[5] = 0xffff,
			.oct[6] = (s->quad[0] << 8) | s->quad[1],
			.oct[7] = (s->quad[2] << 8) | s->quad[3],
		};
	return MAL_SUCCEED;
}

static str
INETinet42inet6_bulk(bat *ret, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	inet4 *restrict bv;
	inet6 *restrict dv;
	str msg = NULL;
	struct canditer ci;
	oid off;
	bool nils = false;
	BATiter bi;

	if (sid && !is_bat_nil(*sid)) {
		if ((s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.inet6",
								  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	} else {
		BBPretain(*ret = *bid);	/* nothing to convert, return */
		return MAL_SUCCEED;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.inet6",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_inet6, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.inet6",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	bv = bi.base;
	dv = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			if (is_inet4_nil(bv[p])) {
				dv[i] = inet6_nil;
				nils = true;
			} else {
				dv[i] = (inet6) {
					.oct[5] = 0xffff,
					.oct[6] = (bv[p].quad[0] << 8) | bv[p].quad[1],
					.oct[7] = (bv[p].quad[1] << 8) | bv[p].quad[3],
				};
			}
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			if (is_inet4_nil(bv[p])) {
				dv[i] = inet6_nil;
				nils = true;
			} else {
				dv[i] = (inet6) {
					.oct[5] = 0xffff,
					.oct[6] = (bv[p].quad[0] << 8) | bv[p].quad[1],
					.oct[7] = (bv[p].quad[1] << 8) | bv[p].quad[3],
				};
			}
		}
	}
	BATsetcount(dst, ci.ncand);
	dst->tnil = nils;
	dst->tnonil = !nils;
	dst->tkey = bi.key;
	dst->tsorted = bi.sorted;
	dst->trevsorted = bi.revsorted;
	*ret = dst->batCacheid;
	BBPkeepref(dst);
	bat_iterator_end(&bi);

  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	return msg;
}

static str
INETinet62inet6(inet6 *ret, const inet6 *val)
{
	*ret = *val;
	return MAL_SUCCEED;
}

static str
INETstr2inet6_bulk(bat *ret, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	str msg = NULL;
	inet6 *restrict vals;
	struct canditer ci;
	oid off;
	bool nils = false, btkey = false;
	size_t l = sizeof(inet6);
	ssize_t (*conv)(const char *, size_t *, void **, bool) = BATatoms[TYPE_inet6].atomFromStr;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.inet6",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.inet6",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_inet6, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.inet6",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			const char *v = BUNtvar(bi, p);
			inet6 *up = &vals[i], **pp = &up;

			if (conv(v, &l, (void **) pp, false) <= 0) {
				msg = createException(SQL, "batcalc.inet6",
									  SQLSTATE(42000) "Not an IPv6 address");
				goto bailout1;
			}
			nils |= strNil(v);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			const char *v = BUNtvar(bi, p);
			inet6 *up = &vals[i], **pp = &up;

			if (conv(v, &l, (void **) pp, false) <= 0) {
				msg = createException(SQL, "batcalc.inet6",
									  SQLSTATE(42000) "Not an IPv6 address");
				goto bailout1;
			}
			nils |= strNil(v);
		}
	}
	btkey = bi.key;
  bailout1:
	bat_iterator_end(&bi);

  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*ret = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

static str
INETinet62inet6_bulk(bat *ret, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	inet6 *restrict bv, *restrict dv;
	str msg = NULL;
	struct canditer ci;
	oid off;
	bool nils = false, btsorted = false, btrevsorted = false, btkey = false;
	BATiter bi;

	if (sid && !is_bat_nil(*sid)) {
		if ((s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.inet6",
								  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	} else {
		BBPretain(*ret = *bid);	/* nothing to convert, return */
		return MAL_SUCCEED;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.inet6",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_inet6, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.inet6",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	bv = bi.base;
	dv = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			inet6 v = bv[p];

			dv[i] = v;
			nils |= is_inet6_nil(v);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			inet6 v = bv[p];

			dv[i] = v;
			nils |= is_inet6_nil(v);
		}
	}
	btkey = bi.key;
	btsorted = bi.sorted;
	btrevsorted = bi.revsorted;
	bat_iterator_end(&bi);

  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	if (dst) {					/* implies msg==MAL_SUCCEED */
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = btsorted;
		dst->trevsorted = btrevsorted;
		*ret = dst->batCacheid;
		BBPkeepref(dst);
	}
	return msg;
}

static str
INETinet62str_bulk(bat *ret, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	str msg = NULL;
	inet6 *restrict vals;
	struct canditer ci;
	oid off;
	bool nils = false, btkey = false;
	char buf[16], *pbuf = buf;
	size_t l = sizeof(buf);
	ssize_t (*conv)(char **, size_t *, const void *, bool) = BATatoms[TYPE_inet6].atomToStr;
	BATiter bi;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.str",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.str",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_str, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.str",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = bi.base;
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			inet6 v = vals[p];

			if (conv(&pbuf, &l, &v, false) < 0) {	/* it should never be reallocated */
				msg = createException(MAL, "batcalc.str",
									  GDK_EXCEPTION);
				goto bailout1;
			}
			if (tfastins_nocheckVAR(dst, i, buf) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.str",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout1;
			}
			nils |= strNil(buf);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			inet6 v = vals[p];

			if (conv(&pbuf, &l, &v, false) < 0) {	/* it should never be reallocated */
				msg = createException(MAL, "batcalc.str",
									  GDK_EXCEPTION);
				goto bailout1;
			}
			if (tfastins_nocheckVAR(dst, i, buf) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.str",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout1;
			}
			nils |= strNil(buf);
		}
	}
	btkey = bi.key;
  bailout1:
	bat_iterator_end(&bi);

  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*ret = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

#include "mel.h"
mel_func inet46_init_funcs[] = {
 command("calc", "inet4", INETinet42inet4, false, "", args(1,2, arg("",inet4),arg("u",inet4))),
 command("calc", "inet4", INETstr2inet4, false, "Coerce a string to a inet4, validating its format", args(1,2, arg("",inet4),arg("s",str))),
 command("batcalc", "inet4", INETinet42inet4_bulk, false, "", args(1,3, batarg("",inet4),batarg("u",inet4),batarg("c",oid))),
 command("batcalc", "inet4", INETstr2inet4_bulk, false, "Coerce a string to a inet4, validating its format", args(1,3, batarg("",inet4),batarg("s",str),batarg("c",oid))),
 command("calc", "inet6", INETinet62inet6, false, "", args(1,2, arg("",inet6),arg("u",inet6))),
 command("calc", "inet6", INETstr2inet6, false, "Coerce a string to a inet6, validating its format", args(1,2, arg("",inet6),arg("s",str))),
 command("calc", "inet6", INETinet42inet6, false, "Coerce a inet4 to a inet6", args(1,2, arg("",inet6),arg("s",inet4))),
 command("batcalc", "inet6", INETinet62inet6_bulk, false, "", args(1,3, batarg("",inet6),batarg("u",inet6),batarg("c",oid))),
 command("batcalc", "inet6", INETstr2inet6_bulk, false, "Coerce a string to a inet6, validating its format", args(1,3, batarg("",inet6),batarg("s",str),batarg("c",oid))),
 command("batcalc", "inet6", INETinet42inet6_bulk, false, "Coerce a inet4 to a inet6", args(1,3, batarg("",inet6),batarg("s",inet4),batarg("c",oid))),
 command("calc", "str", INETinet42str, false, "Coerce a inet4 to a string type", args(1,2, arg("",str),arg("s",inet4))),
 command("calc", "str", INETinet62str, false, "Coerce a inet6 to a string type", args(1,2, arg("",str),arg("s",inet6))),
 command("batcalc", "str", INETinet42str_bulk, false, "Coerce a inet4 to a string type", args(1,3, batarg("",str),batarg("s",inet4),batarg("c",oid))),
 command("batcalc", "str", INETinet62str_bulk, false, "Coerce a inet6 to a string type", args(1,3, batarg("",str),batarg("s",inet6),batarg("c",oid))),
 command("inet46", "inet4", INETstr2inet4, false, "Coerce a string to a inet4, validating its format", args(1,2, arg("",inet4),arg("s",str))),
 command("inet46", "inet6", INETstr2inet6, false, "Coerce a string to a inet6, validating its format", args(1,2, arg("",inet6),arg("s",str))),
 command("inet46", "str", INETinet42str, false, "Coerce a inet4 to its string type", args(1,2, arg("",str),arg("u",inet4))),
 command("inet46", "str", INETinet62str, false, "Coerce a inet6 to its string type", args(1,2, arg("",str),arg("u",inet6))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_inet46_mal)
{ mal_module("inet46", NULL, inet46_init_funcs); }
