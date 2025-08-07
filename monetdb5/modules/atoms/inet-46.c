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

static inline uint32_t
inet42int(const inet4 *ip)
{
	return (ip->quad[0] << 24) | (ip->quad[1] << 16) |
		(ip->quad[2] << 8) | ip->quad[3];
}

static bte
inet4containsinet4(const inet4 *ip1, const bte *msk1, const inet4 *ip2, const bte *msk2, bool strict, bool symmetric)
{
	if (is_inet4_nil(*ip1) || is_inet4_nil(*ip2)) {
		/* something is nil, so result is nil as well */
		return bte_nil;
	}
	bte m1 = msk1 ? *msk1 : 32;
	bte m2 = msk2 ? *msk2 : 32;
	if (is_bte_nil(m1) || is_bte_nil(m2)) {
		/* something is nil, so result is nil as well */
		return bte_nil;
	}
	if (m1 < 0 || m1 > 32 || m2 < 0 || m2 > 32)
		return -1;				/* indicate error */
	if (symmetric && m1 < m2) {
		/* switch the arguments, so more specific is "1" */
		m2 = m1;
		/* old value of m2 is not needed anymore */
		const inet4 *ip = ip1;
		ip1 = ip2;
		ip2 = ip;
	} else if (m1 < m2 || (strict && m1 == m2)) {
		/* a less specific address (fewer bits used, i.e. smaller mask
		 * value) cannot be contained in a more specific one */
		return 0;
	}
	uint32_t netmask2 = m2 == 0 ? 0 :
		m2 >= 32 ? ~UINT32_C(0) : ~((UINT32_C(1) << (32 - m2)) - 1);
	return (inet42int(ip1) & netmask2) == (inet42int(ip2) & netmask2);
}

static str
INETinet4containsinet4(bit *ret, const inet4 *ip1, const bte *msk1, const inet4 *ip2, const bte *msk2)
{
	bte r = inet4containsinet4(ip1, msk1, ip2, msk2, true, false);
	if (r == -1)
		throw(MAL, "inet46.inet4contains", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
	return MAL_SUCCEED;
}

static str
INETinet4containsinet4nomask(bit *ret, const inet4 *ip1, const inet4 *ip2, const bte *msk2)
{
	bte r = inet4containsinet4(ip1, &(bte){32}, ip2, msk2, true, false);
	if (r == -1)
		throw(MAL, "inet46.inet4contains", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
	return MAL_SUCCEED;
}

static str
INETinet4containsorequalinet4(bit *ret, const inet4 *ip1, const bte *msk1, const inet4 *ip2, const bte *msk2)
{
	bte r = inet4containsinet4(ip1, msk1, ip2, msk2, false, false);
	if (r == -1)
		throw(MAL, "inet46.inet4containsorequal", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
	return MAL_SUCCEED;
}

static str
INETinet4containsorequalinet4nomask(bit *ret, const inet4 *ip1, const inet4 *ip2, const bte *msk2)
{
	bte r = inet4containsinet4(ip1, &(bte){32}, ip2, msk2, false, false);
	if (r == -1)
		throw(MAL, "inet46.inet4containsorequal", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
	return MAL_SUCCEED;
}

static str
INETinet4containssymmetricinet4(bit *ret, const inet4 *ip1, const bte *msk1, const inet4 *ip2, const bte *msk2)
{
	bte r = inet4containsinet4(ip1, msk1, ip2, msk2, false, true);
	if (r == -1)
		throw(MAL, "inet46.inet4containssymmetric", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
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
inet4containsinet4_bulk(bat *ret, const bat *bip1, const bat *bmsk1, const bat *sid1,
						const bat *bip2, const bat *bmsk2, const bat *sid2,
						bool strict, bool symmetric)
{
	BAT *ip1 = NULL, *msk1 = NULL, *ip2 = NULL, *msk2 = NULL, *s1 = NULL, *s2 = NULL, *dst = NULL;
	struct canditer c1, c2;
	str msg = MAL_SUCCEED;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	if ((ip1 = BATdescriptor(*bip1)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((symmetric || (bmsk1 && !is_bat_nil(*bmsk1))) &&
		(msk2 = BATdescriptor(*bmsk1)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (msk1 && (ip1->batCount != msk1->batCount || ip1->hseqbase != msk1->hseqbase)) {
		msg = createException(MAL, "batinet46.contains", "Alignment error");
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && (s1 = BATdescriptor(*sid1)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((ip2 = BATdescriptor(*bip2)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((msk2 = BATdescriptor(*bmsk2)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (ip2->batCount != msk2->batCount || ip2->hseqbase != msk2->hseqbase) {
		msg = createException(MAL, "batinet46.contains", "Alignment error");
		goto bailout;
	}
	if (sid2 && !is_bat_nil(*sid2) && (s2 = BATdescriptor(*sid2)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}

	canditer_init(&c1, ip1, s1);
	canditer_init(&c2, ip2, s2);

	if (c1.ncand != c2.ncand) {
		msg = createException(MAL, "batinet46.contains", "Alignment error");
		goto bailout;
	}

	if ((dst = COLnew(c1.hseq, TYPE_bit, c1.ncand, TRANSIENT)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	BATiter ip1i = bat_iterator(ip1);
	BATiter ip2i = bat_iterator(ip2);
	BATiter msk2i = bat_iterator(msk2);
	bit *dstp = Tloc(dst, 0);
	bool nils = false;
	if (msk1) {
		BATiter msk1i = bat_iterator(msk1);
		TIMEOUT_LOOP(c1.ncand, qry_ctx) {
			oid o1 = canditer_next(&c1);
			oid o2 = canditer_next(&c2);
			bte v = inet4containsinet4(BUNtloc(ip1i, o1 - ip1->hseqbase),
									   BUNtloc(msk1i, o1 - msk1->hseqbase),
									   BUNtloc(ip2i, o2 - ip2->hseqbase),
									   BUNtloc(msk2i, o2 - msk2->hseqbase),
									   strict, symmetric);
			if (v == -1) {
				msg = createException(MAL, "batinet46.contains",
									  SQLSTATE(22003) "mask value out of range");
				goto bailout;
			}
			*dstp++ = (bit) v;
			nils |= is_bte_nil(v);
		}
		bat_iterator_end(&msk1i);
	} else {
		assert(!symmetric);
		TIMEOUT_LOOP(c1.ncand, qry_ctx) {
			oid o1 = canditer_next(&c1);
			oid o2 = canditer_next(&c2);
			bte v = inet4containsinet4(BUNtloc(ip1i, o1 - ip1->hseqbase),
									   &(bte){32},
									   BUNtloc(ip2i, o2 - ip2->hseqbase),
									   BUNtloc(msk2i, o2 - msk2->hseqbase),
									   strict, false);
			if (v == -1) {
				msg = createException(MAL, "batinet46.contains",
									  SQLSTATE(22003) "mask value out of range");
				goto bailout;
			}
			*dstp++ = (bit) v;
			nils |= is_bte_nil(v);
		}
	}
	bat_iterator_end(&ip1i);
	bat_iterator_end(&ip2i);
	bat_iterator_end(&msk2i);
	TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
	dst->tnil = nils;
	dst->tnonil = !nils;
	dst->tsorted = false;
	dst->trevsorted = false;
	dst->tkey = false;
	BATsetcount(dst, c1.ncand);
	*ret = dst->batCacheid;
	BBPkeepref(dst);

  bailout:
	BBPreclaim(ip1);
	BBPreclaim(ip2);
	BBPreclaim(msk1);
	BBPreclaim(msk2);
	BBPreclaim(s1);
	BBPreclaim(s2);
	if (msg)					/* only reclaim if there was an error */
		BBPreclaim(dst);
	return msg;
}

static str
INETinet4containsinet4_bulk(bat *ret, const bat *bip1, const bat *bmsk1, const bat *sid1,
							const bat *bip2, const bat *bmsk2, const bat *sid2)
{
	return inet4containsinet4_bulk(ret, bip1, bmsk1, sid1, bip2, bmsk2, sid2,
								   true, false);
}

static str
INETinet4containsorequalinet4_bulk(bat *ret, const bat *bip1, const bat *bmsk1, const bat *sid1,
							const bat *bip2, const bat *bmsk2, const bat *sid2)
{
	return inet4containsinet4_bulk(ret, bip1, bmsk1, sid1, bip2, bmsk2, sid2,
								   false, false);
}

static str
INETinet4containssymmetricinet4_bulk(bat *ret, const bat *bip1, const bat *bmsk1, const bat *sid1,
							const bat *bip2, const bat *bmsk2, const bat *sid2)
{
	return inet4containsinet4_bulk(ret, bip1, bmsk1, sid1, bip2, bmsk2, sid2,
								   false, true);
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

static bte
inet6containsinet6(const inet6 *ip1, const sht *msk1, const inet6 *ip2, const sht *msk2, bool strict, bool symmetric)
{
	if (is_inet6_nil(*ip1) || is_inet6_nil(*ip2)) {
		/* something is nil, so result is nil as well */
		return bte_nil;
	}
	sht m1 = msk1 ? *msk1 : 128;
	sht m2 = msk2 ? *msk2 : 128;
	if (is_sht_nil(m1) || is_sht_nil(m2)) {
		/* something is nil, so result is nil as well */
		return bte_nil;
	}
	if (m1 < 0 || m1 > 128 || m2 < 0 || m2 > 128)
		return -1;				/* indicate error */
	if (symmetric && m1 < m2) {
		/* switch the arguments, so more specific is "1" */
		m2 = m1;
		/* old value of m2 is not needed anymore */
		const inet6 *ip = ip1;
		ip1 = ip2;
		ip2 = ip;
	} else if (m1 < m2 || (strict && m1 == m2)) {
		/* a less specific address (fewer bits used, i.e. smaller mask
		 * value) cannot be contained in a more specific one */
		return 0;
	}
	sht n = m2 / 16;
	sht r = m2 % 16;
	for (sht i = 0; i < n; i++) {
		if (ip1->oct[i] != ip2->oct[i])
			return 0;
	}
	if (r == 0)
		return 1;
	uint16_t m = (UINT16_C(1) << (16 - r)) - 1;
	return (ip1->oct[n] & m) == (ip2->oct[n] & m);
}

static str
INETinet6containsinet6(bit *ret, const inet6 *ip1, const sht *msk1, const inet6 *ip2, const sht *msk2)
{
	bte r = inet6containsinet6(ip1, msk1, ip2, msk2, true, false);
	if (r == -1)
		throw(MAL, "inet46.inet6contains", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
	return MAL_SUCCEED;
}

static str
INETinet6containsinet6nomask(bit *ret, const inet6 *ip1, const inet6 *ip2, const sht *msk2)
{
	bte r = inet6containsinet6(ip1, &(sht){128}, ip2, msk2, true, false);
	if (r == -1)
		throw(MAL, "inet46.inet6contains", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
	return MAL_SUCCEED;
}

static str
INETinet6containsorequalinet6(bit *ret, const inet6 *ip1, const sht *msk1, const inet6 *ip2, const sht *msk2)
{
	bte r = inet6containsinet6(ip1, msk1, ip2, msk2, false, false);
	if (r == -1)
		throw(MAL, "inet46.inet6containsorequal", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
	return MAL_SUCCEED;
}

static str
INETinet6containsorequalinet6nomask(bit *ret, const inet6 *ip1, const inet6 *ip2, const sht *msk2)
{
	bte r = inet6containsinet6(ip1, &(sht){128}, ip2, msk2, false, false);
	if (r == -1)
		throw(MAL, "inet46.inet6containsorequal", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
	return MAL_SUCCEED;
}

static str
INETinet6containssymmetricinet6(bit *ret, const inet6 *ip1, const sht *msk1, const inet6 *ip2, const sht *msk2)
{
	bte r = inet6containsinet6(ip1, msk1, ip2, msk2, false, true);
	if (r == -1)
		throw(MAL, "inet46.inet6containssymmetric", "Network mask value out of range.\n");
	assert(is_bit_nil(r) || r == 0 || r == 1);
	*ret = (bit) r;
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

static str
inet6containsinet6_bulk(bat *ret, const bat *bip1, const bat *bmsk1, const bat *sid1,
						const bat *bip2, const bat *bmsk2, const bat *sid2,
						bool strict, bool symmetric)
{
	BAT *ip1 = NULL, *msk1 = NULL, *ip2 = NULL, *msk2 = NULL, *s1 = NULL, *s2 = NULL, *dst = NULL;
	struct canditer c1, c2;
	str msg = MAL_SUCCEED;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	if ((ip1 = BATdescriptor(*bip1)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((symmetric || (bmsk1 && !is_bat_nil(*bmsk1))) &&
		(msk2 = BATdescriptor(*bmsk1)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (msk1 && (ip1->batCount != msk1->batCount || ip1->hseqbase != msk1->hseqbase)) {
		msg = createException(MAL, "batinet46.contains", "Alignment error");
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && (s1 = BATdescriptor(*sid1)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((ip2 = BATdescriptor(*bip2)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((msk2 = BATdescriptor(*bmsk2)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (ip2->batCount != msk2->batCount || ip2->hseqbase != msk2->hseqbase) {
		msg = createException(MAL, "batinet46.contains", "Alignment error");
		goto bailout;
	}
	if (sid2 && !is_bat_nil(*sid2) && (s2 = BATdescriptor(*sid2)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}

	canditer_init(&c1, ip1, s1);
	canditer_init(&c2, ip2, s2);

	if (c1.ncand != c2.ncand) {
		msg = createException(MAL, "batinet46.contains", "Alignment error");
		goto bailout;
	}

	if ((dst = COLnew(c1.hseq, TYPE_bit, c1.ncand, TRANSIENT)) == NULL) {
		msg = createException(MAL, "batinet46.contains",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	BATiter ip1i = bat_iterator(ip1);
	BATiter ip2i = bat_iterator(ip2);
	BATiter msk2i = bat_iterator(msk2);
	bit *dstp = Tloc(dst, 0);
	bool nils = false;
	if (msk1) {
		BATiter msk1i = bat_iterator(msk1);
		TIMEOUT_LOOP(c1.ncand, qry_ctx) {
			oid o1 = canditer_next(&c1);
			oid o2 = canditer_next(&c2);
			bte v = inet6containsinet6(BUNtloc(ip1i, o1 - ip1->hseqbase),
									   BUNtloc(msk1i, o1 - msk1->hseqbase),
									   BUNtloc(ip2i, o2 - ip2->hseqbase),
									   BUNtloc(msk2i, o2 - msk2->hseqbase),
									   strict, symmetric);
			if (v == -1) {
				msg = createException(MAL, "batinet46.contains",
									  SQLSTATE(22003) "mask value out of range");
				goto bailout;
			}
			*dstp++ = (bit) v;
			nils |= is_bte_nil(v);
		}
		bat_iterator_end(&msk1i);
	} else {
		assert(!symmetric);
		TIMEOUT_LOOP(c1.ncand, qry_ctx) {
			oid o1 = canditer_next(&c1);
			oid o2 = canditer_next(&c2);
			bte v = inet6containsinet6(BUNtloc(ip1i, o1 - ip1->hseqbase),
									   &(sht){128},
									   BUNtloc(ip2i, o2 - ip2->hseqbase),
									   BUNtloc(msk2i, o2 - msk2->hseqbase),
									   strict, false);
			if (v == -1) {
				msg = createException(MAL, "batinet46.contains",
									  SQLSTATE(22003) "mask value out of range");
				goto bailout;
			}
			*dstp++ = (bit) v;
			nils |= is_bte_nil(v);
		}
	}
	bat_iterator_end(&ip1i);
	bat_iterator_end(&ip2i);
	bat_iterator_end(&msk2i);
	TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
	dst->tnil = nils;
	dst->tnonil = !nils;
	dst->tsorted = false;
	dst->trevsorted = false;
	dst->tkey = false;
	BATsetcount(dst, c1.ncand);
	*ret = dst->batCacheid;
	BBPkeepref(dst);

  bailout:
	BBPreclaim(ip1);
	BBPreclaim(ip2);
	BBPreclaim(msk1);
	BBPreclaim(msk2);
	BBPreclaim(s1);
	BBPreclaim(s2);
	if (msg)					/* only reclaim if there was an error */
		BBPreclaim(dst);
	return msg;
}

static str
INETinet6containsinet6_bulk(bat *ret, const bat *bip1, const bat *bmsk1, const bat *sid1,
							const bat *bip2, const bat *bmsk2, const bat *sid2)
{
	return inet6containsinet6_bulk(ret, bip1, bmsk1, sid1, bip2, bmsk2, sid2,
								   true, false);
}

static str
INETinet6containsorequalinet6_bulk(bat *ret, const bat *bip1, const bat *bmsk1, const bat *sid1,
								   const bat *bip2, const bat *bmsk2, const bat *sid2)
{
	return inet6containsinet6_bulk(ret, bip1, bmsk1, sid1, bip2, bmsk2, sid2,
								   false, false);
}

static str
INETinet6containssymmetricinet6_bulk(bat *ret, const bat *bip1, const bat *bmsk1, const bat *sid1,
								   const bat *bip2, const bat *bmsk2, const bat *sid2)
{
	return inet6containsinet6_bulk(ret, bip1, bmsk1, sid1, bip2, bmsk2, sid2,
								   false, true);
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
 command("inet46", "inet4containsinet4", INETinet4containsinet4, false, "", args(1,5, arg("",bit),arg("ip1",inet4),arg("msk1",bte),arg("ip2",inet4),arg("msk2",bte))),
 command("inet46", "inet4containsinet4", INETinet4containsinet4nomask, false, "", args(1,4, arg("",bit),arg("ip1",inet4),arg("ip2",inet4),arg("msk2",bte))),
 command("inet46", "inet4containsorequalinet4", INETinet4containsorequalinet4, false, "", args(1,5, arg("",bit),arg("ip1",inet4),arg("msk1",bte),arg("ip2",inet4),arg("msk2",bte))),
 command("inet46", "inet4containsorequalinet4", INETinet4containsorequalinet4nomask, false, "", args(1,4, arg("",bit),arg("ip1",inet4),arg("ip2",inet4),arg("msk2",bte))),
 command("inet46", "inet4containssymmetricinet4", INETinet4containssymmetricinet4, false, "", args(1,5, arg("",bit),arg("ip1",inet4),arg("msk1",bte),arg("ip2",inet4),arg("msk2",bte))),
 command("inet46", "inet6containsinet6", INETinet6containsinet6, false, "", args(1,5, arg("",bit),arg("ip1",inet6),arg("msk1",sht),arg("ip2",inet6),arg("msk2",sht))),
 command("inet46", "inet6containsinet6", INETinet6containsinet6nomask, false, "", args(1,4, arg("",bit),arg("ip1",inet6),arg("ip2",inet6),arg("msk2",sht))),
 command("inet46", "inet6containsorequalinet6", INETinet6containsorequalinet6, false, "", args(1,5, arg("",bit),arg("ip1",inet6),arg("msk1",sht),arg("ip2",inet6),arg("msk2",sht))),
 command("inet46", "inet6containsorequalinet6", INETinet6containsorequalinet6nomask, false, "", args(1,4, arg("",bit),arg("ip1",inet6),arg("ip2",inet6),arg("msk2",sht))),
 command("inet46", "inet6containssymmetricinet6", INETinet6containssymmetricinet6, false, "", args(1,5, arg("",bit),arg("ip1",inet6),arg("msk1",sht),arg("ip2",inet6),arg("msk2",sht))),
 command("inet46", "inet4containsinet4", INETinet4containsinet4_bulk, false, "", args(1,7, batarg("",bit),batarg("ip1",inet4),batarg("msk1",bte),batarg("c1",oid),batarg("ip2",inet4),batarg("msk2",bte),batarg("c2",oid))),
 command("inet46", "inet4containsorequalinet4", INETinet4containsorequalinet4_bulk, false, "", args(1,7, batarg("",bit),batarg("ip1",inet4),batarg("msk1",bte),batarg("c1",oid),batarg("ip2",inet4),batarg("msk2",bte),batarg("c2",oid))),
 command("inet46", "inet4containssymmetricinet4", INETinet4containssymmetricinet4_bulk, false, "", args(1,7, batarg("",bit),batarg("ip1",inet4),batarg("msk1",bte),batarg("c1",oid),batarg("ip2",inet4),batarg("msk2",bte),batarg("c2",oid))),
 command("inet46", "inet6containsinet6", INETinet6containsinet6_bulk, false, "", args(1,7, batarg("",bit),batarg("ip1",inet6),batarg("msk1",sht),batarg("c1",oid),batarg("ip2",inet6),batarg("msk2",sht),batarg("c2",oid))),
 command("inet46", "inet6containsorequalinet6", INETinet6containsorequalinet6_bulk, false, "", args(1,7, batarg("",bit),batarg("ip1",inet6),batarg("msk1",sht),batarg("c1",oid),batarg("ip2",inet6),batarg("msk2",sht),batarg("c2",oid))),
 command("inet46", "inet6containssymmetricinet6", INETinet6containssymmetricinet6_bulk, false, "", args(1,7, batarg("",bit),batarg("ip1",inet6),batarg("msk1",sht),batarg("c1",oid),batarg("ip2",inet6),batarg("msk2",sht),batarg("c2",oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_inet46_mal)
{ mal_module("inet46", NULL, inet46_init_funcs); }
