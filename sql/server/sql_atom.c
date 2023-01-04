/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_atom.h"
#include "sql_string.h"
#include "sql_decimal.h"
#include "gdk_time.h"

void
atom_init( atom *a )
{
	a->isnull = 1;
	a->data.vtype = 0;
	a->tpe.type = NULL;
}

static atom *
atom_create( sql_allocator *sa )
{
	atom *a = SA_NEW(sa, atom);

	if (!a)
		return NULL;
	*a = (atom) {
		.data = (ValRecord) {.vtype = TYPE_void,},
	};
	return a;
}

atom *
atom_bool( sql_allocator *sa, sql_subtype *tpe, bit val)
{
	atom *a = atom_create(sa);
	if(!a)
		return NULL;

	a->isnull = 0;
	a->tpe = *tpe;
	a->data.vtype = tpe->type->localtype;
	a->data.val.btval = val;
	a->data.len = 0;
	return a;
}

atom *
atom_int( sql_allocator *sa, sql_subtype *tpe,
#ifdef HAVE_HGE
	hge val
#else
	lng val
#endif
)
{
	if (tpe->type->eclass == EC_FLT) {
		return atom_float(sa, tpe, (dbl) val);
	} else {
		atom *a = atom_create(sa);
		if(!a)
			return NULL;

		a->isnull = 0;
		a->tpe = *tpe;
		a->data.vtype = tpe->type->localtype;
		switch (ATOMstorage(a->data.vtype)) {
		case TYPE_bte:
			a->data.val.btval = (bte) val;
			break;
		case TYPE_sht:
			a->data.val.shval = (sht) val;
			break;
		case TYPE_int:
			a->data.val.ival = (int) val;
			break;
		case TYPE_oid:
			a->data.val.oval = (oid) val;
			break;
		case TYPE_lng:
			a->data.val.lval = (lng) val;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			a->data.val.hval = val;
			break;
#endif
		default:
			assert(0);
		}
		a->data.len = 0;
		return a;
	}
}

#ifdef HAVE_HGE
hge
#else
lng
#endif
atom_get_int(atom *a)
{
#ifdef HAVE_HGE
	hge r = 0;
#else
	lng r = 0;
#endif

	if (!a->isnull) {
		switch (ATOMstorage(a->data.vtype)) {
		case TYPE_bte:
			r = a->data.val.btval;
			break;
		case TYPE_sht:
			r = a->data.val.shval;
			break;
		case TYPE_int:
			r = a->data.val.ival;
			break;
		case TYPE_oid:
			r = a->data.val.oval;
			break;
		case TYPE_lng:
			r = a->data.val.lval;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			r = a->data.val.hval;
			break;
#endif
		}
	}
	return r;
}

atom *
atom_dec(sql_allocator *sa, sql_subtype *tpe,
#ifdef HAVE_HGE
	hge val)
#else
	lng val)
#endif
{
	return atom_int(sa, tpe, val);
}

atom *
atom_string(sql_allocator *sa, sql_subtype *tpe, const char *val)
{
	atom *a = atom_create(sa);
	if(!a)
		return NULL;

	a->isnull = 1;
	a->tpe = *tpe;
	a->data.val.sval = NULL;
	a->data.vtype = TYPE_str;
	a->data.len = 0;
	if (val) {
		a->isnull = 0;
		a->data.val.sval = (char*)val;
		a->data.len = strlen(a->data.val.sval);
	}
	return a;
}

atom *
atom_float(sql_allocator *sa, sql_subtype *tpe, dbl val)
{
	atom *a = atom_create(sa);
	if(!a)
		return NULL;

	a->isnull = 0;
	a->tpe = *tpe;
	if (tpe->type->localtype == TYPE_dbl)
		a->data.val.dval = val;
	else {
		assert((dbl) GDK_flt_min <= val && val <= (dbl) GDK_flt_max);
		a->data.val.fval = (flt) val;
	}
	a->data.vtype = tpe->type->localtype;
	a->data.len = 0;
	return a;
}

#ifdef HAVE_HGE
const hge scales[39] = {
	(hge) LL_CONSTANT(1),
	(hge) LL_CONSTANT(10),
	(hge) LL_CONSTANT(100),
	(hge) LL_CONSTANT(1000),
	(hge) LL_CONSTANT(10000),
	(hge) LL_CONSTANT(100000),
	(hge) LL_CONSTANT(1000000),
	(hge) LL_CONSTANT(10000000),
	(hge) LL_CONSTANT(100000000),
	(hge) LL_CONSTANT(1000000000),
	(hge) LL_CONSTANT(10000000000),
	(hge) LL_CONSTANT(100000000000),
	(hge) LL_CONSTANT(1000000000000),
	(hge) LL_CONSTANT(10000000000000),
	(hge) LL_CONSTANT(100000000000000),
	(hge) LL_CONSTANT(1000000000000000),
	(hge) LL_CONSTANT(10000000000000000),
	(hge) LL_CONSTANT(100000000000000000),
	(hge) LL_CONSTANT(1000000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000000),
	(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000000U)
};
#else
const lng scales[19] = {
	LL_CONSTANT(1),
	LL_CONSTANT(10),
	LL_CONSTANT(100),
	LL_CONSTANT(1000),
	LL_CONSTANT(10000),
	LL_CONSTANT(100000),
	LL_CONSTANT(1000000),
	LL_CONSTANT(10000000),
	LL_CONSTANT(100000000),
	LL_CONSTANT(1000000000),
	LL_CONSTANT(10000000000),
	LL_CONSTANT(100000000000),
	LL_CONSTANT(1000000000000),
	LL_CONSTANT(10000000000000),
	LL_CONSTANT(100000000000000),
	LL_CONSTANT(1000000000000000),
	LL_CONSTANT(10000000000000000),
	LL_CONSTANT(100000000000000000),
	LL_CONSTANT(1000000000000000000)
};
#endif

atom *
atom_general(sql_allocator *sa, sql_subtype *tpe, const char *val)
{
	atom *a = atom_create(sa);

	if(!a)
		return NULL;
	a->tpe = *tpe;
	a->data.vtype = tpe->type->localtype;
	assert(a->data.vtype >= 0);

	if (!strNil(val)) {
		int type = a->data.vtype;

		if (type == TYPE_str) {
			a->data.len = strLen(val);
			a->data.val.sval = sa_alloc(sa, a->data.len);
			memcpy(a->data.val.sval, val, a->data.len);
		} else {
			ptr p = NULL;
			ssize_t res = ATOMfromstr(type, &p, &a->data.len, val, false);

			/* no result or nil means error (SQL has NULL not nil) */
			if (res < 0 || !p || ATOMcmp(type, p, ATOMnilptr(type)) == 0) {
				GDKfree(p);
				GDKclrerr();
				return NULL;
			}
			VALset(&a->data, a->data.vtype, p);
			SA_VALcopy(sa, &a->data, &a->data);
			if (tpe->type->eclass == EC_TIME && tpe->digits <= 7) {
				unsigned int diff = 6-(tpe->digits-1);

				assert(diff < MAX_SCALE);
#ifdef HAVE_HGE
				hge d = scales[diff];
#else
				lng d = scales[diff];
#endif

				a->data.val.lval /= d;
				a->data.val.lval *= d;
			}
			GDKfree(p);
		}
	} else {
		VALset(&a->data, a->data.vtype, (ptr) ATOMnilptr(a->data.vtype));
		a->isnull = 1;
	}
	return a;
}

atom *
atom_ptr( sql_allocator *sa, sql_subtype *tpe, void *v)
{
	atom *a = atom_create(sa);
	if(!a)
		return NULL;
	a->tpe = *tpe;
	a->isnull = 0;
	a->data.vtype = TYPE_ptr;
	VALset(&a->data, a->data.vtype, &v);
	a->data.len = 0;
	return a;
}

atom *
atom_general_ptr( sql_allocator *sa, sql_subtype *tpe, void *v)
{
	atom *a = atom_create(sa);
	if(!a)
		return NULL;
	a->tpe = *tpe;
	a->data.vtype = tpe->type->localtype;
	if (!ATOMextern(a->data.vtype)) {
		VALset(&a->data, a->data.vtype, v);
	} else if (a->data.vtype == TYPE_str) {
		const char *p = (const char*) v;
		a->data.len = strLen(p);
		a->data.val.sval = sa_alloc(sa, a->data.len);
		memcpy(a->data.val.sval, p, a->data.len);
	} else {
		a->data.len = ATOMlen(a->data.vtype, v);
		a->data.val.pval = sa_alloc(sa, a->data.len);
		memcpy(a->data.val.pval, v, a->data.len);
	}
	a->isnull = VALisnil(&a->data);
	return a;
}

char *
atom2string(sql_allocator *sa, atom *a)
{
	char buf[BUFSIZ], *p = NULL;

	if (a->isnull)
		return sa_strdup(sa, "NULL");
	switch (a->data.vtype) {
#ifdef HAVE_HGE
	case TYPE_hge:
	{	char *_buf = buf;
		size_t _bufsiz = BUFSIZ;
		hgeToStr(&_buf, &_bufsiz, &a->data.val.hval, true);
		break;
	}
#endif
	case TYPE_lng:
		sprintf(buf, LLFMT, a->data.val.lval);
		break;
	case TYPE_oid:
		sprintf(buf, OIDFMT "@0", a->data.val.oval);
		break;
	case TYPE_int:
		sprintf(buf, "%d", a->data.val.ival);
		break;
	case TYPE_sht:
		sprintf(buf, "%d", a->data.val.shval);
		break;
	case TYPE_bte:
		sprintf(buf, "%d", a->data.val.btval);
		break;
	case TYPE_bit:
		if (a->data.val.btval)
			return sa_strdup(sa, "true");
		return sa_strdup(sa, "false");
	case TYPE_flt:
		sprintf(buf, "%f", a->data.val.fval);
		break;
	case TYPE_dbl:
		sprintf(buf, "%f", a->data.val.dval);
		break;
	case TYPE_str:
		assert(a->data.val.sval);
		return sa_strdup(sa, a->data.val.sval);
	default:
		if ((p = ATOMformat(a->data.vtype, VALget(&a->data))) == NULL) {
			snprintf(buf, BUFSIZ, "atom2string(TYPE_%d) not implemented", a->data.vtype);
		} else {
			 char *r = sa_strdup(sa, p);
			 GDKfree(p);
			 return r;
		}
	}
	return sa_strdup(sa, buf);
}

char *
atom2sql(sql_allocator *sa, atom *a, int timezone)
{
	sql_class ec = a->tpe.type->eclass;
	char buf[BUFSIZ];

	if (a->data.vtype == TYPE_str && EC_INTERVAL(ec))
		ec = EC_STRING;
	if (a->isnull)
		return "NULL";
	switch (ec) {
	case EC_BIT:
		assert( a->data.vtype == TYPE_bit);
		if (a->data.val.btval)
			return "true";
		return "false";
	case EC_CHAR:
	case EC_STRING: {
		char *val, *res;
		assert(a->data.vtype == TYPE_str && a->data.val.sval);

		if (!(val = sql_escape_str(sa, a->data.val.sval)))
			return NULL;
		if ((res = SA_NEW_ARRAY(sa, char, strlen(val) + 3)))
			stpcpy(stpcpy(stpcpy(res, "'"), val), "'");
		return res;
	} break;
	case EC_BLOB: {
		char *res;
		blob *b = (blob*)a->data.val.pval;
		size_t blobstr_size = b->nitems * 2 + 1;

		if ((res = SA_NEW_ARRAY(sa, char, blobstr_size + 8))) {
			char *tail = stpcpy(res, "blob '");
			ssize_t blobstr_offset = BATatoms[TYPE_blob].atomToStr(&tail, &blobstr_size, b, true);
			strcpy(res + blobstr_offset + 6, "'");
		}
		return res;
	} break;
	case EC_MONTH:
	case EC_SEC: {
		lng v;
		switch (a->data.vtype) {
		case TYPE_lng:
			v = a->data.val.lval;
			break;
		case TYPE_int:
			v = a->data.val.ival;
			break;
		case TYPE_sht:
			v = a->data.val.shval;
			break;
		case TYPE_bte:
			v = a->data.val.btval;
			break;
		default:
			v = 0;
			break;
		}
		switch (a->tpe.digits) {
		case 1:		/* year */
			v /= 12;
			break;
		case 2:		/* year to month */
		case 3:		/* month */
			break;
		case 4:		/* day */
			v /= 60 * 60 * 24;
			break;
		case 5:		/* day to hour */
		case 8:		/* hour */
			v /= 60 * 60;
			break;
		case 6:		/* day to minute */
		case 9:		/* hour to minute */
		case 11:	/* minute */
			v /= 60;
			break;
		case 7:		/* day to second */
		case 10:	/* hour to second */
		case 12:	/* minute to second */
		case 13:	/* second */
			break;
		}
		sprintf(buf, "interval '" LLFMT "' %s", ec == EC_MONTH ? v : v/1000, ec == EC_MONTH ? "month" : "second");
		break;
	}
	case EC_NUM:
		switch (a->data.vtype) {
#ifdef HAVE_HGE
		case TYPE_hge:
		{	char *_buf = buf;
			size_t _bufsiz = BUFSIZ;
			hgeToStr(&_buf, &_bufsiz, &a->data.val.hval, true);
			break;
		}
#endif
		case TYPE_lng:
			sprintf(buf, LLFMT, a->data.val.lval);
			break;
		case TYPE_int:
			sprintf(buf, "%d", a->data.val.ival);
			break;
		case TYPE_sht:
			sprintf(buf, "%d", a->data.val.shval);
			break;
		case TYPE_bte:
			sprintf(buf, "%d", a->data.val.btval);
			break;
		default:
			break;
		}
		break;
	case EC_DEC: {
#ifdef HAVE_HGE
		hge v = 0;
#else
		lng v = 0;
#endif
		switch (a->data.vtype) {
#ifdef HAVE_HGE
		case TYPE_hge: v = a->data.val.hval; break;
#endif
		case TYPE_lng: v = a->data.val.lval; break;
		case TYPE_int: v = a->data.val.ival; break;
		case TYPE_sht: v = a->data.val.shval; break;
		case TYPE_bte: v = a->data.val.btval; break;
		default: break;
		}
		return decimal_to_str(sa, v, &a->tpe);
	}
	case EC_FLT:
		if (a->data.vtype == TYPE_dbl)
			sprintf(buf, "%f", a->data.val.dval);
		else
			sprintf(buf, "%f", a->data.val.fval);
		break;
	case EC_TIME:
	case EC_TIME_TZ:
	case EC_DATE:
	case EC_TIMESTAMP:
	case EC_TIMESTAMP_TZ: {
		char val1[64], sbuf[64], *val2 = sbuf, *res;
		size_t len = sizeof(sbuf);

		switch (ec) {
		case EC_TIME:
		case EC_TIME_TZ:
		case EC_TIMESTAMP:
		case EC_TIMESTAMP_TZ: {
			char *n = stpcpy(val1, (ec == EC_TIME || ec == EC_TIME_TZ) ? "TIME" : "TIMESTAMP");
			if (a->tpe.digits) {
				char str[16];
				sprintf(str, "%u", a->tpe.digits);
				n = stpcpy(stpcpy(stpcpy(n, " ("), str), ")");
			}
			if (ec == EC_TIME_TZ || ec == EC_TIMESTAMP_TZ)
				stpcpy(n, " WITH TIME ZONE");
		} break;
		case EC_DATE:
			strcpy(val1, "DATE");
		break;
		default:
			assert(0);
		}

		switch (ec) {
		case EC_TIME:
		case EC_TIME_TZ: {
			daytime dt = a->data.val.lval;
			unsigned int digits = a->tpe.digits ? a->tpe.digits - 1 : 0;
			char *s = val2;
			ssize_t lens;

			if (ec == EC_TIME_TZ)
				dt = daytime_add_usec_modulo(dt, timezone * 1000);
			if ((lens = daytime_precision_tostr(&s, &len, dt, (int) digits, true)) < 0)
				assert(0);

			if (ec == EC_TIME_TZ) {
				lng timezone_hours = llabs(timezone / 60000);
				char *end = sbuf + sizeof(sbuf) - 1;

				s += lens;
				snprintf(s, end - s, "%c%02d:%02d", (timezone >= 0) ? '+' : '-', (int) (timezone_hours / 60), (int) (timezone_hours % 60));
			}
		} break;
		case EC_DATE: {
			date dt = a->data.val.ival;
			if (date_tostr(&val2, &len, &dt, false) < 0)
				assert(0);
		} break;
		case EC_TIMESTAMP:
		case EC_TIMESTAMP_TZ: {
			timestamp ts = a->data.val.lval;
			unsigned int digits = a->tpe.digits ? a->tpe.digits - 1 : 0;
			char *s = val2;
			size_t nlen;
			ssize_t lens;
			date days;
			daytime usecs;

			if (ec == EC_TIMESTAMP_TZ)
				ts = timestamp_add_usec(ts, timezone * 1000);
			days = timestamp_date(ts);
			if ((lens = date_tostr(&s, &len, &days, true)) < 0)
				assert(0);

			s += lens;
			*s++ = ' ';
			nlen = len - lens - 1;
			assert(nlen < len);

			usecs = timestamp_daytime(ts);
			if ((lens = daytime_precision_tostr(&s, &nlen, usecs, (int) digits, true)) < 0)
				assert(0);

			if (ec == EC_TIMESTAMP_TZ) {
				lng timezone_hours = llabs(timezone / 60000);
				char *end = sbuf + sizeof(sbuf) - 1;

				s += lens;
				snprintf(s, end - s, "%c%02d:%02d", (timezone >= 0) ? '+' : '-', (int) (timezone_hours / 60), (int) (timezone_hours % 60));
			}
		} break;
		default:
			assert(0);
		}

		if ((res = SA_NEW_ARRAY(sa, char, strlen(val1) + strlen(val2) + 4)))
			stpcpy(stpcpy(stpcpy(stpcpy(res, val1)," '"), val2), "'");
		return res;
	} break;
	default:
		snprintf(buf, BUFSIZ, "atom2sql(TYPE_%d) not implemented", a->data.vtype);
	}
	return sa_strdup(sa, buf);
}

sql_subtype *
atom_type(atom *a)
{
	return &a->tpe;
}

atom *
atom_set_type(sql_allocator *sa, atom *a, sql_subtype *t)
{
	atom *na = atom_copy(sa, a);
	na->tpe = *t;
	return na;
}

unsigned int
atom_num_digits( atom *a )
{
#ifdef HAVE_HGE
	hge v = 0;
#else
	lng v = 0;
#endif
	unsigned int inlen = 1;

	switch (a->tpe.type->localtype) {
	case TYPE_bte:
		v = a->data.val.btval;
		break;
	case TYPE_sht:
		v = a->data.val.shval;
		break;
	case TYPE_int:
		v = a->data.val.ival;
		break;
	case TYPE_lng:
		v = a->data.val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		v = a->data.val.hval;
		break;
#endif
	default:
		return 64;
	}
	/* count the number of digits in the input */
	while (v /= 10)
		inlen++;
	return inlen;
}

/* cast atom a to type tp (success returns not NULL, fail returns NULL) */
atom *
atom_cast(sql_allocator *sa, atom *a, sql_subtype *tp)
{
	atom *na = NULL;
	sql_subtype *at = &a->tpe;

	if (subtype_cmp(at, tp) == 0) {
		/* it may be a subtype, but still a different one */
		if (at->type->base.id != tp->type->base.id ||
			at->digits != tp->digits || at->scale != tp->scale) {
			na = atom_create(sa);
			SA_VALcopy(sa, &na->data, &a->data);
			na->data.vtype = tp->type->localtype;
			na->tpe = *tp;
			na->isnull = a->isnull;
			return na;
		}
		return a;
	}
	if (!a->isnull) {
		/* need to do a cast, start simple is atom type a subtype of tp */
		if ((at->type->eclass == tp->type->eclass ||
		    (EC_VARCHAR(at->type->eclass) && EC_VARCHAR(tp->type->eclass))) &&
		    at->type->localtype == tp->type->localtype &&
		   (EC_TEMP(tp->type->eclass) || !tp->digits|| at->digits <= tp->digits) &&
		   (!tp->type->scale || at->scale == tp->scale)) {
			na = atom_create(sa);
			SA_VALcopy(sa, &na->data, &a->data);
			na->tpe = *tp;
			na->data.vtype = tp->type->localtype;
			return na;
		}
		if (((at->type->eclass == EC_DEC ||
			  at->type->eclass == EC_NUM) &&
			 (tp->type->eclass == EC_DEC ||
			  tp->type->eclass == EC_NUM ||
			  tp->type->eclass == EC_FLT)) ||
			(EC_VARCHAR(at->type->eclass) &&
			 (tp->type->eclass == EC_DATE ||
			  EC_TEMP_NOFRAC(tp->type->eclass)))) {
			ValRecord v;
			v.vtype = tp->type->localtype;
			if (VARconvert(&v, &a->data, at->scale, tp->scale, tp->type->eclass == EC_DEC ? tp->digits : 0) != GDK_SUCCEED) {
				GDKclrerr();
				return NULL;
			}
			na = atom_create(sa);
			na->tpe = *tp;
			na->isnull = 0;
			SA_VALcopy(sa, &na->data, &v);
			if (ATOMextern(v.vtype))
				GDKfree(v.val.pval);
			return na;
		}
	} else {
		na = atom_create(sa);
		na->tpe = *tp;
		na->isnull = 1;
		na->data.vtype = tp->type->localtype;
		if (!VALset(&na->data, na->data.vtype, (ptr) ATOMnilptr(na->data.vtype)))
			return NULL;
		return na;
	}
	return NULL;
}

atom *
atom_neg(sql_allocator *sa, atom *a)
{
	ValRecord dst;

	if (a->isnull)
		return a;
	dst.vtype = a->data.vtype;
	if (VARcalcnegate(&dst, &a->data) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	atom *res = atom_create(sa);
	if (!res)
		return NULL;
	res->tpe = a->tpe;
	res->data = dst;
	return res;
}

atom *
atom_absolute(sql_allocator *sa, atom *a)
{
	ValRecord dst;

	if (a->isnull)
		return a;
	dst.vtype = a->data.vtype;
	if (VARcalcabsolute(&dst, &a->data) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	atom *res = atom_create(sa);
	if (!res)
		return NULL;
	res->tpe = a->tpe;
	res->data = dst;
	return res;
}

int
atom_cmp(atom *a1, atom *a2)
{
	if (a1->isnull != a2->isnull)
		return -1;
	if ( a1->isnull)
		return 0;
	if ( a1->tpe.type->localtype != a2->tpe.type->localtype) {
		switch (ATOMstorage(a1->tpe.type->localtype)) {
		case TYPE_bte:
			switch (ATOMstorage(a2->tpe.type->localtype)) {
			case TYPE_sht:
				return (a1->data.val.btval < a2->data.val.shval)?-1:
				       (a1->data.val.btval > a2->data.val.shval)?1:0;
			case TYPE_int:
				return (a1->data.val.btval < a2->data.val.ival)?-1:
				       (a1->data.val.btval > a2->data.val.ival)?1:0;
			case TYPE_lng:
				return (a1->data.val.btval < a2->data.val.lval)?-1:
				       (a1->data.val.btval > a2->data.val.lval)?1:0;
	#ifdef HAVE_HGE
			case TYPE_hge:
				return (a1->data.val.btval < a2->data.val.hval)?-1:
				       (a1->data.val.btval > a2->data.val.hval)?1:0;
	#endif
			}
			return -1;
		case TYPE_sht:
			switch (ATOMstorage(a2->tpe.type->localtype)) {
			case TYPE_bte:
				return (a1->data.val.shval < a2->data.val.btval)?-1:
				       (a1->data.val.shval > a2->data.val.btval)?1:0;
			case TYPE_int:
				return (a1->data.val.shval < a2->data.val.ival)?-1:
				       (a1->data.val.shval > a2->data.val.ival)?1:0;
			case TYPE_lng:
				return (a1->data.val.shval < a2->data.val.lval)?-1:
				       (a1->data.val.shval > a2->data.val.lval)?1:0;
	#ifdef HAVE_HGE
			case TYPE_hge:
				return (a1->data.val.shval < a2->data.val.hval)?-1:
				       (a1->data.val.shval > a2->data.val.hval)?1:0;
	#endif
			}
			return -1;
		case TYPE_int:
			switch (ATOMstorage(a2->tpe.type->localtype)) {
			case TYPE_bte:
				return (a1->data.val.ival < a2->data.val.btval)?-1:
				       (a1->data.val.ival > a2->data.val.btval)?1:0;
			case TYPE_sht:
				return (a1->data.val.ival < a2->data.val.shval)?-1:
				       (a1->data.val.ival > a2->data.val.shval)?1:0;
			case TYPE_lng:
				return (a1->data.val.ival < a2->data.val.lval)?-1:
				       (a1->data.val.ival > a2->data.val.lval)?1:0;
	#ifdef HAVE_HGE
			case TYPE_hge:
				return (a1->data.val.ival < a2->data.val.hval)?-1:
				       (a1->data.val.ival > a2->data.val.hval)?1:0;
	#endif
			}
			return -1;
		case TYPE_lng:
			switch (ATOMstorage(a2->tpe.type->localtype)) {
			case TYPE_bte:
				return (a1->data.val.lval < a2->data.val.btval)?-1:
				       (a1->data.val.lval > a2->data.val.btval)?1:0;
			case TYPE_sht:
				return (a1->data.val.lval < a2->data.val.shval)?-1:
				       (a1->data.val.lval > a2->data.val.shval)?1:0;
			case TYPE_int:
				return (a1->data.val.lval < a2->data.val.ival)?-1:
				       (a1->data.val.lval > a2->data.val.ival)?1:0;
	#ifdef HAVE_HGE
			case TYPE_hge:
				return (a1->data.val.lval < a2->data.val.hval)?-1:
				       (a1->data.val.lval > a2->data.val.hval)?1:0;
	#endif
			}
			return -1;
#ifdef HAVE_HGE
		case TYPE_hge:
			switch (ATOMstorage(a2->tpe.type->localtype)) {
			case TYPE_bte:
				return (a1->data.val.hval < a2->data.val.btval)?-1:
				       (a1->data.val.hval > a2->data.val.btval)?1:0;
			case TYPE_sht:
				return (a1->data.val.hval < a2->data.val.shval)?-1:
				       (a1->data.val.hval > a2->data.val.shval)?1:0;
			case TYPE_int:
				return (a1->data.val.hval < a2->data.val.ival)?-1:
				       (a1->data.val.hval > a2->data.val.ival)?1:0;
			case TYPE_lng:
				return (a1->data.val.hval < a2->data.val.lval)?-1:
				       (a1->data.val.hval > a2->data.val.lval)?1:0;
		}
			return -1;
#endif
		}
	}
	return VALcmp(&a1->data, &a2->data);
}

atom *
atom_add(sql_allocator *sa, atom *a1, atom *a2)
{
	ValRecord dst;

	if ((!EC_COMPUTE(a1->tpe.type->eclass) && (a1->tpe.type->eclass != EC_DEC || a1->tpe.digits != a2->tpe.digits || a1->tpe.scale != a2->tpe.scale)) || a1->tpe.digits < a2->tpe.digits || a1->tpe.type->localtype != a2->tpe.type->localtype)
		return NULL;
	if (a1->tpe.type->localtype < a2->tpe.type->localtype ||
	    (a1->tpe.type->localtype == a2->tpe.type->localtype &&
	     a1->tpe.digits < a2->tpe.digits)) {
		atom *t = a1;
		a1 = a2;
		a2 = t;
	}
	if (a1->isnull || a2->isnull)
		return atom_general(sa, &a1->tpe, NULL);
	dst.vtype = a1->tpe.type->localtype;
	if (VARcalcadd(&dst, &a1->data, &a2->data) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	atom *res = atom_create(sa);
	if (!res)
		return NULL;
	res->tpe = a1->tpe;
	res->data = dst;
	return res;
}

atom *
atom_sub(sql_allocator *sa, atom *a1, atom *a2)
{
	ValRecord dst;

	if (!EC_NUMBER(a1->tpe.type->eclass))
		return NULL;
	if (a1->tpe.type->localtype < a2->tpe.type->localtype ||
		(a1->tpe.type->localtype == a2->tpe.type->localtype && a1->tpe.digits < a2->tpe.digits)) {
		atom *na1 = atom_cast(sa, a1, &a2->tpe);
		/*
		atom *t = a1;
		a1 = a2;
		a2 = t;
		*/
		if (!na1)
			return NULL;
		a1 = na1;
	}
	if (a1->isnull || a2->isnull)
		return atom_general(sa, &a1->tpe, NULL);
	dst.vtype = a1->tpe.type->localtype;
	if (VARcalcsub(&dst, &a1->data, &a2->data) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	atom *res = atom_create(sa);
	if (!res)
		return NULL;
	res->tpe = a1->tpe;
	res->data = dst;
	return res;
}

atom *
atom_mul(sql_allocator *sa, atom *a1, atom *a2)
{
	ValRecord dst;

	if (!EC_NUMBER(a1->tpe.type->eclass))
		return NULL;
	if (!EC_INTERVAL(a1->tpe.type->eclass) && (a1->tpe.type->localtype < a2->tpe.type->localtype ||
		(a1->tpe.type->localtype == a2->tpe.type->localtype && a1->tpe.digits < a2->tpe.digits))) {
		atom *t = a1;
		a1 = a2;
		a2 = t;
	}
	if (a1->isnull || a2->isnull)
		return atom_general(sa, &a1->tpe, NULL);
	dst.vtype = a1->tpe.type->localtype;
	if (VARcalcmul(&dst, &a1->data, &a2->data) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	atom *res = atom_create(sa);
	if (!res)
		return NULL;
	res->tpe = a1->tpe;
	res->tpe.digits += a2->tpe.digits;
	res->data = dst;
	return res;
}

atom *
atom_div(sql_allocator *sa, atom *a1, atom *a2)
{
	ValRecord dst;

	if (!EC_NUMBER(a1->tpe.type->eclass))
		return NULL;
	if (a1->isnull || a2->isnull)
		return atom_general(sa, &a1->tpe, NULL);
	dst.vtype = a1->tpe.type->localtype;
	if (VARcalcdiv(&dst, &a1->data, &a2->data) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	atom *res = atom_create(sa);
	if (!res)
		return NULL;
	res->tpe = a1->tpe;
	res->data = dst;
	return res;
}

atom *
atom_inc(sql_allocator *sa, atom *a)
{
	ValRecord dst;

	if (a->isnull)
		return a;
	dst.vtype = a->data.vtype;
	if (VARcalcincr(&dst, &a->data) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	atom *res = atom_create(sa);
	if (!res)
		return NULL;
	res->tpe = a->tpe;
	res->data = dst;
	return res;
}

int
atom_is_zero(atom *a)
{
	if (a->isnull || !ATOMlinear(a->tpe.type->localtype))
		return 0;
	switch (ATOMstorage(a->tpe.type->localtype)) {
	case TYPE_bte:
		return a->data.val.btval == 0;
	case TYPE_sht:
		return a->data.val.shval == 0;
	case TYPE_int:
		return a->data.val.ival == 0;
	case TYPE_lng:
		return a->data.val.lval == 0;
#ifdef HAVE_HGE
	case TYPE_hge:
		return a->data.val.hval == 0;
#endif
	case TYPE_flt:
		return a->data.val.fval == 0;
	case TYPE_dbl:
		return a->data.val.dval == 0;
	default:
		return 0;
	}
}

int
atom_is_true(atom *a)
{
	if (a->isnull)
		return 0;
	switch (ATOMstorage(a->tpe.type->localtype)) {
	case TYPE_bte:
		return a->data.val.btval != 0;
	case TYPE_sht:
		return a->data.val.shval != 0;
	case TYPE_int:
		return a->data.val.ival != 0;
	case TYPE_lng:
		return a->data.val.lval != 0;
#ifdef HAVE_HGE
	case TYPE_hge:
		return a->data.val.hval != 0;
#endif
	case TYPE_flt:
		return a->data.val.fval != 0;
	case TYPE_dbl:
		return a->data.val.dval != 0;
	default:
		return 0;
	}
}

int
atom_is_false(atom *a)
{
	if (a->isnull)
		return 0;
	switch (ATOMstorage(a->tpe.type->localtype)) {
	case TYPE_bte:
		return a->data.val.btval == 0;
	case TYPE_sht:
		return a->data.val.shval == 0;
	case TYPE_int:
		return a->data.val.ival == 0;
	case TYPE_lng:
		return a->data.val.lval == 0;
#ifdef HAVE_HGE
	case TYPE_hge:
		return a->data.val.hval == 0;
#endif
	case TYPE_flt:
		return a->data.val.fval == 0;
	case TYPE_dbl:
		return a->data.val.dval == 0;
	default:
		return 0;
	}
}

atom *
atom_zero_value(sql_allocator *sa, sql_subtype* tpe)
{
	void *ret = NULL;
	atom *res = NULL;
	int localtype = tpe->type->localtype;

	bte bval = 0;
	sht sval = 0;
	int ival = 0;
	lng lval = 0;
#ifdef HAVE_HGE
	hge hval = 0;
#endif
	flt fval = 0;
	dbl dval = 0;

	if (ATOMlinear(localtype)) {
		switch (ATOMstorage(localtype)) {
		case TYPE_bte:
			ret = &bval;
			break;
		case TYPE_sht:
			ret = &sval;
			break;
		case TYPE_int:
			ret = &ival;
			break;
		case TYPE_lng:
			ret = &lval;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ret = &hval;
			break;
#endif
		case TYPE_flt:
			ret = &fval;
			break;
		case TYPE_dbl:
			ret = &dval;
			break;
		default: /* no support for strings and blobs zero value */
			break;
		}
	}

	if (ret != NULL) {
		res = atom_create(sa);
		res->tpe = *tpe;
		res->isnull = 0;
		res->data.vtype = localtype;
		VALset(&res->data, res->data.vtype, ret);
	}

	return res;
}

atom *
atom_max_value(sql_allocator *sa, sql_subtype *tpe)
{
	void *ret = NULL;
	atom *res = NULL;
	int localtype = tpe->type->localtype;

	bte bval = GDK_bte_max;
	sht sval = GDK_sht_max;
	int ival = GDK_int_max;
	lng lval = GDK_lng_max;
#ifdef HAVE_HGE
	hge hval = GDK_hge_max;
#endif
	flt fval = GDK_flt_max;
	dbl dval = GDK_dbl_max;

	if (ATOMlinear(localtype)) {
		switch (ATOMstorage(localtype)) {
		case TYPE_bte:
			ret = &bval;
			break;
		case TYPE_sht:
			ret = &sval;
			break;
		case TYPE_int:
			ret = &ival;
			break;
		case TYPE_lng:
			ret = &lval;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ret = &hval;
			break;
#endif
		case TYPE_flt:
			ret = &fval;
			break;
		case TYPE_dbl:
			ret = &dval;
			break;
		default: /* no support for strings and blobs zero value */
			break;
		}
	}

	if (ret != NULL) {
		res = atom_create(sa);
		res->tpe = *tpe;
		res->isnull = 0;
		res->data.vtype = localtype;
		VALset(&res->data, res->data.vtype, ret);
	}

	return res;
}
