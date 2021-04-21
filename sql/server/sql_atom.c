/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_atom.h"
#include "sql_string.h"
#include "sql_decimal.h"
#include "blob.h"
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

static ValPtr
SA_VALcopy(sql_allocator *sa, ValPtr d, const ValRecord *s)
{
	if (sa == NULL)
		return VALcopy(d, s);
	if (!ATOMextern(s->vtype)) {
		*d = *s;
	} else if (s->val.pval == 0) {
		d->val.pval = ATOMnil(s->vtype);
		if (d->val.pval == NULL)
			return NULL;
		d->vtype = s->vtype;
	} else if (s->vtype == TYPE_str) {
		d->vtype = TYPE_str;
		d->val.sval = sa_strdup(sa, s->val.sval);
		if (d->val.sval == NULL)
			return NULL;
		d->len = strLen(d->val.sval);
	} else {
		ptr p = s->val.pval;

		d->vtype = s->vtype;
		d->len = ATOMlen(d->vtype, p);
		d->val.pval = sa_alloc(sa, d->len);
		if (d->val.pval == NULL)
			return NULL;
		memcpy(d->val.pval, p, d->len);
	}
	return d;
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
hge scales[39] = {
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
lng scales[19] = {
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
	atom *a;
	ptr p = NULL;

	if (tpe->type->localtype == TYPE_str)
		return atom_string(sa, tpe, val);
	a = atom_create(sa);
	if(!a)
		return NULL;
	a->tpe = *tpe;
	a->data.val.pval = NULL;
	a->data.vtype = tpe->type->localtype;
	a->data.len = 0;

	assert(a->data.vtype >= 0);

	if (!strNil(val)) {
		int type = a->data.vtype;

		a->isnull = 0;
		if (ATOMstorage(type) == TYPE_str) {
			a->isnull = 0;
			a->data.val.sval = sa_strdup(sa, val);
			a->data.len = strlen(a->data.val.sval);
		} else {
			ssize_t res = ATOMfromstr(type, &p, &a->data.len, val, false);

			/* no result or nil means error (SQL has NULL not nil) */
			if (res < 0 || !p || ATOMcmp(type, p, ATOMnilptr(type)) == 0) {
				if (p)
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
	atom *a = SA_ZNEW(sa, atom);

	a->tpe = *tpe;
	a->data.vtype = tpe->type->localtype;
	if (ATOMstorage(a->data.vtype) == TYPE_str) {
		if (strNil((char*)v)) {
			VALset(&a->data, a->data.vtype, (ptr) ATOMnilptr(a->data.vtype));
		} else {
			a->data.val.sval = sa_strdup(sa, v);
			a->data.len = strlen(a->data.val.sval);
		}
	} else {
		VALset(&a->data, a->data.vtype, v);
	}
	a->isnull = VALisnil(&a->data);
	return a;
}

char *
atom2string(sql_allocator *sa, atom *a)
{
	char buf[BUFSIZ], *p = NULL;
	void *v;

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
		v = &a->data.val.ival;
		if (ATOMvarsized(a->data.vtype))
			v = a->data.val.pval;
		if ((p = ATOMformat(a->data.vtype, v)) == NULL) {
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
			ssize_t blobstr_offset = BLOBtostr(&tail, &blobstr_size, b, true);
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

void
atom_set_type(atom *a, sql_subtype *t)
{
	a->tpe = *t;
}

atom *
atom_dup(sql_allocator *sa, atom *a)
{
	atom *r = atom_create(sa);
	if(!r)
		return NULL;

	*r = *a;
	r->tpe = a->tpe;
	if (!a->isnull)
		SA_VALcopy(sa, &r->data, &a->data);
	return r;
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

/* cast atom a to type tp (success == 1, fail == 0) */
int
atom_cast(sql_allocator *sa, atom *a, sql_subtype *tp)
{
	sql_subtype *at = &a->tpe;

	if (!a->isnull) {
		if (subtype_cmp(at, tp) == 0)
			return 1;
		/* need to do a cast, start simple is atom type a subtype of tp */
		if ((at->type->eclass == tp->type->eclass ||
		    (EC_VARCHAR(at->type->eclass) && EC_VARCHAR(tp->type->eclass))) &&
		    at->type->localtype == tp->type->localtype &&
		   (EC_TEMP(tp->type->eclass) || !tp->digits|| at->digits <= tp->digits) &&
		   (!tp->type->scale || at->scale == tp->scale)) {
			*at = *tp;
			return 1;
		}
		if (at->type->eclass == EC_NUM && tp->type->eclass == EC_NUM) {
			if (at->type->localtype <= tp->type->localtype) { /* cast to a larger numeric */
				switch (tp->type->localtype) {
				case TYPE_bte:
					if (at->type->localtype != TYPE_bte)
						return 0;
					break;
				case TYPE_sht:
					if (at->type->localtype == TYPE_bte)
						a->data.val.shval = a->data.val.btval;
					else if (at->type->localtype != TYPE_sht)
						return 0;
					break;
				case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
				case TYPE_oid:
#endif
					if (at->type->localtype == TYPE_bte)
						a->data.val.ival = a->data.val.btval;
					else if (at->type->localtype == TYPE_sht)
						a->data.val.ival = a->data.val.shval;
					else if (at->type->localtype != TYPE_int)
						return 0;
					break;
				case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
				case TYPE_oid:
#endif
					if (at->type->localtype == TYPE_bte)
						a->data.val.lval = a->data.val.btval;
					else if (at->type->localtype == TYPE_sht)
						a->data.val.lval = a->data.val.shval;
					else if (at->type->localtype == TYPE_int)
						a->data.val.lval = a->data.val.ival;
					else if (at->type->localtype != TYPE_lng)
						return 0;
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					if (at->type->localtype == TYPE_bte)
						a->data.val.hval = a->data.val.btval;
					else if (at->type->localtype == TYPE_sht)
						a->data.val.hval = a->data.val.shval;
					else if (at->type->localtype == TYPE_int)
						a->data.val.hval = a->data.val.ival;
					else if (at->type->localtype == TYPE_lng)
						a->data.val.hval = a->data.val.lval;
					else if (at->type->localtype != TYPE_hge)
						return 0;
					break;
#endif
				default:
					return 0;
				}
			} else { /* cast to a smaller numeric */
				switch (tp->type->localtype) {
#ifdef HAVE_HGE
				case TYPE_bte:
					if (a->data.val.hval > (hge) GDK_bte_max || a->data.val.hval <= (hge) GDK_bte_min)
						return 0;
					a->data.val.btval = (bte) a->data.val.hval;
					break;
				case TYPE_sht:
					if (a->data.val.hval > (hge) GDK_sht_max || a->data.val.hval <= (hge) GDK_sht_min)
						return 0;
					a->data.val.shval = (sht) a->data.val.hval;
					break;
				case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
				case TYPE_oid:
#endif
					if (a->data.val.hval > (hge) GDK_int_max || a->data.val.hval <= (hge) GDK_int_min)
						return 0;
					a->data.val.ival = (int) a->data.val.hval;
					break;
				case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
				case TYPE_oid:
#endif
					if (a->data.val.hval > (hge) GDK_lng_max || a->data.val.hval <= (hge) GDK_lng_min)
						return 0;
					a->data.val.lval = (lng) a->data.val.hval;
					break;
#else
				case TYPE_bte:
					if (a->data.val.lval > (lng) GDK_bte_max || a->data.val.lval <= (lng) GDK_bte_min)
						return 0;
					a->data.val.btval = (bte) a->data.val.lval;
					break;
				case TYPE_sht:
					if (a->data.val.lval > (lng) GDK_sht_max || a->data.val.lval <= (lng) GDK_sht_min)
						return 0;
					a->data.val.shval = (sht) a->data.val.lval;
					break;
				case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
				case TYPE_oid:
#endif
					if (a->data.val.lval > (lng) GDK_int_max || a->data.val.lval <= (lng) GDK_int_min)
						return 0;
					a->data.val.ival = (int) a->data.val.lval;
					break;
#endif
				default:
					return 0;
				}
			}
			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
			return 1;
		}
		if (at->type->eclass == EC_DEC && tp->type->eclass == EC_DEC &&
		    at->type->localtype <= tp->type->localtype &&
		    at->digits <= tp->digits /* &&
		    at->scale <= tp->scale*/) {
#ifdef HAVE_HGE
			hge mul = 1, div = 0, rnd = 0;
#else
			lng mul = 1, div = 0, rnd = 0;
#endif
			/* cast numerics */
			switch (tp->type->localtype) {
			case TYPE_bte:
				if (at->type->localtype != TYPE_bte)
					return 0;
				break;
			case TYPE_sht:
				if (at->type->localtype == TYPE_bte)
					a->data.val.shval = a->data.val.btval;
				else if (at->type->localtype != TYPE_sht)
					return 0;
				break;
			case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			case TYPE_oid:
#endif
				if (at->type->localtype == TYPE_bte)
					a->data.val.ival = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht)
					a->data.val.ival = a->data.val.shval;
				else if (at->type->localtype != TYPE_int)
					return 0;
				break;
			case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			case TYPE_oid:
#endif
				if (at->type->localtype == TYPE_bte)
					a->data.val.lval = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht)
					a->data.val.lval = a->data.val.shval;
				else if (at->type->localtype == TYPE_int)
					a->data.val.lval = a->data.val.ival;
				else if (at->type->localtype != TYPE_lng)
					return 0;
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				if (at->type->localtype == TYPE_bte)
					a->data.val.hval = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht)
					a->data.val.hval = a->data.val.shval;
				else if (at->type->localtype == TYPE_int)
					a->data.val.hval = a->data.val.ival;
				else if (at->type->localtype == TYPE_lng)
					a->data.val.hval = a->data.val.lval;
				else if (at->type->localtype != TYPE_hge)
					return 0;
				break;
#endif
			default:
				return 0;
			}
			/* fix scale */
			if (tp->scale >= at->scale) {
				mul = scales[tp->scale-at->scale];
			} else {
				/* only round when going to a lower scale */
				mul = scales[at->scale-tp->scale];
#ifndef TRUNCATE_NUMBERS
				rnd = mul>>1;
#endif
				div = 1;
			}
			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
#ifdef HAVE_HGE
			if (a->data.vtype == TYPE_hge) {
				if (div) {
					if (a->data.val.hval < 0)
						a->data.val.hval -= rnd;
					else
						a->data.val.hval += rnd;
					a->data.val.hval /= mul;
				} else
					a->data.val.hval *= mul;
			} else if (a->data.vtype == TYPE_lng) {
				if (!div && ((hge) GDK_lng_min > (hge) a->data.val.lval * mul || (hge) a->data.val.lval * mul > (hge) GDK_lng_max))
					return 0;
				if (div) {
					if (a->data.val.lval < 0)
						a->data.val.lval -= (lng)rnd;
					else
						a->data.val.lval += (lng)rnd;
					a->data.val.lval /= (lng) mul;
				} else
					a->data.val.lval *= (lng) mul;
			} else if (a->data.vtype == TYPE_int) {
				if (!div && ((hge) GDK_int_min > (hge) a->data.val.ival * mul || (hge) a->data.val.ival * mul > (hge) GDK_int_max))
					return 0;
				if (div) {
					if (a->data.val.ival < 0)
						a->data.val.ival -= (int)rnd;
					else
						a->data.val.ival += (int)rnd;
					a->data.val.ival /= (int) mul;
				} else
					a->data.val.ival *= (int) mul;
			} else if (a->data.vtype == TYPE_sht) {
				if (!div && ((hge) GDK_sht_min > (hge) a->data.val.shval * mul || (hge) a->data.val.shval * mul > (hge) GDK_sht_max))
					return 0;
				if (div) {
					if (a->data.val.shval < 0)
						a->data.val.shval -= (sht)rnd;
					else
						a->data.val.shval += (sht)rnd;
					a->data.val.shval /= (sht) mul;
				} else
					a->data.val.shval *= (sht) mul;
			} else if (a->data.vtype == TYPE_bte) {
				if (!div && ((hge) GDK_bte_min > (hge) a->data.val.btval * mul || (hge) a->data.val.btval * mul > (hge) GDK_bte_max))
					return 0;
				if (div) {
					if (a->data.val.btval < 0)
						a->data.val.btval -= (bte)rnd;
					else
						a->data.val.btval += (bte)rnd;
					a->data.val.btval /= (bte) mul;
				} else
					a->data.val.btval *= (bte) mul;
			}
#else
			if (a->data.vtype == TYPE_lng) {
				if (div) {
					if (a->data.val.lval < 0)
						a->data.val.lval -= rnd;
					else
						a->data.val.lval += rnd;
					a->data.val.lval /= mul;
				} else
					a->data.val.lval *= mul;
			} else if (a->data.vtype == TYPE_int) {
				if (!div && ((lng) GDK_int_min > (lng) a->data.val.ival * mul || (lng) a->data.val.ival * mul > (lng) GDK_int_max))
					return 0;
				if (div) {
					if (a->data.val.ival < 0)
						a->data.val.ival -= (int)rnd;
					else
						a->data.val.ival += (int)rnd;
					a->data.val.ival /= (int) mul;
				} else
					a->data.val.ival *= (int) mul;
			} else if (a->data.vtype == TYPE_sht) {
				if (!div && ((lng) GDK_sht_min > (lng) a->data.val.shval * mul || (lng) a->data.val.shval * mul > (lng) GDK_sht_max))
					return 0;
				if (div) {
					if (a->data.val.shval < 0)
						a->data.val.shval -= (sht)rnd;
					else
						a->data.val.shval += (sht)rnd;
					a->data.val.shval /= (sht) mul;
				} else
					a->data.val.shval *= (sht) mul;
			} else if (a->data.vtype == TYPE_bte) {
				if (!div && ((lng) GDK_bte_min > (lng) a->data.val.btval * mul || (lng) a->data.val.btval * mul > (lng) GDK_bte_max))
					return 0;
				if (div) {
					if (a->data.val.btval < 0)
						a->data.val.btval -= (bte)rnd;
					else
						a->data.val.btval += (bte)rnd;
					a->data.val.btval /= (bte) mul;
				} else
					a->data.val.btval *= (bte) mul;
			}
#endif
			return 1;
		}
		/* truncating decimals */
		if (at->type->eclass == EC_DEC && tp->type->eclass == EC_DEC &&
		    at->type->localtype >= tp->type->localtype &&
		    at->digits >= tp->digits &&
			(at->digits - tp->digits) == (at->scale - tp->scale)) {
#ifdef HAVE_HGE
			hge mul = 1, rnd = 0, val = 0;
#else
			lng mul = 1, rnd = 0, val = 0;
#endif

			/* fix scale */

			/* only round when going to a lower scale */
			mul = scales[at->scale-tp->scale];
#ifndef TRUNCATE_NUMBERS
			rnd = mul>>1;
#endif

#ifdef HAVE_HGE
			if (a->data.vtype == TYPE_hge) {
				val = a->data.val.hval;
			} else
#endif
			if (a->data.vtype == TYPE_lng) {
				val = a->data.val.lval;
			} else if (a->data.vtype == TYPE_int) {
				val = a->data.val.ival;
			} else if (a->data.vtype == TYPE_sht) {
				val = a->data.val.shval;
			} else if (a->data.vtype == TYPE_bte) {
				val = a->data.val.btval;
			}

			if (val < 0)
				val -= rnd;
			else
				val += rnd;
			val /= mul;

			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
#ifdef HAVE_HGE
			if (a->data.vtype == TYPE_hge) {
				a->data.val.hval = val;
			} else if (a->data.vtype == TYPE_lng) {
				if ( ((hge) GDK_lng_min > val || val > (hge) GDK_lng_max))
					return 0;
				a->data.val.lval = (lng) val;
			} else if (a->data.vtype == TYPE_int) {
				if ( ((hge) GDK_int_min > val || val > (hge) GDK_int_max))
					return 0;
				a->data.val.ival = (int) val;
			} else if (a->data.vtype == TYPE_sht) {
				if ( ((hge) GDK_sht_min > val || val > (hge) GDK_sht_max))
					return 0;
				a->data.val.shval = (sht) val;
			} else if (a->data.vtype == TYPE_bte) {
				if ( ((hge) GDK_bte_min > val || val > (hge) GDK_bte_max))
					return 0;
				a->data.val.btval = (bte) val;
			}
#else
			if (a->data.vtype == TYPE_lng) {
				a->data.val.lval = (lng) val;
			} else if (a->data.vtype == TYPE_int) {
				if ( ((lng) GDK_int_min > val || val > (lng) GDK_int_max))
					return 0;
				a->data.val.ival = (int) val;
			} else if (a->data.vtype == TYPE_sht) {
				if ( ((lng) GDK_sht_min > val || val > (lng) GDK_sht_max))
					return 0;
				a->data.val.shval = (sht) val;
			} else if (a->data.vtype == TYPE_bte) {
				if ( ((lng) GDK_bte_min > val || val > (lng) GDK_bte_max))
					return 0;
				a->data.val.btval = (bte) val;
			}
#endif
			return 1;
		}
		if (at->type->eclass == EC_NUM && tp->type->eclass == EC_DEC &&
		    at->type->localtype <= tp->type->localtype &&
		    (at->digits <= tp->digits || atom_num_digits(a) <= tp->digits) &&
		    at->scale <= tp->scale) {
#ifdef HAVE_HGE
			hge mul = 1;
#else
			lng mul = 1;
#endif
			/* cast numerics */
			switch (tp->type->localtype) {
			case TYPE_bte:
				if (at->type->localtype != TYPE_bte)
					return 0;
				break;
			case TYPE_sht:
				if (at->type->localtype == TYPE_bte)
					a->data.val.shval = a->data.val.btval;
				else if (at->type->localtype != TYPE_sht)
					return 0;
				break;
			case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			case TYPE_oid:
#endif
				if (at->type->localtype == TYPE_bte)
					a->data.val.ival = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht)
					a->data.val.ival = a->data.val.shval;
				else if (at->type->localtype != TYPE_int)
					return 0;
				break;
			case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			case TYPE_oid:
#endif
				if (at->type->localtype == TYPE_bte)
					a->data.val.lval = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht)
					a->data.val.lval = a->data.val.shval;
				else if (at->type->localtype == TYPE_int)
					a->data.val.lval = a->data.val.ival;
				else if (at->type->localtype != TYPE_lng)
					return 0;
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				if (at->type->localtype == TYPE_bte)
					a->data.val.hval = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht)
					a->data.val.hval = a->data.val.shval;
				else if (at->type->localtype == TYPE_int)
					a->data.val.hval = a->data.val.ival;
				else if (at->type->localtype == TYPE_lng)
					a->data.val.hval = a->data.val.lval;
				else if (at->type->localtype != TYPE_hge)
					return 0;
				break;
#endif
			default:
				return 0;
			}
			/* fix scale */
			mul = scales[tp->scale-at->scale];
			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
#ifdef HAVE_HGE
			if (a->data.vtype == TYPE_hge) {
				a->data.val.hval *= mul;
			}
			else if (a->data.vtype == TYPE_lng) {
				if ((hge) GDK_lng_min > (hge) a->data.val.lval * mul || (hge) a->data.val.lval * mul > (hge) GDK_lng_max)
					return 0;
				a->data.val.lval *= (lng) mul;
			}
			else if (a->data.vtype == TYPE_int) {
				if ((hge) GDK_int_min > (hge) a->data.val.ival * mul || (hge) a->data.val.ival * mul > (hge) GDK_int_max)
					return 0;
				a->data.val.ival *= (int) mul;
			}
			else if (a->data.vtype == TYPE_sht) {
				if ((hge) GDK_sht_min > (hge) a->data.val.shval * mul || (hge) a->data.val.shval * mul > (hge) GDK_sht_max)
					return 0;
				a->data.val.shval *= (sht) mul;
			}
			else if (a->data.vtype == TYPE_bte) {
				if ((hge) GDK_bte_min > (hge) a->data.val.btval * mul || (hge) a->data.val.btval * mul > (hge) GDK_bte_max)
					return 0;
				a->data.val.btval *= (bte) mul;
			}
#else
			if (a->data.vtype == TYPE_lng) {
				a->data.val.lval *= mul;
			}
			else if (a->data.vtype == TYPE_int) {
				if ((lng) GDK_int_min > (lng) a->data.val.ival * mul || (lng) a->data.val.ival * mul > (lng) GDK_int_max)
					return 0;
				a->data.val.ival *= (int) mul;
			}
			else if (a->data.vtype == TYPE_sht) {
				if ((lng) GDK_sht_min > (lng) a->data.val.shval * mul || (lng) a->data.val.shval * mul > (lng) GDK_sht_max)
					return 0;
				a->data.val.shval *= (sht) mul;
			}
			else if (a->data.vtype == TYPE_bte) {
				if ((lng) GDK_bte_min > (lng) a->data.val.btval * mul || (lng) a->data.val.btval * mul > (lng) GDK_bte_max)
					return 0;
				a->data.val.btval *= (bte) mul;
			}
#endif
			return 1;
		}
		if ((at->type->eclass == EC_DEC ||
		     at->type->eclass == EC_NUM) &&
		    tp->type->eclass == EC_FLT) {
			if (!VALisnil(&a->data)) {
				char *s;
#ifdef HAVE_HGE
				hge dec = 0;
#else
				lng dec = 0;
#endif
				/* cast decimals to doubles */
				switch (at->type->localtype) {
				case TYPE_bte:
					dec = a->data.val.btval;
					break;
				case TYPE_sht:
					dec = a->data.val.shval;
					break;
				case TYPE_int:
					dec = a->data.val.ival;
					break;
				case TYPE_lng:
					dec = a->data.val.lval;
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					dec = a->data.val.hval;
					break;
#endif
				default:
					return 0;
				}
				s = decimal_to_str(sa, dec, at);
				if (s) {
					int tpe = tp->type->localtype;
					size_t len = (tpe == TYPE_dbl) ? sizeof(dbl) : sizeof(flt);
					ssize_t res;
					ptr p = &(a->data.val);
					if ((res = ATOMfromstr(tpe, &p, &len, s, false)) < 0) {
						GDKclrerr();
						return 0;
					}
				} else {
					return 0;
				}
			}
			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
			return 1;
		}
		if (EC_VARCHAR(at->type->eclass) && (tp->type->eclass == EC_DATE || EC_TEMP_NOFRAC(tp->type->eclass))){
			int type = tp->type->localtype;
			ssize_t res = 0;
			ptr p = NULL;

			a->data.len = 0;
			res = ATOMfromstr(type, &p, &a->data.len, a->data.val.sval, false);
			/* no result or nil means error (SQL has NULL not nil) */
			if (res < (ssize_t) strlen(a->data.val.sval) || !p ||
			    ATOMcmp(type, p, ATOMnilptr(type)) == 0) {
				GDKfree(p);
				a->data.len = strlen(a->data.val.sval);
				GDKclrerr();
				return 0;
			}
			a->tpe = *tp;
			a->data.vtype = type;
			VALset(&a->data, a->data.vtype, p);
			SA_VALcopy(sa, &a->data, &a->data);
			GDKfree(p);
			return 1;
		}
	} else {
		a->tpe = *tp;
		a->data.vtype = tp->type->localtype;
		return VALset(&a->data, a->data.vtype, (ptr) ATOMnilptr(a->data.vtype)) != NULL;
	}
	return 0;
}

int
atom_neg(atom *a)
{
	ValRecord dst;
	if (a->isnull)
		return 0;
	VALempty(&dst);
	dst.vtype = a->data.vtype;
	if (VARcalcnegate(&dst, &a->data) != GDK_SUCCEED) {
		GDKclrerr();
		return -1;
	}
	a->data = dst;
	return 0;
}

int
atom_cmp(atom *a1, atom *a2)
{
	if ( a1->tpe.type->localtype != a2->tpe.type->localtype)
		return -1;
	if ( a1->isnull != a2->isnull)
		return -1;
	if ( a1->isnull)
		return 0;
	return VALcmp(&a1->data, &a2->data);
}

atom *
atom_add(atom *a1, atom *a2)
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
	dst.vtype = a1->tpe.type->localtype;
	if (VARcalcadd(&dst, &a1->data, &a2->data, 1) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	a1->data = dst;
	if (a1->isnull || a2->isnull)
		a1->isnull = 1;
	return a1;
}

atom *
atom_sub(atom *a1, atom *a2)
{
	ValRecord dst;

	if ((!EC_COMPUTE(a1->tpe.type->eclass) && (a1->tpe.type->eclass != EC_DEC || a1->tpe.digits != a2->tpe.digits || a1->tpe.scale != a2->tpe.scale)) || a1->tpe.digits < a2->tpe.digits || a1->tpe.type->localtype != a2->tpe.type->localtype)
		return NULL;
	if (a1->tpe.type->localtype < a2->tpe.type->localtype ||
	    (a1->tpe.type->localtype == a2->tpe.type->localtype &&
	     a1->tpe.digits < a2->tpe.digits))
		dst.vtype = a2->tpe.type->localtype;
	else
		dst.vtype = a1->tpe.type->localtype;
	if (VARcalcsub(&dst, &a1->data, &a2->data, 1) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	if (a1->tpe.type->localtype < a2->tpe.type->localtype ||
	    (a1->tpe.type->localtype == a2->tpe.type->localtype &&
	     a1->tpe.digits < a2->tpe.digits))
		a1 = a2;
	a1->data = dst;
	if (a1->isnull || a2->isnull)
		a1->isnull = 1;
	return a1;
}

atom *
atom_mul(atom *a1, atom *a2)
{
	ValRecord dst;

	if (!EC_COMPUTE(a1->tpe.type->eclass))
		return NULL;
	if (a1->tpe.type->localtype < a2->tpe.type->localtype ||
	    (a1->tpe.type->localtype == a2->tpe.type->localtype &&
	     a1->tpe.digits < a2->tpe.digits)) {
		atom *t = a1;
		a1 = a2;
		a2 = t;
	}
	if (a1->isnull || a2->isnull) {
		a1->isnull = 1;
		return a1;
	}
	dst.vtype = a1->tpe.type->localtype;
	if (VARcalcmul(&dst, &a1->data, &a2->data, 1) != GDK_SUCCEED) {
		GDKclrerr();
		return NULL;
	}
	a1->data = dst;
	a1->tpe.digits += a2->tpe.digits;
	return a1;
}

int
atom_inc(atom *a)
{
	ValRecord dst;

	if (a->isnull)
		return -1;
	dst.vtype = a->data.vtype;
	if (VARcalcincr(&dst, &a->data, 1) != GDK_SUCCEED) {
		GDKclrerr();
		return -1;
	}
	a->data = dst;
	return 0;
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
