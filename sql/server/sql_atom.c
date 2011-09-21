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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "sql_atom.h"
#include <sql_string.h>
#include <sql_decimal.h>

static int atom_debug = 0;

static atom *
atom_create( sql_allocator *sa )
{
	atom *a;
	a = SA_NEW(sa, atom);

	memset(&a->data, 0, sizeof(a->data));
	a->d = dbl_nil;
	return a;
}

static ValPtr
SA_VALcopy(sql_allocator *sa, ValPtr d, ValPtr s)
{
	if (!ATOMextern(s->vtype)) {
		*d = *s;
	} else if (s->val.pval == 0) {
		d->val.pval = ATOMnil(s->vtype);
		d->vtype = s->vtype;
	} else if (s->vtype == TYPE_str) {
		d->vtype = TYPE_str;
		d->val.sval = sa_strdup(sa, s->val.sval);
		d->len = strLen(d->val.sval);
	} else if (s->vtype == TYPE_bit) {
		d->vtype = s->vtype;
		d->len = 1;
		d->val.cval[0] = s->val.cval[0];
	} else {
		ptr p = s->val.pval;

		d->vtype = s->vtype;
		d->len = ATOMlen(d->vtype, p);
		d->val.pval = sa_alloc(sa, d->len);
		memcpy(d->val.pval, p, d->len);
	}
	return d;
}

atom *
atom_bool( sql_allocator *sa, sql_subtype *tpe, bit val)
{
	atom *a = atom_create(sa);
	
	a->isnull = 0;
	a->tpe = *tpe;
	a->data.vtype = tpe->type->localtype;
	a->data.val.cval[0] = val;
	a->data.len = 0;
	return a;
}

atom *
atom_int( sql_allocator *sa, sql_subtype *tpe, lng val)
{
	if (tpe->type->eclass == EC_FLT) {
		return atom_float(sa, tpe, (double) val);
	} else {
		atom *a = atom_create(sa);

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
		case TYPE_wrd:
			a->data.val.wval = (wrd) val;
			break;
		case TYPE_lng:
			a->data.val.lval = val;
			break;
		default:
			printf("atom_int %d\n", a->data.vtype);
			assert(0);
		}
		a->d = (dbl) val;
		a->data.len = 0;
		if (atom_debug)
			fprintf(stderr, "atom_int(%s,"LLFMT")\n", tpe->type->sqlname, val);
		return a;
	}
}

lng 
atom_get_int(atom *a)
{
	lng r = 0;

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
		case TYPE_wrd:
			r = a->data.val.wval;
			break;
		case TYPE_lng:
			r = a->data.val.lval;
			break;
		}
	}
	return r;
}


atom *
atom_dec(sql_allocator *sa, sql_subtype *tpe, lng val, double dval)
{
	atom *a = atom_int(sa, tpe, val);
	if (a) 
		a -> d = dval;
	return a;
}

atom *
atom_string(sql_allocator *sa, sql_subtype *tpe, char *val)
{
	atom *a = atom_create(sa);

	a->isnull = 1;
	a->tpe = *tpe;
	a->data.val.sval = NULL;
	a->data.vtype = TYPE_str;
	a->data.len = 0;
	if (val) {
		a->isnull = 0;
		a->data.val.sval = val;
		a->data.len = (int)strlen(a->data.val.sval);
	}

	if (atom_debug)
		fprintf(stderr, "atom_string(%s,%s)\n", tpe->type->sqlname, val);
	return a;
}

atom *
atom_float(sql_allocator *sa, sql_subtype *tpe, double val)
{
	atom *a = atom_create(sa);

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
	if (atom_debug)
		fprintf(stderr, "atom_float(%s,%f)\n", tpe->type->sqlname, val);
	return a;
}

atom *
atom_general(sql_allocator *sa, sql_subtype *tpe, char *val)
{
	atom *a;
	ptr p = NULL;

	if (atom_debug)
		fprintf(stderr, "atom_general(%s,%s)\n", tpe->type->sqlname, val);

	if (tpe->type->localtype == TYPE_str)
		return atom_string(sa, tpe, val);
	a = atom_create(sa);
	a->tpe = *tpe;
	a->data.val.pval = NULL;
	a->data.vtype = tpe->type->localtype;
	a->data.len = 0;

	assert(a->data.vtype >= 0);

	if (val) {
		int type = a->data.vtype;

		a->isnull = 0;
		if (ATOMstorage(type) == TYPE_str) {
			a->isnull = 0;
			a->data.val.sval = sql2str(sa_strdup(sa, val));
			a->data.len = (int)strlen(a->data.val.sval);
		} else { 
			int res = ATOMfromstr(type, &p, &a->data.len, val);

			/* no result or nil means error (SQL has NULL not nil) */
			if (res < 0 || !p || ATOMcmp(type, p, ATOMnilptr(type)) == 0) {
				/*_DELETE(val);*/
				if (p)
					GDKfree(p);
				return NULL;
			}
			VALset(&a->data, a->data.vtype, p);
			SA_VALcopy(sa, &a->data, &a->data);

			if (p && ATOMextern(a->data.vtype) == 0)
				GDKfree(p);
			/*_DELETE(val);*/
		}
	} else { 
		p = ATOMnilptr(a->data.vtype);
		VALset(&a->data, a->data.vtype, p);
		a->isnull = 1;
	}
	return a;
}

atom *
atom_ptr( sql_allocator *sa, sql_subtype *tpe, void *v)
{
	atom *a = atom_create(sa);
	a->tpe = *tpe;
	a->isnull = 0;
	a->data.vtype = TYPE_ptr;
	VALset(&a->data, a->data.vtype, &v);
	a->data.len = 0;
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
	case TYPE_lng:
		sprintf(buf, LLFMT, a->data.val.lval);
		break;
	case TYPE_wrd:
		sprintf(buf, SSZFMT, a->data.val.wval);
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
		if (a->data.val.cval[0])
			return sa_strdup(sa, "true");
		return sa_strdup(sa, "false");
	case TYPE_flt:
		sprintf(buf, "%f", a->data.val.fval);
		break;
	case TYPE_dbl:
		sprintf(buf, "%f", a->data.val.dval);
		break;
	case TYPE_str:
		if (a->data.val.sval)
			return sa_strdup(sa, a->data.val.sval);
		else
			sprintf(buf, "NULL");
		break;
        default:  
		v = &a->data.val.ival;
		if (ATOMvarsized(a->data.vtype))
			v = a->data.val.pval;
		if (ATOMformat(a->data.vtype, v, &p) < 0) {
                	snprintf(buf, BUFSIZ, "atom2string(TYPE_%d) not implemented", a->data.vtype);
		} else {
			 char *r = sa_strdup(sa, p);
			 _DELETE(p);
			 return r;
		}
	}
	return sa_strdup(sa, buf);
}

char *
atom2sql(atom *a)
{
	int ec = a->tpe.type->eclass;
	char buf[BUFSIZ];

	if (a->data.vtype == TYPE_str && ec == EC_INTERVAL)
		ec = EC_STRING; 
	/* todo handle NULL's early */
	switch (ec) {
	case EC_BIT:
		assert( a->data.vtype == TYPE_bit);
		if (a->data.val.cval[0])
			return _strdup("true");
		return _strdup("false");
	case EC_CHAR:
	case EC_STRING:
		assert (a->data.vtype == TYPE_str);
		if (a->data.val.sval)
			sprintf(buf, "'%s'", a->data.val.sval);
		else
			sprintf(buf, "NULL");
		break;
	case EC_BLOB:
		/* TODO atom to string */
		break;
	case EC_INTERVAL: {
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
		sprintf(buf, LLFMT, v);
		break;
	}
	case EC_NUM:
		switch (a->data.vtype) {
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
		lng v = 0;
		switch (a->data.vtype) {
		case TYPE_lng: v = a->data.val.lval; break;
		case TYPE_int: v = a->data.val.ival; break;
		case TYPE_sht: v = a->data.val.shval; break;
		case TYPE_bte: v = a->data.val.btval; break;
		default: break;
		}
		return decimal_to_str(v, &a->tpe);
	}
	case EC_FLT:
		if (a->data.vtype == TYPE_dbl)
			sprintf(buf, "%f", a->data.val.dval);
		else
			sprintf(buf, "%f", a->data.val.fval);
		break;
	case EC_TIME:
	case EC_DATE:
	case EC_TIMESTAMP:
		if (a->data.vtype == TYPE_str) {
			if (a->data.val.sval)
				sprintf(buf, "%s '%s'", a->tpe.type->sqlname, 
					a->data.val.sval);
			else
				sprintf(buf, "NULL");
		}
		break;
        default:
                snprintf(buf, BUFSIZ, "atom2sql(TYPE_%d) not implemented", a->data.vtype);
	}
	return _strdup(buf);
}


sql_subtype *
atom_type(atom *a)
{
	return &a->tpe;
}

atom *
atom_dup(sql_allocator *sa, atom *a)
{
	atom *r = atom_create(sa);

	*r = *a;
	r->tpe = a->tpe;
	if (!a->isnull) 
		SA_VALcopy(sa, &r->data, &a->data);
	return r;
}

unsigned int
atom_num_digits( atom *a ) 
{
	lng v = 0;
	int inlen = 1;

	switch(a->tpe.type->localtype) {
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
	default:
		return 64;
	}
	/* count the number of digits in the input */
	while (v /= 10)
		inlen++;
	return inlen;
}

static lng scales[] = {
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
/* cast atom a to type tp (success == 1, fail == 0) */
int 
atom_cast(atom *a, sql_subtype *tp) 
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
		if (at->type->eclass == EC_NUM && tp->type->eclass == EC_NUM &&
	    	    at->type->localtype <= tp->type->localtype) {
			/* cast numerics */
			switch( tp->type->localtype) {
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
#if SIZEOF_WRD == SIZEOF_INT
			case TYPE_wrd:
#endif
				if (at->type->localtype == TYPE_bte) 
					a->data.val.ival = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht) 
					a->data.val.ival = a->data.val.shval;
				else if (at->type->localtype != TYPE_int) 
					return 0;
				break;
			case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
			case TYPE_wrd:
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
			default:
				return 0;
			}
			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
			return 1;
		}
		if (at->type->eclass == EC_DEC && tp->type->eclass == EC_DEC &&
		    at->type->localtype <= tp->type->localtype &&
		    at->digits <= tp->digits /* &&
		    at->scale <= tp->scale*/) {
			lng mul = 1, div = 0, rnd = 0;
			/* cast numerics */
			switch( tp->type->localtype) {
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
#if SIZEOF_WRD == SIZEOF_INT
			case TYPE_wrd:
#endif
				if (at->type->localtype == TYPE_bte) 
					a->data.val.ival = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht) 
					a->data.val.ival = a->data.val.shval;
				else if (at->type->localtype != TYPE_int) 
					return 0;
				break;
			case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
			case TYPE_wrd:
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
			default:
				return 0;
			}
			/* fix scale */
			if (tp->scale >= at->scale) {
				mul = scales[tp->scale-at->scale];
			} else {
				/* only round when going to a lower scale */
				mul = scales[at->scale-tp->scale];
				rnd = mul>>1;
				div = 1;
			}
			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
			if (a->data.vtype == TYPE_lng) {
				a->data.val.lval += rnd;
				if (div)
					a->data.val.lval /= mul;
				else
					a->data.val.lval *= mul;
			} else if (a->data.vtype == TYPE_int) {
				assert(div || ((lng) GDK_int_min <= (lng) a->data.val.ival * mul && (lng) a->data.val.ival * mul <= (lng) GDK_int_max));
				a->data.val.ival += (int)rnd;
				if (div)
					a->data.val.ival /= (int) mul;
				else
					a->data.val.ival *= (int) mul;
			} else if (a->data.vtype == TYPE_sht) {
				assert(div || ((lng) GDK_sht_min <= (lng) a->data.val.shval * mul && (lng) a->data.val.shval * mul <= (lng) GDK_sht_max));
				a->data.val.shval += (sht)rnd;
				if (div)
					a->data.val.shval /= (sht) mul;
				else
					a->data.val.shval *= (sht) mul;
			} else if (a->data.vtype == TYPE_bte) {
				assert(div || ((lng) GDK_bte_min <= (lng) a->data.val.btval * mul && (lng) a->data.val.btval * mul <= (lng) GDK_bte_max));
				a->data.val.btval += (bte)rnd;
				if (div)
					a->data.val.btval /= (bte) mul;
				else
					a->data.val.btval *= (bte) mul;
			}
			return 1;
		}
		if (at->type->eclass == EC_NUM && tp->type->eclass == EC_DEC &&
		    at->type->localtype <= tp->type->localtype &&
		    (at->digits <= tp->digits || atom_num_digits(a) <= tp->digits) &&
		    at->scale <= tp->scale) {
			lng mul = 1;
			/* cast numerics */
			switch( tp->type->localtype) {
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
#if SIZEOF_WRD == SIZEOF_INT
			case TYPE_wrd:
#endif
				if (at->type->localtype == TYPE_bte) 
					a->data.val.ival = a->data.val.btval;
				else if (at->type->localtype == TYPE_sht) 
					a->data.val.ival = a->data.val.shval;
				else if (at->type->localtype != TYPE_int) 
					return 0;
				break;
			case TYPE_lng:
#if SIZEOF_WRD == SIZEOF_LNG
			case TYPE_wrd:
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
			default:
				return 0;
			}
			/* fix scale */
			mul = scales[tp->scale-at->scale];
			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
			if (a->data.vtype == TYPE_lng)
				a->data.val.lval *= mul;
			else if (a->data.vtype == TYPE_int) {
				assert((lng) GDK_int_min <= (lng) a->data.val.ival * mul && (lng) a->data.val.ival * mul <= (lng) GDK_int_max);
				a->data.val.ival *= (int) mul;
			}
			else if (a->data.vtype == TYPE_sht) {
				assert((lng) GDK_sht_min <= (lng) a->data.val.shval * mul && (lng) a->data.val.shval * mul <= (lng) GDK_sht_max);
				a->data.val.shval *= (sht) mul;
			}
			else if (a->data.vtype == TYPE_bte) {
				assert((lng) GDK_bte_min <= (lng) a->data.val.btval * mul && (lng) a->data.val.btval * mul <= (lng) GDK_bte_max);
				a->data.val.btval *= (bte) mul;
			}
			return 1;
		}
		if ((at->type->eclass == EC_DEC || 
		     at->type->eclass == EC_NUM) && 
		    tp->type->eclass == EC_FLT) {
			if (a->d == dbl_nil) {
				ptr p = &a->d;
				char *s;
				lng dec = 0;
				int len = 0, res = 0;
				/* cast decimals to doubles */
				switch( at->type->localtype) {
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
				default:
					return 0;
				}
				s = decimal_to_str(dec, at);
				len = sizeof(double);
				res = ATOMfromstr(TYPE_dbl, &p, &len, s);
				GDKfree(s);
				if (res <= 0)
					return 0;
			}
			if (tp->type->localtype == TYPE_dbl)
				a->data.val.dval = a->d;
			else {
				assert((dbl) GDK_flt_min <= a->d && a->d <= (dbl) GDK_flt_max);
				a->data.val.fval = (flt) a->d;
			}
			a->tpe = *tp;
			a->data.vtype = tp->type->localtype;
			return 1;
		}
	} else {
		ptr p = NULL;

		a->tpe = *tp;
		a->data.vtype = tp->type->localtype;
		p = ATOMnilptr(a->data.vtype);
		VALset(&a->data, a->data.vtype, p);
		return 1;
	}
	return 0;
}

void
atom_dump(atom *a, stream *s)
{
	if (!a->isnull && a->data.vtype == TYPE_str) {
		ATOMprint(a->data.vtype, VALget(&a->data), s);
	} else if (!a->isnull && ATOMstorage(a->data.vtype) == TYPE_str) {
		mnstr_write(s, a->tpe.type->base.name, strlen(a->tpe.type->base.name), 1);
		mnstr_write(s, "(", 1, 1);
		ATOMprint(a->data.vtype, VALget(&a->data), s);
		mnstr_write(s, ")", 1, 1);
	} else if (!a->isnull) {
		mnstr_write(s, a->tpe.type->base.name, strlen(a->tpe.type->base.name), 1);
		mnstr_write(s, "(\"", 2, 1);
		ATOMprint(a->data.vtype, VALget(&a->data), s);
		mnstr_write(s, "\")", 2, 1);
	} else {
		mnstr_write(s, a->tpe.type->base.name, strlen(a->tpe.type->base.name), 1);
		mnstr_write(s, "(nil)", 5, 1);
	}
}

int 
atom_neg( atom *a )
{
	switch( a->tpe.type->localtype) {
	case TYPE_bte:
		a->data.val.btval = -a->data.val.btval;
		break;
	case TYPE_sht:
		a->data.val.shval = -a->data.val.shval;
		break;
	case TYPE_int:
		a->data.val.ival = -a->data.val.ival;
		break;
	case TYPE_lng:
		a->data.val.lval = -a->data.val.lval;
		break;
	case TYPE_flt:
		a->data.val.fval = -a->data.val.fval;
		break;
	case TYPE_dbl:
		a->data.val.dval = -a->data.val.dval;
		break;
	default:
		return -1;
	}
	if (a->d != dbl_nil)
		a->d = -a->d;
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
