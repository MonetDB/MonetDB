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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a Martin L. Kersten & Peter Boncz
 * @v 2.0
 * @+ Value representation
 *
 *
 * When manipulating values, MonetDB puts them into value records.
 * The built-in types have a direct entry in the union. Others should
 * be represented as a pointer of memory in pval or as a string, which
 * is basically the same. In such cases the len field indicates the
 * size of this piece of memory.
 *
 * MonetDB extenders will use value records for passing parameters to
 * their new operators. MonetDB algebraic commands receive an (argc,
 * argv) combination, where argc is an integer indicating the size of
 * the the argv array of value records. On call, the first record,
 * argv[0], is always empty. The routine must place its return value -
 * if any - there. The other values are the parameters.
 *
 * Actually, the gdk value type defined here should become a built-in
 * type in the kernel. Next step will be to define the corresponding
 * extension module.
 *
 * @+ Value operations
 * The following primitives are required to manipulate value records.
 * Note that binding a BAT requires upgrading its reference count.
 * The receiver of the value should have been cleared or represent
 * free space.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

ValPtr
VALset(ValPtr v, int t, ptr p)
{
	switch (ATOMstorage(v->vtype = t)) {
	case TYPE_void:
		v->val.oval = *(oid *) p;
		break;
	case TYPE_bte:
		v->val.btval = *(bte *) p;
		break;
	case TYPE_sht:
		v->val.shval = *(sht *) p;
		break;
	case TYPE_int:
		v->val.ival = *(int *) p;
		break;
	case TYPE_flt:
		v->val.fval = *(flt *) p;
		break;
	case TYPE_dbl:
		v->val.dval = *(dbl *) p;
		break;
	case TYPE_lng:
		v->val.lval = *(lng *) p;
		break;
	case TYPE_str:
		v->val.sval = (str) p;
		v->len = ATOMlen(t, p);
		break;
	case TYPE_ptr:
		v->val.pval = *(ptr *) p;
		v->len = ATOMlen(t, *(ptr *) p);
		break;
	default:
		v->val.pval = p;
		v->len = ATOMlen(t, p);
		break;
	}
	return v;
}

/* also see VALptr */
void *
VALget(ValPtr v)
{
	switch (ATOMstorage(v->vtype)) {
	case TYPE_void: return (void *) &v->val.oval;
	case TYPE_bte: return (void *) &v->val.btval;
	case TYPE_sht: return (void *) &v->val.shval;
	case TYPE_int: return (void *) &v->val.ival;
	case TYPE_flt: return (void *) &v->val.fval;
	case TYPE_dbl: return (void *) &v->val.dval;
	case TYPE_lng: return (void *) &v->val.lval;
	case TYPE_str: return (void *) v->val.sval;
	default:       return (void *) v->val.pval;
	}
}

void
VALclear(ValPtr v)
{
	if (v->vtype == TYPE_str || ATOMextern(v->vtype)) {
		if (v->val.pval && v->val.pval != str_nil)
			GDKfree(v->val.pval);
	}
	VALempty(v);
}

void
VALempty(ValPtr v)
{
	v->len = 0;
	v->val.oval = oid_nil;
	v->vtype = TYPE_void;
}

ValPtr
VALcopy(ValPtr d, const ValRecord *s)
{
	if (!ATOMextern(s->vtype)) {
		*d = *s;
	} else if (s->val.pval == 0) {
		d->val.pval = ATOMnil(s->vtype);
		d->vtype = s->vtype;
	} else if (s->vtype == TYPE_str) {
		d->vtype = TYPE_str;
		d->val.sval = GDKstrdup(s->val.sval);
		d->len = strLen(d->val.sval);
	} else if (s->vtype == TYPE_bit) {
		d->vtype = s->vtype;
		d->len = 1;
		d->val.btval = s->val.btval;
	} else {
		ptr p = s->val.pval;

		d->vtype = s->vtype;
		d->len = ATOMlen(d->vtype, p);
		d->val.pval = GDKmalloc(d->len);
		memcpy(d->val.pval, p, d->len);
	}
	return d;
}

ValPtr
VALinit(ValPtr d, int tpe, const void *s)
{
	if (ATOMextern(tpe) == 0) {
		d->vtype = tpe;
		memcpy(&d->val.ival, s, ATOMlen(tpe, s));
	} else if (s == 0) {
		GDKerror("VALinit:unsupported init\n");
		d->vtype = TYPE_int;
	} else if (tpe >= TYPE_str && ATOMstorage(tpe) == TYPE_str) {
		d->vtype = TYPE_str;
		d->val.sval = GDKstrdup(s);
		d->len = strLen(s);
	} else {
		d->vtype = tpe;
		d->len = ATOMlen(tpe, s);
		d->val.pval = GDKmalloc(d->len);
		memcpy(d->val.pval, s, d->len);
	}
	return d;
}

int
VALformat(char **buf, const ValRecord *res)
{
	int t = res->vtype;

	*buf = 0;
	return ATOMformat(t, VALptr(res), buf);
}

/*
 * The routine VALconvert transforms a value for interpretation in a
 * certain type. It uses some standard cast conventions to do this.
 * The result, a pointer to a value, is returned. If there are illegal
 * values, or type combinations involved, it gives up with an
 * ILLEGALVALUE.
 */
ptr
VALconvert(int typ, ValPtr t)
{
	int src_tpe = t->vtype;
	ValRecord dst;

	dst.vtype = typ;
	/* use base types for user types */
	if (src_tpe > TYPE_str)
		src_tpe = ATOMstorage(src_tpe);
	if (dst.vtype > TYPE_str)
		dst.vtype = ATOMstorage(dst.vtype);
	else if (dst.vtype == TYPE_void)
		dst.vtype = TYPE_oid;

	/* first convert into a new location */
	if (VARconvert(&dst, t, 0) == GDK_FAIL)
		return ILLEGALVALUE;

	/* then maybe free the old */
	if (src_tpe != dst.vtype &&
	    t->vtype != typ &&
	    dst.vtype != TYPE_void &&
	    (src_tpe >= TYPE_str || dst.vtype >= TYPE_str))
		VALclear(t);
	/* and finally copy the result */
	*t = dst;
	/* make sure we return the correct type (not the storage type) */
	t->vtype = typ;
	return VALget(t);
}

int
VALcmp(const ValRecord *p, const ValRecord *q)
{

	int (*cmp)(const void *, const void *);
	int tpe;
	const void *nilptr, *pp, *pq;

	if (p == 0 || q == 0)
		return -1;
	if ((tpe = p->vtype) != q->vtype)
		return -1;

	if (tpe == TYPE_ptr)
		return 0;	/* ignore comparing C pointers */
	cmp = BATatoms[tpe].atomCmp;
	nilptr = ATOMnilptr(tpe);
	pp = VALptr(p);
	pq = VALptr(q);
	if ((*cmp)(pp, nilptr) == 0 && (*cmp)(pq, nilptr) == 0)
		return 0;	/* eq nil val */
	if ((*cmp)(pp, nilptr) == 0 || (*cmp)(pq, nilptr) == 0)
		return -1;
	return (*cmp)(pp, pq);

}

int
VALisnil(const ValRecord *v)
{
	switch (v->vtype) {
	case TYPE_void:
		return 1;
	case TYPE_bte:
		return v->val.btval == bte_nil;
	case TYPE_sht:
		return v->val.shval == sht_nil;
	case TYPE_int:
	case TYPE_bat:
		return v->val.ival == int_nil;
	case TYPE_lng:
		return v->val.lval == lng_nil;
	case TYPE_flt:
		return v->val.fval == flt_nil;
	case TYPE_dbl:
		return v->val.dval == dbl_nil;
	default:
		break;
	}
	return (*BATatoms[v->vtype].atomCmp)(VALptr(v), ATOMnilptr(v->vtype)) == 0;
}
