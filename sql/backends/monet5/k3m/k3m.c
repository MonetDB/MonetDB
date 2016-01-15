/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _K3M_LIB_
#define _K3M_LIB_

#include "monetdb_config.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include <math.h>

#include "k3match.h"

#ifdef WIN32
#define k3m_export extern __declspec(dllexport)
#else
#define k3m_export extern
#endif

k3m_export str K3Mprelude(void *ret);
k3m_export str K3Mbuild(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
		 InstrPtr pci);
k3m_export str K3Mfree(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
		 InstrPtr pci);
k3m_export str K3Mquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
		 InstrPtr pci);

typedef struct {
	node_t *tree;
	real_t *values;
	point_t **catalog;
} k3m_tree_tpe;

static k3m_tree_tpe *k3m_tree = NULL;
static MT_Lock k3m_lock;

k3m_export str K3Mprelude(void *ret) {
	(void) ret;
	MT_lock_init(&k3m_lock, "k3m_lock");
	return MAL_SUCCEED;
}

str K3Mbuild(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	BAT *ids, *ra, *dec, *ret;
	int *ids_a;
	dbl *ra_a, *dec_a;
	size_t N_a;
	int_t i;
	int_t npool = 0;
	bit b = bit_nil;

	K3Mfree(cntxt, mb, stk, pci);
	MT_lock_set(&k3m_lock);
	k3m_tree = GDKmalloc(sizeof(k3m_tree_tpe));
	assert(k3m_tree);

	if (!isaBatType(getArgType(mb,pci,0)) || !isaBatType(getArgType(mb,pci,1)) ||
			!isaBatType(getArgType(mb,pci,2)) || !isaBatType(getArgType(mb,pci,3))) {
		return createException(MAL, "k3m.build", "Can only deal with BAT types. Sorry.");
	}
	for (i = 0; i <= 3; i++) {
		if (!isaBatType(getArgType(mb,pci,i))) {
			return createException(MAL, "k3m.build", "Can only deal with BAT types. Sorry.");
		}
	}
	ids = BATdescriptor(*getArgReference_bat(stk, pci, 1));
	ra = BATdescriptor(*getArgReference_bat(stk, pci, 2));
	dec = BATdescriptor(*getArgReference_bat(stk, pci, 3));

	N_a = BATcount(ids);
	assert(ids && ra && dec); // TODO: dynamic checks & errors instead of asserts

	ids_a = ((int*) Tloc(ids, BUNfirst(ids)));
	ra_a  = ((dbl*) Tloc(ra, BUNfirst(ra)));
	dec_a = ((dbl*) Tloc(dec, BUNfirst(dec)));

	assert(ids_a && ra_a && dec_a); // TODO: dynamic checks & errors instead of asserts

	k3m_tree->values = GDKmalloc(3 * N_a * sizeof(real_t));
	k3m_tree->catalog = GDKmalloc(N_a * sizeof(point_t*));
	*k3m_tree->catalog = GDKmalloc(N_a * sizeof(point_t));
	k3m_tree->tree = (node_t*) GDKmalloc(N_a * sizeof(node_t));

	if (!k3m_tree->values || !k3m_tree->catalog || !*k3m_tree->catalog || !k3m_tree) {
		if (k3m_tree->values) {
			GDKfree(k3m_tree->values);
		}
		if (k3m_tree->catalog) {
			GDKfree(k3m_tree->catalog);
		}
		if (*k3m_tree->catalog) {
			GDKfree(*k3m_tree->catalog);
		}
		if (k3m_tree->tree) {
			GDKfree(k3m_tree->tree);
		}
		return createException(MAL, "k3m.build", "Memory allocation failed.");
	}

	for (i=0; i<N_a; i++) {
		k3m_tree->catalog[i] = k3m_tree->catalog[0] + i;
		k3m_tree->catalog[i]->id = ids_a[i];
		k3m_tree->catalog[i]->value = k3m_tree->values + 3 * i;
		k3m_tree->catalog[i]->value[0] = cos(dec_a[i]) * cos(ra_a[i]);
		k3m_tree->catalog[i]->value[1] = cos(dec_a[i]) * sin(ra_a[i]);
		k3m_tree->catalog[i]->value[2] = sin(dec_a[i]);
	}

	k3m_tree->tree->parent = NULL;
	k3m_build_balanced_tree(k3m_tree->tree, k3m_tree->catalog, N_a, 0, &npool);

	ret = BATnew(TYPE_void, TYPE_bit, 0, TRANSIENT);
	BUNappend(ret, &b, 0);
	*getArgReference_bat(stk, pci, 0) = ret->batCacheid;
	BBPkeepref(ret->batCacheid);
	return MAL_SUCCEED;

}

str K3Mfree(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	BAT *ret;
	bit b = bit_nil;

	MT_lock_set(&k3m_lock);
	if (k3m_tree) {
		GDKfree(k3m_tree->tree);
		GDKfree(k3m_tree->values);
		//GDKfree(*k3m_tree->catalog); // TODO: why does this not work?
		GDKfree(k3m_tree->catalog);
		k3m_tree = NULL;
	}
	MT_lock_unset(&k3m_lock);

	ret = BATnew(TYPE_void, TYPE_bit, 0, TRANSIENT);
	BUNappend(ret, &b, 0);
	*getArgReference_bat(stk, pci, 0) = ret->batCacheid;
	BBPkeepref(ret->batCacheid);

	return MAL_SUCCEED;
}

str K3Mquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	(void) cntxt;
	size_t i;
	BAT *in_ids, *in_ra, *in_dec, *in_dist, *out_id_cat, *out_id_sl, *out_dist;
	int *in_ids_a;
	dbl *in_ra_a, *in_dec_a, *in_dist_a;
	point_t search, *match;
	size_t N_b;
	point_t *mi = NULL;
	int_t nmatch = 0;


	for (i = 0; i <= 6; i++) {
		if (!isaBatType(getArgType(mb,pci,i))) {
			return createException(MAL, "k3m.build", "Can only deal with BAT types. Sorry.");
		}
	}

	if (!k3m_tree) {
		return createException(MAL, "k3m.build", "Tree not built!");
	}

	in_ids     = BATdescriptor(*getArgReference_bat(stk, pci, 3));
	in_ra      = BATdescriptor(*getArgReference_bat(stk, pci, 4));
	in_dec     = BATdescriptor(*getArgReference_bat(stk, pci, 5));
	in_dist    = BATdescriptor(*getArgReference_bat(stk, pci, 6));

	assert(in_ids && in_ra && in_dec && in_dist);

	in_ids_a = ((int*) Tloc(in_ids, BUNfirst(in_ids)));
	in_ra_a  = ((dbl*) Tloc(in_ra, BUNfirst(in_ra)));
	in_dec_a = ((dbl*) Tloc(in_dec, BUNfirst(in_dec)));
	in_dist_a = ((dbl*) Tloc(in_dist, BUNfirst(in_dist)));

	assert(in_ids_a && in_ra_a && in_dec_a && in_dist_a);

	out_id_cat = BATnew(TYPE_void, TYPE_int, 0, TRANSIENT);
	out_id_sl  = BATnew(TYPE_void, TYPE_int, 0, TRANSIENT);
	out_dist   = BATnew(TYPE_void, TYPE_dbl, 0, TRANSIENT);

	N_b = BATcount(in_ids);

	search.value = GDKmalloc(3 * sizeof(real_t));

	for (i=0; i<N_b; i++) {
		search.id = in_ids_a[i];
		search.value[0] = cos(in_dec_a[i]) * cos(in_ra_a[i]);
		search.value[1] = cos(in_dec_a[i]) * sin(in_ra_a[i]);
		search.value[2] = sin(in_dec_a[i]);

		match = NULL;
		nmatch = k3m_in_range(k3m_tree->tree, &match, &search, in_dist_a[i]);

		mi = match;
		nmatch++;
		while (--nmatch) {
			BUNappend(out_id_sl, &search.id, 0);
			BUNappend(out_id_cat, &mi->id, 0);
			BUNappend(out_dist, &mi->ds, 0);
			mi = mi->neighbour;
		}
	}
	GDKfree(search.value);
	MT_lock_unset(&k3m_lock);

	BBPkeepref(out_id_cat->batCacheid);
	BBPkeepref(out_id_sl->batCacheid);
	BBPkeepref(out_dist->batCacheid);

	*getArgReference_bat(stk, pci, 0) = out_id_cat->batCacheid;
	*getArgReference_bat(stk, pci, 1) = out_id_sl->batCacheid;
	*getArgReference_bat(stk, pci, 2) = out_dist->batCacheid;

	return MAL_SUCCEED;
}

#endif /* _K3M_LIB_ */
