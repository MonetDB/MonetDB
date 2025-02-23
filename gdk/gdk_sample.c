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

/*
 * @a Lefteris Sidirourgos, Hannes Muehleisen
 * @* Low level sample facilities
 *
 * This sampling implementation generates a sorted set of OIDs by
 * calling the random number generator, and uses a binary tree to
 * eliminate duplicates.  The elements of the tree are then used to
 * create a sorted sample BAT.  This implementation has a logarithmic
 * complexity that only depends on the sample size.
 *
 * There is a pathological case when the sample size is almost the
 * size of the BAT.  Then, many collisions occur and performance
 * degrades. To catch this, we switch to antiset semantics when the
 * sample size is larger than half the BAT size. Then, we generate the
 * values that should be omitted from the sample.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "xoshiro256starstar.h"

/* this is a straightforward implementation of a binary tree */
struct oidtreenode {
	union {
		struct {	/* use as a binary tree */
			oid o;
			struct oidtreenode *left;
			struct oidtreenode *right;
		};
		uint64_t r;	/* temporary storage for random numbers */
	};
};

static bool
OIDTreeMaybeInsert(struct oidtreenode *tree, oid o, BUN allocated)
{
	struct oidtreenode **nodep;

	if (allocated == 0) {
		tree->left = tree->right = NULL;
		tree->o = o;
		return true;
	}
	nodep = &tree;
	while (*nodep) {
		if (o == (*nodep)->o)
			return false;
		if (o < (*nodep)->o)
			nodep = &(*nodep)->left;
		else
			nodep = &(*nodep)->right;
	}
	*nodep = &tree[allocated];
	tree[allocated].left = tree[allocated].right = NULL;
	tree[allocated].o = o;
	return true;
}

/* inorder traversal, gives us a sorted BAT */
static void
OIDTreeToBAT(struct oidtreenode *node, BAT *bn)
{
	if (node->left != NULL)
		OIDTreeToBAT(node->left, bn);
	((oid *) bn->theap->base)[bn->batCount++] = node->o;
	if (node->right != NULL )
		OIDTreeToBAT(node->right, bn);
}

/* Antiset traversal, give us all values but the ones in the tree */
static void
OIDTreeToBATAntiset(struct oidtreenode *node, BAT *bn, oid start, oid stop)
{
	oid noid;

	if (node->left != NULL)
		OIDTreeToBATAntiset(node->left, bn, start, node->o);
	else
		for (noid = start; noid < node->o; noid++)
			((oid *) bn->theap->base)[bn->batCount++] = noid;

	if (node->right != NULL)
		OIDTreeToBATAntiset(node->right, bn, node->o + 1, stop);
	else
		for (noid = node->o+1; noid < stop; noid++)
			((oid *) bn->theap->base)[bn->batCount++] = noid;
}

static BAT *
do_batsample(oid hseq, BUN cnt, BUN n, random_state_engine rse, MT_Lock *lock)
{
	BAT *bn;
	BUN slen;
	BUN rescnt;
	struct oidtreenode *tree = NULL;

	ERRORcheck(n > BUN_MAX, "sample size larger than BUN_MAX\n", NULL);
	/* empty sample size */
	if (n == 0) {
		bn = BATdense(0, 0, 0);
	} else if (cnt <= n) {
		/* sample size is larger than the input BAT, return
		 * all oids */
		bn = BATdense(0, hseq, cnt);
	} else {
		oid minoid = hseq;
		oid maxoid = hseq + cnt;

		/* if someone samples more than half of our tree, we
		 * do the antiset */
		bool antiset = n > cnt / 2;
		slen = n;
		if (antiset)
			n = cnt - n;

		tree = GDKmalloc(n * sizeof(struct oidtreenode));
		if (tree == NULL) {
			return NULL;
		}
		bn = COLnew(0, TYPE_oid, slen, TRANSIENT);
		if (bn == NULL) {
			GDKfree(tree);
			return NULL;
		}

		if (lock)
			MT_lock_set(lock);
		/* generate a list of random numbers; note we use the
		 * "tree" array, but we use the value from each location
		 * before it is overwritten by the use as part of the
		 * binary tree */
		for (rescnt = 0; rescnt < n; rescnt++)
			tree[rescnt].r = next(rse);

		/* while we do not have enough sample OIDs yet */
		BUN rnd = 0;
		for (rescnt = 0; rescnt < n; rescnt++) {
			oid candoid;
			do {
				if (rnd == n) {
					/* we ran out of random numbers,
					 * so generate more */
					for (rnd = rescnt; rnd < n; rnd++)
						tree[rnd].r = next(rse);
					rnd = rescnt;
				}
				candoid = minoid + tree[rnd++].r % cnt;
				/* if that candidate OID was already
				 * generated, try again */
			} while (!OIDTreeMaybeInsert(tree, candoid, rescnt));
		}
		if (lock)
			MT_lock_unset(lock);
		if (!antiset) {
			OIDTreeToBAT(tree, bn);
		} else {
			OIDTreeToBATAntiset(tree, bn, minoid, maxoid);
		}
		GDKfree(tree);

		BATsetcount(bn, slen);
		bn->trevsorted = bn->batCount <= 1;
		bn->tsorted = true;
		bn->tkey = true;
		bn->tseqbase = bn->batCount == 0 ? 0 : bn->batCount == 1 ? *(oid *) Tloc(bn, 0) : oid_nil;
	}
	return bn;
}

/* BATsample implements sampling for BATs */
BAT *
BATsample_with_seed(BAT *b, BUN n, uint64_t seed)
{
	random_state_engine rse;

	init_random_state_engine(rse, seed);

	BAT *bn = do_batsample(b->hseqbase, BATcount(b), n, rse, NULL);
	TRC_DEBUG(ALGO, ALGOBATFMT "," BUNFMT " -> " ALGOOPTBATFMT "\n",
		  ALGOBATPAR(b), n, ALGOOPTBATPAR(bn));
	return bn;
}

static MT_Lock rse_lock = MT_LOCK_INITIALIZER(rse_lock);
BAT *
BATsample(BAT *b, BUN n)
{
	static random_state_engine rse;

	MT_lock_set(&b->theaplock);
	BUN batcount = BATcount(b);
	MT_lock_unset(&b->theaplock);
	MT_lock_set(&rse_lock);
	if (rse[0] == 0 && rse[1] == 0 && rse[2] == 0 && rse[3] == 0)
		init_random_state_engine(rse, (uint64_t) GDKusec());
	MT_lock_unset(&rse_lock);
	BAT *bn = do_batsample(b->hseqbase, batcount, n, rse, &rse_lock);
	TRC_DEBUG(ALGO, ALGOBATFMT "," BUNFMT " -> " ALGOOPTBATFMT "\n",
		  ALGOBATPAR(b), n, ALGOOPTBATPAR(bn));
	return bn;
}
