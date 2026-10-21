/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
	oid o;
	struct oidtreenode *left;
	struct oidtreenode *right;
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
OIDTreeToBAT(struct oidtreenode *node, oid *oids, BUN *n)
{
	if (node->left != NULL)
		OIDTreeToBAT(node->left, oids, n);
	oids[*n] = node->o;
	(*n)++;
	if (node->right != NULL )
		OIDTreeToBAT(node->right, oids, n);
}

static BAT *
do_batsample(oid hseq, BUN cnt, BUN n, random_state_engine rse)
{
	BAT *bn;
	BUN rescnt;		/* counter used for several purposes */

	ERRORcheck(n > BUN_MAX, "sample size larger than BUN_MAX\n", NULL);
	if (n == 0) {
		/* empty sample size */
		return BATdense(0, 0, 0);
	}
	if (cnt <= n) {
		/* sample size is larger than the input BAT, return
		 * all oids */
		return BATdense(0, hseq, cnt);
	}
	bool antiset = n > cnt / 2;
	if (antiset)
		n = cnt - n;
	/* temp use: approximate size of mask if we were to create one */
	rescnt = cnt / 8;
	/* n * sizeof(oid) is size of BAT if we were to not use a mask */
	if (n * sizeof(oid) > rescnt) {
		/* create a mask candidate list since it is smaller than
		 * a list of oids */
		bn = COLnew(0, TYPE_void, 0, TRANSIENT);
		Heap *msks = GDKmalloc(sizeof(Heap));
		if (bn == NULL || msks == NULL) {
			GDKfree(msks);
			BBPreclaim(bn);
			return NULL;
		}
		*msks = (Heap) {
			.farmid = BBPselectfarm(TRANSIENT, TYPE_void, varheap),
			.role = TRANSIENT,
			.parentid = bn->batCacheid,
			.dirty = true,
			.refs = ATOMIC_VAR_INIT(1),
		};
		strtconcat(msks->filename, sizeof(msks->filename),
			   BBP_physical(bn->batCacheid), ".theap", NULL);
		BUN nmask = (cnt + 31) / 32;
		if (msks->farmid < 0 ||
		    HEAPalloc(msks,
			      nmask + sizeof(ccand_t) / sizeof(uint32_t),
			      sizeof(uint32_t)) != GDK_SUCCEED) {
			GDKfree(msks);
			BBPreclaim(bn);
			return NULL;
		}
		ccand_t *c = (ccand_t *) msks->base;
		*c = (ccand_t) {
			.type = CAND_MSK,
			.firstbit = cnt, /* too large, will be reduced */
		};
		msks->free = sizeof(ccand_t) + nmask * sizeof(uint32_t);
		uint32_t *r = (uint32_t *) (msks->base + sizeof(ccand_t));
		BATsetcount(bn, antiset ? cnt - n : n);
		bn->tunique_est = (double) bn->batCount;
		if (!antiset) {
			memset(r, 0, nmask * sizeof(uint32_t));
			for (rescnt = 0; rescnt < n; rescnt++) {
				BUN i;
				do {
					i = next(rse) % cnt;
				} while ((r[i / 32] & (1U << (i % 32))) != 0);
				r[i / 32] |= 1U << (i % 32);
				if (i < c->firstbit)
					c->firstbit = i;
			}
		} else {
			memset(r, 0xFF, nmask * sizeof(uint32_t));
			/* clear last few bits */
			if (cnt % 32 != 0)
				r[nmask - 1] &= (1U << (cnt % 32)) - 1;
			for (rescnt = 0; rescnt < n; rescnt++) {
				BUN i;
				do {
					i = next(rse) % cnt;
				} while ((r[i / 32] & (1U << (i % 32))) == 0);
				r[i / 32] &= ~(1U << (i % 32));
			}
			for (rescnt = 0; rescnt < nmask; rescnt++) {
				if (r[rescnt] != 0) {
					c->firstbit = candmask_lobit(r[rescnt]) + rescnt * 32;
					break;
				}
			}
		}
		bn->tvheap = msks;
		BATtseqbase(bn, (oid) (hseq + c->firstbit));
		return bn;
	}
	oid *oids;
	if (antiset) {
		/* create negative candidate list */
		bn = COLnew(0, TYPE_void, 0, TRANSIENT);
		Heap *dels = GDKmalloc(sizeof(Heap));
		if (bn == NULL || dels == NULL) {
			GDKfree(dels);
			BBPreclaim(bn);
			return NULL;
		}
		*dels = (Heap) {
			.farmid = BBPselectfarm(TRANSIENT, TYPE_void, varheap),
			.role = TRANSIENT,
			.parentid = bn->batCacheid,
			.dirty = true,
			.refs = ATOMIC_VAR_INIT(1),
		};
		strtconcat(dels->filename, sizeof(dels->filename),
			   BBP_physical(bn->batCacheid), ".theap", NULL);
		if (dels->farmid < 0 ||
		    HEAPalloc(dels, n + sizeof(ccand_t) / sizeof(oid),
			      sizeof(oid)) != GDK_SUCCEED) {
			GDKfree(dels);
			BBPreclaim(bn);
			return NULL;
		}
		ccand_t *c = (ccand_t *) dels->base;
		*c = (ccand_t) {
			.type = CAND_NEGOID,
		};
		dels->free = sizeof(ccand_t) + n * sizeof(oid);
		bn->tvheap = dels;
		oids = (oid *) ccand_first(bn);
		BATsetcount(bn, cnt - n); /* ??? */
		BATtseqbase(bn, hseq);
	} else {
		/* create a positive candidate list */
		bn = COLnew(0, TYPE_oid, n, TRANSIENT);
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, n);
		oids = (oid *) Tloc(bn, 0);
		bn->trevsorted = bn->batCount <= 1;
		bn->tsorted = true;
		bn->tkey = true;
		bn->tseqbase = bn->batCount == 0 ? 0 : bn->batCount == 1 ? *(oid *) Tloc(bn, 0) : oid_nil;
	}

	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);

	struct oidtreenode *tree = ma_alloc(ta, n * sizeof(struct oidtreenode));
	if (tree == NULL) {
		ma_close(&ta_state);
		return NULL;
	}

	/* while we do not have enough sample OIDs yet */
	for (rescnt = 0; rescnt < n; rescnt++) {
		oid candoid;
		do {
			candoid = hseq + next(rse) % cnt;
			/* if that candidate OID was already generated,
			 * try again */
		} while (!OIDTreeMaybeInsert(tree, candoid, rescnt));
	}
	rescnt = 0;
	OIDTreeToBAT(tree, oids, &rescnt);
	ma_close(&ta_state);

	return bn;
}

/* BATsample implements sampling for BATs */
BAT *
BATsample_with_seed(BAT *b, BUN n, uint64_t seed)
{
	random_state_engine rse;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	init_random_state_engine(rse, seed);

	BAT *bn = do_batsample(b->hseqbase, BATcount(b), n, rse);
	TRC_DEBUG(ALGO, ALGOBATFMT "," BUNFMT " -> " ALGOOPTBATFMT
		  " (" LLFMT " usec)\n",
		  ALGOBATPAR(b), n, ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;
}

BAT *
BATsample(BAT *b, BUN n)
{
	return BATsample_with_seed(b, n, (uint64_t) GDKusec());
}
