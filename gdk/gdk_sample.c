/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

#undef BATsample

#ifdef STATIC_CODE_ANALYSIS
#define DRAND (0.5)
#else
/* the range of rand() is [0..RAND_MAX], i.e. inclusive;
 * cast first, add later: on Linux RAND_MAX == INT_MAX, so adding 1
 * will overflow, but INT_MAX does fit in a double */
#if RAND_MAX < 46340	    /* 46340*46340 = 2147395600 < INT_MAX */
/* random range is too small, double it */
#define DRAND ((double)(rand() * (RAND_MAX + 1) + rand()) / ((double) ((RAND_MAX + 1) * (RAND_MAX + 1))))
#else
#define DRAND ((double)rand() / ((double)RAND_MAX + 1))
#endif
#endif


/* this is a straightforward implementation of a binary tree */
struct oidtreenode {
	oid o;
	struct oidtreenode *left;
	struct oidtreenode *right;
};

static int
OIDTreeMaybeInsert(struct oidtreenode *tree, oid o, BUN allocated)
{
	struct oidtreenode **nodep;

	if (allocated == 0) {
		tree->left = tree->right = NULL;
		tree->o = o;
		return 1;
	}
	nodep = &tree;
	while (*nodep) {
		if (o == (*nodep)->o)
			return 0;
		if (o < (*nodep)->o)
			nodep = &(*nodep)->left;
		else
			nodep = &(*nodep)->right;
	}
	*nodep = &tree[allocated];
	tree[allocated].left = tree[allocated].right = NULL;
	tree[allocated].o = o;
	return 1;
}

/* inorder traversal, gives us a sorted BAT */
static void
OIDTreeToBAT(struct oidtreenode *node, BAT *bat)
{
	if (node->left != NULL)
		OIDTreeToBAT(node->left, bat);
	((oid *) bat->theap.base)[bat->batCount++] = node->o;
	if (node->right != NULL )
		OIDTreeToBAT(node->right, bat);
}

/* Antiset traversal, give us all values but the ones in the tree */
static void
OIDTreeToBATAntiset(struct oidtreenode *node, BAT *bat, oid start, oid stop)
{
	oid noid;

	if (node->left != NULL)
        	OIDTreeToBATAntiset(node->left, bat, start, node->o);
	else
		for (noid = start; noid < node->o; noid++)
			((oid *) bat->theap.base)[bat->batCount++] = noid;

        if (node->right != NULL)
 		OIDTreeToBATAntiset(node->right, bat, node->o + 1, stop);
	else
		for (noid = node->o+1; noid < stop; noid++)
                        ((oid *) bat->theap.base)[bat->batCount++] = noid;
}

/* BATsample implements sampling for void headed BATs */
BAT *
BATsample(BAT *b, BUN n)
{
	BAT *bn;
	BUN cnt, slen;
	BUN rescnt;
	struct oidtreenode *tree = NULL;

	BATcheck(b, "BATsample", NULL);
	ERRORcheck(n > BUN_MAX, "BATsample: sample size larger than BUN_MAX\n", NULL);
	ALGODEBUG
		fprintf(stderr, "#BATsample: sample " BUNFMT " elements.\n", n);

	cnt = BATcount(b);
	/* empty sample size */
	if (n == 0) {
		bn = BATdense(0, 0, 0);
		if (bn == NULL) {
			return NULL;
		}
	/* sample size is larger than the input BAT, return all oids */
	} else if (cnt <= n) {
		bn = BATdense(0, b->hseqbase, cnt);
		if (bn == NULL) {
			return NULL;
		}
	} else {
		oid minoid = b->hseqbase;
		oid maxoid = b->hseqbase + cnt;
		/* if someone samples more than half of our tree, we
		 * do the antiset */
		bit antiset = n > cnt / 2;
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
		/* while we do not have enough sample OIDs yet */
		for (rescnt = 0; rescnt < n; rescnt++) {
			oid candoid;
			do {
				/* generate a new random OID */
				candoid = (oid) (minoid + DRAND * (maxoid - minoid));
				/* if that candidate OID was already
				 * generated, try again */
			} while (!OIDTreeMaybeInsert(tree, candoid, rescnt));
		}
		if (!antiset) {
			OIDTreeToBAT(tree, bn);
		} else {
			OIDTreeToBATAntiset(tree, bn, minoid, maxoid);
		}
		GDKfree(tree);

		BATsetcount(bn, slen);
		bn->trevsorted = bn->batCount <= 1;
		bn->tsorted = 1;
		bn->tkey = 1;
		bn->tdense = bn->batCount <= 1;
		if (bn->batCount == 1)
			bn->tseqbase = *(oid *) Tloc(bn, 0);
	}
	return bn;
}
