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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a Lefteris Sidirourgos, Hannes Muehleisen
 * @* Low level sample facilities
 *
 * This sampling implementation generates a sorted set of OIDs by calling the
 * random number generator, and uses a binary tree to eliminate duplicates.
 * The elements of the tree are then used to create a sorted sample BAT.
 * This implementation has a logarithmic complexity that only depends on the
 * sample size.
 *
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#undef BATsample

#define DRAND ((double)rand()/(double)RAND_MAX)

/* this is a straightforward implementation of a binary tree */
struct oidtreenode {
	BUN oid;
	struct oidtreenode* left;
	struct oidtreenode* right;
};

static int OIDTreeLookup(struct oidtreenode* node, BUN target) {
	if (node == NULL) {
		return (FALSE);
	} else {
		if (target == node->oid)
			return (TRUE);
		else {
			if (target < node->oid)
				return (OIDTreeLookup(node->left, target));
			else
				return (OIDTreeLookup(node->right, target));
		}
	}
}

static struct oidtreenode* OIDTreeNew(BUN oid) {
	struct oidtreenode *node = malloc(sizeof(struct oidtreenode));
	if (node == NULL) {
		GDKerror("#BATsample: memory allocation error");
		return NULL ;
	}
	node->oid = oid;
	node->left = NULL;
	node->right = NULL;
	return (node);
}

static struct oidtreenode* OIDTreeInsert(struct oidtreenode* node, BUN oid) {
	if (node == NULL) {
		return (OIDTreeNew(oid));
	} else {
		if (oid <= node->oid)
			node->left = OIDTreeInsert(node->left, oid);
		else
			node->right = OIDTreeInsert(node->right, oid);
		return (node);
	}
}

/* inorder traversal, gives us a sorted BAT */
static void OIDTreeToBAT(struct oidtreenode* node, BAT *bat) {
	if (node->left != NULL)
		OIDTreeToBAT(node->left, bat);
	((oid *) bat->T->heap.base)[bat->batFirst + bat->batCount++] = node->oid;
	if (node->right != NULL )
		OIDTreeToBAT(node->right, bat);
}

static void OIDTreeDestroy(struct oidtreenode* node) {
	if (node == NULL) {
		return;
	}
	if (node->left != NULL) {
		OIDTreeDestroy(node->left);
	}
	if (node->right != NULL) {
		OIDTreeDestroy(node->right);
	}
	free(node);
}


/* BATsample implements sampling for void headed BATs */
BAT *
BATsample(BAT *b, BUN n) {
	BAT *bn;
	BUN cnt;
	BUN rescnt = 0;
	struct oidtreenode* tree = NULL;

	BATcheck(b, "BATsample");
	assert(BAThdense(b));
	ERRORcheck(n > BUN_MAX, "BATsample: sample size larger than BUN_MAX\n");
	ALGODEBUG
		fprintf(stderr, "#BATsample: sample " BUNFMT " elements.\n", n);

	cnt = BATcount(b);
	/* empty sample size */
	if (n == 0) {
		bn = BATnew(TYPE_void, TYPE_void, 0);
		BATsetcount(bn, 0);
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), 0);
		/* sample size is larger than the input BAT, return all oids */
	} else if (cnt <= n) {
		bn = BATnew(TYPE_void, TYPE_void, cnt);
		BATsetcount(bn, cnt);
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), b->H->seq);
	} else {
		BUN minoid = b->hseqbase;
		BUN maxoid = b->hseqbase + cnt;
		//oid *o;
		bn = BATnew(TYPE_void, TYPE_oid, n);

		if (bn == NULL ) {
			GDKerror("#BATsample: memory allocation error");
			return NULL;
		}
		/* while we do not have enough sample OIDs yet */
		while (rescnt < n) {
			BUN candoid;
			struct oidtreenode* ttree;
			do {
				/* generate a new random OID */
				candoid = minoid + DRAND * (maxoid - minoid);
				/* if that candidate OID was already generated, try again */
			} while (OIDTreeLookup(tree, candoid));
			ttree = OIDTreeInsert(tree, candoid);
			if (ttree == NULL) {
				GDKerror("#BATsample: memory allocation error");
				/* if malloc fails, we still need to clean up the tree */
				OIDTreeDestroy(tree);
				return NULL;
			}
			tree = ttree;
			rescnt++;
		}
		OIDTreeToBAT(tree, bn);
		OIDTreeDestroy(tree);

		BATsetcount(bn, n);
		bn->trevsorted = bn->batCount <= 1;
		bn->tsorted = 1;
		bn->tkey = 1;
		bn->tdense = bn->batCount <= 1;
		if (bn->batCount == 1)
			bn->tseqbase = *(oid *) Tloc(bn, BUNfirst(bn));
		bn->hdense = 1;
		bn->hseqbase = 0;
		bn->hkey = 1;
		bn->hrevsorted = bn->batCount <= 1;
		bn->hsorted = 1;
	}
	return bn;
}

