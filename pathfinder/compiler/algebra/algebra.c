/**
 * @file
 *
 * Functions related to algebra tree construction.
 *
 * This file mainly contains the constructor functions to create an
 * internal representation of the intermediate relational algebra.
 * This algebra comprises of the following operators (corresponding
 * constructor functions are stated after each operator):
 *
 * @verbatim
   project    Projection and attribute renaming           #PFalg_project()
   cross      Cartesian product                           #PFalg_cross()
   rownum     Generate consecutive numbers given a sort   #PFalg_rownum()
              order. Numbering may be partitioned if a
              grouping attribute is specified.
   lit_tbl    Literal tables                              #PFalg_lit_tbl_()
   serialize  Serialize the expression result. (This is   #PFalg_serialize()
              the root node of the algebra expression
              tree.)
@endverbatim
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

/** handling of variable argument lists */
#include <stdarg.h>
/** strcpy, strlen, ... */
#include <string.h>
#include <stdio.h>
/** assert() */
#include <assert.h>

#include "pathfinder.h"
#include "oops.h"
#include "mem.h"
#include "array.h"

/** declarations of algebra tree types and constructor functions */
#include "algebra.h"

/** include mnemonic names for constructor functions */
#include "algebra_mnemonic.h"


/**
 * Encapsulates initialization stuff common to binary numeric operators.
 */
static PFalg_op_t *
numeric_op(PFalg_op_kind_t kind, PFalg_op_t *n, PFalg_att_t att1,
	   PFalg_att_t att2, PFalg_att_t res);

/**
 * Encapsulates initialization stuff common to binary boolean operators.
 */
static PFalg_op_t *
boolean_op(PFalg_op_kind_t kind, PFalg_op_t *n, PFalg_att_t att1,
	   PFalg_att_t att2, PFalg_att_t res);

/**
 * Encapsulates initialization stuff common to unary operators.
 */
static PFalg_op_t *
unary_op(PFalg_op_kind_t kind, PFalg_op_t *n, PFalg_att_t att,
	 PFalg_att_t res);


/**
 * Create (and remember) the schema (attribute names and data types)
 * of the document table.
 *
 * @note The icc compiler crashes with an internal compiler error
 *       if the struct fields are explicitly stated as commented out
 *       below. So we have to fall back to the (less readable)
 *       alternative without the explicit field names.
 */
static PFalg_schema_t doc_schm = {
    .count = 6,
    .items = (struct PFalg_schm_item_t []) { { "pre",   aat_node },
                                             { "size",  aat_int },
                                             { "level", aat_int },
                                             { "kind",  aat_int },
                                             { "prop",  aat_str },
                                             { "frag",  aat_nat } } };
/*
static PFalg_schema_t doc_schm = { 
    .count = 6,
    .items = (struct PFalg_schm_item_t[]) {{.name = "pre", .type = aat_node},
					   {.name = "size", .type = aat_int},
					   {.name = "level", .type = aat_int},
					   {.name = "kind", .type = aat_int},
					   {.name = "prop", .type = aat_str},
					   {.name = "frag", .type = aat_nat}}};
*/

/** construct literal integer (atom) */
PFalg_atom_t
PFalg_lit_nat (nat value)
{
    return (PFalg_atom_t) { .type = aat_nat, .val = { .nat = value } };
}

/** construct literal integer (atom) */
PFalg_atom_t
PFalg_lit_int (int value)
{
    return (PFalg_atom_t) { .type = aat_int, .val = { .int_ = value } };
}

/** construct literal string (atom) */
PFalg_atom_t
PFalg_lit_str (char *value)
{
    return (PFalg_atom_t) { .type = aat_str, .val = { .str = value } };
}

/** construct literal decimal (atom) */
PFalg_atom_t
PFalg_lit_dec (float value)
{
    return (PFalg_atom_t) { .type = aat_dec, .val = { .dec = value } };
}

/** construct literal double (atom) */
PFalg_atom_t
PFalg_lit_dbl (double value)
{
    return (PFalg_atom_t) { .type = aat_dbl, .val = { .dbl = value } };
}

/** construct literal boolean (atom) */
PFalg_atom_t
PFalg_lit_bln (bool value)
{
    return (PFalg_atom_t) { .type = aat_bln, .val = { .bln = value } };
}


/**
 * Construct a tuple for a literal table.
 *
 * @see PFalg_lit_tbl_()
 *
 * @param count Number of values in the tuple that follow
 * @param atoms Values of type #PFalg_atom_t that form the tuple.
 *              The array must be exactly @a count items long.
 *
 * @note
 *   You should never need to call this function directly. Use the
 *   wrapper macro #PFalg_tuple instead (which is available as
 *   #tuple if you have included the mnemonic constructor names in
 *   algebra_mnemonic.h). This macro will detect the @a count
 *   argument on its own, so you only need to pass the tuple's
 *   atoms.
 *
 * @b Example:
 *
 * @code
   PFalg_tuple_t t = tuple (lit_int (1), lit_str ("foo"));
@endcode
 */
PFalg_tuple_t
PFalg_tuple_ (int count, PFalg_atom_t *atoms)
{
    return (PFalg_tuple_t) {.count = count,
                            .atoms = memcpy (PFmalloc (count * sizeof (*atoms)),
                                             atoms, count * sizeof (*atoms)) };
}


#if 0
/**
 * Test the equality of two schema specifications.
 *
 * @param a Schema to test against schema @a b.
 * @param b Schema to test against schema @a a.
 * @return Boolean value @c true, if the two schemata are equal.
 */
static bool
schema_eq (PFalg_schema_t a, PFalg_schema_t b)
{
    int i, j;

    /* schemata are not equal if they have a different number of attributes */
    if (a.count != b.count)
        return false;

    /* see if any attribute in a is also available in b */
    for (j = 0; i < a.count; i++) {
        for (j = 0; j < b.count; j++)
            if ((a.items[i].type == b.items[j].type)
                && !strcmp (a.items[i].name, b.items[j].name))
                break;
        if (j == b.count)
            return false;
    }

    return true;
}
#endif


/**
 * Constructor for an item in an algebra projection list;
 * a pair consisting of the new and old attribute name.
 * Particularly useful in combination with the constructor
 * function for the algebra projection operator (see
 * #PFalg_project() or its wrapper macro #project()).
 *
 * @param new Attribute name after the projection
 * @param old ``Old'' attribute name in the argument of
 *            the projection operator.
 */
PFalg_proj_t
PFalg_proj (PFalg_att_t new, PFalg_att_t old)
{
    return (PFalg_proj_t) { .new = strcpy (PFmalloc (strlen (new) + 1), new),
                            .old = strcpy (PFmalloc (strlen (old) + 1), old) };
}


/**
 * Constructor for attribute lists (e.g., for literal table
 * construction, or sort specifications in the rownum operator).
 *
 * @param count Number of array elements that follow.
 * @param atts  Array of attribute names.
 *              Must be exactly @a count elements long.
 *
 * @note
 *   You typically won't need to call this function directly. Use
 *   the wrapper macro #PFalg_attlist() (or its abbreviation #attlist(),
 *   if you have included algebra_mnemonic.h). It will determine
 *   @a count on its own, so you only have to pass an arbitrary
 *   number of attribute names.
 *
 * @b Example:
 *
 * @code
   PFalg_attlist_t s = attlist ("iter", "pos");
@endcode
 */
PFalg_attlist_t
PFalg_attlist_ (int count, PFalg_att_t *atts)
{
    PFalg_attlist_t ret;
    int             i;

    ret.count = count;
    ret.atts  = PFmalloc (count * sizeof (*(ret.atts)));

    for (i = 0; i < count; i++)
        ret.atts[i] = strcpy (PFmalloc (strlen (atts[i]) + 1), atts[i]);

    return ret;
}


/**
 * Create an algebra operator (leaf) node.
 *
 * Allocates memory for an algebra operator leaf node
 * and initializes all its fields. The node will have the
 * kind @a kind.
 */
static PFalg_op_t *
alg_op_leaf (PFalg_op_kind_t kind)
{
    unsigned int i;
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->kind = kind;

    ret->schema.count = 0;
    ret->schema.items = NULL;

    ret->refctr = 0;
    ret->usectr = 0;
    ret->bat_prefix = NULL;

    for (i = 0; i < PFALG_OP_MAXCHILD; i++)
        ret->child[i] = NULL;

    ret->node_id = 0;

    return ret;
}

/**
 * Create an algebra operator node with one child.
 * Similar to #alg_op_leaf(), but additionally wires one child.
 */
static PFalg_op_t *
alg_op_wire1 (PFalg_op_kind_t kind, PFalg_op_t *n)
{
    PFalg_op_t *ret = alg_op_leaf (kind);

    assert (n);

    ret->child[0] = n;

    return ret;
}

/**
 * Create an algebra operator node with two children.
 * Similar to #alg_op_wire1(), but additionally wires another child.
 */
static PFalg_op_t *
alg_op_wire2 (PFalg_op_kind_t kind, PFalg_op_t *n1, PFalg_op_t *n2)
{
    PFalg_op_t *ret = alg_op_wire1 (kind, n1);

    assert (n2);

    ret->child[1] = n2;

    return ret;
}

/**
 * Create an algebra operator node with three children.
 * Similar to #alg_op_wire2(), but additionally wires another child.
 * Required for element construction.
 */
static PFalg_op_t *
alg_op_wire3 (PFalg_op_kind_t kind, PFalg_op_t *n1, PFalg_op_t *n2,
	      PFalg_op_t *n3)
{
    PFalg_op_t *ret = alg_op_wire2 (kind, n1, n2);

    assert (n3);

    ret->child[2] = n3;

    return ret;
}

/**
 * Construct an algebra node representing a literal table, given
 * an attribute list and a list of tuples.
 *
 * If the exact same literal table is requested twice, this function
 * will automatically return the previously constructed node. This
 * is to facilitate common subexpression detection and you should
 * therefore @b never modify any constructed nodes afterwards. (Rather
 * just construct a new one, the garbage collector will take care of
 * cleaning up.)
 *
 * @param attlist Attribute list of the literal table. (Most easily
 *                constructed using #PFalg_attlist() or its abbreviated
 *                macro #attlist().)
 * @param count  Number of tuples that follow
 * @param tuples Tuples of this literal table, as #PFalg_tuple_t.
 *               This array must be exactly @a count items long.
 *
 * @note
 *   You should never need to call this function directly. Use the
 *   wrapper macro #PFalg_lit_tbl() instead (which is available as
 *   #lit_tbl() if you have included the mnemonic constructor names in
 *   algebra_mnemonic.h). This macro will detect the @a count
 *   argument on its own, so you only need to list all attribute
 *   specifictions.
 *
 * @b Example:
 *
 * @code
   PFalg_op_t t = lit_tbl (attlist ("iter", "pos", "item"),
                           tuple (lit_int (1), lit_int (1), lit_str ("foo")),
                           tuple (lit_int (1), lit_int (2), lit_str ("bar")),
                           tuple (lit_int (2), lit_int (1), lit_str ("baz")));
@endcode
 */
PFalg_op_t *
PFalg_lit_tbl_ (PFalg_attlist_t attlist, int count, PFalg_tuple_t *tuples)
{
    PFalg_op_t     *ret;      /* return value we are building */
    int             i;
    int             j;

    /* instantiate the new algebra operator node */
    ret = alg_op_leaf (aop_lit_tbl);

    /* set its schema */
    ret->schema.items
        = PFmalloc (attlist.count * sizeof (*(ret->schema.items)));
    for (i = 0; i < attlist.count; i++) {
        ret->schema.items[i].name = attlist.atts[i];
        ret->schema.items[i].type = 0;
    }
    ret->schema.count = attlist.count;

    /* pick all the given tuples from the variable argument list,
     * after allocating memory for them. */
    ret->sem.lit_tbl.count = count;
    ret->sem.lit_tbl.tuples
        = PFmalloc (count * sizeof (*(ret->sem.lit_tbl.tuples)));

    for (i = 0; i < count; i++) {

        /* copy tuple */
        ret->sem.lit_tbl.tuples[i] = tuples[i];

        /* add type of this tuple to schema */
        for (j = 0; j < tuples[i].count; j++) {
            ret->schema.items[j].type |= tuples[i].atoms[j].type;
	    fprintf (stderr, "lit_tbl: type: %i, column: %s\n",
		     ret->schema.items[j].type, ret->schema.items[j].name);
	}
    }

#ifndef NDEBUG
    { /* sanity checks. Do arguments match schema? */
        int tup;
        for (tup = 0; tup < count; tup++) {
            if (ret->sem.lit_tbl.tuples[tup].count != ret->schema.count)
                PFoops (OOPS_FATAL,
                        "tuple does not match schema "
                        "(expected %i attributes, got %i)",
                        ret->schema.count, ret->sem.lit_tbl.tuples[tup].count);
        }
    }
#endif

    return ret;
}


/**
 * Algebra projection/renaming.
 *
 * @param n     Argument for the projection operator.
 * @param count Number of items in the projection list that follows,
 *              i.e., number of attributes in the projection result.
 * @param proj  Projection list. Pass exactly @a count items of type
 *              #PFalg_proj_t in this array. All attributes referenced
 *              in the projection list must be available in relation @a
 *              n, and projection must not result in duplicate attribute
 *              names. You may, however, reference the same attribute in
 *              @a n twice (with different names in the projection
 *              result).
 *
 * @note
 *   You should never need to call this function directly. Use the
 *   wrapper macro #PFalg_project() instead (which is available as
 *   #project() if you have included the mnemonic constructor names in
 *   algebra_mnemonic.h). This macro will detect the @a count
 *   argument on its own, so you only need to list all attribute
 *   specifictions.
 */
PFalg_op_t *
PFalg_project_ (PFalg_op_t *n, int count, PFalg_proj_t *proj)
{
    PFalg_op_t *ret = alg_op_wire1 (aop_project, n);
    int         i;
    int         j;

    assert (n);

    /* allocate space for projection list */
    ret->sem.proj.count = count;
    ret->sem.proj.items = PFmalloc (count * sizeof (*(ret->sem.proj.items)));

    /* allocate space for result schema */
    ret->schema.count = count;
    ret->schema.items = PFmalloc (count * sizeof (*(ret->schema.items)));

    /* fetch projection list */
    for (i = 0; i < count; i++)
        ret->sem.proj.items[i] = proj[i];

    /* check for sanity and set result schema */
    for (i = 0; i < ret->sem.proj.count; i++) {

        /* lookup old name in n's schema
         * and use its type for the result schema */
        for (j = 0; j < n->schema.count; j++)
            if (!strcmp (ret->sem.proj.items[i].old,
                         n->schema.items[j].name)) {
                /* set name and type for this attribute in the result schema */
                ret->schema.items[i].name = ret->sem.proj.items[i].new;
                ret->schema.items[i].type = n->schema.items[j].type;

                break;
            }

        /* did we find the attribute? */
        if (j >= n->schema.count)
            PFoops (OOPS_FATAL,
                    "attribute `%s' referenced in projection not found",
                    ret->sem.proj.items[i].old);

        /* see if we have duplicate attributes now */
        for (j = 0; j < i; j++)
            if (!strcmp (ret->sem.proj.items[i].new,
                         ret->sem.proj.items[j].new))
                PFoops (OOPS_FATAL,
                        "projection results in duplicate attribute `%s' "
                        "(attributes %i and %i)",
                        ret->sem.proj.items[i].new, i+1, j+1);
    }

    return ret;
}


/**
 * Cross product (Cartesian product) between two algebra expressions.
 * Arguments @a n1 and @a n2 must not have any equally named attribute.
 */
PFalg_op_t *
PFalg_cross (PFalg_op_t *n1, PFalg_op_t *n2)
{
    PFalg_op_t *ret = alg_op_wire2 (aop_cross, n1, n2);
    int         i;
    int         j;

    assert (n1); assert (n2);

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 1 */
    for (i = 0; i < n1->schema.count; i++) {
        ret->schema.items[i] = n1->schema.items[i];
	fprintf (stderr, "cross: type: %i, column: %s\n",
		     ret->schema.items[i].type, ret->schema.items[i].name);
    }

    /* copy schema from argument 2, check for duplicate attribute names */
    for (j = 0; j < n2->schema.count; j++) {

        ret->schema.items[n1->schema.count + j] = n2->schema.items[j];
	fprintf (stderr, "cross: type: %i, column: %s\n",
		 ret->schema.items[n1->schema.count + j].type,
		 ret->schema.items[n1->schema.count + j].name);
#ifndef NDEBUG
        for (i = 0; i < n1->schema.count; i++)
            if (!strcmp (n1->schema.items[i].name, n2->schema.items[j].name))
                PFoops (OOPS_FATAL,
                        "duplicate attribute `%s' in cross product",
                        n2->schema.items[j].name);
#endif
    }

    return ret;
}


/**
 * Equi-join between two operator nodes.
 *
 * Assert that @a a1 is an attribute of @a n1 and @a a2 is an attribute
 * of @a n2. @a n1 and @a n2 must not have duplicate attribute names.
 * The schema of the result is (schema(@a n1) + schema(@a n2)).
 */
PFalg_op_t *
PFalg_eqjoin (PFalg_op_t *n1, PFalg_op_t *n2, PFalg_att_t a1, PFalg_att_t a2)
{
    PFalg_op_t *ret;
    int         i;
    int         j;

    assert (n1); assert (n2);

    /* verify that a1 is attribute of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
	if (!strcmp (a1, n1->schema.items[i].name))
	    break;

    /* did we find attribute a1? */
    if (i >= n1->schema.count)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in join not found", a1);

    /* ... and a2 is attribute of n2 */
    for (i = 0; i < n2->schema.count; i++)
	if (!strcmp (a2, n2->schema.items[i].name))
	    break;

    /* did we find attribute a2? */
    if (i >= n2->schema.count)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in join not found", a2);

    /* build new equi-join node */
    ret = alg_op_wire2 (aop_eqjoin, n1, n2);

    /* insert semantic value (join attributes) into the result */
    ret->sem.eqjoin.att1 = a1;
    ret->sem.eqjoin.att2 = a2;

    /* allocate memory for the result schema (schema(n1) + schema(n2)) */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n1' */
    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* copy schema from argument 'n2', check for duplicate attribute names */
    for (j = 0; j < n2->schema.count; j++) {

        ret->schema.items[n1->schema.count + j] = n2->schema.items[j];

#ifndef NDEBUG
        for (i = 0; i < n1->schema.count; i++)
            if (!strcmp (n1->schema.items[i].name, n2->schema.items[j].name))
                PFoops (OOPS_FATAL,
                        "duplicate attribute `%s' in equi-join",
                        n2->schema.items[j].name);
#endif
    }

    return ret;
}


/**
 * Staircase join between two operator nodes.
 *
 * Each such join corresponds to the evaluation of an XPath location
 * step. @a scj is not a "real" algebra node, but just serves as a
 * container for semantic information on the kind test and location
 * step represented by this join. We extract this information, store
 * it in the newly created join operator and discard the @a scj node. 
 */
PFalg_op_t *
PFalg_scjoin (PFalg_op_t *proj, PFalg_op_t *uni, PFalg_op_t *scj)
{
    PFalg_op_t *ret;
    int         i;

    assert (proj); assert (uni);

    /* verify node type because the schema of the projection node
     * will become the overall schema
     */
    assert (proj->kind == aop_project);

    /* create new join node */
    ret = alg_op_wire2 (aop_scjoin, proj, uni);

    /* insert semantic value (axis/kind test) into the result */
    ret->sem = scj->sem;

#ifndef NDEBUG
    /* verify both schemata */
    for (i = 0; i < proj->schema.count; i++) {
	if (!strcmp(proj->schema.items[i].name, "iter")
	 || !strcmp(proj->schema.items[i].name, "item"))
	    continue;
	else
	    PFoops (OOPS_FATAL,
		    "illegal attribute `%s' in staircase join",
		    proj->schema.items[i].name);
    }
    for (i = 0; i < uni->schema.count; i++) {
	if (!strcmp(uni->schema.items[i].name, "pre")
	 || !strcmp(uni->schema.items[i].name, "size")
	 || !strcmp(uni->schema.items[i].name, "level")
	 || !strcmp(uni->schema.items[i].name, "kind")
	 || !strcmp(uni->schema.items[i].name, "prop")
	 || !strcmp(uni->schema.items[i].name, "frag"))
	    continue;
	else
	    PFoops (OOPS_FATAL,
		    "illegal attribute `%s' in staircase join",
		    uni->schema.items[i].name);
    }
#endif

    /* allocate memory for the result schema (= schema(proj)) */
    ret->schema.count = proj->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'proj' argument */
    for (i = 0; i < proj->schema.count; i++)
        ret->schema.items[i] = proj->schema.items[i];

    return ret;
}


/** Creates a representation of the document table. */
PFalg_op_t * PFalg_doc_tbl (char *rel)
{
    PFalg_op_t         *ret;

    /* instantiate a new document table representation */
    ret = alg_op_leaf (aop_doc_tbl);

    /* insert name of document relation */
    ret->sem.doc_tbl.rel = rel;

    /* set doc table schema */
    ret->schema = doc_schm;

    return ret;
}


/**
 * Disjoint union of two relations.
 * Both argument must have the same schema.
 *
 * FIXME: This is currently wrong. Need to implement polymorphism here.
 */
PFalg_op_t *
PFalg_disjunion (PFalg_op_t *n1, PFalg_op_t *n2)
{
    PFalg_op_t *ret = alg_op_wire2 (aop_disjunion, n1, n2);
    int         i, j;

    /* see if both operands have same number of attributes */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of UNION do not match");

    /* see if we find each attribute of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (!strcmp (n1->schema.items[i].name, n2->schema.items[j].name))
                break;
        if (j == n2->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of two arguments of UNION do not match");
    }

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* set schema */
    for (i = 0; i < n1->schema.count; i++) {
	fprintf (stderr, "disunion (n1): type: %i, column: %s\n",
		 n1->schema.items[i].type,
		 n1->schema.items[i].name);
	fprintf (stderr, "disunion (n2): type: %i, column: %s\n",
		 n2->schema.items[i].type,
		 n2->schema.items[i].name);

        ret->schema.items[i] =
            (struct PFalg_schm_item_t) { .name = n1->schema.items[i].name,
                                         .type = n1->schema.items[i].type
                                                 | n2->schema.items[i].type };

	fprintf (stderr, "disunion (ret): type: %i, column: %s\n",
		 ret->schema.items[i].type,
		 ret->schema.items[i].name);
    }

    return ret;
}


/**
 * Difference of two relations.
 * Both argument must have the same schema.
 */
PFalg_op_t * PFalg_difference (PFalg_op_t *n1, PFalg_op_t *n2)
{
    PFalg_op_t *ret = alg_op_wire2 (aop_difference, n1, n2);
    int         i, j;

    /* see if both operands have same number of attributes */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of DIFFERENCE do not match");

    /* see if we find each attribute of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (!strcmp (n1->schema.items[i].name, n2->schema.items[j].name))
                break;
        if (j == n2->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of two arguments of DIFFERENCE do not match");
    }

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* set schema */
    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] =
            (struct PFalg_schm_item_t) { .name = n1->schema.items[i].name,
                                         .type = n1->schema.items[i].type
                                               | n2->schema.items[i].type };

    return ret;
}


/**
 * The `rownum' operator, a Pathfinder-specific extension to the
 * relational algebra.
 */
PFalg_op_t *
PFalg_rownum (PFalg_op_t *n, PFalg_att_t a, PFalg_attlist_t s, PFalg_att_t p)
{
    PFalg_op_t *ret = alg_op_wire1 (aop_rownum, n);
    int i;
    int j;
    PFalg_simple_type_t t;

    /* copy parameters into semantic content of return node */
    ret->sem.rownum.attname = strcpy (PFmalloc (strlen (a)+1), a);
    ret->sem.rownum.sortby = (PFalg_attlist_t) {
        .count = s.count,
        .atts = memcpy (PFmalloc (s.count * sizeof (PFalg_att_t)), s.atts,
                        s.count * sizeof (PFalg_att_t)) };
    ret->sem.rownum.part = p ? strcpy (PFmalloc (strlen (p)+1), p) : NULL;

    /* result schema is input schema plus the new attribute */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));
    
    for (i = 0; i < n->schema.count; i++) {

        /* there must not be an argument attribute named like the new one */
        if (!strcmp (n->schema.items[i].name, a))
            PFoops (OOPS_FATAL,
                    "rownum operator would result in duplicate attribute `%s'",
                    a);

        /* copy this attribute specification */
        ret->schema.items[i] = n->schema.items[i];
    }
    /* append new attribute, named as given in a, with type int */
    ret->schema.items[ret->schema.count - 1]
        = (struct PFalg_schm_item_t) { .name = a, .type = aat_nat };

    /* sanity checks below */
    if (s.count == 0)
        PFinfo (OOPS_WARNING,
                "applying rownum operator without sort specifier");

    /* see if we can find all sort specifications */
    for (i = 0; i < ret->sem.rownum.sortby.count; i++) {

        for (j = 0; j < n->schema.count; j++)
            if (!strcmp (n->schema.items[j].name,
                         ret->sem.rownum.sortby.atts[i]))
                break;

        if (j == n->schema.count)
            PFoops (OOPS_FATAL,
                    "could not find sort attribute `%s'",
                    ret->sem.rownum.sortby.atts[i]);

        for (t = 1; t; t <<= 1)
            if (t == n->schema.items[j].type)
                break;
        if (t == 0)
            PFoops (OOPS_FATAL,
                    "sort criterion for rownum must be monomorphic, type: %i, name: %s",
		    n->schema.items[j].type, n->schema.items[j].name);

        if (ret->sem.rownum.part
            && !strcmp (ret->sem.rownum.sortby.atts[i], ret->sem.rownum.part))
            PFoops (OOPS_FATAL,
                    "partitioning attribute must not appear in sort clause");
    }

    return ret;
}


/**
 * Selection of all rows where the value of column @a att is not 0.
 *
 * The result schema corresponds to the schema of the input
 * relation @a n.
 */
PFalg_op_t*
PFalg_select (PFalg_op_t *n, PFalg_att_t att)
{
    PFalg_op_t *ret;
    int         i;

    assert (n);

    /* verify that 'att' is an attribute of 'n' ... */
    for (i = 0; i < n->schema.count; i++)
	if (!strcmp (att, n->schema.items[i].name))
	    break;

    /* did we find attribute att? */
    if (i >= n->schema.count)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in selection not found", att);

    /* build a new selection node */
    ret = alg_op_wire1 (aop_select, n);

    /* insert semantic value (select-attribute) into the result */
    ret->sem.select.att = att;

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = n->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    return ret;
}

/**
 * Constructor for type test on column values. The result is
 * stored in newly created column @a res.
 */
PFalg_op_t *
PFalg_type (PFalg_op_t *n, PFalg_att_t att, PFalg_att_t res, PFty_t ty)
{
    PFalg_op_t  *ret;
    int          i;

    assert (n);

    /* verify that 'att' is an attribute of 'n' ... */
    for (i = 0; i < n->schema.count; i++)
	if (!strcmp (att, n->schema.items[i].name))
	    break;

    /* did we find attribute att? */
    if (i >= n->schema.count)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in type test not found", att);

    /* create new type test node */
    ret = alg_op_wire1 (aop_type, n);

    /* insert semantic value (type-tested attribute and its type,
     * result attribute) into the result
     */
    ret->sem.type.att = att;
    ret->sem.type.ty = ty;
    ret->sem.type.res = res;

    /* allocate memory for the result schema (= schema(n) + 1 for the
     * 'res' attribute which is to hold the result of the type test) 
     */
    ret->schema.count = n->schema.count + 1;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' attribute; it is of type
     * boolean
     */
    ret->schema.items[ret->schema.count - 1].type = aat_bln;
    ret->schema.items[ret->schema.count - 1].name = res;

    return ret;
}


/**
 * Constructor for a type cast of column @a att. The type of @a att
 * must be casted to type @a ty.
 */
PFalg_op_t *
PFalg_cast (PFalg_op_t *n, PFalg_att_t att, PFalg_simple_type_t ty)
{
    PFalg_op_t  *ret;
    int          i;

    assert (n);

    /* verify that att is an attribute of n ... */
    for (i = 0; i < n->schema.count; i++)
	if (!strcmp (att, n->schema.items[i].name))
	    break;

    /* did we find attribute att? */
    if (i >= n->schema.count)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in type cast not found", att);

    /* create new type cast node */
    ret = alg_op_wire1 (aop_cast, n);

    /*
     * insert semantic value (type-tested attribute and its type)
     * into the result
     */
    ret->sem.cast.att = att;
    ret->sem.cast.ty = ty;

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = n->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* Copy schema from argument, changing type of `att' appropriately. */
    for (i = 0; i < n->schema.count; i++)
        if (!strcmp (n->schema.items[i].name, att)) {
            ret->schema.items[i].name = n->schema.items[i].name;
            ret->schema.items[i].type = ty;
        }
        else
            ret->schema.items[i] = n->schema.items[i];

    return ret;
}


/** Constructs an operator for an arithmetic addition. */
PFalg_op_t *
PFalg_add (PFalg_op_t *n, PFalg_att_t att1,
	   PFalg_att_t att2, PFalg_att_t res)
{
    return numeric_op (aop_num_add, n, att1, att2, res);
}


/** Constructs an operator for an arithmetic subtraction. */
PFalg_op_t *
PFalg_subtract (PFalg_op_t *n, PFalg_att_t att1,
		PFalg_att_t att2, PFalg_att_t res)
{
    return numeric_op (aop_num_subtract, n, att1, att2, res);
}


/** Constructs an operator for an arithmetic multiplication. */
PFalg_op_t *
PFalg_multiply (PFalg_op_t *n, PFalg_att_t att1,
		PFalg_att_t att2, PFalg_att_t res)
{
    return numeric_op (aop_num_multiply, n, att1, att2, res);
}


/** Constructs an operator for an arithmetic division. */
PFalg_op_t *
PFalg_divide (PFalg_op_t *n, PFalg_att_t att1,
	      PFalg_att_t att2, PFalg_att_t res)
{
    return numeric_op (aop_num_divide, n, att1, att2, res);
}


/** Constructor for numeric equal operators. */
PFalg_op_t * PFalg_equal (PFalg_op_t *n, PFalg_att_t att1,
			  PFalg_att_t att2, PFalg_att_t res)
{
    return numeric_op (aop_num_equal, n, att1, att2, res);
}


/** Constructor for numeric less-than operators. */
PFalg_op_t * PFalg_less_than (PFalg_op_t *n, PFalg_att_t att1,
			      PFalg_att_t att2, PFalg_att_t res)
{
    return numeric_op (aop_num_less_than, n, att1, att2, res);
}


/** Constructor for numeric greater-than operators. */
PFalg_op_t * PFalg_greater_than (PFalg_op_t *n, PFalg_att_t att1,
				 PFalg_att_t att2, PFalg_att_t res)
{
    return numeric_op (aop_num_greater_than, n, att1, att2, res);
}


/** Constructor for numeric negation operators. */
PFalg_op_t * PFalg_neg (PFalg_op_t *n, PFalg_att_t att,
			PFalg_att_t res)
{
    return unary_op (aop_num_neg, n, att, res);
}


/** Constructor for boolean AND operators. */
PFalg_op_t * PFalg_and (PFalg_op_t *n, PFalg_att_t att1,
			PFalg_att_t att2, PFalg_att_t res)
{
    return boolean_op (aop_bool_and, n, att1, att2, res);
}


/** Constructor for boolean OR operators. */
PFalg_op_t * PFalg_or (PFalg_op_t *n, PFalg_att_t att1,
		       PFalg_att_t att2, PFalg_att_t res)
{
    return boolean_op (aop_bool_or, n, att1, att2, res);
}


/** Constructor for boolean NOT operators. */
PFalg_op_t * PFalg_not (PFalg_op_t *n, PFalg_att_t att,
			PFalg_att_t res)
{
    return unary_op (aop_bool_not, n, att, res);
}


/**
 * Encapsulates initialization stuff common to binary arithmetic
 * operators.
 *
 * Depending on the @a kind parameter, we connect the two values
 * of columns @a att1 and @a att2 and stores the result in newly
 * created attribute @a res. @a res gets the same data type as @a
 * att1 and @a att2. The result schema corresponds to the schema
 * of the input relation @a n plus @a res.
 */
static PFalg_op_t *
numeric_op (PFalg_op_kind_t kind, PFalg_op_t *n, PFalg_att_t att1,
	    PFalg_att_t att2, PFalg_att_t res)
{
    PFalg_op_t *ret;
    int         i;
    int         ix1 = -1;
    int         ix2 = -1;
    
    assert (n);

    /* verify that 'att1' and 'att2' are attributes of n ... */
    for (i = 0; i < n->schema.count; i++) {
	if (!strcmp (att1, n->schema.items[i].name))
	    ix1 = i;                /* remember array index of att1 */
	else if (!strcmp (att2, n->schema.items[i].name))
	    ix2 = i;                /* remember array index of att2 */
    }

    /* did we find attribute 'att1' and 'att2'? */
    if (ix1 < 0)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in binary operation "
		"not found", att1);
    else if (ix2 < 0)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in binary operation "
		"not found", att2);

    /* make sure both attributes are of the same numeric type
     * TODO: should 'aat_node' type be integrated? */
    assert (n->schema.items[ix1].type == aat_nat ||
	    n->schema.items[ix1].type == aat_int ||
	    n->schema.items[ix1].type == aat_node ||
	    n->schema.items[ix1].type == aat_dec ||
	    n->schema.items[ix1].type == aat_dbl);
    assert (n->schema.items[ix1].type == n->schema.items[ix2].type);

    /* create new binary operator node */
    ret = alg_op_wire1 (kind, n);

    /* insert semantic value (operand attributes and result attribute)
     * into the result
     */
    ret->sem.arithm.att1 = att1;
    ret->sem.arithm.att2 = att2;
    ret->sem.arithm.res = res;

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' attribute; it has the same type
     * as attribute 'att1' (and 'att2'), but a different name
     */
    ret->schema.items[ret->schema.count - 1] = n->schema.items[ix1];
    ret->schema.items[ret->schema.count - 1].name = res;

    return ret;
}


/**
 * Encapsulates initialization stuff common to binary boolean
 * operators.
 *
 * Depending on the @a kind parameter, we connect the two values
 * of columns @a att1 and @a att2 and stores the result in newly
 * created attribute @a res. @a res gets the same data type as @a
 * att1 and @a att2. The result schema corresponds to the schema
 * of the input relation @a n plus @a res.
 * TODO: merge this routine with numeric_op()?
 */
static PFalg_op_t *
boolean_op (PFalg_op_kind_t kind, PFalg_op_t *n, PFalg_att_t att1,
	    PFalg_att_t att2, PFalg_att_t res)
{
    PFalg_op_t *ret;
    int         i;
    int         ix1 = -1;
    int         ix2 = -1;
    
    assert (n);

    /* verify that 'att1' and 'att2' are attributes of n ... */
    for (i = 0; i < n->schema.count; i++) {
	if (!strcmp (att1, n->schema.items[i].name))
	    ix1 = i;                /* remember array index of att1 */
	else if (!strcmp (att2, n->schema.items[i].name))
	    ix2 = i;                /* remember array index of att2 */
    }

    /* did we find attribute 'att1' and 'att2'? */
    if (ix1 < 0)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in binary operation "
		"not found", att1);
    else if (ix2 < 0)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in binary operation "
		"not found", att2);

    /* make sure that both attributes are of type boolean */
    assert (n->schema.items[ix1].type == aat_bln);
    assert (n->schema.items[ix1].type == n->schema.items[ix2].type);

    /* create new binary operator node */
    ret = alg_op_wire1 (kind, n);

    /* insert semantic value (operand attributes and result attribute)
     * into the result
     */
    ret->sem.arithm.att1 = att1;
    ret->sem.arithm.att2 = att2;
    ret->sem.arithm.res = res;

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' attribute; it is of type
     * boolean and named 'res'
     */
    ret->schema.items[ret->schema.count - 1] = n->schema.items[ix1];
    ret->schema.items[ret->schema.count - 1].name = res;

    return ret;
}


/**
 * Encapsulates initialization stuff common to unary operators.
 *
 * Depending on the @a kind parameter, we process the value of
 * column @a att and stores the result in newly created attribute
 * @a res. @a res gets the same data type as @a att. The result
 * schema corresponds to the schema of the input relation @a n plus
 * @a res.
 */
static PFalg_op_t *
unary_op(PFalg_op_kind_t kind, PFalg_op_t *n, PFalg_att_t att,
	 PFalg_att_t res)
{
    PFalg_op_t *ret;
    int         i;
    int         ix = 0;
    
    assert (n);

    /* verify that 'att' is an attribute of n ... */
    for (i = 0; i < n->schema.count; i++) {
	if (!strcmp (att, n->schema.items[i].name)) {
	    ix = i;                /* remember array index of att */
	    break;
	}
    }

    /* did we find attribute 'att'? */
    if (i >= n->schema.count)
	PFoops (OOPS_FATAL,
		"attribute `%s' referenced in unary operation not found",
		att);

    /* assert that att is of correct type TODO: inclusion of node */
    if (kind == aop_num_neg)
	assert (n->schema.items[ix].type == aat_nat ||
		n->schema.items[ix].type == aat_int ||
		n->schema.items[ix].type == aat_node ||
		n->schema.items[ix].type == aat_dec ||
		n->schema.items[ix].type == aat_dbl);
    else if (kind == aop_bool_not)
	assert (n->schema.items[ix].type == aat_bln);

    /* create new unary operator node */
    ret = alg_op_wire1 (kind, n);

    /* insert semantic value (operand attribute and result attribute)
     * into the result
     */
    ret->sem.unary.att = att;
    ret->sem.unary.res = res;

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' attribute; it has the same type
     * as attribute 'att', but a different name
     */
    ret->schema.items[ret->schema.count - 1] = n->schema.items[ix];
    ret->schema.items[ret->schema.count - 1].name = res;

    return ret;
}


/**
 * Constructor for operators forming (partitioned) sum of a column.
 *
 * The values of attribute @a att are to be summed up. The partitioning
 * (group by) attributes are containes in @a part. The results (sums)
 * are stored in attribute @a res.
 */
PFalg_op_t * PFalg_sum (PFalg_op_t *n, PFalg_att_t att,
			PFalg_att_t res, PFalg_attlist_t part)
{
    /* build a new sum node */
    PFalg_op_t *ret = alg_op_wire1 (aop_sum, n);
    int         i;
    int         j;
    int         c = 0;

    /* calculate number of schema items in the result schema
     * (partitioning attribute(s) plus result attribute) and
     * assign it to the result operator
     */
    ret->schema.count = part.count + 1;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));


    /* verify that 'att' and the attributes in 'part' are attributes of n;
     * if so, include them into the array of result schema items
     */
    for (i = 0; i < n->schema.count; i++) {
	/* copy the summed-up attribute 'att'; it will be named
	 * 'res' in the result
	 */
	if (!strcmp (att, n->schema.items[i].name)) {
	    ret->schema.items[c] = n->schema.items[i];
	    ret->schema.items[c].name = res;
	    c++;
	}

	/* copy the partitioning attributes */
	for (j = 0; j < part.count; j++) {
	    if (!strcmp (n->schema.items[i].name, part.atts[j])) {
	        ret->schema.items[c] = n->schema.items[i];
		c++;
		break;
	    }
	}
    }

    /* did we find attribute 'att' and attributes in 'part'? */
    if (c < ret->schema.count)
	PFoops (OOPS_FATAL,
		"attribute referenced in sum operator not found");

    /* insert semantic value (summed-up attribute, partitioning
     * attribute(s), and result attribute) into the result
     */
    ret->sem.sum.att = att;
    ret->sem.sum.part = part;
    ret->sem.sum.res = res;

    return ret;
}


/**
 * Constructor for (partitioned) row counting operators.
 *
 * Counts all rows with identical values in column @a part (which holds
 * the partitioning or group by columns). The results are stored in
 * attribute @a res. 
 */
PFalg_op_t * PFalg_count (PFalg_op_t *n, PFalg_att_t res,
			  PFalg_attlist_t part)
{
    PFalg_op_t *ret = alg_op_wire1 (aop_count, n);
    int         i;
    int         j;
    int         c = 0;

    /* calculate number of schema items in the result schema
     * (partitioning attribute(s) plus result attribute) and
     * assign it to the result operator
     */
    ret->schema.count = part.count + 1;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy the partitioning attributes */
    for (i = 0; i < n->schema.count; i++)
	for (j = 0; j < part.count; j++) {
	    if (!strcmp (n->schema.items[i].name, part.atts[j])) {
	        ret->schema.items[c] = n->schema.items[i];
		c++;
		break;
	    }
	}

    /* did we find all attributes in 'part'? */
    if (c < (ret->schema.count-1))
	PFoops (OOPS_FATAL,
		"attribute referenced in count operator not found");

    /* insert result attribute into schema */
    ret->schema.items[c].name = res;
    ret->schema.items[c].type = aat_int;

    /* insert semantic value (partitioning and result attribute) into
     * the result
     */
    ret->sem.count.part = part;
    ret->sem.count.res = res;

    return ret;
}


/** Constructor for duplicate elimination operators. */
PFalg_op_t * PFalg_distinct (PFalg_op_t *n)
{
    PFalg_op_t *ret = alg_op_wire1 (aop_distinct, n);
    int         i;

    /* allocate memory for the result schema; it's the same schema as n's */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n' */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    return ret;
}


/** Constructor for element operators.
 *
 * @a doc is the document table, @a tags constructs the elements'
 * tag names, and @a cont is the content of the tags.
 */
PFalg_op_t * PFalg_element (PFalg_op_t *doc, PFalg_op_t *tags,
			    PFalg_op_t *cont)
{
    PFalg_op_t *ret = alg_op_wire3 (aop_element, doc, tags, cont);
    int i;

    /* copy schema from argument 'doc' and additionally add attribute
     * 'iter'; the result schema is (<doc_tbl schema> | iter)
     */
    ret->schema.count = doc->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < doc->schema.count; i++)
        ret->schema.items[i] = doc->schema.items[i];

    ret->schema.items[ret->schema.count-1].name = "iter";
    ret->schema.items[ret->schema.count-1].type = aat_nat;

    return ret;
}


/** Constructor for text content operators.
 *
 * @a doc is the document table and @a cont is the text content of
 * the node.
 */
PFalg_op_t * PFalg_textnode (PFalg_op_t *doc, PFalg_op_t *cont)
{
    PFalg_op_t *ret = alg_op_wire2 (aop_textnode, doc, cont);
    int i;

    /* copy schema from argument 'doc' and additionally add attribute
     * 'iter'; the result schema is (<doc_tbl schema> | iter)
     */
    ret->schema.count = doc->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < doc->schema.count; i++)
        ret->schema.items[i] = doc->schema.items[i];

    ret->schema.items[ret->schema.count-1].name = "iter";
    ret->schema.items[ret->schema.count-1].type = aat_nat;

    return ret;
}


/**
 * In case an optional variable $p is present in a let expression,
 * we use rownum() to create the item values bound to the variable.
 * However, the item values of $p must be of type int (instead
 * of nat), so we cast it accordingly.
 */
PFalg_op_t *
PFalg_cast_item (PFalg_op_t * o)
{
    int i;

    assert(o->kind == aop_rownum);

    for (i = 0; i < o->schema.count; i++) {
	if (!strcmp(o->schema.items[i].name, "item"))
	    o->schema.items[i].type = aat_int;
    }

    return o;
}


/**
 * Construct algebra node that will serialize the argument when executed.
 * A serialization node will be placed on the very top of the algebra
 * expression tree. Its main use is to have an explicit Twig match for
 * the expression root.
 */
PFalg_op_t *
PFalg_serialize (PFalg_op_t *n)
{
    return alg_op_wire1 (aop_serialize, n);
}
