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
 * Test the equality of two literal table tuples.
 *
 * @param a Tuple to test against tuple @a b.
 * @param b Tuple to test against tuple @a a.
 * @return Boolean value @c true, if the two tuples are equal.
 */
static bool
tuple_eq (PFalg_tuple_t a, PFalg_tuple_t b)
{
    int i;

    /* schemata are not equal if they have a different number of attributes */
    if (a.count != b.count)
        return false;

    for (i = 0; i < a.count; i++)
        if ((a.atoms[i].type != b.atoms[i].type)
            /* if type is int, compare int member of union */
            || (a.atoms[i].type == aat_int
                && a.atoms[i].val.int_ != b.atoms[i].val.node)
            /* if type is str, compare str member of union */
            || (a.atoms[i].type == aat_str
                && strcmp (a.atoms[i].val.str, b.atoms[i].val.str))
            /* if type is node, compare node member of union */
            || (a.atoms[i].type == aat_node
                && a.atoms[i].val.node != b.atoms[i].val.node))
            break;

    return (i == a.count);
}


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
 * @node
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

    /*
     * Remember all tables we built in here. If the same table is requested
     * twice, we just return a reference to the old one.
     */
    static PFarray_t *old = NULL;
    unsigned int old_idx;
    
    /* initialize variable on first call */
    if (!old)
        old = PFarray (sizeof (PFalg_op_t *));

    /* Search table in the array of existing tables */
    for (old_idx = 0; old_idx < PFarray_last (old); old_idx++) {

        PFalg_op_t *o = *((PFalg_op_t **) PFarray_at (old, old_idx));
        int         j;

        /* see if we have the same number of arguments */
        if (attlist.count != o->schema.count)
            continue;

        /* see if attribute names match old schema */
        for (j = 0; j < attlist.count; j++)
            if (strcmp(attlist.atts[j], o->schema.items[j].name))
                break;
        if (j != attlist.count)
            continue;

        /* test if number of tuples matches */
        if (count != o->sem.lit_tbl.count)
            continue;

        /* test if tuples match */
        for (j = 0; j < count; j++)
            if (!tuple_eq (tuples[j], o->sem.lit_tbl.tuples[j]))
                break;

        if (j != count)
            continue;

        /*
         * If we came until here, old and new table must be equal.
         * So we don't create a new one, but return the existing one.
         */
        return o;
    }

    /* instantiate the new algebra operator node */
    ret = alg_op_leaf (aop_lit_tbl);

    /* set its schema */
    ret->schema.items
        = PFmalloc (attlist.count * sizeof (*(ret->schema.items)));
    for (int i = 0; i < attlist.count; i++) {
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
        for (int j = 0; j < tuples[i].count; j++)
            ret->schema.items[j].type |= tuples[i].atoms[j].type;
    }

#ifndef NDEBUG
    { /* sanity checks. Do arguments match schema? */
        int att, tup;
        for (tup = 0; tup < count; tup++) {
            if (ret->sem.lit_tbl.tuples[tup].count != ret->schema.count)
                PFoops (OOPS_FATAL,
                        "tuple does not match schema "
                        "(expected %i attributes, got %i)",
                        ret->schema.count, ret->sem.lit_tbl.tuples[tup].count);
        }
    }
#endif

    /* remember this node for future calls */
    *((PFalg_op_t **) PFarray_add (old)) = ret;

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

    /*
     * Remember all nodes we built in here. If the same expression is
     * requested twice, we just return a reference to the old one.
     */
    static PFarray_t *old = NULL;
    unsigned int old_idx;
    
    assert (n);

    /* initialize variable on first call */
    if (!old)
        old = PFarray (sizeof (PFalg_op_t *));

    /* Search table in the array of existing project operators */
    for (old_idx = 0; old_idx < PFarray_last (old); old_idx++) {

        PFalg_op_t *o = *((PFalg_op_t **) PFarray_at (old, old_idx));
        int         j;

        /* see if the old node has the same argument */
        if (o->child[0] != n)
            continue;

        /* Does the old node has the same number of attributes? */
        if (o->sem.proj.count != count)
            continue;

        /* See if the projection lists match */
        for (j = 0; j < count; j++)
            if (strcmp (proj[j].new, o->sem.proj.items[j].new)
                || strcmp (proj[j].old, o->sem.proj.items[j].old))
                break;

        if (j != count)
            continue;

        /*
         * If we came until here, old and new table must be equal.
         * So we don't create a new one, but return the existing one.
         */
        return o;
    }

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

    /* remember this node for future calls */
    *((PFalg_op_t **) PFarray_add (old)) = ret;

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

    /*
     * Remember all nodes we built in here. If the same expression is
     * requested twice, we just return a reference to the old one.
     */
    static PFarray_t *old = NULL;
    unsigned int old_idx;
    
    assert (n1); assert (n2);

    /* initialize variable on first call */
    if (!old)
        old = PFarray (sizeof (PFalg_op_t *));

    /* Search table in the array of existing project operators */
    for (old_idx = 0; old_idx < PFarray_last (old); old_idx++) {

        PFalg_op_t *o = *((PFalg_op_t **) PFarray_at (old, old_idx));

        /* see if the old node has the same argument */
        if (!((o->child[0] == n1 && o->child[1] == n2)
              || (o->child[1] == n1 && o->child[0] == n2)))
            continue;

        /*
         * If we came until here, old and new table must be equal.
         * So we don't create a new one, but return the existing one.
         */
        return o;
    }


    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 1 */
    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* copy schema from argument 2, check for duplicate attribute names */
    for (j = 0; j < n2->schema.count; j++) {

        ret->schema.items[n1->schema.count + j] = n2->schema.items[j];

#ifndef NDEBUG
        for (i = 0; i < n1->schema.count; i++)
            if (!strcmp (n1->schema.items[i].name, n2->schema.items[j].name))
                PFoops (OOPS_FATAL,
                        "duplicate attribute `%s' in cross product",
                        n2->schema.items[j].name);
#endif
    }

    /* remember this node for future calls */
    *((PFalg_op_t **) PFarray_add (old)) = ret;

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
PFalg_rownum (PFalg_op_t *n, PFalg_att_t a,
              PFalg_attlist_t s, PFalg_att_t p)
{
    PFalg_op_t *ret = alg_op_wire1 (aop_rownum, n);
    int i;
    int j;

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

        if (ret->sem.rownum.part
            && !strcmp (ret->sem.rownum.sortby.atts[i], ret->sem.rownum.part))
            PFoops (OOPS_FATAL,
                    "partitioning attribute must not appear in sort clause");
    }

    return ret;
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
