/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Functions related to logical algebra tree construction.
 *
 * This file mainly contains the constructor functions to create an
 * internal representation of the intermediate relational algebra.
 *
 * For documentation on the algebra compilation process, please
 * refer to the @ref compilation documentation in algebra/algebra.c.
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

/** handling of variable argument lists */
#include <stdarg.h>
/** strcpy, strlen, ... */
#include <string.h>
#include <stdio.h>
/** assert() */
#include <assert.h>

#include "oops.h"
#include "mem.h"
#include "array.h"

#include "logical.h"

/** include mnemonic names for constructor functions */
#include "logical_mnemonic.h"


/* Encapsulates initialization stuff common to binary comparison operators.  */
static PFla_op_t *
compar_op (PFla_op_kind_t kind, const PFla_op_t *n,
           PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2);

/* Encapsulates initialization stuff common to binary boolean operators. */
static PFla_op_t *
boolean_op (PFla_op_kind_t kind, const PFla_op_t *n, PFalg_att_t att1,
            PFalg_att_t att2, PFalg_att_t res);

/* Encapsulates initialization stuff common to unary operators. */
static PFla_op_t *
unary_op (PFla_op_kind_t kind, const PFla_op_t *n, PFalg_att_t att,
          PFalg_att_t res);


/**
 * Create a logical algebra operator (leaf) node.
 *
 * Allocates memory for an algebra operator leaf node
 * and initializes all its fields. The node will have the
 * kind @a kind.
 */
static PFla_op_t *
la_op_leaf (PFla_op_kind_t kind)
{
    unsigned int i;
    PFla_op_t *ret = PFmalloc (sizeof (PFla_op_t));

    ret->kind = kind;

    ret->schema.count  = 0;
    ret->schema.items  = NULL;

    for (i = 0; i < PFLA_OP_MAXCHILD; i++)
        ret->child[i] = NULL;

    ret->plans         = NULL;
    ret->sql_ann       = NULL;
    ret->prop          = PFprop ();
    ret->node_id       = 0;

    /* bits required to allow DAG traversals */
    ret->bit_reset     = 0;
    ret->bit_dag       = 0;
    /* bits required to look up proxy nodes */
    ret->bit_in        = 0;
    ret->bit_out       = 0;

    return ret;
}


/**
 * Create an algebra operator node with one child.
 * Similar to #la_op_leaf(), but additionally wires one child.
 */
static PFla_op_t *
la_op_wire1 (PFla_op_kind_t kind, const PFla_op_t *n)
{
    PFla_op_t *ret = la_op_leaf (kind);

    assert (n);

    ret->child[0] = (PFla_op_t *) n;

    return ret;
}


/**
 * Create an algebra operator node with two children.
 * Similar to #la_op_wire1(), but additionally wires another child.
 */
static PFla_op_t *
la_op_wire2 (PFla_op_kind_t kind, const PFla_op_t *n1, const PFla_op_t *n2)
{
    PFla_op_t *ret = la_op_wire1 (kind, n1);

    assert (n2);

    ret->child[1] = (PFla_op_t *) n2;

    return ret;
}


/**
 * Construct a dummy operator that is generated whenever some rewrite
 * throws away an operator (e.g., '*p = *L(p);') and the replacement
 * is an already existing node that may not be split into multiple
 * operators (e.g. a number operator).
 */
PFla_op_t * PFla_dummy (PFla_op_t *n)
{
    /* always create the dummy node as otherwise
       the general optimization will relabel quite
       a long time. */
    /*
    assert (n);
    switch (n->kind)
    {
        case la_rownum:
        case la_number:
        case la_roots:
        case la_rec_fix:
        case la_rec_base:
        case la_proxy:
        case la_proxy_base:
        {
    */
            PFla_op_t *ret = la_op_wire1 (la_dummy, n);

            ret->schema.count = n->schema.count;
            ret->schema.items
                = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

            /* copy schema from n */
            for (unsigned int i = 0; i < n->schema.count; i++)
                ret->schema.items[i] = n->schema.items[i];

            return ret;
    /*
        }

        default:
            return n;
    }
    */
}


/**
 * Construct algebra node representing a sequence that will serialize
 * the argument when executed. A serialization node will be placed on
 * the very top of the algebra expression tree. Its main use is to have
 * an explicit Twig match for the expression root.
 *
 * @a doc is the current document (live nodes) and @a alg is the overall
 * algebra expression.
 */
PFla_op_t *
PFla_serialize_seq (const PFla_op_t *doc, const PFla_op_t *alg,
                    PFalg_att_t pos, PFalg_att_t item)
{
    PFla_op_t *ret = la_op_wire2 (la_serialize_seq, doc, alg);

    ret->sem.ser_seq.pos  = pos;
    ret->sem.ser_seq.item = item;

    ret->schema.count = alg->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    /* copy schema from alg */
    for (unsigned int i = 0; i < alg->schema.count; i++)
        ret->schema.items[i] = alg->schema.items[i];

    return ret;
}


/**
 * Construct algebra node representing a relation that will serialize
 * the argument when executed. A serialization node will be placed on
 * the very top of the algebra expression tree. Its main use is to have
 * an explicit Twig match for the expression root.
 *
 * @a doc is the current document (live nodes) and @a alg is the overall
 * algebra expression.
 */
PFla_op_t *
PFla_serialize_rel (const PFla_op_t *alg,
                    PFalg_att_t iter, PFalg_att_t pos,
                    PFalg_attlist_t items)
{
    PFla_op_t *ret = la_op_wire1 (la_serialize_rel, alg);

#ifndef NDEBUG
    unsigned int i, j;

    if (!PFprop_ocol (alg, iter))
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in serialize"
                " operator not found", PFatt_str (iter));
    if (!PFprop_ocol (alg, pos))
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in serialize"
                " operator not found", PFatt_str (pos));
    /* verify that the referenced attributes in items
       are really attributes of alg ... */
    for (i = 0; i < items.count; i++) {
        for (j = 0; j < alg->schema.count; j++)
            if (alg->schema.items[j].name == items.atts[i])
                break;
        if (j == alg->schema.count)
            PFoops (OOPS_FATAL,
                    "attribute `%s' referenced in serialize"
                    " operator not found", PFatt_str (items.atts[i]));
    }
#endif

    ret->sem.ser_rel.iter  = iter;
    ret->sem.ser_rel.pos   = pos;
    ret->sem.ser_rel.items = (PFalg_attlist_t) {
        .count = items.count,
        .atts  = memcpy (PFmalloc (items.count * sizeof (PFalg_att_t)),
                         items.atts,
                         items.count * sizeof (PFalg_att_t)) };

    ret->schema.count = alg->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    /* copy schema from alg */
    for (unsigned int i = 0; i < alg->schema.count; i++)
        ret->schema.items[i] = alg->schema.items[i];

    return ret;
}


/**
 * Construct an algebra node representing a literal table, given
 * an attribute list and a list of tuples.
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
 *   wrapper macro #PFla_lit_tbl() instead (which is available as
 *   #lit_tbl() if you have included the mnemonic constructor names in
 *   logical_mnemonic.h). This macro will detect the @a count
 *   argument on its own, so you only need to list all attribute
 *   specifictions.
 *
 * @b Example:
 *
 * @code
   PFla_op_t t = lit_tbl (attlist (att_iter, att_pos, att_item),
                          tuple (lit_int (1), lit_int (1), lit_str ("foo")),
                          tuple (lit_int (1), lit_int (2), lit_str ("bar")),
                          tuple (lit_int (2), lit_int (1), lit_str ("baz")));
@endcode
 */
PFla_op_t *
PFla_lit_tbl_ (PFalg_attlist_t attlist,
               unsigned int count, PFalg_tuple_t *tuples)
{
    PFla_op_t      *ret;      /* return value we are building */
    unsigned int    i;
    unsigned int    j;

    /*
     * We have a better constructor/node kind for empty tables.
     * (Facilitates optimization.)
     */
    if (count == 0)
        return PFla_empty_tbl (attlist);

    /* instantiate the new algebra operator node */
    ret = la_op_leaf (la_lit_tbl);

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
        for (j = 0; j < tuples[i].count; j++)
            ret->schema.items[j].type |= tuples[i].atoms[j].type;

    }

#ifndef NDEBUG
    { /* sanity checks. Do arguments match schema? */
        unsigned int tup;
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
 * Constructor for an empty table.  Use this constructor (in
 * preference over a literal table with no tuples) to trigger
 * optimization rules concerning empty relations.
 *
 * @param schema Attribute list with annotated types.
 *
 * This variant of the empty table constructor is meant to be
 * be used as replacement for algebra expressions for whom we.
 * can infer an empty result at compile time. For consistency
 * reasons we maintain the single column types.
 */
PFla_op_t *
PFla_empty_tbl_ (PFalg_schema_t schema)
{
    PFla_op_t     *ret;      /* return value we are building */
    unsigned int    i;

    /* instantiate the new algebra operator node */
    ret = la_op_leaf (la_empty_tbl);

    /* set its schema */
    ret->schema.items
        = PFmalloc (schema.count * sizeof (*(ret->schema.items)));
    for (i = 0; i < (unsigned int) schema.count; i++) {
        ret->schema.items[i] = schema.items[i];
    }
    ret->schema.count = schema.count;

    return ret;
}


/**
 * Constructor for an empty table.  Use this constructor (in
 * preference over a literal table with no tuples) to trigger
 * optimization rules concerning empty relations.
 *
 * @param attlist Attribute list, similar to the literal table
 *                constructor PFla_lit_tbl(), see also
 *                PFalg_attlist().
 */
PFla_op_t *
PFla_empty_tbl (PFalg_attlist_t attlist)
{
    PFla_op_t     *ret;      /* return value we are building */
    unsigned int    i;

    /* instantiate the new algebra operator node */
    ret = la_op_leaf (la_empty_tbl);

    /* set its schema */
    ret->schema.items
        = PFmalloc (attlist.count * sizeof (*(ret->schema.items)));
    for (i = 0; i < (unsigned int) attlist.count; i++) {
        ret->schema.items[i].name = attlist.atts[i];
        ret->schema.items[i].type = 0;
    }
    ret->schema.count = attlist.count;

    return ret;
}


/**
 * Construct an algebra node representing a referenced table,
 * given a (external) name, a (internal) schema, a list of the
 * external attribute/column names, and a list of the (internal)
 * key attributes.
 *
 * @param name    The name of the referenced table.
 * @param schema  Attribute list ("internal" attribute names)
 *                with annotated types.
 * @param tatts   String list ("external" attribute/column
 *                names).
 * @param keys    Array holding the *positions* (w.r.t. the
 *                schema) of key attributes
 */
PFla_op_t *
PFla_ref_tbl_ (const char* name, PFalg_schema_t schema, PFarray_t* tatts,
               PFarray_t* keys)
{
    PFla_op_t      *ret;      /* return value we are building */

    assert(name);

    /* instantiate the new algebra operator node */
    ret = la_op_leaf (la_ref_tbl);

    /* set its schema */
    /* deep copy the schema parameter*/
    ret->schema.items
        = PFmalloc (schema.count * sizeof (*(ret->schema.items)));
    for (unsigned int i = 0; i < schema.count; i++) {
        ret->schema.items[i] = schema.items[i];
    }
    ret->schema.count = schema.count;

    /* set its semantical infos */
    /* deep copy the name of the referenced table*/
    ret->sem.ref_tbl.name = PFstrdup(name);

    /* deep copy the "original column names" of the referenced table*/
    ret->sem.ref_tbl.tatts = PFarray(sizeof (char*));
    for (unsigned int i = 0; i < PFarray_last (tatts); i++)
    {
            char* value = *(char**) PFarray_at (tatts, i);
            char* copiedValue = PFstrdup(value);
            *(char**) PFarray_add (ret->sem.ref_tbl.tatts) = copiedValue;
    }

    /* (it's save to) shallow copy the list of key-attribute-names */
    ret->sem.ref_tbl.keys  = PFarray_copy(keys);

    /* return the new algebra operator node */
    return ret;
}


/**
 * ColumnAttach: Attach a column to a table.
 *
 * If you want to attach more than one column, apply ColumnAttach
 * multiple times.
 *
 * @param n     Input relation
 * @param res   Name of the new column.
 * @parma value Value for the new column.
 */
PFla_op_t *
PFla_attach (const PFla_op_t *n, PFalg_att_t res, PFalg_atom_t value)
{
    PFla_op_t   *ret = la_op_wire1 (la_attach, n);

    /* result schema is input schema plus new columns */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add new column */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = res, .type = value.type };

    ret->sem.attach.res   = res;
    ret->sem.attach.value = value;

    return ret;
}


/**
 * Cross product (Cartesian product) between two algebra expressions.
 * Arguments @a n1 and @a n2 must not have any equally named attribute.
 */
PFla_op_t *
PFla_cross (const PFla_op_t *n1, const PFla_op_t *n2)
{
    PFla_op_t    *ret = la_op_wire2 (la_cross, n1, n2);
    unsigned int  i;
    unsigned int  j;

    assert (n1); assert (n2);

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
            if (n1->schema.items[i].name == n2->schema.items[j].name)
                PFoops (OOPS_FATAL,
                        "duplicate attribute `%s' in cross product",
                        PFatt_str (n2->schema.items[j].name));
#endif
    }


    return ret;
}


/**
 * Cross product (Cartesian product) between two algebra expressions.
 * Arguments @a n1 and @a n2 may have equally named attributes.
 */
PFla_op_t *
PFla_cross_clone (const PFla_op_t *n1, const PFla_op_t *n2)
{
    PFla_op_t   *ret = la_op_wire2 (la_cross_mvd, n1, n2);
    unsigned int i;
    unsigned int j;
    unsigned int count;

    assert (n1); assert (n2);

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 1 */
    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    count = n1->schema.count;

    /* copy schema from argument 2, check for duplicate attribute names
       and discard if present */
    for (j = 0; j < n2->schema.count; j++) {
        for (i = 0; i < n1->schema.count; i++)
            if (n1->schema.items[i].name == n2->schema.items[j].name)
                break;

        /* no duplicate attribute found */
        if (i == n1->schema.count)
            ret->schema.items[count++] = n2->schema.items[j];
    }
    /* fix size of the schema */
    ret->schema.count = count;

    return ret;
}


/**
 * Equi-join between two operator nodes.
 *
 * Assert that @a att1 is an attribute of @a n1 and @a att2 is an attribute
 * of @a n2. @a n1 and @a n2 must not have duplicate attribute names.
 * The schema of the result is (schema(@a n1) + schema(@a n2)).
 */
PFla_op_t *
PFla_eqjoin (const PFla_op_t *n1, const PFla_op_t *n2,
             PFalg_att_t att1, PFalg_att_t att2)
{
    PFla_op_t     *ret;
    unsigned int   i;
    unsigned int   j;

    assert (n1); assert (n2);

    /* verify that att1 is attribute of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (att1 == n1->schema.items[i].name)
            break;

    /* did we find attribute att1? */
    if (i >= n1->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in join not found",
                PFatt_str(att1));

    /* ... and att2 is attribute of n2 */
    for (i = 0; i < n2->schema.count; i++)
        if (att2 == n2->schema.items[i].name)
            break;

    /* did we find attribute att2? */
    if (i >= n2->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in join not found",
                PFatt_str (att2));

    /* build new equi-join node */
    ret = la_op_wire2 (la_eqjoin, n1, n2);

    /* insert semantic value (join attributes) into the result */
    ret->sem.eqjoin.att1 = att1;
    ret->sem.eqjoin.att2 = att2;

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
            if (n1->schema.items[i].name == n2->schema.items[j].name)
                PFoops (OOPS_FATAL,
                        "duplicate attribute `%s' in equi-join",
                        PFatt_str (n2->schema.items[j].name));
#endif
    }

    return ret;
}


/**
 * Equi-join between two operator nodes.
 *
 * Assert that @a att1 is an attribute of @a n1 and @a att2 is an attribute
 * of @a n2. The schema of the result is:
 * schema(@a n1) + schema(@a n2) - duplicate columns.
 */
PFla_op_t *
PFla_eqjoin_clone (const PFla_op_t *n1, const PFla_op_t *n2,
                   PFalg_att_t att1, PFalg_att_t att2, PFalg_att_t res)
{
    PFla_op_t    *ret;
    unsigned int  i;
    unsigned int  count;

    assert (n1); assert (n2);

    /* verify that att1 is attribute of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (att1 == n1->schema.items[i].name)
            break;

    /* did we find attribute att1? */
    if (i >= n1->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in join not found",
                PFatt_str(att1));

    /* ... and att2 is attribute of n2 */
    for (i = 0; i < n2->schema.count; i++)
        if (att2 == n2->schema.items[i].name)
            break;

    /* did we find attribute att2? */
    if (i >= n2->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in join not found",
                PFatt_str (att2));

    /* build new equi-join node */
    ret = la_op_wire2 (la_eqjoin_unq, n1, n2);

    /* insert semantic value (join attributes) into the result */
    ret->sem.eqjoin_unq.att1 = att1;
    ret->sem.eqjoin_unq.att2 = att2;
    ret->sem.eqjoin_unq.res  = res;

    /* allocate memory for the result schema (schema(n1) + schema(n2)) */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    count = 0;
    /* add join attribute to the result */
    for (i = 0; i < n1->schema.count; i++)
        if (n1->schema.items[i].name == att1) {
            ret->schema.items[count] = n1->schema.items[i];
            ret->schema.items[count++].name = res;
            break;
        }

    /* copy schema from argument 'n1' */
    for (i = 0; i < n1->schema.count; i++)
        /* discard join columns - they are already added */
        if (n1->schema.items[i].name != att1 &&
            n1->schema.items[i].name != att2)
            ret->schema.items[count++] = n1->schema.items[i];

    /* copy schema from argument 'n2' */
    for (i = 0; i < n2->schema.count; i++)
        /* discard join columns - they are already added */
        if (n2->schema.items[i].name != att1 &&
            n2->schema.items[i].name != att2) {
            /* only include new columns */
            unsigned int j;
            for (j = 0; j < count; j++)
                if (ret->schema.items[j].name
                    == n2->schema.items[i].name)
                    break;

            if (j == count)
                ret->schema.items[count++] = n2->schema.items[i];
        }

    /* adjust schema size */
    ret->schema.count = count;

    return ret;
}


/**
 * Semi-join between two operator nodes.
 *
 * Assert that @a att1 is an attribute of @a n1 and @a att2 is an attribute
 * of @a n2. @a n1 and @a n2 must not have duplicate attribute names.
 * The schema of the result is (schema(@a n1)).
 */
PFla_op_t *
PFla_semijoin (const PFla_op_t *n1, const PFla_op_t *n2,
               PFalg_att_t att1, PFalg_att_t att2)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (n1); assert (n2);

    /* verify that att1 is attribute of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (att1 == n1->schema.items[i].name)
            break;

    /* did we find attribute att1? */
    if (i >= n1->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in join not found",
                PFatt_str(att1));

    /* ... and att2 is attribute of n2 */
    for (i = 0; i < n2->schema.count; i++)
        if (att2 == n2->schema.items[i].name)
            break;

    /* did we find attribute att2? */
    if (i >= n2->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in join not found",
                PFatt_str (att2));

    /* build new semi-join node */
    ret = la_op_wire2 (la_semijoin, n1, n2);

    /* insert semantic value (join attributes) into the result */
    ret->sem.eqjoin.att1 = att1;
    ret->sem.eqjoin.att2 = att2;

    /* allocate memory for the result schema (schema(n1)) */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n1' */
    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    return ret;
}


/**
 * Theta-join between two operator nodes.
 *
 * Assert that all columns listed in the predicate list are
 * available.  @a n1 and @a n2 must not have duplicate attribute names.
 * The schema of the result is (schema(@a n1) + schema(@a n2)).
 */
PFla_op_t *
PFla_thetajoin (const PFla_op_t *n1, const PFla_op_t *n2,
                unsigned int count, PFalg_sel_t *pred)
{
    PFla_op_t     *ret;
    unsigned int   i, j;

    assert (n1); assert (n2);

    /* verify that the join attributes are all available */
    for (i = 0; i < count; i++)
        if (!PFprop_ocol (n1, pred[i].left) ||
            !PFprop_ocol (n2, pred[i].right))
            break;

    /* did we find all attributes? */
    if (i < count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in theta-join not found",
                PFprop_ocol (n2, pred[i].right)
                ? PFatt_str(pred[i].left) : PFatt_str(pred[i].right));

    /* build new theta-join node */
    ret = la_op_wire2 (la_thetajoin, n1, n2);

    /* insert semantic value (predicates) into the result */
    ret->sem.thetajoin.count = count;
    ret->sem.thetajoin.pred  = PFmalloc (count * sizeof (PFalg_sel_t));
    for (i = 0; i < count; i++)
        ret->sem.thetajoin.pred[i] = pred[i];

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
            if (n1->schema.items[i].name == n2->schema.items[j].name)
                PFoops (OOPS_FATAL,
                        "duplicate attribute `%s' in theta-join",
                        PFatt_str (n2->schema.items[j].name));
#endif
    }

    return ret;
}


/**
 * Theta-join between two operator nodes.
 * Special internal variant used during thetajoin optimization.
 *
 * The optimizer will fill check for consistency and
 * fill in the correct schema (which requires internal knowledge
 * of @a data).
 */
PFla_op_t *
PFla_thetajoin_opt_internal (const PFla_op_t *n1, const PFla_op_t *n2,
                             PFarray_t *data)
{
    PFla_op_t     *ret;

    assert (n1); assert (n2);

    /* build new theta-join node */
    ret = la_op_wire2 (la_thetajoin, n1, n2);

    /* insert semantic value (predicates) into the result */
    ret->sem.thetajoin_opt.pred = PFarray_copy (data);

    /* allocate no schema -- this will be done by the optimization */

    return ret;
}

/**
 * Logical algebra projection/renaming.
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
 *   wrapper macro #PFla_project() instead (which is available as
 *   #project() if you have included the mnemonic constructor names in
 *   logical_mnemonic.h). This macro will detect the @a count
 *   argument on its own, so you only need to list all attribute
 *   specifictions.
 */
PFla_op_t *
PFla_project_ (const PFla_op_t *n, unsigned int count, PFalg_proj_t *proj)
{
    PFla_op_t     *ret = la_op_wire1 (la_project, n);
    unsigned int   i;
    unsigned int   j;

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
            if (ret->sem.proj.items[i].old == n->schema.items[j].name) {
                /* set name and type for this attribute in the result schema */
                ret->schema.items[i].name = ret->sem.proj.items[i].new;
                ret->schema.items[i].type = n->schema.items[j].type;

                break;
            }

        /* did we find the attribute? */
        if (j >= n->schema.count)
            PFoops (OOPS_FATAL,
                    "attribute `%s' referenced in projection not found",
                    PFatt_str (ret->sem.proj.items[i].old));

        /* see if we have duplicate attributes now */
        for (j = 0; j < i; j++)
            if (ret->sem.proj.items[i].new == ret->sem.proj.items[j].new)
                PFoops (OOPS_FATAL,
                        "projection results in duplicate attribute `%s' "
                        "(attributes %i and %i)",
                        PFatt_str (ret->sem.proj.items[i].new), i+1, j+1);
    }

    return ret;
}


/**
 * Selection of all rows where the value of column @a att is not 0.
 *
 * The result schema corresponds to the schema of the input
 * relation @a n.
 */
PFla_op_t*
PFla_select (const PFla_op_t *n, PFalg_att_t att)
{
    PFla_op_t    *ret;
    unsigned int  i;

    assert (n);

    /* verify that 'att' is an attribute of 'n' ... */
    for (i = 0; i < n->schema.count; i++)
        if (att == n->schema.items[i].name)
            break;

    /* did we find attribute att? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in selection not found",
                PFatt_str (att));

    /* build a new selection node */
    ret = la_op_wire1 (la_select, n);

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
 * Positional selection at position @a pos for each partition @a p
 * based on the ordering @a s.
 */
PFla_op_t *
PFla_pos_select (const PFla_op_t *n, int pos, PFord_ordering_t s, PFalg_att_t p)
{
    PFla_op_t    *ret = la_op_wire1 (la_pos_select, n);
    unsigned int  i;
    unsigned int  j;

    assert (s);

    /* copy parameters into semantic content of return node */
    ret->sem.pos_sel.pos = pos;
    ret->sem.pos_sel.sortby = s;
    ret->sem.pos_sel.part = p;

    /* result schema is input schema plus the new attribute */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* see if we can find all sort specifications */
    for (i = 0; i < PFord_count (ret->sem.pos_sel.sortby); i++) {

        for (j = 0; j < n->schema.count; j++)
            if (n->schema.items[j].name ==
                         PFord_order_col_at (ret->sem.pos_sel.sortby, i))
                break;

        if (j == n->schema.count)
            PFoops (OOPS_FATAL,
                    "could not find sort attribute `%s'",
                    PFatt_str (PFord_order_col_at (
                                   ret->sem.pos_sel.sortby, i)));

        if (!monomorphic (n->schema.items[j].type))
            PFoops (OOPS_FATAL,
                    "sort criterion for pos_sel must be monomorphic, "
                    "type: %i, name: %s",
                    n->schema.items[j].type,
                    PFatt_str (n->schema.items[j].name));

        if (ret->sem.pos_sel.part
            && PFord_order_col_at (ret->sem.pos_sel.sortby, i)
               == ret->sem.pos_sel.part)
            PFoops (OOPS_FATAL,
                    "partitioning attribute must not appear in sort clause");
    }

    return ret;
}


/**
 * worker for PFla_disjunion, PFla_intersect, and PFla_difference;
 */
static PFla_op_t *
set_operator (PFla_op_kind_t kind, const PFla_op_t *n1, const PFla_op_t *n2)
{
    PFla_op_t    *ret = la_op_wire2 (kind, n1, n2);
    unsigned int  i, j;

    assert (kind == la_disjunion ||
            kind == la_intersect ||
            kind == la_difference);

    /* see if both operands have same number of attributes */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of set operation (%s) "
                "do not match. (%i #cols != %i #cols)",
                kind == la_disjunion
                ? "union"
                : kind == la_intersect
                  ? "intersect"
                  : "difference",
                n1->schema.count,
                n2->schema.count);

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each attribute of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two attributes match, so include their name
                 * and type information into the result. This allows
                 * for the order of schema items in n1 and n2 to be
                 * different.
                 */
                if (kind == la_disjunion)
                    ret->schema.items[i] =
                        (struct PFalg_schm_item_t)
                            { .name = n1->schema.items[i].name,
                              .type = n1->schema.items[i].type
                                    | n2->schema.items[j].type };
                else if (kind == la_intersect) {
                    /* return empty table in case of
                       a complete column type mismatch */
                    if (!(n1->schema.items[i].type &
                          n2->schema.items[j].type))
                        return PFla_empty_tbl_ (n1->schema);

                    ret->schema.items[i] =
                        (struct PFalg_schm_item_t)
                            { .name = n1->schema.items[i].name,
                              .type = n1->schema.items[i].type
                                    & n2->schema.items[j].type };
                }
                else if (kind == la_difference) {
                    /* return lhs argument in case of
                       a complete column type mismatch */
                    if (!(n1->schema.items[i].type &
                          n2->schema.items[j].type))
                        return (PFla_op_t *) n1;

                    ret->schema.items[i] =
                        (struct PFalg_schm_item_t)
                            { .name = n1->schema.items[i].name,
                              .type = n1->schema.items[i].type };
                }
                break;
            }

        if (j == n2->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of two arguments of set operation (union, "
                    "difference, intersect) do not match");
    }

    return ret;
}


/**
 * Disjoint union of two relations.
 * Both argument must have the same schema.
 */
PFla_op_t *
PFla_disjunion (const PFla_op_t *n1, const PFla_op_t *n2)
{
    return set_operator (la_disjunion, n1, n2);
}


/**
 * Intersection between two relations.
 * Both argument must have the same schema.
 */
PFla_op_t *
PFla_intersect (const PFla_op_t *n1, const PFla_op_t *n2)
{
    return set_operator (la_intersect, n1, n2);
}


/**
 * Difference of two relations.
 * Both argument must have the same schema.
 */
PFla_op_t *
PFla_difference (const PFla_op_t *n1, const PFla_op_t *n2)
{
    return set_operator (la_difference, n1, n2);
}


/** Constructor for duplicate elimination operators. */
PFla_op_t * PFla_distinct (const PFla_op_t *n)
{
    PFla_op_t    *ret = la_op_wire1 (la_distinct, n);
    unsigned int  i;

    /* allocate memory for the result schema; it's the same schema as n's */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n' */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    return ret;
}


/** Constructor for generic operator that extends the schema
    with a new column where each value is determined by the values
    of a single row (cardinality stays the same) */
PFla_op_t *
PFla_fun_1to1 (const PFla_op_t *n,
               PFalg_fun_t kind,
               PFalg_att_t res,
               PFalg_attlist_t refs)
{
    PFla_op_t          *ret;
    unsigned int        i, j, ix[refs.count];
    PFalg_simple_type_t res_type = 0;

    assert (n);

    /* verify that the referenced attributes in refs
       are really attributes of n ... */
    for (i = 0; i < refs.count; i++) {
        for (j = 0; j < n->schema.count; j++)
            if (n->schema.items[j].name == refs.atts[i])
                break;
        if (j == n->schema.count)
            PFoops (OOPS_FATAL,
                    "attribute `%s' referenced in generic function"
                    " operator not found (kind = %s)",
                    PFatt_str (refs.atts[i]), PFalg_fun_str(kind));
        ix[i] = j;
    }

    /* we want to perform some more consistency checks
       that are specific to certain operators */
    switch (kind) {
        /**
         * Depending on the @a kind parameter, we add, subtract, multiply, or
         * divide the two values of columns @a att1 and @a att2 and store the
         * result in newly created attribute @a res. @a res gets the same data
         * type as @a att1 and @a att2. The result schema corresponds to the
         * schema of the input relation @a n plus @a res.
         */
        case alg_fun_num_add:
        case alg_fun_num_subtract:
        case alg_fun_num_multiply:
        case alg_fun_num_divide:
        case alg_fun_num_modulo:
            assert (refs.count == 2);
            /* make sure both attributes are of the same numeric type */
            assert (n->schema.items[ix[0]].type == aat_nat ||
                    n->schema.items[ix[0]].type == aat_int ||
                    n->schema.items[ix[0]].type == aat_dec ||
                    n->schema.items[ix[0]].type == aat_dbl);
            assert (n->schema.items[ix[0]].type ==
                    n->schema.items[ix[1]].type);

            res_type = n->schema.items[ix[1]].type;
            break;

        case alg_fun_fn_abs:
        case alg_fun_fn_ceiling:
        case alg_fun_fn_floor:
        case alg_fun_fn_round:
            assert (refs.count == 1);
            /* make sure the attribute is of numeric type */
            assert (n->schema.items[ix[0]].type == aat_int ||
                    n->schema.items[ix[0]].type == aat_dec ||
                    n->schema.items[ix[0]].type == aat_dbl);

            res_type = n->schema.items[ix[0]].type;
            break;

        case alg_fun_fn_substring:
            assert (refs.count == 2);

            /* make sure both attributes are of type str & int */
            assert (n->schema.items[ix[0]].type == aat_str);
            assert (n->schema.items[ix[1]].type == aat_int);

            res_type = aat_str;
            break;

        case alg_fun_fn_substring_lng:
            assert (refs.count == 3);

            /* make sure attributes are of type str & int */
            assert (n->schema.items[ix[0]].type == aat_str);
            assert (n->schema.items[ix[1]].type == aat_int &&
                    n->schema.items[ix[2]].type == aat_int );

            res_type = aat_str;
            break;

        case alg_fun_fn_concat:
        case alg_fun_fn_substring_before:
        case alg_fun_fn_substring_after:
            assert (refs.count == 2);
            /* make sure both attributes are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_string_length:
            assert (refs.count == 1);
            /* make sure the attribute is of type string */
            assert (n->schema.items[ix[0]].type == aat_str);

            res_type = aat_int;
            break;

        case alg_fun_fn_normalize_space:
        case alg_fun_fn_upper_case:
        case alg_fun_fn_lower_case:
            assert (refs.count == 1);
            /* make sure the attribute is of type string */
            assert (n->schema.items[ix[0]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_contains:
        case alg_fun_fn_starts_with:
        case alg_fun_fn_ends_with:
        case alg_fun_fn_matches:
            assert (refs.count == 2);
            /* make sure both attributes are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_bln;
            break;

        case alg_fun_fn_matches_flag:
            assert (refs.count == 3);
            /* make sure all attributes are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str);

            res_type = aat_bln;
            break;

        case alg_fun_fn_translate:
        case alg_fun_fn_replace:
            assert (refs.count == 3);
            /* make sure all attributes are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_replace_flag:
            assert (refs.count == 4);
            /* make sure all attributes are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str &&
                    n->schema.items[ix[3]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_name:
        case alg_fun_fn_local_name:
        case alg_fun_fn_namespace_uri:
            assert (refs.count == 1);
            /* make sure attribute is of type node */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = aat_str;
            break;

        case alg_fun_fn_number:
            assert (refs.count == 1);
            res_type = aat_dbl;
            break;

        case alg_fun_fn_qname:
            assert (refs.count == 2);
            /* make sure both attributes are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_qname;
            break;

        case alg_fun_pf_fragment:
            assert (refs.count == 1);
            /* make sure attribute is of type node */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = aat_pnode;
            break;

        case alg_fun_pf_supernode:
            assert (refs.count == 1);
            /* make sure attribute is of type node */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = n->schema.items[ix[0]].type;
            break;

        case alg_fun_pf_add_doc_str:
            assert(refs.count == 3);

            /* make sure atts are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);
            assert(n->schema.items[ix[1]].type == aat_str);
            assert(n->schema.items[ix[2]].type == aat_str);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the attribute types*/
            res_type = aat_docmgmt | aat_path | aat_docnm | aat_colnm;
            break;

        case alg_fun_pf_add_doc_str_int:
            assert(refs.count == 4);

            /* make sure atts are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);
            assert(n->schema.items[ix[1]].type == aat_str);
            assert(n->schema.items[ix[2]].type == aat_str);
            assert(n->schema.items[ix[3]].type == aat_int);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the attribute types */
            res_type = aat_docmgmt | aat_path | aat_docnm | aat_colnm;
            break;

        case alg_fun_pf_del_doc:
            assert(refs.count == 1);

            /* make sure atts are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the attribute types */
            res_type = aat_docmgmt | aat_docnm;
            break;

        case alg_fun_upd_delete:
            assert(refs.count == 1);

            /* make sure that the attribute is a node */
            assert(n->schema.items[ix[0]].type & aat_node);

            /* the result type is aat_update bitwise OR the type of
               the target_node shifted 4 bits to the left */
            assert((n->schema.items[ix[0]].type << 4) & aat_node1);
            res_type = aat_update | (n->schema.items[ix[0]].type << 4);
            break;

        case alg_fun_upd_rename:
        case alg_fun_upd_insert_into_as_first:
        case alg_fun_upd_insert_into_as_last:
        case alg_fun_upd_insert_before:
        case alg_fun_upd_insert_after:
        case alg_fun_upd_replace_value_att:
        case alg_fun_upd_replace_value:
        case alg_fun_upd_replace_element:
        case alg_fun_upd_replace_node:
            assert(refs.count == 2);

            /* make some assertions according to the fun signature */
            switch (kind) {
                case alg_fun_upd_rename:
                    assert(n->schema.items[ix[0]].type & aat_node);
                    assert(n->schema.items[ix[1]].type & aat_qname);
                    assert((n->schema.items[ix[0]].type << 4) & aat_node1);
                    break;
                case alg_fun_upd_insert_into_as_first:
                case alg_fun_upd_insert_into_as_last:
                case alg_fun_upd_insert_before:
                case alg_fun_upd_insert_after:
                case alg_fun_upd_replace_node:
                    assert(n->schema.items[ix[0]].type & aat_node);
                    assert(n->schema.items[ix[1]].type & aat_node);
                    assert((n->schema.items[ix[0]].type << 4) & aat_node1);
                    break;
                case alg_fun_upd_replace_value_att:
                    assert(n->schema.items[ix[0]].type & aat_anode);
                    assert(n->schema.items[ix[1]].type & aat_uA);
                    assert((n->schema.items[ix[0]].type << 4) & aat_anode1);
                    break;
                case alg_fun_upd_replace_value:
                    assert(n->schema.items[ix[0]].type & aat_pnode);
                    assert(n->schema.items[ix[1]].type & aat_uA);
                    assert((n->schema.items[ix[0]].type << 4) & aat_pnode1);
                    break;
                case alg_fun_upd_replace_element:
                    assert(n->schema.items[ix[0]].type & aat_pnode);
                    assert(n->schema.items[ix[1]].type & aat_str);
                    assert((n->schema.items[ix[0]].type << 4) & aat_pnode1);
                    break;
                default: assert(!"should never reach here"); break;
            }

            /* the result type is aat_update bitwise OR the type of
               the target_node shifted 4 bits to the left bitwise OR
               the type of the second argument */
            res_type = aat_update | (n->schema.items[ix[0]].type << 4)
                                  |  n->schema.items[ix[1]].type;
            break;
    }

    /* create new generic function operator node */
    ret = la_op_wire1 (la_fun_1to1, n);

    /* insert semantic values into the result */
    ret->sem.fun_1to1.kind = kind;
    ret->sem.fun_1to1.res  = res;
    ret->sem.fun_1to1.refs = (PFalg_attlist_t) {
        .count = refs.count,
        .atts  = memcpy (PFmalloc (refs.count * sizeof (PFalg_att_t)),
                         refs.atts,
                         refs.count * sizeof (PFalg_att_t)) };

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' attribute */
    ret->schema.items[ret->schema.count - 1].name = res;
    ret->schema.items[ret->schema.count - 1].type = res_type;

    return ret;
}


/** Constructs an operator for an arithmetic addition. */
PFla_op_t *
PFla_add (const PFla_op_t *n,
          PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_add,
                          res,
                          PFalg_attlist (att1, att2));
}


/** Constructs an operator for an arithmetic subtraction. */
PFla_op_t *
PFla_subtract (const PFla_op_t *n,
               PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_subtract,
                          res,
                          PFalg_attlist (att1, att2));
}


/** Constructs an operator for an arithmetic multiplication. */
PFla_op_t *
PFla_multiply (const PFla_op_t *n,
               PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_multiply,
                          res,
                          PFalg_attlist (att1, att2));
}


/** Constructs an operator for an arithmetic division. */
PFla_op_t *
PFla_divide (const PFla_op_t *n,
             PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_divide,
                          res,
                          PFalg_attlist (att1, att2));
}


/** Constructs an operator for an arithmetic modulo operation. */
PFla_op_t *
PFla_modulo (const PFla_op_t *n,
             PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_modulo,
                          res,
                          PFalg_attlist (att1, att2));
}


/**
 * Constructor for numeric greater-than operators.
 *
 * The algebra operator `gt' works as follows: For each tuple, the
 * numeric value in attribute @a att1 is compared against @a att2.
 * If @a att1 is greater than @a att2 then the comparison yields
 * true, otherwise false. This value is returned as a boolean
 * value in the new attribute named by the argument @a res.
 *
 * @param n    The operand for the algebra operator (``The newly
 *             constructed node's child'')
 * @param res  Attribute name for the comparison result (This
 *             attribute will be appended to the schema.)
 * @param att1 Left operand of the `gt' operator.
 * @param att2 Right operand of the `gt' operator.
 */
PFla_op_t *
PFla_gt (const PFla_op_t *n,
         PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return compar_op (la_num_gt, n, res, att1, att2);
}


/** Constructor for numeric equal operators. */
PFla_op_t *
PFla_eq (const PFla_op_t *n,
         PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return compar_op (la_num_eq, n, res, att1, att2);
}


/** Constructor for boolean AND operators. */
PFla_op_t *
PFla_and (const PFla_op_t *n,
          PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return boolean_op (la_bool_and, n, res, att1, att2);
}


/** Constructor for boolean OR operators. */
PFla_op_t *
PFla_or (const PFla_op_t *n,
         PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    return boolean_op (la_bool_or, n, res, att1, att2);
}


/** Constructor for boolean NOT operators. */
PFla_op_t *
PFla_not (const PFla_op_t *n, PFalg_att_t res, PFalg_att_t att)
{
    return unary_op (la_bool_not, n, res, att);
}


/**
 * Encapsulates initialization stuff common to binary comparison
 * operators.
 *
 * Depending on the @a kind parameter, we connect the two values
 * of columns @a att1 and @a att2 and store the result in newly
 * created attribute @a res. @a res gets the same data type as @a
 * att1 and @a att2. The result schema corresponds to the schema
 * of the input relation @a n plus @a res.
 */
static PFla_op_t *
compar_op (PFla_op_kind_t kind, const PFla_op_t *n,
           PFalg_att_t res, PFalg_att_t att1, PFalg_att_t att2)
{
    PFla_op_t    *ret;
    unsigned int  i;
    int           ix1 = -1;
    int           ix2 = -1;

    assert (n);

    /* verify that 'att1' and 'att2' are attributes of n ... */
    for (i = 0; i < n->schema.count; i++) {
        if (att1 == n->schema.items[i].name)
            ix1 = i;                /* remember array index of att1 */
        if (att2 == n->schema.items[i].name)
            ix2 = i;                /* remember array index of att2 */
    }

    /* did we find attribute 'att1' and 'att2'? */
    if (ix1 < 0)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in arithmetic operation "
                "not found", PFatt_str (att1));
    if (ix2 < 0)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in arithmetic operation "
                "not found", PFatt_str (att2));

    /* make sure both attributes are of the same type */
    assert (n->schema.items[ix1].type == n->schema.items[ix2].type ||
            (n->schema.items[ix1].type & aat_node &&
             n->schema.items[ix2].type & aat_node));

    /* create new binary operator node */
    ret = la_op_wire1 (kind, n);

    /* insert semantic value (operand attributes and result attribute)
     * into the result
     */
    ret->sem.binary.att1 = att1;
    ret->sem.binary.att2 = att2;
    ret->sem.binary.res = res;

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
    ret->schema.items[ret->schema.count - 1].type = aat_bln;
    ret->schema.items[ret->schema.count - 1].name = res;

    return ret;
}


/**
 * Encapsulates initialization stuff common to binary boolean
 * operators.
 *
 * Depending on the @a kind parameter, we connect the two values
 * of columns @a att1 and @a att2 and store the result in newly
 * created attribute @a res. @a res gets the same data type as @a
 * att1 and @a att2. The result schema corresponds to the schema
 * of the input relation @a n plus @a res.
 */
static PFla_op_t *
boolean_op (PFla_op_kind_t kind, const PFla_op_t *n, PFalg_att_t res,
            PFalg_att_t att1, PFalg_att_t att2)
{
    PFla_op_t    *ret;
    unsigned int  i;
    int           ix1 = -1;
    int           ix2 = -1;

    assert (n);

    /* verify that 'att1' and 'att2' are attributes of n ... */
    for (i = 0; i < n->schema.count; i++) {
        if (att1 == n->schema.items[i].name)
            ix1 = i;                /* remember array index of att1 */
        if (att2 == n->schema.items[i].name)
            ix2 = i;                /* remember array index of att2 */
    }

    /* did we find attribute 'att1' and 'att2'? */
    if (ix1 < 0)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in binary operation "
                "not found", PFatt_str (att1));
    if (ix2 < 0)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in binary operation "
                "not found", PFatt_str (att2));

    /* make sure that both attributes are of type boolean */
    assert (n->schema.items[ix1].type == aat_bln);
    assert (n->schema.items[ix1].type == n->schema.items[ix2].type);

    /* create new binary operator node */
    ret = la_op_wire1 (kind, n);

    /* insert semantic value (operand attributes and result attribute)
     * into the result
     */
    ret->sem.binary.att1 = att1;
    ret->sem.binary.att2 = att2;
    ret->sem.binary.res = res;

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
    ret->schema.items[ret->schema.count - 1].type = n->schema.items[ix1].type;
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
static PFla_op_t *
unary_op(PFla_op_kind_t kind, const PFla_op_t *n, PFalg_att_t res,
         PFalg_att_t att)
{
    PFla_op_t    *ret;
    unsigned int  i;
    unsigned int  ix = 0;

    assert (n);

    /* verify that 'att' is an attribute of n ... */
    for (i = 0; i < n->schema.count; i++)
        if (att == n->schema.items[i].name) {
            ix = i;                /* remember array index of att */
            break;
        }

    /* did we find attribute 'att'? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in unary operation not found",
                PFatt_str (att));

    /* assert that 'att' is of correct type */
    if (kind == la_bool_not)
        assert (n->schema.items[ix].type == aat_bln);

    /* create new unary operator node */
    ret = la_op_wire1 (kind, n);

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
 * Constructor for op:to operator
 */
PFla_op_t *
PFla_to (const PFla_op_t *n, PFalg_att_t res,
         PFalg_att_t att1, PFalg_att_t att2)
{
    PFla_op_t    *ret = la_op_wire1 (la_to, n);

#ifndef NDEBUG
    /* verify that attributes 'att1' and 'att2' are attributes of n */
    if (!PFprop_ocol (n, att1))
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in op:to not found",
                PFatt_str (att1));
    if (!PFprop_ocol (n, att2))
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in op:to not found",
                PFatt_str (att2));
#endif

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' attribute; it is of type
     * integer and named 'res'
     */
    ret->schema.items[n->schema.count].name = res;
    ret->schema.items[n->schema.count].type = aat_int;

    /* insert semantic value (operand attributes and result attribute)
     * into the result
     */
    ret->sem.binary.att1 = att1;
    ret->sem.binary.att2 = att2;
    ret->sem.binary.res = res;

    return ret;
}


/**
 * Constructor for operators forming the application of a
 * (partitioned) aggregation function (sum, min, max and avg) on a column.
 *
 * The values of attribute @a att are used by the aggregation functaion.
 * The partitioning (group by) attribute is represented by @a part.
 * The result is stored in attribute @a res.
 */
PFla_op_t * PFla_aggr (PFla_op_kind_t kind, const PFla_op_t *n, PFalg_att_t res,
                      PFalg_att_t att, PFalg_att_t part)
{
    /* build a new aggr node */
    PFla_op_t    *ret = la_op_wire1 (kind, n);
    unsigned int  i;
    bool          c1 = false;
    bool          c2 = false;

    /* set number of schema items in the result schema
     * (partitioning attribute plus result attribute)
     */
    ret->schema.count = part ? 2 : 1;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));


    /* verify that attributes 'att' and 'part' are attributes of n
     * and include them into the result schema
     */
    for (i = 0; i < n->schema.count; i++) {
        if (att == n->schema.items[i].name) {
            ret->schema.items[0] = n->schema.items[i];
            ret->schema.items[0].name = res;
            c1 = true;
        }
        if (part && part == n->schema.items[i].name) {
            ret->schema.items[1] = n->schema.items[i];
            c2 = true;
        }
    }

    /* did we find attribute 'att'? */
    if (!c1)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in aggregation function not found",
                PFatt_str (att));

    /* did we find attribute 'part'? */
    if (part && !c2)
        PFoops (OOPS_FATAL,
                "partitioning attribute `%s' referenced in aggregation "
                "function not found",
                PFatt_str (part));

    /* insert semantic value (result (aggregated) attribute, partitioning
     * attribute(s), and result attribute) into the result
     */
    ret->sem.aggr.att = att;
    ret->sem.aggr.part = part;
    ret->sem.aggr.res = res;

    return ret;
}


/**
 * Constructor for (partitioned) row counting operators.
 *
 * Counts all rows with identical values in column @a part (which holds
 * the partitioning or group by column). The result is stored in
 * attribute @a res.
 */
PFla_op_t *
PFla_count (const PFla_op_t *n, PFalg_att_t res, PFalg_att_t part)
{
    PFla_op_t    *ret = la_op_wire1 (la_count, n);
    unsigned int  i;

    /* set number of schema items in the result schema
     * (partitioning attribute plus result attribute)
     */
    ret->schema.count = part ? 2 : 1;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy the partitioning attribute */
    if (part) {
        for (i = 0; i < n->schema.count; i++)
            if (n->schema.items[i].name == part) {
                ret->schema.items[1] = n->schema.items[i];
                break;
            }

        /* did we find attribute 'part'? */
        if (i >= n->schema.count)
            PFoops (OOPS_FATAL,
                    "partitioning attribute %s referenced in count operator "
                    "not found", PFatt_str (part));
    }

    /* insert result attribute into schema */
    ret->schema.items[0].name = res;
    ret->schema.items[0].type = aat_int;

    /* insert semantic value (partitioning and result attribute) into
     * the result
     */
    ret->sem.aggr.part = part;
    ret->sem.aggr.res  = res;
    ret->sem.aggr.att  = att_NULL; /* don't use att field */


    return ret;
}


/**
 * Worker for PFla_rownum, PFla_rowrank, and PFla_rank.
 */
static PFla_op_t *
sort_col (const PFla_op_t *n, PFla_op_kind_t kind, char *name,
          PFalg_att_t a, PFord_ordering_t s, PFalg_att_t p)
{
    PFla_op_t    *ret = la_op_wire1 (kind, n);
    unsigned int  i;
    unsigned int  j;

    assert (s);

    /* copy parameters into semantic content of return node */
    ret->sem.sort.res = a;
    ret->sem.sort.sortby = s;
    ret->sem.sort.part = p;

    /* result schema is input schema plus the new attribute */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++) {
        /* there must not be an argument attribute named like the new one */
        if (n->schema.items[i].name == a)
            PFoops (OOPS_FATAL,
                    "%s operator would result in duplicate attribute `%s'",
                    name, PFatt_str (a));

        /* copy this attribute specification */
        ret->schema.items[i] = n->schema.items[i];
    }
    /* append new attribute, named as given in a, with type nat */
    ret->schema.items[ret->schema.count - 1]
        = (struct PFalg_schm_item_t) { .name = a, .type = aat_nat };

    /* see if we can find all sort specifications */
    for (i = 0; i < PFord_count (s); i++) {
        for (j = 0; j < n->schema.count; j++)
            if (n->schema.items[j].name == PFord_order_col_at (s, i))
                break;

        if (j == n->schema.count)
            PFoops (OOPS_FATAL,
                    "could not find sort attribute `%s'",
                    PFatt_str (PFord_order_col_at (s, i)));

        if (ret->sem.sort.part &&
            PFord_order_col_at (s, i) == ret->sem.sort.part)
            PFoops (OOPS_FATAL,
                    "partitioning attribute must not appear in sort clause");
    }

    return ret;
}


/**
 * The `rownum' operator, a Pathfinder-specific extension to the
 * relational algebra (behaves like SQL ROW_NUMBER()).
 */
PFla_op_t *
PFla_rownum (const PFla_op_t *n,
             PFalg_att_t a, PFord_ordering_t s, PFalg_att_t p)
{
    return sort_col (n, la_rownum, "rownum", a, s, p);
}


/**
 * The `rowrank' operator, a Pathfinder-specific extension to the
 * relational algebra (behaves like SQL DENSE_RANK()).
 */
PFla_op_t *
PFla_rowrank (const PFla_op_t *n, PFalg_att_t a, PFord_ordering_t s)
{
    if (PFord_count (s) == 0)
        PFinfo (OOPS_FATAL,
                "applying rank operator without sort specifier");

    return sort_col (n, la_rowrank, "rowrank", a, s, att_NULL);
}


/**
 * The `rank' operator, a Pathfinder-specific extension to the
 * relational algebra (behaves like SQL DENSE_RANK()). In comparison
 * to rowrank we do not care about the generated values and can
 * therefore apply more rewrites.
 */
PFla_op_t *
PFla_rank (const PFla_op_t *n, PFalg_att_t a, PFord_ordering_t s)
{
    if (PFord_count (s) == 0)
        PFinfo (OOPS_FATAL,
                "applying rank operator without sort specifier");

    return sort_col (n, la_rank, "rank", a, s, att_NULL);
}


/**
 * The `rowid' operator, a Pathfinder-specific extension to the
 * relational algebra.
 */
PFla_op_t *
PFla_rowid (const PFla_op_t *n, PFalg_att_t a)
{
    PFla_op_t    *ret = la_op_wire1 (la_rowid, n);
    unsigned int  i;

    /* copy parameters into semantic content of return node */
    ret->sem.rowid.res  = a;

    /* result schema is input schema plus the new attribute */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++) {

        /* there must not be an argument attribute named like the new one */
        if (n->schema.items[i].name == a)
            PFoops (OOPS_FATAL,
                    "rowid operator would result in duplicate attribute `%s'",
                    PFatt_str (a));

        /* copy this attribute specification */
        ret->schema.items[i] = n->schema.items[i];
    }
    /* append new attribute, named as given in a, with type nat */
    ret->schema.items[ret->schema.count - 1]
        = (struct PFalg_schm_item_t) { .name = a, .type = aat_nat };

    return ret;
}


/**
 * Constructor for type test on column values. The result is
 * stored in newly created column @a res.
 */
PFla_op_t *
PFla_type (const PFla_op_t *n,
           PFalg_att_t res, PFalg_att_t att, PFalg_simple_type_t ty)
{
    PFla_op_t    *ret;
    unsigned int  i;

    assert (n);

    /* verify that 'att' is an attribute of 'n' ... */
    for (i = 0; i < n->schema.count; i++)
        if (att == n->schema.items[i].name)
            break;

    /* did we find attribute att? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in type test not found",
                PFatt_str (att));

    /* create new type test node */
    ret = la_op_wire1 (la_type, n);

    /* insert semantic value (type-tested attribute and its type,
     * result attribute) into the result
     */
    ret->sem.type.att = att;
    ret->sem.type.ty  = ty;
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
 * Constructor for type assertion check. The result is the
 * input relation n where the type of attribute att is replaced
 * by ty
 */
PFla_op_t * PFla_type_assert (const PFla_op_t *n, PFalg_att_t att,
                              PFalg_simple_type_t ty, bool pos)
{
    PFla_op_t    *ret;
    PFalg_simple_type_t assert_ty = 0;
    unsigned int  i;

    assert (n);

    /* verify that 'att' is an attribute of 'n' ... */
    for (i = 0; i < n->schema.count; i++)
        if (att == n->schema.items[i].name)
        {
            if (pos)
                assert_ty = n->schema.items[i].type & ty;
            else
                /* the restricted type assert_ty is the original
                   type without type ty */
                assert_ty = n->schema.items[i].type -
                            (n->schema.items[i].type & ty);
            break;
        }

    /* did we find attribute att? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in type_assertion not found",
                PFatt_str (att));

    /* if we statically know that the type assertion would yield
       an empty type we can replace it by an empty table */
    if (!assert_ty) {
        /* instantiate the new algebra operator node */
        ret = la_op_leaf (la_empty_tbl);

        /* set its schema */
        ret->schema.count = n->schema.count;
        ret->schema.items
            = PFmalloc (n->schema.count * sizeof (PFalg_schema_t));

        for (i = 0; i < n->schema.count; i++)
            if (att == n->schema.items[i].name)
            {
                ret->schema.items[i].name = att;
                ret->schema.items[i].type = ty;
            }
            else
                ret->schema.items[i] = n->schema.items[i];

        return ret;
    }

    /* create new type test node */
    ret = la_op_wire1 (la_type_assert, n);

    /* insert semantic value (type-tested attribute and its type,
     * result attribute) into the result
     */
    ret->sem.type.att = att;
    ret->sem.type.ty  = assert_ty;
    ret->sem.type.res = att_NULL; /* don't use res field */

    ret->schema.count = n->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
    {
        if (att == n->schema.items[i].name)
        {
            ret->schema.items[i].name = att;
            ret->schema.items[i].type = assert_ty;
        }
        else
            ret->schema.items[i] = n->schema.items[i];
    }

    return ret;
}


/**
 * Constructor for a type cast of column @a att. The type of @a att
 * must be casted to type @a ty.
 */
PFla_op_t *
PFla_cast (const PFla_op_t *n, PFalg_att_t res,
           PFalg_att_t att, PFalg_simple_type_t ty)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (n);
    assert (res != att);

    /* verify that att is an attribute of n ... */
    for (i = 0; i < n->schema.count; i++)
        if (att == n->schema.items[i].name)
            break;

    /* did we find attribute att? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in type cast not found",
                PFatt_str (att));

    /* create new type cast node */
    ret = la_op_wire1 (la_cast, n);

    /*
     * insert semantic value (type-tested attribute and its type)
     * into the result
     */
    ret->sem.type.att = att;
    ret->sem.type.ty = ty;
    ret->sem.type.res = res;

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = n->schema.count + 1;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];
    ret->schema.items[ret->schema.count - 1]
        = (PFalg_schm_item_t) { .name = res, .type = ty };

    return ret;
}

/**
 * Constructor for algebra `seqty1' operator.
 *
 * This operator is particularly crafted to test the occurrence
 * indicator ``exactly one'' (`1'). It groups its argument according
 * to the attribute @a part. For each partition it will look at the
 * value attribute @a att. If there is exactly one tuple for the
 * partition, and if the value of @a att is @c true for this tuple,
 * the result for this partition will be @c true. In all other cases
 * (There is more than one tuple, or the single tuple contains @c false
 * in @a att.) the result for this partition will be @c false.
 */
PFla_op_t *
PFla_seqty1 (const PFla_op_t *n,
             PFalg_att_t res, PFalg_att_t att, PFalg_att_t part)
{
    unsigned int  i;
    bool          att_found = false;
    bool          part_found = false;
    PFla_op_t    *ret;

    assert (n);
    assert (res); assert (att); assert (part);

    /* sanity checks: value attribute must not equal partitioning attribute */
    if (att == part)
        PFoops (OOPS_FATAL,
                "seqty1 operator: value attribute equals partitioning "
                "attribute.");

    /* both attributes must exist and must have appropriate type */
    for (i = 0; i < n->schema.count; i++) {
        if (att == n->schema.items[i].name) {
            att_found = true;
            if (n->schema.items[i].type != aat_bln)
                PFoops (OOPS_FATAL,
                        "algebra operator `seqty1' only allowed on boolean "
                        "attributes. (attribute `%s')", PFatt_str (att));
        }
        if (part == n->schema.items[i].name)
            part_found = true;
    }

    if (!att_found || !part_found)
        PFoops (OOPS_FATAL,
                "seqty1: value attribute or partitioning attribute not found.");

    /* Now we can actually construct the result node */
    ret = la_op_wire1 (la_seqty1, n);

    ret->sem.aggr.res  = res;
    ret->sem.aggr.att  = att;
    ret->sem.aggr.part = part;

    ret->schema.count = 2;
    ret->schema.items = PFmalloc (2 * sizeof (PFalg_schema_t));

    ret->schema.items[0].name = part;
    ret->schema.items[0].type = PFprop_type_of (n, part);
    ret->schema.items[1].name = res;
    ret->schema.items[1].type = aat_bln;

    return ret;
}


/**
 * Construction operator for algebra `all' operator.
 *
 * The `all' operator looks into a group of tuples (by partitioning
 * attribute @a part), and returns @c true for this group iff all
 * values in attribute @a att for this group are @c true.
 *
 * This operator is used, e.g., to back the occurence indicators `+'
 * and `*'.
 */
PFla_op_t *
PFla_all (const PFla_op_t *n,
          PFalg_att_t res, PFalg_att_t att, PFalg_att_t part)
{
    unsigned int  i;
    bool          att_found = false;
    bool          part_found = false;
    PFla_op_t    *ret;

    assert (n);
    assert (res); assert (att); assert (part);

    /* sanity checks: value attribute must not equal partitioning attribute */
    if (att == part)
        PFoops (OOPS_FATAL,
                "all operator: value attribute equals partitioning "
                "attribute.");

    /* both attributes must exist and must have appropriate type */
    for (i = 0; i < n->schema.count; i++) {
        if (att == n->schema.items[i].name) {
            att_found = true;
            if (n->schema.items[i].type != aat_bln)
                PFoops (OOPS_FATAL,
                        "algebra operator `all' only allowed on boolean "
                        "attributes. (attribute `%s')", PFatt_str (att));
        }
        if (part  == n->schema.items[i].name)
            part_found = true;
    }

    if (!att_found || !part_found)
        PFoops (OOPS_FATAL,
                "all: value attribute or partitioning attribute not found.");

    /* Now we can actually construct the result node */
    ret = la_op_wire1 (la_all, n);

    ret->sem.aggr.res  = res;
    ret->sem.aggr.att  = att;
    ret->sem.aggr.part = part;

    ret->schema.count = 2;
    ret->schema.items = PFmalloc (2 * sizeof (PFalg_schema_t));

    ret->schema.items[0].name = part;
    ret->schema.items[0].type = PFprop_type_of (n, part);
    ret->schema.items[1].name = res;
    ret->schema.items[1].type = aat_bln;

    return ret;
}


/**
 * Path step operator.
 *
 * Each such step operator corresponds to the evaluation of an XPath
 * location step starting from the context nodes in column @a item.
 * @a doc is not a "real" algebra node, but just serves
 * as a container for semantic information on the kind test and location
 * step.
 */
PFla_op_t *
PFla_step (const PFla_op_t *doc, const PFla_op_t *n,
           PFalg_step_spec_t spec, int level,
           PFalg_att_t iter, PFalg_att_t item,
           PFalg_att_t item_res)
{
    PFla_op_t    *ret;
#ifndef NDEBUG
    unsigned int  i;
#endif

    assert (n); assert (doc);

    /* create new join node */
    ret = la_op_wire2 (la_step, doc, n);

    /* insert semantic value (axis/kind test, col names) into the result */
    ret->sem.step.spec        = spec;
    ret->sem.step.guide_count = 0;
    ret->sem.step.guides      = NULL;
    ret->sem.step.level       = level;
    ret->sem.step.iter        = iter;
    ret->sem.step.item        = item;
    ret->sem.step.item_res    = item_res;

#ifndef NDEBUG
    /* verify schema of 'n' */
    for (i = 0; i < n->schema.count; i++) {
        if (n->schema.items[i].name == iter
         || n->schema.items[i].name == item)
            continue;
        else
            PFoops (OOPS_FATAL,
                    "illegal attribute `%s' in path step",
                    PFatt_str (n->schema.items[i].name));
    }
    if (!(PFprop_type_of (n, item) & aat_node))
        PFoops (OOPS_FATAL,
                "wrong item type '0x%X' in the input of a path step",
                PFprop_type_of (n, item));
#endif


    /* allocate memory for the result schema */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    ret->schema.items[0]
        = (struct PFalg_schm_item_t) { .name = iter,
                                       .type = PFprop_type_of (n, iter) };
    /* the result of an attribute axis is also of type attribute */
    if (ret->sem.step.spec.axis == alg_attr)
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_anode };
    else
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_pnode };

    return ret;
}

PFla_op_t *
PFla_step_simple (const PFla_op_t *doc, const PFla_op_t *n,
                  PFalg_step_spec_t spec,
                  PFalg_att_t iter, PFalg_att_t item,
                  PFalg_att_t item_res)
{
    return PFla_step (doc, n, spec, -1, iter, item, item_res);
}


/**
 * Path step operator (without duplicate removal).
 *
 * Each such step operator corresponds to the evaluation of an XPath
 * location step starting from the context nodes in column @a item.
 * @a doc is not a "real" algebra node, but just serves
 * as a container for semantic information on the kind test and location
 * step.
 */
PFla_op_t *
PFla_step_join (const PFla_op_t *doc, const PFla_op_t *n,
                PFalg_step_spec_t spec, int level,
                PFalg_att_t item,
                PFalg_att_t item_res)
{
    PFla_op_t    *ret;
    unsigned int  i;

    assert (n); assert (doc);

    /* create new join node */
    ret = la_op_wire2 (la_step_join, doc, n);

    /* insert semantic value (axis/kind test, col names) into the result */
    ret->sem.step.spec        = spec;
    ret->sem.step.guide_count = 0;
    ret->sem.step.guides      = NULL;
    ret->sem.step.level       = level;
    ret->sem.step.iter        = att_NULL;
    ret->sem.step.item        = item;
    ret->sem.step.item_res    = item_res;

#ifndef NDEBUG
    if (PFprop_ocol (n, item_res))
        PFoops (OOPS_FATAL,
                "illegal attribute `%s' in the input "
                "of a path step",
                PFatt_str (item_res));
    if (!PFprop_ocol (n, item))
        PFoops (OOPS_FATAL,
                "column `%s' needed in path step is missing",
                PFatt_str (item));
    if (!(PFprop_type_of (n, item) & aat_node))
        PFoops (OOPS_FATAL,
                "wrong item type '0x%X' in the input of a path step",
                PFprop_type_of (n, item));
#endif

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* the result of an attribute axis is also of type attribute */
    if (ret->sem.step.spec.axis == alg_attr)
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_anode };
    else
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_pnode };

    return ret;
}

PFla_op_t *
PFla_step_join_simple (const PFla_op_t *doc, const PFla_op_t *n,
                       PFalg_step_spec_t spec,
                       PFalg_att_t item, PFalg_att_t item_res)
{
    return PFla_step_join (doc, n, spec, -1, item, item_res);
}


/**
 * Path step operator (with guide node information).
 *
 * Each such step operator corresponds to the evaluation of an XPath
 * location step starting from the context nodes in column @a item.
 * @a doc is not a "real" algebra node, but just serves
 * as a container for semantic information on the kind test and location
 * step.
 */
PFla_op_t *
PFla_guide_step (const PFla_op_t *doc, const PFla_op_t *n,
                 PFalg_step_spec_t spec,
                 unsigned int guide_count, PFguide_tree_t **guides,
                 int level,
                 PFalg_att_t iter, PFalg_att_t item,
                 PFalg_att_t item_res)
{
    PFla_op_t    *ret;
#ifndef NDEBUG
    unsigned int  i;
#endif

    assert (n); assert (doc);

    /* create new join node */
    ret = la_op_wire2 (la_guide_step, doc, n);

    /* insert semantic value (axis/kind test, col names) into the result */
    ret->sem.step.spec        = spec;
    ret->sem.step.guide_count = guide_count;
    ret->sem.step.guides      = memcpy (PFmalloc (guide_count *
                                                  sizeof (PFguide_tree_t *)),
                                        guides,
                                        guide_count *
                                        sizeof (PFguide_tree_t *));
    ret->sem.step.level       = level;
    ret->sem.step.iter        = iter;
    ret->sem.step.item        = item;
    ret->sem.step.item_res    = item_res;

#ifndef NDEBUG
    /* verify schema of 'n' */
    for (i = 0; i < n->schema.count; i++) {
        if (n->schema.items[i].name == iter
         || n->schema.items[i].name == item)
            continue;
        else
            PFoops (OOPS_FATAL,
                    "illegal attribute `%s' in path step",
                    PFatt_str (n->schema.items[i].name));
    }
    if (!(PFprop_type_of (n, item) & aat_node))
        PFoops (OOPS_FATAL,
                "wrong item type (%i) in the input of a path step",
                PFprop_type_of (n, item));
#endif

    /* allocate memory for the result schema */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    ret->schema.items[0]
        = (struct PFalg_schm_item_t) { .name = iter,
                                       .type = PFprop_type_of (n, iter) };
    /* the result of an attribute axis is also of type attribute */
    if (ret->sem.step.spec.axis == alg_attr)
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_anode };
    else
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_pnode };

    return ret;
}

PFla_op_t *
PFla_guide_step_simple (const PFla_op_t *doc, const PFla_op_t *n,
                        PFalg_step_spec_t spec,
                        unsigned int guide_count, PFguide_tree_t **guides,
                        PFalg_att_t iter, PFalg_att_t item,
                        PFalg_att_t item_res)
{
    return PFla_guide_step (
               doc, n,
               spec,
               guide_count, guides,
               -1,
               iter, item, item_res);
}


/**
 * Path step operator (without duplicate removal and with guide
 * information).
 *
 * Each such step operator corresponds to the evaluation of an XPath
 * location step starting from the context nodes in column @a item.
 * @a doc is not a "real" algebra node, but just serves
 * as a container for semantic information on the kind test and location
 * step.
 */
PFla_op_t *
PFla_guide_step_join (const PFla_op_t *doc, const PFla_op_t *n,
                      PFalg_step_spec_t spec,
                      unsigned int guide_count, PFguide_tree_t **guides,
                      int level, PFalg_att_t item, PFalg_att_t item_res)
{
    PFla_op_t    *ret;
    unsigned int  i;

    assert (n); assert (doc);

    /* create new join node */
    ret = la_op_wire2 (la_guide_step_join, doc, n);

    /* insert semantic value (axis/kind test, col names) into the result */
    ret->sem.step.spec        = spec;
    ret->sem.step.guide_count = guide_count;
    ret->sem.step.guides      = memcpy (PFmalloc (guide_count *
                                                  sizeof (PFguide_tree_t *)),
                                        guides,
                                        guide_count *
                                        sizeof (PFguide_tree_t *));
    ret->sem.step.level       = level;
    ret->sem.step.iter        = att_NULL;
    ret->sem.step.item        = item;
    ret->sem.step.item_res    = item_res;

#ifndef NDEBUG
    if (PFprop_ocol (n, item_res))
        PFoops (OOPS_FATAL,
                "illegal attribute `%s' in the input "
                "of a path step",
                PFatt_str (item_res));
    if (!PFprop_ocol (n, item))
        PFoops (OOPS_FATAL,
                "column `%s' needed in path step is missing",
                PFatt_str (item));
    if (!(PFprop_type_of (n, item) & aat_node))
        PFoops (OOPS_FATAL,
                "wrong item type '0x%X' in the input of a path step",
                PFprop_type_of (n, item));
#endif

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* the result of an attribute axis is also of type attribute */
    if (ret->sem.step.spec.axis == alg_attr)
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_anode };
    else
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_pnode };

    return ret;
}

PFla_op_t *
PFla_guide_step_join_simple (const PFla_op_t *doc, const PFla_op_t *n,
                             PFalg_step_spec_t spec,
                             unsigned int guide_count, PFguide_tree_t **guides,
                             PFalg_att_t item, PFalg_att_t item_res)
{
    return PFla_guide_step_join (
               doc, n,
               spec,
               guide_count, guides,
               -1, item, item_res);
}


PFla_op_t *
PFla_doc_index_join (const PFla_op_t *doc, const PFla_op_t *n,
                     PFla_doc_join_kind_t kind,
                     PFalg_att_t item,
                     PFalg_att_t item_res, PFalg_att_t item_doc)
{
    PFla_op_t    *ret;
    unsigned int  i;

    assert (n); assert (doc);

    /* create new join node */
    ret = la_op_wire2 (la_doc_index_join, doc, n);

    /* insert semantic value into the result */
    ret->sem.doc_join.kind     = kind;
    ret->sem.doc_join.item     = item;
    ret->sem.doc_join.item_res = item_res;
    ret->sem.doc_join.item_doc = item_doc;

#ifndef NDEBUG
    if (PFprop_ocol (n, item_res))
        PFoops (OOPS_FATAL,
                "illegal attribute `%s' in the input "
                "of a doc index join",
                PFatt_str (item_res));
    if (!PFprop_ocol (n, item))
        PFoops (OOPS_FATAL,
                "column `%s' needed in doc index join is missing",
                PFatt_str (item));
    if (!(PFprop_type_of (n, item) & aat_str))
        PFoops (OOPS_FATAL,
                "wrong item type '0x%X' in the input of a doc index join",
                PFprop_type_of (n, item));
    if (!PFprop_ocol (n, item_doc))
        PFoops (OOPS_FATAL,
                "column `%s' needed in doc index join is missing",
                PFatt_str (item_doc));
    if (!(PFprop_type_of (n, item_doc) & aat_node))
        PFoops (OOPS_FATAL,
                "wrong item type '0x%X' in the input of a doc index join",
                PFprop_type_of (n, item_doc));
#endif

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[i]
        = (struct PFalg_schm_item_t) { .name = item_res,
                                       .type = aat_pnode };

    return ret;
}


/**
 * Access to (persistently stored) XML documents, the fn:doc()
 * function.  Returns a (frag, result) pair.
 */
PFla_op_t *
PFla_doc_tbl (const PFla_op_t *n, PFalg_att_t res, PFalg_att_t att)
{
    unsigned int i;
    PFla_op_t   *ret;

    ret = la_op_wire1 (la_doc_tbl, n);

    /* store columns to work on in semantical field */
    ret->sem.doc_tbl.res = res;
    ret->sem.doc_tbl.att = att;

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[i]
        = (struct PFalg_schm_item_t) { .name = res,
                                       .type = aat_pnode };

    return ret;
}


/**
 * Access to string content of the loaded documents
 */
PFla_op_t *
PFla_doc_access (const PFla_op_t *doc, const PFla_op_t *n,
                 PFalg_att_t res, PFalg_att_t col, PFalg_doc_t doc_col)
{
    unsigned int i;
    PFla_op_t *ret = la_op_wire2 (la_doc_access, doc, n);

    /* allocate memory for the result schema;
       it's the same schema as n's plus an additional result column  */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n' */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[i]
        = (struct PFalg_schm_item_t) { .type = aat_str, .name = res };

    ret->sem.doc_access.res = res;
    ret->sem.doc_access.att = col;
    ret->sem.doc_access.doc_col = doc_col;

    return ret;
}


/**
 * Constructor for twig root operators.
 */
PFla_op_t * PFla_twig (const PFla_op_t *n,
                       PFalg_att_t iter,
                       PFalg_att_t item)
{
    PFalg_simple_type_t iter_type = 0;
    PFla_op_t *ret = la_op_wire1 (la_twig, n);

    /* store columns to work on in semantical field */
    ret->sem.iter_item.iter = iter;
    ret->sem.iter_item.item = item;

    /* allocate memory for the result schema;
       it's the same schema as n's plus an additional result column  */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    if (n->kind == la_attribute ||
        n->kind == la_processi)
        iter_type = PFprop_type_of (
                        n->child[0],
                        n->sem.iter_item1_item2.iter);
    else if (n->kind == la_content)
        iter_type = PFprop_type_of (
                        n->child[1],
                        n->sem.iter_item.iter);
    else if (n->kind == la_docnode)
        iter_type = PFprop_type_of (
                        n->child[0],
                        n->sem.docnode.iter);
    else
        iter_type = PFprop_type_of (
                        n->child[0],
                        n->sem.iter_item.iter);

    ret->schema.items[0].name = iter;
    ret->schema.items[0].type = iter_type;
    ret->schema.items[1].name = item;
    /* Check if the twig consists of only attributes ... */
    if (n->kind == la_attribute ||
        (n->kind == la_content &&
         PFprop_type_of (n->child[1],
                         n->sem.iter_pos_item.item) == aat_anode))
        ret->schema.items[1].type = aat_anode;
    /* ... attributes and other nodes ... */
    else if (n->kind == la_content &&
             PFprop_type_of (n->child[1],
                             n->sem.iter_pos_item.item) & aat_anode)
        ret->schema.items[1].type = aat_node;
    /* ... or only other nodes (without attributes). */
    else
        ret->schema.items[1].type = aat_pnode;

    return ret;
}


/**
 * Constructor for twig constructor sequence operators.
 *
 * @a fc is the next 'first' child operator and @a ns is the
 * next sibling in the constructor sequence operator list.
 */
PFla_op_t * PFla_fcns (const PFla_op_t *fc,
                       const PFla_op_t *ns)
{
    PFla_op_t *ret = la_op_wire2 (la_fcns, fc, ns);

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for document node operators.
 *
 * @a scope is the current scope the document node is constructed in
 * and @a fcns is the content of the node.
 */
PFla_op_t * PFla_docnode (const PFla_op_t *scope, const PFla_op_t *fcns,
                          PFalg_att_t iter)
{
    PFla_op_t *ret = la_op_wire2 (la_docnode, scope, fcns);

    /* store columns to work on in semantical field */
    ret->sem.docnode.iter = iter;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for element operators.
 *
 * @a tag constructs the elements' tag names, and @a fcns
 * is the content of the new elements.
 */
PFla_op_t * PFla_element (const PFla_op_t *tag,
                          const PFla_op_t *fcns,
                          PFalg_att_t iter,
                          PFalg_att_t item)
{
    PFla_op_t *ret = la_op_wire2 (la_element, tag, fcns);

    /* store columns to work on in semantical field */
    ret->sem.iter_item.iter = iter;
    ret->sem.iter_item.item = item;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for attribute operators.
 *
 * @a cont stores the name-value relation of the attribute. @a iter, @a qn,
 * and @a val reference the iter, qname, and value input columns, respectively.
 */
PFla_op_t * PFla_attribute (const PFla_op_t *cont,
                            PFalg_att_t iter, PFalg_att_t qn, PFalg_att_t val)
{
    PFla_op_t *ret = la_op_wire1 (la_attribute, cont);

    /* store columns to work on in semantical field */
    ret->sem.iter_item1_item2.iter  = iter;
    ret->sem.iter_item1_item2.item1 = qn;
    ret->sem.iter_item1_item2.item2 = val;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for text content operators.
 *
 * @a cont is the relation storing the textnode content; @item points
 * to the respective column and @iter marks the scope in which the
 * nodes are constructed in.
 */
PFla_op_t *
PFla_textnode (const PFla_op_t *cont, PFalg_att_t iter, PFalg_att_t item)
{
    PFla_op_t *ret = la_op_wire1 (la_textnode, cont);

    /* store columns to work on in semantical field */
    ret->sem.iter_item.iter = iter;
    ret->sem.iter_item.item = item;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for comment operators.
 *
 * @a cont is the relation storing the comment content; @item points
 * to the respective column and @iter marks the scope in which the
 * nodes are constructed in.
 */
PFla_op_t *
PFla_comment (const PFla_op_t *cont, PFalg_att_t iter, PFalg_att_t item)
{
    PFla_op_t *ret = la_op_wire1 (la_comment, cont);

    /* store columns to work on in semantical field */
    ret->sem.iter_item.iter = iter;
    ret->sem.iter_item.item = item;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for processing instruction operators.
 *
 * @a cont stores the target-value relation of the processing-instruction.
 * @a iter, @a target, and @a val reference the iter, target, and value
 * input columns, respectively.
 */
PFla_op_t * PFla_processi (const PFla_op_t *cont,
                           PFalg_att_t iter,
                           PFalg_att_t target,
                           PFalg_att_t val)
{
    PFla_op_t *ret = la_op_wire1 (la_processi, cont);

    /* store columns to work on in semantical field */
    ret->sem.iter_item1_item2.iter  = iter;
    ret->sem.iter_item1_item2.item1 = target;
    ret->sem.iter_item1_item2.item2 = val;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for constructor content operators (elem|doc).
 *
 * Use @a doc to retrieve information about the nodes in @a cont,
 * i.e. to collect all subtree nodes (using a descendant-or-self::node()
 * step) for later use in the twig constructor.
 *
 * The input parameters have the following schemata:
 * - @a doc:  none (as it is a node fragment)
 * - @a cont: iter | pos | item
 */
PFla_op_t * PFla_content (const PFla_op_t *doc,
                          const PFla_op_t *cont,
                          PFalg_att_t iter,
                          PFalg_att_t pos,
                          PFalg_att_t item)
{
    PFla_op_t *ret = la_op_wire2 (la_content, doc, cont);

    /* store columns to work on in semantical field */
    ret->sem.iter_pos_item.iter  = iter;
    ret->sem.iter_pos_item.pos   = pos;
    ret->sem.iter_pos_item.item  = item;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for pf:merge-adjacent-text-nodes() functionality.
 *
 * Use @a doc to retrieve information about the nodes in @a cont, i.e. to
 * determine which ones are text nodes. If two consecutive text nodes
 * are found in @a cont (same "iter", consecutive "pos" value), merge
 * them into one text node. If the content of a text node is empty,
 * discard the node.
 * The input parameters have the following schemata:
 * - @a doc:  none (as it is a node fragment)
 * - @a cont: iter | pos | item
 * The output are an algebra representation of all nodes (old and new,
 * i.e. unmerged and merged) and a fragment representation of the newly
 * created nodes only.
 */
PFla_op_t *
PFla_pf_merge_adjacent_text_nodes (const PFla_op_t *doc,
                                   const PFla_op_t *cont,
                                   PFalg_att_t iter_in,
                                   PFalg_att_t pos_in,
                                   PFalg_att_t item_in,
                                   PFalg_att_t iter_res,
                                   PFalg_att_t pos_res,
                                   PFalg_att_t item_res)
{
    PFla_op_t *ret = la_op_wire2 (la_merge_adjacent, doc, cont);

    /* store columns to work on in semantical field */
    ret->sem.merge_adjacent.iter_in  = iter_in;
    ret->sem.merge_adjacent.pos_in   = pos_in;
    ret->sem.merge_adjacent.item_in  = item_in;
    ret->sem.merge_adjacent.iter_res = iter_res;
    ret->sem.merge_adjacent.pos_res  = pos_res;
    ret->sem.merge_adjacent.item_res = item_res;

    /* The schema of the result part is iter|pos|item */
    ret->schema.count = 3;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = iter_res,
                                .type = PFprop_type_of (cont, iter_in) };
    ret->schema.items[1]
        = (PFalg_schm_item_t) { .name = pos_res,
                                .type = PFprop_type_of (cont, pos_in) };
    ret->schema.items[2]
        = (PFalg_schm_item_t) { .name = item_res,
                                .type = PFprop_type_of (cont, item_in) };

    return ret;
}


/**
 * Extract the expression result part from a (frag, result) pair.
 */
PFla_op_t *
PFla_roots (const PFla_op_t *n)
{
    PFla_op_t *ret = la_op_wire1 (la_roots, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    return ret;
}


/**
 * Extract the document fragment part from a (frag, result) pair.
 * It typically contains newly constructed nodes of some node
 * construction operator.  The document representation is dependent
 * on the back-end system.  Hence, the resulting algebra node does
 * not have a meaningful relational schema (in fact, the schema
 * component will be set to NULL).
 */
PFla_op_t *
PFla_fragment (const PFla_op_t *n)
{
    PFla_op_t *ret = la_op_wire1 (la_fragment, n);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}

/** Constructor for a fragment extract operator
    (to be used in combination with a function call) */
PFla_op_t *
PFla_frag_extract (const PFla_op_t *n, unsigned int col_pos)
{
    PFla_op_t *ret = la_op_wire1 (la_frag_extract, n);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    ret->sem.col_ref.pos = col_pos;

    return ret;
}

/**
 * Create empty set of fragments. It signals that an algebra expression
 * does not produce any xml nodes (any fragment).
 */
PFla_set_t *
PFla_empty_set (void)
{
    return PFarray (sizeof (PFla_op_t *));
}

/**
 * Create a new set containing one fragment.
 */
PFla_set_t *
PFla_set (const PFla_op_t *n)
{
    PFarray_t *ret = PFarray (sizeof (PFla_op_t *));

    /* add node */
    *((PFla_op_t **) PFarray_add (ret)) = (PFla_op_t *) n;

    return ret;
}

/**
 * Form a set-oriented union between two sets of fragments. Eliminate
 * duplicate fragments (based on C pointer identity).
 */
PFla_set_t *
PFla_set_union (PFla_set_t *frag1, PFla_set_t *frag2)
{
    unsigned int i, j;
    PFla_op_t *n1;
    PFla_op_t *n2;
    PFarray_t *ret = PFarray (sizeof (PFla_op_t *));

    for (i = 0; i < PFarray_last (frag1); i++) {
        n1 = *((PFla_op_t **) PFarray_at (frag1, i));

        for (j = 0; j < PFarray_last (frag2); j++) {
            n2 = *((PFla_op_t **) PFarray_at (frag2, j));

            /* n2 is a duplicate of n1: n1 must not be added to result */
            if (n1 == n2)
                break;
        }

        /* n1 has no duplicate in frag2: add n1 to result */
        if (j == PFarray_last (frag2))
            *((PFla_op_t **) PFarray_add (ret)) = n1;
    }

    /* now append all fragments from frag 2*/
    for (j = 0; j < PFarray_last (frag2); j++) {
        n2 = *((PFla_op_t **) PFarray_at (frag2, j));
        *((PFla_op_t **) PFarray_add (ret)) = n2;
    }

    return ret;
}

/**
 * Convert a set of fragments into an algebra expression. The fragments
 * are unified by a special, fragment-specific union operator. It creates
 * a binary tree in which the bottom-most leaf is always represented by an
 * empty fragment.
 */
PFla_op_t *
PFla_set_to_la (PFla_set_t *frags)
{
    unsigned int i;
    PFla_op_t *ret;

    /* make sure list contains fragments */
    ret = empty_frag ();

    /* */
    for (i = 0; i < PFarray_last (frags); i++)
        ret = PFla_frag_union (ret, *((PFla_op_t **) PFarray_at (frags, i)));

    return ret;
}



/** Form algebraic disjoint union between two fragments. */
PFla_op_t *
PFla_frag_union (const PFla_op_t *n1, const PFla_op_t *n2)
{
    PFla_op_t *ret = la_op_wire2 (la_frag_union, n1, n2);

    assert (n1->schema.count == 0);
    assert (n2->schema.count == 0);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}

/** Constructor for an empty fragment */
PFla_op_t *
PFla_empty_frag (void)
{
    PFla_op_t *ret = la_op_leaf (la_empty_frag);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for a runtime error message.
 *
 * This operator generates a runtime error (using the string in column
 * @a att) if a tuple flows through it.
 */
PFla_op_t *
PFla_error_ (const PFla_op_t *n, PFalg_att_t att, PFalg_simple_type_t att_ty)
{
    PFla_op_t   *ret;
    unsigned int i;

    assert(n);

    ret = la_op_wire1 (la_error, n);

    /* allocate memory for the result schema; it's the same schema as n's */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n' */
    for (i = 0; i < n->schema.count; i++) {
        ret->schema.items[i] = n->schema.items[i];
        if (att == n->schema.items[i].name)
            ret->schema.items[i].type = att_ty;
    }

    ret->sem.err.att = att;
    ret->sem.err.str = NULL; /* error message is stored in column @a att */

    return ret;
}

/**
 * Constructor for a runtime error message.
 *
 * This operator generates a runtime error (using the string in column
 * @a att) if a tuple flows through it.
 */
PFla_op_t *
PFla_error (const PFla_op_t *n, PFalg_att_t att)
{
    return PFla_error_ (n, att, 0);
}

/**
 * Constructor for a conditional error message.
 *
 * @a err is the relational expression that checks conditions
 * at runtime. If column @a att contains any false values an
 * error is triggered (@a err_string) and the execution is aborted.
 * Otherwise the regular query plan @a n is the return expression.
 */
PFla_op_t *
PFla_cond_err (const PFla_op_t *n, const PFla_op_t *err,
               PFalg_att_t att, char *err_string)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (n);
    assert (err);
    assert (err_string);

    /* verify that att is an attribute of n ... */
    for (i = 0; i < err->schema.count; i++)
        if (att == err->schema.items[i].name)
            break;

    /* did we find attribute att? */
    if (i >= err->schema.count)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in conditional error not found",
                PFatt_str (att));

    /* create new conditional error node */
    ret = la_op_wire2 (la_cond_err, n, err);

    /*
     * insert semantic value (error attribute and error string)
     * into the result
     */
    ret->sem.err.att = att;
    ret->sem.err.str = err_string;

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = n->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    return ret;
}


/**
 * Constructor for a debug operator
 */
PFla_op_t *
PFla_trace (const PFla_op_t *n1,
            const PFla_op_t *n2,
            PFalg_att_t iter,
            PFalg_att_t pos,
            PFalg_att_t item)
{
    PFla_op_t     *ret;
    unsigned int   i, found = 0;

    assert (n1);
    assert (n2);

    /* verify that iter, pos, and item are attributes of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (iter == n1->schema.items[i].name ||
            pos  == n1->schema.items[i].name ||
            item == n1->schema.items[i].name)
            found++;

    /* did we find all attributes? */
    if (found != 3)
        PFoops (OOPS_FATAL,
                "attributes referenced in trace operator not found");

    /* create new trace node */
    ret = la_op_wire2 (la_trace, n1, n2);

    /* insert semantic values (column names) into the result */
    ret->sem.iter_pos_item.iter = iter;
    ret->sem.iter_pos_item.pos  = pos;
    ret->sem.iter_pos_item.item = item;

    /* allocate memory for the result schema (= schema(n1)) */
    ret->schema.count = n1->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    return ret;
}


/**
 * Constructor for a debug message operator
 */
PFla_op_t *
PFla_trace_msg (const PFla_op_t *n1,
                const PFla_op_t *n2,
                PFalg_att_t iter,
                PFalg_att_t item)
{
    PFla_op_t     *ret;
    unsigned int   i, found = 0;

    assert (n1);
    assert (n2);

    /* verify that iter and item are attributes of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (iter == n1->schema.items[i].name ||
            item == n1->schema.items[i].name)
            found++;

    /* did we find all attributes? */
    if (found != 2)
        PFoops (OOPS_FATAL,
                "attributes referenced in trace message operator not found");

    /* create new trace node */
    ret = la_op_wire2 (la_trace_msg, n1, n2);

    /* insert semantic values (column names) into the result */
    ret->sem.iter_item.iter = iter;
    ret->sem.iter_item.item = item;

    /* allocate memory for the result schema (= schema(n1)) */
    ret->schema.count = n1->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    return ret;
}


/**
 * Constructor for debug relation map operator
 * (A set of the trace_map operators link a trace operator
 * to the correct scope.)
 */
PFla_op_t *
PFla_trace_map (const PFla_op_t *n1,
                const PFla_op_t *n2,
                PFalg_att_t      inner,
                PFalg_att_t      outer)
{
    PFla_op_t     *ret;
    unsigned int   i, found = 0;

    assert (n1);
    assert (n2);

    /* verify that inner and outer are attributes of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (inner == n1->schema.items[i].name ||
            outer == n1->schema.items[i].name)
            found++;

    /* did we find all attributes? */
    if (found != 2)
        PFoops (OOPS_FATAL,
                "attributes referenced in trace operator not found");

    /* create new trace map node */
    ret = la_op_wire2 (la_trace_map, n1, n2);

    /* insert semantic values (column names) into the result */
    ret->sem.trace_map.inner    = inner;
    ret->sem.trace_map.outer    = outer;

    /* allocate memory for the result schema (= schema(n1)) */
    ret->schema.count = n1->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    return ret;
}


/**
 * Constructor for the last item of a parameter list
 */
PFla_op_t *PFla_nil (void)
{
    PFla_op_t     *ret;

    /* create end of recursion parameter list operator */
    ret = la_op_leaf (la_nil);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for a tail recursion operator
 */
PFla_op_t *PFla_rec_fix (const PFla_op_t *paramList,
                         const PFla_op_t *res)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (paramList);
    assert (res);

    /* create recursion operator */
    ret = la_op_wire2 (la_rec_fix, paramList, res);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = res->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < res->schema.count; i++)
        ret->schema.items[i] = res->schema.items[i];

    return ret;
}


/**
 * Constructor for a list item of a parameter list
 * related to recursion
 */
PFla_op_t *PFla_rec_param (const PFla_op_t *arguments,
                           const PFla_op_t *paramList)
{
    PFla_op_t     *ret;

    assert (arguments);
    assert (paramList);

    /* create recursion parameter operator */
    ret = la_op_wire2 (la_rec_param, arguments, paramList);

    assert (paramList->schema.count == 0);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for the arguments of a parameter (seed and recursion
 * will be the input relations for the base operator)
 */
PFla_op_t *PFla_rec_arg (const PFla_op_t *seed,
                         const PFla_op_t *recursion,
                         const PFla_op_t *base)
{
    PFla_op_t     *ret;
    unsigned int   i, j;

    assert (seed);
    assert (recursion);
    assert (base);

    /* see if both operands have same number of attributes */
    if (seed->schema.count != recursion->schema.count ||
        seed->schema.count != base->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of the arguments of recursion "
                "argument to not match");

    /* see if we find each attribute in all of the input relations */
    for (i = 0; i < seed->schema.count; i++) {
        for (j = 0; j < recursion->schema.count; j++)
            if (seed->schema.items[i].name == recursion->schema.items[j].name) {
                break;
            }

        if (j == recursion->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of the arguments of recursion "
                    "argument to not match");

        for (j = 0; j < base->schema.count; j++)
            if (seed->schema.items[i].name == base->schema.items[j].name) {
                break;
            }

        if (j == base->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of the arguments of recursion "
                    "argument to not match");
    }

    /* create recursion operator */
    ret = la_op_wire2 (la_rec_arg, seed, recursion);

    /*
     * insert semantic value (reference to the base
     * relation) into the result
     */
    ret->sem.rec_arg.base = (PFla_op_t *) base;

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = seed->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < seed->schema.count; i++)
        ret->schema.items[i] = seed->schema.items[i];

    return ret;
}


/**
 * Constructor for the base relation in a recursion (-- a dummy
 * operator representing the seed relation as well as the argument
 * computed in the recursion).
 */
PFla_op_t *PFla_rec_base (PFalg_schema_t schema)
{
    PFla_op_t     *ret;
    unsigned int   i;

    /* create base operator for the recursion */
    ret = la_op_leaf (la_rec_base);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < schema.count; i++)
        ret->schema.items[i] = schema.items[i];

    return ret;
}


/**
 * Constructor for the function application
 */
PFla_op_t *
PFla_fun_call (const PFla_op_t *loop, const PFla_op_t *param_list,
               PFalg_schema_t schema, PFalg_fun_call_t kind,
               PFqname_t qname, void *ctx,
               PFalg_att_t iter, PFalg_occ_ind_t occ_ind)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (loop);
    assert (param_list);

    /* create new function application node */
    ret = la_op_wire2 (la_fun_call, loop, param_list);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = schema.count;

    ret->schema.items
        = PFmalloc (schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < schema.count; i++)
        ret->schema.items[i] = schema.items[i];

    /* insert semantic value */
    ret->sem.fun_call.kind    = kind;
    ret->sem.fun_call.qname   = qname;
    ret->sem.fun_call.ctx     = ctx;
    ret->sem.fun_call.iter    = iter;
    ret->sem.fun_call.occ_ind = occ_ind;

    return ret;
}


/**
 * Constructor for a list item of a parameter list
 * related to function application
 */
PFla_op_t *
PFla_fun_param (const PFla_op_t *argument, const PFla_op_t *param_list,
                PFalg_schema_t schema)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (argument);
    assert (param_list);

    /* create new function application parameter node */
    ret = la_op_wire2 (la_fun_param, argument, param_list);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = schema.count;

    ret->schema.items
        = PFmalloc (schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < schema.count; i++)
        ret->schema.items[i] = schema.items[i];

    return ret;
}


/**
 * Constructor for the fragment information of a list item
 * of a parameter list related to function application
 */
PFla_op_t *
PFla_fun_frag_param (const PFla_op_t *argument,
                     const PFla_op_t *param_list,
                     unsigned int col_pos)
{
    PFla_op_t     *ret;

    assert (argument);
    assert (param_list);

    /* create new function application parameter node */
    ret = la_op_wire2 (la_fun_frag_param, argument, param_list);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    ret->sem.col_ref.pos = col_pos;

    return ret;
}


/**
 * Constructor for a proxy operator with a single child
 */
PFla_op_t *
PFla_proxy (const PFla_op_t *n, unsigned int kind,
            PFla_op_t *ref, PFla_op_t *base,
            PFalg_attlist_t new_cols, PFalg_attlist_t req_cols)
{
    return PFla_proxy2 (n, kind, ref, base, NULL, new_cols, req_cols);
}


/**
 * Constructor for a proxy operator with a two children
 */
PFla_op_t *
PFla_proxy2 (const PFla_op_t *n, unsigned int kind,
             PFla_op_t *ref, PFla_op_t *base1, PFla_op_t *base2,
             PFalg_attlist_t new_cols, PFalg_attlist_t req_cols)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (n);
    assert (ref);
    assert (base1);

    /* create new proxy node */
    ret = la_op_wire1 (la_proxy, n);

    /*
     * insert semantic value (kind, ref and base operator, new columns
     * and required columns into the result
     */
    ret->sem.proxy.kind     = kind;
    ret->sem.proxy.ref      = ref;
    ret->sem.proxy.base1    = base1;
    ret->sem.proxy.base2    = base2;
    ret->sem.proxy.new_cols = (PFalg_attlist_t) {
        .count = new_cols.count,
        .atts = memcpy (PFmalloc (new_cols.count * sizeof (PFalg_att_t)),
                        new_cols.atts,
                        new_cols.count * sizeof (PFalg_att_t)) };
    ret->sem.proxy.req_cols = (PFalg_attlist_t) {
        .count = req_cols.count,
        .atts = memcpy (PFmalloc (req_cols.count * sizeof (PFalg_att_t)),
                        req_cols.atts,
                        req_cols.count * sizeof (PFalg_att_t)) };

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = n->schema.count;

    ret->schema.items
        = PFmalloc (n->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    return ret;
}


/**
 * Constructor for a proxy base operator
 */
PFla_op_t *
PFla_proxy_base (const PFla_op_t *n)
{
    PFla_op_t *ret = la_op_wire1 (la_proxy_base, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    return ret;
}


/**
 * Constructor for builtin function fn:string-join
 */
PFla_op_t *
PFla_fn_string_join (const PFla_op_t *text, const PFla_op_t *sep,
                     PFalg_att_t iter, PFalg_att_t pos, PFalg_att_t item,
                     PFalg_att_t iter_sep, PFalg_att_t item_sep,
                     PFalg_att_t iter_res, PFalg_att_t item_res)
{
    PFla_op_t *ret = la_op_wire2 (la_string_join, text, sep);

    /* store columns to work on in semantical field */
    ret->sem.string_join.iter     = iter;
    ret->sem.string_join.pos      = pos;
    ret->sem.string_join.item     = item;
    ret->sem.string_join.iter_sep = iter_sep;
    ret->sem.string_join.item_sep = item_sep;
    ret->sem.string_join.iter_res = iter_res;
    ret->sem.string_join.item_res = item_res;

    /* The schema of the result part is iter|item */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = iter_res,
                                .type = PFprop_type_of (sep, iter_sep) };
    ret->schema.items[1]
        = (PFalg_schm_item_t) { .name = item_res, .type = aat_str };

    return ret;
}

/**
 * Duplicates an operator.
 * The @a left and @a right arguments are its new children.
 * Each algebra node has at most 2 children, so the left children
 * can even be misused as document.
 *
 * This function does not care about XQuery semantics and also
 * duplicates operators whose semantics change this way (e.g., node
 * constructors).
 */
PFla_op_t *
PFla_op_duplicate (PFla_op_t *n, PFla_op_t *left, PFla_op_t *right)
{
    assert (n);

    switch (n->kind) {
        case la_serialize_seq:
            return PFla_serialize_seq (left, right,
                                       n->sem.ser_seq.pos,
                                       n->sem.ser_seq.item);

        case la_serialize_rel:
            return PFla_serialize_rel (left,
                                       n->sem.ser_rel.iter,
                                       n->sem.ser_rel.pos,
                                       n->sem.ser_rel.items);

        case la_lit_tbl:
        {
            PFalg_att_t *atts;
            atts = (PFalg_att_t *) PFmalloc (n->schema.count *
                                             sizeof (PFalg_att_t));

            for (unsigned int i = 0; i < n->schema.count; i++)
                atts[i] = n->schema.items[i].name;

            PFalg_attlist_t attlist = (PFalg_attlist_t)
                {
                    .count = n->schema.count,
                    .atts  = atts
                };

            return PFla_lit_tbl_ (attlist,
                                  n->sem.lit_tbl.count,
                                  n->sem.lit_tbl.tuples);
        } break;

        case la_empty_tbl:
            return PFla_empty_tbl_ (n->schema);

        case la_ref_tbl:
        {

            return PFla_ref_tbl_ (n->sem.ref_tbl.name,
                                  n->schema,
                                  n->sem.ref_tbl.tatts,
                                  n->sem.ref_tbl.keys);
        } break;

        case la_attach:
            return PFla_attach (left,
                                n->sem.attach.res,
                                n->sem.attach.value);

        case la_cross:
            return PFla_cross (left, right);

        case la_eqjoin:
            return PFla_eqjoin (left, right,
                                n->sem.eqjoin.att1,
                                n->sem.eqjoin.att2);

        case la_semijoin:
            return PFla_semijoin (left, right,
                                  n->sem.eqjoin.att1,
                                  n->sem.eqjoin.att2);

        case la_thetajoin:
            return PFla_thetajoin (left, right,
                                   n->sem.thetajoin.count,
                                   n->sem.thetajoin.pred);

        case la_project:
            return PFla_project_ (left,
                                  n->sem.proj.count,
                                  n->sem.proj.items);

        case la_select:
            return PFla_select (left, n->sem.select.att);

        case la_pos_select:
            return PFla_pos_select (left,
                                    n->sem.pos_sel.pos,
                                    n->sem.pos_sel.sortby,
                                    n->sem.pos_sel.part);

        case la_disjunion:
        case la_intersect:
        case la_difference:
            return set_operator (n->kind, left, right);

        case la_distinct:
            return PFla_distinct (left);

        case la_fun_1to1:
            return fun_1to1 (left,
                             n->sem.fun_1to1.kind,
                             n->sem.fun_1to1.res,
                             n->sem.fun_1to1.refs);

        case la_num_eq:
        case la_num_gt:
            return compar_op (n->kind, left,
                              n->sem.binary.res,
                              n->sem.binary.att1,
                              n->sem.binary.att2);

        case la_bool_and:
        case la_bool_or:
            return boolean_op (n->kind, left,
                               n->sem.binary.res,
                               n->sem.binary.att1,
                               n->sem.binary.att2);

        case la_bool_not:
            return unary_op (n->kind, left,
                             n->sem.unary.res,
                             n->sem.unary.att);

        case la_to:
            return PFla_to (left,
                            n->sem.binary.res,
                            n->sem.binary.att1,
                            n->sem.binary.att2);

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
            return PFla_aggr (n->kind, left,
                              n->sem.aggr.res,
                              n->sem.aggr.att,
                              n->sem.aggr.part);

        case la_count:
            return PFla_count (left,
                               n->sem.aggr.res,
                               n->sem.aggr.part);

        case la_rownum:
            return PFla_rownum (left,
                                n->sem.sort.res,
                                n->sem.sort.sortby,
                                n->sem.sort.part);

        case la_rowrank:
            return PFla_rowrank (left,
                                 n->sem.sort.res,
                                 n->sem.sort.sortby);

        case la_rank:
            return PFla_rank (left,
                              n->sem.sort.res,
                              n->sem.sort.sortby);

        case la_rowid:
            return PFla_rowid (left,
                               n->sem.rowid.res);

        case la_type:
            return PFla_type (left,
                              n->sem.type.res,
                              n->sem.type.att,
                              n->sem.type.ty);

        case la_type_assert:
            return PFla_type_assert (left,
                                     n->sem.type.att,
                                     n->sem.type.ty,
                                     true);

        case la_cast:
            return PFla_cast (left,
                              n->sem.type.res,
                              n->sem.type.att,
                              n->sem.type.ty);

        case la_seqty1:
            return PFla_seqty1 (left,
                                n->sem.aggr.res,
                                n->sem.aggr.att,
                                n->sem.aggr.part);

        case la_all:
            return PFla_all (left,
                             n->sem.aggr.res,
                             n->sem.aggr.att,
                             n->sem.aggr.part);

        case la_step:
            return PFla_step (left, right,
                              n->sem.step.spec,
                              n->sem.step.level,
                              n->sem.step.iter,
                              n->sem.step.item,
                              n->sem.step.item_res);

        case la_step_join:
            return PFla_step_join (left, right,
                                   n->sem.step.spec,
                                   n->sem.step.level,
                                   n->sem.step.item,
                                   n->sem.step.item_res);

        case la_guide_step:
            return PFla_guide_step (left, right,
                                    n->sem.step.spec,
                                    n->sem.step.guide_count,
                                    n->sem.step.guides,
                                    n->sem.step.level,
                                    n->sem.step.iter,
                                    n->sem.step.item,
                                    n->sem.step.item_res);

        case la_guide_step_join:
            return PFla_guide_step_join (left, right,
                                         n->sem.step.spec,
                                         n->sem.step.guide_count,
                                         n->sem.step.guides,
                                         n->sem.step.level,
                                         n->sem.step.item,
                                         n->sem.step.item_res);

        case la_doc_index_join:
            return PFla_doc_index_join (left, right,
                                        n->sem.doc_join.kind,
                                        n->sem.doc_join.item,
                                        n->sem.doc_join.item_res,
                                        n->sem.doc_join.item_doc);

        case la_doc_tbl:
            return PFla_doc_tbl (left,
                                 n->sem.doc_tbl.res,
                                 n->sem.doc_tbl.att);

        case la_doc_access:
            return PFla_doc_access (left, right,
                                    n->sem.doc_access.res,
                                    n->sem.doc_access.att,
                                    n->sem.doc_access.doc_col);

        case la_twig:
            return PFla_twig (left,
                              n->sem.iter_item.iter,
                              n->sem.iter_item.item);

        case la_fcns:
            return PFla_fcns (left, right);

        case la_docnode:
            return PFla_docnode (left, right,
                                 n->sem.docnode.iter);

        case la_element:
            return PFla_element (left, right,
                                 n->sem.iter_item.iter,
                                 n->sem.iter_item.item);

        case la_attribute:
            return PFla_attribute (left,
                                   n->sem.iter_item1_item2.iter,
                                   n->sem.iter_item1_item2.item1,
                                   n->sem.iter_item1_item2.item2);

        case la_textnode:
            return PFla_textnode (left,
                                  n->sem.iter_item.iter,
                                  n->sem.iter_item.item);

        case la_comment:
            return PFla_comment (left,
                                 n->sem.iter_item.iter,
                                 n->sem.iter_item.item);

        case la_processi:
            return PFla_processi (left,
                                  n->sem.iter_item1_item2.iter,
                                  n->sem.iter_item1_item2.item1,
                                  n->sem.iter_item1_item2.item2);

        case la_content:
            return PFla_content (left, right,
                                 n->sem.iter_pos_item.iter,
                                 n->sem.iter_pos_item.pos,
                                 n->sem.iter_pos_item.item);

        case la_merge_adjacent:
            return PFla_pf_merge_adjacent_text_nodes (
                       left, right,
                       n->sem.merge_adjacent.iter_in,
                       n->sem.merge_adjacent.pos_in,
                       n->sem.merge_adjacent.item_in,
                       n->sem.merge_adjacent.iter_res,
                       n->sem.merge_adjacent.pos_res,
                       n->sem.merge_adjacent.item_res);

        case la_roots:
            return PFla_roots (left);

        case la_fragment:
            return PFla_fragment (left);

        case la_frag_extract:
            return PFla_frag_extract (left, n->sem.col_ref.pos);

        case la_frag_union:
            return PFla_frag_union (left, right);

        case la_empty_frag:
            return PFla_empty_frag ();

        case la_error:
            return PFla_error_ (left, n->sem.err.att,
                                PFprop_type_of (n, n->sem.err.att));

        case la_cond_err:
            return PFla_cond_err (left, right,
                                  n->sem.err.att,
                                  n->sem.err.str);

        case la_nil:
            return PFla_nil ();

        case la_trace:
            return PFla_trace (left, right,
                               n->sem.iter_pos_item.iter,
                               n->sem.iter_pos_item.pos,
                               n->sem.iter_pos_item.item);

        case la_trace_msg:
            return PFla_trace_msg (left, right,
                                   n->sem.iter_item.iter,
                                   n->sem.iter_item.item);

        case la_trace_map:
            return PFla_trace_msg (left, right,
                                   n->sem.trace_map.inner,
                                   n->sem.trace_map.outer);

        case la_fun_call:
            return PFla_fun_call (left, right,
                                  n->schema,
                                  n->sem.fun_call.kind,
                                  n->sem.fun_call.qname,
                                  n->sem.fun_call.ctx,
                                  n->sem.fun_call.iter,
                                  n->sem.fun_call.occ_ind);

        case la_fun_param:
            return PFla_fun_param (left, right,
                                   n->schema);

        case la_fun_frag_param:
            return PFla_fun_frag_param (left, right,
                                        n->sem.col_ref.pos);

        case la_string_join:
            return PFla_fn_string_join (
                       left, right,
                       n->sem.string_join.iter,
                       n->sem.string_join.pos,
                       n->sem.string_join.item,
                       n->sem.string_join.iter_sep,
                       n->sem.string_join.item_sep,
                       n->sem.string_join.iter_res,
                       n->sem.string_join.item_res);

        case la_dummy:
            return PFla_dummy (left);

        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_proxy:
        case la_proxy_base:
        case la_cross_mvd:
        case la_eqjoin_unq:
            PFoops (OOPS_FATAL,
                    "Logical operator cloning does not"
                    "support node kind (%i).",
                    n->kind);
    }

    assert (!"this should never be reached (duplicate nodes)");

    return n; /* satisfy picky compilers */
}

/* vim:set shiftwidth=4 expandtab: */
