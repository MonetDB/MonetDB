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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include pf_config.h first! */
#include "pf_config.h"
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

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* filter out dummy error columns */
#define ERR_TYPE_MASK(t)  ((t) & ~aat_error)

/* Encapsulates initialization stuff common to binary comparison operators.  */
static PFla_op_t *
compar_op (PFla_op_kind_t kind, const PFla_op_t *n,
           PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2);

/* Encapsulates initialization stuff common to binary boolean operators. */
static PFla_op_t *
boolean_op (PFla_op_kind_t kind, const PFla_op_t *n, PFalg_col_t col1,
            PFalg_col_t col2, PFalg_col_t res);

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

    ret->state_label   = 0;
    ret->plans         = NULL;
    ret->sql_ann       = NULL;
    ret->prop          = PFprop ();
    ret->node_id       = 0;
    ret->refctr        = 0;

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
 * operators (e.g. a rowid operator). Furthermore it prevents operators
 * from accidentally sharing properties (only a reference pointer is copied).
 */
PFla_op_t *
PFla_dummy (PFla_op_t *n)
{
    PFla_op_t *ret = la_op_wire1 (la_dummy, n);

    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    return ret;
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
                    PFalg_col_t pos, PFalg_col_t item)
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
PFla_serialize_rel (const PFla_op_t *side, const PFla_op_t *alg,
                    PFalg_col_t iter, PFalg_col_t pos,
                    PFalg_collist_t *items)
{
    PFla_op_t *ret = la_op_wire2 (la_serialize_rel, side, alg);

#ifndef NDEBUG
    unsigned int i, j;

    if (!PFprop_ocol (alg, iter))
        PFoops (OOPS_FATAL,
                "column `%s' referenced in serialize"
                " operator not found", PFcol_str (iter));
    if (!PFprop_ocol (alg, pos))
        PFoops (OOPS_FATAL,
                "column `%s' referenced in serialize"
                " operator not found", PFcol_str (pos));
    /* verify that the referenced columns in items
       are really columns of alg ... */
    for (i = 0; i < clsize (items); i++) {
        for (j = 0; j < alg->schema.count; j++)
            if (alg->schema.items[j].name == clat (items, i))
                break;
        if (j == alg->schema.count)
            PFoops (OOPS_FATAL,
                    "column `%s' referenced in serialize"
                    " operator not found", PFcol_str (clat (items, i)));
    }
#endif

    ret->sem.ser_rel.iter  = iter;
    ret->sem.ser_rel.pos   = pos;
    ret->sem.ser_rel.items = PFalg_collist_copy (items);

    ret->schema.count = alg->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    /* copy schema from alg */
    for (unsigned int i = 0; i < alg->schema.count; i++)
        ret->schema.items[i] = alg->schema.items[i];

    return ret;
}


/**
 * A `side_effects' node will be placed directly below a serialize operator
 * or below a `rec_fix' operator if the side effects appear in the recursion
 * body.
 * The `side_effects' operator contains a (possibly empty) list of operations
 * that may trigger side effects (operators `error', `cache' and `trace')
 * in its left child and the fragment or recursion parameters in the right
 * child.
 */
PFla_op_t *
PFla_side_effects (const PFla_op_t *side_effects, const PFla_op_t *params)
{
    PFla_op_t *ret = la_op_wire2 (la_side_effects, side_effects, params);

    assert (side_effects);
    assert (side_effects->kind == la_error ||
            side_effects->kind == la_cache ||
            side_effects->kind == la_trace ||
            side_effects->kind == la_nil);
    assert (params);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Construct an algebra node representing a literal table, given
 * a column list and a list of tuples.
 *
 * @param collist Column list of the literal table. (Most easily
 *                constructed using #PFalg_collist_worker() or
 *                its abbreviated macro #collist().)
 * @param count  Number of tuples that follow
 * @param tuples Tuples of this literal table, as #PFalg_tuple_t.
 *               This array must be exactly @a count items long.
 *
 * @note
 *   You should never need to call this function directly. Use the
 *   wrapper macro #PFla_lit_tbl() instead (which is available as
 *   #lit_tbl() if you have included the mnemonic constructor names in
 *   logical_mnemonic.h). This macro will detect the @a count
 *   argument on its own, so you only need to list all column
 *   specifictions.
 *
 * @b Example:
 *
 * @code
   PFla_op_t t = lit_tbl (collist (col_iter, col_pos, col_item),
                          tuple (lit_int (1), lit_int (1), lit_str ("foo")),
                          tuple (lit_int (1), lit_int (2), lit_str ("bar")),
                          tuple (lit_int (2), lit_int (1), lit_str ("baz")));
@endcode
 */
PFla_op_t *
PFla_lit_tbl_ (PFalg_collist_t *collist,
               unsigned int count, PFalg_tuple_t *tuples)
{
    PFla_op_t      *ret;      /* return value we are building */
    unsigned int    i;
    unsigned int    j;

    /* instantiate the new algebra operator node */
    ret = la_op_leaf (la_lit_tbl);

    /* set its schema */
    ret->schema.items
        = PFmalloc (clsize (collist) * sizeof (*(ret->schema.items)));
    for (i = 0; i < clsize (collist); i++) {
        ret->schema.items[i].name = clat (collist, i);
        ret->schema.items[i].type = 0;
    }
    ret->schema.count = clsize (collist);

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
                        "(expected %i columns, got %i)",
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

    ret->sem.lit_tbl.count = 0;
    ret->sem.lit_tbl.tuples = NULL;

    return ret;
}


/**
 * Constructor for an empty table.  Use this constructor (in
 * preference over a literal table with no tuples) to trigger
 * optimization rules concerning empty relations.
 *
 * @param collist Attribute list, similar to the literal table
 *                constructor PFla_lit_tbl().
 */
PFla_op_t *
PFla_empty_tbl (PFalg_collist_t *collist)
{
    PFla_op_t     *ret;      /* return value we are building */
    unsigned int    i;

    /* instantiate the new algebra operator node */
    ret = la_op_leaf (la_empty_tbl);

    /* set its schema */
    ret->schema.items
        = PFmalloc (clsize (collist) * sizeof (*(ret->schema.items)));
    for (i = 0; i < (unsigned int) clsize (collist); i++) {
        ret->schema.items[i].name = clat (collist, i);
        ret->schema.items[i].type = 0;
    }
    ret->schema.count = clsize (collist);

    ret->sem.lit_tbl.count = 0;
    ret->sem.lit_tbl.tuples = NULL;

    return ret;
}


/**
 * Construct an algebra node representing a referenced table,
 * given a (external) name, a (internal) schema, a list of the
 * external column names, and a list of the (internal)
 * key columns.
 *
 * @param name    The name of the referenced table.
 * @param schema  Attribute list ("internal" column names)
 *                with annotated types.
 * @param tcols   String list ("external" column/column
 *                names).
 * @param keys    Array holding the *positions* (w.r.t. the
 *                schema) of key columns
 */
PFla_op_t *
PFla_ref_tbl_ (const char* name, PFalg_schema_t schema, PFarray_t* tcols,
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
    ret->sem.ref_tbl.tcols = PFarray(sizeof (char*), PFarray_last (tcols));
    for (unsigned int i = 0; i < PFarray_last (tcols); i++)
    {
            char* value = *(char**) PFarray_at (tcols, i);
            char* copiedValue = PFstrdup(value);
            *(char**) PFarray_add (ret->sem.ref_tbl.tcols) = copiedValue;
    }

    /* deep copy the list of lists of key-column-positions */
    ret->sem.ref_tbl.keys = PFarray(sizeof (PFarray_t*), PFarray_last (keys));
    for (unsigned int i = 0; i < PFarray_last (keys); i++)
    {
            PFarray_t * keyPositions = *((PFarray_t**) PFarray_at (keys, i));
            /* (it's save to) shallow copy the list of key-column-names */
            PFarray_t * copiedKeyPositions = PFarray_copy(keyPositions);
            *(PFarray_t**) PFarray_add (ret->sem.ref_tbl.keys) = copiedKeyPositions;
    }

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
PFla_attach (const PFla_op_t *n, PFalg_col_t res, PFalg_atom_t value)
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
 * Arguments @a n1 and @a n2 must not have any equally named column.
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

    /* copy schema from argument 2, check for duplicate column names */
    for (j = 0; j < n2->schema.count; j++) {

        ret->schema.items[n1->schema.count + j] = n2->schema.items[j];

#ifndef NDEBUG
        for (i = 0; i < n1->schema.count; i++)
            if (n1->schema.items[i].name == n2->schema.items[j].name)
                PFoops (OOPS_FATAL,
                        "duplicate column `%s' in cross product",
                        PFcol_str (n2->schema.items[j].name));
#endif
    }


    return ret;
}


/**
 * Cross product (Cartesian product) between two algebra expressions.
 * Arguments @a n1 and @a n2 may have equally named columns.
 */
PFla_op_t *
PFla_cross_opt_internal (const PFla_op_t *n1, const PFla_op_t *n2)
{
    PFla_op_t   *ret = la_op_wire2 (la_internal_op, n1, n2);
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

    /* indicate what kind of internal operator we are working on */
    ret->sem.eqjoin_opt.kind   = la_cross;
    
    /* copy schema from argument 2, check for duplicate column names
       and discard if present */
    for (j = 0; j < n2->schema.count; j++) {
        for (i = 0; i < n1->schema.count; i++)
            if (n1->schema.items[i].name == n2->schema.items[j].name)
                break;

        /* no duplicate column found */
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
 * Assert that @a col1 is a column of @a n1 and @a col2 is a column
 * of @a n2. @a n1 and @a n2 must not have duplicate column names.
 * The schema of the result is (schema(@a n1) + schema(@a n2)).
 */
PFla_op_t *
PFla_eqjoin (const PFla_op_t *n1, const PFla_op_t *n2,
             PFalg_col_t col1, PFalg_col_t col2)
{
    PFla_op_t     *ret;
    unsigned int   i;
    unsigned int   j;

    assert (n1); assert (n2);

    /* verify that col1 is column of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (col1 == n1->schema.items[i].name)
            break;

    /* did we find column col1? */
    if (i >= n1->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in join not found",
                PFcol_str(col1));

    /* ... and col2 is column of n2 */
    for (i = 0; i < n2->schema.count; i++)
        if (col2 == n2->schema.items[i].name)
            break;

    /* did we find column col2? */
    if (i >= n2->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in join not found",
                PFcol_str (col2));

    /* build new equi-join node */
    ret = la_op_wire2 (la_eqjoin, n1, n2);

    /* insert semantic value (join columns) into the result */
    ret->sem.eqjoin.col1 = col1;
    ret->sem.eqjoin.col2 = col2;

    /* allocate memory for the result schema (schema(n1) + schema(n2)) */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n1' */
    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* copy schema from argument 'n2', check for duplicate column names */
    for (j = 0; j < n2->schema.count; j++) {

        ret->schema.items[n1->schema.count + j] = n2->schema.items[j];

#ifndef NDEBUG
        for (i = 0; i < n1->schema.count; i++)
            if (n1->schema.items[i].name == n2->schema.items[j].name)
                PFoops (OOPS_FATAL,
                        "duplicate column `%s' in equi-join",
                        PFcol_str (n2->schema.items[j].name));
#endif
    }

    return ret;
}


/**
 * Equi-join between two operator nodes.
 *
 * Assert that @a col1 is a column of @a n1 and @a col2 is a column
 * of @a n2. The schema of the result is:
 * schema(@a n1) + schema(@a n2) - duplicate join column.
 */
PFla_op_t *
PFla_eqjoin_opt_internal (const PFla_op_t *n1, const PFla_op_t *n2,
                          PFarray_t *lproj, PFarray_t *rproj)
{
#define proj_at(l,i) (*(PFalg_proj_t *) PFarray_at ((l),(i)))
    PFalg_simple_type_t res_ty;
    PFla_op_t          *ret;
    unsigned int        i,
                        j,
                        count;

    assert (n1); assert (n2);
    assert (lproj && PFarray_last (lproj));
    assert (rproj && PFarray_last (rproj));

#ifndef NDEBUG
    if (proj_at(lproj,0).new != proj_at(rproj,0).new)
        PFoops (OOPS_FATAL,
                "common result column name for join columns expected");
#endif

    /* verify that col1 is column of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (proj_at(lproj,0).old == n1->schema.items[i].name) {
            res_ty = n1->schema.items[i].type;
            break;
        }

    /* did we find column col1? */
    if (i >= n1->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in join not found",
                PFcol_str(proj_at(lproj,0).old));

    /* ... and col2 is column of n2 */
    for (i = 0; i < n2->schema.count; i++)
        if (proj_at(rproj,0).old == n2->schema.items[i].name) {
            if (n2->schema.items[i].type != res_ty)
                PFoops (OOPS_FATAL,
                        "column types in join do not match");
            break;
        }

    /* did we find column col2? */
    if (i >= n2->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in join not found",
                PFcol_str(proj_at(rproj,0).old));

    /* build new equi-join node */
    ret = la_op_wire2 (la_internal_op, n1, n2);

    /* insert a copy of the semantic value (join columns) into the result */
    lproj = PFarray_copy (lproj);
    rproj = PFarray_copy (rproj);
    ret->sem.eqjoin_opt.kind  = la_eqjoin;
    ret->sem.eqjoin_opt.lproj = lproj;
    ret->sem.eqjoin_opt.rproj = rproj;

    /* allocate memory for the result schema (schema(n1) + schema(n2)) */
    ret->schema.count = PFarray_last (lproj) + PFarray_last (rproj);
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* add join column to the result */
    ret->schema.items[0].name = proj_at(lproj,0).new;
    ret->schema.items[0].type = res_ty;
    count = 1;

    /* copy schema from projection list 'lproj' */
    /* discard join columns - they are already added */
    for (i = 1; i < PFarray_last (lproj); i++) {
        PFalg_proj_t proj_item = proj_at(lproj, i);
#ifndef NDEBUG
        /* check that we do not add duplicate names */
        for (j = 0; j < count; j++)
            if (ret->schema.items[j].name == proj_item.new)
                PFoops (OOPS_FATAL,
                        "duplicate column `%s' in join operator",
                        PFcol_str (proj_item.new));
#endif
        for (j = 0; j < n1->schema.count; j++)
            if (proj_item.old == n1->schema.items[j].name) {
                ret->schema.items[count].name = proj_item.new;
                ret->schema.items[count].type = n1->schema.items[j].type;
                count++;
                break;
            }
        if (j == n1->schema.count) {
            /* projection column is missing --
               remove it from the projection list */
            proj_at(lproj, i) = *(PFalg_proj_t *) PFarray_top (lproj);
            PFarray_last (lproj)--;
            i--;
        }
    }
    
    /* copy schema from projection list 'rproj' */
    /* discard join columns - they are already added */
    for (i = 1; i < PFarray_last (rproj); i++) {
        PFalg_proj_t proj_item = proj_at(rproj, i);
#ifndef NDEBUG
        /* check that we do not add duplicate names */
        for (j = 0; j < count; j++)
            if (ret->schema.items[j].name == proj_item.new)
                PFoops (OOPS_FATAL,
                        "duplicate column `%s' in join operator",
                        PFcol_str (proj_item.new));
#endif
        for (j = 0; j < n2->schema.count; j++)
            if (proj_item.old == n2->schema.items[j].name) {
                ret->schema.items[count].name = proj_item.new;
                ret->schema.items[count].type = n2->schema.items[j].type;
                count++;
                break;
            }
        if (j == n2->schema.count) {
            /* projection column is missing --
               remove it from the projection list */
            proj_at(rproj, i) = *(PFalg_proj_t *) PFarray_top (rproj);
            PFarray_last (rproj)--;
            i--;
        }
    }

    /* adjust schema size */
    ret->schema.count = count;

    return ret;
}


/**
 * Semi-join between two operator nodes.
 *
 * Assert that @a col1 is a column of @a n1 and @a col2 is a column
 * of @a n2. @a n1 and @a n2 must not have duplicate column names.
 * The schema of the result is (schema(@a n1)).
 */
PFla_op_t *
PFla_semijoin (const PFla_op_t *n1, const PFla_op_t *n2,
               PFalg_col_t col1, PFalg_col_t col2)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (n1); assert (n2);

    /* verify that col1 is column of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (col1 == n1->schema.items[i].name)
            break;

    /* did we find column col1? */
    if (i >= n1->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in join not found",
                PFcol_str(col1));

    /* ... and col2 is column of n2 */
    for (i = 0; i < n2->schema.count; i++)
        if (col2 == n2->schema.items[i].name)
            break;

    /* did we find column col2? */
    if (i >= n2->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in join not found",
                PFcol_str (col2));

    /* build new semi-join node */
    ret = la_op_wire2 (la_semijoin, n1, n2);

    /* insert semantic value (join columns) into the result */
    ret->sem.eqjoin.col1 = col1;
    ret->sem.eqjoin.col2 = col2;

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
 * available.  @a n1 and @a n2 must not have duplicate column names.
 * The schema of the result is (schema(@a n1) + schema(@a n2)).
 */
PFla_op_t *
PFla_thetajoin (const PFla_op_t *n1, const PFla_op_t *n2,
                unsigned int count, PFalg_sel_t *pred)
{
    PFla_op_t     *ret;
    unsigned int   i, j, pred_count;

    assert (n1); assert (n2);

    /* verify that the join columns are all available */
    for (i = 0; i < count; i++)
        if (!PFprop_ocol (n1, pred[i].left) ||
            !PFprop_ocol (n2, pred[i].right))
            break;

    /* did we find all columns? */
    if (i < count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in theta-join not found",
                PFprop_ocol (n2, pred[i].right)
                ? PFcol_str(pred[i].left) : PFcol_str(pred[i].right));

    /* build new theta-join node */
    ret = la_op_wire2 (la_thetajoin, n1, n2);

    /* insert semantic value (predicates) into the result */
    ret->sem.thetajoin.pred  = PFmalloc (count * sizeof (PFalg_sel_t));
    pred_count = 0;
    for (i = 0; i < count; i++) {
        /* Throw away all identical predicates
           (after the first appearance of the predicate). */
        for (j = 0; j < i; j++)
            if (pred[i].left  == pred[j].left  &&
                pred[i].right == pred[j].right &&
                pred[i].comp  == pred[j].comp)
                break;
        if (i == j)
            ret->sem.thetajoin.pred[pred_count++] = pred[i];
    }
    ret->sem.thetajoin.count = pred_count;

    /* allocate memory for the result schema (schema(n1) + schema(n2)) */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n1' */
    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* copy schema from argument 'n2', check for duplicate column names */
    for (j = 0; j < n2->schema.count; j++) {

        ret->schema.items[n1->schema.count + j] = n2->schema.items[j];

#ifndef NDEBUG
        for (i = 0; i < n1->schema.count; i++)
            if (n1->schema.items[i].name == n2->schema.items[j].name)
                PFoops (OOPS_FATAL,
                        "duplicate column `%s' in theta-join",
                        PFcol_str (n2->schema.items[j].name));
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
    ret = la_op_wire2 (la_internal_op, n1, n2);

    /* insert semantic value (predicates) into the result */
    ret->sem.thetajoin_opt.kind = la_thetajoin;
    ret->sem.thetajoin_opt.pred = PFarray_copy (data);

    /* allocate no schema -- this will be done by the optimization */

    return ret;
}

/**
 * Logical algebra projection/renaming.
 *
 * @param n     Argument for the projection operator.
 * @param count Number of items in the projection list that follows,
 *              i.e., number of columns in the projection result.
 * @param proj  Projection list. Pass exactly @a count items of type
 *              #PFalg_proj_t in this array. All columns referenced
 *              in the projection list must be available in relation @a
 *              n, and projection must not result in duplicate column
 *              names. You may, however, reference the same column in
 *              @a n twice (with different names in the projection
 *              result).
 *
 * @note
 *   You should never need to call this function directly. Use the
 *   wrapper macro #PFla_project() instead (which is available as
 *   #project() if you have included the mnemonic constructor names in
 *   logical_mnemonic.h). This macro will detect the @a count
 *   argument on its own, so you only need to list all column
 *   specifictions.
 */
PFla_op_t *
#ifndef NDEBUG
PFla_project__ (const PFla_op_t *n, unsigned int count, PFalg_proj_t *proj,
                const char *file, const char *func, const int line)
#else
PFla_project_ (const PFla_op_t *n, unsigned int count, PFalg_proj_t *proj)
#endif
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
                /* set name and type for this column in the result schema */
                ret->schema.items[i].name = ret->sem.proj.items[i].new;
                ret->schema.items[i].type = n->schema.items[j].type;

                break;
            }

        /* did we find the column? */
        if (j >= n->schema.count) {
#ifndef NDEBUG
            fprintf (stderr,
                    "\nThe following error is triggered"
                    " in line %i of function %s() in file %s\n"
                    "The input has the schema (",
                    line, func, file);
            for (unsigned int k = 0; k < n->schema.count; k++)
                fprintf (stderr, "%s%s",
                         k ? ", " : "",
                         PFcol_str (n->schema.items[k].name));
            fprintf (stderr,
                     ")\nThe projection list contains"
                     " the following mappings (");
            for (unsigned int k = 0; k < count; k++)
                fprintf (stderr, "%s%s:%s",
                         k ? ", " : "",
                         PFcol_str (proj[k].new),
                         PFcol_str (proj[k].old));
            fprintf (stderr, ")\n\n");
#endif
            PFoops (OOPS_FATAL,
                    "column `%s' referenced in projection not found",
                    PFcol_str (ret->sem.proj.items[i].old));
        }

        /* see if we have duplicate columns now */
        for (j = 0; j < i; j++)
            if (ret->sem.proj.items[i].new == ret->sem.proj.items[j].new) {
#ifndef NDEBUG
                fprintf (stderr,
                        "\nThe following error is triggered"
                        " in line %i of function %s() in file %s\n"
                        "The input has the schema (",
                        line, func, file);
                for (unsigned int k = 0; k < n->schema.count; k++)
                    fprintf (stderr, "%s%s",
                             k ? ", " : "",
                             PFcol_str (n->schema.items[k].name));
                fprintf (stderr,
                         ")\nThe projection list contains"
                         " the following mappings (");
                for (unsigned int k = 0; k < count; k++)
                    fprintf (stderr, "%s%s:%s",
                             k ? ", " : "",
                             PFcol_str (proj[k].new),
                             PFcol_str (proj[k].old));
                fprintf (stderr, ")\n\n");
#endif
                PFoops (OOPS_FATAL,
                        "projection results in duplicate column `%s' "
                        "(columns %i and %i)",
                        PFcol_str (ret->sem.proj.items[i].new), i+1, j+1);
            }
    }

    return ret;
}


/**
 * Selection of all rows where the value of column @a col is not 0.
 *
 * The result schema corresponds to the schema of the input
 * relation @a n.
 */
PFla_op_t*
PFla_select (const PFla_op_t *n, PFalg_col_t col)
{
    PFla_op_t    *ret;
    unsigned int  i;

    assert (n);

    /* verify that 'col' is a column of 'n' ... */
    for (i = 0; i < n->schema.count; i++)
        if (col == n->schema.items[i].name)
            break;

    /* did we find column col? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in selection not found",
                PFcol_str (col));

    /* build a new selection node */
    ret = la_op_wire1 (la_select, n);

    /* insert semantic value (select-column) into the result */
    ret->sem.select.col = col;

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
PFla_pos_select (const PFla_op_t *n, int pos, PFord_ordering_t s, PFalg_col_t p)
{
    PFord_ordering_t sortby = PFordering ();
    PFla_op_t       *ret    = la_op_wire1 (la_pos_select, n);
    unsigned int     i;
    unsigned int     j;

    assert (s);

    /* result schema is input schema plus the new column */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* see if we can find all sort specifications */
    for (i = 0; i < PFord_count (s); i++) {

        for (j = 0; j < n->schema.count; j++)
            if (n->schema.items[j].name == PFord_order_col_at (s, i))
                break;

        if (j == n->schema.count)
            PFoops (OOPS_FATAL,
                    "could not find sort column `%s'",
                    PFcol_str (PFord_order_col_at (s, i)));

        if (!monomorphic (n->schema.items[j].type))
            PFoops (OOPS_FATAL,
                    "sort criterion for pos_sel must be monomorphic, "
                    "type: %i, name: %s",
                    n->schema.items[j].type,
                    PFcol_str (n->schema.items[j].name));

        /* Throw away all identical order criteria that appear after
           the first appearance (as they cannot change the order). */
        for (j = 0; j < i; j++)
            if (PFord_order_col_at (s, i) == PFord_order_col_at (s, j))
                break;

        if ((!p || PFord_order_col_at (s, i) != p) && j == i)
            sortby = PFord_refine (sortby,
                                   PFord_order_col_at (s, i),
                                   PFord_order_dir_at (s, i));
    }

    /* copy parameters into semantic content of return node */
    ret->sem.pos_sel.pos = pos;
    ret->sem.pos_sel.sortby = sortby;
    ret->sem.pos_sel.part = p;

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

    /* see if both operands have same number of columns */
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

    /* see if we find each column of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two columns match, so include their name
                 * and type information into the result. This allows
                 * for the order of schema items in n1 and n2 to be
                 * different.
                 */
                if (kind == la_disjunion)
                    ret->schema.items[i] =
                        (struct PFalg_schm_item_t)
                            { .name = n1->schema.items[i].name,
                              .type = ERR_TYPE_MASK (
                                          n1->schema.items[i].type
                                        | n2->schema.items[j].type) };
                else if (kind == la_intersect)
                    ret->schema.items[i] =
                        (struct PFalg_schm_item_t)
                            { .name = n1->schema.items[i].name,
                              .type = n1->schema.items[i].type
                                    & n2->schema.items[j].type };
                else if (kind == la_difference)
                    ret->schema.items[i] =
                        (struct PFalg_schm_item_t)
                            { .name = n1->schema.items[i].name,
                              .type = n1->schema.items[i].type };
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
PFla_op_t *
PFla_distinct (const PFla_op_t *n)
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
               PFalg_col_t res,
               PFalg_collist_t *refs)
{
    PFla_op_t          *ret;
    unsigned int        i, j, ix[clsize (refs)];
    PFalg_simple_type_t res_type = 0;

    assert (n);

    /* verify that the referenced columns in refs
       are really columns of n ... */
    for (i = 0; i < clsize (refs); i++) {
        for (j = 0; j < n->schema.count; j++)
            if (n->schema.items[j].name == clat (refs, i))
                break;
        if (j == n->schema.count)
            PFoops (OOPS_FATAL,
                    "column `%s' referenced in generic function"
                    " operator not found (kind = %s)",
                    PFcol_str (clat (refs, i)), PFalg_fun_str(kind));
        ix[i] = j;
    }

    /* we want to perform some more consistency checks
       that are specific to certain operators */
    switch (kind) {
        /**
         * Depending on the @a kind parameter, we add, subtract, multiply, or
         * divide the two values of columns @a col1 and @a col2 and store the
         * result in newly created column @a res. @a res gets the same data
         * type as @a col1 and @a col2. The result schema corresponds to the
         * schema of the input relation @a n plus @a res.
         */
        case alg_fun_num_add:
        case alg_fun_num_subtract:
        case alg_fun_num_multiply:
        case alg_fun_num_divide:
        case alg_fun_num_modulo:
            assert (clsize (refs) == 2);
            /* make sure both columns are of the same numeric type */
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
        case alg_fun_pf_log:
        case alg_fun_pf_sqrt:
        case alg_fun_fn_round:
            assert (clsize (refs) == 1);
            /* make sure the column is of numeric type */
            assert (n->schema.items[ix[0]].type == aat_int ||
                    n->schema.items[ix[0]].type == aat_dec ||
                    n->schema.items[ix[0]].type == aat_dbl);

            res_type = n->schema.items[ix[0]].type;
            break;

        case alg_fun_fn_substring:
            assert (clsize (refs) == 2);
            /* make sure both columns are of type str & dbl */
            assert (n->schema.items[ix[0]].type == aat_str);
            assert (n->schema.items[ix[1]].type == aat_dbl);

            res_type = aat_str;
            break;

        case alg_fun_fn_substring_dbl:
            assert (clsize (refs) == 3);

            /* make sure columns are of type str & dbl */
            assert (n->schema.items[ix[0]].type == aat_str);
            assert (n->schema.items[ix[1]].type == aat_dbl &&
                    n->schema.items[ix[2]].type == aat_dbl );

            res_type = aat_str;
            break;

        case alg_fun_fn_concat:
        case alg_fun_fn_substring_before:
        case alg_fun_fn_substring_after:
            assert (clsize (refs) == 2);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_string_length:
            assert (clsize (refs) == 1);
            /* make sure the column is of type string */
            assert (n->schema.items[ix[0]].type == aat_str);

            res_type = aat_int;
            break;
        case alg_fun_fn_normalize_space:
        case alg_fun_fn_upper_case:
        case alg_fun_fn_lower_case:
            assert (clsize (refs) == 1);
            /* make sure the column is of type string */
            assert (n->schema.items[ix[0]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_contains:
        case alg_fun_fn_starts_with:
        case alg_fun_fn_ends_with:
        case alg_fun_fn_matches:
            assert (clsize (refs) == 2);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_bln;
            break;

#ifdef HAVE_GEOXML
        case alg_fun_geo_wkb:
            assert (clsize(refs) == 1);
            assert (n->schema.items[ix[0]].type == aat_str );
            res_type = aat_wkb;
            break;
        case alg_fun_geo_point:
            assert (clsize(refs) == 2);
            /* make sure both attributes are of type string */
	    /*
             * assert (n->schema.items[ix[0]].type == aat_dbl &&
             *        n->schema.items[ix[1]].type == aat_dbl);
	     */

            res_type = aat_wkb;
            break;
        case alg_fun_geo_distance:
            assert (clsize(refs) == 2);
            /* make sure both attributes are of type string */
	    /*
             * assert (n->schema.items[ix[0]].type == aat_dbl &&
             *        n->schema.items[ix[1]].type == aat_dbl);
	     */

            res_type = aat_dbl;
            break;
        case alg_fun_geo_geometry:
            assert (clsize(refs) == 1);
            assert (n->schema.items[ix[0]].type & aat_node );
            res_type = aat_wkb;
            break;
        case alg_fun_geo_relate:
            assert (clsize(refs) == 3);
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_wkb &&
		    n->schema.items[ix[2]].type == aat_wkb);
            res_type = aat_bln;
            break;
        case alg_fun_geo_intersection:
            assert (clsize(refs) == 2);
            assert (n->schema.items[ix[0]].type == aat_wkb &&
	            n->schema.items[ix[1]].type == aat_wkb );
            res_type = aat_wkb;
            break;
#endif
        case alg_fun_fn_matches_flag:
            assert (clsize (refs) == 3);
            /* make sure all columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str);

            res_type = aat_bln;
            break;

        case alg_fun_fn_translate:
        case alg_fun_fn_replace:
            assert (clsize (refs) == 3);
            /* make sure all columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_replace_flag:
            assert (clsize (refs) == 4);
            /* make sure all columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str &&
                    n->schema.items[ix[3]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_name:
        case alg_fun_fn_local_name:
        case alg_fun_fn_namespace_uri:
            assert (clsize (refs) == 1);
            /* make sure column is of type node */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = aat_str;
            break;

        case alg_fun_fn_number:
        case alg_fun_fn_number_lax:
            assert (clsize (refs) == 1);
            res_type = aat_dbl;
            break;

        case alg_fun_fn_qname:
            assert (clsize (refs) == 2);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_qname;
            break;

        case alg_fun_fn_doc_available:
            assert (clsize (refs) == 1);
            /* make sure column is of type string */
            assert (n->schema.items[ix[0]].type == aat_str);

            res_type = aat_bln;
            break;

        case alg_fun_pf_fragment:
            assert (clsize (refs) == 1);
            /* make sure column is of type node */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = aat_pnode;
            break;

        case alg_fun_pf_supernode:
            assert (clsize (refs) == 1);
            /* make sure column is of type node */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = n->schema.items[ix[0]].type;
            break;

        case alg_fun_pf_add_doc_str:
            assert(clsize (refs) == 3);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);
            assert(n->schema.items[ix[1]].type == aat_str);
            assert(n->schema.items[ix[2]].type == aat_str);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the column types*/
            res_type = aat_docmgmt | aat_path | aat_docnm | aat_colnm;
            break;

        case alg_fun_pf_add_doc_str_int:
            assert(clsize (refs) == 4);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);
            assert(n->schema.items[ix[1]].type == aat_str);
            assert(n->schema.items[ix[2]].type == aat_str);
            assert(n->schema.items[ix[3]].type == aat_int);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the column types */
            res_type = aat_docmgmt | aat_path | aat_docnm | aat_colnm;
            break;

        case alg_fun_pf_del_doc:
            assert(clsize (refs) == 1);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the column types */
            res_type = aat_docmgmt | aat_docnm;
            break;

        case alg_fun_pf_nid:
            assert(clsize (refs) == 1);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_pnode);

            res_type = aat_str;
            break;

        case alg_fun_pf_docname:
            assert(clsize (refs) == 1);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type & aat_node);

            res_type = aat_str;
            break;

        case alg_fun_upd_delete:
            assert(clsize (refs) == 1);

            /* make sure that the column is a node */
            assert(n->schema.items[ix[0]].type & aat_node);

            /* the result type is aat_update bitwise OR the type of
               the target_node shifted 4 bits to the left */
            assert((n->schema.items[ix[0]].type << 4) & aat_node1);
            res_type = aat_update | (n->schema.items[ix[0]].type << 4);
            break;

        case alg_fun_fn_year_from_datetime:
        case alg_fun_fn_month_from_datetime:
        case alg_fun_fn_day_from_datetime:
        case alg_fun_fn_hours_from_datetime:
        case alg_fun_fn_minutes_from_datetime:
            assert (clsize (refs) == 1);
            /* make sure column is of type datetime */
            assert (n->schema.items[ix[0]].type == aat_dtime);

            res_type = aat_int;
            break;

        case alg_fun_fn_seconds_from_datetime:
            assert (clsize (refs) == 1);
            /* make sure column is of type datetime */
            assert (n->schema.items[ix[0]].type == aat_dtime);

            res_type = aat_dec;
            break;

        case alg_fun_fn_year_from_date:
        case alg_fun_fn_month_from_date:
        case alg_fun_fn_day_from_date:
            assert (clsize (refs) == 1);
            /* make sure column is of type date */
            assert (n->schema.items[ix[0]].type == aat_date);

            res_type = aat_int;
            break;

        case alg_fun_fn_hours_from_time:
        case alg_fun_fn_minutes_from_time:
            assert (clsize (refs) == 1);
            /* make sure column is of type time */
            assert (n->schema.items[ix[0]].type == aat_time);

            res_type = aat_int;
            break;

        case alg_fun_fn_seconds_from_time:
            assert (clsize (refs) == 1);
            /* make sure column is of type time */
            assert (n->schema.items[ix[0]].type == aat_time);

            res_type = aat_dec;
            break;

        case alg_fun_add_dur:
        case alg_fun_subtract_dur:
        case alg_fun_multiply_dur:
        case alg_fun_divide_dur:
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
            assert(clsize (refs) == 2);

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
                    assert(n->schema.items[ix[1]].type == aat_uA);
                    assert((n->schema.items[ix[0]].type << 4) & aat_anode1);
                    break;
                case alg_fun_upd_replace_value:
                    assert(n->schema.items[ix[0]].type & aat_pnode);
                    assert(n->schema.items[ix[1]].type == aat_uA);
                    assert((n->schema.items[ix[0]].type << 4) & aat_pnode1);
                    break;
                case alg_fun_upd_replace_element:
                    assert(n->schema.items[ix[0]].type & aat_pnode);
                    assert(n->schema.items[ix[1]].type & aat_pnode);
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
    ret->sem.fun_1to1.refs = PFalg_collist_copy (refs);

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' column */
    ret->schema.items[ret->schema.count - 1].name = res;
    ret->schema.items[ret->schema.count - 1].type = res_type;

    return ret;
}


/** Constructs an operator for an arithmetic addition. */
PFla_op_t *
PFla_add (const PFla_op_t *n,
          PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_add,
                          res,
                          collist (col1, col2));
}


/** Constructs an operator for an arithmetic subtraction. */
PFla_op_t *
PFla_subtract (const PFla_op_t *n,
               PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_subtract,
                          res,
                          collist (col1, col2));
}


/** Constructs an operator for an arithmetic multiplication. */
PFla_op_t *
PFla_multiply (const PFla_op_t *n,
               PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_multiply,
                          res,
                          collist (col1, col2));
}


/** Constructs an operator for an arithmetic division. */
PFla_op_t *
PFla_divide (const PFla_op_t *n,
             PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_divide,
                          res,
                          collist (col1, col2));
}


/** Constructs an operator for an arithmetic modulo operation. */
PFla_op_t *
PFla_modulo (const PFla_op_t *n,
             PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return PFla_fun_1to1 (n,
                          alg_fun_num_modulo,
                          res,
                          collist (col1, col2));
}


/**
 * Constructor for numeric greater-than operators.
 *
 * The algebra operator `gt' works as follows: For each tuple, the
 * numeric value in column @a col1 is compared against @a col2.
 * If @a col1 is greater than @a col2 then the comparison yields
 * true, otherwise false. This value is returned as a boolean
 * value in the new column named by the argument @a res.
 *
 * @param n    The operand for the algebra operator (``The newly
 *             constructed node's child'')
 * @param res  Attribute name for the comparison result (This
 *             column will be appended to the schema.)
 * @param col1 Left operand of the `gt' operator.
 * @param col2 Right operand of the `gt' operator.
 */
PFla_op_t *
PFla_gt (const PFla_op_t *n,
         PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return compar_op (la_num_gt, n, res, col1, col2);
}


/** Constructor for numeric equal operators. */
PFla_op_t *
PFla_eq (const PFla_op_t *n,
         PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return compar_op (la_num_eq, n, res, col1, col2);
}


/** Constructor for boolean AND operators. */
PFla_op_t *
PFla_and (const PFla_op_t *n,
          PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return boolean_op (la_bool_and, n, res, col1, col2);
}


/** Constructor for boolean OR operators. */
PFla_op_t *
PFla_or (const PFla_op_t *n,
         PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    return boolean_op (la_bool_or, n, res, col1, col2);
}


/** Constructor for boolean NOT operators. */
PFla_op_t *
PFla_not (const PFla_op_t *n, PFalg_col_t res, PFalg_col_t col)
{
    PFla_op_t    *ret;
    unsigned int  i;

    assert (n);

    /* verify that 'col' is a column of n ... */
    if (!PFprop_ocol (n, col))
        PFoops (OOPS_FATAL,
                "column `%s' referenced in not operation not found",
                PFcol_str (col));

    assert (!PFprop_ocol (n, res));

    /* assert that 'col' is of correct type */
    assert (PFprop_type_of (n, col) == aat_bln);

    /* create new unary operator node */
    ret = la_op_wire1 (la_bool_not, n);

    /* insert semantic value (operand column and result column)
     * into the result
     */
    ret->sem.unary.col = col;
    ret->sem.unary.res = res;

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' column; it has the same type
     * as column 'col', but a different name
     */
    ret->schema.items[ret->schema.count - 1].name = res;
    ret->schema.items[ret->schema.count - 1].type = aat_bln;

    return ret;
}


/**
 * Encapsulates initialization stuff common to binary comparison
 * operators.
 *
 * Depending on the @a kind parameter, we connect the two values
 * of columns @a col1 and @a col2 and store the result in newly
 * created column @a res. @a res gets the same data type as @a
 * col1 and @a col2. The result schema corresponds to the schema
 * of the input relation @a n plus @a res.
 */
static PFla_op_t *
compar_op (PFla_op_kind_t kind, const PFla_op_t *n,
           PFalg_col_t res, PFalg_col_t col1, PFalg_col_t col2)
{
    PFla_op_t    *ret;
    unsigned int  i;
    int           ix1 = -1;
    int           ix2 = -1;

    assert (n);

    /* verify that 'col1' and 'col2' are columns of n ... */
    for (i = 0; i < n->schema.count; i++) {
        if (col1 == n->schema.items[i].name)
            ix1 = i;                /* remember array index of col1 */
        if (col2 == n->schema.items[i].name)
            ix2 = i;                /* remember array index of col2 */
    }

    /* did we find column 'col1' and 'col2'? */
    if (ix1 < 0)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in arithmetic operation "
                "not found", PFcol_str (col1));
    if (ix2 < 0)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in arithmetic operation "
                "not found", PFcol_str (col2));

    /* make sure both columns are of the same type */
    assert (n->schema.items[ix1].type == n->schema.items[ix2].type ||
            (n->schema.items[ix1].type & aat_node &&
             n->schema.items[ix2].type & aat_node));

    /* create new binary operator node */
    ret = la_op_wire1 (kind, n);

    /* insert semantic value (operand columns and result column)
     * into the result
     */
    ret->sem.binary.col1 = col1;
    ret->sem.binary.col2 = col2;
    ret->sem.binary.res = res;

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' column; it is of type
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
 * of columns @a col1 and @a col2 and store the result in newly
 * created column @a res. @a res gets the same data type as @a
 * col1 and @a col2. The result schema corresponds to the schema
 * of the input relation @a n plus @a res.
 */
static PFla_op_t *
boolean_op (PFla_op_kind_t kind, const PFla_op_t *n, PFalg_col_t res,
            PFalg_col_t col1, PFalg_col_t col2)
{
    PFla_op_t    *ret;
    unsigned int  i;
    int           ix1 = -1;
    int           ix2 = -1;

    assert (n);

    /* verify that 'col1' and 'col2' are columns of n ... */
    for (i = 0; i < n->schema.count; i++) {
        if (col1 == n->schema.items[i].name)
            ix1 = i;                /* remember array index of col1 */
        if (col2 == n->schema.items[i].name)
            ix2 = i;                /* remember array index of col2 */
    }

    /* did we find column 'col1' and 'col2'? */
    if (ix1 < 0)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in binary operation "
                "not found", PFcol_str (col1));
    if (ix2 < 0)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in binary operation "
                "not found", PFcol_str (col2));

    /* make sure that both columns are of type boolean */
    assert (n->schema.items[ix1].type == aat_bln);
    assert (n->schema.items[ix1].type == n->schema.items[ix2].type);

    /* create new binary operator node */
    ret = la_op_wire1 (kind, n);

    /* insert semantic value (operand columns and result column)
     * into the result
     */
    ret->sem.binary.col1 = col1;
    ret->sem.binary.col2 = col2;
    ret->sem.binary.res = res;

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' column; it is of type
     * boolean and named 'res'
     */
    ret->schema.items[ret->schema.count - 1].type = n->schema.items[ix1].type;
    ret->schema.items[ret->schema.count - 1].name = res;

    return ret;
}


/**
 * Constructor for op:to operator
 */
PFla_op_t *
PFla_to (const PFla_op_t *n, PFalg_col_t res,
         PFalg_col_t col1, PFalg_col_t col2)
{
    PFla_op_t    *ret = la_op_wire1 (la_to, n);

#ifndef NDEBUG
    /* verify that columns 'col1' and 'col2' are columns of n */
    if (!PFprop_ocol (n, col1))
        PFoops (OOPS_FATAL,
                "column `%s' referenced in op:to not found",
                PFcol_str (col1));
    if (!PFprop_ocol (n, col2))
        PFoops (OOPS_FATAL,
                "column `%s' referenced in op:to not found",
                PFcol_str (col2));
#endif

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' column; it is of type
     * integer and named 'res'
     */
    ret->schema.items[n->schema.count].name = res;
    ret->schema.items[n->schema.count].type = aat_int;

    /* insert semantic value (operand columns and result column)
     * into the result
     */
    ret->sem.binary.col1 = col1;
    ret->sem.binary.col2 = col2;
    ret->sem.binary.res = res;

    return ret;
}


/**
 * Constructor for aggregate operator that builds @a count number
 * of aggregates in parallel (based on the same partitioning column
 * @a part). Every attribute is of the kind count, sum, min, max, avg,
 * seqty1, all, prod, or dist.
 *
 *
 * The `seqty1' aggregate is particularly crafted to test the occurrence
 * indicator ``exactly one'' (`1'). It groups its argument according
 * to the column @a part. For each partition it will look at the
 * value column @a col. If there is exactly one tuple for the
 * partition, and if the value of @a col is @c true for this tuple,
 * the result for this partition will be @c true. In all other cases
 * (There is more than one tuple, or the single tuple contains @c false
 * in @a col.) the result for this partition will be @c false.
 *
 * The `all' aggregate looks into a group of tuples (by partitioning
 * column @a part), and returns @c true for this group iff all
 * values in column @a col for this group are @c true.
 *
 * The `dist' aggregate can be used only for columns that functionally
 * dependent on the partitioning column (and could be replaced by min or max).
 */
PFla_op_t *
PFla_aggr (const PFla_op_t *n, PFalg_col_t part,
           unsigned int count, PFalg_aggr_t *aggr)
{
    /* build a new aggregate node */
    PFla_op_t    *ret = la_op_wire1 (la_aggr, n);
    unsigned int  i;

    /* set number of schema items in the result schema
     * (result columns plus partitioning column)
     */
    ret->schema.count = count + (part ? 1 : 0);

    ret->schema.items = PFmalloc (ret->schema.count *
                                  sizeof (*(ret->schema.items)));

    /* insert semantic value (aggregates) into the result */
    ret->sem.aggr.part  = part;
    ret->sem.aggr.count = count;
    ret->sem.aggr.aggr  = PFmalloc (count * sizeof (PFalg_aggr_t));

    for (i = 0; i < count; i++) {
        PFalg_col_t col = aggr[i].col;
        if (col) {
            if (!PFprop_ocol (n, col))
                PFoops (OOPS_FATAL,
                        "column `%s' referenced in aggregate not found",
                        PFcol_str (col));
            ret->schema.items[i].type = PFprop_type_of (n, col);
        }
        else {
            ret->schema.items[i].type = aat_int;
        }
        ret->sem.aggr.aggr[i] = aggr[i];
        ret->schema.items[i].name = aggr[i].res;
    }
    if (part) {
        if (!PFprop_ocol (n, part))
            PFoops (OOPS_FATAL,
                    "column `%s' referenced in aggregate not found",
                    PFcol_str (part));
        ret->schema.items[count].name = part;
        ret->schema.items[count].type = PFprop_type_of (n, part);
    }

    return ret;
}


/**
 * Worker for PFla_rownum, PFla_rowrank, and PFla_rank.
 */
static PFla_op_t *
sort_col (const PFla_op_t *n, PFla_op_kind_t kind, char *name,
          PFalg_col_t a, PFord_ordering_t s, PFalg_col_t p)
{
    PFord_ordering_t sortby = PFordering ();
    PFla_op_t       *ret    = la_op_wire1 (kind, n);
    unsigned int     i;
    unsigned int     j;

    assert (s);

    /* result schema is input schema plus the new column */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++) {
        /* there must not be an argument column named like the new one */
        if (n->schema.items[i].name == a)
            PFoops (OOPS_FATAL,
                    "%s operator would result in duplicate column `%s'",
                    name, PFcol_str (a));

        /* copy this column specification */
        ret->schema.items[i] = n->schema.items[i];
    }
    /* append new column, named as given in a, with type nat */
    ret->schema.items[ret->schema.count - 1]
        = (struct PFalg_schm_item_t) { .name = a, .type = aat_nat };

    /* see if we can find all sort specifications */
    for (i = 0; i < PFord_count (s); i++) {
        for (j = 0; j < n->schema.count; j++)
            if (n->schema.items[j].name == PFord_order_col_at (s, i))
                break;

        if (j == n->schema.count)
            PFoops (OOPS_FATAL,
                    "could not find sort column `%s'",
                    PFcol_str (PFord_order_col_at (s, i)));

        /* Throw away all identical order criteria that appear after
           the first appearance (as they cannot change the order). */
        for (j = 0; j < i; j++)
            if (PFord_order_col_at (s, i) == PFord_order_col_at (s, j))
                break;

        if ((!p || PFord_order_col_at (s, i) != p) && j == i)
            sortby = PFord_refine (sortby,
                                   PFord_order_col_at (s, i),
                                   PFord_order_dir_at (s, i));
    }

    /* copy parameters into semantic content of return node */
    ret->sem.sort.res = a;
    ret->sem.sort.sortby = sortby;
    ret->sem.sort.part = p;

    return ret;
}


/**
 * The `rownum' operator, a Pathfinder-specific extension to the
 * relational algebra (behaves like SQL ROW_NUMBER()).
 */
PFla_op_t *
PFla_rownum (const PFla_op_t *n,
             PFalg_col_t a, PFord_ordering_t s, PFalg_col_t p)
{
    return sort_col (n, la_rownum, "rownum", a, s, p);
}


/**
 * The `rowrank' operator, a Pathfinder-specific extension to the
 * relational algebra (behaves like SQL DENSE_RANK()).
 */
PFla_op_t *
PFla_rowrank (const PFla_op_t *n, PFalg_col_t a, PFord_ordering_t s)
{
    if (PFord_count (s) == 0)
        PFinfo (OOPS_FATAL,
                "applying rank operator without sort specifier");

    return sort_col (n, la_rowrank, "rowrank", a, s, col_NULL);
}


/**
 * Constructor for the row ranking operator.
 * Special internal variant used during thetajoin optimization.
 *
 * The optimizer will fill check for consistency and
 * fill in the correct schema (which requires internal knowledge
 * of @a data).
 */
PFla_op_t *
PFla_rank_opt_internal (const PFla_op_t *n, PFalg_col_t res,
                        PFarray_t *data)
{
    PFla_op_t     *ret;

    assert (n);

    /* build new rowrank node */
    ret = la_op_wire1 (la_internal_op, n);

    /* insert semantic values into the result */
    ret->sem.rank_opt.kind   = la_rank;
    ret->sem.rank_opt.res    = res;
    ret->sem.rank_opt.sortby = PFarray_copy (data);

    /* allocate no schema -- this will be done by the optimization */

    return ret;
}


/**
 * The `rank' operator, a Pathfinder-specific extension to the
 * relational algebra (behaves like SQL DENSE_RANK()). In comparison
 * to rowrank we do not care about the generated values and can
 * therefore apply more rewrites.
 */
PFla_op_t *
PFla_rank (const PFla_op_t *n, PFalg_col_t a, PFord_ordering_t s)
{
    if (PFord_count (s) == 0)
        PFinfo (OOPS_FATAL,
                "applying rank operator without sort specifier");

    return sort_col (n, la_rank, "rank", a, s, col_NULL);
}


/**
 * The `rowid' operator, a Pathfinder-specific extension to the
 * relational algebra.
 */
PFla_op_t *
PFla_rowid (const PFla_op_t *n, PFalg_col_t a)
{
    PFla_op_t    *ret = la_op_wire1 (la_rowid, n);
    unsigned int  i;

    /* copy parameters into semantic content of return node */
    ret->sem.rowid.res  = a;

    /* result schema is input schema plus the new column */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++) {

        /* there must not be an argument column named like the new one */
        if (n->schema.items[i].name == a)
            PFoops (OOPS_FATAL,
                    "rowid operator would result in duplicate column `%s'",
                    PFcol_str (a));

        /* copy this column specification */
        ret->schema.items[i] = n->schema.items[i];
    }
    /* append new column, named as given in a, with type nat */
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
           PFalg_col_t res, PFalg_col_t col, PFalg_simple_type_t ty)
{
    PFla_op_t    *ret;
    unsigned int  i;

    assert (n);

    /* verify that 'col' is a column of 'n' ... */
    for (i = 0; i < n->schema.count; i++)
        if (col == n->schema.items[i].name)
            break;

    /* did we find column col? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in type test not found",
                PFcol_str (col));

    /* create new type test node */
    ret = la_op_wire1 (la_type, n);

    /* insert semantic value (type-tested column and its type,
     * result column) into the result
     */
    ret->sem.type.col = col;
    ret->sem.type.ty  = ty;
    ret->sem.type.res = res;

    /* allocate memory for the result schema (= schema(n) + 1 for the
     * 'res' column which is to hold the result of the type test)
     */
    ret->schema.count = n->schema.count + 1;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' column; it is of type
     * boolean
     */
    ret->schema.items[ret->schema.count - 1].type = aat_bln;
    ret->schema.items[ret->schema.count - 1].name = res;

    return ret;
}

/**
 * Constructor for type assertion check. The result is the
 * input relation n where the type of column col is replaced
 * by ty
 */
PFla_op_t * PFla_type_assert (const PFla_op_t *n, PFalg_col_t col,
                              PFalg_simple_type_t ty, bool pos)
{
    PFla_op_t    *ret;
    PFalg_simple_type_t assert_ty = 0;
    unsigned int  i;

    assert (n);

    /* verify that 'col' is a column of 'n' ... */
    for (i = 0; i < n->schema.count; i++)
        if (col == n->schema.items[i].name)
        {
            if (pos)
                assert_ty = n->schema.items[i].type & ty;
            else {
                /* the restricted type assert_ty is the original
                   type without type ty */
                assert_ty = n->schema.items[i].type -
                            (n->schema.items[i].type & ty);
                /* make sure that all node type bits are retained */
                if (assert_ty & aat_nkind)
                    assert_ty = assert_ty | aat_pre | aat_frag;
                /* make sure that all attr type bits are retained */
                if (assert_ty & aat_attr)
                    assert_ty = assert_ty | aat_pre | aat_frag;
            }
            break;
        }

    /* did we find column col? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in type_assertion not found",
                PFcol_str (col));

    /* if we statically know that the type assertion would yield
       an empty type we can replace it by an empty table. This
       is done in ope/opt_general.brg. Until then we however have
       to provide the expected type (if we have given an exact type). */
    if (!assert_ty && pos) {
        assert_ty = ty;
    }

    /* create new type test node */
    ret = la_op_wire1 (la_type_assert, n);

    /* insert semantic value (type-tested column and its type,
     * result column) into the result
     */
    ret->sem.type.col = col;
    ret->sem.type.ty  = assert_ty;
    ret->sem.type.res = col_NULL; /* don't use res field */

    ret->schema.count = n->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
    {
        if (col == n->schema.items[i].name)
        {
            ret->schema.items[i].name = col;
            ret->schema.items[i].type = assert_ty;
        }
        else
            ret->schema.items[i] = n->schema.items[i];
    }

    return ret;
}


/**
 * Constructor for a type cast of column @a col. The type of @a col
 * must be casted to type @a ty.
 */
PFla_op_t *
PFla_cast (const PFla_op_t *n, PFalg_col_t res,
           PFalg_col_t col, PFalg_simple_type_t ty)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (n);
    assert (res != col);

    /* verify that col is a column of n ... */
    for (i = 0; i < n->schema.count; i++)
        if (col == n->schema.items[i].name)
            break;

    /* did we find column col? */
    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in type cast not found",
                PFcol_str (col));

    /* create new type cast node */
    ret = la_op_wire1 (la_cast, n);

    /*
     * insert semantic value (type-tested column and its type)
     * into the result
     */
    ret->sem.type.col = col;
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
           PFalg_col_t iter, PFalg_col_t item,
           PFalg_col_t item_res)
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
                    "illegal column `%s' in path step",
                    PFcol_str (n->schema.items[i].name));
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
    else if (ret->sem.step.spec.axis == alg_anc_s)
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = PFprop_type_of (n, item)
                                                   | aat_pnode };
    else if (ret->sem.step.spec.axis == alg_desc_s ||
             ret->sem.step.spec.axis == alg_self)
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = PFprop_type_of (n, item) };
    else
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_pnode };

    return ret;
}

PFla_op_t *
PFla_step_simple (const PFla_op_t *doc, const PFla_op_t *n,
                  PFalg_step_spec_t spec,
                  PFalg_col_t iter, PFalg_col_t item,
                  PFalg_col_t item_res)
{
    return PFla_step (doc, n, spec, UNKNOWN_LEVEL, iter, item, item_res);
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
                PFalg_col_t item,
                PFalg_col_t item_res)
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
    ret->sem.step.iter        = col_NULL;
    ret->sem.step.item        = item;
    ret->sem.step.item_res    = item_res;

#ifndef NDEBUG
    if (PFprop_ocol (n, item_res))
        PFoops (OOPS_FATAL,
                "illegal column `%s' in the input "
                "of a path step",
                PFcol_str (item_res));
    if (!PFprop_ocol (n, item))
        PFoops (OOPS_FATAL,
                "column `%s' needed in path step is missing",
                PFcol_str (item));
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
    else if (ret->sem.step.spec.axis == alg_anc_s)
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = PFprop_type_of (n, item)
                                                   | aat_pnode };
    else if (ret->sem.step.spec.axis == alg_desc_s ||
             ret->sem.step.spec.axis == alg_self)
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = PFprop_type_of (n, item) };
    else
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = aat_pnode };

    return ret;
}

PFla_op_t *
PFla_step_join_simple (const PFla_op_t *doc, const PFla_op_t *n,
                       PFalg_step_spec_t spec,
                       PFalg_col_t item, PFalg_col_t item_res)
{
    return PFla_step_join (doc, n, spec, UNKNOWN_LEVEL, item, item_res);
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
                 PFalg_col_t iter, PFalg_col_t item,
                 PFalg_col_t item_res)
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
                    "illegal column `%s' in path step",
                    PFcol_str (n->schema.items[i].name));
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
    else if (ret->sem.step.spec.axis == alg_anc_s)
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = PFprop_type_of (n, item)
                                                   | aat_pnode };
    else if (ret->sem.step.spec.axis == alg_desc_s ||
             ret->sem.step.spec.axis == alg_self)
        ret->schema.items[1]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = PFprop_type_of (n, item) };
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
                        PFalg_col_t iter, PFalg_col_t item,
                        PFalg_col_t item_res)
{
    return PFla_guide_step (
               doc, n,
               spec,
               guide_count, guides,
               UNKNOWN_LEVEL,
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
                      int level, PFalg_col_t item, PFalg_col_t item_res)
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
    ret->sem.step.iter        = col_NULL;
    ret->sem.step.item        = item;
    ret->sem.step.item_res    = item_res;

#ifndef NDEBUG
    if (PFprop_ocol (n, item_res))
        PFoops (OOPS_FATAL,
                "illegal column `%s' in the input "
                "of a path step",
                PFcol_str (item_res));
    if (!PFprop_ocol (n, item))
        PFoops (OOPS_FATAL,
                "column `%s' needed in path step is missing",
                PFcol_str (item));
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
    else if (ret->sem.step.spec.axis == alg_anc_s)
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = PFprop_type_of (n, item)
                                                   | aat_pnode };
    else if (ret->sem.step.spec.axis == alg_desc_s ||
             ret->sem.step.spec.axis == alg_self)
        ret->schema.items[i]
            = (struct PFalg_schm_item_t) { .name = item_res,
                                           .type = PFprop_type_of (n, item) };
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
                             PFalg_col_t item, PFalg_col_t item_res)
{
    return PFla_guide_step_join (
               doc, n,
               spec,
               guide_count, guides,
               UNKNOWN_LEVEL, item, item_res);
}


PFla_op_t *
PFla_doc_index_join (const PFla_op_t *doc, const PFla_op_t *n,
                     PFla_doc_join_kind_t kind,
                     PFalg_col_t item,
                     PFalg_col_t item_res, PFalg_col_t item_doc,
                     const char* ns1, const char* loc1, const char* ns2, const char* loc2)
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
    ret->sem.doc_join.ns1      = ns1;
    ret->sem.doc_join.loc1     = loc1;
    ret->sem.doc_join.ns2      = ns2;
    ret->sem.doc_join.loc2     = loc2;

#ifndef NDEBUG
    if (PFprop_ocol (n, item_res))
        PFoops (OOPS_FATAL,
                "illegal column `%s' in the input "
                "of a doc index join",
                PFcol_str (item_res));
    if (!PFprop_ocol (n, item))
        PFoops (OOPS_FATAL,
                "column `%s' needed in doc index join is missing",
                PFcol_str (item));
    if (!(PFprop_type_of (n, item) == aat_str))
        PFoops (OOPS_FATAL,
                "wrong item type '0x%X' in the input of a doc index join",
                PFprop_type_of (n, item));
    if (!PFprop_ocol (n, item_doc))
        PFoops (OOPS_FATAL,
                "column `%s' needed in doc index join is missing",
                PFcol_str (item_doc));
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
PFla_doc_tbl (const PFla_op_t *n, PFalg_col_t res, PFalg_col_t col,
              PFalg_doc_tbl_kind_t kind)
{
    unsigned int i;
    PFla_op_t   *ret;

    ret = la_op_wire1 (la_doc_tbl, n);

    /* store columns to work on in semantical field */
    ret->sem.doc_tbl.res  = res;
    ret->sem.doc_tbl.col  = col;
    ret->sem.doc_tbl.kind = kind;

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
                 PFalg_col_t res, PFalg_col_t col, PFalg_doc_t doc_col)
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
        = (struct PFalg_schm_item_t) {
				.type = (doc_col != doc_qname)
						? aat_str
						: aat_qname,
				.name = res
			};

    ret->sem.doc_access.res = res;
    ret->sem.doc_access.col = col;
    ret->sem.doc_access.doc_col = doc_col;

    return ret;
}


/**
 * Constructor for twig root operators.
 */
PFla_op_t *
PFla_twig (const PFla_op_t *n, PFalg_col_t iter, PFalg_col_t item)
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
PFla_op_t *
PFla_fcns (const PFla_op_t *fc, const PFla_op_t *ns)
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
PFla_op_t *
PFla_docnode (const PFla_op_t *scope, const PFla_op_t *fcns, PFalg_col_t iter)
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
PFla_op_t *
PFla_element (const PFla_op_t *tag, const PFla_op_t *fcns,
              PFalg_col_t iter, PFalg_col_t item)
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
PFla_op_t *
PFla_attribute (const PFla_op_t *cont,
                PFalg_col_t iter, PFalg_col_t qn, PFalg_col_t val)
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
PFla_textnode (const PFla_op_t *cont, PFalg_col_t iter, PFalg_col_t item)
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
PFla_comment (const PFla_op_t *cont, PFalg_col_t iter, PFalg_col_t item)
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
PFla_op_t *
PFla_processi (const PFla_op_t *cont,
               PFalg_col_t iter, PFalg_col_t target, PFalg_col_t val)
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
PFla_op_t *
PFla_content (const PFla_op_t *doc, const PFla_op_t *cont,
              PFalg_col_t iter, PFalg_col_t pos, PFalg_col_t item)
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
                                   PFalg_col_t iter_in,
                                   PFalg_col_t pos_in,
                                   PFalg_col_t item_in,
                                   PFalg_col_t iter_res,
                                   PFalg_col_t pos_res,
                                   PFalg_col_t item_res)
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
PFla_frag_extract (const PFla_op_t *n, unsigned int pos_col)
{
    PFla_op_t *ret = la_op_wire1 (la_frag_extract, n);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    ret->sem.col_ref.pos = pos_col;

    return ret;
}

/**
 * Create empty set of fragments. It signals that an algebra expression
 * does not produce any xml nodes (any fragment).
 */
PFla_set_t *
PFla_empty_set (void)
{
    return PFarray (sizeof (PFla_op_t *), 0);
}

/**
 * Create a new set containing one fragment.
 */
PFla_set_t *
PFla_set (const PFla_op_t *n)
{
    PFarray_t *ret = PFarray (sizeof (PFla_op_t *), 1);

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
    PFarray_t *ret = PFarray (sizeof (PFla_op_t *),
                              PFarray_last (frag1) +
                              PFarray_last (frag2));

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
 * @a col) if a tuple flows through it.
 */
PFla_op_t *
PFla_error (const PFla_op_t *n1, const PFla_op_t *n2, PFalg_col_t col)
{
    PFla_op_t   *ret;
    unsigned int i;

    assert(n1);

    ret = la_op_wire2 (la_error, n1, n2);

    /* allocate memory for the result schema; it's the same schema as n's */
    ret->schema.count = n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n' */
    for (i = 0; i < n2->schema.count; i++) {
        ret->schema.items[i] = n2->schema.items[i];
    }

    ret->sem.err.col = col;

    return ret;
}


/**
 * Constructor for a caching operator.
 *
 * This operator puts a query to the query cache (with the key in @a id).
 */
PFla_op_t *
PFla_cache (const PFla_op_t *n1, const PFla_op_t *n2, char *id,
            PFalg_col_t pos, PFalg_col_t item)
{
    PFla_op_t   *ret;
    unsigned int i;

    assert(n1);

    ret = la_op_wire2 (la_cache, n1, n2);

    /* allocate memory for the result schema; it's the same schema as n's */
    ret->schema.count = n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 'n' */
    for (i = 0; i < n2->schema.count; i++) {
        ret->schema.items[i] = n2->schema.items[i];
    }

    ret->sem.cache.id   = id;
    ret->sem.cache.pos  = pos;
    ret->sem.cache.item = item;

    return ret;
}


/**
 * Constructor for a debug operator
 */
PFla_op_t *
PFla_trace (const PFla_op_t *n1, const PFla_op_t *n2)
{
    PFla_op_t     *ret;

    assert (n1);
    assert (n2);

    /* create new trace node */
    ret = la_op_wire2 (la_trace, n1, n2);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    return ret;
}


/**
 * Constructor for a debug items operator
 */
PFla_op_t *
PFla_trace_items (const PFla_op_t *n1,
                  const PFla_op_t *n2,
                  PFalg_col_t iter,
                  PFalg_col_t pos,
                  PFalg_col_t item)
{
    PFla_op_t     *ret;
    unsigned int   i;

    assert (n1);
    assert (n2);

    /* verify that iter, pos, and item are columns of n1 ... */
    if (!PFprop_ocol (n1, iter) ||
        !PFprop_ocol (n1, pos) ||
        !PFprop_ocol (n1, item))
        PFoops (OOPS_FATAL,
                "columns referenced in trace items operator not found");

    /* create new trace items node */
    ret = la_op_wire2 (la_trace_items, n1, n2);

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
                PFalg_col_t iter,
                PFalg_col_t item)
{
    PFla_op_t     *ret;
    unsigned int   i, found = 0;

    assert (n1);
    assert (n2);

    /* verify that iter and item are columns of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (iter == n1->schema.items[i].name ||
            item == n1->schema.items[i].name)
            found++;

    /* did we find all columns? */
    if (found != 2)
        PFoops (OOPS_FATAL,
                "columns referenced in trace message operator not found");

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
                PFalg_col_t      inner,
                PFalg_col_t      outer)
{
    PFla_op_t     *ret;
    unsigned int   i, found = 0;

    assert (n1);
    assert (n2);

    /* verify that inner and outer are columns of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (inner == n1->schema.items[i].name ||
            outer == n1->schema.items[i].name)
            found++;

    /* did we find all columns? */
    if (found != 2)
        PFoops (OOPS_FATAL,
                "columns referenced in trace operator not found");

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
PFla_op_t *
PFla_nil (void)
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
PFla_op_t *
PFla_rec_fix (const PFla_op_t *paramList, const PFla_op_t *res)
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
PFla_op_t *
PFla_rec_param (const PFla_op_t *arguments, const PFla_op_t *paramList)
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
PFla_op_t *
PFla_rec_arg (const PFla_op_t *seed, const PFla_op_t *recursion,
              const PFla_op_t *base)
{
    PFla_op_t     *ret;
    unsigned int   i, j;

    assert (seed);
    assert (recursion);
    assert (base);

    /* see if both operands have same number of columns */
    if (seed->schema.count != recursion->schema.count ||
        seed->schema.count != base->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of the arguments of recursion "
                "argument to not match");

    /* see if we find each column in all of the input relations */
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
PFla_op_t *
PFla_rec_base (PFalg_schema_t schema)
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
               PFalg_col_t iter, PFalg_occ_ind_t occ_ind)
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

    for (i = 0; i < schema.count; i++) {
        ret->schema.items[i].name = schema.items[i].name;
        ret->schema.items[i].type = PFprop_type_of (argument,
                                                    schema.items[i].name);
    }

    return ret;
}


/**
 * Constructor for the fragment information of a list item
 * of a parameter list related to function application
 */
PFla_op_t *
PFla_fun_frag_param (const PFla_op_t *argument,
                     const PFla_op_t *param_list,
                     unsigned int pos_col)
{
    PFla_op_t     *ret;

    assert (argument);
    assert (param_list);

    /* create new function application parameter node */
    ret = la_op_wire2 (la_fun_frag_param, argument, param_list);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    ret->sem.col_ref.pos = pos_col;

    return ret;
}


/**
 * Constructor for a proxy operator with a single child
 */
PFla_op_t *
PFla_proxy (const PFla_op_t *n, unsigned int kind,
            PFla_op_t *ref, PFla_op_t *base,
            PFalg_collist_t *new_cols, PFalg_collist_t *req_cols)
{
    return PFla_proxy2 (n, kind, ref, base, NULL, new_cols, req_cols);
}


/**
 * Constructor for a proxy operator with a two children
 */
PFla_op_t *
PFla_proxy2 (const PFla_op_t *n, unsigned int kind,
             PFla_op_t *ref, PFla_op_t *base1, PFla_op_t *base2,
             PFalg_collist_t *new_cols, PFalg_collist_t *req_cols)
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
    ret->sem.proxy.new_cols = PFalg_collist_copy (new_cols);
    ret->sem.proxy.req_cols = PFalg_collist_copy (req_cols);

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
                     PFalg_col_t iter, PFalg_col_t pos, PFalg_col_t item,
                     PFalg_col_t iter_sep, PFalg_col_t item_sep,
                     PFalg_col_t iter_res, PFalg_col_t item_res)
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
            return PFla_serialize_rel (left, right,
                                       n->sem.ser_rel.iter,
                                       n->sem.ser_rel.pos,
                                       n->sem.ser_rel.items);

        case la_side_effects:
            return PFla_side_effects (left, right);

        case la_lit_tbl:
        {
            PFalg_collist_t *collist = PFalg_collist (n->schema.count);
            for (unsigned int i = 0; i < n->schema.count; i++)
                cladd (collist) = n->schema.items[i].name;

            return PFla_lit_tbl_ (collist,
                                  n->sem.lit_tbl.count,
                                  n->sem.lit_tbl.tuples);
        } break;

        case la_empty_tbl:
            return PFla_empty_tbl_ (n->schema);

        case la_ref_tbl:
        {

            return PFla_ref_tbl_ (n->sem.ref_tbl.name,
                                  n->schema,
                                  n->sem.ref_tbl.tcols,
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
                                n->sem.eqjoin.col1,
                                n->sem.eqjoin.col2);

        case la_semijoin:
            return PFla_semijoin (left, right,
                                  n->sem.eqjoin.col1,
                                  n->sem.eqjoin.col2);

        case la_thetajoin:
            return PFla_thetajoin (left, right,
                                   n->sem.thetajoin.count,
                                   n->sem.thetajoin.pred);

        case la_project:
            return PFla_project_ (left,
                                  n->sem.proj.count,
                                  n->sem.proj.items);

        case la_select:
            return PFla_select (left, n->sem.select.col);

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
                              n->sem.binary.col1,
                              n->sem.binary.col2);

        case la_bool_and:
        case la_bool_or:
            return boolean_op (n->kind, left,
                               n->sem.binary.res,
                               n->sem.binary.col1,
                               n->sem.binary.col2);

        case la_bool_not:
            return PFla_not (left,
                             n->sem.unary.res,
                             n->sem.unary.col);

        case la_to:
            return PFla_to (left,
                            n->sem.binary.res,
                            n->sem.binary.col1,
                            n->sem.binary.col2);

        case la_aggr:
            return PFla_aggr (left,
                              n->sem.aggr.part,
                              n->sem.aggr.count,
                              n->sem.aggr.aggr);

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
                              n->sem.type.col,
                              n->sem.type.ty);

        case la_type_assert:
            return PFla_type_assert (left,
                                     n->sem.type.col,
                                     n->sem.type.ty,
                                     true);

        case la_cast:
            return PFla_cast (left,
                              n->sem.type.res,
                              n->sem.type.col,
                              n->sem.type.ty);

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
                                        n->sem.doc_join.item_doc,
                                        n->sem.doc_join.ns1,
                                        n->sem.doc_join.loc1,
                                        n->sem.doc_join.ns2,
                                        n->sem.doc_join.loc2);

        case la_doc_tbl:
            return PFla_doc_tbl (left,
                                 n->sem.doc_tbl.res,
                                 n->sem.doc_tbl.col,
                                 n->sem.doc_tbl.kind);

        case la_doc_access:
            return PFla_doc_access (left, right,
                                    n->sem.doc_access.res,
                                    n->sem.doc_access.col,
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
            return PFla_error (left,
                               right,
                               n->sem.err.col);

        case la_nil:
            return PFla_nil ();

        case la_cache:
            return PFla_cache (left,
                               right,
                               n->sem.cache.id,
                               n->sem.cache.pos,
                               n->sem.cache.item);

        case la_trace:
            return PFla_trace (left, right);

        case la_trace_items:
            return PFla_trace_items (left, right,
                                     n->sem.iter_pos_item.iter,
                                     n->sem.iter_pos_item.pos,
                                     n->sem.iter_pos_item.item);

        case la_trace_msg:
            return PFla_trace_msg (left, right,
                                   n->sem.iter_item.iter,
                                   n->sem.iter_item.item);

        case la_trace_map:
            return PFla_trace_map (left, right,
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
        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "Logical operator cloning does not"
                    "support node kind (%i).",
                    n->kind);
    }

    assert (!"this should never be reached (duplicate nodes)");

    return n; /* satisfy picky compilers */
}

/* vim:set shiftwidth=4 expandtab: */
