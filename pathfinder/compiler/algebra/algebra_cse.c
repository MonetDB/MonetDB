/**
 * @file
 *
 * Declarations for common subexpression elimination
 *
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
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Sabine Mayer <mayers@inf.uni-konstanz.de>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include "algebra_cse.h"
#include "array.h"
#include "subtyping.h"
#include "oops.h"

#include <string.h>
#include <assert.h>
#include <stdio.h>


static PFarray_t *subexps = NULL;

static PFalg_op_t * eliminate_subexp (PFalg_op_t *new);
static PFalg_op_t * find_subexp (PFalg_op_t *new);

static PFalg_op_t * find_lit_tbl (PFalg_op_t *new);
static PFalg_op_t * find_empty_tbl (PFalg_op_t *new);
static PFalg_op_t * find_doc_tbl (PFalg_op_t *new);
static PFalg_op_t * find_disjunion (PFalg_op_t *new);
static PFalg_op_t * find_intersect (PFalg_op_t *new);
static PFalg_op_t * find_cross (PFalg_op_t *new);
static PFalg_op_t * find_difference (PFalg_op_t *new);
static PFalg_op_t * find_eqjoin (PFalg_op_t *new);
static PFalg_op_t * find_scjoin (PFalg_op_t *new);
static PFalg_op_t * find_select (PFalg_op_t *new);
static PFalg_op_t * find_type (PFalg_op_t *new);
static PFalg_op_t * find_cast (PFalg_op_t *new);
static PFalg_op_t * find_project (PFalg_op_t *new);
static PFalg_op_t * find_rownum (PFalg_op_t *new);
static PFalg_op_t * find_binary (PFalg_op_t *new);
static PFalg_op_t * find_unary (PFalg_op_t *new);
static PFalg_op_t * find_sum (PFalg_op_t *new);
static PFalg_op_t * find_count (PFalg_op_t *new);
static PFalg_op_t * find_distinct (PFalg_op_t *new);
static PFalg_op_t * find_strconcat (PFalg_op_t *new);
static PFalg_op_t * find_merge_adjacent (PFalg_op_t *new);
static PFalg_op_t * find_roots (PFalg_op_t *new);
static PFalg_op_t * find_fragment (PFalg_op_t *new);
static PFalg_op_t * find_frag_union (PFalg_op_t *new);
static PFalg_op_t * find_empty_frag (PFalg_op_t *new);

static bool tuple_eq (PFalg_tuple_t a, PFalg_tuple_t b);


PFalg_op_t *
PFcse_eliminate (PFalg_op_t *n)
{
    /* initialize subexpression array */
    subexps = PFarray (sizeof (PFalg_op_t *));

    /* initiate subexpression elimination */
    return eliminate_subexp (n);
}


/**
 * Recursively traverses the algebra tree and searches it bottom-up
 * for common subexpressions. If we find an expression identical
 * to @ new in the array of existing expressions, we return
 * the old one. Otherwise, @ new itself is used.
 */
PFalg_op_t *
eliminate_subexp (PFalg_op_t *new)
{
    int i;

    /* recursively call subexpression elimination on all children of 'n' */
    for (i = 0; i < PFALG_OP_MAXCHILD && new->child[i] !=NULL; i++)
        new->child[i] = eliminate_subexp (new->child[i]);

    /* check if this node (n) was already built */
    return find_subexp (new);
}


/**
 * Test the equality of two literal table tuples.
 *
 * @param a Tuple to test against tuple @a b.
 * @param b Tuple to test against tuple @a a.
 * @return Boolean value @c true, if the two tuples are equal.
 */
bool
tuple_eq (PFalg_tuple_t a, PFalg_tuple_t b)
{
    int i;
    bool mismatch = false;

    /* schemata are not equal if they have a different number of attributes */
    if (a.count != b.count)
        return false;

    for (i = 0; i < a.count; i++) {
        if (a.atoms[i].type != b.atoms[i].type)
            break;

        switch (a.atoms[i].type) {
            /* if type is nat, compare nat member of union */
            case aat_nat:
                if (a.atoms[i].val.nat != b.atoms[i].val.nat)
                    mismatch = true;
                break;
            /* if type is int, compare int member of union */
            case aat_int:
                if (a.atoms[i].val.int_ != b.atoms[i].val.int_)
                    mismatch = true;
                break;
            /* if type is str, compare str member of union */
            case aat_str:
                if (strcmp(a.atoms[i].val.str, b.atoms[i].val.str))
                    mismatch = true;
                break;
            /* if type is node, compare node member of union */
            case aat_node:
                if (a.atoms[i].val.node != b.atoms[i].val.node)
                    mismatch = true;
                break;
            /* if type is float, compare float member of union */
            case aat_dec:
                if (a.atoms[i].val.dec != b.atoms[i].val.dec)
                    mismatch = true;
                break;
            /* if type is double, compare double member of union */
            case aat_dbl:
                if (a.atoms[i].val.dbl != b.atoms[i].val.dbl)
                    mismatch = true;
                break;
            /* if type is double, compare double member of union */
            case aat_bln:
                if ((a.atoms[i].val.bln && !b.atoms[i].val.bln) ||
                    (!a.atoms[i].val.bln && b.atoms[i].val.bln))
                    mismatch = true;
                break;
            case aat_qname:
                if (!PFqname_eq (a.atoms[i].val.qname, b.atoms[i].val.qname))
                    mismatch = true;
                break;
        }
        if (mismatch)
            break;
    }

    return (i == a.count);
}


/**
 * Check whether table @ new already exists in the array of
 * existing operators. It must have the same schema and the
 * same tuples as a table already in the array.
 */
PFalg_op_t *
find_lit_tbl (PFalg_op_t *new)
{
    unsigned int subex_idx;
    int j;

    assert (new);

    /* search for table in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /* see if we have the same number of arguments */
            if (new->schema.count != old->schema.count)
                continue;

            /* see if attribute names match old schema */
            for (j = 0; j < new->schema.count; j++)
                if (strcmp(new->schema.items[j].name,
                           old->schema.items[j].name))
                    break;
            if (j != new->schema.count)
                continue;

            /* test if number of tuples matches */
            if (new->sem.lit_tbl.count != old->sem.lit_tbl.count)
                continue;

            /* test if tuples match */
            for (j = 0; j < new->sem.lit_tbl.count; j++)
                if (!tuple_eq (new->sem.lit_tbl.tuples[j],
                               old->sem.lit_tbl.tuples[j]))
                    break;

            if (j != new->sem.lit_tbl.count)
                continue;

            /*
             * if we came until here, old and new table must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the table is a new operator, so add it to the array of
     * existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether table @ new already exists in the array of
 * existing operators. It must have the same schema as a table
 * already in the array.
 */
PFalg_op_t *
find_empty_tbl (PFalg_op_t *new)
{
    unsigned int subex_idx;
    int j;

    assert (new);

    /* search for table in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /* see if we have the same number of arguments */
            if (new->schema.count != old->schema.count)
                continue;

            /* see if attribute names match old schema */
            for (j = 0; j < new->schema.count; j++)
                if (strcmp(new->schema.items[j].name,
                           old->schema.items[j].name))
                    break;
            if (j != new->schema.count)
                continue;

            /*
             * if we came until here, old and new table must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the table is a new operator, so add it to the array of
     * existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}



/**
 * Search the arry of existing expressions for a representation of the
 * document table. This table can only exist once, so it suffices to
 * search for operators of kind 'document table'.
 */
PFalg_op_t *
find_doc_tbl (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for document table in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if ((old->kind != aop_doc_tbl)
            || (old->child[0] != new->child[0])
            || (old->child[1] != new->child[1]))
            continue;

        return old;
    }

    /*
     * the table is a new operator, so add it to the array of
     * existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * In case of a disjoint union operator, we just have to make sure
 * that @ new has the same two children as a union operator in the
 * array of existing operators.
 */
PFalg_op_t *
find_disjunion (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /*
     * search for disjoint union operator in the array of existing
     * operators
     */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child nodes: the order
             * of the operands (child[0] and child[1]) does not matter,
             * i.e we try both combinations; the schema of both nodes
             * is guaranteed to be equal; as for the node kind, we only
             * have to make sure that both node kinds match; the calling
             * routine makes sure that only union nodes can arrive here
             */
            if (!((new->child[0] == old->child[0]
                      && new->child[1] == old->child[1])
                || (new->child[0] == old->child[1]
                      && new->child[1] == old->child[0])))
                continue;

        return old;
        }
    }

    /* 
     * the union is a new operator, so add it to the array of existing
     * subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Intersection expressions look the same as disjoint union
 * operators. We check, if @ new has the same child nodes as
 * another existing expression.
 */
static PFalg_op_t * find_intersect (PFalg_op_t *new)
{
    return find_disjunion (new);
}


/**
 * Cross product expressions look the same as disjoint union
 * operators. We check, if @ new has the same child nodes as
 * another existing expression.
 */
static PFalg_op_t * find_cross (PFalg_op_t *new)
{
    return find_disjunion (new);
}


/**
 * Check whether difference operator @ new already exists in
 * the array of existing expressions. It must have the same
 * child nodes and order of child nodes as the node from the
 * array.
 */
PFalg_op_t *
find_difference (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /*
     * search for difference operator in the array of existing
     * operators
     */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child nodes: in contrast
             * to union and cross product operators, the order of the
             * child operands of a difference operator does matter
             */
            if (new->child[0] != old->child[0]
             || new->child[1] != old->child[1])
                continue;

            return old;
        }
    }

    /*
     * the difference is a new operator, so add it to the array
     * of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether equi-join operator @ new already exists in
 * the array of existing expressions. The equivalent must
 * have the same children and join attributes.
 */
PFalg_op_t *
find_eqjoin (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /*
     * search for equi-join operator in the array of existing
     * operators
     */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same children and join
             * attributes
             */
            if (new->child[0] != old->child[0]
             || new->child[1] != old->child[1]
	     || strcmp (new->sem.eqjoin.att1, old->sem.eqjoin.att1)
	     || strcmp (new->sem.eqjoin.att2, old->sem.eqjoin.att2))
                continue;

            /*
             * if we came until here, old and new join must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the equi-join is a new operator, so add it to the array
     * of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether staircase operator @ new already exists in
 * the array of existing expressions. The equivalent must
 * have the same children and represent the same location step
 * and kind test.
 */
PFalg_op_t *
find_scjoin (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /*
     * search for staircase join operator in the array of existing
     * operators
     */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same children, same location
             * step, and kind test
             */
            if (new->child[0] != old->child[0]
             || new->child[1] != old->child[1]
             || new->sem.scjoin.test != old->sem.scjoin.test
             || new->sem.scjoin.axis != old->sem.scjoin.axis
             || (old->sem.scjoin.test == aop_name
                 &&  PFqname_eq(new->sem.scjoin.str.qname,
                                old->sem.scjoin.str.qname))
             || (old->sem.scjoin.test == aop_pi_tar
                 && strcmp(new->sem.scjoin.str.target,
                           old->sem.scjoin.str.target)))
                continue;

            /*
             * if we came until here, old and new join must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the equi-join is a new operator, so add it to the array
     * of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Search the array of existing expressions for a selection that
 * retrieves all rows where the value of a certain column is not
 * 0. An equivalent operator must have the same child node and
 * selection attribute.
 */
PFalg_op_t *
find_select (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for selection operator in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child and selection
             * attribute
             */
            if (new->child[0] != old->child[0]
                || strcmp (new->sem.select.att, old->sem.select.att))
                continue;

            /*
             * if we came until here, old and new selection must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the selection is a new operator, so add it to the array
     * of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}

/**
 * Search the array of existing expressions for a type test that
 * compares the type of a certain column and stores the result of
 * the comparison in a new column. An equivalent operator must
 * have the same child node, the same typed and result attribute,
 * and the same requested type.
 */
PFalg_op_t *
find_type (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for type test operator in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child, typed attribute,
             * result attribute, and requested type
             */
            if (new->child[0] != old->child[0]
                || strcmp (new->sem.type.att, old->sem.type.att)
                || strcmp (new->sem.type.res, old->sem.type.res)
                || new->sem.type.ty != old->sem.type.ty)
                continue;

            /*
             * if we came until here, old and new type test must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the type test is a new operator, so add it to the array
     * of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Search the array of existing expressions for a type cast that
 * casts a certain column to a new type. An equivalent operator must
 * have the same child node, the same typed attribute, and the same
 * requested type.
 */
PFalg_op_t *
find_cast (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for type cast operator in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child, typed attribute,
             * and requested type
             */
            if (new->child[0] != old->child[0]
                || strcmp (new->sem.cast.att, old->sem.cast.att)
                || new->sem.cast.ty != old->sem.cast.ty)
                continue;
            
            /*
             * if we came until here, old and new type cast must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the type cast is a new operator, so add it to the array
     * of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether projection operator @ new already exists in
 * the array of existing expressions. It must have the same
 * child node, attribute number, and projection list as its
 * equivalent operator.
 */
PFalg_op_t *
find_project (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for projection operator in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));
        int         j;

        if (new->kind == old->kind) {

            /* see if the old node has the same child */
            if (new->child[0] != old->child[0])
                continue;

            /* does the old node have the same number of attributes? */
            if (new->sem.proj.count != old->sem.proj.count)
                continue;

            /* see if the projection lists match */
            for (j = 0; j < new->sem.proj.count; j++)
                if (strcmp (new->sem.proj.items[j].new,
                            old->sem.proj.items[j].new)
                 || strcmp (new->sem.proj.items[j].old,
                            old->sem.proj.items[j].old))
                    break;

            if (j != new->sem.proj.count)
                continue;

            /*
             * if we came until here, old and new projection must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the projection is a new operator, so add it to the array
     * of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether row-numbering operator @ new already exists in
 * the array of existing expressions. It must have the same
 * child node, row-numbering attribute, partitioning attribute,
 * and sort-by list.
 */
PFalg_op_t *
find_rownum (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for row-numbering operator in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));
        int         j;

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child and row-numbering
             * attribute
             */
            if (new->child[0] != old->child[0]
                || strcmp (new->sem.rownum.attname, old->sem.rownum.attname))
                continue;

            /* see if both nodes have the same partitioning attribute */
            if ((new->sem.rownum.part == NULL
                    && old->sem.rownum.part != NULL)
                || (new->sem.rownum.part != NULL
                    && old->sem.rownum.part == NULL)
                || (new->sem.rownum.part && old->sem.rownum.part
                    && strcmp (new->sem.rownum.part, old->sem.rownum.part)))
                continue;

            /* does the old node have the same number of sort attributes? */
            if (new->sem.rownum.sortby.count != old->sem.rownum.sortby.count)
                continue;

            /* see if the sortby lists match */
            for (j = 0; j < new->sem.rownum.sortby.count; j++)
                if (strcmp (new->sem.rownum.sortby.atts[j],
                            old->sem.rownum.sortby.atts[j]))
                    break;

            if (j != new->sem.rownum.sortby.count)
                continue;

            /*
             * if we came until here, old and new projection must be equal;
             * return the existing one
             */
            return old;
        }
    }

    /*
     * the projection is a new operator, so add it to the array
     * of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether binary expression @ new already exists in
 * the array of existing expressions. It must have the same
 * child nodes, operand attributes, and result attribute.
 */
PFalg_op_t *
find_binary (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for binary expression in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child, operand
             * attributes, and result attribute
             */
            if (new->child[0] != old->child[0]
             || strcmp (new->sem.binary.att1, old->sem.binary.att1)
             || strcmp (new->sem.binary.att2, old->sem.binary.att2)
             || strcmp (new->sem.binary.res, old->sem.binary.res))
                continue;
            
            /*
             * if we came until here, old and new binary expression
             * must be equal; return the existing one
             */
            return old;
        }
    }

    /*
     * the binary expression is a new operator, so add it to
     * the array of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether unary expression @ new already exists in
 * the array of existing expressions. It must have the same
 * child nodes, operand attribute, and result attribute.
 */
PFalg_op_t *
find_unary (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for unary expression in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child, operand
             * attribute, and result attribute
             */
            if (new->child[0] != old->child[0]
             || strcmp (new->sem.unary.att, old->sem.unary.att)
             || strcmp (new->sem.unary.res, old->sem.unary.res))
                continue;
            
            /*
             * if we came until here, old and new unary expression
             * must be equal; return the existing one
             */
            return old;
        }
    }

    /*
     * the unary expression is a new operator, so add it to
     * the array of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether the sum expression @ new already exists in
 * the array of existing expressions. It must have the same
 * child node, summed-up attribute, result attribute and list
 * of partitioning attributes.
 */
static PFalg_op_t * find_sum (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for sum expression in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
             * see if the old node has the same child, summed-up
             * attribute and result attribute
             */
            if (new->child[0] != old->child[0]
             || strcmp (new->sem.sum.att, old->sem.sum.att)
             || strcmp (new->sem.sum.res, old->sem.sum.res)
             || strcmp (new->sem.sum.part, old->sem.sum.part))
                continue;
            
            /*
             * if we came until here, old and new sum expression
             * must be equal; return the existing one
             */
            return old;
        }
    }

    /*
     * the sum expression is a new operator, so add it to
     * the array of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether the count expression @ new already exists in
 * the array of existing expressions. It must have the same
 * child node, result attribute and list of partitioning attributes.
 */
static PFalg_op_t * find_count (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for count expression in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /*
	     * see if the old node has the same child, same partitioning
	     * attribute and same result attribute
	     */
            if (new->child[0] != old->child[0]
             || strcmp (new->sem.count.res, old->sem.count.res)
             || strcmp (new->sem.count.part, old->sem.count.part))
                continue;
           
            /*
             * if we came until here, old and new row count expression
             * must be equal; return the existing one
             */
            return old;
        }
    }

    /*
     * the row count expression is a new operator, so add it to
     * the array of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether the duplicate elimination expression @ new
 * already exists in the array of existing expressions. It must
 * have the same child node.
 */
static PFalg_op_t * find_distinct (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for distinct expression in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind == old->kind) {

            /* see if the old node has the same child */
            if (new->child[0] != old->child[0])
                continue;
            
            /*
             * if we came until here, old and new distinct expression
             * must be equal; return the existing one
             */
            return old;
        }
    }

    /*
     * the distinct expression is a new operator, so add it to
     * the array of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether the strconcat expression @ new already exists
 * in the array of existing expressions. It must have the same child
 * node.
 */
static PFalg_op_t * find_strconcat (PFalg_op_t *new)
{
    return find_distinct (new);
}


/**
 * Check whether the merge_adjacent expression @ new already exists
 * in the array of existing expressions. It must have the same child
 * nodes and the same order of child nodes, just like a difference
 * expression.
 */
static PFalg_op_t * find_merge_adjacent (PFalg_op_t *new)
{
    return find_difference (new);
}


/**
 * See if we already saw a boolean grouping expression
 * (`seqty1' or `all') like @a new during CSE.
 */
static PFalg_op_t *
find_boolean_grouping (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for distinct expression in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (new->kind != old->kind)
            continue;

        if (new->child[0] != old->child[0])
            continue;

        if (strcmp (new->sem.blngroup.res, old->sem.blngroup.res)
            || strcmp (new->sem.blngroup.att, old->sem.blngroup.att)
            || strcmp (new->sem.blngroup.part, old->sem.blngroup.part))
            continue;

        /*
         * if we came until here, old and new grouping expression
         * must be equal; return the existing one
         */
        return old;
    }

    /*
     * the grouping expression is a new operator, so add it to
     * the array of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 * Check whether the roots expression @ new already exists in the
 * array of existing expressions. It must have the same child node.
 * We can use find_distinct () for this pupose, it offers the
 * required functionality.
 */
static PFalg_op_t * find_roots (PFalg_op_t *new)
{
    return find_distinct (new);
}


/**
 * Check whether the fragment expression @ new already exists in the
 * array of existing expressions. It must have the same child node.
 * We can use find_distinct () for this pupose, it offers the
 * required functionality.
 */
static PFalg_op_t * find_fragment (PFalg_op_t *new)
{
    return find_distinct (new);
}


/**
 * In case of a fragment union operator, we just have to make sure
 * that @ new has the same two children as any other union operator
 * in the array of existing operators.
 * The find_disjunion () routine offers the required functionality.
 */
static PFalg_op_t * find_frag_union (PFalg_op_t *new)
{
    return find_disjunion (new);
}


/**
 * Check whether an empty fragment has already been created. If so,
 * return the existing one.
 */
static PFalg_op_t * find_empty_frag (PFalg_op_t *new)
{
    unsigned int subex_idx;

    assert (new);

    /* search for empty fragment in the array of existing operators */
    for (subex_idx = 0; subex_idx < PFarray_last (subexps); subex_idx++) {

        PFalg_op_t *old = *((PFalg_op_t **) PFarray_at (subexps, subex_idx));

        if (old->kind != aop_empty_frag)
            continue;

        return old;
    }

    /*
     * the empty fragment has not yet been created, so add it to the
     * array of existing subexpressions
     */
    *((PFalg_op_t **) PFarray_add (subexps)) = new;

    return new;
}


/**
 *
 */
PFalg_op_t *
find_subexp (PFalg_op_t *new)
{
    switch (new->kind)
    {
        case aop_lit_tbl:
            return find_lit_tbl (new);

        case aop_empty_tbl:
            return find_empty_tbl (new);

        case aop_doc_tbl:
            return find_doc_tbl (new);

        case aop_disjunion:
            return find_disjunion (new);

        case aop_intersect:
            return find_intersect (new);

        case aop_cross:
            return find_cross (new);

        case aop_difference:
            return find_difference (new);

        case aop_eqjoin:
            return find_eqjoin (new);

        case aop_scjoin:
            return find_scjoin (new);

        case aop_select:
            return find_select (new);

        case aop_type:
            return find_type (new);

        case aop_cast:
            return find_cast (new);

        case aop_project:
            return find_project (new);

        case aop_rownum:
            return find_rownum (new);

        case aop_serialize:
            return new;

        case aop_num_add:
        case aop_num_subtract:
        case aop_num_multiply:
        case aop_num_divide:
        case aop_num_modulo:
        case aop_num_eq:
        case aop_num_gt:
        case aop_bool_and:
        case aop_bool_or:
            return find_binary (new);

        case aop_num_neg:
        case aop_bool_not:
	    return find_unary (new);

        case aop_sum:
	    return find_sum (new);

        case aop_count:
	    return find_count (new);

        case aop_distinct:
	    return find_distinct (new);

	/* no common subexpression elimination performed here, since, even
	 * if two element constructors are identical, they create two
	 * separate instances of the element(s)
	 */
        case aop_element:
        case aop_element_tag:
        case aop_attribute:
        case aop_textnode:
        case aop_docnode:
        case aop_comment:
        case aop_processi:
	    return new;

        case aop_concat:
	    return find_strconcat (new);

        case aop_merge_adjacent:
	    return find_merge_adjacent (new);

        case aop_seqty1:
        case aop_all:
            return find_boolean_grouping (new);

        case aop_roots:
	    return find_roots (new);

        case aop_fragment:
	    return find_fragment (new);

        case aop_frag_union:
	    return find_frag_union (new);

        case aop_empty_frag:
	    return find_empty_frag (new);

        case aop_dummy:
            PFoops (OOPS_FATAL, "illegal node in algebra expression tree");
            break;
    }

    return new;
}
