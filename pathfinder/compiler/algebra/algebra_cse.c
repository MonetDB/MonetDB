/**
 * @file
 *
 * Common subexpression elimination on the logical algebra.
 *
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include "oops.h"
#include "algebra_cse.h"
#include "array.h"

/* compare types in staircase join operator nodes */
#include "subtyping.h"

#include <assert.h>

/**
 * Subexpressions that we already saw.
 *
 * This is an array of arrays.  We create a separate for each algebra
 * node kind that we encounter.  This speeds up lookups when we search
 * for an existing algebra node.
 */
static PFarray_t *subexps;

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
    unsigned int i;
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
                if (a.atoms[i].val.str != b.atoms[i].val.str)
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

            /* anything else is actually bogus (e.g. there are no
             * literal nodes */
            default:
                PFinfo (OOPS_WARNING, "literal value that do not make sense");
                mismatch = true;
                break;
        }
        if (mismatch)
            break;
    }

    return (i == a.count);
}

/**
 * Test if two subexpressions are equal:
 *
 *  - both nodes must have the same kind,
 *  - both nodes must have the identical children (in terms of C
 *    pointers), and
 *  - if the nodes carry additional semantic content, the content
 *    of both nodes must match.
 */
static bool
subexp_eq (PFla_op_t *a, PFla_op_t *b)
{
    /* shortcut for the trivial case */
    if (a == b)
        return true;

    /* see if node kind is identical */
    if (a->kind != b->kind)
        return false;

    /* both nodes must have identical children */
    if (! (a->child[0] == b->child[0] && a->child[1] == b->child[1]))
        return false;

    /* now look at semantic content */
    switch (a->kind) {

        case la_lit_tbl:

            if (a->schema.count != b->schema.count)
                return false;

            for (unsigned int i = 0; i < a->schema.count; i++)
                if (a->schema.items[i].name != b->schema.items[i].name)
                    return false;

            if (a->sem.lit_tbl.count != b->sem.lit_tbl.count)
                return false;

            for (unsigned int i = 0; i < a->sem.lit_tbl.count; i++)
                if (!tuple_eq (a->sem.lit_tbl.tuples[i],
                               b->sem.lit_tbl.tuples[i]))
                    return false;

            return true;
            break;

        case la_eqjoin:
            return (a->sem.eqjoin.att1 == b->sem.eqjoin.att1
                    && a->sem.eqjoin.att2 == b->sem.eqjoin.att2);
            break;

        case la_scjoin:
            return (a->sem.scjoin.axis == b->sem.scjoin.axis
                    && PFty_subtype (a->sem.scjoin.ty, b->sem.scjoin.ty)
                    && PFty_subtype (b->sem.scjoin.ty, a->sem.scjoin.ty));
            break;

        case la_select:
            return a->sem.select.att == b->sem.select.att;
            break;

        case la_type:
            return (a->sem.type.att == b->sem.type.att
                    && a->sem.type.res == b->sem.type.res
                    && a->sem.type.ty == b->sem.type.ty);
            break;

        case la_cast:
            return (a->sem.cast.att == b->sem.cast.att
                    && a->sem.cast.ty == b->sem.cast.ty);
            break;

        case la_project:
            if (a->sem.proj.count != b->sem.proj.count)
                return false;

            for (unsigned int i = 0; i < a->sem.proj.count; i++)
                if (a->sem.proj.items[i].new != b->sem.proj.items[i].new
                    || a->sem.proj.items[i].old != b->sem.proj.items[i].old)
                    return false;

            return true;
            break;

        case la_rownum:
            if (a->sem.rownum.attname != b->sem.rownum.attname)
                return false;

            /* either both rownums are partitioned or none */ 
            /* partitioning attribute must be equal (if available) */
            if (a->sem.rownum.part != b->sem.rownum.part)
                return false;

            return true;
            break;

        case la_num_add:
        case la_num_subtract:
        case la_num_multiply:
        case la_num_divide:
        case la_num_modulo:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            return (a->sem.binary.att1 == b->sem.binary.att1
                    && a->sem.binary.att2 == b->sem.binary.att2
                    && a->sem.binary.res == b->sem.binary.res);
            break;

        case la_num_neg:
        case la_bool_not:
            return (a->sem.unary.att == b->sem.unary.att
                    && a->sem.unary.res == b->sem.unary.res);
            break;

        case la_sum:
            if (a->sem.sum.att != b->sem.sum.att
                || a->sem.sum.res != b->sem.sum.res)
                return false;

            /* either both aggregates are partitioned or none */
            /* partitioning attribute must be equal (if available) */
            if (a->sem.sum.part != b->sem.sum.part)
                return false;

            return true;
            break;

        case la_count:
            if (a->sem.count.res != b->sem.count.res)
                return false;

            /* either both aggregates are partitioned or none */
            /* partitioning attribute must be equal (if available) */
            if (a->sem.count.part != b->sem.count.part)
                return false;

            return true;
            break;

        case la_element:
        case la_element_tag:
        case la_attribute:
        case la_textnode:
        case la_docnode:
        case la_comment:
        case la_processi:
            /*
             * No common subexpression elimination performed here, since,
             * even if two node constructors are identical, they create
             * two separate instances of the node(s).
             */
            return false;

        case la_seqty1:
        case la_all:
            return (a->sem.blngroup.res == b->sem.blngroup.res
                    && a->sem.blngroup.att == b->sem.blngroup.att
                    && a->sem.blngroup.part == b->sem.blngroup.part);
            break;

        case la_empty_tbl:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_cross:
        case la_distinct:
        case la_concat:
        case la_merge_adjacent:
        case la_doc_access:
        case la_string_join:
        case la_serialize:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_doc_tbl:
            return true;
            break;

        case la_dummy:
            assert (!"should not encounter a `dummy' node here!");
            return true;
            break;
    }

    /* pacify compilers */
    assert (!"should never be reached");
    return true;
}

/**
 * Worker for PFla_cse().
 */
static PFla_op_t *
la_cse (PFla_op_t *n)
{
    PFarray_t *a;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        n->child[i] = la_cse (n->child[i]);

    /*
     * Fetch the subexpressions for this node kind that we
     * encountered so far (we maintain one array for each
     * node type).  If we have not yet seen any subexpressions
     * of that kind, we will have to initialize the respective
     * array first.
     */
    a = *(PFarray_t **) PFarray_at (subexps, n->kind);

    if (!a)
        *(PFarray_t **) PFarray_at (subexps, n->kind)
            = a = PFarray (sizeof (PFla_op_t *));

    /* see if we already saw that subexpression */
    for (unsigned int i = 0; i < PFarray_last (a); i++)
        if (subexp_eq (n, *(PFla_op_t **) PFarray_at (a, i)))
            return *(PFla_op_t **) PFarray_at (a, i);

    /* if not, add it to the list */
    *(PFla_op_t **) PFarray_add (a) = n;

    return n;
}

/**
 * Eliminate common subexpressions in logical algebra tree and
 * convert the expression @em tree into a @em DAG.  (Actually,
 * the input often is already a tree due to the way it was built
 * in core2alg.brg.  This function will detect @em any common
 * subexpression, though.)
 *
 * @param n logical algebra tree
 * @return the equivalent of @a n, with common subexpressions
 *         translated into @em sharing in a DAG.
 */
PFla_op_t *
PFla_cse (PFla_op_t *n)
{
    subexps = PFarray (sizeof (PFarray_t *));

    return la_cse (n);
}

/* vim:set shiftwidth=4 expandtab: */
