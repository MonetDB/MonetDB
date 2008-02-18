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

#include "oops.h"
#include "algebra_cse.h"
#include "array.h"
#include "alg_dag.h"

#include <assert.h>
#include <string.h> /* strcmp */

/* prune already checked nodes */
#define SEEN(p) ((p)->bit_dag)

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
static bool
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
                if (a.atoms[i].val.nat_ != b.atoms[i].val.nat_)
                    mismatch = true;
                break;
            /* if type is int, compare int member of union */
            case aat_int:
                if (a.atoms[i].val.int_ != b.atoms[i].val.int_)
                    mismatch = true;
                break;
            /* if type is str, compare str member of union */
            case aat_uA:
            case aat_str:
                if (strcmp (a.atoms[i].val.str, b.atoms[i].val.str))
                    mismatch = true;
                break;
            /* if type is float, compare float member of union */
            case aat_dec:
                if (a.atoms[i].val.dec_ != b.atoms[i].val.dec_)
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
                if (PFqname_eq (a.atoms[i].val.qname, b.atoms[i].val.qname))
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
        case la_empty_tbl:

            if (a->schema.count != b->schema.count)
                return false;

            for (unsigned int i = 0; i < a->schema.count; i++)
                if (a->schema.items[i].name != b->schema.items[i].name ||
                    a->schema.items[i].type != b->schema.items[i].type)
                    return false;

            if (a->sem.lit_tbl.count != b->sem.lit_tbl.count)
                return false;

            for (unsigned int i = 0; i < a->sem.lit_tbl.count; i++)
                if (!tuple_eq (a->sem.lit_tbl.tuples[i],
                               b->sem.lit_tbl.tuples[i]))
                    return false;

            return true;
            break;


        case la_ref_tbl:
    
            return  (strcmp(a->sem.ref_tbl.name, b->sem.ref_tbl.name) == 0);
            break;

        case la_attach:
            return (a->sem.attach.res == b->sem.attach.res &&
                    PFalg_atom_comparable (a->sem.attach.value,
                                           b->sem.attach.value) &&
                    !PFalg_atom_cmp (a->sem.attach.value,
                                     b->sem.attach.value));
            break;

        case la_eqjoin:
        case la_semijoin:
            return (a->sem.eqjoin.att1 == b->sem.eqjoin.att1
                    && a->sem.eqjoin.att2 == b->sem.eqjoin.att2);
            break;

        case la_thetajoin:
            if (a->sem.thetajoin.count != b->sem.thetajoin.count)
                return false;

            for (unsigned int i = 0; i < a->sem.thetajoin.count; i++)
                if ((a->sem.thetajoin.pred[i].comp !=
                     b->sem.thetajoin.pred[i].comp) ||
                    (a->sem.thetajoin.pred[i].left !=
                     b->sem.thetajoin.pred[i].left) ||
                    (a->sem.thetajoin.pred[i].right !=
                     b->sem.thetajoin.pred[i].right))
                    return false;

            return true;
            break;

        case la_eqjoin_unq:
            return (a->sem.eqjoin_unq.att1 == b->sem.eqjoin_unq.att1
                    && a->sem.eqjoin_unq.att2 == b->sem.eqjoin_unq.att2
                    && a->sem.eqjoin_unq.res == b->sem.eqjoin_unq.res);
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

        case la_select:
            return a->sem.select.att == b->sem.select.att;
            break;

        case la_pos_select:
            if (a->sem.pos_sel.pos != b->sem.pos_sel.pos)
                return false;

            for (unsigned int i = 0;
                 i < PFord_count (a->sem.pos_sel.sortby);
                 i++)
                if (PFord_order_col_at (a->sem.pos_sel.sortby, i) !=
                    PFord_order_col_at (b->sem.pos_sel.sortby, i) ||
                    PFord_order_dir_at (a->sem.pos_sel.sortby, i) !=
                    PFord_order_dir_at (b->sem.pos_sel.sortby, i))
                    return false;

            /* either both positional selection are partitioned or none */
            /* partitioning attribute must be equal (if available) */
            if (a->sem.pos_sel.part != b->sem.pos_sel.part)
                return false;

            return true;
            break;

        case la_fun_1to1:
            if (a->sem.fun_1to1.kind        != b->sem.fun_1to1.kind      ||
                a->sem.fun_1to1.res         != b->sem.fun_1to1.res       ||
                a->sem.fun_1to1.refs.count  != b->sem.fun_1to1.refs.count)
                return false;

            for (unsigned int i = 0; i < a->sem.fun_1to1.refs.count; i++)
                if (a->sem.fun_1to1.refs.atts[i] !=
                    b->sem.fun_1to1.refs.atts[i])
                    return false;

            return true;
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            return (a->sem.binary.att1 == b->sem.binary.att1
                    && a->sem.binary.att2 == b->sem.binary.att2
                    && a->sem.binary.res == b->sem.binary.res);
            break;

        case la_bool_not:
            return (a->sem.unary.att == b->sem.unary.att
                    && a->sem.unary.res == b->sem.unary.res);
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
            if (a->sem.aggr.att != b->sem.aggr.att
                || a->sem.aggr.res != b->sem.aggr.res)
                return false;

            /* either both aggregates are partitioned or none */
            /* partitioning attribute must be equal (if available) */
            if (a->sem.aggr.part != b->sem.aggr.part)
                return false;

            return true;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            if (a->sem.sort.res != b->sem.sort.res)
                return false;

            for (unsigned int i = 0; i < PFord_count (a->sem.sort.sortby); i++)
                if (PFord_order_col_at (a->sem.sort.sortby, i) !=
                    PFord_order_col_at (b->sem.sort.sortby, i) ||
                    PFord_order_dir_at (a->sem.sort.sortby, i) !=
                    PFord_order_dir_at (b->sem.sort.sortby, i))
                    return false;

            /* either both operators are partitioned or none */
            /* partitioning attribute must be equal (if available) */
            if (a->sem.sort.part != b->sem.sort.part)
                return false;

            return true;
            break;

        case la_rowid:
            if (a->sem.rowid.res != b->sem.rowid.res)
                return false;

            return true;
            break;

        case la_type:
        case la_cast:
            return (a->sem.type.att == b->sem.type.att
                    && a->sem.type.res == b->sem.type.res
                    && a->sem.type.ty == b->sem.type.ty);
            break;

        case la_type_assert:
            return (a->sem.type.att == b->sem.type.att
                    && a->sem.type.ty == b->sem.type.ty);
            break;

        case la_step:
        case la_step_join:
        case la_guide_step:
        case la_guide_step_join:
            if (a->sem.step.spec.axis   != b->sem.step.spec.axis
             || a->sem.step.spec.kind   != b->sem.step.spec.kind
             || PFqname_eq (a->sem.step.spec.qname, b->sem.step.spec.qname)
             || a->sem.step.guide_count != b->sem.step.guide_count
             || a->sem.step.level       != b->sem.step.level
             || a->sem.step.iter        != b->sem.step.iter
             || a->sem.step.item        != b->sem.step.item
             || a->sem.step.item_res    != b->sem.step.item_res)
                return false;

            for (unsigned int i = 0;
                 i < a->sem.step.guide_count;
                 i++)
                if (a->sem.step.guides[i] != b->sem.step.guides[i])
                    return false;

            return true;
            break;

        case la_doc_index_join:
            return (a->sem.doc_join.kind == b->sem.doc_join.kind
                    && a->sem.doc_join.item == b->sem.doc_join.item
                    && a->sem.doc_join.item_res == b->sem.doc_join.item_res
                    && a->sem.doc_join.item_doc == b->sem.doc_join.item_doc);
            break;

        case la_doc_access:
            return (a->sem.doc_access.res == b->sem.doc_access.res
                    && a->sem.doc_access.att == b->sem.doc_access.att
                    && a->sem.doc_access.doc_col == b->sem.doc_access.doc_col);
            break;

        case la_doc_tbl:
            return (a->sem.doc_tbl.att == b->sem.doc_tbl.att &&
                    a->sem.doc_tbl.res == b->sem.doc_tbl.res);
            break;

        case la_merge_adjacent:
            return (a->sem.merge_adjacent.iter_in
                    == b->sem.merge_adjacent.iter_in
                    && a->sem.merge_adjacent.pos_in
                    == b->sem.merge_adjacent.pos_in
                    && a->sem.merge_adjacent.item_in
                    == b->sem.merge_adjacent.item_in
                    && a->sem.merge_adjacent.iter_res
                    == b->sem.merge_adjacent.iter_res
                    && a->sem.merge_adjacent.pos_res
                    == b->sem.merge_adjacent.pos_res
                    && a->sem.merge_adjacent.item_res
                    == b->sem.merge_adjacent.item_res);
            break;

        case la_string_join:
            return (a->sem.string_join.iter
                    == b->sem.string_join.iter
                    && a->sem.string_join.pos
                    == b->sem.string_join.pos
                    && a->sem.string_join.item
                    == b->sem.string_join.item
                    && a->sem.string_join.iter_sep
                    == b->sem.string_join.iter_sep
                    && a->sem.string_join.item_sep
                    == b->sem.string_join.item_sep
                    && a->sem.string_join.iter_res
                    == b->sem.string_join.iter_res
                    && a->sem.string_join.item_res
                    == b->sem.string_join.item_res);
            break;

        case la_serialize_seq:
        case la_serialize_rel:
        case la_cross:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_distinct:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
            /* no extra semantic content to check for these operators */
            return true;
            break;

        case la_twig:
        case la_fcns:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
            /*
             * No common subexpression elimination performed here, since,
             * even if two node constructors are identical, they create
             * two separate instances of the node(s).
             */
            return false;
            break;

        case la_frag_extract:
            return (a->sem.col_ref.pos == b->sem.col_ref.pos);
            break;

        case la_error:
            return (a->sem.err.att == b->sem.err.att);
            break;

        case la_cond_err:
            return (a->sem.err.att == b->sem.err.att
                    && a->sem.err.str == b->sem.err.str);
            break;

        case la_nil:
            return true;
            break;

        case la_trace:
        case la_trace_msg:
            return false;
            break;

        case la_trace_map:
            return (a->sem.trace_map.inner == b->sem.trace_map.inner &&
                    a->sem.trace_map.outer == b->sem.trace_map.outer);
            break;

        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
            /*
             * we do neither split up nor merge recursion operators
             */
            return false;
            break;

        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
            /*
             * we do neither split up nor merge function application
             */
            return false;
            break;
            
        case la_proxy:
        case la_proxy_base:
            /* we assume that we do not split up proxy nodes */
            /* references would be screwed up by the rewriting */
            return false;
            break;

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");

        case la_dummy:
            PFoops (OOPS_FATAL,
                    "dummy operator has to be eliminated before "
                    "comparison!");
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

    /* skip operator if cse has been already applied */
    if (SEEN(n))
        return n;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++) {
        while (n->child[i]->kind == la_dummy)
            n->child[i] = n->child[i]->child[0];

        n->child[i] = la_cse (n->child[i]);

    }

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

    SEEN(n) = true;

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
    PFla_op_t *res;

    subexps = PFarray (sizeof (PFarray_t *));

    res = la_cse (n);
    PFla_dag_reset (res);

    return res;
}

/* vim:set shiftwidth=4 expandtab: */
