/**
 * @file
 *
 * Check whether applying the delta approach in the recursion
 * is possible without conflicts.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"
#include <assert.h>

#include "properties.h"
#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])

#define ITER(n) ((n)->prop->icols)
#define INNER(n) ((n)->prop->l_icols)
#define POS(n) ((n)->prop->r_icols)

#define REFS(n) ((n)->state_label)
#define NOT_USED(n) ((n)->prop->set)

/**
 * check the operator @a n based on the properties
 * of its children; worker for prop_check().
 */
static bool
check_op (PFla_op_t *n)
{
    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        /* we assume the following operators do not appear
           in initial plans */
        case la_semijoin:
        case la_thetajoin:
        case la_step_join:
        case la_guide_step_join:
        case la_proxy:
        case la_proxy_base:
        case la_cross_mvd:
        case la_eqjoin_unq:
        case la_dummy:
            PFoops (OOPS_FATAL,
                    "This property checking phase should be run "
                    "before optimization and inside recursion "
                    "generation only.");
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
            /* do not propagate or introduce any column information */
            break;

        case la_attach:
        case la_select:
        case la_pos_select:
        case la_fun_1to1:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_type:
        case la_type_assert:
        case la_doc_tbl:
        case la_roots:
        case la_trace:
        case la_cond_err:
            /* just propagate all column information */
            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            INNER(n) = INNER(L(n));
            break;

        case la_cross:
        case la_disjunion:
        case la_intersect:
            /* just propagate all column information */
            ITER (n) = ITER (L(n)) | ITER (R(n));
            POS  (n) = POS  (L(n)) | POS  (R(n));
            INNER(n) = INNER(L(n)) | INNER(R(n));
            break;

        case la_difference:
            /******************************************************************/
            /*                                                                */
            /* We can also allow difference operators if R(n) contains        */
            /* column that is marked ITER if no column in L(n) is marked      */
            /* ITER.                                                          */
            /* To make this change effective we need to get rid of distinct   */
            /* operators (that appear next to the difference operators in     */
            /* many scenarios -- e.g., exists, empty). Using the required     */
            /* value property to ignore the unused distinct operator makes    */
            /* this change effective (see also case difference and the        */
            /* introduction of NOT_USED in prop_check()).                    .*/
            /*                                                                */
            /* Applying the difference inside the DELTA approach would not    */
            /* result in wrong results as each loop relation on the left side */
            /* of the difference is adjusted during the recursion. Thus we    */
            /* still maintain the same syntax as the IFP variant:             */
            /*                                                                */
            /* R \ (X u Y u Z) = ((R \ X) \ Y) \ Z                            */
            /*                                                                */
            /* (The right side of the difference is X in the first recursive  */
            /* call and Y and Z in the respective second and third recursive  */
            /* call. Thus the union of X, Y, and Z is the input of the third  */
            /* recursive call using the IFP variant. The delta variant uses   */
            /* only Z to calculate the difference. Because the loop relation  */
            /* is already affected by X and Y in the previous recursive calls */
            /* the difference operator produces the same result.)             */
            /*                                                                */
            /******************************************************************/
            /*
            if (n->schema.count == 1 &&
                n->schema.items[0].name & ITER(R(n)))
                return true;
            */

            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            INNER(n) = INNER(L(n));
            break;

        case la_distinct:
            /******************************************************************/
            /*                                                                */
            /* Ignore conflicts in distinct operator if its result is never   */
            /* used. This usage is decided using the required value property. */
            /* Also look for the introduction of NOT_USED in prop_check.      */
            /*                                                                */
            /* This additional rule extends the XQuery subset that can be     */
            /* translated using the DELTA approach. It allows more than the   */
            /* queries that fulfill the distributivity property (see also     */
            /* case difference for further information).                      */
            /*                                                                */
            /******************************************************************/
            if (!NOT_USED(n) &&
                /* check for a reference to iter */
                n->schema.count == 1 &&
                n->schema.items[0].name & ITER(L(n)))
                return true;
            else if (NOT_USED(n) &&
                n->schema.count == 1 &&
                n->schema.items[0].name & ITER(L(n)))
                PFlog ("recursion strategy detection: "
                       "DISTINCT operator ignored.");
                /* do not infer the ITER column */
            else
                ITER (n) = ITER (L(n));

            POS  (n) = POS  (L(n));
            INNER(n) = INNER(L(n));
            break;

        case la_eqjoin:
            /* get the iter column through the map relation */
            if (n->sem.eqjoin.att1 & ITER(L(n)) &&
                n->sem.eqjoin.att2 == att_outer &&
                /* as consistency check make sure the map
                   relation has the schema inner|outer */
                R(n)->schema.count == 2 &&
                (R(n)->schema.items[0].name == att_inner ||
                 R(n)->schema.items[0].name == att_outer) &&
                (R(n)->schema.items[1].name == att_inner ||
                 R(n)->schema.items[1].name == att_outer))
                ITER(n) = att_inner;
            else if (n->sem.eqjoin.att2 & ITER(R(n)) &&
                n->sem.eqjoin.att1 == att_outer &&
                /* as consistency check make sure the map
                   relation has the schema inner|outer */
                L(n)->schema.count == 2 &&
                (L(n)->schema.items[0].name == att_inner ||
                 L(n)->schema.items[0].name == att_outer) &&
                (L(n)->schema.items[1].name == att_inner ||
                 L(n)->schema.items[1].name == att_outer))
                ITER(n) = att_inner;
            else
                ITER(n) = ITER(L(n)) | ITER(R(n));

            POS  (n) = POS  (L(n)) | POS  (R(n));
            INNER(n) = INNER(L(n)) | INNER(R(n));
            break;

        case la_project:
            /* rename ITER, POS and INNER columns */
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                if (ITER(L(n)) & n->sem.proj.items[i].old)
                    ITER(n) |= n->sem.proj.items[i].new;
                if (POS(L(n)) & n->sem.proj.items[i].old)
                    POS(n) |= n->sem.proj.items[i].new;
                if (INNER(L(n)) & n->sem.proj.items[i].old)
                    INNER(n) |= n->sem.proj.items[i].new;
            }
            break;

        case la_to:
            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            INNER(n) = INNER(L(n));
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
            /* check for a reference to iter */
            if (n->sem.aggr.part &&
                ITER(L(n)) & n->sem.aggr.part)
                return true;

            /* do not propagate or introduce any column information */
            break;

        case la_rownum:
            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            INNER(n) = INNER(L(n));

            /* a numbering partitioned by ITER indicates a new sequence
               order -- we thus add new position column */
            if (n->sem.rownum.part && ITER(n) & n->sem.rownum.part)
                POS(n) |= n->sem.rownum.res;
            break;

        case la_rank:
            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            INNER(n) = INNER(L(n));

            /* a numbering indicates a new sequence
               order -- we thus add new position column */
            if (ITER(n))
                POS(n) |= n->sem.rank.res;
            break;

        case la_number:
            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            INNER(n) = INNER(L(n));

            /* mark new iteration column that is generated
               based on the old input sequence.
               This column is needed to ensure that we do not
               use the cardinality to generate new nodes */
            if ((ITER(L(n)) & att_iter ||
                 INNER(L(n)) & att_iter) &&
                n->sem.number.res == att_inner)
                INNER(n) |= att_inner;
            break;

        case la_cast:
            /* check for a reference to pos */
            if (POS(L(n)) & n->sem.type.att)
                return true;

            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            INNER(n) = INNER(L(n));
            break;

        case la_step:
        case la_guide_step:
        case la_doc_index_join:
        case la_doc_access:
            ITER (n) = ITER (R(n));
            POS  (n) = POS  (R(n));
            INNER(n) = INNER(R(n));
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
            /* every constructor breaks the delta evaluation */
            return true;
            break;

        case la_merge_adjacent:
            /* we may not allow a reference to the input sequence */
            if (ITER(R(n)) & n->sem.merge_adjacent.iter_in)
                return true;

            ITER (n) = ITER (R(n));
            POS  (n) = POS  (R(n));
            INNER(n) = INNER(R(n));
            break;

        case la_nil:
        case la_trace_msg:
        case la_trace_map:
            /* do not propagate or introduce any column information */
            break;

        /* we have to assume that we see a nested recursion */
        case la_rec_fix:
            if (ITER(L(n)))
                ITER(n) = att_iter;
            if (INNER(L(n)))
                INNER(n) = att_iter;
            break;

        case la_rec_param:
            /* make sure to collect the iter and inner columns */
            ITER (n) = ITER (L(n)) | ITER (R(n));
            INNER(n) = INNER(L(n)) | INNER(R(n));
            break;

        case la_rec_arg:
            /* get the iter and inner columns only from the seeds */
            ITER (n) = ITER (L(n));
            INNER(n) = INNER(L(n));
            break;

        case la_rec_base:
        {
            int checks_failed = 3;
            /* initialize the iter and pos columns for the input
               relation (thus ignoring loop and result bases). */
            if (n->schema.count != 3) break;

            for (unsigned int i = 0; i < n->schema.count; i++) {
                if (n->schema.items[i].name == att_iter) {
                    ITER(n) = att_iter;
                    checks_failed--;
                }
                else if (n->schema.items[i].name == att_pos) {
                    POS(n) = att_pos;
                    checks_failed--;
                }
                else if (n->schema.items[i].name == att_item)
                    checks_failed--;
            }

            /* reset column information */
            if (checks_failed)
                ITER(n) = POS(n) = att_NULL;
        }   break;

        case la_string_join:
            /* we may not allow an aggregate on the input sequence */
            if (ITER(L(n)) & n->sem.string_join.iter)
                return true;

            ITER (n) = ITER (L(n));
            /* no pos available */
            INNER(n) = INNER(L(n));
            break;
    }
    return false;
}

/* worker for PFprop_check_rec_delta */
static bool
prop_check (PFla_op_t *n)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return false;

    /******************************************************************/
    /*                                                                */
    /* Mark distinct operators whose result is filtered out by a      */
    /* subsequent selection as NOT_USED. This allows us to be less    */
    /* restrictive with the DELTA recursion check. To decide whether  */
    /* a distinct operator is used we look at the required value      */
    /* property. If an attach operator that produces the filtered     */
    /* Boolean value sits on top of the distinct operator the         */
    /* result of the distinct operator will be ignored. (For more     */
    /* information see also case difference and case distinct in      */
    /* check_op()).                                                   */
    /*                                                                */
    /******************************************************************/
    if (L(n) && L(n)->kind == la_distinct) NOT_USED(L(n)) = false;
    if (R(n) && R(n)->kind == la_distinct) NOT_USED(R(n)) = false;

    if (n->kind == la_attach &&
        L(n)->kind == la_distinct &&
        n->schema.count == 2 &&
        /* this reference is the only one */
        REFS(L(n)) == 1 &&
        /* column item generates a value that may be required */
        PFprop_reqval (n->prop, n->sem.attach.res) &&
        /* the required value differs from the attached value */
        PFprop_reqval_val (n->prop, n->sem.attach.res) !=
        n->sem.attach.value.val.bln)
        /* mark the distinct operator as unused */
        NOT_USED(L(n)) = true;

    /* check properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        if (prop_check (n->child[i]))
            return true;

    n->bit_dag = true;

    /* reset ITER, POS, and INNER information */
    ITER (n) = att_NULL;
    POS  (n) = att_NULL;
    INNER(n) = att_NULL;

    return check_op (n);
}

/* worker for PFprop_check_rec_delta that counts the number
   of incoming edges for each operator */
static void
prop_count_refs (PFla_op_t *n)
{
    REFS(n)++;

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return;
    /* otherwise initialize edge counter (first occurrence) */
    else
        REFS(n) = 1;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_count_refs (n->child[i]);

    n->bit_dag = true;
}

/**
 * Check whether applying the delta approach in the recursion
 * body (@a root) is possible without conflicts.
 */
bool
PFprop_check_rec_delta (PFla_op_t *root) {
    bool found_conflict;

    /* infer the required value property to make
       the delta recursion check less restrictive
       for distinct operators */
    PFprop_infer_reqval (root);
    PFla_dag_reset (root);

    /* infer the number of incoming edges to be
       sure that we do not mark a distinct operator
       NOT_USED if it is referenced multiple times */
    prop_count_refs (root);
    PFla_dag_reset (root);

    found_conflict = prop_check (root);
    PFla_dag_reset (root);

    PFlog ("DELTA recursion strategy: %s",
           found_conflict ? "NO" : "YES");

    return !found_conflict;
}

/* vim:set shiftwidth=4 expandtab: */
