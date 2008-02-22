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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
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
#include <stdio.h>

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define ITER(n) ((n)->prop->icols)
#define POS(n) ((n)->prop->r_icols)
#define IN(n) ((n)->prop->set)

#define REFS(n) ((n)->refctr)

/**
 * check the operator @a n based on the properties
 * of its children; worker for prop_check().
 */
static bool
check_op (PFla_op_t *n, bool op_used)
{
    if (L(n)) IN(n) |= IN(L(n));
    if (R(n)) IN(n) |= IN(R(n));
    
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
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_fun_frag_param:
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
        case la_to:
        case la_rowid:
        case la_type:
        case la_type_assert:
        case la_doc_tbl:
        case la_roots:
        case la_trace:
        case la_error:
        case la_cond_err:
            /* just propagate all column information */
            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            break;

        case la_cross:
            /* don't allow more than a single reference
               to the recursion variable */
            if (ITER (L(n)) && ITER (R(n)))
                return true;
            /* else fall through */
        case la_disjunion:
        case la_intersect:
            /* just propagate all column information */
            ITER (n) = ITER (L(n)) | ITER (R(n));
            POS  (n) = POS  (L(n)) | POS  (R(n));
            break;

        case la_difference:
            /******************************************************************/
            /*                                                                */
            /* Ignore conflicts in difference operator if its result is never */
            /* used. This usage is decided using the required value property. */
            /* Also look for the introduction of child_used in prop_check.    */
            /*                                                                */
            /* This additional rule extends the XQuery subset that can be     */
            /* translated using the DELTA approach. It allows more than the   */
            /* queries that fulfill the syntactic distributivity-safety       */
            /* property (see also case distinct for further information).     */
            /*                                                                */
            /******************************************************************/
            if (op_used &&
                /* check for a reference to iter */
                (ITER(L(n)) || ITER(R(n))))
                return true;
                
            /* just propagate all column information */
            ITER (n) = ITER (L(n)) | ITER (R(n));
            POS  (n) = POS  (L(n)) | POS  (R(n));
            break;

        case la_distinct:
            /******************************************************************/
            /*                                                                */
            /* We can also allow distinct operators because the existence     */
            /* check either does not use the recursion variable or is based   */
            /* on the recursion variable but only allows nodes to be added    */
            /* to the result that are independent of the recursion variable.  */
            /* This is guaranteed by the checks in rule la_cross and          */
            /* la_eqjoin. The added nodes furthermore can only appear once    */
            /* in the result (due to duplicate elimination and node           */
            /* constructors being forbidden for delta). It is thus correct to */
            /* apply a Delta approach as all nodes are added the first time   */
            /* the distinct check is true.                                    */
            /*                                                                */
            /******************************************************************/
            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            break;

        case la_eqjoin:
            /* don't allow more than a single reference
               to the recursion variable */
            if (ITER(L(n)) && ITER(R(n)))
                return true;

            ITER(n) = ITER(L(n)) | ITER(R(n));
            POS  (n) = POS  (L(n)) | POS  (R(n));

            /* get the iter column through the map relations */
            if (n->sem.eqjoin.att1 & ITER(L(n)) &&
                n->sem.eqjoin.att2 == att_outer &&
                /* as consistency check make sure the map
                   relation has the schema inner|outer */
                R(n)->schema.count == 2 &&
                (R(n)->schema.items[0].name == att_inner ||
                 R(n)->schema.items[0].name == att_outer) &&
                (R(n)->schema.items[1].name == att_inner ||
                 R(n)->schema.items[1].name == att_outer))
                ITER(n) = ITER(n) | att_inner;
            else if (n->sem.eqjoin.att2 & ITER(R(n)) &&
                n->sem.eqjoin.att1 == att_outer &&
                /* as consistency check make sure the map
                   relation has the schema inner|outer */
                L(n)->schema.count == 2 &&
                (L(n)->schema.items[0].name == att_inner ||
                 L(n)->schema.items[0].name == att_outer) &&
                (L(n)->schema.items[1].name == att_inner ||
                 L(n)->schema.items[1].name == att_outer))
                ITER(n) = ITER(n) | att_inner;
            /* get the iter column through the map relations */
            else if (n->sem.eqjoin.att1 & ITER(L(n)) &&
                n->sem.eqjoin.att2 == att_inner &&
                PFprop_ocol (R(n), att_outer))
                ITER(n) = ITER(n) | att_outer;
            else if (n->sem.eqjoin.att2 & ITER(R(n)) &&
                n->sem.eqjoin.att1 == att_inner &&
                PFprop_ocol (L(n), att_outer))
                ITER(n) = ITER(n) | att_outer;
            break;

        case la_project:
            /* rename ITER and POS columns */
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                if (ITER(L(n)) & n->sem.proj.items[i].old)
                    ITER(n) |= n->sem.proj.items[i].new;
                if (POS(L(n)) & n->sem.proj.items[i].old)
                    POS(n) |= n->sem.proj.items[i].new;
            }
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
        case la_rowrank:
        case la_rank:
            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));

            /* a numbering indicates a new sequence
               order -- we thus add new position column */
            if (ITER(n))
                POS(n) |= n->sem.sort.res;
            break;

        case la_cast:
            /* check for a reference to pos */
            if (POS(L(n)) & n->sem.type.att)
                return true;

            ITER (n) = ITER (L(n));
            POS  (n) = POS  (L(n));
            break;

        case la_step:
        case la_guide_step:
        case la_doc_index_join:
        case la_doc_access:
            ITER (n) = ITER (R(n));
            POS  (n) = POS  (R(n));
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
            if (IN(n))
                /* every inside constructor breaks the delta evaluation */
                return true;
            break;

        case la_merge_adjacent:
            /* we may not allow a reference to the input sequence */
            if (ITER(R(n)) & n->sem.merge_adjacent.iter_in)
                return true;

            ITER (n) = ITER (R(n));
            POS  (n) = POS  (R(n));
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
            break;

        case la_rec_param:
            /* make sure to collect the iter columns */
            ITER (n) = ITER (L(n)) | ITER (R(n));
            break;

        case la_rec_arg:
            /* get the iter columns only from the seeds */
            ITER (n) = ITER (L(n));
            break;

        case la_rec_base:
            /* loop relation */
            if (n->schema.count == 1) {
                if (n->schema.items[0].name == att_iter)
                    IN(n) = true;
            }
            /* recursion variable */
            else if (n->schema.count == 3) {
                bool iter = false;
                bool pos  = false;
                bool item = false;

                for (unsigned int i = 0; i < n->schema.count; i++) {
                    if (n->schema.items[i].name == att_iter)
                        iter = true;
                    else if (n->schema.items[i].name == att_pos)
                        pos = true;
                    else if (n->schema.items[i].name == att_item)
                        item = true;
                }

                if (iter && pos && item) {
                    ITER(n) = att_iter;
                    POS(n) = att_pos;
                    IN(n) = true;
                }
            }
            break;

        case la_fun_call:
        case la_fun_param:
            /* don't know what to do -- avoid the delta evaluation */
            return true;
            break;

        case la_string_join:
            /* we may not allow an aggregate on the input sequence */
            if (ITER(L(n)) & n->sem.string_join.iter)
                return true;

            ITER (n) = ITER (L(n));
            /* no pos available */
            break;
    }
    return false;
}

/* worker for PFprop_check_rec_delta */
static bool
prop_check (PFla_op_t *n, bool op_used)
{
    bool child_used = true;
    
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return false;

    /* do not follow fragment information */
    if (n->kind == la_frag_union)
        return false;

    /******************************************************************/
    /*                                                                */
    /* Mark difference operators whose result is filtered out by a    */
    /* subsequent selection as unused. This allows us to be less      */
    /* restrictive with the DELTA recursion check. To decide whether  */
    /* a difference operator is used we look at the required value    */
    /* property. If an attach operator that produces the filtered     */
    /* Boolean value sits on top of the distinct operator the         */
    /* result of the distinct operator will be ignored. (For more     */
    /* information see also case difference and case distinct in      */
    /* check_op()).                                                   */
    /*                                                                */
    /******************************************************************/
    if (n->kind == la_attach &&
        L(n)->kind == la_difference &&
        /* this reference is the only one */
        REFS(L(n)) == 1 &&
        /* column item generates a value that may be required */
        PFprop_req_bool_val (n->prop, n->sem.attach.res) &&
        /* the required value differs from the attached value */
        PFprop_req_bool_val_val (n->prop, n->sem.attach.res) !=
        n->sem.attach.value.val.bln)
        /* mark the difference operator as unused */
        child_used = false;

    if (n->kind == la_frag_union)
        return false;

    /* check properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        if (prop_check (n->child[i], child_used))
            return true;

    n->bit_dag = true;

    /* reset ITER and POS information */
    ITER (n) = att_NULL;
    POS  (n) = att_NULL;
    IN   (n) = false;

    return check_op (n, op_used);
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
       for difference operators */
    PFprop_infer_reqval (root);

    /* infer the number of incoming edges to be
       sure that we do not ignore a difference operator
       if it is referenced multiple times */
    PFprop_infer_refctr (root);

    found_conflict = prop_check (root, true);
    PFla_dag_reset (root);

    PFlog ("DELTA recursion strategy: %s",
           found_conflict ? "NO" : "YES");

    return !found_conflict;
}

/* vim:set shiftwidth=4 expandtab: */
