/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on the required node properties.
 * (This requires no burg pattern matching as we
 *  apply optimizations in a peep-hole style on
 *  single nodes only.)
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
 * 2008-2009 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"
#include <assert.h>
#include <stdio.h>

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */
#include "oops.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* worker for */
static void
opt_monetxq (PFla_op_t *p) {
 
    assert(p);
    
    /* node not seen then optimize */
    if (!p->bit_dag) {

        p->bit_dag = true;
    
        switch (p->kind) {
             
            /** 
            * replace
            *
            *             
            *       |                         |
            *     select      with         project     
            *       |                         |
            *                              distinct
            *                                 |
            *                              project
            *                                 |
            *                               select
            *                                 |
            */
            case la_doc_index_join:
            case la_thetajoin:     
            case la_pos_select: 
            case la_select: 
                if(PFprop_set (p->prop)) 
                {
                    /* getting needed colums (icols) and filling the 
                       list for later project */
                    PFalg_collist_t *icols = PFprop_icols_to_collist (p->prop);
                    PFalg_proj_t *cols = PFmalloc (clsize (icols) *
                                                    sizeof (PFalg_proj_t));
                    for (unsigned int i = 0; i < clsize (icols); i++)
                            cols[i] = PFalg_proj (
                                            clat (icols, i), clat (icols, i));
                                                
                    /* duplicate current select */                                                                                                                                                                                    
                    PFla_op_t *dup = PFla_op_duplicate(p, L(p), R(p));
                        
                    /* adding the inner project */                                                               
                    PFla_op_t *project = PFla_project_ (
                                                dup , clsize (icols), cols);
                
                    /* setting select and project as visited */
                    project->bit_dag = true;
                    dup->bit_dag = true;
                
                    /* denerate dummy colums needed for later operations */
                    PFalg_proj_t *dummy_cols = PFmalloc (p->schema.count *
                                                    sizeof (PFalg_proj_t));
                
                    bool found;
                    for (unsigned int i = 0; i < p->schema.count; i++) {
                        found = false;
                        for (unsigned int j = 0; j < clsize (icols); j++)
                            if (p->schema.items[i].name == clat (icols, j)) {
                                dummy_cols[i] = PFalg_proj (
                                            clat (icols, i), clat (icols, i));
                                found = true;
                                break;
                            }
                        if (!found) 
                            dummy_cols[i] = PFalg_proj (
                                    p->schema.items[i].name, clat (icols, 0));
                    }

                    /* adding outer project with dummy colums and the distinct 
                       operator around project<-select */
                    PFla_op_t *outer_project = PFla_project_ (
                                                        PFla_distinct(project), 
                                                        p->schema.count, 
                                                        dummy_cols);
            
                    /* replace current node with 
                       outer_project <- distinct <- inner_project <- select */
                    *p = *outer_project;
                    p->bit_dag = true;
                }
                break;
            
            case la_step_join: 
            {
                bool used = false;
                PFalg_collist_t *icols = PFprop_icols_to_collist (p->prop);
                
                for (unsigned int i = 0; i < clsize (icols); i++) 
                    if (PFprop_icol (p->prop, p->sem.step.item_res)) {
                        used = true;
                        break;
                    }
            
                if(PFprop_set (p->prop) &&
                    used) 
                {
                    /* getting needed colums (icols) and filling the 
                       list for later project */
                    PFalg_proj_t *cols = PFmalloc (clsize (icols) *
                                                    sizeof (PFalg_proj_t));
                    for (unsigned int i = 0; i < clsize (icols); i++)
                            cols[i] = PFalg_proj (
                                            clat (icols, i), clat (icols, i));
                                                
                    /* duplicate current select */                                                                                                                                                                                    
                    PFla_op_t *dup = PFla_op_duplicate(p, L(p), R(p));
                        
                    /* adding the inner project */                                                               
                    PFla_op_t *project = PFla_project_ (
                                                dup , clsize (icols), cols);
                
                    /* setting select and project as visited */
                    project->bit_dag = true;
                    dup->bit_dag = true;
                
                    /* denerate dummy colums needed for later operations */
                    PFalg_proj_t *dummy_cols = PFmalloc (p->schema.count *
                                                    sizeof (PFalg_proj_t));
                
                    bool found;
                    unsigned int i;
                    for ( i = 0; i < p->schema.count-1; i++) {
                        found = false;
                        for (unsigned int j = 0; j < clsize (icols); j++)
                            if (p->schema.items[i].name == clat (icols, j)) {
                                dummy_cols[i] = PFalg_proj (
                                            clat (icols, i), clat (icols, i));
                                found = true;
                                break;
                            }
                        if (!found) 
                            dummy_cols[i] = PFalg_proj (
                                    p->schema.items[i].name, clat (icols, 0));
                    }
                    dummy_cols[i] = PFalg_proj (
                                    p->sem.step.item_res, clat (icols, 0));

                    /* adding outer project with dummy colums and the distinct 
                       operator around project<-select */
                    PFla_op_t *outer_project = PFla_project_ (
                                                        PFla_distinct(project), 
                                                        p->schema.count, 
                                                        dummy_cols);
            
                    /* replace current node with 
                       outer_project <- distinct <- inner_project <- select */
                    *p = *outer_project;
                    p->bit_dag = true;
                }
                break;
            }
            
            case la_distinct:
            case la_project:
            case la_serialize_seq:
            case la_serialize_rel:
            case la_lit_tbl:
            case la_empty_tbl:
            case la_ref_tbl:
            case la_attach:
            case la_cross:
            case la_eqjoin:
            case la_semijoin:
            case la_disjunion:
            case la_intersect:
            case la_difference:
            case la_fun_1to1:
            case la_num_eq:
            case la_num_gt:
            case la_bool_and:
            case la_bool_or:
            case la_bool_not:
            case la_to:
            case la_avg:
            case la_max:
            case la_min:
            case la_sum:
            case la_count:
            case la_rownum:
            case la_rowrank:
            case la_rank:
            case la_rowid:
            case la_type:
            case la_type_assert:
            case la_cast:
            case la_seqty1:
            case la_all:
            case la_step:
            case la_guide_step:
            case la_guide_step_join:
            case la_doc_tbl:
            case la_doc_access:
            case la_twig:
            case la_fcns:
            case la_docnode:
            case la_element:
            case la_attribute:
            case la_textnode:
            case la_comment:
            case la_processi:
            case la_content:
            case la_merge_adjacent:
            case la_roots:
            case la_fragment:
            case la_frag_extract:
            case la_frag_union:
            case la_empty_frag:
            case la_error:
            case la_nil:
            case la_trace:
            case la_trace_msg:
            case la_trace_map:
            case la_rec_fix:
            case la_rec_param:
            case la_rec_arg:
            case la_rec_base:
            case la_fun_call:
            case la_fun_param:
            case la_fun_frag_param:
            case la_proxy:
            case la_proxy_base:
            case la_internal_op:
            case la_string_join:
            case la_dummy:
            case la_side_effects:
            case la_trace_items:
                break;
        }
    }
    
    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_monetxq (p->child[i]);
}

PFla_op_t *
PFalgopt_monetxq (PFla_op_t *root)
{
    PFprop_infer_set (root);
    PFprop_infer_icol (root);

    opt_monetxq (root);
        
                
    PFla_dag_reset (root);

    return root;
}
