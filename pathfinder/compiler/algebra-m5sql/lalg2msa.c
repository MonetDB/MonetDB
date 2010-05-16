/**
 * @file
 *
 * Transforms the logical algebra tree into a tree that represents
 * SQL statements.
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
 * $Id: $
 */

/* always include pf_config.h first! */
#include "pf_config.h"
#include "pathfinder.h"

/** assert() */
#include <assert.h>
/** fprintf() */
#include <stdio.h>
/** strcpy, strlen, ... */
#include <string.h>

#include "oops.h"             /* PFoops() */
#include "mem.h"
#include "array.h"
#include "string_utils.h"

#include "lalg2msa.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#include "msa.h"
#include "algebra.h"
#include "alg_dag.h"
#include "ordering.h"

#include "msa_mnemonic.h"

/* Auxiliary function that searches for a certain col 
   in a expression list and returns an expression
   representing the found column. If no column was
   found, NULL will be returned */
static PFmsa_expr_t *
find_expr(PFmsa_exprlist_t *expr_list, PFalg_col_t col)
{
    unsigned int i;
    
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        if ((curr_expr->col) == col)
            return curr_expr;
    }
    return NULL;
}

/* Auxiliary function that constructs an expression list containing
   column references of the columns in a schema */
static PFmsa_exprlist_t *
exprlist_from_schema(PFalg_schema_t schema)
{
    unsigned int i;
    PFmsa_exprlist_t *ret = el(schema.count);
    
    for (i = 0; i < schema.count; i++) {
        PFmsa_expr_t *new_expr = PFmsa_expr_column(schema.items[i].name, schema.items[i].type);
        eladd(ret) = new_expr;
    }
    
    return ret;
}

static PFmsa_expr_t *
deep_copy_expr(PFmsa_expr_t *n)
{
    PFmsa_expr_t *ret = PFmalloc (sizeof(PFmsa_expr_t));
    memcpy(ret, n, sizeof(PFmsa_expr_t));
    unsigned int i;
    
    for (i = 0; i < PFMSA_OP_MAXCHILD && n->child[i]; i++) {
        ret->child[i] = deep_copy_expr(n->child[i]);
    }
    
    return ret;
}

/* Bind a msa operator to a la operator during translation. That is:
   Put a selection and projection operator on top of the current
   operator to 'materialize' changes in schema and predicates */
static void
bind(PFla_op_t *n)
{
    PFmsa_op_t *sel = PFmsa_op_select (OP(n), SEL_LIST(n));
    PFmsa_op_t *prj = PFmsa_op_project (sel, false, PRJ_LIST(n));
    
    OP(n) = prj;
    PRJ_LIST(n) = exprlist_from_schema (n->schema);
    SEL_LIST(n) = el(1);
    
    return;
}

/* ----- Translate the logical algebra into M5 SQL algebra ----- */

/* Worker that performes translation from la to msa. Note that 
   not every operator is translated 1-to-1 but so called selection
   and projection lists are carried along, which represent changes
   in schema (e.g. attaching of a row) or selection predicates.
 
   The function is bottom-up: We descend to leaf nodes, compute
   operators and lists and write them to an annotation field (msa_ann)
   in the original la operator */
static void
alg2msa_worker(PFla_op_t *n)
{
    unsigned int i;
    /* declarations need for the translation: msa annotation and its contents */
    PFmsa_ann_t *msa_ann;
    PFmsa_op_t *op;
    PFmsa_exprlist_t *prj_list;
    PFmsa_exprlist_t *sel_list;
    
    /* traverse child operators recursively */
    for (i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++) {
        if (!n->child[i]->bit_dag) 
            alg2msa_worker(n->child[i]);
    }
    
    switch (n->kind) {
            
        case la_serialize_seq:
        {
            PFalg_collist_t         *collist;
            
            /* bind node containing la DAG */
            bind(R(n));
            
            /* la_side_effects node will not be bound */
            
            /* build list containing the item column */
            collist = PFalg_collist(1);
            PFalg_collist_add(collist) = n->sem.ser_seq.item;
            
            op = PFmsa_op_serialize_rel(OP (R(n)),
                                        OP (L(n)),
                                        col_NULL,
                                        n->sem.ser_seq.pos,
                                        collist);
            
            /* not necessary to set project nor select list */
        }
            break;
            
        case la_serialize_rel:
            
            /* FIXME: NOT TESTED YET! */
            
            /* bind node with la DAG */
            bind(R(n));
            
            /* la_side_effects node will not be bound */
            
            op = PFmsa_op_serialize_rel(OP (R(n)),
                                        OP (L(n)),
                                        n->sem.ser_rel.iter,
                                        n->sem.ser_rel.pos,
                                        n->sem.ser_rel.items);
            
            /* not necessary to set project nor select list */
            
            break;
            
        case la_side_effects:
            
            /* build operator and lists */
            op = PFmsa_op_nil_node();
            prj_list = NULL;
            sel_list = NULL;
            
            break;
            
        case la_lit_tbl:
            /* Build literal table operator with the information
               of the original la operator. */
            
            /* build operator and lists */
            op = PFmsa_op_literal_table(n->schema, n->sem.lit_tbl.count,
                                        n->sem.lit_tbl.tuples);
            prj_list = exprlist_from_schema(n->schema);
            sel_list = el(1);
            
            break;
            
        case la_empty_tbl:
            /* Build empty table operator with the schema
             of the original la operator. */
            
            /* build operator and lists */
            op = PFmsa_op_literal_table(n->schema, 0,
                                        NULL);
            prj_list = exprlist_from_schema(n->schema);
            sel_list = el(1);
            
            break;
            
        case la_ref_tbl:
        {
            /* Build table reference operator with the information
               of the original la operator. Therefor construct a list
               of atom expressions representing the original column
               names */
            
            /* FIXME: NOT TESTED YET! */
            
            char*               table_name;
            char*               org_name;
            PFmsa_exprlist_t    *exprlist;
            PFmsa_expr_t        *atom;
            
            table_name = PFstrdup(n->sem.ref_tbl.name);
            
            /* fill projection list with original names of columns */
            exprlist = el(PFarray_last (n->sem.ref_tbl.tcols));
            for (i = 0; i < PFarray_last (n->sem.ref_tbl.tcols); i++){
                org_name = *(char**) PFarray_at (n->sem.ref_tbl.tcols, i);
                
                atom = PFmsa_expr_atom(n->schema.items[i].name, 
                                       PFalg_lit_str(org_name));

                eladd(exprlist) = atom;
            }

            prj_list = exprlist_from_schema(n->schema);
            /* build operator and lists */
            op = PFmsa_op_table(table_name,
                                n->schema,
                                prj_list,
                                exprlist);

            sel_list = el(1);

        }
            break;
            
        case la_attach:
            /* Append atom expression representing attached value
             to projection list. Operator and selection list stay
             unchanged. */
            
            op = OP (L(n));
            prj_list = elcopy (PRJ_LIST (L(n)));
            
            eladd(prj_list) = PFmsa_expr_atom(n->sem.attach.res,
                                              n->sem.attach.value);
            sel_list = SEL_LIST (L(n));
            
            break;
            
        case la_cross:
            
            bind(L(n));
            bind(R(n));
            
            /* build operator and lists */
            op = PFmsa_op_cross(OP(L(n)), OP(R(n)));
            
            prj_list = exprlist_from_schema (n->schema);
            
            sel_list = el(1);
            
            break;
            
        case la_eqjoin:
        case la_semijoin:
        {
            /* Translate eqjoin/semijoin operator. Append both projection
             lists (cross) and build join operator with selection predicates.
             Of course all selection expressions from underlying operators
             are preserved. */
            
            PFmsa_exprlist_t                *exprlist;
            PFmsa_expr_t                    *expr;
            PFmsa_expr_t                    *left_col;
            PFmsa_expr_t                    *right_col;
            
            PFmsa_exprlist_t                *prj_list2;
            PFmsa_exprlist_t                *sel_list2;
            
            /* build list with equality join predicate */
            exprlist = el(1);
            
            left_col = find_expr(PRJ_LIST (L(n)),
                                 n->sem.eqjoin.col1);
            
            right_col = find_expr(PRJ_LIST (R(n)),
                                  n->sem.eqjoin.col2);
            
            /* build expression for equality condition */
            expr = PFmsa_expr_comp(left_col, 
                                        right_col,
                                        msa_comp_equal,
                                        PFcol_new(col_item));
            eladd(exprlist) = expr;
            
            switch (n->kind) {
                case la_eqjoin:
                    op = PFmsa_op_join(OP(L(n)),
                                       OP(R(n)),
                                       exprlist);
                    
                    break;
                case la_semijoin:
                    op = PFmsa_op_semijoin(OP(L(n)),
                                           OP(R(n)),
                                           exprlist);
                    break;
                default:
                    break;
            }
            
            prj_list = elcopy(PRJ_LIST(L(n)));
            prj_list2 = elcopy(PRJ_LIST(R(n)));
            elconcat(prj_list, prj_list2);
            
            sel_list = elcopy(SEL_LIST(L(n)));
            sel_list2 = elcopy(SEL_LIST(R(n)));
            elconcat(sel_list, sel_list2);
        }
            break;
            
        case la_thetajoin:
        {
            /* FIXME: NOT TESTED YET! */
            
            /* Translate thetajoin operator. Append both projection
             lists (cross) and build join operator with selection predicates.
             Of course all selection expressions from underlying operators
             are preserved. */
            
            PFmsa_exprlist_t        *exprlist;
            PFmsa_comp_kind_t       comp_kind;
            PFalg_sel_t             curr_pred;
            PFmsa_expr_t            *left_col;
            PFmsa_expr_t            *right_col;
            PFmsa_expr_t            *pred_expr;
            
            PFmsa_exprlist_t        *prj_list2;
            PFmsa_exprlist_t        *sel_list2;
            
            /* build list with join predicates */
            exprlist = el(n->sem.thetajoin.count);
            for (i = 0; i < n->sem.thetajoin.count; i++) {
                
                curr_pred = n->sem.thetajoin.pred[i];
                
                left_col = find_expr(PRJ_LIST (L(n)),
                                     curr_pred.left);
                
                right_col = find_expr(PRJ_LIST (R(n)),
                                      curr_pred.right);
                
                /* determine adequate comparison kind */
                switch (curr_pred.comp) {
                    case alg_comp_eq:
                        comp_kind = msa_comp_equal;
                        break;
                    case alg_comp_gt:
                        comp_kind = msa_comp_gt;
                        break;
                    case alg_comp_ge:
                        comp_kind = msa_comp_gte;
                        break;
                    case alg_comp_lt:
                        comp_kind = msa_comp_lt;
                        break;
                    case alg_comp_le:
                        comp_kind = msa_comp_lte;
                        break;
                    case alg_comp_ne:
                        comp_kind = msa_comp_notequal;
                        break;
                    default:
                        PFoops (OOPS_FATAL, "unknown comparison kind (%i) found in"
                                " thetajoin predicates", curr_pred.comp);
                        break;
                }
                
                /* build expression for left_col comp right_col */
                pred_expr = PFmsa_expr_comp(left_col, right_col,
                                                               comp_kind,
                                                               PFcol_new(col_item));
                eladd(exprlist) = pred_expr;
            }
            
            op = PFmsa_op_join(OP(L(n)),
                               OP(R(n)),
                               exprlist);
            
            prj_list = elcopy(PRJ_LIST(L(n)));
            prj_list2 = elcopy(PRJ_LIST(R(n)));
            elconcat(prj_list, prj_list2);
            
            sel_list = elcopy(SEL_LIST(L(n)));
            sel_list2 = elcopy(SEL_LIST(R(n)));
            elconcat(sel_list, sel_list2);
            
        }
            break;
            
        case la_project:
        {
            /* Translate project operator. Note that actually NO
             msa project operator is built, filling the projection
             list with the columns of the la operator projection
             list suffices. Operator and selection list stay
             unchanged. */
            
            PFmsa_expr_t        *expr;
            PFmsa_expr_t        *expr_cpy;
            
            op = OP (L(n));
            prj_list = el(n->sem.proj.count);
            
            /* add only those expressions to projection 
             list which appear in proj list */
            for (i = 0; i < n->sem.proj.count; i++) {
                expr = find_expr (PRJ_LIST (L(n)), 
                                  n->sem.proj.items[i].old);
                
                /* copy expression */
                expr_cpy = PFmalloc(sizeof(PFmsa_expr_t));
                memcpy(expr_cpy, expr, sizeof(PFmsa_expr_t));
                
                /* consider possible renaming */
                expr_cpy->col = n->sem.proj.items[i].new;
                
                eladd(prj_list) = expr_cpy;
            }
            
            sel_list = SEL_LIST (L(n));
        }
            break;
            
        case la_select:
        {
            /* Translate select operator. Note that actually NO
             msa select operator is built, adding the selection
             column to the selection list suffices. Operator and 
             projection list stay unchanged. */
        
            /* FIXME: NOT TESTED YET! */
            
            PFmsa_expr_t        *expr;
            PFmsa_expr_t        *expr_cpy;
            
            op = OP (L(n));
            prj_list = PRJ_LIST (L(n));
            
            sel_list = elcopy(SEL_LIST (L(n)));
            
            /* find "selected" col */
            expr = find_expr(prj_list,
                             n->sem.select.col);
            
            /* deep copy */
            expr_cpy = deep_copy_expr(expr);
            
            /* add comparison expression to selection list */
            eladd(sel_list) = expr_cpy;
        }
            break;
            
            
        case la_disjunion:
            
            bind(L(n));
            bind(R(n));
            
            /* build operator and lists */
            op = PFmsa_op_union(OP (L(n)), OP (R(n)));
            
            prj_list = exprlist_from_schema (op->schema);
            
            sel_list = el(1);
            
            break;
            
        case la_intersect:
        {
            /* FIXME: NOT TESTED YET! */
            
            /* The intersection of two tables is equivalent to
             an eqjoin over all attributes of the tables. In
             our case, the tables have same schema */
            
            PFmsa_exprlist_t        *exprlist;
            PFmsa_expr_t            *expr;
            
            bind(L(n));
            bind(R(n));
            
            exprlist = el(n->schema.count);
            
            PFmsa_expr_t *expr2;
            for (i = 0; i < n->schema.count; i++) {
                
                expr = find_expr (PRJ_LIST(L(n)),
                                  n->schema.items[i].name);
                expr2 = find_expr (PRJ_LIST(R(n)),
                                  n->schema.items[i].name);
                
                eladd(exprlist) = PFmsa_expr_comp(expr,
                                                       expr2,
                                                       msa_comp_equal,
                                                       PFcol_new(col_item));
            }
            
            op = PFmsa_op_join(OP(L(n)),
                               OP(R(n)),
                               exprlist);
            
            prj_list = exprlist_from_schema (n->schema);
            
            sel_list = el(1);
        }
            break;
            
        case la_difference:
            
            bind(L(n));
            bind(R(n));
            
            /* build operator and lists */
            op = PFmsa_op_except(OP (L(n)), OP (R(n)));
            prj_list = exprlist_from_schema (n->schema);
            
            sel_list = el(1);
            
            break;
            
        case la_distinct:
        {
            /* FIXME: NOT TESTED YET! */
            
            /* To preserve sematics of plan safely, bind at occurence
             of distinct operator */
            
            PFmsa_op_t *sel = PFmsa_op_select (OP(L(n)), SEL_LIST(L(n)));
            PFmsa_op_t *prj = PFmsa_op_project (sel, true, PRJ_LIST(L(n)));
            
            op = prj;
            prj_list = exprlist_from_schema (n->schema);
            sel_list = el(1);
        }   
            break;
            
        case la_fun_1to1:
        {
            /* Append expression representing the function
             to projection list. Operator and selection list stay
             unchanged. */
            
            PFmsa_exprlist_t            *exprlist;
            PFmsa_expr_t                *expr;
            PFmsa_expr_func_name        func_name;
            PFalg_col_t                 curr_col;
            
            op = OP (L(n));
            prj_list = elcopy (PRJ_LIST (L(n)));
            
            /* determine function kind */
            switch (n->sem.fun_1to1.kind) {
                case alg_fun_num_add:
                    func_name = msa_func_add;
                    break;
                case alg_fun_num_subtract:
                    func_name = msa_func_sub;
                    break;
                case alg_fun_num_multiply:
                    func_name = msa_func_mult;
                    break;
                case alg_fun_num_divide:
                    func_name = msa_func_div;
                    break;
                case alg_fun_num_modulo:
                    func_name = msa_func_mod;
                    break;
                default:
                    PFoops (OOPS_FATAL, "unknown function kind (%i) found", 
                                        n->sem.fun_1to1.kind);
                    break;
            }
            
            /* build list with input columns for function */
            exprlist = el(PFalg_collist_size(n->sem.fun_1to1.refs));
            for (i = 0; i < PFalg_collist_size(n->sem.fun_1to1.refs); i++) {
                curr_col = PFalg_collist_at(n->sem.fun_1to1.refs, i);
                
                expr = find_expr(prj_list, 
                                 curr_col);
                eladd(exprlist) = expr;
            }
            
            eladd(prj_list) = PFmsa_expr_func (func_name,
                                               exprlist,
                                               n->sem.fun_1to1.res);
            sel_list = SEL_LIST (L(n));
        }
            break;
            
        case la_num_eq:
        case la_num_gt:
        {
            /* Append comparison expression representing gt/eq 
             to projection list. Operator and selection list
             stay unchanged. */
            PFmsa_comp_kind_t           comp_kind;
            PFmsa_expr_t                *col1_expr;
            PFmsa_expr_t                *col2_expr;
            
            op = OP (L(n));
            prj_list = elcopy (PRJ_LIST (L(n)));
            
            /* find left operand col1 in projection list */
            col1_expr = find_expr(prj_list, 
                                  n->sem.binary.col1);
            
            /* find left operand col2 in projection list */
            col2_expr = find_expr(prj_list, 
                                  n->sem.binary.col2);
            
            /* determine comparison kind */
            switch (n->kind) {
                case la_num_eq:
                    comp_kind = msa_comp_equal;
                    break;
                case la_num_gt:
                    comp_kind = msa_comp_gt;
                    break;
                default:
                    break;
            }
            
            /* add comparison expression to projection list */
            eladd(prj_list) = PFmsa_expr_comp (col1_expr,
                                                    col2_expr,
                                                    comp_kind,
                                                    n->sem.binary.res);
            sel_list = SEL_LIST (L(n));
        }
            break;
            
        case la_bool_and:
        case la_bool_or:
        {
            /* Append function expression representing and/or 
             to projection list. Operator and selection list
             stay unchanged. */
            
            PFmsa_exprlist_t        *exprlist;
            PFmsa_expr_t            *expr;
            
            op = OP (L(n));
            prj_list = elcopy (PRJ_LIST (L(n)));
            
            /* build list with input columns for function */
            exprlist = el(2);
            
            /* append col1 to input list for and function */
            expr = find_expr(prj_list, 
                             n->sem.binary.col1);
            eladd(exprlist) = expr;
            
            /* append col2 to input list for and function */
            expr = find_expr(prj_list, 
                             n->sem.binary.col2);
            eladd(exprlist) = expr;
            
            PFmsa_expr_func_name func_name;
            switch (n->kind) {
                case la_bool_or:
                    func_name = msa_func_or;
                    break;
                case la_bool_and:
                    func_name = msa_func_and;
                    break;
                default:
                    break;
            }
            
            eladd(prj_list) = PFmsa_expr_func (func_name,
                                               exprlist,
                                               n->sem.binary.res);
            sel_list = SEL_LIST (L(n));
        }
            break;
            
        case la_bool_not:
        {
            /* Append function expression representing 'not' 
             to projection list. Operator and selection list
             stay unchanged. */
            
            PFmsa_exprlist_t        *exprlist;
            PFmsa_expr_t            *expr;
            
            op = OP (L(n));
            prj_list = elcopy (PRJ_LIST (L(n)));
            
            /* build list with input columns for function */
            exprlist = el(1);
            
            /* append col1 to input list for and function */
            expr = find_expr(prj_list, 
                             n->sem.unary.col);
            eladd(exprlist) = expr;
            
            eladd(prj_list) = PFmsa_expr_func (msa_func_not,
                                               exprlist,
                                               n->sem.unary.res);
            sel_list = SEL_LIST (L(n));
        }
            break;
            
        case la_aggr:
        {
            /* FIXME: NOT TESTED YET! */
            
            /* Append aggregate columns and partitioning column (if exists)
               to projection list */
            
            PFmsa_expr_t            *expr;
            PFmsa_exprlist_t        *exprlist;
            PFmsa_exprlist_t        *grpbylist;
            PFalg_aggr_t            aggr;
            PFmsa_aggr_kind_t       msa_aggr_kind;
            
            exprlist = el(n->sem.aggr.count);
            grpbylist = el(1);
            
            if (n->sem.aggr.part != col_NULL) {
                /* find partitioning column and append it to projection 
                 and groupby list */
                expr = find_expr(PRJ_LIST (L(n)),
                                 n->sem.aggr.part);
                eladd(exprlist) = expr;
                eladd(grpbylist) = expr;
            }
            
            /* for every aggregation in list, append a column to prj_list */
            for (i = 0; i < n->sem.aggr.count; i++) {
                aggr = n->sem.aggr.aggr[i];
                
                expr = find_expr(PRJ_LIST (L(n)),
                                 aggr.col);
                
                /* determine aggregation kind */
                switch (aggr.kind) {
                    case alg_aggr_count:
                        msa_aggr_kind = msa_aggr_count;
                        break;
                    case alg_aggr_min:
                        msa_aggr_kind = msa_aggr_min;
                        break;
                    case alg_aggr_max:
                        msa_aggr_kind = msa_aggr_max;
                        break;
                    case alg_aggr_avg:
                        msa_aggr_kind = msa_aggr_avg;
                        break;
                    case alg_aggr_sum:
                        msa_aggr_kind = msa_aggr_sum;
                        break;
                    case alg_aggr_dist:
                        msa_aggr_kind = msa_aggr_dist;
                        break;
                    case alg_aggr_seqty1:
                        msa_aggr_kind = msa_aggr_seqty1;
                        break;
                    case alg_aggr_all:
                        msa_aggr_kind = msa_aggr_all;
                        break;
                    case alg_aggr_prod:
                        msa_aggr_kind = msa_aggr_prod;
                        break;
                    default:
                        PFoops (OOPS_FATAL, "translation of la aggregation kind %i"
                                " not implemented yet", aggr.kind);
                        break;
                }
                
                eladd(exprlist) = PFmsa_expr_aggr(expr,
                                                  msa_aggr_kind,
                                                  aggr.res);
            }
            
            op = PFmsa_op_groupby(OP (L(n)), grpbylist, exprlist);
            
            prj_list = exprlist_from_schema(n->schema);
            sel_list = SEL_LIST (L(n));
        }   
            break;
            
        case la_rownum:
        {
            /* FIXME: NOT TESTED YET */
            
            PFmsa_expr_t            *expr;
            PFmsa_exprlist_t        *exprlist;
            PFmsa_exprlist_t        *partlist;
            PFmsa_expr_t            *expr_cpy;
            PFalg_col_t             curr_col;
            bool                    curr_sortorder;
            
            op = OP (L(n));
            prj_list = elcopy(PRJ_LIST (L(n)));
            sel_list = SEL_LIST (L(n));
            
            /* copy the sort columns from semantic field into exprlist */
            exprlist = el(PFord_count (n->sem.sort.sortby));
            partlist = el(1);
            
            if (n->sem.sort.part != col_NULL) {
                /* find partitioning column and append it to partitioning 
                 list */
                expr = find_expr(PRJ_LIST (L(n)),
                                 n->sem.sort.part);
                
                expr_cpy = PFmalloc(sizeof(PFmsa_expr_t));
                memcpy(expr_cpy, expr, sizeof(PFmsa_expr_t));
                
                eladd(partlist) = expr_cpy;
            }
            
            for (i = 0; i < PFord_count (n->sem.sort.sortby); i++) {
                curr_col = PFord_order_col_at (n->sem.sort.sortby, i);
                curr_sortorder = PFord_order_dir_at(n->sem.sort.sortby, i);
                
                expr = find_expr(PRJ_LIST (L(n)),
                                 curr_col);
                
                expr_cpy = PFmalloc(sizeof(PFmsa_expr_t));
                memcpy(expr_cpy, expr, sizeof(PFmsa_expr_t));
                
                /* consider possible different sortorder */
                expr_cpy->sortorder = curr_sortorder;
                
                eladd(exprlist) = expr_cpy;
            }
            
            expr = PFmsa_expr_num_gen(msa_num_gen_rownum, exprlist, n->sem.sort.res);
            
            expr->sem.num_gen.sort_cols = elcopy(exprlist);
            expr->sem.num_gen.part_cols = elcopy(partlist);
            
            eladd(prj_list) = expr;
        }
            break;
            
        case la_rowrank:
        case la_rank:
        {
            PFmsa_exprlist_t        *exprlist;
            PFmsa_expr_t            *expr;
            PFmsa_expr_t            *expr_cpy;
            PFalg_col_t             curr_col;
            bool                    curr_sortorder;
            PFmsa_num_gen_kind_t    kind;
            
            op = OP (L(n));
            prj_list = elcopy(PRJ_LIST (L(n)));
            
            /* copy the sort columns from semantic field into exprlist */
            exprlist = el(PFord_count (n->sem.sort.sortby));
            
            for (i = 0; i < PFord_count (n->sem.sort.sortby); i++) {
                curr_col = PFord_order_col_at (n->sem.sort.sortby, i);
                curr_sortorder = PFord_order_dir_at(n->sem.sort.sortby, i);
                
                expr = find_expr(prj_list,
                                 curr_col);
                
                expr_cpy = PFmalloc(sizeof(PFmsa_expr_t));
                memcpy(expr_cpy, expr, sizeof(PFmsa_expr_t));
                
                /* consider possible different sortorder */
                expr_cpy->sortorder = curr_sortorder;
                
                eladd(exprlist) = expr_cpy;
            }
            
            switch (n->kind) {
                case la_rowrank:
                    kind = msa_num_gen_rowrank;
                    break;
                case la_rank:
                    kind = msa_num_gen_rank;
                    break;
                default:
                    break;
            }
            
            expr = PFmsa_expr_num_gen(kind, exprlist, n->sem.sort.res);
            
            expr->sem.num_gen.sort_cols = elcopy(exprlist);
            
            /* copy empty list into part_cols */
            exprlist = el(1);
            expr->sem.num_gen.part_cols = elcopy(exprlist);
            
            eladd(prj_list) = expr;
            
            sel_list = SEL_LIST (L(n));
        }
            break;
            
        case la_rowid:
        {
            /* FIXME: NOT TESTED YET! */
            
            PFmsa_exprlist_t        *exprlist;
            PFmsa_expr_t            *expr;
            
            op = OP (L(n));
            prj_list = elcopy(PRJ_LIST (L(n)));
            
            exprlist = el(1);
            expr = PFmsa_expr_num_gen(msa_num_gen_rowid, exprlist, n->sem.rowid.res);
            
            
            /* rowid does not have partitioning or sort columns -> fill in dummy values*/
            
            /* copy empty list into part_cols */
            exprlist = el(1);
            expr->sem.num_gen.part_cols = elcopy(exprlist);
            
            /* copy empty list into sort_cols */
            exprlist = el(1);
            expr->sem.num_gen.sort_cols = elcopy(exprlist);
            
            eladd(prj_list) = expr;
            
            sel_list = SEL_LIST (L(n));
        }   
            break;
            
        case la_cast:
        {
            /* Append convert expression representing the conversion
             to projection list. Operator and selection list stay
             unchanged. */
            
            PFmsa_expr_t            *org_col;
            PFmsa_expr_t            *new_col;
        
            op = OP (L(n));
            prj_list = elcopy(PRJ_LIST (L(n)));
            
            org_col = find_expr(prj_list, 
                                              n->sem.type.col);
            
            new_col = PFmsa_expr_convert (org_col, n->sem.type.ty,
                                                        n->sem.type.res);
            
            eladd(prj_list) = new_col;
            
            sel_list = SEL_LIST (L(n));
        }
            break;
        
        case la_empty_frag:
            
            /* build operator and lists */
            op = PFmsa_op_nil_node();
            prj_list = NULL;
            sel_list = NULL;
            
            break;
            
        case la_nil:
            
            /* build operator and lists */
            op = PFmsa_op_nil_node();
            prj_list = NULL;
            sel_list = NULL;
            
            break;
            
        default:
            PFoops (OOPS_FATAL, "translation of la operator kind %i not"
                                " not implemented yet", n->kind);
            break;
    }
    
    /* build annotation */
    msa_ann = PFmalloc(sizeof(PFmsa_ann_t));
    msa_ann->op = op;
    msa_ann->prj_list = prj_list;
    msa_ann->sel_list = sel_list;
    
    /* set annotation */
    n->msa_ann = msa_ann;
    
    /* mark node vistited */
    n->bit_dag = true;
}

/* Translate the logical algebra into M5 SQL algebra */
PFmsa_op_t *
PFlalg2msa (PFla_op_t * n)
{
    /* start worker to fill msa_ann fields in n */
    PFla_dag_reset(n);
    alg2msa_worker(n);
    PFla_dag_reset(n);
    
    return OP(n);
}

/* vim:set shiftwidth=4 expandtab filetype=c: */

