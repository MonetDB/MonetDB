/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on multiple properties.
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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
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

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
#define LL(p) (L(L(p)))
#define RL(p) (L(R(p)))
#define LLL(p) (LL(L(p)))
#define LLR(p) (R(LL(p)))

#define SEEN(p) ((p)->bit_dag)

/* worker for PFalgopt_complex */
static void
opt_complex (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply complex optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_complex (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_attach:
            /**
             * if an attach column is the only required column
             * and we know its exact cardinality we can replace
             * the complete subtree by a literal table.
             */
            if (PFprop_icols_count (p->prop) == 1 &&
                PFprop_icol (p->prop, p->sem.attach.attname) &&
                PFprop_card (p->prop) >= 1) {
                
                PFla_op_t *res;
                unsigned int count = PFprop_card (p->prop);
                /* create projection list to avoid missing attributes */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));

                /* create list of tuples each containing a list of atoms */
                PFalg_tuple_t *tuples = PFmalloc (count *
                                                  sizeof (*(tuples)));;
                                                  
                for (unsigned int i = 0; i < count; i++) {
                    tuples[i].atoms = PFmalloc (sizeof (*(tuples[i].atoms)));
                    tuples[i].atoms[0] = p->sem.attach.value;
                    tuples[i].count = 1;
                }

                res = PFla_lit_tbl_ (PFalg_attlist (p->sem.attach.attname),
                                     count, tuples);

                /* Every column of the relation will point
                   to the attach argument to avoid missing
                   references. (Columns that are not required
                   may be still referenced by the following
                   operators.) */
                for (unsigned int i = 0; i < p->schema.count; i++)
                    proj[i] = PFalg_proj (p->schema.items[i].name,
                                          p->sem.attach.attname);
                                          
                *p = *PFla_project_ (res, p->schema.count, proj);
            }
            /* prune unnecessary attach-project operators */
            if (L(p)->kind == la_project &&
                L(p)->schema.count == 1 &&
                LL(p)->kind == la_scjoin &&
                p->sem.attach.attname == LL(p)->sem.scjoin.iter &&
                PFprop_const (LL(p)->prop, LL(p)->sem.scjoin.iter) &&
                PFalg_atom_comparable (
                    p->sem.attach.value,
                    PFprop_const_val (LL(p)->prop, LL(p)->sem.scjoin.iter)) &&
                !PFalg_atom_cmp (
                    p->sem.attach.value,
                    PFprop_const_val (LL(p)->prop, LL(p)->sem.scjoin.iter)) &&
                L(p)->sem.proj.items[0].new == LL(p)->sem.scjoin.item_res) {
                *p = *PFla_dummy (LL(p));
                break;
            }
            /* prune unnecessary attach-project operators */
            if (L(p)->kind == la_project &&
                L(p)->schema.count == 1 &&
                LL(p)->kind == la_scjoin &&
                PFprop_const (LL(p)->prop, LL(p)->sem.scjoin.iter) &&
                PFalg_atom_comparable (
                    p->sem.attach.value,
                    PFprop_const_val (LL(p)->prop, LL(p)->sem.scjoin.iter)) &&
                !PFalg_atom_cmp (
                    p->sem.attach.value,
                    PFprop_const_val (LL(p)->prop, LL(p)->sem.scjoin.iter)) &&
                L(p)->sem.proj.items[0].old == LL(p)->sem.scjoin.item_res) {
                *p = *PFla_project (PFla_dummy (LL(p)),
                                    PFalg_proj (p->sem.attach.attname,
                                                LL(p)->sem.scjoin.iter),
                                    L(p)->sem.proj.items[0]);
                break;
            }
            /* prune unnecessary attach-project operators */
            if (L(p)->kind == la_project &&
                L(p)->schema.count == 1 &&
                LL(p)->kind == la_roots &&
                LLL(p)->kind == la_doc_tbl &&
                p->sem.attach.attname == LLL(p)->sem.doc_tbl.iter &&
                PFprop_const (LLL(p)->prop, LLL(p)->sem.doc_tbl.iter) &&
                PFalg_atom_comparable (
                    p->sem.attach.value,
                    PFprop_const_val (LLL(p)->prop,
                                      LLL(p)->sem.doc_tbl.iter)) &&
                !PFalg_atom_cmp (
                    p->sem.attach.value,
                    PFprop_const_val (LLL(p)->prop, 
                                      LLL(p)->sem.doc_tbl.iter)) &&
                L(p)->sem.proj.items[0].new == LLL(p)->sem.doc_tbl.item_res) {
                *p = *PFla_dummy (LL(p));
                break;
            }

            break;
            
        case la_eqjoin:
            /**
             * if we have a key join (key property) on a 
             * domain-subdomain relationship (domain property)
             * where the columns of the argument marked as 'domain'
             * are not required (icol property) we can skip the join
             * completely.
             */
        {
            /* we can use the schema information of the children
               as no rewrite adds more columns to that subtree. */
            bool left_arg_req = false;
            bool right_arg_req = false;

            /* discard join attributes as one of them always remains */
            for (unsigned int i = 0; i < L(p)->schema.count; i++) {
                left_arg_req = left_arg_req ||
                               (PFprop_unq_name (
                                    L(p)->prop, 
                                    L(p)->schema.items[i].name) !=
                                PFprop_unq_name (
                                    p->prop, 
                                    p->sem.eqjoin.att1) &&
                                PFprop_icol (
                                   p->prop, 
                                   L(p)->schema.items[i].name));
            }
            if (PFprop_key_left (p->prop, p->sem.eqjoin.att1) &&
                PFprop_subdom (p->prop, 
                               PFprop_dom_right (p->prop,
                                                 p->sem.eqjoin.att2),
                               PFprop_dom_left (p->prop,
                                                p->sem.eqjoin.att1)) &&
                !left_arg_req) {
                /* Every column of the left argument will point
                   to the join argument of the right argument to
                   avoid missing references. (Columns that are not
                   required may be still referenced by the following
                   operators.) */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                unsigned int count = 0;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        p->sem.eqjoin.att2);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        R(p)->schema.items[i].name);

                *p = *PFla_project_ (R(p), count, proj);
                break;
            }
            
            /* discard join attributes as one of them always remains */
            for (unsigned int i = 0; i < R(p)->schema.count; i++) {
                right_arg_req = right_arg_req ||
                                (PFprop_unq_name (
                                     R(p)->prop,
                                     R(p)->schema.items[i].name) !=
                                 PFprop_unq_name (
                                     p->prop,
                                     p->sem.eqjoin.att2) &&
                                 PFprop_icol (
                                     p->prop, 
                                     R(p)->schema.items[i].name));
            }
            if (PFprop_key_right (p->prop, p->sem.eqjoin.att2) &&
                PFprop_subdom (p->prop, 
                               PFprop_dom_left (p->prop,
                                                p->sem.eqjoin.att1),
                               PFprop_dom_right (p->prop,
                                                 p->sem.eqjoin.att2)) &&
                !right_arg_req) {
                /* Every column of the right argument will point
                   to the join argument of the left argument to
                   avoid missing references. (Columns that are not
                   required may be still referenced by the following
                   operators.) */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                unsigned int count = 0;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        L(p)->schema.items[i].name);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        p->sem.eqjoin.att1);

                *p = *PFla_project_ (L(p), count, proj);
                break;
            }

            /* introduce semi-join operator if possible */
            if (!left_arg_req &&
                (PFprop_key_left (p->prop, p->sem.eqjoin.att1) ||
                 PFprop_set (p->prop))) {
                /* Every column of the left argument will point
                   to the join argument of the right argument to
                   avoid missing references. (Columns that are not
                   required may be still referenced by the following
                   operators.) */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                unsigned int count = 0;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        p->sem.eqjoin.att2);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        R(p)->schema.items[i].name);

                *p = *PFla_project_ (
                          PFla_semijoin (
                              R(p),
                              L(p),
                              p->sem.eqjoin.att2,
                              p->sem.eqjoin.att1),
                          count,
                          proj);
                break;
            }

            /* introduce semi-join operator if possible */
            if (!right_arg_req &&
                (PFprop_key_right (p->prop, p->sem.eqjoin.att2) ||
                 PFprop_set (p->prop))) {
                /* Every column of the right argument will point
                   to the join argument of the left argument to
                   avoid missing references. (Columns that are not
                   required may be still referenced by the following
                   operators.) */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                unsigned int count = 0;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        L(p)->schema.items[i].name);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        p->sem.eqjoin.att1);

                *p = *PFla_project_ (
                          PFla_semijoin (
                              L(p),
                              R(p),
                              p->sem.eqjoin.att1,
                              p->sem.eqjoin.att2),
                          count,
                          proj);
                break;
            }
        }   break;
            
        case la_semijoin:
            if (!PFprop_key_left (p->prop, p->sem.eqjoin.att1) ||
                !PFprop_subdom (p->prop, 
                                PFprop_dom_right (p->prop,
                                                  p->sem.eqjoin.att2),
                                PFprop_dom_left (p->prop,
                                                 p->sem.eqjoin.att1)))
                break;
            
            /* remove the distinct operator and redirect the
               references to the semijoin operator */
            if (R(p)->kind == la_distinct) {
                PFla_op_t *distinct = R(p);
                R(p) = L(distinct);
                *distinct = *PFla_project (p, PFalg_proj (p->sem.eqjoin.att2,
                                                          p->sem.eqjoin.att1));
            }
            else if (R(p)->kind == la_project &&
                     RL(p)->kind == la_distinct &&
                     R(p)->schema.count == 1 &&
                     RL(p)->schema.count == 1) {
                PFla_op_t *project = R(p),
                          *distinct = RL(p);
                R(p) = L(distinct);
                *project = *PFla_project (
                                p,
                                PFalg_proj (p->sem.eqjoin.att2,
                                            p->sem.eqjoin.att1));
                *distinct = *PFla_project (
                                 p,
                                 PFalg_proj (distinct->schema.items[0].name,
                                             p->sem.eqjoin.att1));

                /* we need to adjust the semijoin argument as well */
                p->sem.eqjoin.att2 = R(p)->schema.items[0].name;
            }
            break;

        case la_cross:
            /* PFprop_icols_count () == 0 is also true 
               for nodes without inferred properties 
               (newly created nodes). The cardinality
               constraint however ensures that the 
               properties are available. */
            if (PFprop_card (L(p)->prop) == 1 &&
                PFprop_icols_count (L(p)->prop) == 0) {
                *p = *PFla_dummy (R(p));
                break;
            }
            if (PFprop_card (R(p)->prop) == 1 &&
                PFprop_icols_count (R(p)->prop) == 0) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;
            
        case la_select:
        /**
         * Rewrite the pattern (1) into expression (2):
         *
         *          select_(att1) [icols:att2]          pi_(att2,...:att2)
         *            |                                  | 
         *         ( pi_(att1,att2) )                 distinct
         *            |                                  |
         *           or_(att1:att3,att4)               union
         *            |                              ____/\____
         *         ( pi_(att2,att3,att4) )          /          \
         *            |                            pi_(att2)   pi_(att2:att5)
         *           |X|_(att2,att5)              /              \
         *        __/   \__                    select_(att3)   select_(att4)
         *       /         \                     |                |
         *      /1\       /2\                   /1\              /2\
         *     /___\     /___\                 /___\            /___\
         * (att2,att3) (att5,att4)            (att2,att3)      (att5,att4)
         *                                                                        
         *           (1)                                 (2)
         */
        {
            unsigned int i;
            PFalg_att_t att_sel, 
                        att_join1, att_join2, 
                        att_sel_in1, att_sel_in2;
            PFla_op_t *cur, *left, *right;
            PFalg_proj_t *lproj, *rproj, *top_proj;

            if (p->schema.count != 2 ||
                PFprop_icols_count (p->prop) != 1 ||
                PFprop_icol (p->prop, p->sem.select.att))
                break;

            att_sel = p->sem.select.att;
            att_join1 = p->schema.items[0].name != att_sel
                        ? p->schema.items[0].name
                        : p->schema.items[1].name;
            cur = L(p);
            
            /* cope with intermediate projections */
            if (cur->kind == la_project) {
                for (i = 0; i < cur->sem.proj.count; i++)
                    if (L(p)->sem.proj.items[i].new == att_sel)
                        att_sel = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == att_join1)
                        att_join1 = L(p)->sem.proj.items[i].old;
                cur = L(cur);
            }
            
            if (cur->kind != la_bool_or ||
                att_sel != cur->sem.binary.res)
                break;
            
            att_sel_in1 = cur->sem.binary.att1;
            att_sel_in2 = cur->sem.binary.att2;
            
            cur = L(cur);
            
            /* cope with intermediate projections */
            if (cur->kind == la_project) {
                for (i = 0; i < cur->sem.proj.count; i++)
                    if (L(p)->sem.proj.items[i].new == att_join1)
                        att_join1 = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == att_sel_in1)
                        att_sel_in1 = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == att_sel_in2)
                        att_sel_in2 = L(p)->sem.proj.items[i].old;
                cur = L(cur);
            }
            
            if (cur->kind != la_eqjoin ||
                (att_join1 != cur->sem.eqjoin.att1 &&
                 att_join1 != cur->sem.eqjoin.att2))
                break;
           
            if (PFprop_ocol (L(cur), att_sel_in1) &&
                PFprop_ocol (R(cur), att_sel_in2)) {
                att_join1 = cur->sem.eqjoin.att1;
                att_join2 = cur->sem.eqjoin.att2;
                left = L(cur);
                right = R(cur);
            }
            else if (PFprop_ocol (L(cur), att_sel_in2) &&
                    PFprop_ocol (R(cur), att_sel_in1)) {
                att_join1 = cur->sem.eqjoin.att2;
                att_join2 = cur->sem.eqjoin.att1;
                left = R(cur);
                right = L(cur);
            }
            else
                break;
            
            /* pattern (1) is now ensured: create pattern (2) */
            lproj = PFmalloc (sizeof (PFalg_proj_t));
            rproj = PFmalloc (sizeof (PFalg_proj_t));
            top_proj = PFmalloc (2 * sizeof (PFalg_proj_t));

            lproj[0] = PFalg_proj (att_join1, att_join1);
            rproj[0] = PFalg_proj (att_join1, att_join2);
            top_proj[0] = PFalg_proj (p->schema.items[0].name, att_join1);
            top_proj[1] = PFalg_proj (p->schema.items[1].name, att_join1);

            *p = *PFla_project_ (
                      PFla_distinct (
                          PFla_disjunion (
                              PFla_project_ (
                                  PFla_select (left, att_sel_in1), 
                                  1, lproj),
                              PFla_project_ (
                                  PFla_select (right, att_sel_in2),
                                  1, rproj))),
                      2, top_proj);
        }   break;
        
        case la_rownum:
            /* match the pattern rownum - (project -) rownum and
               try to merge both row number operators if the nested
               one only prepares some columns for the outer rownum.
                 As most operators are separated by a projection
               we also support projections that do not rename. */
        {
            PFla_op_t *rownum;
            bool proj = false, renamed = false;
            unsigned int i;
            
            /* check for a projection */
            if (L(p)->kind == la_project) {
                proj = true;
                for (i = 0; i < L(p)->sem.proj.count; i++)
                    renamed = renamed || (L(p)->sem.proj.items[i].new !=
                                          L(p)->sem.proj.items[i].old);
                rownum = LL(p);
            }
            else
                rownum = L(p);
            
            /* don't handle patterns with renaming projections */
            if (renamed) break;

            /* check the remaining part of the pattern (nested rownum)
               and ensure that the column generated by the nested
               row number operator is not used above the outer rownum. */
            if (rownum->kind == la_rownum &&
                !PFprop_icol (p->prop, rownum->sem.rownum.attname)) {
                
                PFalg_attlist_t sortby;
                PFalg_proj_t *proj_list;
                PFalg_att_t inner_att = rownum->sem.rownum.attname;
                PFalg_att_t inner_part = rownum->sem.rownum.part;
                unsigned int pos_part = 0, pos_att = 0, count = 0;

                /* if the inner rownum has a partitioning column
                   this column has to occur in the outer rownum as
                   partitioning attribute or as a sort criterion
                   preceding the new column generated by the nested
                   rownum. */
                if (inner_part) {
                    /* get the position of the inner partition attribute
                       in the sort criteria of the outer rownum */
                    for (i = 0; i < p->sem.rownum.sortby.count; i++)
                        if (p->sem.rownum.sortby.atts[i] == inner_part) {
                            pos_part = i;
                            break;
                        }
                    /* don't handle patterns where the inner partition
                       column does not occur in the outer rownum */
                    if (i == p->sem.rownum.sortby.count)
                        if (!p->sem.rownum.part ||
                            p->sem.rownum.part != inner_part)
                            break;
                }
                
                /* lookup position of the inner rownum column in
                   the list of sort criteria of the outer rownum */
                for (i = 0; i < p->sem.rownum.sortby.count; i++)
                    if (p->sem.rownum.sortby.atts[i] == inner_att) {
                        pos_att = i;
                        break;
                    }
                    
                /* inner rownum column is not used in the outer rownum
                   (thus the inner rownum is probably superfluous
                    -- let the icols optimization remove the operator)
                   or the inner partition column does not occur as
                   sort criterion before the inner rownum column */
                if (i == p->sem.rownum.sortby.count ||
                    pos_part > pos_att)
                    break;
                
                sortby.count = p->sem.rownum.sortby.count + 
                               rownum->sem.rownum.sortby.count - 1;
                sortby.atts = PFmalloc (sortby.count *
                                        sizeof (PFalg_attlist_t));

                /* create new sort list where the sort criteria of the
                   inner rownum substitute the inner rownum column */
                for (i = 0; i < pos_att; i++)
                    sortby.atts[count++] = p->sem.rownum.sortby.atts[i];
                    
                for (i = 0; i < rownum->sem.rownum.sortby.count; i++)
                    sortby.atts[count++] = rownum->sem.rownum.sortby.atts[i];

                for (i = pos_att + 1; i < p->sem.rownum.sortby.count; i++)
                    sortby.atts[count++] = p->sem.rownum.sortby.atts[i];

                assert (count == sortby.count);
                    
                if (proj) {
                    /* Introduce the projection above the new rownum
                       operator to maintain the correct result schema.
                       As the result column name of the old outer rownum
                       may collide with the attribute name of one of the
                       inner rownums sort criteria, we use the column name
                       of the inner rownum as resulting attribute name
                       and adjust the name in the new projection. */
                       
                    count = 0;

                    /* create projection list */
                    proj_list = PFmalloc (p->schema.count *
                                          sizeof (*(proj_list)));
                                          
                    /* adjust column name of the rownum operator */
                    proj_list[count++] = PFalg_proj (
                                             p->sem.rownum.attname,
                                             rownum->sem.rownum.attname);
                                             
                    for (i = 0; i < p->schema.count; i++)
                        if (p->schema.items[i].name != 
                            p->sem.rownum.attname)
                            proj_list[count++] = PFalg_proj (
                                                     p->schema.items[i].name,
                                                     p->schema.items[i].name);
                                                   
                    *p = *PFla_project_ (PFla_rownum (L(rownum),
                                                      rownum->sem.rownum.attname,
                                                      sortby,
                                                      p->sem.rownum.part),
                                         count, proj_list);
                }
                else
                    *p = *PFla_rownum (rownum,
                                       p->sem.rownum.attname,
                                       sortby,
                                       p->sem.rownum.part);

                break;
            }

            /* check a pattern that occurs very often 
               ('doc_access(.../text())') in the generated
               code and remove the unnecessary rownum operator */
            if (L(p)->kind == la_project &&
                LL(p)->kind == la_doc_access &&
                LLR(p)->kind == la_rownum &&
                p->sem.rownum.sortby.count == 1 &&
                p->sem.rownum.part &&
                LLR(p)->sem.rownum.sortby.count == 1 &&
                LLR(p)->sem.rownum.part) {
                unsigned int  i;
                PFalg_att_t   pos, iter, cur;
                PFalg_proj_t *proj;

                pos   = LLR(p)->sem.rownum.attname;
                iter  = LLR(p)->sem.rownum.part;

                for (i = 0; i < L(p)->sem.proj.count; i++)
                    if (L(p)->sem.proj.items[i].old == iter) {
                        iter = L(p)->sem.proj.items[i].new;
                        break;
                    }
                for (i = 0; i < L(p)->sem.proj.count; i++)
                    if (L(p)->sem.proj.items[i].old == pos) {
                        pos = L(p)->sem.proj.items[i].new;
                        break;
                    }
                if (pos == iter ||
                    pos != p->sem.rownum.sortby.atts[0] ||
                    iter != p->sem.rownum.part)
                    break;
                
                /* We have now checked that the above rownumber does
                   nothing new so replace it by a projection. */
                proj = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));
                for (i = 0; i < p->schema.count; i++) {
                    cur = p->schema.items[i].name;
                    if (cur == p->sem.rownum.attname)
                        /* replace the new column by the already
                           existing sort criterion */
                        proj[i] = PFalg_proj (cur, pos);
                    else
                        proj[i] = PFalg_proj (cur, cur);
                }
                *p = *PFla_project_ (L(p), p->schema.count, proj);
                break;                
            }
        }   break;

        case la_scjoin:
            if (R(p)->kind == la_project &&
                RL(p)->kind == la_scjoin) {
                if ((p->sem.scjoin.item == 
                     R(p)->sem.proj.items[0].new &&
                     RL(p)->sem.scjoin.item_res == 
                     R(p)->sem.proj.items[0].old &&
                     p->sem.scjoin.iter ==
                     R(p)->sem.proj.items[1].new &&
                     RL(p)->sem.scjoin.iter == 
                     R(p)->sem.proj.items[1].old) ||
                    (p->sem.scjoin.item == 
                     R(p)->sem.proj.items[1].new &&
                     RL(p)->sem.scjoin.item_res == 
                     R(p)->sem.proj.items[1].old &&
                     p->sem.scjoin.iter ==
                     R(p)->sem.proj.items[0].new &&
                     RL(p)->sem.scjoin.iter == 
                     R(p)->sem.proj.items[0].old))
                    *p = *PFla_project (PFla_scjoin (
                                            L(p),
                                            RL(p),
                                            p->sem.scjoin.axis,
                                            p->sem.scjoin.ty,
                                            RL(p)->sem.scjoin.iter,
                                            RL(p)->sem.scjoin.item,
                                            RL(p)->sem.scjoin.item_res),
                                        R(p)->sem.proj.items[0],
                                        R(p)->sem.proj.items[1]);
            }
        default:
            break;
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_complex (PFla_op_t *root)
{
    /* Infer key, icols, domain, and unique names
       properties first */
    PFprop_infer_key (root);
    PFprop_infer_icol (root);
    /* key property inference already requires 
       the domain property inference. Thus we can
       skip it:
    PFprop_infer_dom (root);
    */
    PFprop_infer_set (root);
    PFprop_infer_unq_names (root);

    /* Optimize algebra tree */
    opt_complex (root);
    PFla_dag_reset (root);

    /* In addition optimize the resulting DAG using the icols property 
       to remove inconsistencies introduced by changing the types 
       of unreferenced columns (rule eqjoin). The icols optimization
       will ensure that these columns are 'really' never used. */
    root = PFalgopt_icol (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
