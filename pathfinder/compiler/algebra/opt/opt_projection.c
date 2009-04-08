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

/* changes the column name back to the old
 * when swaping an an operator and a projection, it is sometimes 
 * necsesary to rename colums for the operator, because the 
 * projection can renames colums 
 * this method renames single colums */ 
static PFalg_col_t
re_rename_col (PFalg_col_t col, PFalg_proj_t *proj, unsigned int count) 
{
    for(unsigned int i = 0; i < count; i++ )
        /* check only if current column in projectio has been renamed */
        if (proj[i].new != proj[i].old && col == proj[i].new) 
            return proj[i].old;
            
    return col;
}

/* changes the column name back to the old
 * when swaping an an operator and a projection, it is sometimes 
 * necsesary to rename colums for the operator, because the 
 * projection can renames colums 
 * this method renames multiple colums in column lists */ 
static PFalg_collist_t
*re_rename_col_in_collist (PFalg_collist_t *collist, PFalg_proj_t *proj, 
                          unsigned int count) 
{
    PFalg_collist_t *cl = PFalg_collist_copy (collist);
    
    /* check for each column in the columnlist */
    for(unsigned int i = 0; i < count; i++)
        for (unsigned int j = 0; j < clsize(cl); j++)
            /* check only if current column in projectio has been renamed */
            if(proj[i].new != proj[i].old && clat(cl, j) == proj[i].new) 
                clat(cl, j) = proj[i].old;
    
    return cl;
}

/* changes the column name back to the old
 * when swaping an an operator and a projection, it is sometimes 
 * necsesary to rename colums for the operator, because the 
 * projection can renames colums 
 * this method renames multiple colums in ordering arrays */ 
static PFord_ordering_t
re_rename_col_in_ordering (PFord_ordering_t ord, PFalg_proj_t *proj, 
                          unsigned int count) 
{
    /* allocation new predicate array */
    PFord_ordering_t order = PFarray_copy(ord);
    
    /* check for each column in the ordering */
    for(unsigned int i = 0; i < count; i++)
        for (unsigned int j = 0; j < PFord_count(ord); j++)
            /* check only if current column in projectio has been renamed */
            if (proj[i].new != proj[i].old 
                && PFord_order_col_at (ord, j) == proj[i].new) 
            {
                 PFord_set_order_col_at (order, j, proj[i].old);
            }
                 
    return order;
}

/* renames columns in predicates back to the old name 
 * when swaping an a join and a projection, it is sometimes 
 * necsesary to rename the predicate columns, because the 
 * projection might have renamed them 
 * this method renames multiple colums in join predicate lists */
static PFalg_sel_t
*re_rename_col_in_pred (PFalg_sel_t *pred, 
                    PFalg_proj_t *left_proj, 
                    PFalg_proj_t *right_proj, 
                    unsigned int pred_count, 
                    unsigned int left_proj_count,
                    unsigned int right_proj_count,
                    bool left,
                    bool right) 
{
    /* allocation new predicate array */
    PFalg_sel_t *predicates = PFmalloc (pred_count * sizeof (PFalg_sel_t));

    for(unsigned int i = 0; i < pred_count; i++) {
        /* duplicate predicate */
        predicates[i] = pred[i];
        
        /* left predicate check */
        if(left) 
            for (unsigned int j = 0; j < left_proj_count; j++)
                /* check if the new name is used */
                if(pred[i].left == left_proj[j].new) {
                    /* change colume name to the old name */
                    predicates[i].left = left_proj[j].old;
                    break;
                }
                    
        /* right predicate check */
        if(right)
            for (unsigned int j = 0; j < right_proj_count; j++)
                /* check if the new name is used */
                if(pred[i].right == right_proj[j].new) {
                    /* change colume name to the old name */
                    predicates[i].right = right_proj[j].old;
                    break;
                }

    }
    
    return predicates;
}

/* check if a colum name has a unique name, if not, make a new unique name */
static PFalg_col_t
make_col_unq (PFalg_col_t col) 
{
    if (!PFcol_is_name_unq(col))
        return PFcol_new(col);
    return col;
}

/* add new colums to an existing projection
 * this is necessary when a projection moves over an join. 
 * if a projection of the left join subtree is moved over 
 * the join, the schema of the right subtree must be added
 * tho the projection 
 */
static PFalg_proj_t * 
extend_proj (PFalg_proj_t *proj, unsigned int count, 
                  PFalg_schema_t schema) 
{
    /* allocate new projection array */
    PFalg_proj_t *ex_proj = PFmalloc ((count + schema.count) * 
                                                        sizeof (PFalg_proj_t));
    
    /* copying old projections */
    unsigned int i;
    for (i = 0; i < count; i++)
        ex_proj[i] = proj[i];
        
    /* adding new projections */
    for (unsigned int j = 0; j < schema.count; j++, i++) {
        ex_proj[i].old = schema.items[j].name;
        ex_proj[i].new = schema.items[j].name;
    }
    
    return ex_proj;
}

/* merging two projections with different schemas
 * this is necessary when a projection moves over an join. 
 * if a projection of the left join subtree is moved over 
 * the join and one of the right subtree, the schemas both
 * projections must be merged.
 */
static PFalg_proj_t * 
mergeProjection (PFalg_proj_t *left_proj,  unsigned int left_count, 
                 PFalg_proj_t *right_proj, unsigned int right_count)
{
    /* allocate new projection array */
    PFalg_proj_t *ex_proj = PFmalloc ((left_count + right_count) * 
                                                        sizeof (PFalg_proj_t));
    
    /* copying left projections */
    unsigned int i;
    for (i = 0; i < left_count; i++)
        ex_proj[i] = left_proj[i];
    
    /* adding right projections */
    for (unsigned int j = 0; j < right_count; j++, i++) {
        ex_proj[i] = right_proj[j];
    }
    
    return ex_proj;
}

/* worker for PFalgopt_projection */
/* this optimization pushes projection up in the tree to minimize them. 
 * the goal is to simplify the tree for other optimizations
 */
static void
opt_projection (PFla_op_t *p) 
{
    assert(p);
    
    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_projection (p->child[i]);
    
    /* action code */
    
    /* swap projection and current roots operator p followed by a doc_tbl op
     *
     *      |                  |
     *   roots (p)          project
     *      |                  |
     *   doc_tbl    -->     roots (p)
     *      |                  |
     *   project            doc_tbl
     *      |                  |
     * 
     */
    if(p->kind == la_roots
        && L(p)->kind == la_doc_tbl
        && LL(p)->kind == la_project)
    {
        /* add result column to projection 
         * create new projection list */
        unsigned int count = LL(p)->schema.count;
        PFalg_proj_t *proj = PFmalloc ((count + 1) * sizeof (PFalg_proj_t));
                           
        /* copy projections */
        for (unsigned int i = 0; i < count; i++)
            proj[i] = LL(p)->sem.proj.items[i];
                    
        /* make column name unique */
        PFalg_col_t res = make_col_unq(L(p)->sem.doc_tbl.res);
                    
        /* add result column to projection */
        proj[count] = PFalg_proj (res, L(p)->sem.doc_tbl.res);
    
        *p = *PFla_project_ (
                PFla_roots ( 
                    PFla_doc_tbl (
                        LLL(p),
                        L(p)->sem.doc_tbl.res, 
                        re_rename_col (L(p)->sem.doc_tbl.col, 
                            LL(p)->sem.proj.items,
                            LL(p)->schema.count),
                        L(p)->sem.doc_tbl.kind)),
                (count+1),
                proj);
    }
    
    /* generally:    
     * swap an operator p followed by an projection
     *    
     *      |                    |
     *     (p)                project
     *      |          -->       |
     *   project                (p)
     *      |                    |    
     */
    if ((L(p) && L(p)->kind == la_project) ||
        (R(p) && R(p)->kind == la_project))
    {
    
        switch (p->kind) {
        
            /* is a projection followed by a projection, then merge them
             *
             *      |                 
             *   project (p)           |
             *      |        -->    project (p)
             *   project               |
             *      |                 
             */
            case la_project:
                /* merge adjacent projection operators */
                *p = *PFla_project_ (
                            LL(p),
                            p->schema.count,
                            PFalg_proj_merge (
                                p->sem.proj.items,
                                p->sem.proj.count,
                                L(p)->sem.proj.items,
                                L(p)->sem.proj.count)); 
                break;
                
            /* swap projection and current select p
             *
             *      |                  |
             *   select (p)         project
             *      |        -->       |
             *   project            select (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */
            case la_select:
                *p = *PFla_project_ (
                        PFla_select (
                            LL(p), 
                            re_rename_col (p->sem.select.col, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        L(p)->schema.count,
                        L(p)->sem.proj.items);
                break;
                
            /* swap projection and current operatior p
             *
             *      |                  |
             *   attach (p)         project
             *      |        -->       |
             *   project            attach (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */
             case la_attach: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.attach.res);
                    
                /* add rowrank column to projection */
                proj[count] = PFalg_proj (res, p->sem.attach.res);

                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_attach (
                            LL(p), 
                            p->sem.attach.res,
                            p->sem.attach.value),
                        (count+1),
                        proj);
                break;
            }
            
            /* swap projection and current operatior p
             *
             *      |                  |
             *   pos_sel (p)         project
             *      |        -->       |
             *   project            pos_sel (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */
            case la_pos_select:
                *p = *PFla_project_ (
                        PFla_pos_select (
                            LL(p), 
                            p->sem.pos_sel.pos,
                            re_rename_col_in_ordering (p->sem.pos_sel.sortby,
                                                       L(p)->sem.proj.items,
                                                       L(p)->sem.proj.count),
                            re_rename_col (p->sem.pos_sel.part, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        L(p)->schema.count,
                        L(p)->sem.proj.items);
                break;
                
            /* swap projection and current operatior 
             *
             *      |                  |
             *    to (p)            project
             *      |        -->       |
             *   project             to (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */    
            case la_to: {
                /* add rowrank column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.binary.res);
                    
                /* add rowrank column to projection */
                proj[count] = PFalg_proj (res, p->sem.binary.res);
            
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_to (
                            LL(p), 
                            p->sem.binary.res, 
                            re_rename_col (p->sem.binary.col1, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.binary.col2, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),

                        (count+1),
                        proj);   
                break;
            }
            
            /* swap projection and current operatior p
             *
             *      |                  |
             *   rowrank (p)        project
             *      |        -->       |
             *   project            rowrank (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             *
             * the new resulting column from operator p is added to 
             * the projection 
             */
            case la_rowrank:  {
            
                /* add rowrank column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.sort.res);
                    
                /* add rowrank column to projection */
                proj[count] = PFalg_proj (res, p->sem.sort.res);
            
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_rowrank (
                            LL(p), 
                            p->sem.sort.res, 
                            re_rename_col_in_ordering (p->sem.sort.sortby,
                                                       L(p)->sem.proj.items,
                                                       L(p)->sem.proj.count)),
                        (count + 1),
                        proj);
                break; 
            } 
                
            /* swap projection and current operatior p
             *
             *      |                  |
             *   rownum (p)         project
             *      |        -->       |
             *   project            rownum (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             *
             * the new resulting column from operator p is added to 
             * the projection 
             */
            case la_rownum: {
                /* add rownum column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.sort.res);
                    
                /* add rownum column to projection */
                proj[count] = PFalg_proj (res, p->sem.sort.res);

                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_rownum (
                            LL(p), 
                            p->sem.sort.res,
                            re_rename_col_in_ordering (p->sem.sort.sortby,
                                                       L(p)->sem.proj.items,
                                                       L(p)->sem.proj.count),
                            re_rename_col (p->sem.sort.part, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        (count+1),
                        proj);
                break; 
            }
            
            /* swap projection and current operatior p
             *
             *      |                  |
             *   rank (p)           project
             *      |        -->       |
             *   project            rank (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             *
             * the new resulting column from operator p is added to 
             * the projection 
             */
            case la_rank:
            {
                /* add rank column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.sort.res);
                    
                /* add rank column to projection */
                proj[count] = PFalg_proj (res, p->sem.sort.res);
            
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_rank (
                            LL(p), 
                            p->sem.sort.res, 
                            re_rename_col_in_ordering (p->sem.sort.sortby,
                                                       L(p)->sem.proj.items,
                                                       L(p)->sem.proj.count)),
                        (count + 1),
                        proj); 
                break; 
            } 
            
            /* swap projection and current operatior p
             *
             *      |                  |
             *   rowid (p)          project
             *      |        -->       |
             *   project            rowid (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             *
             * the new resulting column from operator p is added to 
             * the projection 
             */
            case la_rowid: {
                /* add rowid column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.rowid.res);
                    
                /* add rowid column to projection */
                proj[count] = PFalg_proj (res, p->sem.rowid.res);
            
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_rowid (
                            LL(p), 
                            p->sem.rowid.res),
                        (count+1),
                        proj);
                break; 
            }
            
            /* swap projection and current operatior p
             *
             *      |                  |
             *   type (p)           project
             *      |        -->       |
             *   project            type (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */    
            case la_type: {
                /* add resulting column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.type.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.type.res);
                
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_type (
                            LL(p), 
                            p->sem.type.res,
                            re_rename_col (p->sem.type.col, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            p->sem.type.ty),
                        (count+1),
                        proj);
                break;  
            }
              
            /* swap projection and current operatior p
             *
             *      |                  |
             *   cast (p)           project
             *      |        -->       |
             *   project            cast (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */    
            case la_cast: {
                /* add resulting column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.type.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.type.res);
            
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_cast (
                            LL(p), 
                            p->sem.type.res,
                            re_rename_col (p->sem.type.col, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            p->sem.type.ty),
                        (count+1),
                        proj);
                break; 
            } 
            
            /* swap projection and current operatior p
             *
             *      |                  |
             *   twig (p)           project
             *      |        -->       |
             *   project            twig (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */    
            case la_twig:
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_twig (
                            LL(p), 
                            re_rename_col (p->sem.iter_item.iter, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.iter_item.item, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        L(p)->schema.count,
                        L(p)->sem.proj.items); 
                break; 
            
            /* swap projection and current operatior p
             *
             *      |                  |
             *   processi (p)       project
             *      |        -->       |
             *   project            processi (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */    
            case la_processi:
                *p = *PFla_project_ (
                        PFla_processi (
                            LL(p), 
                            re_rename_col (p->sem.iter_item1_item2.iter, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.iter_item1_item2.item1, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.iter_item1_item2.item2, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        L(p)->schema.count,
                        L(p)->sem.proj.items);
                break;
                
            /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             *
             *      |                  |
             *    1to1 (p)          project
             *      |        -->       |
             *   project             1to1 (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */ 
            case la_fun_1to1: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.fun_1to1.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.fun_1to1.res);

                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_fun_1to1 (
                            LL(p), 
                            p->sem.fun_1to1.kind,
                            p->sem.fun_1to1.res,
                            re_rename_col_in_collist (p->sem.fun_1to1.refs, 
                                                      L(p)->sem.proj.items,
                                                      L(p)->schema.count)),
                        (count + 1),
                        proj);
                break; 
            }
            
            /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             *
             *      |                  |
             *   bool_or (p)         project
             *      |        -->       |
             *   project            bool_or (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */ 
            case la_bool_or: {
                /* add rowid column to projection */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                                                   
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.binary.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.binary.res);

                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_or (
                            LL(p), 
                            p->sem.binary.res,
                            re_rename_col (p->sem.binary.col1, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.binary.col2, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        (count+1),
                        proj );
                break; 
            }

            /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             *
             *      |                  |
             *   bool_and (p)       project
             *      |        -->       |
             *   project            bool_and (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */ 
            case la_bool_and: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.binary.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.binary.res);

                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_and (
                            LL(p), 
                            p->sem.binary.res,
                            re_rename_col (p->sem.binary.col1, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.binary.col2, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        (count+1),
                        proj ); 
                break; 
            }

            /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             *
             *      |                  |
             *   bool_not (p)       project
             *      |        -->       |
             *   project            bool_not (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */ 
            case la_bool_not: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.unary.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.unary.res);

                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_not (
                            LL(p), 
                            p->sem.unary.res,
                            re_rename_col (p->sem.unary.col, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        (count+1),
                        proj );  
                break; 
            }
            
             /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             *
             *      |                  |
             *    num_eq (p)        project
             *      |        -->       |
             *   project             num_eq (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */ 
            case la_num_eq: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.binary.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.binary.res);

                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_eq (
                            LL(p), 
                            p->sem.binary.res,
                            re_rename_col (p->sem.binary.col1, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.binary.col2, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        (count+1),
                        proj ); 
                break; 
            }
            
             /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             *
             *      |                  |
             *    num_gt (p)        project
             *      |        -->       |
             *   project             num_gt (p)
             *      |                  |
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */ 
            case la_num_gt: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = L(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = L(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.binary.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.binary.res);

                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_gt (
                            LL(p), 
                            p->sem.binary.res,
                            re_rename_col (p->sem.binary.col1, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.binary.col2, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        (count+1),
                        proj ); 
                break; 
            }
            
            /* swap projection and current operatior p
             *
             *      |                    |
             *   step_join (p)        project
             *     / \                   |
             *    /   \               step_join (p)
             *   |     |      -->       / \
             *  doc  project           /   \
             *   |     |             doc   LL(p) 
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */    
            case la_step_join: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = R(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = R(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.step.item_res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.step.item_res);
            
                *p = *PFla_project_ (
                        PFla_step_join (
                            L(p), 
                            RL(p),
                            p->sem.step.spec,
                            p->sem.step.level,
                            re_rename_col (p->sem.step.item, 
                                            R(p)->sem.proj.items,
                                            R(p)->schema.count),
                            p->sem.step.item_res),
                        (count+1),
                        proj );
                break;
            }

             /* swap projection and current operatior p
             *
             *      |                        |
             *  doc_indx_join (p)         project
             *     / \                       |
             *    /   \                  doc_index_join (p)
             *   |     |        -->         / \
             *  doc  project               /   \
             *   |     |                 doc   LL(p) 
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */    
            case la_doc_index_join: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = R(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                           
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = R(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.doc_join.item_res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.doc_join.item_res);
            
                *p = *PFla_project_ (
                        PFla_doc_index_join (
                            L(p), 
                            RL(p),
                            p->sem.doc_join.kind,
                            p->sem.doc_join.item,
                            p->sem.doc_join.item_res,
                            re_rename_col (p->sem.doc_join.item_doc, 
                                            R(p)->sem.proj.items,
                                            R(p)->schema.count)),
                        (count+1),
                        proj );
                break;
            }
            
            /* swap projection and current operatior p
             *
             *      |                        |
             *  doc_indx_join (p)         project
             *     / \                       |
             *    /   \                  doc_index_join (p)
             *   |     |        -->         / \
             *  doc  project               /   \
             *   |     |                 doc   LL(p) 
             * 
             * if the projection renames columns, the columns have to 
             * be re-renamed before swaping the projection and the
             * operator p
             */    
            case la_guide_step_join: {
                 /* add rowid column to projection */
                unsigned int count = R(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                                                   
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = R(p)->sem.proj.items[i];
                    
                 /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.step.item_res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, p->sem.step.item_res);
            
                *p = *PFla_project_ (
                        PFla_guide_step_join (
                            L(p), 
                            RL(p),
                            p->sem.step.spec,
                            p->sem.step.guide_count,
                            p->sem.step.guides,
                            p->sem.step.level,
                            re_rename_col (p->sem.step.item, 
                                            R(p)->sem.proj.items,
                                            R(p)->schema.count),
                            p->sem.step.item_res),
                        (count+1),
                        proj );
                break;
            }
            
            /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             */    
            case la_eqjoin: {
                bool pi_r = false;
                bool pi_l = false;
            
                /* check if one column used in the left projection is used
                   in the eqjoin */
                if (L(p)->kind == la_project) {
                    pi_l = true;
                    for(unsigned int i = 0; i < L(p)->schema.count; i++) 
                        if ((L(p)->sem.proj.items[i].new != 
                                                    L(p)->sem.proj.items[i].old)
                          && (L(p)->sem.proj.items[i].new == p->sem.eqjoin.col1
                          || L(p)->sem.proj.items[i].new == p->sem.eqjoin.col2)) 
                        {
                            pi_l = false;
                            break;
                        }
                }
                
                /* check if one column used in the right projection is used
                   in the eqjoin */
                if (R(p)->kind == la_project) {
                    pi_r = true;
                    for(unsigned int i = 0; i < R(p)->schema.count; i++) 
                        if ((R(p)->sem.proj.items[i].new != 
                                                    R(p)->sem.proj.items[i].old)
                          && (R(p)->sem.proj.items[i].new == p->sem.eqjoin.col1
                          || R(p)->sem.proj.items[i].new == p->sem.eqjoin.col2)) 
                        {
                            pi_r = false;
                            break;
                        }
                }
                
                /* check if one column used in the left projection has the 
                   same name of an colum used in the right projection */
                if (L(p)->kind == la_project
                    && R(p)->kind == la_project) 
                {
                    for (unsigned int i = 0; i < L(p)->schema.count; i++) {
                        for (unsigned int j = 0; j < R(p)->schema.count; j++) 
                            if (R(p)->sem.proj.items[j].new == 
                                                    L(p)->sem.proj.items[i].new)
                            {
                                pi_l = false;
                                pi_r = false;
                                break;
                            }
                        if (!pi_l && !pi_r)
                            break;
                    }
                }
                
                /* if pi_l == true and pi_r == true then the left and right 
                 * projection can pass trought the join 
                 *
                 *       |                      |
                 *    eqjoin (p)             pi_r & pi_l
                 *     /   \         -->        |
                 *  pi_l    pi_r             eqjoin (p)
                 *    |      |                /   \
                 *
                 */ 
                if (pi_l && pi_r) {
                    unsigned int count = L(p)->sem.proj.count +
                                                        R(p)->sem.proj.count;
                
                     *p = *PFla_project_ (
                                PFla_eqjoin (
                                    LL(p), 
                                    RL(p),
                                    re_rename_col (
                                        re_rename_col (p->sem.eqjoin.col1, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count), 
                                        R(p)->sem.proj.items,
                                        R(p)->schema.count),
                                    re_rename_col (
                                        re_rename_col (p->sem.eqjoin.col2, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count), 
                                        R(p)->sem.proj.items,
                                        R(p)->schema.count)),
                                count,
                                mergeProjection (L(p)->sem.proj.items,
                                    L(p)->sem.proj.count,
                                    R(p)->sem.proj.items,
                                    R(p)->sem.proj.count)); 
                    break;
                }
                
                 /* if pi_l == true and pi_r == false then only the left 
                 * projection can pass trought the join 
                 *
                 *       |                      |
                 *    eqjoin (p)              pi_l
                 *     /   \         -->        |
                 *  pi_l    pi_r             eqjoin (p)
                 *    |      |                /   \
                 *   (*)    (*)             (*)   pi_r
                 *                                  |
                 */
                if (pi_l) {
                    unsigned int count = L(p)->schema.count + 
                                                        R(p)->schema.count;
                
                    *p = *PFla_project_ (
                        PFla_eqjoin (
                            LL(p), 
                            R(p),
                            re_rename_col (p->sem.eqjoin.col1, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count),
                            re_rename_col (p->sem.eqjoin.col2, 
                                            L(p)->sem.proj.items,
                                            L(p)->schema.count)),
                        count,
                        extend_proj(L(p)->sem.proj.items, 
                                         L(p)->sem.proj.count,
                                         R(p)->schema));
                        
                    break;
                }
                
                /* if pi_l == false and pi_r == true then only the right 
                 * projection can pass trought the join 
                 *
                 *       |                      |
                 *    eqjoin (p)              pi_r
                 *     /   \         -->        |
                 *  pi_l    pi_r             eqjoin (p)
                 *    |      |                /   \
                 *   (*)    (*)             pi_l  (*)
                 *                           |
                 */
                if (pi_r) {
                unsigned int count = R(p)->schema.count + 
                                                        L(p)->schema.count;
                
                    *p = *PFla_project_ (
                        PFla_eqjoin (
                            L(p), 
                            RL(p),
                            re_rename_col (p->sem.eqjoin.col1, 
                                            R(p)->sem.proj.items,
                                            R(p)->schema.count),
                            re_rename_col (p->sem.eqjoin.col2, 
                                            R(p)->sem.proj.items,
                                            R(p)->schema.count)),
                        count,
                        extend_proj(R(p)->sem.proj.items, 
                                         R(p)->sem.proj.count,
                                         L(p)->schema));
                        
                    break;
                }
                    
                    
                break;            
            }
            
            /* theta join */
            case la_thetajoin: {
                bool pi_r = false;
                bool pi_l = false;
            
                /* check if one column used in the left projection is used
                   in the left thetajoin predicates */
                if (L(p)->kind == la_project) {
                    pi_l = true;
                    for (unsigned int k = 0; k < p->sem.thetajoin.count; k++) {
                        for (unsigned int i = 0; i < L(p)->schema.count; i++) 
                            if (L(p)->sem.proj.items[i].new != 
                                                    L(p)->sem.proj.items[i].old
                                && L(p)->sem.proj.items[i].new == 
                                                p->sem.thetajoin.pred[k].left) 
                            {
                                pi_l = false;
                                break;
                            }
                        if (!pi_l)
                            break;
                    }
                } 
                
                /* check if one column used in the right projection is used
                   in the right thetajoin predicates */
                if (R(p)->kind == la_project) {
                    pi_r = true;
                    for (unsigned int k = 0; k < p->sem.thetajoin.count; k++) {
                        for (unsigned int i = 0; i < R(p)->schema.count; i++) 
                            if (R(p)->sem.proj.items[i].new != 
                                                    R(p)->sem.proj.items[i].old
                                && R(p)->sem.proj.items[i].new == 
                                                p->sem.thetajoin.pred[k].right) 
                            {
                                pi_r = false;
                                break;
                            }
                        if (!pi_r)
                            break;
                    }
                }
                
                /* check if one column used in the left projection has the 
                   same name of an colum used in the right projection */
                if (L(p)->kind == la_project
                    && R(p)->kind == la_project) 
                {
                    for (unsigned int i = 0; i < L(p)->schema.count; i++) { 
                        for (unsigned int j = 0; j < R(p)->schema.count; j++) 
                            if (R(p)->sem.proj.items[j].new == 
                                                    L(p)->sem.proj.items[i].new)
                            {
                                pi_l = false;
                                pi_r = false;
                                break;
                            }
                        if (!pi_l && !pi_r)
                            break;
                    }
                }
                
                /* if pi_l == true and pi_r == true then the left and right 
                 * projection can pass trought the join 
                 *
                 *       |                      |
                 *    eqjoin (p)             pi_r & pi_l
                 *     /   \         -->        |
                 *  pi_l    pi_r             eqjoin (p)
                 *    |      |                /   \
                 *
                 */ 
                if (pi_l && pi_r) {
                    unsigned int count = L(p)->sem.proj.count +
                                                        R(p)->sem.proj.count;
                
                    /* swap operators */
                     *p = *PFla_project_ (
                                PFla_thetajoin (
                                    LL(p), 
                                    RL(p),
                                    p->sem.thetajoin.count,
                                    re_rename_col_in_pred (
                                                    p->sem.thetajoin.pred,
                                                    L(p)->sem.proj.items,
                                                    R(p)->sem.proj.items,
                                                    p->sem.thetajoin.count,
                                                    L(p)->schema.count,
                                                    R(p)->schema.count,
                                                    true, true)),
                                count,
                                mergeProjection (L(p)->sem.proj.items,
                                    L(p)->sem.proj.count,
                                    R(p)->sem.proj.items,
                                    R(p)->sem.proj.count)); 
                    break;
                }
                
                 /* if pi_l == true and pi_r == false then only the left 
                 * projection can pass trought the join 
                 *
                 *       |                      |
                 *    eqjoin (p)              pi_l
                 *     /   \         -->        |
                 *  pi_l    pi_r             eqjoin (p)
                 *    |      |                /   \
                 *   (*)    (*)             (*)   pi_r
                 *                                  |
                 */
                if (pi_l) {
                    unsigned int count = L(p)->schema.count + 
                                                        R(p)->schema.count;
                
                    *p = *PFla_project_ (
                        PFla_thetajoin (
                            LL(p), 
                            R(p),
                            p->sem.thetajoin.count,
                            re_rename_col_in_pred (p->sem.thetajoin.pred,
                                            L(p)->sem.proj.items,
                                            NULL,
                                            p->sem.thetajoin.count,
                                            L(p)->schema.count,
                                            0,
                                            true, false)),
                        count,
                        extend_proj(L(p)->sem.proj.items, 
                                         L(p)->sem.proj.count,
                                         R(p)->schema));
                        
                    break;
                }
                
                /* if pi_l == false and pi_r == true then only the right 
                 * projection can pass trought the join 
                 *
                 *       |                      |
                 *    eqjoin (p)              pi_r
                 *     /   \         -->        |
                 *  pi_l    pi_r             eqjoin (p)
                 *    |      |                /   \
                 *   (*)    (*)             pi_l  (*)
                 *                           |
                 */
                if (pi_r) {
                    unsigned int count = L(p)->schema.count + 
                                                        R(p)->schema.count;
                
                    *p = *PFla_project_ (
                        PFla_thetajoin (
                            L(p), 
                            RL(p),
                            p->sem.thetajoin.count,
                            re_rename_col_in_pred (p->sem.thetajoin.pred,
                                            NULL,
                                            R(p)->sem.proj.items,
                                            p->sem.thetajoin.count,
                                            0,
                                            R(p)->schema.count,
                                            false, true)),
                        count,
                        extend_proj(R(p)->sem.proj.items, 
                                         R(p)->sem.proj.count,
                                         L(p)->schema));
                        
                    break;
                }
                    
                    
                break;            
            }

            /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             *
             *       |                     |
             *  doc_access (p)          project
             *       |          -->        |
             *    project             doc_access (p)
             *       |                     |
             */    
            case la_doc_access: {
                /* add result column to projection 
                 * create new projection list */
                unsigned int count = R(p)->schema.count;
                PFalg_proj_t *proj = PFmalloc ((count + 1) *
                                                   sizeof (PFalg_proj_t));
                        
                /* copy projections */
                 for (unsigned int i = 0; i < count; i++)
                    proj[i] = R(p)->sem.proj.items[i];

                /* make column name unique */
                PFalg_col_t res = make_col_unq(p->sem.doc_access.res);
                    
                /* add result column to projection */
                proj[count] = PFalg_proj (res, res);
            
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_doc_access (
                            L(p),
                            RL(p), 
                            p->sem.doc_access.res,
                            re_rename_col (p->sem.doc_access.col,
                                            R(p)->sem.proj.items,
                                            R(p)->schema.count),
                            p->sem.doc_access.doc_col),
                        (count+1),
                        proj);
                break; 
            }
 

            /* pass the projection rigth thru and duplicate operator 
             * swap projection and current operatior p
             *
             *      |                  |
             *     (p)              project
             *      |        -->       |
             *   project              (p)
             *      |                  |
             */    
            case la_dummy:
            case la_side_effects:
            case la_fragment:
            case la_frag_extract:
            case la_roots:
            case la_empty_frag:
            case la_nil:
            case la_rec_fix:
            case la_rec_param:
            case la_rec_arg:
            case la_rec_base:
            case la_proxy_base:
                /* swap projection and current operatior */
                *p = *PFla_project_ (
                        PFla_op_duplicate (p, LL(p), R(p)),
                        L(p)->schema.count,
                        L(p)->sem.proj.items);
                break;
            
            /* do nothing */
            default:
                break;
        }
    }
}

PFla_op_t *
PFalgopt_projection (PFla_op_t *root)
{
    /* Infer icol properties first */
    PFprop_infer_icol (root);

    /* Optimize algebra tree */
    opt_projection (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */

