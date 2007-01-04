/**
 * @file
 *
 * Map relational algebra expression DAG with
 * bit-encoded attribute names into an equivalent
 * one with unique attribute names.
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
#include <stdio.h>

#include "map_names.h"
#include "properties.h"
#include "mem.h"          /* PFmalloc() */
#include "oops.h"

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])

#define SEEN(p) ((p)->bit_dag)

/* lookup subtree with unique attribute names */
#define U(p) (lookup (map, (p)))
/* shortcut for function PFprop_unq_name */
#define UNAME(p,att) (PFprop_unq_name ((p)->prop,(att)))

struct ori_unq_map {
    PFla_op_t *ori;
    PFla_op_t *unq;
};
typedef struct ori_unq_map ori_unq_map;

/* worker for macro 'U(p)': based on an original subtree
   looks up the corresponding subtree with unique attribute names */
static PFla_op_t *
lookup (PFarray_t *map, PFla_op_t *ori)
{
    for (unsigned int i = 0; i < PFarray_last (map); i++)
        if (((ori_unq_map *) PFarray_at (map, i))->ori == ori)
            return ((ori_unq_map *) PFarray_at (map, i))->unq;

    assert (!"could not look up node");

    return NULL;
}

/* worker for unary operators */
static PFla_op_t *
unary_op (PFla_op_t *(*OP) (const PFla_op_t *,
                            PFalg_att_t, PFalg_att_t),
          PFla_op_t *p,
          PFarray_t *map)
{
    return OP (U(L(p)), 
               UNAME(p, p->sem.unary.res),
               UNAME(p, p->sem.unary.att));
}

/* worker for binary operators */
static PFla_op_t *
binary_op (PFla_op_t *(*OP) (const PFla_op_t *, PFalg_att_t,
                             PFalg_att_t, PFalg_att_t),
           PFla_op_t *p,
           PFarray_t *map)
{
    return OP (U(L(p)), 
               UNAME(p, p->sem.binary.res),
               UNAME(p, p->sem.binary.att1),
               UNAME(p, p->sem.binary.att2));
}

/* worker for PFmap_unq_names */
static void
map_unq_names (PFla_op_t *p, PFarray_t *map)
{
    PFla_op_t *res = NULL;

    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply name mapping for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        map_unq_names (p->child[i], map);

    /* action code */
    switch (p->kind) {
        case la_serialize:
            res = serialize (U(L(p)), U(R(p)),
                             UNAME(p, p->sem.serialize.pos),
                             UNAME(p, p->sem.serialize.item));
            break;

        case la_lit_tbl:
        {
            PFalg_attlist_t attlist;
            attlist.count = p->schema.count;
            attlist.atts  = PFmalloc (attlist.count *
                                      sizeof (PFalg_attlist_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                attlist.atts[i] = UNAME(p, p->schema.items[i].name);
                                                   
            res = PFla_lit_tbl_ (attlist,
                                 p->sem.lit_tbl.count,
                                 p->sem.lit_tbl.tuples);
        }   break;

        case la_empty_tbl:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items  = PFmalloc (schema.count *
                                      sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] = 
                    (struct PFalg_schm_item_t)
                        { .name = UNAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };
                                                   
            res = PFla_empty_tbl_ (schema);
        }   break;

        case la_attach:
            res = attach (U(L(p)),
                          UNAME(p, p->sem.attach.attname),
                          p->sem.attach.value);
            break;

        case la_cross:
        {
            /* Add a projection for each operand to ensure
               that all columns are renamed. */
            PFla_op_t *left, *right;
            PFalg_proj_t *projlist1, *projlist2;
            PFalg_att_t ori, unq;
            unsigned int count1, count2;

            left = U(L(p));
            right = U(R(p));

            projlist1 = PFmalloc (left->schema.count *
                                  sizeof (PFalg_proj_t));
            projlist2 = PFmalloc (right->schema.count *
                                  sizeof (PFalg_proj_t));

            count1 = 0;
            count2 = 0;

            for (unsigned int i = 0; i < left->schema.count; i++) {
                unq = left->schema.items[i].name;
                ori = PFprop_ori_name_left (p->prop, unq);
                assert (ori);
                
                projlist1[count1++] =
                    proj (UNAME(p, ori), 
                          unq);
            }

            for (unsigned int i = 0; i < right->schema.count; i++) {
                unq = right->schema.items[i].name;
                ori = PFprop_ori_name_right (p->prop, unq);
                assert (ori);
                
                projlist2[count2++] = 
                    proj (UNAME(p, ori),
                          unq);
            }
            
            left  = PFla_project_ (left, count1, projlist1);
            right = PFla_project_ (right, count2, projlist2);
                                
            res = cross (left, right);
        }   break;

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column unaware cross product operator is "
                    "only allowed inside mvd optimization!");
            break;

        case la_eqjoin:
        {
            /* Add a projection for each operand to ensure
               that all columns are present. */
            PFla_op_t *left, *right;
            PFalg_proj_t *projlist, *projlist2;
            PFalg_att_t ori, unq, l_unq, r_unq, att1_unq, att2_unq;
            unsigned int count, count2;
            bool renamed;

            left = U(L(p));
            right = U(R(p));

            att1_unq = PFprop_unq_name_left (p->prop, p->sem.eqjoin.att1);
            att2_unq = PFprop_unq_name_right (p->prop, p->sem.eqjoin.att2);
            
            projlist  = PFmalloc (left->schema.count *
                                  sizeof (PFalg_proj_t));
            projlist2 = PFmalloc (right->schema.count *
                                  sizeof (PFalg_proj_t));
            count = 0;
            count2 = 0;
            projlist [count++]  = proj (att1_unq, att1_unq);
            projlist2[count2++] = proj (att2_unq, att2_unq);
                                              
            renamed = false;
            for (unsigned int i = 0; i < left->schema.count; i++) {
                l_unq = left->schema.items[i].name;
                ori = PFprop_ori_name_left (p->prop, l_unq);
                assert (ori);
                
                unq = PFprop_unq_name (p->prop, ori);
                if (l_unq != att1_unq) {
                    projlist[count++] = proj (unq, l_unq);
                    renamed = renamed || (unq != l_unq);
                }
            }
            if (renamed)
                left = PFla_project_ (left, count, projlist);

            renamed = false;
            for (unsigned int i = 0; i < right->schema.count; i++) {
                r_unq = right->schema.items[i].name;
                ori = PFprop_ori_name_right (p->prop, r_unq);
                assert (ori);
                
                unq = PFprop_unq_name (p->prop, ori);
                if (r_unq != att2_unq) {
                    projlist2[count2++] = proj (unq, r_unq);
                    renamed = renamed || (unq != r_unq);
                }
            }
            if (renamed)
                right = PFla_project_ (right, count2, projlist2);
                                
            res = PFla_eqjoin_clone (left, right, 
                                     att1_unq, att2_unq,
                                     UNAME(p, p->sem.eqjoin.att1));
        }   break;
            
        case la_eqjoin_unq:
            PFoops (OOPS_FATAL,
                    "clone column aware eqjoin operator is "
                    "only allowed with unique attribute names!");

        case la_semijoin:
            /* Transform semi-join operations into equi-joins
               as these semi-joins might be superfluous as well. */
            if (PFprop_set (p->prop)) {
                res = PFla_eqjoin_clone (
                          U(L(p)),
                          U(R(p)),
                          UNAME (p, p->sem.eqjoin.att1),
                          PFprop_unq_name_right (p->prop, 
                                                 p->sem.eqjoin.att2),
                          UNAME (p, p->sem.eqjoin.att1));
            } else {
                res = semijoin (U(L(p)), U(R(p)),
                                UNAME (p, p->sem.eqjoin.att1),
                                PFprop_unq_name_right (p->prop, 
                                                       p->sem.eqjoin.att2));
            }
            break;

        case la_project:
        {
            PFla_op_t *left;
            PFalg_proj_t *projlist = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
            PFalg_att_t unq;
            unsigned int count = 0;
            
            left = U(L(p));
            
            for (unsigned int i = 0; i < left->schema.count; i++)
                for (unsigned int j = 0; j < p->sem.proj.count; j++)
                    if ((unq = UNAME(p, p->sem.proj.items[j].new)) ==
                        left->schema.items[i].name) {
                        projlist[count++] = proj (unq, unq);
                        break;
                    }
                    
            /* if the projection does not prune a column
               we may skip the projection operator */
            if (count == left->schema.count)
                res = left;
            else
                res = PFla_project_ (left, count, projlist);

        }   break;

        case la_select:
            res = select_ (U(L(p)), UNAME(p, p->sem.select.att));
            break;

        case la_disjunion:
        case la_intersect:
        case la_difference:
        {
            /* Add a projection for each operand to ensure
               that all columns are present and with the correct
               name (names matching in pairs again). */
            PFla_op_t *left, *right;
            PFalg_proj_t *projlist1, *projlist2;
            PFalg_att_t ori, unq;

            projlist1 = PFmalloc (p->schema.count *
                                  sizeof (PFalg_proj_t));
            projlist2 = PFmalloc (p->schema.count *
                                  sizeof (PFalg_proj_t));

            for (unsigned int i = 0; i < p->schema.count; i++) {
                ori = p->schema.items[i].name;
                unq = UNAME(p, ori);
                projlist1[i] = proj (unq, 
                                     PFprop_unq_name_left (p->prop, ori));
                projlist2[i] = proj (unq, 
                                     PFprop_unq_name_right (p->prop, ori));
            }
            
            left  = PFla_project_ (U(L(p)), p->schema.count, projlist1);
            right = PFla_project_ (U(R(p)), p->schema.count, projlist2);
                                
            if (p->kind == la_disjunion)
                res = disjunion (left, right);
            else if (p->kind == la_intersect)
                res = intersect (left, right);
            else if (p->kind == la_difference)
                res = difference (left, right);
        }   break;

        case la_distinct:
            res = distinct (U(L(p)));
            break;

        case la_fun_1to1:
        {
            PFalg_attlist_t refs;
            
            refs.count = p->sem.fun_1to1.refs.count;
            refs.atts  = PFmalloc (refs.count *
                                   sizeof (*(refs.atts)));

            for (unsigned int i = 0; i < refs.count; i++)
                refs.atts[i] = UNAME(p, p->sem.fun_1to1.refs.atts[i]);

            res = fun_1to1 (U(L(p)),
                            p->sem.fun_1to1.kind,
                            UNAME(p, p->sem.fun_1to1.res),
                            refs);
        }   break;
            
        case la_num_eq:
            res = binary_op (PFla_eq, p, map);
            break;
        case la_num_gt:
            res = binary_op (PFla_gt, p, map);
            break;
        case la_bool_and:
            res = binary_op (PFla_and, p, map);
            break;
        case la_bool_or:
            res = binary_op (PFla_or, p, map);
            break;
        case la_bool_not:
            res = unary_op (PFla_not, p, map);
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
            res = aggr (p->kind, U(L(p)), 
                        UNAME(p, p->sem.aggr.res),
                        /* attribute att is stored only in child operator */
                        UNAME(L(p), p->sem.aggr.att),
                        p->sem.aggr.part?UNAME(p, p->sem.aggr.part):att_NULL);
            break;
            
        case la_count:
            res = count (U(L(p)),
                         UNAME(p, p->sem.aggr.res),
                         p->sem.aggr.part?UNAME(p, p->sem.aggr.part):att_NULL);
            break;
                           
        case la_rownum:
        {
            PFord_ordering_t sortby = PFordering ();

            for (unsigned int i = 0;
                 i < PFord_count (p->sem.rownum.sortby);
                 i++)
                sortby = PFord_refine (
                             sortby,
                             UNAME(p, PFord_order_col_at (
                                          p->sem.rownum.sortby, i)),
                             PFord_order_dir_at (
                                 p->sem.rownum.sortby, i));
                                                   
            res = rownum (U(L(p)),
                          UNAME(p, p->sem.rownum.attname),
                          sortby,
                          UNAME(p, p->sem.rownum.part));
        }   break;
        
        case la_number:
            res = number (U(L(p)),
                          UNAME(p, p->sem.number.attname),
                          UNAME(p, p->sem.number.part));
            break;
        
        case la_type:
            res = type (U(L(p)),
                        UNAME(p, p->sem.type.res),
                        UNAME(p, p->sem.type.att),
                        p->sem.type.ty);
            break;

        case la_type_assert:
            res = type_assert_pos (U(L(p)),
                                   UNAME(p, p->sem.type.att),
                                   p->sem.type.ty);
            break;
        
        case la_cast:
            res = cast (U(L(p)),
                        UNAME(p, p->sem.type.res),
                        UNAME(p, p->sem.type.att),
                        p->sem.type.ty);
            break;
        
        case la_seqty1:
            res = seqty1 (U(L(p)),
                          UNAME(p, p->sem.aggr.res),
                          /* attribute att is stored only in child operator */
                          UNAME(L(p), p->sem.aggr.att),
                          p->sem.aggr.part?UNAME(p, p->sem.aggr.part):att_NULL);
            break;
            
        case la_all:
            res = all (U(L(p)),
                       UNAME(p, p->sem.aggr.res),
                       /* attribute att is stored only in child operator */
                       UNAME(L(p), p->sem.aggr.att),
                       p->sem.aggr.part?UNAME(p, p->sem.aggr.part):att_NULL);
            break;

        case la_scjoin:
            res = scjoin (U(L(p)), U(R(p)),
                          p->sem.scjoin.axis,
                          p->sem.scjoin.ty,
                          UNAME(p, p->sem.scjoin.iter),
                          /* unique name of input attribute item is 
                             stored in the child operator only */
                          UNAME(R(p), p->sem.scjoin.item),
                          UNAME(p, p->sem.scjoin.item_res));
            break;
                           
        case la_dup_scjoin:
            res = dup_scjoin (U(L(p)), U(R(p)),
                              p->sem.scjoin.axis,
                              p->sem.scjoin.ty,
                              UNAME(p, p->sem.scjoin.item),
                              UNAME(p, p->sem.scjoin.item_res));
            break;
                           
        case la_doc_tbl:
            res = doc_tbl (U(L(p)),
                           UNAME(p, p->sem.doc_tbl.iter),
                           /* unique name of input attribute item is 
                              stored in the child operator only */
                           UNAME(L(p), p->sem.doc_tbl.item),
                           UNAME(p, p->sem.doc_tbl.item_res));
            break;
                           
        case la_doc_access:
            res = doc_access (U(L(p)), U(R(p)),
                              UNAME(p, p->sem.doc_access.res),
                              UNAME(p, p->sem.doc_access.att),
                              p->sem.doc_access.doc_col);
            break;

        case la_element:
            res = element (
                      U(L(p)), U(L(R(p))), U(R(R(p))),
                      /* unique name of input attributes iter_qn, item_qn,
                         iter_val, pos_val, and item_val are stored in the
                         child operators only */
                      UNAME(L(R(p)), p->sem.elem.iter_qn),
                      UNAME(L(R(p)), p->sem.elem.item_qn),
                      UNAME(R(R(p)), p->sem.elem.iter_val),
                      UNAME(R(R(p)), p->sem.elem.pos_val),
                      UNAME(R(R(p)), p->sem.elem.item_val),
                      UNAME(p, p->sem.elem.iter_res),
                      UNAME(p, p->sem.elem.item_res));
            break;
        
        case la_element_tag:
            return; /* skip element tag */

        case la_attribute:
            res = attribute (U(L(p)),
                             UNAME(p, p->sem.attr.res),
                             UNAME(p, p->sem.attr.qn),
                             UNAME(p, p->sem.attr.val));
            break;

        case la_textnode:
            res = textnode (U(L(p)),
                            UNAME(p, p->sem.textnode.res),
                            UNAME(p, p->sem.textnode.item));
            break;

        case la_docnode:
        case la_comment:
        case la_processi:
            break;

        case la_merge_adjacent:
            res = merge_adjacent (
                      U(L(p)), U(R(p)),
                      /* unique name of input attributes iter_in, pos_in,
                         and item_in are stored in the child operator only */
                      UNAME(R(p), p->sem.merge_adjacent.iter_in),
                      UNAME(R(p), p->sem.merge_adjacent.pos_in),
                      UNAME(R(p), p->sem.merge_adjacent.item_in),
                      UNAME(p, p->sem.merge_adjacent.iter_res),
                      UNAME(p, p->sem.merge_adjacent.pos_res),
                      UNAME(p, p->sem.merge_adjacent.item_res));
            break;

        case la_roots:
            res = roots (U(L(p)));
            break;

        case la_fragment:
            res = fragment (U(L(p)));
            break;

        case la_frag_union:
            res = PFla_frag_union (U(L(p)), U(R(p)));
            break;

        case la_empty_frag:
            res = empty_frag ();
            break;

        case la_cond_err:
            res = cond_err (U(L(p)), U(R(p)), 
                            /* unique name of input attribute att is 
                               stored in the child operator only */
                            UNAME(R(p), p->sem.err.att),
                            p->sem.err.str);
            break;
        
        case la_rec_fix:
            res = rec_fix (U(L(p)), U(R(p)));
            break;
            
        case la_rec_param:
            res = rec_param (U(L(p)), U(R(p)));
            break;
            
        case la_rec_nil:
            res = rec_nil ();
            break;
            
        case la_rec_arg:
        /* The results of the left (seed) and the right (recursion) argument 
           have different result schemas. Thus we introduce a mapping 
           projection that transforms the recursion schema into the seed 
           (and base) schema. */
        {
            PFalg_proj_t *projlist = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                projlist[i] = proj (UNAME(p, p->schema.items[i].name),
                                    UNAME(R(p), p->schema.items[i].name));
                    
            res = rec_arg (U(L(p)),
                           PFla_project_ (U(R(p)), p->schema.count, projlist),
                           U(p->sem.rec_arg.base));
        }   break;

        case la_rec_base:
        /* combine the new original names with the already
           existing types (see also 'case la_empty_tbl') */
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items  = PFmalloc (schema.count *
                                      sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] = 
                    (struct PFalg_schm_item_t)
                        { .name = UNAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };
                                                   
            res = rec_base (schema);
        } break;
            
        case la_proxy:
        case la_proxy_base:
            PFoops (OOPS_FATAL,
                    "PROXY EXPANSION MISSING");
            break;

        case la_string_join:
            res = fn_string_join (
                      U(L(p)), U(R(p)),
                      /* unique name of input attributes iter, pos, item,
                         iter_seq, and item_sep are stored in the child
                         operators only */
                      UNAME(L(p), p->sem.string_join.iter),
                      UNAME(L(p), p->sem.string_join.pos),
                      UNAME(L(p), p->sem.string_join.item),
                      UNAME(R(p), p->sem.string_join.iter_sep),
                      UNAME(R(p), p->sem.string_join.item_sep),
                      UNAME(p, p->sem.string_join.iter_res),
                      UNAME(p, p->sem.string_join.item_res));
            break;

        case la_dummy:
            res = U(L(p));
            break;
    }

    assert(res);

    /* Add pair (p, res) to the environment map
       to allow lookup of already generated subplans. */
    *(ori_unq_map *) PFarray_add (map) = 
        (ori_unq_map) { .ori = p, .unq = res};
}

/**
 * Invoke name mapping.
 */
PFla_op_t *
PFmap_unq_names (PFla_op_t *root)
{
    PFarray_t *map = PFarray (sizeof (ori_unq_map));

    /* infer unique names */
    PFprop_infer_unq_names (root);
    /* infer the set property */
    PFprop_infer_set (root);
 
    /* generate equivalent algebra DAG */
    map_unq_names (root, map);

    /* return algebra DAG with unique names */
    return U (root);
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
