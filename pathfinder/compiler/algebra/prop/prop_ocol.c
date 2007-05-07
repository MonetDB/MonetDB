/**
 * @file
 *
 * Inference of schema information (ocol property) of logical
 * algebra expressions.
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

/*
 * access ocol information
 */
#define ocols_count(p) (p)->schema.count
#define ocols(p)       (p)->schema
#define ocol_at(p,i)   (p)->schema.items[i]
#define new_ocols(p,i) ocols_count (p) = (i); \
                       (p)->schema.items = PFmalloc ((i) * \
                                           sizeof (*((p)->schema.items)))

/**
 * Test if @a attr is in the list of ocol columns of node @a n
 */
bool
PFprop_ocol (const PFla_op_t *n, PFalg_att_t attr)
{
    assert (n);

    for (unsigned int i = 0; i < n->schema.count; i++)
        if (attr == n->schema.items[i].name)
            return true;

    return false;
}

/**
 * worker for ocol property inference;
 * Copies schema using size as array size
 */
static PFalg_schema_t
copy_ocols (PFalg_schema_t ori, unsigned int size)
{
    PFalg_schema_t ret;

    assert (ori.count <= size);

    ret.items = PFmalloc (size * sizeof (*(ret.items)));
    ret.count = ori.count;

    for (unsigned int i = 0; i < ori.count; i++)
        ret.items[i] = ori.items[i];

    return ret;
}

/**
 * Infer schema (ocol property).
 * (schema inference should be aligned to logical.c)
 */
static void
infer_ocol (PFla_op_t *n)
{
    switch (n->kind)
    {
        case la_serialize:
            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)));

        /* only a rewrite can change the ocol property
           - thus update schema (property) during rewrite */
        case la_lit_tbl:
        case la_empty_tbl:
            break;

        case la_attach:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.attach.attname;
            ocol_at (n, ocols_count (n)).type = n->sem.attach.value.type;
            ocols_count (n)++;
            break;

        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
            ocols (n) = copy_ocols (ocols (L(n)), 
                                    ocols_count (L(n)) +
                                    ocols_count (R(n)));
            for (unsigned int i = 0; i < ocols_count (R(n)); i++) {
                ocol_at (n, ocols_count(n)) = ocol_at (R(n), i);
                ocols_count (n)++;
            }
            break;
            
        case la_semijoin:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;
            
        case la_project:
        {
            PFarray_t *proj_list = PFarray (sizeof (PFalg_proj_t));

            /* prune projection list according to
               the ocols of its argument */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                for (unsigned int j = 0; j < ocols_count (L(n)); j++)
                    if (n->sem.proj.items[i].old == 
                        ocol_at (L(n), j).name) {
                        *(PFalg_proj_t *) PFarray_add (proj_list)
                            = n->sem.proj.items[i];
                        break;
                    }

            /* allocate space for new ocol property and projection list */
            n->sem.proj.count = PFarray_last (proj_list);
            n->sem.proj.items = PFmalloc (n->sem.proj.count * 
                                          sizeof (*(n->sem.proj.items)));
            new_ocols (n, PFarray_last (proj_list));

            /* copy ocols and projection list during the second pass */
            for (unsigned int i = 0; i < PFarray_last (proj_list); i++)
                for (unsigned int j = 0; j < ocols_count (L(n)); j++)
                    if ((*(PFalg_proj_t *) PFarray_at (proj_list, i)).old ==
                        ocol_at (L(n), j).name) {
                        n->sem.proj.items[i] = 
                            *(PFalg_proj_t *) PFarray_at (proj_list, i);
                        (ocol_at (n, i)).type = (ocol_at (L(n), j)).type;
                        (ocol_at (n, i)).name = n->sem.proj.items[i].new;
                        break;
                    }

        } break;

        case la_select:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;

        case la_disjunion:
        {
            unsigned int  i, j;

            /* see if both operands have same number of attributes */
            if (ocols_count (L(n)) != ocols_count (R(n)))
                PFoops (OOPS_FATAL,
                        "Schema of two arguments of UNION does not match");

            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));

            /* combine types of the both arguments */
            for (i = 0; i < ocols_count (n); i++) {
                for (j = 0; j < ocols_count (R(n)); j++)
                    if ((ocol_at (n, i)).name == (ocol_at (R(n), j)).name) {
                        /* The two attributes match, so include their name
                         * and type information into the result. This allows
                         * for the order of schema items in n1 and n2 to be
                         * different.
                         */
                        (ocol_at (n, i)).type = (ocol_at (n, i)).type
                                                | (ocol_at (R(n), j)).type;
                        break;
                    }

                if (j == ocols_count (R(n)))
                    PFoops (OOPS_FATAL,
                            "Schema of two arguments of "
                            "UNION does not match");
            }
        } break;

        case la_intersect:
        {
            unsigned int  i, j;

            /* see if both operands have same number of attributes */
            if (ocols_count (L(n)) != ocols_count (R(n)))
                PFoops (OOPS_FATAL,
                        "Schema of two arguments of INTERSECTION does not match");

            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));

            /* combine types of the both arguments */
            for (i = 0; i < ocols_count (n); i++) {
                for (j = 0; j < ocols_count (R(n)); j++)
                    if ((ocol_at (n, i)).name == (ocol_at (R(n), j)).name) {
                        /* The two attributes match, so include their name
                         * and type information into the result. This allows
                         * for the order of schema items in n1 and n2 to be
                         * different.
                         */
                        (ocol_at (n, i)).type = (ocol_at (n, i)).type
                                                & (ocol_at (R(n), j)).type;
                        break;
                    }

                if (j == ocols_count (R(n)))
                    PFoops (OOPS_FATAL,
                            "Schema of two arguments of "
                            "INTERSECTION does not match");
            }
        } break;

        case la_difference:
        case la_distinct:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;

        case la_fun_1to1:
        {
            unsigned int        i, j, ix[n->sem.fun_1to1.refs.count];
            PFalg_simple_type_t res_type = 0;
            
            /* verify that the referenced attributes in refs
               are really attributes of n ... */
            for (i = 0; i < n->sem.fun_1to1.refs.count; i++) {
                for (j = 0; j < ocols_count (L(n)); j++)
                    if (ocol_at (L(n), j).name == n->sem.fun_1to1.refs.atts[i])
                        break;
                if (j == ocols_count (L(n)))
                    PFoops (OOPS_FATAL,
                            "attribute `%s' referenced in generic function"
                            " operator not found",
                            PFatt_str (n->sem.fun_1to1.refs.atts[i]));
                ix[i] = j;
            }

            /* we want to perform some more consistency checks
               that are specific to certain operators */
            switch (n->sem.fun_1to1.kind) {
                /**
                 * Depending on the @a kind parameter, we add, subtract, 
                 * multiply, or divide the two values of columns @a att1
                 * and @a att2 and store the result in newly created attribute
                 * @a res. @a res gets the same data type as @a att1 and 
                 * @a att2. The result schema corresponds to the schema 
                 * of the input relation @a n plus @a res.
                 */
                case alg_fun_num_add:
                case alg_fun_num_subtract:
                case alg_fun_num_multiply:
                case alg_fun_num_divide:
                case alg_fun_num_modulo:
                    assert (n->sem.fun_1to1.refs.count == 2);
                    /* make sure both attributes are of the same numeric type */
                    assert (ocol_at (L(n), ix[0]).type == aat_nat ||
                            ocol_at (L(n), ix[0]).type == aat_int ||
                            ocol_at (L(n), ix[0]).type == aat_dec ||
                            ocol_at (L(n), ix[0]).type == aat_dbl);
                    assert (ocol_at (L(n), ix[0]).type == 
                            ocol_at (L(n), ix[1]).type);

                    res_type = ocol_at (L(n), ix[1]).type;
                    break;

                case alg_fun_fn_abs:
                case alg_fun_fn_ceiling:
                case alg_fun_fn_floor:
                case alg_fun_fn_round:
                    assert (n->sem.fun_1to1.refs.count == 1);
                    /* make sure the attribute is of numeric type */
                    assert (ocol_at (L(n), ix[0]).type == aat_int ||
                            ocol_at (L(n), ix[0]).type == aat_dec ||
                            ocol_at (L(n), ix[0]).type == aat_dbl);

                    res_type = ocol_at (L(n), ix[0]).type;
                    break;

                case alg_fun_fn_concat:
                    assert (n->sem.fun_1to1.refs.count == 2);
                    /* make sure both attributes are of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str &&
                            ocol_at (L(n), ix[1]).type == aat_str);

                    res_type = aat_str;
                    break;
                    
                case alg_fun_fn_contains:
                    assert (n->sem.fun_1to1.refs.count == 2);
                    /* make sure both attributes are of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str &&
                            ocol_at (L(n), ix[1]).type == aat_str);

                    res_type = aat_bln;
                    break;
            
                case alg_fun_fn_number:
                    assert (n->sem.fun_1to1.refs.count == 1);
                    res_type = aat_dbl;
                    break;
            }

            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.fun_1to1.res;
            ocol_at (n, ocols_count (n)).type = res_type;
            ocols_count (n)++;
            
        }   break;
            
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.binary.res;
            ocol_at (n, ocols_count (n)).type = aat_bln;
            ocols_count (n)++;
            break;

        case la_bool_not:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.unary.res;
            ocol_at (n, ocols_count (n)).type = aat_bln;
            ocols_count (n)++;
            break;

        case la_avg:
	case la_max:
	case la_min:
        case la_sum:
            /* set number of schema items in the result schema:
             * result attribute plus partitioning attribute 
             * (if available -- constant optimizations may
             *  have removed it).
             */
            new_ocols (n, n->sem.aggr.part ? 2 : 1);

            /* verify that attributes 'att' and 'part' are attributes of n
             * and include them into the result schema
             */
            for (unsigned int i = 0; i < ocols_count (L(n)); i++) {
                if (n->sem.aggr.att == ocol_at (L(n), i).name) {
                    ocol_at (n, 0) = ocol_at (L(n), i);
                    ocol_at (n, 0).name = n->sem.aggr.res;
                }
                if (n->sem.aggr.part &&
                    n->sem.aggr.part == ocol_at (L(n), i).name) {
                    ocol_at (n, 1) = ocol_at (L(n), i);
                }
            }
            break;
            
        case la_count:
            /* set number of schema items in the result schema:
             * result attribute plus partitioning attribute 
             * (if available -- constant optimizations may
             *  have removed it).
             */
            new_ocols (n, n->sem.aggr.part ? 2 : 1);

            /* insert result attribute into schema */
            ocol_at (n, 0).name = n->sem.aggr.res;
            ocol_at (n, 0).type = aat_int;

            /* copy the partitioning attribute */
            if (n->sem.aggr.part)
                for (unsigned int i = 0; i < ocols_count (L(n)); i++)
                    if (ocol_at (L(n), i).name == n->sem.aggr.part) {
                        ocol_at (n, 1) = ocol_at (L(n), i);
                        break;
                    }
            break;

        case la_rownum:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.rownum.attname;
            ocol_at (n, ocols_count (n)).type = aat_nat;
            ocols_count (n)++;
            break;

        case la_number:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.number.attname;
            ocol_at (n, ocols_count (n)).type = aat_nat;
            ocols_count (n)++;
            break;

        case la_type:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.type.res;
            ocol_at (n, ocols_count (n)).type = aat_bln;
            ocols_count (n)++;
            break;

        case la_type_assert:
            new_ocols (n, ocols_count (L(n)));

            /* copy schema from 'n' argument */
            for (unsigned int i = 0; i < ocols_count (L(n)); i++)
            {
                if (n->sem.type.att == ocol_at (L(n), i).name)
                {
                    ocol_at (n, i).name = n->sem.type.att;
                    ocol_at (n, i).type = n->sem.type.ty;
                }
                else
                    ocol_at (n, i) = ocol_at (L(n), i);
            }
            break;

        case la_cast:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.type.res;
            ocol_at (n, ocols_count (n)).type = n->sem.type.ty;
            ocols_count (n)++;
            break;

        case la_seqty1:
        case la_all:
            new_ocols (n, n->sem.aggr.part ? 2 : 1);
            
            ocol_at (n, 0).name = n->sem.aggr.res;
            ocol_at (n, 0).type = aat_bln;
            if (n->sem.aggr.part) {
                ocol_at (n, 1).name = n->sem.aggr.part;
                ocol_at (n, 1).type = aat_nat;
            }
            break;

        case la_scjoin:
            new_ocols (n, 2);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.scjoin.iter,
                                        .type = aat_nat };

            if (n->sem.scjoin.axis == alg_attr) 
                ocol_at (n, 1)
                    = (PFalg_schm_item_t) { .name = n->sem.scjoin.item_res,
                                            .type = aat_anode };
            else
                ocol_at (n, 1)
                    = (PFalg_schm_item_t) { .name = n->sem.scjoin.item_res,
                                            .type = aat_pnode };
            break;

        case la_dup_scjoin:
            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)) + 1);
            if (n->sem.scjoin.axis == alg_attr) 
                ocol_at (n, ocols_count (n))
                    = (PFalg_schm_item_t) { .name = n->sem.scjoin.item_res,
                                            .type = aat_anode };
            else
                ocol_at (n, ocols_count (n))
                    = (PFalg_schm_item_t) { .name = n->sem.scjoin.item_res,
                                            .type = aat_pnode };
            ocols_count (n)++;
            break;
            
        case la_doc_access:
            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.doc_access.res;
            ocol_at (n, ocols_count (n)).type = aat_str;
            ocols_count (n)++;
            break;

        /* operators with static iter|item schema */
        case la_doc_tbl:
            new_ocols (n, 2);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.doc_tbl.iter,
                                        .type = aat_nat };
            ocol_at (n, 1)
                = (PFalg_schm_item_t) { .name = n->sem.doc_tbl.item_res,
                                        .type = aat_pnode };
            break;

        case la_element:
            new_ocols (n, 2);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.elem.iter_res,
                                        .type = aat_nat };
            ocol_at (n, 1)
                = (PFalg_schm_item_t) { .name = n->sem.elem.item_res,
                                        .type = aat_pnode };
            break;

        case la_docnode:
        case la_comment:
        case la_processi:
            break;

        case la_attribute:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.attr.res;
            ocol_at (n, ocols_count (n)).type = aat_anode;
            ocols_count (n)++;
            break;

        case la_textnode:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.textnode.res;
            ocol_at (n, ocols_count (n)).type = aat_pnode;
            ocols_count (n)++;
            break;

        /* operator with static iter|pos|item schema */
        case la_merge_adjacent:
            new_ocols (n, 3);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.merge_adjacent.iter_res,
                                        .type = aat_nat };
            ocol_at (n, 1)
                = (PFalg_schm_item_t) { .name = n->sem.merge_adjacent.pos_res,
                                        .type = aat_nat };
            ocol_at (n, 2)
                = (PFalg_schm_item_t) { .name = n->sem.merge_adjacent.item_res,
                                        .type = aat_node };
            break;

        case la_roots:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;

        /* operators without schema */
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_element_tag:
            /* keep empty schema */
            break;

        case la_cond_err:
        case la_trace:
        case la_trace_msg:
        case la_trace_map:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;

        case la_nil:
            /* nil does not have a schema */
            break;
            
        case la_rec_fix:
            /* get the schema of the overall result */
            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)));
            break;
            
        case la_rec_param:
            /* recursion parameters do not have properties */
            break;
            
        case la_rec_arg:
        {
            unsigned int  i, j;

            /* see if both operands have same number of attributes */
            if (ocols_count (L(n)) != ocols_count (R(n)) ||
                ocols_count (L(n)) != ocols_count (n->sem.rec_arg.base))
                PFoops (OOPS_FATAL,
                        "Schema of the arguments of recursion "
                        "argument to not match");

            /* see if we find each attribute in all of the input relations */
            for (i = 0; i < ocols_count (L(n)); i++) {
                for (j = 0; j < ocols_count (R(n)); j++)
                    if (ocol_at (L(n), i).name == ocol_at (R(n), j).name) {
                        break;
                    }

                if (j == ocols_count (R(n)))
                    PFoops (OOPS_FATAL,
                            "Schema of the arguments of recursion "
                            "argument to not match");

                for (j = 0; j < ocols_count (n->sem.rec_arg.base); j++)
                    if (ocol_at (L(n), i).name == 
                        ocol_at (n->sem.rec_arg.base, j).name) {
                        break;
                    }

                if (j == ocols_count (n->sem.rec_arg.base))
                    PFoops (OOPS_FATAL,
                            "Schema of the arguments of recursion "
                            "argument to not match");
            }

            /* keep the schema of its inputs */
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
        } break;
            
        /* only a rewrite can change the ocol property
           - thus update schema (property) during rewrite */
        case la_rec_base:
            break;
            
        case la_proxy:
        case la_proxy_base:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;

        case la_string_join:
            new_ocols (n, 2);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.string_join.iter_res,
                                        .type = aat_nat };
            ocol_at (n, 1)
                = (PFalg_schm_item_t) { .name = n->sem.string_join.item_res,
                                        .type = aat_str };
            break;

        case la_eqjoin_unq:
            PFoops (OOPS_FATAL,
                    "clone column aware equi-join operator is "
                    "only allowed with unique names!");
            
        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");
            
        case la_dummy:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;
    }
}

/* forward declaration */
static void
prop_infer (PFla_op_t *n);

/* Helper function that walks through a recursion paramter list
   and only calls the property inference for the seed expressions. */
static void
prop_infer_rec_seed (PFla_op_t *n)
{
    switch (n->kind)
    {
        case la_rec_param:
            /* infer the ocols of the arguments */
            prop_infer_rec_seed (L(n));
            prop_infer_rec_seed (R(n));
            break;

        case la_rec_arg:
            /* infer the ocols of the seed */
            prop_infer (L(n));
            
            n->sem.rec_arg.base->bit_dag = true;
            ocols (n->sem.rec_arg.base) = copy_ocols (ocols (L(n)),
                                                      ocols_count (L(n)));
            break;

        case la_nil:
            break;

        default:
            PFoops (OOPS_FATAL,
                    "unexpected node kind %i",
                    n->kind);
            break;
    }
}

/* Helper function that walks through a recursion paramter list
   and only calls the property inference for the recursion body. */
static void
prop_infer_rec_body (PFla_op_t *n)
{
    switch (n->kind)
    {
        case la_rec_param:
            /* infer the ocols of the arguments */
            prop_infer_rec_body (L(n));
            prop_infer_rec_body (R(n));
            break;

        case la_rec_arg:
            /* infer the ocols of the recursion body */
            prop_infer (R(n));
            break;

        case la_nil:
            break;

        default:
            PFoops (OOPS_FATAL,
                    "unexpected node kind %i",
                    n->kind);
            break;
    }

    n->bit_dag = true;

    /* infer information on resulting columns */
    infer_ocol (n);
}
    
/* worker for PFprop_infer_ocol */
static void
prop_infer (PFla_op_t *n)
{
    bool bottom_up = true;
    
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return;

    /* Make sure to first collect all seeds and adjust
       the rec_base properties before inferring the properties
       for the body and result expression. */
    switch (n->kind)
    {
        case la_rec_fix:
            /* infer the ocols of the arguments */
            prop_infer_rec_seed (L(n));
            prop_infer_rec_body (L(n));
            prop_infer (R(n));
            bottom_up = false;
            break;

        default:
            break;
    }
    
    if (bottom_up)
        /* infer properties for children bottom-up (ensure that
           the fragment information is translated after the value part) */
        for (unsigned int i = PFLA_OP_MAXCHILD; i > 0; i--)
            if (n->child[i - 1])
                prop_infer (n->child[i - 1]);

    n->bit_dag = true;

    /* infer information on resulting columns */
    infer_ocol (n);
}

/**
 * Infer ocol property for a single node based on 
 * the schemas of its children
 */
void
PFprop_update_ocol (PFla_op_t *n) {
    infer_ocol (n);
}

/**
 * Infer ocol property for a DAG rooted in root
 */
void
PFprop_infer_ocol (PFla_op_t *root) {
    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
