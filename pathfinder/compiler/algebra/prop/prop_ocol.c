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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include pf_config.h first! */
#include "pf_config.h"
#include "pathfinder.h"
#include <assert.h>

#include "properties.h"
#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

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
 * Test if @a col is in the list of ocol columns of node @a n
 */
bool
PFprop_ocol (const PFla_op_t *n, PFalg_col_t col)
{
    assert (n);

    for (unsigned int i = 0; i < n->schema.count; i++)
        if (col == n->schema.items[i].name)
            return true;

    return false;
}

/**
 * Determine type of column @a col in schema @a schema. 
 */
PFalg_simple_type_t
PFprop_type_of_ (PFalg_schema_t schema, PFalg_col_t col)
{
    for (unsigned int i = 0; i < schema.count; i++)
        if (col == schema.items[i].name)
            return schema.items[i].type;

    /* you should never get there */
    PFoops (OOPS_FATAL,
                 "Type of %s not found in schema", PFcol_str (col));

    return aat_int; /* satisfy picky compilers */
}

/**
 * Return the type of @a col in the list of ocol columns
 */
PFalg_simple_type_t
PFprop_type_of (const PFla_op_t *n, PFalg_col_t col)
{
    assert (n);
    return PFprop_type_of_ (n->schema, col);
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
        case la_serialize_seq:
        case la_serialize_rel:
            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)));
            break;

        case la_side_effects:
            /* keep empty schema */
            break;

        /* only a rewrite can change the ocol property
           - thus update schema (property) during rewrite */
        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            break;

        case la_attach:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.attach.res;
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
            PFarray_t *proj_list = PFarray (sizeof (PFalg_proj_t),
                                            n->sem.proj.count);

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
        case la_pos_select:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;

        case la_disjunion:
        {
            unsigned int  i, j;

            /* see if both operands have same number of columns */
            if (ocols_count (L(n)) != ocols_count (R(n)))
                PFoops (OOPS_FATAL,
                        "Schema of two arguments of UNION does not match");

            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));

            /* combine types of the both arguments */
            for (i = 0; i < ocols_count (n); i++) {
                for (j = 0; j < ocols_count (R(n)); j++)
                    if ((ocol_at (n, i)).name == (ocol_at (R(n), j)).name) {
                        /* The two columns match, so include their name
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

            /* see if both operands have same number of columns */
            if (ocols_count (L(n)) != ocols_count (R(n)))
                PFoops (OOPS_FATAL,
                        "Schema of two arguments of INTERSECTION does not match");

            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));

            /* combine types of the both arguments */
            for (i = 0; i < ocols_count (n); i++) {
                for (j = 0; j < ocols_count (R(n)); j++)
                    if ((ocol_at (n, i)).name == (ocol_at (R(n), j)).name) {
                        /* The two columns match, so include their name
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
            unsigned int        i, j, ix[clsize (n->sem.fun_1to1.refs)];
            PFalg_simple_type_t res_type = 0;

            /* verify that the referenced columns in refs
               are really columns of n ... */
            for (i = 0; i < clsize (n->sem.fun_1to1.refs); i++) {
                for (j = 0; j < ocols_count (L(n)); j++)
                    if (ocol_at (L(n), j).name == clat (n->sem.fun_1to1.refs, i))
                        break;
                if (j == ocols_count (L(n)))
                    PFoops (OOPS_FATAL,
                            "column `%s' referenced in generic function"
                            " operator not found",
                            PFcol_str (clat (n->sem.fun_1to1.refs, i)));
                ix[i] = j;
            }

            /* we want to perform some more consistency checks
               that are specific to certain operators */
            switch (n->sem.fun_1to1.kind) {
                /**
                 * Depending on the @a kind parameter, we add, subtract,
                 * multiply, or divide the two values of columns @a col1
                 * and @a col2 and store the result in newly created column
                 * @a res. @a res gets the same data type as @a col1 and
                 * @a col2. The result schema corresponds to the schema
                 * of the input relation @a n plus @a res.
                 */
                case alg_fun_num_add:
                case alg_fun_num_subtract:
                case alg_fun_num_multiply:
                case alg_fun_num_divide:
                case alg_fun_num_modulo:
                    assert (clsize (n->sem.fun_1to1.refs) == 2);
                    /* make sure both columns are of the same numeric type */
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
                case alg_fun_pf_log:
                case alg_fun_pf_sqrt:
                case alg_fun_fn_round:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure the column is of numeric type */
                    assert (ocol_at (L(n), ix[0]).type == aat_int ||
                            ocol_at (L(n), ix[0]).type == aat_dec ||
                            ocol_at (L(n), ix[0]).type == aat_dbl);

                    res_type = ocol_at (L(n), ix[0]).type;
                    break;

                case alg_fun_fn_substring:
                    assert (clsize (n->sem.fun_1to1.refs) == 2);

                    /* make sure both columns are of type str & dbl */
                    assert (ocol_at (L(n), ix[0]).type == aat_str);
                    assert (ocol_at (L(n), ix[1]).type == aat_dbl);

                    res_type = aat_str;
                    break;

                case alg_fun_fn_substring_dbl:
                    assert (clsize (n->sem.fun_1to1.refs) == 3);
                    /* make sure both columns are of type str & dbl */
                    assert (ocol_at (L(n), ix[0]).type == aat_str);
                    assert (ocol_at (L(n), ix[1]).type == aat_dbl &&
                            ocol_at (L(n), ix[2]).type == aat_dbl);

                    res_type = aat_str;
                    break;

                case alg_fun_fn_concat:
                case alg_fun_fn_substring_before:
                case alg_fun_fn_substring_after:
                    assert (clsize (n->sem.fun_1to1.refs) == 2);
                    /* make sure both columns are of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str &&
                            ocol_at (L(n), ix[1]).type == aat_str);

                    res_type = aat_str;
                    break;

                case alg_fun_fn_string_length:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure the column is of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str);

                    res_type = aat_int;
                    break;

                case alg_fun_fn_normalize_space:
                case alg_fun_fn_upper_case:
                case alg_fun_fn_lower_case:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure the column is of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str);

                    res_type = aat_str;
                    break;

                case alg_fun_fn_contains:
                case alg_fun_fn_starts_with:
                case alg_fun_fn_ends_with:
                case alg_fun_fn_matches:
                    assert (clsize (n->sem.fun_1to1.refs) == 2);
                    /* make sure both columns are of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str &&
                            ocol_at (L(n), ix[1]).type == aat_str);

                    res_type = aat_bln;
                    break;

#ifdef HAVE_GEOXML
                case alg_fun_geo_wkb:
                case alg_fun_geo_point:
                    res_type = aat_wkb;
                    break;
                case alg_fun_geo_distance:
                    res_type = aat_dbl;
                    break;
                case alg_fun_geo_relate:
                    res_type = aat_bln;
                    break;
                case alg_fun_geo_geometry:
                case alg_fun_geo_intersection:
                    res_type = aat_wkb;
                    break;
#endif

                case alg_fun_fn_matches_flag:
                    assert (clsize (n->sem.fun_1to1.refs) == 3);
                    /* make sure all columns are of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str &&
                            ocol_at (L(n), ix[1]).type == aat_str &&
                            ocol_at (L(n), ix[2]).type == aat_str);

                    res_type = aat_bln;
                    break;

                case alg_fun_fn_translate:
                case alg_fun_fn_replace:
                    assert (clsize (n->sem.fun_1to1.refs) == 3);
                    /* make sure all columns are of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str &&
                            ocol_at (L(n), ix[1]).type == aat_str &&
                            ocol_at (L(n), ix[2]).type == aat_str);

                    res_type = aat_str;
                    break;

                case alg_fun_fn_replace_flag:
                    assert (clsize (n->sem.fun_1to1.refs) == 4);
                    /* make sure all columns are of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str &&
                            ocol_at (L(n), ix[1]).type == aat_str &&
                            ocol_at (L(n), ix[2]).type == aat_str &&
                            ocol_at (L(n), ix[3]).type == aat_str);

                    res_type = aat_str;
                    break;

                case alg_fun_fn_name:
                case alg_fun_fn_local_name:
                case alg_fun_fn_namespace_uri:
                    assert(clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure column is of type node */
                    assert (ocol_at (L(n), ix[0]).type & aat_node);

                    res_type = aat_str;
                    break;

                case alg_fun_fn_number:
                case alg_fun_fn_number_lax:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    res_type = aat_dbl;
                    break;

                case alg_fun_fn_qname:
                    assert (clsize (n->sem.fun_1to1.refs) == 2);
                    /* make sure both columns are of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str &&
                            ocol_at (L(n), ix[1]).type == aat_str);

                    res_type = aat_qname;
                    break;

                case alg_fun_fn_doc_available:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure columns is of type string */
                    assert (ocol_at (L(n), ix[0]).type == aat_str);

                    res_type = aat_bln;
                    break;

                case alg_fun_pf_fragment:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure both columns are of type string */
                    assert (ocol_at (L(n), ix[0]).type & aat_node);

                    res_type = aat_pnode;
                    break;

                case alg_fun_pf_supernode:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure both columns are of type string */
                    assert (ocol_at (L(n), ix[0]).type & aat_node);

                    res_type = ocol_at (L(n), ix[0]).type;
                    break;

                case alg_fun_pf_add_doc_str:
                    assert(clsize (n->sem.fun_1to1.refs) == 3);

                    /* make sure cols are of the correct type */
                    assert(ocol_at (L(n), ix[0]).type == aat_str);
                    assert(ocol_at (L(n), ix[1]).type == aat_str);
                    assert(ocol_at (L(n), ix[2]).type == aat_str);

                    /* the returning type of doc management functions
                     * is aat_docmgmt bitwise OR the column types*/
                    res_type = aat_docmgmt | aat_path | aat_docnm | aat_colnm;
                    break;

                case alg_fun_pf_add_doc_str_int:
                    assert(clsize (n->sem.fun_1to1.refs) == 4);

                    /* make sure cols are of the correct type */
                    assert(ocol_at (L(n), ix[0]).type == aat_str);
                    assert(ocol_at (L(n), ix[1]).type == aat_str);
                    assert(ocol_at (L(n), ix[2]).type == aat_str);
                    assert(ocol_at (L(n), ix[3]).type == aat_int);

                    /* the returning type of doc management functions
                     * is aat_docmgmt bitwise OR the column types */
                    res_type = aat_docmgmt | aat_path | aat_docnm | aat_colnm;
                    break;

                case alg_fun_pf_del_doc:
                    assert(clsize (n->sem.fun_1to1.refs) == 1);

                    /* make sure cols are of the correct type */
                    assert(ocol_at (L(n), ix[0]).type == aat_str);

                    /* the returning type of doc management functions
                     * is aat_docmgmt bitwise OR the column types */
                    res_type = aat_docmgmt | aat_docnm;
                    break;

                case alg_fun_pf_nid:
                    assert(clsize (n->sem.fun_1to1.refs) == 1);

                    /* make sure cols are of the correct type */
                    assert(ocol_at (L(n), ix[0]).type == aat_pnode);

                    res_type = aat_str;
                    break;

                case alg_fun_pf_docname:
                    assert(clsize (n->sem.fun_1to1.refs) == 1);

                    /* make sure cols are of the correct type */
                    assert(ocol_at (L(n), ix[0]).type & aat_node);

                    res_type = aat_str;
                    break;

                case alg_fun_upd_delete:
                    assert(clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure that the column is a node */
                    assert(ocol_at (L(n), ix[0]).type & aat_node);

                    /* the result type is aat_update bitwise OR the type of
                    the target node shifted 4 bits to the left */
                    assert((ocol_at (L(n), ix[0]).type << 4) & aat_node1);
                    res_type = aat_update | (ocol_at (L(n), ix[0]).type << 4);
                    break;

                case alg_fun_fn_year_from_datetime:
                case alg_fun_fn_month_from_datetime:
                case alg_fun_fn_day_from_datetime:
                case alg_fun_fn_hours_from_datetime:
                case alg_fun_fn_minutes_from_datetime:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure column is of type datetime */
                    assert (ocol_at (L(n), ix[0]).type == aat_dtime);

                    res_type = aat_int;
                    break;

                case alg_fun_fn_seconds_from_datetime:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure column is of type datetime */
                    assert (ocol_at (L(n), ix[0]).type == aat_dtime);

                    res_type = aat_dec;
                    break;

                case alg_fun_fn_year_from_date:
                case alg_fun_fn_month_from_date:
                case alg_fun_fn_day_from_date:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure column is of type date */
                    assert (ocol_at (L(n), ix[0]).type == aat_date);

                    res_type = aat_int;
                    break;

                case alg_fun_fn_hours_from_time:
                case alg_fun_fn_minutes_from_time:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure column is of type time */
                    assert (ocol_at (L(n), ix[0]).type == aat_time);

                    res_type = aat_int;
                    break;

                case alg_fun_fn_seconds_from_time:
                    assert (clsize (n->sem.fun_1to1.refs) == 1);
                    /* make sure column is of type time */
                    assert (ocol_at (L(n), ix[0]).type == aat_time);

                    res_type = aat_dec;
                    break;

                case alg_fun_add_dur:
                case alg_fun_subtract_dur:
                case alg_fun_multiply_dur:
                case alg_fun_divide_dur:
                    break;

                case alg_fun_upd_rename:
                case alg_fun_upd_insert_into_as_first:
                case alg_fun_upd_insert_into_as_last:
                case alg_fun_upd_insert_before:
                case alg_fun_upd_insert_after:
                case alg_fun_upd_replace_value_att:
                case alg_fun_upd_replace_value:
                case alg_fun_upd_replace_element:
                case alg_fun_upd_replace_node:
                    assert(clsize (n->sem.fun_1to1.refs) == 2);

                    /* make some assertions according to the fun signature */
                    switch (n->sem.fun_1to1.kind) {
                        case alg_fun_upd_rename:
                            assert(ocol_at (L(n), ix[0]).type & aat_node);
                            assert(ocol_at (L(n), ix[1]).type & aat_qname);
                            assert((ocol_at (L(n), ix[0]).type << 4) &
                                   aat_node1);
                            break;
                        case alg_fun_upd_insert_into_as_first:
                        case alg_fun_upd_insert_into_as_last:
                        case alg_fun_upd_insert_before:
                        case alg_fun_upd_insert_after:
                        case alg_fun_upd_replace_node:
                            assert(ocol_at (L(n), ix[0]).type & aat_node);
                            assert(ocol_at (L(n), ix[1]).type & aat_node);
                            assert((ocol_at (L(n), ix[0]).type << 4) &
                                   aat_node1);
                            break;
                        case alg_fun_upd_replace_value_att:
                            assert(ocol_at (L(n), ix[0]).type & aat_anode);
                            assert(ocol_at (L(n), ix[1]).type == aat_uA);
                            assert((ocol_at (L(n), ix[0]).type << 4) &
                                   aat_anode1);
                            break;
                        case alg_fun_upd_replace_value:
                            assert(ocol_at (L(n), ix[0]).type & aat_pnode);
                            assert(ocol_at (L(n), ix[1]).type == aat_uA);
                            assert((ocol_at (L(n), ix[0]).type << 4) &
                                   aat_pnode1);
                            break;
                        case alg_fun_upd_replace_element:
                            assert(ocol_at (L(n), ix[0]).type & aat_pnode);
                            assert(ocol_at (L(n), ix[1]).type & aat_pnode);
                            assert((ocol_at (L(n), ix[0]).type << 4) &
                                   aat_pnode1);
                            break;
                        default: assert(!"should never reach here"); break;
                    }

                    /* the result type is aat_update bitwise OR the type of
                       the target_node shifted 4 bits to the left bitwise OR
                       the type of the second argument */
                    res_type = aat_update | (ocol_at (L(n), ix[0]).type << 4)
                                          |  ocol_at (L(n), ix[1]).type;
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

        case la_to:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.binary.res;
            ocol_at (n, ocols_count (n)).type = aat_int;
            ocols_count (n)++;
            break;

        case la_aggr:
            /* set number of schema items in the result schema:
             * result columns plus partitioning column
             * (if available -- constant optimizations may
             *  have removed it).
             */
            new_ocols (n, n->sem.aggr.count + (n->sem.aggr.part ? 1 : 0));

            /* verify that columns 'col' and 'part' are columns of n
             * and include them into the result schema
             */
            for (unsigned int i = 0; i < n->sem.aggr.count; i++) {
                PFalg_col_t col = n->sem.aggr.aggr[i].col;
                if (col) {
                    if (!PFprop_ocol (L(n), col))
                        PFoops (OOPS_FATAL,
                                "column `%s' referenced in aggregate not found",
                                PFcol_str (col));
                    ocol_at (n, i).type = PFprop_type_of (L(n), col);
                }
                else
                    ocol_at (n, i).type = aat_int;

                ocol_at (n, i).name = n->sem.aggr.aggr[i].res;
            }
            if (n->sem.aggr.part) {
                unsigned int i    = n->sem.aggr.count;
                PFalg_col_t  part = n->sem.aggr.part;
                if (!PFprop_ocol (L(n), part))
                    PFoops (OOPS_FATAL,
                            "column `%s' referenced in aggregate not found",
                            PFcol_str (part));

                ocol_at (n, i).type = PFprop_type_of (L(n), part);
                ocol_at (n, i).name = part;
            }
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.sort.res;
            ocol_at (n, ocols_count (n)).type = aat_nat;
            ocols_count (n)++;
            break;

        case la_rowid:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.rowid.res;
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
                if (n->sem.type.col == ocol_at (L(n), i).name)
                {
                    ocol_at (n, i).name = n->sem.type.col;
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

        case la_step:
        case la_guide_step:
#ifndef NDEBUG
            /* verify schema of 'n' */
            for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                if (R(n)->schema.items[i].name == n->sem.step.iter
                 || R(n)->schema.items[i].name == n->sem.step.item)
                    continue;
                else
                    PFoops (OOPS_FATAL,
                            "illegal column `%s' in path step",
                            PFcol_str (R(n)->schema.items[i].name));
            }
            if (!(PFprop_type_of (R(n), n->sem.step.item) & aat_node))
                PFoops (OOPS_FATAL,
                        "wrong item type '0x%X' in the input of a path step",
                        PFprop_type_of (R(n), n->sem.step.item));
#endif
        {
            PFalg_simple_type_t ty;

            new_ocols (n, 2);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.step.iter,
                                        .type = PFprop_type_of (
                                                    R(n),
                                                    n->sem.step.iter) };

            if (n->sem.step.spec.axis == alg_attr)
                ty = aat_anode;
            else if (n->sem.step.spec.axis == alg_anc_s)
                ty = PFprop_type_of (R(n), n->sem.step.item) | aat_pnode;
            else if (n->sem.step.spec.axis == alg_desc_s ||
                     n->sem.step.spec.axis == alg_self)
                ty = PFprop_type_of (R(n), n->sem.step.item);
            else
                ty = aat_pnode;

            ocol_at (n, 1) = (PFalg_schm_item_t) { .name = n->sem.step.item_res,
                                                   .type = ty };
        }   break;

        case la_step_join:
        case la_guide_step_join:
#ifndef NDEBUG
            if (PFprop_ocol (R(n), n->sem.step.item_res))
                PFoops (OOPS_FATAL,
                        "illegal column `%s' in the input "
                        "of a path step",
                        PFcol_str (n->sem.step.item_res));
            if (!PFprop_ocol (R(n), n->sem.step.item))
                PFoops (OOPS_FATAL,
                        "column `%s' needed in path step is missing",
                        PFcol_str (n->sem.step.item));
            if (!(PFprop_type_of (R(n), n->sem.step.item) & aat_node))
                PFoops (OOPS_FATAL,
                        "wrong item type '0x%X' in the input of a path step",
                        PFprop_type_of (R(n), n->sem.step.item));
#endif
        {
            PFalg_simple_type_t ty;

            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)) + 1);
            if (n->sem.step.spec.axis == alg_attr)
                ty = aat_anode;
            else if (n->sem.step.spec.axis == alg_anc_s)
                ty = PFprop_type_of (R(n), n->sem.step.item) | aat_pnode;
            else if (n->sem.step.spec.axis == alg_desc_s ||
                     n->sem.step.spec.axis == alg_self)
                ty = PFprop_type_of (R(n), n->sem.step.item);
            else
                ty = aat_pnode;

            ocol_at (n, ocols_count (n))
                = (PFalg_schm_item_t) { .name = n->sem.step.item_res,
                                        .type = ty };
            ocols_count (n)++;
        }   break;

        case la_doc_index_join:
#ifndef NDEBUG
            if (PFprop_ocol (R(n), n->sem.doc_join.item_res))
                PFoops (OOPS_FATAL,
                        "illegal column `%s' in the input "
                        "of a doc index join",
                        PFcol_str (n->sem.doc_join.item_res));
            if (!PFprop_ocol (R(n), n->sem.doc_join.item))
                PFoops (OOPS_FATAL,
                        "column `%s' needed in doc index join is missing",
                        PFcol_str (n->sem.doc_join.item));
            if (!(PFprop_type_of (R(n), n->sem.doc_join.item) == aat_str))
                PFoops (OOPS_FATAL,
                        "wrong item type '0x%X' in the input of doc index join",
                        PFprop_type_of (R(n), n->sem.doc_join.item));
            if (!PFprop_ocol (R(n), n->sem.doc_join.item_doc))
                PFoops (OOPS_FATAL,
                        "column `%s' needed in doc index join is missing",
                        PFcol_str (n->sem.doc_join.item_doc));
            if (!(PFprop_type_of (R(n), n->sem.doc_join.item_doc) & aat_node))
                PFoops (OOPS_FATAL,
                        "wrong doc item type '0x%X' in the input "
                        "of doc index join",
                        PFprop_type_of (R(n), n->sem.doc_join.item_doc));
#endif
            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)) + 1);
            ocol_at (n, ocols_count (n))
                = (PFalg_schm_item_t) { .name = n->sem.doc_join.item_res,
                                        .type = aat_pnode };
            ocols_count (n)++;
            break;

        case la_doc_access:
            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.doc_access.res;
            ocol_at (n, ocols_count (n)).type = aat_str;
            ocols_count (n)++;
            break;

        case la_doc_tbl:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)) + 1);
            ocol_at (n, ocols_count (n)).name = n->sem.doc_tbl.res;
            ocol_at (n, ocols_count (n)).type = aat_pnode;
            ocols_count (n)++;
            break;

        case la_twig:
        {
            PFalg_simple_type_t iter_type = 0;

            if (L(n)->kind == la_attribute ||
                L(n)->kind == la_processi)
                iter_type = PFprop_type_of (
                                L(L(n)),
                                L(n)->sem.iter_item1_item2.iter);
            else if (L(n)->kind == la_content)
                iter_type = PFprop_type_of (
                                R(L(n)),
                                L(n)->sem.iter_item.iter);
            else if (L(n)->kind == la_docnode)
                iter_type = PFprop_type_of (
                                L(L(n)),
                                L(n)->sem.docnode.iter);
            else
                iter_type = PFprop_type_of (
                                L(L(n)),
                                L(n)->sem.iter_item.iter);

            new_ocols (n, 2);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.iter_item.iter,
                                        .type = iter_type };

            /* Check if the twig consists of only attributes ... */
            if (L(n)->kind == la_attribute ||
                (L(n)->kind == la_content &&
                 PFprop_type_of (L(n)->child[1],
                                 L(n)->sem.iter_pos_item.item) == aat_anode))
                ocol_at (n, 1)
                    = (PFalg_schm_item_t) { .name = n->sem.iter_item.item,
                                            .type = aat_anode };
            /* ... attributes and other nodes ... */
            else if (L(n)->kind == la_content &&
                     PFprop_type_of (L(n)->child[1],
                                     L(n)->sem.iter_pos_item.item) & aat_anode)
                ocol_at (n, 1)
                    = (PFalg_schm_item_t) { .name = n->sem.iter_item.item,
                                            .type = aat_node };
            /* ... or only other nodes (without attributes). */
            else
                ocol_at (n, 1)
                    = (PFalg_schm_item_t) { .name = n->sem.iter_item.item,
                                            .type = aat_pnode };
        }   break;

        /* operators without schema */
        case la_fcns:
        case la_element:
        case la_docnode:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
            /* keep empty schema */
            break;

        /* operator with static iter|pos|item schema */
        case la_merge_adjacent:
            new_ocols (n, 3);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.merge_adjacent.iter_res,
                                        .type = PFprop_type_of (
                                                    R(n),
                                                    n->sem.merge_adjacent
                                                          .iter_in) };
            ocol_at (n, 1)
                = (PFalg_schm_item_t) { .name = n->sem.merge_adjacent.pos_res,
                                        .type = PFprop_type_of (
                                                    R(n),
                                                    n->sem.merge_adjacent
                                                          .pos_in) };
            ocol_at (n, 2)
                = (PFalg_schm_item_t) { .name = n->sem.merge_adjacent.item_res,
                                        .type = PFprop_type_of (
                                                    R(n),
                                                    n->sem.merge_adjacent
                                                          .item_in) };
          break;

        case la_roots:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;

        /* operators without schema */
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_fun_frag_param:
            /* keep empty schema */
            break;

        case la_error:
        case la_cache:
            ocols (n) = copy_ocols (ocols (R(n)), ocols_count (R(n)));
            break;

        case la_trace:
            /* trace does not have a schema */
            break;

        case la_trace_items:
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

            /* see if both operands have same number of columns */
            if (ocols_count (L(n)) != ocols_count (R(n)) ||
                ocols_count (L(n)) != ocols_count (n->sem.rec_arg.base))
                PFoops (OOPS_FATAL,
                        "Schema of the arguments of recursion "
                        "argument to not match");

            /* see if we find each column in all of the input relations */
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

        /* only a rewrite can change the ocol property
           - thus update schema (property) during rewrite */
        case la_fun_call:
            break;

        case la_fun_param:
        {
            unsigned int  i, j;

            /* see if we find each column in all of the input relations */
            for (i = 0; i < ocols_count (L(n)); i++) {
                for (j = 0; j < ocols_count (n); j++)
                    if (ocol_at (L(n), i).name == ocol_at (n, j).name) {
                        /* the type may have changed */
                        ocol_at (n, j).type = ocol_at (L(n), i).type;
                        break;
                    }

                if (j == ocols_count (n))
                    PFoops (OOPS_FATAL,
                            "Schema of the arguments of function application "
                            "argument to not match");
            }

            /* keep the schema of its inputs */
        } break;

        case la_proxy:
        case la_proxy_base:
            ocols (n) = copy_ocols (ocols (L(n)), ocols_count (L(n)));
            break;

        case la_string_join:
            new_ocols (n, 2);

            ocol_at (n, 0)
                = (PFalg_schm_item_t) { .name = n->sem.string_join.iter_res,
                                        .type = PFprop_type_of (
                                                    R(n),
                                                    n->sem.string_join
                                                          .iter_sep) };
            ocol_at (n, 1)
                = (PFalg_schm_item_t) { .name = n->sem.string_join.item_res,
                                        .type = aat_str };
            break;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");

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
            prop_infer_rec_seed (LR(n));
            prop_infer (LL(n));
            prop_infer_rec_body (LR(n));
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
