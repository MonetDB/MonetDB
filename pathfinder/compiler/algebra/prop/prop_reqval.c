/**
 * @file
 *
 * Inference of required value property of logical algebra expressions.
 *
 * For each used column we record in which setting it is used. The most
 * general information is that the column is used as ``value'' column.
 *
 * More specific usage information is:
 *
 * - unique:    The column is only consumed as an uniqueness criterion
 *              during duplicate elimination.
 *
 * - order:     The column is only consumed as an order criterion.
 *
 * - multi-col: The column can be split into two columns as it is used
 *              only inside mapping joins or as order criterion.
 *
 * - bijective: The column can be replaced by any column that provides
 *              a bijection (columns stem either from partition or join
 *              columns.
 *
 * - filter:    The column is a value column where we know that it is
 *              only used inside an equality predicate.
 *
 * - link:      The column is a value column where we know that it is
 *              only used as a link in a serialize_rel operator.
 *
 * - selection: The column is a value column where we inferred that it
 *              is a Boolean column where we are interested in the rows
 *              with either true or false values.
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
#include <stdio.h>

#include "properties.h"

#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/* store the number of incoming edges for each operator
   in the refctr field */
#define EDGE(n) ((n)->refctr)
/* store if an operator contains multiple input edges */
#define MULTIPLE_INPUT_EDGES(n) ((n)->bit_in)

/* required value property list */
struct req_val_t {
    PFalg_col_t col;       /* column name ... */

    /* ... and the corresponding property that tests if ... */

    bool        value;     /* ... its value is required */
    bool        join;      /* ... the column is used in equi-joins */
    bool        part;      /* ... the column is used as partition column */
    bool        order;     /* ... the column is used for ordering */
    bool        distinct;  /* ... the column is used for duplicate
                                  elimination */
    bool        filter;    /* ... its value is used as filter only */
    bool        link;      /* ... its value is used as link only */
    bool        sel_name;  /* ... only one boolean value is required */
    bool        sel_val;   /* ... together with the value of the boolean */
};
typedef struct req_val_t req_val_t;

#define MAP_LIST(n)       ((n)->prop->reqvals)
#define ADD(l,p)          (*((req_val_t *) PFarray_add ((l))) = (p))
#define MAP_AT(n,i)       (*((req_val_t *) PFarray_at ((n), (i))))

#define COL_AT(n,i)       (((req_val_t *) PFarray_at ((n), (i)))->col)
#define VAL_AT(n,i)       (((req_val_t *) PFarray_at ((n), (i)))->value)
#define JOIN_AT(n,i)      (((req_val_t *) PFarray_at ((n), (i)))->join)
#define PART_AT(n,i)      (((req_val_t *) PFarray_at ((n), (i)))->part)
#define ORD_AT(n,i)       (((req_val_t *) PFarray_at ((n), (i)))->order)
#define DIST_AT(n,i)      (((req_val_t *) PFarray_at ((n), (i)))->distinct)
#define FILTER_AT(n,i)    (((req_val_t *) PFarray_at ((n), (i)))->filter)
#define LINK_AT(n,i)      (((req_val_t *) PFarray_at ((n), (i)))->link)
#define SNAME_AT(n,i)     (((req_val_t *) PFarray_at ((n), (i)))->sel_name)
#define SVAL_AT(n,i)      (((req_val_t *) PFarray_at ((n), (i)))->sel_val)

/**
 * @brief look up the property mapping (in @a l) for a given column @a col.
 */
static req_val_t *
find_map (PFarray_t *l, PFalg_col_t col)
{
    if (!l)
        return NULL;

    for (unsigned int i = 0; i < PFarray_last (l); i++) {
        if (col == COL_AT(l, i))
            return (req_val_t *) PFarray_at (l, i);
    }
    return NULL;
}

/**
 * Test if @a col is in the list of required value columns
 * in container @a prop
 */
bool
PFprop_req_bool_val (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else
        return reqval->sel_name;
}

/**
 * Looking up required value of column @a col
 * in container @a prop
 */
bool
PFprop_req_bool_val_val (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else if (reqval->sel_name == false) {
        PFoops (OOPS_FATAL,
                "cannot look up required value property");
        return false; /* dummy return */
    }
    else
        return reqval->sel_val;
}

/**
 * Test if @a col is in the list of filter columns
 * in container @a prop
 */
bool
PFprop_req_filter_col (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else
        /* only value columns can be filter columns */
        return reqval->value && reqval->filter;
}

/**
 * Test if @a col is in the list of link columns
 * (columns only used in the iter column of operator serialize_rel)
 * in container @a prop
 */
bool
PFprop_req_link_col (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else
        /* only value columns can be link columns */
        return reqval->value && reqval->link;
}

/**
 * Test if @a col is in the list of value columns
 * in container @a prop
 */
bool
PFprop_req_value_col (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        /* in case we have no information
           we assume its a value column */
        return true;
    else
        return reqval->value;
}

/**
 * Test if @a col is in the list of order columns
 * in container @a prop
 */
bool
PFprop_req_order_col (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else
        /* column col is only used inside ordering operators */
        return reqval->order &&
               !reqval->part && !reqval->join && !reqval->value;
}

/**
 * Test if @a col is in the list of bijective columns
 * in container @a prop
 */
bool
PFprop_req_bijective_col (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else
        /* column col is only used inside equi-joins or as
           partition column */
        return (reqval->part || reqval->join) &&
               !reqval->distinct && !reqval->order && !reqval->value;
}

/**
 * Test if @a col is in the list of rank columns
 * in container @a prop
 */
bool
PFprop_req_rank_col (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else
        /* column col is only used inside partition, order,
           or distinct columns */
        return (reqval->part || reqval->distinct || reqval->order) &&
               !reqval->join && !reqval->value;
}

/**
 * Test if @a col may be represented by multiple columns
 */
bool
PFprop_req_multi_col_col (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else
        /* column col is only used inside equi-joins or
           ordering operators */
        return (reqval->join || reqval->order || reqval->distinct) &&
               !reqval->part && !reqval->value;
}

/**
 * Test if @a col may be represented by something that maintains
 * the same duplicates.
 */
bool
PFprop_req_unique_col (const PFprop_t *prop, PFalg_col_t col)
{
    req_val_t *reqval = find_map (prop->reqvals, col);

    if (!reqval)
        return false;
    else
        /* column col is only used inside equi-joins or
           ordering operators */
        return (reqval->distinct) &&
               !reqval->join && !reqval->order &&
               !reqval->part && !reqval->value;
}

/* define possible inputs for adjust_map and adjust_map_ */
#define NO  0
#define YES 1
#define KEEP 2 /* keep-old-result-value flag */
/**
 * @brief look up the property mapping (in @a l) for a given column @a col.
 */
static void
adjust_map_ (PFarray_t *map_list, PFalg_col_t col,
             char value, char join, char part, char order, char distinct,
             char filter, char link, char sel_name, char sel_val)
{
    req_val_t *map;

    /* lookup the mapping (if present) */
    map = find_map (map_list, col);

    /* add a new mapping */
    if (!map) {
        *((req_val_t *) PFarray_add (map_list))
            = (req_val_t) { .col      = col,
                            .value    = (value    == YES),
                            .join     = (join     == YES),
                            .part     = (part     == YES),
                            .order    = (order    == YES),
                            .distinct = (distinct == YES),
                            .filter   = (filter   != NO),
                            .link     = (link     != NO),
                            /* we mark the selection mapping
                               with (name=false, val=true) in
                               case we don't know anything about it */
                            .sel_name = (sel_name == YES),
                            .sel_val  = (sel_val  != NO) };
    }
    /* modify already existing mapping */
    else {
        if (value    != KEEP) map->value    |= (value    == YES);
        if (join     != KEEP) map->join     |= (join     == YES);
        if (part     != KEEP) map->part     |= (part     == YES);
        if (order    != KEEP) map->order    |= (order    == YES);
        if (distinct != KEEP) map->distinct |= (distinct == YES);
        if (filter   != KEEP) map->filter   &= (filter   == YES);
        if (link     != KEEP) map->link     &= (link     == YES);
        if (sel_name != KEEP) {
            assert (sel_val != KEEP);
            /* if we don't know anything about the selection
               mapping yet we replace it */
            if (!map->sel_name && map->sel_val) {
                map->sel_name = (sel_name == YES);
                map->sel_val  = (sel_val  == YES);
            }
            /* the selection mapping conflicts -- we should not keep one */
            else if ((map->sel_name && sel_name == NO) ||
                     (!map->sel_name && sel_name == YES)) {
                map->sel_name = false;
                map->sel_val  = false;
            }
            else /* map->sel_name == (sel_name == YES) */ {
                map->sel_val &= (sel_val == YES);
            }
        }
    }
}
#define adjust_value_(list,col)   \
        adjust_map_ ((list), (col), YES,  NO,   NO,   NO,   NO,   NO,   NO,   NO,   NO)
#define adjust_join_(list,col)    \
        adjust_map_ ((list), (col), KEEP, YES,  KEEP, KEEP, NO,   KEEP, KEEP, KEEP, KEEP)
#define adjust_part_(list,col)    \
        adjust_map_ ((list), (col), KEEP, KEEP, YES,  KEEP, NO,   KEEP, KEEP, KEEP, KEEP)
#define adjust_order_(list,col)   \
        adjust_map_ ((list), (col), KEEP, KEEP, KEEP, YES,  NO,   KEEP, KEEP, KEEP, KEEP)
#define adjust_distinct_(list,col)\
        adjust_map_ ((list), (col), KEEP, KEEP, KEEP, KEEP, YES,  KEEP, KEEP, KEEP, KEEP)
#define adjust_filter_(list,col)  \
        adjust_map_ ((list), (col), YES,  NO,   NO,   NO,   NO,   YES,  NO,   KEEP, KEEP)
#define adjust_link_(list,col)    \
        adjust_map_ ((list), (col), YES,  NO,   NO,   NO,   NO,   NO,   YES,  KEEP, KEEP)
#define adjust_sel_(list,col)     \
        adjust_map_ ((list), (col), YES,  NO,   NO,   NO,   NO,   KEEP, KEEP, YES,  YES)
#define adjust_nosel_(list,col)   \
        adjust_map_ ((list), (col), YES,  NO,   NO,   NO,   NO,   KEEP, KEEP, YES,  NO)

static void
adjust_map (PFla_op_t *n, PFalg_col_t col,
            char value, char join, char part, char order, char distinct,
            char filter, char link, char sel_name, char sel_val)
{
    assert (n);

    /* create a new mapping list if not already available */
    if (!MAP_LIST(n))
        MAP_LIST(n) = PFarray (sizeof (req_val_t),
                               n->schema.count);

    adjust_map_ (MAP_LIST(n), col,
                 value, join, part, order, distinct,
                 filter, link, sel_name, sel_val);
}
#define adjust_value(col)   \
        adjust_map (n, (col), YES,  NO,   NO,   NO,   NO,   NO,   NO,   NO,   NO)
#define adjust_join(col)    \
        adjust_map (n, (col), KEEP, YES,  KEEP, KEEP, NO,   KEEP, KEEP, KEEP, KEEP)
#define adjust_part(col)    \
        adjust_map (n, (col), KEEP, KEEP, YES,  KEEP, NO,   KEEP, KEEP, KEEP, KEEP)
#define adjust_order(col)   \
        adjust_map (n, (col), KEEP, KEEP, KEEP, YES,  NO,   KEEP, KEEP, KEEP, KEEP)
#define adjust_distinct(col)\
        adjust_map (n, (col), KEEP, KEEP, KEEP, KEEP, YES,  KEEP, KEEP, KEEP, KEEP)
#define adjust_filter(col)  \
        adjust_map (n, (col), YES,  NO,   NO,   NO,   NO,   YES,  NO,   KEEP, KEEP)
#define adjust_link(col)    \
        adjust_map (n, (col), YES,  NO,   NO,   NO,   NO,   NO,   YES,  KEEP, KEEP)
#define adjust_sel(col)     \
        adjust_map (n, (col), YES,  NO,   NO,   NO,   NO,   KEEP, KEEP, YES,  YES)
#define adjust_nosel(col)   \
        adjust_map (n, (col), YES,  NO,   NO,   NO,   NO,   KEEP, KEEP, YES,  NO)


/* short version to descend to fragment information */
#define prop_infer_reqvals_empty(n) prop_infer_reqvals((n), NULL)

/**
 * worker for PFprop_infer_reqval
 * infers the required values property during the second run
 * (uses edge counter from the first run)
 */
static void
prop_infer_reqvals (PFla_op_t *n, PFarray_t *reqvals)
{
    req_val_t *map;

    assert (n);

    /* initialize the mapping list if we need it
       and haven't got one already */
    if (!MAP_LIST(n) && reqvals && PFarray_last (reqvals) > 0)
        MAP_LIST(n) = PFarray (sizeof (req_val_t), n->schema.count);

    if (reqvals) {
        for (unsigned int i = 0; i < PFarray_last (reqvals); i++) {
            /* Only try to merge nodes visible at that node.
               (Non-matching columns may result from binary
                operators as e.g., the cross product.) */
            if (!PFprop_ocol (n, COL_AT(reqvals, i)))
                continue;

            /* lookup the mapping in the already existing mapping list */
            map = find_map (MAP_LIST(n), COL_AT(reqvals, i));

            /* modify the mapping */
            if (map) {
                map->value    |= VAL_AT(reqvals, i);
                map->join     |= JOIN_AT(reqvals, i);
                map->part     |= PART_AT(reqvals, i);
                map->order    |= ORD_AT(reqvals, i);
                map->distinct |= DIST_AT(reqvals, i);
                map->filter   &= FILTER_AT(reqvals, i);
                map->link     &= LINK_AT(reqvals, i);
                /* if we don't know anything about the selection
                   mapping yet we replace it */
                if (!map->sel_name && map->sel_val) {
                    map->sel_name = SNAME_AT(reqvals, i);
                    map->sel_val  = SVAL_AT(reqvals, i);
                }
                else if (map->sel_name && SNAME_AT(reqvals, i)) {
                    if (map->sel_val != SVAL_AT(reqvals, i)) {
                        map->sel_name = false;
                        map->sel_val  = false;
                    }
                    /* else we look for the column in both parents
                       and the value is the same */
                } else {
                    map->sel_name = false;
                    map->sel_val  = false;
                }
            }
            else
                ADD(MAP_LIST(n), MAP_AT(reqvals, i));
        }
    }

    /* nothing to do if we haven't collected
       all incoming required values lists of that node */
    if (EDGE(n) > 1) {
        EDGE(n)--;
        return;
    }

    /* In case there are multiple incoming edges we are conservative
       and do not allow required Boolean values to be inferred.

       This check is necessary to make sure that the rewrites don't
       change the cardinality for unaffected branches (e.g. a difference
       operator referencing the result of a Boolean expression) and 
       also do not change the cardinality if a consuming operator works
       on the complete input. */
    if (MAP_LIST(n) &&
        (MULTIPLE_INPUT_EDGES(n) ||
         n->kind == la_pos_select ||
         n->kind == la_aggr ||
         n->kind == la_rownum ||
         n->kind == la_rowrank ||
         n->kind == la_twig ||
         n->kind == la_rec_fix ||
         n->kind == la_fun_call ||
         n->kind == la_string_join))
        for (unsigned int i = 0; i < PFarray_last (MAP_LIST(n)); i++) {
            map = (req_val_t *) PFarray_at (MAP_LIST(n), i);
            map->sel_name = false;
            map->sel_val  = false;
            map->filter   = false;
        }

    /* Adjust the properties of the algebra operators based
       on the current mapping list (MAP_LIST(n)). The adjust_*()
       macros add the respective usage information to the
       mapping list. */
    switch (n->kind) {
        case la_serialize_seq:
            adjust_order (n->sem.ser_seq.pos);
            adjust_value (n->sem.ser_seq.item);

            prop_infer_reqvals_empty (L(n)); /* side effects */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_serialize_rel:
            adjust_link  (n->sem.ser_rel.iter);
            adjust_order (n->sem.ser_rel.pos);

            for (unsigned int i = 0; i < clsize (n->sem.ser_rel.items); i++)
                adjust_value (clat (n->sem.ser_rel.items, i));

            prop_infer_reqvals_empty (L(n)); /* side effects */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_side_effects:
            prop_infer_reqvals_empty (L(n)); /* side effects */
            prop_infer_reqvals_empty (R(n)); /* params */
            return; /* only infer once */


        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_empty_frag:
        case la_nil:
            /* leafs */
            break;

        case la_attach:
            /* no additional usage information */
            break;

        case la_cross:
            /* no additional usage information */
            break;

        case la_eqjoin:
            /* Check whether the join columns can be marked as mapping join
               columns or if they have to produce the exact values. */
            if ((PFprop_subdom (n->prop,
                                PFprop_dom_right (n->prop,
                                                  n->sem.eqjoin.col2),
                                PFprop_dom_left (n->prop,
                                                 n->sem.eqjoin.col1)) ||
                 PFprop_subdom (n->prop,
                                PFprop_dom_left (n->prop,
                                                 n->sem.eqjoin.col1),
                                PFprop_dom_right (n->prop,
                                                  n->sem.eqjoin.col2)))) {
                adjust_join (n->sem.eqjoin.col1);
                adjust_join (n->sem.eqjoin.col2);
            } else {
                adjust_value (n->sem.eqjoin.col1);
                adjust_value (n->sem.eqjoin.col2);
            }
            break;

        case la_semijoin:
        {
            PFarray_t *rmap = PFarray (sizeof (req_val_t), 1);
            /* Check whether the join columns can be marked as bijective value
               columns or if they have to produce the exact values. */
            if ((PFprop_subdom (n->prop,
                                PFprop_dom_right (n->prop,
                                                  n->sem.eqjoin.col2),
                                PFprop_dom_left (n->prop,
                                                 n->sem.eqjoin.col1)) ||
                 PFprop_subdom (n->prop,
                                PFprop_dom_left (n->prop,
                                                 n->sem.eqjoin.col1),
                                PFprop_dom_right (n->prop,
                                                  n->sem.eqjoin.col2)))) {
                adjust_join (n->sem.eqjoin.col1);
                adjust_join_ (rmap, n->sem.eqjoin.col2);
            } else {
                adjust_value (n->sem.eqjoin.col1);
                adjust_value_ (rmap, n->sem.eqjoin.col2);
            }

            prop_infer_reqvals (L(n), MAP_LIST(n));
            prop_infer_reqvals (R(n), rmap);
        }   return; /* only infer once */

        case la_thetajoin:
            /* add all join columns to the inferred icols */
            for (unsigned int i = 0; i < n->sem.thetajoin.count; i++) {
                PFalg_col_t left  = n->sem.thetajoin.pred[i].left,
                            right = n->sem.thetajoin.pred[i].right;

                if (n->sem.thetajoin.pred[i].comp == alg_comp_eq &&
                    (PFprop_subdom (n->prop,
                                    PFprop_dom_right (n->prop,
                                                      right),
                                    PFprop_dom_left (n->prop,
                                                     left)) ||
                     PFprop_subdom (n->prop,
                                    PFprop_dom_left (n->prop,
                                                     left),
                                    PFprop_dom_right (n->prop,
                                                      right)))) {
                    adjust_join (left);
                    adjust_join (right);
                } else if (n->sem.thetajoin.pred[i].comp == alg_comp_eq) {
                    adjust_filter (left);  /* also sets value flag */
                    adjust_filter (right); /* also sets value flag */
                } else {
                    adjust_value (left);
                    adjust_value (right);
                }
            }
            break;

        case la_project:
        {
            PFarray_t *new_map = PFarray (sizeof (req_val_t),
                                          n->sem.proj.count);

            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                map = find_map (MAP_LIST(n), n->sem.proj.items[i].new);
                if (map) {
                    req_val_t map_item = *map;
                    map_item.col = n->sem.proj.items[i].old;
                    ADD(new_map, map_item);
                }
            }

            prop_infer_reqvals (L(n), new_map);
        }   return; /* only infer once */

        case la_select:
            /* introduce new required value column */
            adjust_sel (n->sem.select.col);

            /* overrule any previous negative setting
               (which may be introduced by multiple parent edges) */
            map = find_map (MAP_LIST(n), n->sem.select.col);
            if (map) {
                map->sel_name = true;
                map->sel_val  = true;
            }
            break;

        case la_pos_select:
        {
            PFord_ordering_t sortby = n->sem.pos_sel.sortby;
            for (unsigned int i = 0; i < PFord_count (sortby); i++)
                 /* for the order criteria we only have to ensure
                    the correct order */
                adjust_order (PFord_order_col_at (sortby, i));

            if (n->sem.pos_sel.part)
                /* we only have to provide the same groups */
                adjust_part (n->sem.pos_sel.part);
        }   break;

        case la_disjunion:
        case la_intersect:
            /* We have to apply a natural join here which means
               the values should be comparable. (Including domain
               information might improve this inference.) */
            for (unsigned int i = 0; i < n->schema.count; i++) {
                PFla_op_t  *origin;
                PFalg_col_t cur = n->schema.items[i].name;
                req_val_t  *map = find_map (MAP_LIST(n), cur);

                if (map && map->value)
                    continue;

                /* Keep order, join, and part columns if they stem from
                   the same operator.  For all other columns we require
                   the real values. */
                origin  = PFprop_lineage (n->prop, cur);
                if (origin && origin != n) {
                    PFalg_col_t ori_col = PFprop_lineage_col (n->prop, cur);
                    if (PFprop_subdom (n->prop,
                                       PFprop_dom_left (n->prop, cur),
                                       PFprop_dom (origin->prop, ori_col)) &&
                        PFprop_subdom (n->prop,
                                       PFprop_dom_right (n->prop, cur),
                                       PFprop_dom (origin->prop, ori_col))) {
                        if (!map) {
                            adjust_join (cur);
                            continue;
                        }
                        else if (map->order || map->join || map->part)
                            continue;
                    }
                }
                adjust_value (cur);
            }
            break;

        case la_difference:
        {
            PFarray_t *rmap;

            /* We have to apply a natural join here which means
               the values should be comparable. (Including domain
               information might improve this inference.) */
            for (unsigned int i = 0; i < n->schema.count; i++) {
                PFalg_col_t cur = n->schema.items[i].name;
                req_val_t  *map = find_map (MAP_LIST(n), cur);

                if (map && map->value)
                    continue;

                if (!map &&
                    (PFprop_subdom (n->prop,
                                    PFprop_dom_right (n->prop, cur),
                                    PFprop_dom_left (n->prop, cur)) ||
                     PFprop_subdom (n->prop,
                                    PFprop_dom_left (n->prop, cur),
                                    PFprop_dom_right (n->prop, cur)))) {
                    adjust_join (cur);
                    continue;
                }

                /* Keep order, join, and part columns if they stem from
                   the same operator.  For all other columns we require
                   the real values. */
                if (!map ||
                    !((PFprop_subdom (n->prop,
                                      PFprop_dom_right (n->prop, cur),
                                      PFprop_dom_left (n->prop, cur)) ||
                       PFprop_subdom (n->prop,
                                      PFprop_dom_left (n->prop, cur),
                                      PFprop_dom_right (n->prop, cur))) &&
                      (map->order || map->join || map->part)))
                    adjust_value (cur);
            }
            prop_infer_reqvals (L(n), MAP_LIST(n));

            rmap = PFarray_copy (MAP_LIST(n));
            for (unsigned int i = 0; i < PFarray_last (rmap); i++) {
                req_val_t *map = (req_val_t *) PFarray_at (rmap, i);
                /* we don't know anything about the selection
                   mapping so we replace it */
                if (map && map->value) {
                    map->sel_name = false;
                    map->sel_val  = true;
                }
            }
            prop_infer_reqvals (R(n), rmap);
        }   return; /* only infer once */

        case la_distinct:
            /* for all columns where we have no usage information
               we need to ensure at least the bijectivity */
            for (unsigned int i = 0; i < n->schema.count; i++) {
                PFalg_col_t cur = n->schema.items[i].name;
                req_val_t  *map = find_map (MAP_LIST(n), cur);
                if (!map)
                    adjust_distinct (cur);
            }
            break;

        case la_fun_1to1:
            /* mark the input columns as value columns */
            for (unsigned int i = 0; i < clsize (n->sem.fun_1to1.refs); i++)
                adjust_value (clat (n->sem.fun_1to1.refs, i));
            break;

        case la_num_eq:
            if (PFprop_req_bool_val (n->prop, n->sem.binary.res)) {
                adjust_filter (n->sem.binary.col1);
                adjust_filter (n->sem.binary.col2);
            }
            else {
                adjust_value (n->sem.binary.col1);
                adjust_value (n->sem.binary.col2);
            }
            break;

        case la_num_gt:
        case la_to:
            adjust_value (n->sem.binary.col1);
            adjust_value (n->sem.binary.col2);
            break;

        case la_bool_and:
            if (PFprop_req_bool_val (n->prop, n->sem.binary.res) &&
                PFprop_req_bool_val_val (n->prop, n->sem.binary.res)) {
                adjust_sel (n->sem.binary.col1);
                adjust_sel (n->sem.binary.col2);
            }
            else {
                adjust_value (n->sem.binary.col1);
                adjust_value (n->sem.binary.col2);
            }
            break;

        case la_bool_or:
            if (PFprop_req_bool_val (n->prop, n->sem.binary.res) &&
                !PFprop_req_bool_val_val (n->prop, n->sem.binary.res)) {
                adjust_nosel (n->sem.binary.col1);
                adjust_nosel (n->sem.binary.col2);
            }
            else {
                adjust_value (n->sem.binary.col1);
                adjust_value (n->sem.binary.col2);
            }
            break;

        case la_bool_not:
            /* if res is a required value column also add col
               with the switched boolean value */
            if (PFprop_req_bool_val (n->prop, n->sem.unary.res)) {
                if (PFprop_req_bool_val_val (n->prop, n->sem.unary.res))
                    adjust_nosel (n->sem.unary.col);
                else
                    adjust_sel (n->sem.unary.col);
            }
            else
                adjust_value (n->sem.unary.col);
            break;

        case la_aggr:
        {
            PFarray_t *lmap = PFarray (sizeof (req_val_t), n->schema.count);
            if (n->sem.aggr.part) {
                /* keep properties */
                req_val_t *map  = find_map (MAP_LIST(n), n->sem.aggr.part);
                if (map) ADD(lmap, *map);
                /* we only have to provide the same groups */
                adjust_part_ (lmap, n->sem.aggr.part);
            }
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                if (n->sem.aggr.aggr[i].col)
                    /* to make up for the schema change
                       we add the input columns by hand */
                    adjust_value_ (lmap, n->sem.aggr.aggr[i].col);

            prop_infer_reqvals (L(n), lmap);
        }   return; /* only infer once */

        case la_rownum:  /* for rownum, rowrank, rank, and rowid */
        case la_rowrank: /* type of res is != boolean and */
        case la_rank:    /* therefore never needs to be removed */
        {
            PFord_ordering_t sortby = n->sem.sort.sortby;
            for (unsigned int i = 0; i < PFord_count (sortby); i++)
                 /* for the order criteria we only have to ensure
                    the correct order */
                adjust_order (PFord_order_col_at (sortby, i));

            if (n->sem.sort.part)
                /* we only have to provide the same groups */
                adjust_part (n->sem.sort.part);
        }   break;

        case la_rowid:
            /* no additional usage information */
            break;

        case la_cast:
        {
            bool col_adjusted = false;

            if (n->sem.type.ty == aat_bln) {
                bool col_bln = false;
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (n->sem.type.col == n->schema.items[i].name) {
                        col_bln = (n->schema.items[i].type == aat_bln);
                        break;
                    }

                if (col_bln && PFprop_req_bool_val (n->prop, n->sem.type.res)) {
                    if (PFprop_req_bool_val_val (n->prop, n->sem.type.res))
                        adjust_sel (n->sem.type.col);
                    else
                        adjust_nosel (n->sem.type.col);
                    col_adjusted = true;
                }
            }
            if (!col_adjusted) {
                req_val_t *map = find_map (MAP_LIST(n), n->sem.type.res);
                if (!map || !map->order)
                    /* mark the input column as value columns */
                    adjust_value (n->sem.type.col);
                else
                    /* mark the input column as order column if
                       not used differently */
                    adjust_order (n->sem.type.col);
            }
        }   break;

        case la_type:
        case la_type_assert:
            /* mark the input columns as value columns */
            adjust_value (n->sem.type.col);
            break;

        case la_step:
        case la_guide_step:
            /* mark the input columns as value columns */
            adjust_value (n->sem.step.iter);
            adjust_value (n->sem.step.item);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_step_join:
        case la_guide_step_join:
            /* mark the input column as value column */
            adjust_value (n->sem.step.item);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_doc_index_join:
            /* mark the input columns as value columns */
            adjust_value (n->sem.doc_join.item);
            adjust_value (n->sem.doc_join.item_doc);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_doc_tbl:
            /* mark the input column as value column */
            adjust_value (n->sem.doc_tbl.col);
            break;

        case la_doc_access:
            /* mark the input column as value column */
            adjust_value (n->sem.doc_access.col);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_twig:
            prop_infer_reqvals_empty (L(n)); /* constructor */
            return; /* only infer once */

        case la_fcns:
            prop_infer_reqvals_empty (L(n)); /* constructor */
            prop_infer_reqvals_empty (R(n)); /* constructor */
            return; /* only infer once */

        case la_docnode:
            /* mark the input column as value column */
            adjust_value (n->sem.docnode.iter);

            prop_infer_reqvals (L(n), MAP_LIST(n));
            prop_infer_reqvals_empty (R(n)); /* constructor */
            return; /* only infer once */

        case la_element:
            /* mark the input columns as value columns */
            adjust_value (n->sem.iter_item.iter);
            adjust_value (n->sem.iter_item.item);

            prop_infer_reqvals (L(n), MAP_LIST(n));
            prop_infer_reqvals_empty (R(n)); /* constructor */
            return; /* only infer once */

        case la_textnode:
        case la_comment:
            /* mark the input columns as value columns */
            adjust_value (n->sem.iter_item.iter);
            adjust_value (n->sem.iter_item.item);
            break;

        case la_attribute:
        case la_processi:
            /* mark the input columns as value columns */
            adjust_value (n->sem.iter_item1_item2.iter);
            adjust_value (n->sem.iter_item1_item2.item1);
            adjust_value (n->sem.iter_item1_item2.item2);
            break;

        case la_content:
            /* mark the input columns as value columns */
            adjust_value (n->sem.iter_pos_item.iter);
            adjust_order (n->sem.iter_pos_item.pos);
            adjust_value (n->sem.iter_pos_item.item);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_merge_adjacent:
            /* mark the input columns as value columns */
            adjust_value (n->sem.merge_adjacent.iter_in);
            adjust_value (n->sem.merge_adjacent.pos_in);
            adjust_value (n->sem.merge_adjacent.item_in);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_roots:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
            /* propagate required values list to left subtree */
            break;

        case la_fragment:
        case la_frag_extract:
            prop_infer_reqvals_empty (L(n)); /* fragments */
            return; /* only infer once */

        case la_frag_union:
            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals_empty (R(n)); /* fragments */
            return; /* only infer once */

        case la_error:
        {
            PFarray_t *rmap = PFarray (sizeof (req_val_t), 1);
            adjust_value_ (rmap, n->sem.err.col);

            prop_infer_reqvals_empty (L(n)); /* side effects */
            prop_infer_reqvals (R(n), rmap);
        }   return; /* only infer once */

        case la_cache:
        {
            PFarray_t *rmap = PFarray (sizeof (req_val_t), 2);
            adjust_order_ (rmap, n->sem.cache.pos);
            adjust_value_ (rmap, n->sem.cache.item);

            prop_infer_reqvals_empty (L(n)); /* side effects */
            prop_infer_reqvals (R(n), rmap);
        }   return; /* only infer once */

        case la_trace:
            prop_infer_reqvals_empty (L(n)); /* side effects */
            prop_infer_reqvals_empty (R(n)); /* traces */
            return; /* only infer once */

        case la_trace_items:
        {
            PFarray_t *lmap = PFarray (sizeof (req_val_t), 3);
            /* mark the input columns as value columns */
            adjust_value_ (lmap, n->sem.iter_pos_item.iter);
            adjust_value_ (lmap, n->sem.iter_pos_item.pos);
            adjust_value_ (lmap, n->sem.iter_pos_item.item);

            prop_infer_reqvals (L(n), lmap);
            prop_infer_reqvals_empty (R(n)); /* traces */

        }   return; /* only infer once */

        case la_trace_msg:
        {
            PFarray_t *lmap = PFarray (sizeof (req_val_t), 3);
            /* mark the input columns as value columns */
            adjust_value_ (lmap, n->sem.iter_item.iter);
            adjust_value_ (lmap, n->sem.iter_item.item);

            prop_infer_reqvals (L(n), lmap);
            prop_infer_reqvals_empty (R(n)); /* traces */

        }   return; /* only infer once */

        case la_trace_map:
        {
            PFarray_t *lmap = PFarray (sizeof (req_val_t), 3);
            /* mark the input columns as value columns */
            adjust_value_ (lmap, n->sem.trace_map.inner);
            adjust_value_ (lmap, n->sem.trace_map.outer);

            prop_infer_reqvals (L(n), lmap);
            prop_infer_reqvals_empty (R(n)); /* traces */

        }   return; /* only infer once */

        case la_rec_fix:
            for (unsigned int i = 0; i < n->schema.count; i++)
                adjust_value (n->schema.items[i].name);

            prop_infer_reqvals_empty (L(n)); /* recursion param */
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_rec_param:
            prop_infer_reqvals_empty (L(n)); /* recursion arg */
            prop_infer_reqvals_empty (R(n)); /* recursion param */
            return; /* only infer once */

        case la_rec_arg:
            for (unsigned int i = 0; i < n->schema.count; i++)
                adjust_value (n->schema.items[i].name);

            prop_infer_reqvals (L(n), MAP_LIST(n));
            prop_infer_reqvals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_rec_base:
            break;

        case la_fun_call:
        {
            PFarray_t *lmap = PFarray (sizeof (req_val_t), 1);
            adjust_value_ (lmap, n->sem.fun_call.iter);

            prop_infer_reqvals (L(n), lmap);
            prop_infer_reqvals_empty (R(n)); /* function param */
        }   return; /* only infer once */

        case la_fun_param:
            for (unsigned int i = 0; i < n->schema.count; i++)
                adjust_value (n->schema.items[i].name);

            prop_infer_reqvals (L(n), MAP_LIST(n));
            prop_infer_reqvals_empty (R(n)); /* function param */
            return; /* only infer once */

        case la_fun_frag_param:
            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals_empty (R(n)); /* function param */
            return; /* only infer once */

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");

        case la_string_join:
        {
            PFarray_t *lmap = PFarray (sizeof (req_val_t), 3),
                      *rmap = PFarray (sizeof (req_val_t), 2);
            adjust_value_ (lmap, n->sem.string_join.iter);
            adjust_value_ (lmap, n->sem.string_join.pos);
            adjust_value_ (lmap, n->sem.string_join.item);
            adjust_value_ (rmap, n->sem.string_join.iter_sep);
            adjust_value_ (rmap, n->sem.string_join.item_sep);

            prop_infer_reqvals (L(n), lmap);
            prop_infer_reqvals (R(n), rmap);
        }   return; /* only infer once */
    }

    if (L(n)) prop_infer_reqvals (L(n), MAP_LIST(n));
    if (R(n)) prop_infer_reqvals (R(n), MAP_LIST(n));
}

/* worker for PFprop_infer_reqval */
static void
prop_infer (PFla_op_t *n)
{
    assert (n);

    /* count number of incoming edges
       (during first run) */
    EDGE(n)++;
    MULTIPLE_INPUT_EDGES(n) = true;

    /* nothing to do if we already visited that node */
    if (SEEN(n))
        return;
    /* otherwise initialize edge counter (first occurrence) */
    else {
        EDGE(n)                 = 1;
        MULTIPLE_INPUT_EDGES(n) = false;
    }

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i]);

    SEEN(n) = true;

    /* reset required values property list
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (MAP_LIST(n))
        PFarray_last (MAP_LIST(n)) = 0;
}

/**
 * Infer required values properties for a DAG rooted in @a root
 */
void
PFprop_infer_reqval (PFla_op_t *root)
{
    /* We need the domain property and the lineage
       to detect more bijective columns (in operators
       with two children). */
    PFprop_infer_nat_dom (root);
    PFprop_infer_lineage (root);

    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    /* second run infers required values properties */
    prop_infer_reqvals (root, NULL);
    /* reset the multiple input edges flag */
    PFla_in_reset (root);
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
