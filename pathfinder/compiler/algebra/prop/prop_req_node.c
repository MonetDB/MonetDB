/**
 * @file
 *
 * @brief Inference of required node value property of logical
 *        algebra expressions.
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
#include <stdio.h>

#include "properties.h"

#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/* store the number of incoming edges for each operator
   in the refctr field */
#define EDGE(n) ((n)->refctr)

#define type_of(n,a) PFprop_type_of ((n), (a))

/* required node property list */
struct req_node_t {
    PFalg_att_t col;       /* column name ... */
    
    /* ... and the corresponding property that tests if ... */

    bool        serialize; /* ... the node may be used (indirectly) for
                              serialization */
    bool        id;        /* ... the node id may be used */
    bool        order;     /* ... the node order may be used */
    bool        access;    /* ... the node values may be accessed */
    bool        axis_down; /* ... a downward axis is applied lateron */
    bool        axis_side; /* ... a side-way axis is applied lateron */
    bool        axis_up;   /* ... an upward axis is applied lateron */
    bool        axis_self; /* ... a *self axis is applied lateron */
    bool        constr;    /* ... the node may be used as input to
                              node construction */
};
typedef struct req_node_t req_node_t;

#define MAP_LIST(n)       ((n)->prop->req_node_vals)
#define ADD(l,p)          (*((req_node_t *) PFarray_add ((l))) = (p))
#define MAP_AT(n,i)       (*((req_node_t *) PFarray_at ((n), (i))))

#define COL_AT(n,i)       (((req_node_t *) PFarray_at ((n), (i)))->col)
#define SERIALIZE_AT(n,i) (((req_node_t *) PFarray_at ((n), (i)))->serialize)
#define ID_AT(n,i)        (((req_node_t *) PFarray_at ((n), (i)))->id)
#define ORDER_AT(n,i)     (((req_node_t *) PFarray_at ((n), (i)))->order)
#define ACCESS_AT(n,i)    (((req_node_t *) PFarray_at ((n), (i)))->access)
#define AXIS_DOWN_AT(n,i) (((req_node_t *) PFarray_at ((n), (i)))->axis_down)
#define AXIS_SIDE_AT(n,i) (((req_node_t *) PFarray_at ((n), (i)))->axis_side)
#define AXIS_UP_AT(n,i)   (((req_node_t *) PFarray_at ((n), (i)))->axis_up)
#define AXIS_SELF_AT(n,i) (((req_node_t *) PFarray_at ((n), (i)))->axis_self)
#define CONSTR_AT(n,i)    (((req_node_t *) PFarray_at ((n), (i)))->constr)
    
/**
 * @brief look up the property mapping (in @a l) for a given column @a attr.
 */
static req_node_t *
find_map (PFarray_t *l, PFalg_att_t attr)
{
    if (!l)
        return NULL;

    for (unsigned int i = 0; i < PFarray_last (l); i++) {
        if (attr == COL_AT(l, i))
            return (req_node_t *) PFarray_at (l, i);
    }
    return NULL;
}

/**
 * @brief look up the property mapping (in @a l) for a given column @a attr.
 */
static void
add_map_ (PFla_op_t *n, PFalg_att_t attr,
          bool serialize, bool id, bool order,
          bool access, bool axis_down, bool axis_side,
          bool axis_up, bool axis_self, bool constr)
{
    req_node_t *map;
    assert (n);

    /* lookup the mapping (if present) */
    map = find_map (MAP_LIST(n), attr);
    
    /* add a new mapping */
    if (!map) {
        /* create a new mapping list if not already available */
        if (!MAP_LIST(n))
            MAP_LIST(n) = PFarray (sizeof (req_node_t), 3);
    
        *((req_node_t *) PFarray_add (MAP_LIST(n)))
            = (req_node_t) { .col       = attr,
                             .serialize = serialize,
                             .id        = id,
                             .order     = order,
                             .access    = access,
                             .axis_down = axis_down,
                             .axis_side = axis_side,
                             .axis_up   = axis_up,
                             .axis_self = axis_self,
                             .constr    = constr };
    }
    /* modify already existing mapping */
    else {
        map->serialize |= serialize;
        map->id        |= id;
        map->order     |= order;
        map->access    |= access;
        map->axis_down |= axis_down;
        map->axis_side |= axis_side;
        map->axis_up   |= axis_up;
        map->axis_self |= axis_self;
        map->constr    |= constr;
    }
}

#define add_map(n,attr) add_map_ ((n),(attr), \
                                  false, false, false \
                                  false, false, false \
                                  false, false, false)
#define add_serialize_map(n,attr) \
        add_map_ ((n),(attr), true , false, false, \
                  false, false, false, false, false, false)
#define add_id_map(n,attr) \
        add_map_ ((n),(attr), false, true , false, \
                  false, false, false, false, false, false)
#define add_order_map(n,attr) \
        add_map_ ((n),(attr), false, false, true , \
                  false, false, false, false, false, false)
#define add_access_map(n,attr) \
        add_map_ ((n),(attr), false, false, false, \
                  true , false, false, false, false, false)
#define add_axis_down_map(n,attr) \
        add_map_ ((n),(attr), false, false, false, \
                  false, true , false, false, false, false)
#define add_axis_side_map(n,attr) \
        add_map_ ((n),(attr), false, false, false, \
                  false, false, true , false, false, false)
#define add_axis_up_map(n,attr) \
        add_map_ ((n),(attr), false, false, false, \
                  false, false, false, true , false, false)
#define add_axis_self_map(n,attr) \
        add_map_ ((n),(attr), false, false, false, \
                  false, false, false, false, true , false)
#define add_constr_map(n,attr) \
        add_map_ ((n),(attr), false, false, false, \
                  false, false, false, false, false, true)


/**
 * @brief Test if column @a attr is linked to any node properties.
 */
bool
PFprop_node_property (const PFprop_t *prop, PFalg_att_t attr)
{
    return (find_map (prop->req_node_vals, attr) != NULL);
}

/**
 * @brief Test if the subtree of column @a attr is queried.
 */
bool
PFprop_node_content_queried (const PFprop_t *prop, PFalg_att_t attr)
{
    req_node_t *map = find_map (prop->req_node_vals, attr);

    if (!map)
        return true;
    else
        return map->axis_down;
}

/**
 * @brief Test if the nodes of column @a attr are serialized.
 */
bool
PFprop_node_serialize (const PFprop_t *prop, PFalg_att_t attr)
{
    req_node_t *map = find_map (prop->req_node_vals, attr);

    if (!map)
        return true;
    else
        return map->serialize;
}

/**
 * worker for PFprop_infer_req_node
 * infers the required node properties during the second run
 * (uses edge counter from the first run)
 */
static void
prop_infer_req_node_vals (PFla_op_t *n, PFarray_t *req_node_vals)
{
    req_node_t *map;
    
    assert (n);

    /* initialize the mapping list if we need it
       and haven't got one already */
    if (!MAP_LIST(n) && req_node_vals && PFarray_last (req_node_vals) > 0)
        MAP_LIST(n) = PFarray (sizeof (req_node_t),
                               PFarray_last (req_node_vals) + 3);

    if (req_node_vals) {
        /* merge all mappings */
        for (unsigned int i = 0; i < PFarray_last (req_node_vals); i++) {
            /* Only try to merge nodes visible at that node.
               (Non-matching columns may result from binary
                operators as e.g., the cross product.) */
            if (!PFprop_ocol (n, COL_AT(req_node_vals, i)) &&
                /* special case for constructors */
                n->kind != la_fcns &&
                n->kind != la_docnode &&
                n->kind != la_element &&
                n->kind != la_textnode &&
                n->kind != la_comment &&
                n->kind != la_attribute &&
                n->kind != la_processi &&
                n->kind != la_content)
                continue;

            /* lookup the mapping in the already existing mapping list */
            map = find_map (MAP_LIST(n), COL_AT(req_node_vals, i));

            /* modify the mapping */
            if (map) {
                map->serialize |= SERIALIZE_AT(req_node_vals, i);
                map->id        |= ID_AT(req_node_vals, i);
                map->order     |= ORDER_AT(req_node_vals, i);
                map->access    |= ACCESS_AT(req_node_vals, i);
                map->axis_down |= AXIS_DOWN_AT(req_node_vals, i);
                map->axis_side |= AXIS_SIDE_AT(req_node_vals, i);
                map->axis_up   |= AXIS_UP_AT(req_node_vals, i);
                map->axis_self |= AXIS_SELF_AT(req_node_vals, i);
                map->constr    |= CONSTR_AT(req_node_vals, i);
            }
            else
                ADD(MAP_LIST(n), MAP_AT(req_node_vals, i));
        }
    }
                
    /* nothing to do if we haven't collected
       all incoming required node property lists of that node */
    if (EDGE(n) > 1) {
        EDGE(n)--;
        return;
    }

    switch (n->kind) {
        case la_serialize_seq:
            if (type_of (n, n->sem.ser_seq.item) & aat_node)
                add_serialize_map (n, n->sem.ser_seq.item);

            if (type_of (n, n->sem.ser_seq.pos) & aat_node)
                add_order_map (n, n->sem.ser_seq.pos);
                
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_serialize_rel:
            if (type_of (n, n->sem.ser_rel.pos) & aat_node)
                add_order_map (n, n->sem.ser_rel.pos);
            
            for (unsigned int i = 0; i < n->sem.ser_rel.items.count; i++)
                if (type_of (n, n->sem.ser_rel.items.atts[i]) & aat_node)
                    add_serialize_map (n, n->sem.ser_rel.items.atts[i]);
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_empty_frag:
        case la_nil:
            /* leafs */
            break;

        case la_attach:
            assert ((type_of (n, n->sem.attach.res) & aat_node) == 0);
            break;

        case la_cross:
            break;

        case la_eqjoin:
        case la_semijoin:
            if (type_of (n, n->sem.eqjoin.att1) & aat_node) {
                add_id_map (n, n->sem.eqjoin.att1);
                add_id_map (n, n->sem.eqjoin.att2);
            }
            break;

        case la_thetajoin:
            /* add all join columns to the inferred icols */
            for (unsigned int i = 0; i < n->sem.thetajoin.count; i++)
                if (type_of (n, n->sem.thetajoin.pred[i].left) & aat_node) {
                    add_id_map (n, n->sem.thetajoin.pred[i].left);
                    add_id_map (n, n->sem.thetajoin.pred[i].right);
                }
            break;

        case la_project:
            if (MAP_LIST(n) != NULL && PFarray_last (MAP_LIST(n)) > 0) {
                PFarray_t *new_map = PFarray (sizeof (req_node_t),
                                              PFarray_last (MAP_LIST(n)));

                for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                    map = find_map (MAP_LIST(n), n->sem.proj.items[i].new);
                    if (map) {
                        req_node_t map_item = *map;
                        map_item.col = n->sem.proj.items[i].old;
                        ADD(new_map, map_item);
                    }
                }

                prop_infer_req_node_vals (L(n), new_map);
                return; /* only infer once */
            }
            break;

        case la_select:
            break;

        case la_pos_select:
        {
            PFord_ordering_t sortby = n->sem.pos_sel.sortby;
            for (unsigned int i = 0; i < PFord_count (sortby); i++)
                if (type_of (n, PFord_order_col_at (sortby, i)) & aat_node)
                    add_order_map (n, PFord_order_col_at (sortby, i));
            
            if (n->sem.pos_sel.part &&
                type_of (n, n->sem.pos_sel.part) & aat_node)
                add_id_map (n, n->sem.pos_sel.part);
        }   break;

        case la_disjunion:
            break;
            
        case la_intersect:
        case la_difference:
        case la_distinct:
            for (unsigned int i = 0; i < n->schema.count; i++)
                if (n->schema.items[i].type & aat_node)
                    add_id_map (n, n->schema.items[i].name);
            break;
            
        case la_fun_1to1:
            /* mark the input columns as access columns */
            for (unsigned int i = 0; i < n->sem.fun_1to1.refs.count; i++)
                if (type_of (n, n->sem.fun_1to1.refs.atts[i]) & aat_node)
                    add_access_map (n, n->sem.fun_1to1.refs.atts[i]);
            break;

        case la_num_eq:
        case la_num_gt:
            if (type_of (n, n->sem.binary.att1) & aat_node) {
                add_id_map (n, n->sem.binary.att1);
                add_id_map (n, n->sem.binary.att2);
            }
            break;
            
        case la_to:
        case la_bool_or:
        case la_bool_and:
            /* the output cannot be of type node */
            assert ((type_of (n, n->sem.binary.att1) & aat_node) == 0);
            break;

        case la_bool_not:
            /* the output cannot be of type node */
            assert ((type_of (n, n->sem.unary.att) & aat_node) == 0);
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
            /* the output cannot be of type node */
            if (!n->sem.aggr.part) {
                prop_infer_req_node_vals (L(n), NULL);
            }
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
        {
            PFord_ordering_t sortby = n->sem.sort.sortby;
            for (unsigned int i = 0; i < PFord_count (sortby); i++)
                if (type_of (n, PFord_order_col_at (sortby, i)) & aat_node)
                    add_order_map (n, PFord_order_col_at (sortby, i));
            
            if (n->sem.sort.part &&
                type_of (n, n->sem.sort.part) & aat_node)
                add_id_map (n, n->sem.sort.part);
        }   break;
            
        case la_rowid:
            /* the output cannot be of type node */
            break;
            
        case la_cast:
        case la_type:
            assert ((type_of (n, n->sem.type.res) & aat_node) == 0);
            assert ((type_of (n, n->sem.type.att) & aat_node) == 0);
            break;

        case la_type_assert:
            break;

        case la_step:
        case la_guide_step:
            assert ((type_of (n, n->sem.step.iter) & aat_node) == 0);
        {
            PFarray_t *new_map = PFarray (sizeof (req_node_t), 1),
                      *old_map;
            
            map = find_map (MAP_LIST(n), n->sem.step.item_res);

            /* inherit the properties of the output */
            if (map) {
                req_node_t map_item = *map;
                map_item.col = n->sem.step.item;
                ADD(new_map, map_item);
            }
            
            /* replace current map by new_map and store the current */
            old_map = MAP_LIST(n);
            MAP_LIST(n) = new_map;
            
            switch (n->sem.step.spec.axis) {
                case alg_anc_s:
                    add_axis_self_map (n, n->sem.step.item);
                case alg_anc:
                case alg_par:
                    add_axis_up_map (n, n->sem.step.item);
                    break;

                case alg_desc_s:
                    add_axis_self_map (n, n->sem.step.item);
                case alg_attr:
                case alg_chld:
                case alg_desc:
                    add_axis_down_map (n, n->sem.step.item);
                    break;
                    
                case alg_fol:
                case alg_fol_s:
                case alg_prec:
                case alg_prec_s:
                    add_axis_side_map (n, n->sem.step.item);
                    break;
                                 
                case alg_self:
                    add_axis_self_map (n, n->sem.step.item);
                    break;
            }
            
            /* switch back */
            MAP_LIST(n) = old_map;
            
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), new_map);
        }   return; /* only infer once */

        case la_step_join:
        case la_guide_step_join:
            map = find_map (MAP_LIST(n), n->sem.step.item_res);

            /* inherit the properties of the output */
            if (map) {
                req_node_t map_item = *map;
                map_item.col = n->sem.step.item;
                ADD(MAP_LIST(n), map_item);
            }
            
            switch (n->sem.step.spec.axis) {
                case alg_anc_s:
                    add_axis_self_map (n, n->sem.step.item);
                case alg_anc:
                case alg_par:
                    add_axis_up_map (n, n->sem.step.item);
                    break;

                case alg_desc_s:
                    add_axis_self_map (n, n->sem.step.item);
                case alg_attr:
                case alg_chld:
                case alg_desc:
                    add_axis_down_map (n, n->sem.step.item);
                    break;
                    
                case alg_fol:
                case alg_fol_s:
                case alg_prec:
                case alg_prec_s:
                    add_axis_side_map (n, n->sem.step.item);
                    break;
                                 
                case alg_self:
                    add_axis_self_map (n, n->sem.step.item);
                    break;
            }
            
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_doc_index_join:
            map = find_map (MAP_LIST(n), n->sem.step.item_res);

            /* inherit the properties of the output */
            if (map) {
                req_node_t map_item = *map;
                map_item.col = n->sem.step.item;
                ADD(MAP_LIST(n), map_item);
            }
            
            /* we need to look up the ids in a special relation */
            add_id_map (n, n->sem.step.item);
            
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_doc_tbl:
            if (MAP_LIST(n) != NULL && PFarray_last (MAP_LIST(n)) > 0) {
                PFarray_t *new_map = PFarray (sizeof (req_node_t),
                                              PFarray_last (MAP_LIST(n)));
                for (unsigned int i = 0; i < PFarray_last (MAP_LIST(n)); i++)
                    if (COL_AT(MAP_LIST(n), i) != n->sem.doc_tbl.res)
                        ADD(new_map, MAP_AT(MAP_LIST(n), i));

                prop_infer_req_node_vals (L(n), new_map);
                return; /* only infer once */
            }
            break;

        case la_doc_access:
            assert ((type_of (n, n->sem.doc_access.res) & aat_node) == 0);
            add_access_map (n, n->sem.doc_access.att);
            
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_twig:
            if (MAP_LIST(n) != NULL && PFarray_last (MAP_LIST(n)) > 0) {
                PFarray_t *new_map = PFarray (sizeof (req_node_t), 2);
                
                map = find_map (MAP_LIST(n), n->sem.iter_item.iter);

                /* inherit the properties of the iter column */
                if (map) {
                    req_node_t map_item = *map;
                    map_item.col = att_iter;
                    ADD(new_map, map_item);
                }
                
                map = find_map (MAP_LIST(n), n->sem.iter_item.item);

                /* inherit the properties of the item column */
                if (map) {
                    req_node_t map_item = *map;
                    map_item.col = att_item;
                    /* if no downward axis step is applied
                       to the constructed fragment the id,
                       order, and access properties of the
                       content are not needed. */
                    if (map_item.axis_down == false) {
                        map_item.id     = false;
                        map_item.order  = false;
                        map_item.access = false;
                    }
                    
                    ADD(new_map, map_item);
                }
                
                prop_infer_req_node_vals (L(n), new_map);
                return; /* only infer once */
            }
            break;
            
        case la_fcns:
            break;

        case la_docnode:
            if (MAP_LIST(n) != NULL && PFarray_last (MAP_LIST(n)) > 0) {
                PFarray_t *new_map = PFarray (sizeof (req_node_t), 1);
                
                map = find_map (MAP_LIST(n), att_iter);

                /* inherit the properties of the iter column */
                if (map) {
                    req_node_t map_item = *map;
                    map_item.col = n->sem.docnode.iter;
                    ADD(new_map, map_item);
                }
                
                prop_infer_req_node_vals (L(n), new_map);
                return; /* only infer once */
            }
            break;

        case la_element:
            if (MAP_LIST(n) != NULL && PFarray_last (MAP_LIST(n)) > 0) {
                PFarray_t *new_map = PFarray (sizeof (req_node_t), 1);
                
                map = find_map (MAP_LIST(n), att_iter);

                /* inherit the properties of the iter column */
                if (map) {
                    req_node_t map_item = *map;
                    map_item.col = n->sem.iter_item.iter;
                    ADD(new_map, map_item);
                }
                
                prop_infer_req_node_vals (L(n), new_map);
                prop_infer_req_node_vals (R(n), MAP_LIST(n));
                return; /* only infer once */
            }
            break;

        case la_textnode:
        case la_comment:
            if (MAP_LIST(n) != NULL && PFarray_last (MAP_LIST(n)) > 0) {
                PFarray_t *new_map = PFarray (sizeof (req_node_t), 1);
                
                map = find_map (MAP_LIST(n), att_iter);

                /* inherit the properties of the iter column */
                if (map) {
                    req_node_t map_item = *map;
                    map_item.col = n->sem.iter_item.iter;
                    ADD(new_map, map_item);
                }
                
                prop_infer_req_node_vals (L(n), new_map);
                return; /* only infer once */
            }
            break;

        case la_attribute:
        case la_processi:
            if (MAP_LIST(n) != NULL && PFarray_last (MAP_LIST(n)) > 0) {
                PFarray_t *new_map = PFarray (sizeof (req_node_t), 2);
                
                map = find_map (MAP_LIST(n), att_iter);

                /* inherit the properties of the iter column */
                if (map) {
                    req_node_t map_item = *map;
                    map_item.col = n->sem.iter_item1_item2.iter;
                    ADD(new_map, map_item);
                }
                
                prop_infer_req_node_vals (L(n), new_map);
                return; /* only infer once */
            }
            break;

        case la_content:
        {
            PFarray_t *new_map = PFarray (sizeof (req_node_t), 2),
                      *old_map;
            
            map = find_map (MAP_LIST(n), att_iter);

            /* inherit the properties of the iter column */
            if (map) {
                req_node_t map_item = *map;
                map_item.col = n->sem.iter_pos_item.iter;
                ADD(new_map, map_item);
            }
            
            map = find_map (MAP_LIST(n), att_item);

            /* inherit the properties of the iter column */
            if (map) {
                req_node_t map_item = *map;
                map_item.col = n->sem.iter_pos_item.item;
                ADD(new_map, map_item);
            }

            /* replace current map by new_map and store the current */
            old_map = MAP_LIST(n);
            MAP_LIST(n) = new_map;
            
            add_constr_map (n, n->sem.iter_pos_item.item);
            
            /* switch back */
            MAP_LIST(n) = old_map;
            
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), new_map);
            return; /* only infer once */
        }

        case la_merge_adjacent:
            add_constr_map (n, n->sem.merge_adjacent.item_in);
            
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), MAP_LIST(n));
            return; /* only infer once */

        case la_roots:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
        case la_error:
            /* propagate required property list to left subtree */
            break;
            
        case la_fragment:
        case la_frag_extract:
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            return; /* only infer once */

        case la_frag_union:
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), NULL); /* fragments */
            return; /* only infer once */

        case la_cond_err:
            assert ((type_of (R(n), n->sem.err.att) & aat_node) == 0);
            prop_infer_req_node_vals (L(n), MAP_LIST(n));
            prop_infer_req_node_vals (R(n), NULL);
            return; /* only infer once */

        case la_trace:
            if (type_of (n, n->sem.iter_pos_item.item) & aat_node)
                add_serialize_map (n, n->sem.iter_pos_item.item);
            
            prop_infer_req_node_vals (L(n), MAP_LIST(n));
            prop_infer_req_node_vals (R(n), NULL); /* trace */
            return; /* only infer once */

        case la_trace_msg:
            assert ((type_of (n, n->sem.iter_item.item) & aat_node) == 0);
            prop_infer_req_node_vals (L(n), MAP_LIST(n));
            prop_infer_req_node_vals (R(n), NULL); /* trace */
            return; /* only infer once */

        case la_trace_map:
            assert ((type_of (n, n->sem.trace_map.inner) & aat_node) == 0);
            assert ((type_of (n, n->sem.trace_map.outer) & aat_node) == 0);
            prop_infer_req_node_vals (L(n), MAP_LIST(n));
            prop_infer_req_node_vals (R(n), NULL); /* trace */
            return; /* only infer once */

        case la_rec_fix:
        {
            PFarray_t *new_map = PFarray (sizeof (req_node_t),
                                          n->schema.count);
            
            for (unsigned int i = 0; i < n->schema.count; i++)
                if (n->schema.items[i].type & aat_node) {
                    req_node_t map_item;
                    map_item.col       = n->schema.items[i].name; 
                    map_item.serialize = true; 
                    map_item.id        = true; 
                    map_item.order     = true; 
                    map_item.access    = true; 
                    map_item.axis_down = true; 
                    map_item.axis_side = true; 
                    map_item.axis_up   = true; 
                    map_item.axis_self = true; 
                    map_item.constr    = true; 

                    ADD(new_map, map_item);
                }

            prop_infer_req_node_vals (L(n), NULL); /* recursion param */
            prop_infer_req_node_vals (R(n), new_map);
        }   return; /* only infer once */

        case la_rec_param:
            prop_infer_req_node_vals (L(n), NULL); /* recursion arg */
            prop_infer_req_node_vals (R(n), NULL); /* recursion param */
            return; /* only infer once */

        case la_rec_arg:
        {
            PFarray_t *new_map = PFarray (sizeof (req_node_t),
                                          n->schema.count);
            
            for (unsigned int i = 0; i < n->schema.count; i++)
                if (n->schema.items[i].type & aat_node) {
                    req_node_t map_item;
                    map_item.col       = n->schema.items[i].name; 
                    map_item.serialize = true; 
                    map_item.id        = true; 
                    map_item.order     = true; 
                    map_item.access    = true; 
                    map_item.axis_down = true; 
                    map_item.axis_side = true; 
                    map_item.axis_up   = true; 
                    map_item.axis_self = true; 
                    map_item.constr    = true; 

                    ADD(new_map, map_item);
                }

            prop_infer_req_node_vals (L(n), new_map);
            prop_infer_req_node_vals (R(n), new_map);
        }   return; /* only infer once */

        case la_rec_base:
            break;
            
        case la_fun_call:
            assert ((type_of (n, n->sem.fun_call.iter) & aat_node) == 0);
            prop_infer_req_node_vals (L(n), NULL);
            prop_infer_req_node_vals (R(n), NULL); /* function param */
            return; /* only infer once */
            
        case la_fun_param:
        {
            PFarray_t *new_map = PFarray (sizeof (req_node_t),
                                          n->schema.count);
            
            for (unsigned int i = 0; i < n->schema.count; i++)
                if (n->schema.items[i].type & aat_node) {
                    req_node_t map_item;
                    map_item.col       = n->schema.items[i].name; 
                    map_item.serialize = true; 
                    map_item.id        = true; 
                    map_item.order     = true; 
                    map_item.access    = true; 
                    map_item.axis_down = true; 
                    map_item.axis_side = true; 
                    map_item.axis_up   = true; 
                    map_item.axis_self = true; 
                    map_item.constr    = true; 

                    ADD(new_map, map_item);
                }

            prop_infer_req_node_vals (L(n), new_map);
            prop_infer_req_node_vals (R(n), NULL); /* function param */
        }   return; /* only infer once */
            
        case la_fun_frag_param:
            prop_infer_req_node_vals (L(n), NULL); /* fragments */
            prop_infer_req_node_vals (R(n), NULL); /* function param */
            return; /* only infer once */

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");

        case la_eqjoin_unq:
            PFoops (OOPS_FATAL,
                    "clone column aware equi-join operator is "
                    "only allowed with unique attribute names!");
            
        case la_string_join:
            if (MAP_LIST(n) != NULL && PFarray_last (MAP_LIST(n)) > 0) {
                PFarray_t *new_map = PFarray (sizeof (req_node_t), 1);
                
                map = find_map (MAP_LIST(n), n->sem.string_join.iter_res);

                /* inherit the properties of the iter column */
                if (map) {
                    req_node_t map_item = *map;
                    map_item.col = n->sem.string_join.iter;
                    ADD(new_map, map_item);
                }
                
                prop_infer_req_node_vals (L(n), new_map);

                PFarray_last (new_map) = 0;
                
                /* inherit the properties of the iter column */
                if (map) {
                    req_node_t map_item = *map;
                    map_item.col = n->sem.string_join.iter_sep;
                    ADD(new_map, map_item);
                }
                
                prop_infer_req_node_vals (R(n), new_map);
                return; /* only infer once */
            }
            break;
    }

    if (L(n)) prop_infer_req_node_vals (L(n), MAP_LIST(n));
    if (R(n)) prop_infer_req_node_vals (R(n), MAP_LIST(n));
}

/* worker for PFprop_infer_reqval */
static void
prop_infer (PFla_op_t *n)
{
    assert (n);

    /* count number of incoming edges
       (during first run) */
    EDGE(n)++;

    /* nothing to do if we already visited that node */
    if (SEEN(n))
        return;
    /* otherwise initialize edge counter (first occurrence) */
    else
        EDGE(n) = 1;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i]);

    SEEN(n) = true;

    /* reset required node property list
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (MAP_LIST(n))
        PFarray_last (MAP_LIST(n)) = 0;
}

/**
 * Infer required node properties for a DAG rooted in @a root
 */
void
PFprop_infer_req_node (PFla_op_t *root) {
    /* initial empty list of required node properties */
    PFarray_t *init = PFarray (sizeof (req_node_t), 0);

    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    /* second run infers required node properties */
    prop_infer_req_node_vals (root, init);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
