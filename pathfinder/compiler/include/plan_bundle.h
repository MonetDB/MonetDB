/**
 * @file
 *
 * Declarations specific to algebraic plan bundles.
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

#ifndef PLAN_BUNDLE_H
#define PLAN_BUNDLE_H

#include "array.h"
#include "logical.h"

/** Logical algebra plan bundle item */
struct PFla_pb_item_t {
    PFla_op_t * op;         /**< the logical algebra plan */
    int         id;         /**< the id of the plan */
    int         idref;      /**< the reference to the parent plan */
    int         colref;     /**< the column reference to the parent plan */
    PFarray_t*	properties; /**< properties of the logical algebra plan */
};
typedef struct PFla_pb_item_t PFla_pb_item_t;

/** A property of a Logical algebra plan bundle item  */
struct PFla_pb_item_property_t {
    char*      	name;       /**< property name  */
    char*      	value;      /**< property value */
    PFarray_t*	properties; /**< sub properties */
};
typedef struct PFla_pb_item_property_t PFla_pb_item_property_t;


/** A list of columns (actually: column names) */
#define PFla_pb_t               PFarray_t
/** Constructor for a column list */
#define PFla_pb(size)           PFarray (sizeof (PFla_pb_item_t), (size))
#define PFla_pb_copy(pb)        PFarray_copy ((pb))
/** Positional access to a column list */
#define PFla_pb_at(pb,i)        *(PFla_pb_item_t *) PFarray_at ((pb), (i))
#define PFla_pb_top(pb)         *(PFla_pb_item_t *) PFarray_top ((pb))
/** Append to a column list */
#define PFla_pb_add(pb)         *(PFla_pb_item_t *) PFarray_add ((pb))
#define PFla_pb_concat(pb1,pb2) PFarray_concat ((pb1), (pb2))
/** Size of a column list */
#define PFla_pb_size(pb)        PFarray_last ((pb))

#define PFla_pb_op_at(pb,i)         (PFla_pb_at(pb,i)).op
#define PFla_pb_id_at(pb,i)         (PFla_pb_at(pb,i)).id
#define PFla_pb_idref_at(pb,i)      (PFla_pb_at(pb,i)).idref
#define PFla_pb_colref_at(pb,i)     (PFla_pb_at(pb,i)).colref
#define PFla_pb_properties_at(pb,i) (PFla_pb_at(pb,i)).properties

/**
 * Helper function to print the property structure 
 * to a an character array @a a.
 */
void PFla_pb_property_print (PFchar_array_t *a,
                             PFla_pb_item_property_t property,
                             unsigned int indent_size);

/**
 * Iterate over the plan_bundle list lapb and bind every item to la_op.
 * If no plan bundle is present (pb == NULL) la_single is used instead.
 * XML Output is written to the PFchar_array_t f.
 *
 * BEWARE: macro has to be used in combination with macro PFla_pb_end.
 */
#define PFla_pb_foreach(f,la_op,lapb,la_single)                             \
        {                                                                   \
            PFla_op_t   *la_op = NULL;                                      \
            unsigned int la_op_index = 0;                                   \
                                                                            \
            if (lapb)                                                       \
                PFarray_printf (f, "<query_plan_bundle>\n");                \
            else                                                            \
                la_op = la_single;                                          \
                                                                            \
            /* If we have a plan bundle we have to generate additional       
               output and bind the items. */                                \
            do {                                                            \
                if (lapb) {                                                 \
                    unsigned int prop_index,                                \
                                 schema_index;                              \
                    int          id, idref, colref;                         \
                    PFarray_t   *properties;                                \
                                                                            \
                    la_op      = PFla_pb_op_at (lapb, la_op_index);         \
                    id         = PFla_pb_id_at (lapb, la_op_index);         \
                    idref      = PFla_pb_idref_at (lapb, la_op_index);      \
                    colref     = PFla_pb_colref_at (lapb, la_op_index);     \
                    properties = PFla_pb_properties_at (lapb, la_op_index); \
                                                                            \
                    assert (la_op->kind == la_serialize_rel);               \
                                                                            \
                    PFarray_printf (f, "  <query_plan id=\"%i\"", id);      \
                    if (idref != -1)                                        \
                        PFarray_printf (f, " idref=\"%i\" colref=\"%i\"",   \
                                        idref, colref);                     \
                    PFarray_printf (f, ">\n");                              \
                                                                            \
                    if (properties) {                                       \
                        PFla_pb_item_property_t property;                   \
                        PFarray_printf (f, "    <properties>\n");           \
                        for (prop_index = 0;                                \
                             prop_index < PFarray_last (properties);        \
                             prop_index++) {                                \
                             property = *((PFla_pb_item_property_t*)        \
                                            PFarray_at (properties,         \
                                                        prop_index));       \
                             PFla_pb_property_print (f, property, 6);       \
                        }                                                   \
                        PFarray_printf (f, "    </properties>\n");          \
                    }                                                       \
                                                                            \
                    PFarray_printf (                                        \
                        f,                                                  \
                        "    <schema>\n"                                    \
                        "      <column name=\"%s\" function=\"iter\"/>\n",  \
                        PFcol_str (la_op->sem.ser_rel.iter));               \
                    for (schema_index = 0;                                  \
                         schema_index < PFalg_collist_size (                \
                                            la_op->sem.ser_rel.items);      \
                         schema_index++)                                    \
                        PFarray_printf (                                    \
                            f,                                              \
                            "      <column name=\"%s\" new=\"false\""       \
                                         " function=\"item\""               \
                                         " position=\"%u\"/>\n",            \
                            PFcol_str (PFalg_collist_at (                   \
                                           la_op->sem.ser_rel.items,        \
                                           schema_index)),                  \
                            schema_index);                                  \
                    PFarray_printf (f, "    </schema>\n");                  \
                                                                            \
                    la_op_index++;                                          \
                }

/**
 * XML Output is written to the PFchar_array_t f.
 *
 * BEWARE: macro has to be used in combination with macro PFla_pb_foreach.
 */
#define PFla_pb_foreach_end(f,lapb)                                         \
                if (lapb)                                                   \
                    PFarray_printf (f, "  </query_plan>\n");                \
                                                                            \
            /* iterate over the plans in the plan bundle */                 \
            } while (lapb && la_op_index < PFla_pb_size (lapb));            \
                                                                            \
            if (lapb)                                                       \
                PFarray_printf (f, "</query_plan_bundle>\n");               \
        }
                    
#endif  /* PLAN_BUNDLE_H */

/* vim:set shiftwidth=4 expandtab: */
