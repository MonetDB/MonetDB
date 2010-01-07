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

#endif  /* PLAN_BUNDLE_H */

/* vim:set shiftwidth=4 expandtab: */
