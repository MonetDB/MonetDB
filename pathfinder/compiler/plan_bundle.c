/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Helper functions for the plan bundle treatment.
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

#include "plan_bundle.h"

/**
 * Helper function to print the property structure 
 * to a an character array @a a.
 */
void
PFla_pb_property_print (PFchar_array_t *a,
                        PFla_pb_item_property_t property,
                        unsigned int indent_size)
{
    unsigned int i;
    char         indent[indent_size+1];

    for (i = 0; i < indent_size; i++)
        indent[i] = ' ';
    indent[indent_size] = '\0';

    PFarray_printf (a, "%s<property name=\"%s\"", indent, property.name);

    if (property.value)
        PFarray_printf (a, " value=\"%s\"", property.value);

    if (property.properties) {
        PFarray_printf (a, ">\n");
        for (i = 0; i < PFarray_last (property.properties); i++)
            PFla_pb_property_print (
                a, 
                *(PFla_pb_item_property_t *) PFarray_at (property.properties,
                                                         i),
                indent_size + 2);
        PFarray_printf (a, "%s</property>\n", indent);
    }
    else
        PFarray_printf (a, "/>\n");
}

/* vim:set shiftwidth=4 expandtab: */
