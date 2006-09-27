/**
 * @file
 *
 * Resolve proxy operators.
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

#include "la_proxy.h"
#include "alg_dag.h"
#include "properties.h"

#define SEEN(p) ((p)->bit_dag)

static void
resolve_proxies (PFla_op_t *p)
{
    assert (p);

    /* look at each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* traverse children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        resolve_proxies (p->child[i]);

    if (p->kind == la_proxy || p->kind == la_proxy_base)
        *p = *PFla_dummy (p->child[0]);
}

/**
 * Resolve proxy operators.
 */
PFla_op_t *
PFresolve_proxies (PFla_op_t *root)
{
    resolve_proxies (root);
    PFla_dag_reset (root);
    /* ensure that each operator has its own properties */
    PFprop_create_prop (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
