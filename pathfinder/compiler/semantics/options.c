/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Extract options given in the query prolog
 *
 *   declare option ns:loc "option string";
 *
 * and populate a mapping table #PFoptions
 *
 *   ns:loc     -->     "option string"
 *
 * for use by later compilation stages.
 *
 * Example usage of #PFoptions:
 *
 *   PFarray_t *options;
 *
 *   options = PFenv_lookup (PFoptions, PFqname (PFns_lib, "foo"));
 *
 *   if (!options)
 *       fprintf (stderr, "pf:foo not set.\n");
 *   else
 *       for (unsigned int i = 0; i < PFarray_last (options); i++)
 *           fprintf (stderr,
 *                    "pf:foo set to `%s'.\n",
 *                    *((char **) PFarray_at (options, i)));
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include <assert.h>

#include "options.h"

#define L(p) ((p)->child[0])
#define R(p) ((p)->child[1])

/**
 * This is the environment we populate.
 */
PFenv_t *PFoptions;

static void
recurse (PFpnode_t *n)
{
    assert (n);

    switch (n->kind) {

        case p_option:
            PFenv_bind (PFoptions, n->sem.qname, L(n)->sem.str);
            break;

        case p_main_mod:
            recurse (L(n));
            break;

        default:
            if (L(n))
                recurse (L(n));
            if (R(n))
                recurse (R(n));
            break;
    }
}

void
PFextract_options (PFpnode_t *root)
{
    assert (root);

    PFoptions = PFenv ();

    recurse (root);

    return;
}

/* vim:set shiftwidth=4 expandtab: */
