/**
 * @file
 *
 * Optimize relational algebra expression tree.
 * (The optimization phases are implemented in separate files
 *  in subfolder opt/.)
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2006 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"
#include <assert.h>

#include "algopt.h"
#include "map_names.h"
#include "oops.h"
#include "timer.h"

static PFla_op_t *
magic_map_ori_names (PFla_op_t *root, char *phase)
{
    PFinfo (OOPS_WARNING,
            "%s requires original names - "
            "automatical mapping added", phase);
    return PFmap_ori_names (root);
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt (PFla_op_t *root, bool timing)
{
    assert (PFstate.opt_alg);

    long tm;
    bool const_no_attach = false;
    bool unq_names = false;
    char *args = PFstate.opt_alg;

    while (*args) {
        switch (*args) {
            case 'A': /* disabled */
                /*
                tm = PFtimer_start ();
                
                root = PFalgopt_card (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tcardinality optimization:\t %s",
                           PFtimer_str (tm));
                */
                break;

            case 'C':
                if (unq_names) {
                    root = magic_map_ori_names (root, "complex optimization");
                    unq_names = false;
                }

                tm = PFtimer_start ();
                
                root = PFalgopt_complex (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tcomplex optimization:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'O':
                tm = PFtimer_start ();
                
                root = PFalgopt_const (root, const_no_attach);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tconstant optimization:\t\t %s",
                           PFtimer_str (tm));
                /* avoid adding attach nodes in the following
                   constant optimization runs */
                const_no_attach = true;
                break;

            case 'D':
                tm = PFtimer_start ();
                
                root = PFalgopt_dom (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tdomain optimization:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'G':
                tm = PFtimer_start ();
                
                root = PFalgopt_general (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tgeneral optimization:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'I':
                if (unq_names) {
                    root = magic_map_ori_names (root, "icol optimization");
                    unq_names = false;
                }

                tm = PFtimer_start ();
                
                root = PFalgopt_icol (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\ticol optimization:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'J':
                if (!unq_names) {
                    PFinfo (OOPS_WARNING,
                            "equi-join pushdown requires unique names"
                            " - automatical mapping added");
                    root = PFmap_unq_names (root);
                    unq_names = true;
                }

                tm = PFtimer_start ();
                
                root = PFalgopt_join_pd (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tequi-join pushdown:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'K':
                if (unq_names) {
                    root = magic_map_ori_names (root, "key optimization");
                    unq_names = false;
                }

                tm = PFtimer_start ();
                
                root = PFalgopt_key (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tkey optimization:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'M':
                if (unq_names) {
                    root = magic_map_ori_names (root, "mvd optimization");
                    unq_names = false;
                }

                tm = PFtimer_start ();

                /* give up rewriting after 20 noneffective
                   cross product - cross product rewrites. */
                root = PFalgopt_mvd (root, 20);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tmvd optimization:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'V':
                if (unq_names) {
                    root = magic_map_ori_names (root,
                                                "required value optimization");
                    unq_names = false;
                }

                tm = PFtimer_start ();
                
                root = PFalgopt_reqval (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\trequired value optimization:\t %s",
                           PFtimer_str (tm));
                break;

            case 'P':
                tm = PFtimer_start ();
                
                if (unq_names)
                    PFprop_infer (true  /* card */,
                                  true  /* const */,
                                  true  /* dom */,
                                  false /* icols */,
                                  true  /* key */,
                                  false /* ocols */, 
                                  false /* reqval */,
                                  true  /* original names */,
                                  false /* unique names */,
                                  root);
                
                else
                    PFprop_infer (true  /* card */,
                                  true  /* const */,
                                  true  /* dom */,
                                  true  /* icols */,
                                  true  /* key */,
                                  true  /* ocols */, 
                                  true  /* reqval */,
                                  false /* original names */,
                                  true  /* unique names */,
                                  root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tcomplete property inference:\t %s",
                           PFtimer_str (tm));
                break;

            case '[':
                if (unq_names) {
                    PFinfo (OOPS_WARNING,
                            "already using unique attribute names");
                    break;
                }

                tm = PFtimer_start ();
                
                root = PFmap_unq_names (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tmap to unique attribute names:\t %s",
                           PFtimer_str (tm));

                unq_names = true;
                break;

            case ']':
                if (!unq_names) {
                    PFinfo (OOPS_WARNING,
                            "already using original attribute names");
                    break;
                }

                tm = PFtimer_start ();
                
                root = PFmap_ori_names (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tmap to original attribute names: %s",
                           PFtimer_str (tm));

                unq_names = false;
                break;

            case ' ':
            case '_':
                break;

            default:
                PFinfo (OOPS_WARNING,
                        "discarding unknown optimization option '%c'",
                        *args);
                break;
        }
        args++;
    }

    if (unq_names)
        PFinfo (OOPS_WARNING,
                "Physical algebra requires original names. "
                "Add ']' optimization option (at the end) to "
                "ensure correct attribute name usage.");

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
