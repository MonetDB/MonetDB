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

#include "algopt.h"
#include "map_names.h"
#include "oops.h"
#include "timer.h"
#include "algebra_cse.h"
#include "la_proxy.h"

#define MAP_ORI_NAMES(phase)                                                \
        if (unq_names) {                                                    \
            PFinfo (OOPS_WARNING,                                           \
                    "%s requires original names - "                         \
                    "automatical mapping added", phase);                    \
                                                                            \
            tm = PFtimer_start ();                                          \
                                                                            \
            root = PFmap_ori_names (root);                                  \
                                                                            \
            tm = PFtimer_stop (tm);                                         \
                                                                            \
            if (timing)                                                     \
                PFlog ("   map to original attribute names:    %s",         \
                       PFtimer_str (tm));                                   \
                                                                            \
            unq_names = false;                                              \
        }

#define REMOVE_PROXIES(phase)                                               \
        if (proxies_involved) {                                             \
            PFinfo (OOPS_WARNING,                                           \
                    "%s does not cope with proxy nodes - "                  \
                    "proxy nodes are automatical removed", phase);          \
                                                                            \
            tm = PFtimer_start ();                                          \
                                                                            \
            root = PFresolve_proxies (root);                                \
                                                                            \
            tm = PFtimer_stop (tm);                                         \
                                                                            \
            if (timing)                                                     \
                PFlog ("   resolve proxy operators:\t    %s",               \
                       PFtimer_str (tm));                                   \
                                                                            \
            proxies_involved = false;                                       \
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
    bool proxies_involved = false;
    char *args = PFstate.opt_alg;

    while (*args) {
        switch (*args) {
            case 'A': /* disabled */
                /*
                tm = PFtimer_start ();
                
                root = PFalgopt_card (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   cardinality optimization:\t    %s",
                           PFtimer_str (tm));
                */
                break;

            case 'C':
                MAP_ORI_NAMES("complex optimization")
                REMOVE_PROXIES("complex optimization")

                tm = PFtimer_start ();
                
                root = PFalgopt_complex (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   complex optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'O':
                REMOVE_PROXIES("constant optimization")

                tm = PFtimer_start ();
                
                root = PFalgopt_const (root, const_no_attach);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   constant optimization:\t    %s",
                           PFtimer_str (tm));
                /* avoid adding attach nodes in the following
                   constant optimization runs */
                const_no_attach = true;
                break;

            case 'D':
                REMOVE_PROXIES("domain optimization")

                tm = PFtimer_start ();
                
                root = PFalgopt_dom (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   domain optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'G':
                REMOVE_PROXIES("general optimization")

                tm = PFtimer_start ();
                
                root = PFalgopt_general (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   general optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'I':
                MAP_ORI_NAMES("icol optimization")

                tm = PFtimer_start ();
                
                root = PFalgopt_icol (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   icol optimization:\t\t    %s",
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
                REMOVE_PROXIES("equi-join pushdown")

                tm = PFtimer_start ();
                
                root = PFalgopt_join_pd (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   equi-join pushdown:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'K':
                MAP_ORI_NAMES("key optimization")
                REMOVE_PROXIES("key optimization")

                tm = PFtimer_start ();
                
                root = PFalgopt_key (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   key optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'M':
                MAP_ORI_NAMES("mvd optimization")

                tm = PFtimer_start ();

                /* give up rewriting after 20 noneffective
                   cross product - cross product rewrites. */
                root = PFalgopt_mvd (root, 20);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   mvd optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'S':
                /* we need the icols property and thus cannot 
                   apply the optimization if our names are unique */
                MAP_ORI_NAMES("set optimization")
                    
                tm = PFtimer_start ();
                
                root = PFalgopt_set (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   set optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'V':
                MAP_ORI_NAMES("required value optimization")
                REMOVE_PROXIES("required value optimization")

                tm = PFtimer_start ();
                
                root = PFalgopt_reqval (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   required value optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'P':
                tm = PFtimer_start ();
                
                if (unq_names)
                    PFprop_infer (true  /* card */,
                                  true  /* const */,
                                  true  /* set */,
                                  true  /* dom */,
                                  false /* icol */,
                                  true  /* key */,
                                  false /* ocols */, 
                                  false /* reqval */,
                                  true  /* original names */,
                                  false /* unique names */,
                                  root);
                
                else
                    PFprop_infer (true  /* card */,
                                  true  /* const */,
                                  true  /* set */,
                                  true  /* dom */,
                                  true  /* icol */,
                                  true  /* key */,
                                  true  /* ocols */, 
                                  true  /* reqval */,
                                  false /* original names */,
                                  true  /* unique names */,
                                  root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   complete property inference:\t    %s",
                           PFtimer_str (tm));
                break;

            case '[':
                if (unq_names) {
                    PFinfo (OOPS_WARNING,
                            "already using unique attribute names");
                    break;
                }
                REMOVE_PROXIES("variable name mapping")

                tm = PFtimer_start ();
                
                root = PFmap_unq_names (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   map to unique attribute names:   %s",
                           PFtimer_str (tm));

                unq_names = true;
                break;

            case ']':
                if (!unq_names) {
                    PFinfo (OOPS_WARNING,
                            "already using original attribute names");
                    break;
                }
                REMOVE_PROXIES("variable name mapping")

                tm = PFtimer_start ();
                
                root = PFmap_ori_names (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   map to original attribute names: %s",
                           PFtimer_str (tm));

                unq_names = false;
                break;

            case '}':
                MAP_ORI_NAMES("proxy introduction")

                proxies_involved = true;

                tm = PFtimer_start ();
                
                root = PFintro_proxies (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   introduce proxy operators:\t    %s",
                           PFtimer_str (tm));
                break;

            case '{':
                proxies_involved = false;

                tm = PFtimer_start ();
                
                root = PFresolve_proxies (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   resolve proxy operators:\t    %s",
                           PFtimer_str (tm));
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
        root = PFla_cse (root);
    }

    if (unq_names)
        PFinfo (OOPS_WARNING,
                "Physical algebra requires original names. "
                "Add ']' optimization option (at the end) to "
                "ensure correct attribute name usage.");

    if (proxies_involved)
        PFinfo (OOPS_WARNING,
                "Physical algebra does not cope with proxies. "
                "Add '{' optimization option (at the end) to "
                "ensure that every operator is known in the "
                "physical algebra.");

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
