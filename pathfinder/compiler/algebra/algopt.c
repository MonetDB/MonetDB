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
#include <stdlib.h>

#include "algopt.h"
#include "map_names.h"
#include "oops.h"
#include "timer.h"
#include "opt_algebra_cse.h"
#include "algebra_cse.h"
#include "la_proxy.h"
#include "la_thetajoin.h"

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
                PFlog ("   map to original column names:    %s",            \
                       PFtimer_str (tm));                                   \
                                                                            \
            unq_names = false;                                              \
        }

#define MAP_UNQ_NAMES(phase)                                                \
        if (!unq_names) {                                                   \
            PFinfo (OOPS_WARNING,                                           \
                    "%s requires unique names - "                           \
                    "automatical mapping added", phase);                    \
                                                                            \
            tm = PFtimer_start ();                                          \
                                                                            \
            root = PFmap_unq_names (root);                                  \
                                                                            \
            tm = PFtimer_stop (tm);                                         \
                                                                            \
            if (timing)                                                     \
                PFlog ("   map to unique column names:    %s",              \
                       PFtimer_str (tm));                                   \
                                                                            \
            unq_names = true;                                               \
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
PFalgopt (PFla_op_t *root, bool timing, PFguide_list_t* guide_list,
          char *opt_args)
{
    bool debug_opt = getenv("PF_DEBUG_OPTIMIZATIONS") != NULL;
    long tm;
    bool const_no_attach = false;
    bool unq_names = true;
    bool proxies_involved = false;

    /* make sure that we can rely on the 'correct' usage of column names */
    tm = PFtimer_start ();
    root = PFmap_unq_names (root);
    tm = PFtimer_stop (tm);
    if (timing)
        PFlog ("   map to unique column names:\t    %s",
               PFtimer_str (tm));

    if (debug_opt)
        fprintf (stderr, "-o");

    while (*opt_args) {
        root = PFla_cse (root);

        if (debug_opt)
            fputc (*opt_args, stderr);

        switch (*opt_args) {
            case 'A':
                tm = PFtimer_start ();

                root = PFalgopt_step_join (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   step-join push-down:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'C':
                REMOVE_PROXIES("complex optimization")

                tm = PFtimer_start ();

                root = PFalgopt_complex (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   complex optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'D':
                REMOVE_PROXIES("MonetDB specific optimization")

                tm = PFtimer_start ();

                root = PFalgopt_monetxq (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   MonetDB specific optimizations:  %s",
                           PFtimer_str (tm));
                break;
                
            case 'E':
                MAP_UNQ_NAMES("common subexpression elimination")
                REMOVE_PROXIES("common subexpression elimination")

                tm = PFtimer_start ();

                root = PFalgopt_cse (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog("   structural CSE optimization:\t    %s",
                            PFtimer_str (tm));
                break;

            case 'O':
                MAP_UNQ_NAMES("constant optimization")
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
                tm = PFtimer_start ();

                root = PFalgopt_icol (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   icol optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'J':
                MAP_UNQ_NAMES("equi-join pushdown")
                REMOVE_PROXIES("equi-join pushdown")

                tm = PFtimer_start ();

                root = PFalgopt_join_pd (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   equi-join pushdown:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'K':
                REMOVE_PROXIES("key optimization")

                tm = PFtimer_start ();

                root = PFalgopt_key (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   key optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'M':
                MAP_UNQ_NAMES("mvd optimization")

                tm = PFtimer_start ();

                /* give up rewriting after 20 noneffective
                   cross product - cross product rewrites. */
                root = PFalgopt_mvd (root, 20);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   mvd optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'N':
                MAP_UNQ_NAMES("required nodes optimization")
                tm = PFtimer_start ();

                root = PFalgopt_req_node (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   required nodes optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'Q':
                proxies_involved = false;

                tm = PFtimer_start ();

                root = PFalgopt_join_graph (root, guide_list);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   join-graph optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'R':
                MAP_UNQ_NAMES("rank optimization")
                proxies_involved = false;

                tm = PFtimer_start ();

                root = PFalgopt_rank (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   rank optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'S':
                tm = PFtimer_start ();

                root = PFalgopt_set (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   set optimization:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'T':
                MAP_UNQ_NAMES("thetajoin optimization")

                tm = PFtimer_start ();

                root = PFintro_thetajoins (root);
                root = PFalgopt_thetajoin (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   thetajoin optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'U':
                if (!guide_list) break;
                tm = PFtimer_start ();

                root = PFalgopt_guide (root, guide_list);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   guide optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'V':
                REMOVE_PROXIES("required value optimization")

                tm = PFtimer_start ();

                root = PFalgopt_reqval (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   required value optimization:\t    %s",
                           PFtimer_str (tm));
                break;

            case 'Y':
                REMOVE_PROXIES("projection removal")

                tm = PFtimer_start ();

                root = PFalgopt_projection (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   projection removal:\t\t    %s",
                           PFtimer_str (tm));
                break;

            case 'P':
                tm = PFtimer_start ();

                PFprop_infer (true  /* card */,
                              true  /* const */,
                              true  /* set */,
                              true  /* dom */,
                              true  /* lineage */,
                              true  /* icol */,
                              true  /* composite key */,
                              true  /* key */,
                              true  /* fd */,
                              true  /* ocols */,
                              true  /* req_node */,
                              true  /* reqval */,
                              true  /* level */,
                              true  /* refctr */,
                              true  /* guides */,
                /* disable the following property as there might
                   be too many columns involved */
                              false /* original names */,
                              true  /* unique names */,
                              root, guide_list);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   complete property inference:\t    %s",
                           PFtimer_str (tm));
                break;

            case '[':
                REMOVE_PROXIES("variable name mapping")

                tm = PFtimer_start ();

                root = PFmap_unq_names (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   map to unique column names:\t   %s",
                           PFtimer_str (tm));

                unq_names = true;
                break;

            case ']':
                if (!unq_names) {
                    PFinfo (OOPS_WARNING,
                            "already using original column names");
                    break;
                }
                REMOVE_PROXIES("variable name mapping")

                tm = PFtimer_start ();

                root = PFmap_ori_names (root);

                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("   map to original column names: %s",
                           PFtimer_str (tm));

                unq_names = false;
                break;

            case '}':
                MAP_UNQ_NAMES("proxy introduction")

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
                        *opt_args);
                break;
        }
        opt_args++;
    }
    if (debug_opt)
        fputc ('\n', stderr);

    if (proxies_involved)
        PFinfo (OOPS_WARNING,
                "Physical algebra does not cope with proxies. "
                "Add '{' optimization option (at the end) to "
                "ensure that every operator is known in the "
                "physical algebra.");

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
