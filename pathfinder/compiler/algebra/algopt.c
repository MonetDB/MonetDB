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
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"
#include <assert.h>

#include "algopt.h"
#include "oops.h"
#include "timer.h"

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt (PFla_op_t *root, bool timing)
{
    assert (PFstate.opt_alg);

    long tm;
    bool const_no_attach = false;
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
                tm = PFtimer_start ();
                
                root = PFalgopt_icol (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\ticol optimization:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'K':
                tm = PFtimer_start ();
                
                root = PFalgopt_key (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tkey optimization:\t\t %s",
                           PFtimer_str (tm));
                break;

            case 'V':
                tm = PFtimer_start ();
                
                root = PFalgopt_reqval (root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\trequired value optimization:\t %s",
                           PFtimer_str (tm));
                break;

            case 'P':
                tm = PFtimer_start ();
                
                PFprop_infer (true /* card */, true /* const */,
                              true /* dom */, true /* icols */,
                              true /* key */, true /* ocols */, 
                              true /* reqval */, root);
                
                tm = PFtimer_stop (tm);
                if (timing)
                    PFlog ("\tcomplete property inference:\t %s",
                           PFtimer_str (tm));
                break;

            case 'N':
                return root;

            case ' ':
                break;

            default:
                PFinfo (OOPS_WARNING,
                        "discarding unknown optimization option '%c'",
                        *args);
                break;
        }
        args++;
    }

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
