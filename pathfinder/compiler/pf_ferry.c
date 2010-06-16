/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Compiler Driver for the Ferry/Pathfinder compiler.
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
 */

#include "pf_config.h"
#include "pathfinder.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#if HAVE_SIGNAL_H
#include <signal.h>
#endif

#include "pf_ferry.h"
#include "oops.h"
#include "mem.h"
#include "array.h"
#include "qname.h"        /* Pathfinder's QName handling */
#include "nsres.h"        /* namespace resolution */
/* include libxml2 library to parse module definitions from an URI */
#include "libxml/xmlIO.h"
#include "xml2lalg.h"     /* xml importer */
#include "algopt.h"
#include "logdebug.h"
#include "lalg2sql.h"
#include "sql_opt.h"
#include "sqlprint.h"

#if HAVE_SIGNAL_H
/**
 * This handler is called whenever we get a SIGSEGV.
 *
 * It will print out some informative message and then terminate the program.
 */
RETSIGTYPE
segfault_handler (int sig)
{
    fprintf (stderr,
             "!ERROR: Segmentation fault.\n"
             "The Pathfinder module experienced an internal problem.\n"
             "You may want to report this problem to the Pathfinder \n"
             "development team (pathfinder@pathfinder-xquery.org).\n\n"
             "When reporting problems, please attach your input query.\n\n"
             "We apologize for the inconvenience...\n");

    signal (sig, SIG_DFL);
    raise (sig);
}
#endif

jmp_buf PFexitPoint;

/**
 * Accept a logical query plan bundle in XML format, optimize it based
 * on the argument @a opt_args or (if missing) the default optimization
 * arguments in PFopt_args, and transform it into one of the output formats.
 *
 * @param res      contains a pointer to the result string after completion
 * @param err      an empty string (with memory initialized) storing the error
 *                 messages after completion
 * @param xml      the input XML plan
 * @param format   the output format (see definition of PFoutput_format_t)
 * @param opt_args the optimization arguments (see pf option -o)
 * @return         the return code
 */
int
PFcompile_ferry_opt (char **res, 
                     char *err,
                     char *xml,
                     PFoutput_format_t format,
                     char *opt_args)
{

    /* Call setjmp() before variables are declared;
     * otherwise, some compilers complain about clobbered variables.
     */
    int rtrn = 0;
    if ((rtrn = setjmp(PFexitPoint)) != 0 ) {
        PFmem_destroy ();
        return rtrn<0 ? -rtrn : rtrn;
    }

 {
    PFla_pb_t       *lapb = NULL;
    XML2LALGContext *ctx;
    PFarray_t       *output;

    /* setup the error buffer (needed for error handling and segfault trap) */
    PFerrbuf = err;

    /* create segfault trap to provide an error instead of a segfault */
#if HAVE_SIGNAL_H
    /* setup sementation fault signal handler */
    signal (SIGSEGV, segfault_handler);
#endif

    /* Initialize memory allocation facility. */
    PFmem_init ();
    
    /* Initialize data structures in the Namespace department */
    PFns_init ();

    /* Initialize data structures in the QName department */
    PFqname_init ();

    /*
     * XML IMPORT
     */

    /* Get an XML context */
    ctx = PFxml2la_xml2lalgContext();

    lapb = PFxml2la_importXMLFromMemory (ctx, xml, strlen (xml));

    /*
     * QUERY PLAN OPTIMIZATION
     */

    /* Use the default optimization arguments 
       if no optimizations are available. */
    if (!opt_args)
        opt_args = PFopt_args;

    /* Rewrite/optimize the algebra plans. */
    for (unsigned int i = 0; i < PFla_pb_size(lapb); i++)
        PFla_pb_op_at (lapb, i) 
            = PFalgopt (PFla_pb_op_at (lapb, i), false, NULL, opt_args);

    /*
     * OUTPUT GENERATION
     */

    output = PFarray (sizeof (char), 4096);
    /* Create the strings for the different output formats. */
    switch (format) {
        case PFoutput_format_sql:
            /**
             * Iterate over the plan_bundle list lapb and bind every item to laroot.
             *
             * BEWARE: macro has to be used in combination with macro PFla_pb_end.
             */
            PFla_pb_foreach (output, laroot, lapb, NULL)



                /* plan bundle emits SQL code wrapped in XML tags */ 
                if (lapb)
                    PFarray_printf (output, 
                                    "    <query>"
                                    "<![CDATA[\n");

                PFsql_print (output, PFsql_opt (PFlalg2sql (laroot)));

                /* plan bundle emits SQL code wrapped in XML tags */ 
                if (lapb)
                    PFarray_printf (output, 
                                    "]]>"
                                    "</query>\n");



            /**
             * Iterate over the plan_bundle list lapb and bind every item to laroot.
             *
             * BEWARE: macro has to be used in combination with macro PFla_pb_foreach.
             */
            PFla_pb_foreach_end (output, lapb)
            break;

        case PFoutput_format_xml:
            PFla_xml_bundle (output, lapb, "" /* no format */);
            break;

        case PFoutput_format_dot:
            PFla_dot_bundle (output, lapb, "" /* no format */);
            break;
    }

    /* Provide a new (malloced) copy of the output string as result. */
    *res = malloc (strlen (output->base) + 1);
    if (! *res)
        PFoops (OOPS_OUTOFMEM, "problem allocating memory");

    strcpy (*res, output->base);

    /* Release the internally allocated memory. */
    PFmem_destroy ();

    return 0; /* EXIT_SUCCESS */
 }
}

/**
 * Accept a logical query plan bundle in XML format
 * and transform it into one of the output formats.
 *
 * @param res    contains a pointer to the result string after completion
 * @param err    an empty string (with memory initialized) storing the error
 *               messages after completion
 * @param xml    the input XML plan
 * @param format the output format (see definition of PFoutput_format_t)
 * @return       the return code
 */
int
PFcompile_ferry (char **res,
                 char *err,
                 char *xml,
                 PFoutput_format_t format)
{
    return PFcompile_ferry_opt (res, err, xml, format, "P");
}

/* vim:set shiftwidth=4 expandtab: */
