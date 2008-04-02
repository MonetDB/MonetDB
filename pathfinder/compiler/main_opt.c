/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Entry point to Pathfinder Logical Algebra Optimizer.
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"

#include <stdlib.h>
#ifdef HAVE_LIBGEN_H
#include <libgen.h>
#endif
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#if HAVE_SIGNAL_H
#include <signal.h>
#endif

#ifndef NDEBUG

#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#ifndef HAVE_STRCASECMP
#define strcasecmp(a,b) strcmp ((a), (b))
#endif

#endif

#if HAVE_GETOPT_H && HAVE_GETOPT_LONG
#include <getopt.h>

/**
 * long (GNU-style) command line options and their abbreviated
 * one-character variants.  Keep this list SORTED in ascending
 * order of one-character option names.
 */
static struct option long_options[] = {
    { "format",                    required_argument, NULL, 'f' },
    { "help",                      no_argument,       NULL, 'h' },
    { "optimize-algebra",          required_argument, NULL, 'o' },
    { NULL,                        no_argument,       NULL, 0 }
};

/**
 * character buffer large enough to hold longest
 * command line option plus some extra formatting space
 */
static char opt_buf[sizeof ("optimize-algebra") + 10];

static int
cmp_opt (const void *o1, const void *o2)
{
    return ((struct option *)o1)->val - ((struct option *)o2)->val;
}

/**
 * map a one-character command line option to its equivalent
 * long form
 */
static const char
*long_option (char *buf, char *t, char o)
{
    struct option key = { 0, 0, 0, o };
    struct option *l;

    if ((l = (struct option *) bsearch (&key, long_options,
                                        sizeof (long_options) /
                                            sizeof (struct option) - 1,
                                        sizeof (struct option),
                                        cmp_opt))) {
        snprintf (buf, sizeof (opt_buf), t, l->name);
        buf[sizeof (opt_buf) - 1] = 0;
        return buf;
    }
    else
        return "";
}

#else

#ifndef HAVE_GETOPT_H

#include "win32_getopt.c" /* fall back on a standalone impl */

#define getopt win32_getopt
#define getopt_long win32_getopt_long
#define getopt_long_only win32_getopt_long_only
#define _getopt_internal _win32_getopt_internal
#define opterr win32_opterr
#define optind win32_optind
#define optopt win32_optopt
#define optarg win32_optarg
#endif

/* no long option names w/o GNU getopt */
#include <unistd.h>

#define long_option(buf,t,o) ""
#define opt_buf 0

#endif

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
             "The Pathfinder Logical Algebra Optimizer experienced "
             "an internal problem.\n"
             "You may want to report this problem to the Pathfinder \n"
             "development team (pathfinder@pathfinder-xquery.org).\n\n"
             "When reporting problems, please attach your XQuery input.\n\n"
             "We apologize for the inconvenience...\n\n");

    signal (sig, SIG_DFL);
    raise (sig);
}
#endif

jmp_buf PFexitPoint;

#include "oops.h"
#include "mem.h"
#include "array.h"
#include "qname.h"        /* Pathfinder's QName handling */
#include "ns.h"           /* namespaces */
#include "nsres.h"        /* namespace resolution */
/* include libxml2 library to parse module definitions from an URI */
#include "libxml/xmlIO.h"
#include "xml2lalg.h"     /* xml importer */
#include "algopt.h"
#include "logdebug.h"

#define MAIN_EXIT(rtrn) \
        fputs (PFerrbuf, stderr);\
        exit (rtrn);

/**
 * Entry point to the Pathfinder compiler,
 * parses the command line (switches), then invokes the compiler driver
 * function PFcompile();
 */
int
main (int argc, char *argv[])
{


    /* Call setjmp() before variables are declared;
     * otherwise, some compilers complain about clobbered variables.
     */
    int rtrn = 0;
    if ((rtrn = setjmp(PFexitPoint)) != 0 ) {
        PFmem_destroy ();
        MAIN_EXIT ( rtrn<0 ? -rtrn : rtrn );
    }

 {
    /**
     * @c basename(argv[0]) is stored here later. The basename() call may
     * modify its argument (according to the man page). To avoid modifying
     * @c argv[0] we make a copy first and store it here.
     */
    static char *progname = 0;

    char *opt_args  = "OIKDCG_VGO_[J]OKVCG"
                          "}IM__{_[J]OKVCG"
                          "}IM__{_[J]OKVCGCG"
                          "}IM__{_[J]OKVCG"
                          "}IMTS{_[J]OKVCGUCG"
                          "}IMTS{_[J]OKVCGU"
                          "}IMTS{_[J]OKVCGUE[]CGP"
                          "}IQ[J]}IQ[J]IOKVCGQUCGP",
         *prop_args = NULL;

    /* URL of query file (if present) */
    char *url = "-";

    PFerrbuf = malloc(OOPS_SIZE);
    PFerrbuf[0] = 0;

    PFmem_init ();
    /*
     * Determine basename(argv[0]) and dirname(argv[0]) on *copies*
     * of argv[0] as both functions may modify their arguments.
     */
#ifdef HAVE_BASENAME
    progname = basename (PFstrdup (argv[0]));
#else
    progname = PFstrdup (argv[0]);
#endif
    /*pathname = dirname (PFstrdup (argv[0]));*/ /* StM: unused! */

#ifndef HAVE_GETOPT_H
    win32_getopt_reset();
#endif
    
    /* getopt-based command line parsing */
    while (true) {
        int c;
#if HAVE_GETOPT_H && HAVE_GETOPT_LONG
        int option_index = 0;
        opterr = 1;
        c = getopt_long (argc, argv, "f:ho:",
                         long_options, &option_index);
#else
        c = getopt (argc, argv, "f:ho:");
#endif

        if (c == -1)
            break;            /* end of command line */
        switch (c) {
            case 'f':
                if (!prop_args)
                    prop_args = PFstrdup (optarg);
                else {
                    prop_args = PFrealloc (prop_args,
                                           strlen (prop_args)+1,
                                           strlen (prop_args)
                                           + strlen (optarg) +1);
                    strcat (prop_args, optarg);
                }
                break;

            case 'h':
                printf ("\n      Pathfinder Logical Algebra Optimizer\n\n"
                        "Reads in XML Representation of Logical Algebra Dag\n"
                        "and returns the optimized Logical Plan in XML format."
                        "\n\n"
                        "($Revision$, $Date$)\n");
                printf ("(c) Database Group, Technische Universitaet Muenchen\n\n");
                printf ("Usage: %s [OPTION] [FILE]\n\n", argv[0]);
                printf ("  Reads from standard input if FILE is omitted.\n\n");
                printf ("  -o<options>%s: optimize algebra according to "
                                            "options:\n",
                        long_option (opt_buf, ", --%s=<options>", 'o'));

                printf ("         O  apply optimization based on constant "
                                            "property\n");
                printf ("         I  apply optimization based on icols "
                                            "property\n");
                printf ("         K  apply optimization based on key "
                                            "property\n");
                printf ("         D  apply optimization based on domain "
                                            "property\n");
                printf ("         C  apply optimization using multiple "
                                            "properties (complex)\n");
                printf ("            (and icols based optimization will be "
                                            "applied afterwards)\n");
                printf ("         G  apply general optimization (without "
                                            "properties)\n");
                printf ("         V  apply optimization based on required "
                                            "values property\n");
                printf ("         [  map column names to unique column "
                                            "names\n");
                printf ("         J  push down equi-joins (requires unique "
                                            "column names)\n");
                printf ("         ]  map column names back (from unique "
                                            "names to original names)\n");
                printf ("         M  apply optimization based on multi-value "
                                            "dependencies\n");
                printf ("         S  apply optimization based on set "
                                            "property\n");
                printf ("         }  introduce proxy operators that represent"
                                            "operator groups\n");
                printf ("         {  remove proxy operators\n");
                printf ("         P  infer all properties\n");
                printf ("         _  does nothing (used for structuring "
                                            "the options)\n");
                printf ("            (default is: '-o %s')\n",
                        opt_args);
                printf ("  -f<options>%s: enable generation of algebra "
                        "properties (choose same options as -o)\n",
                        long_option (opt_buf, ", --%s=<options>", 'f'));
                printf ("         +  print all available properties\n");
                printf ("\n");
                printf ("  -h%s: print this help message\n",
                        long_option (opt_buf, ", --%s", 'h'));
                printf ("\n");
                printf ("Enjoy.\n");
                exit (0);

            case 'o':
                opt_args = PFstrdup (optarg);
                break;

            default:
                PFoops (OOPS_CMDLINEARGS, "try `%s -h'", progname);
                break;

        }           /* end of switch */
    }           /* end of while */

    PFla_op_t  *laroot  = NULL;

#if HAVE_SIGNAL_H
    /* setup sementation fault signal handler */
    signal (SIGSEGV, segfault_handler);
#endif

    XML2LALGContext *ctx = PFxml2la_xml2lalgContext();

    /* Initialize data structures in the Namespace department */
    PFns_init ();

    /* Initialize data structures in the QName department */
    PFqname_init ();

    /* should the input explicitely be read from a file (instead of stdin)? */
    if (optind < argc) {
        /**
         * If the input is explicitely specified with an filename,
         * we drive the xml-importer with this filename instead with the
         * url-cache-buffer.
         * Reason: If the validation of the input-file against a schema
         * fails, we explicitely get the locations in the file
         * (linenumbers) from the xml-parser. But only, if the parser is
         * driven with an explicit filename. If the parser is driven
         * with an buffer, then we don't get explicit error-positions
         * from the parser in case of validation errors (which makes
         * fixing/locating this validation-errors unneccessary hard)
         */
        laroot = PFxml2la_importXMLFromFile (ctx, argv[optind]);
    }
    else {
        /**
         * Note, that we don't get explicit error-positions (line
         * numbers of the xml-input) from the parser in case of
         * validation errors...
         *
         * Call PF from the command line with an explicit filename
         * (instead of using stdin/pipelining) if you want to get
         * explicit error positions (line numbers) from the xml-parser
         * in case of validation-errors
         */
        int num_read = -1;
        xmlParserInputBufferPtr  buf;
        char *xml;

        /*
         * open file via parser from the libxml2 library
         * (capable of loading URIs also via HTTP or FTP)
         */
        buf = xmlParserInputBufferCreateFilename (url, XML_CHAR_ENCODING_NONE);
        if (buf) {
            /*
             * Load in chunks of 2048 chars.  libxml2 does not really
             * stick to these 2048 chars, but we don't care anyway...
             */
            while ((num_read = xmlParserInputBufferGrow (buf, 2048)))
                /* empty */;
        }
        if (num_read < 0) {
            if (buf) free(buf);
            PFoops (OOPS_FATAL, "Cannot read input query");
        }
        xml = (char*) buf->buffer->content;
        buf->buffer->content = NULL; /* otherwise it will be deleted */
        xmlFreeParserInputBuffer (buf);
        laroot = PFxml2la_importXMLFromMemory (ctx, xml, strlen (xml));
    }

    /*
     * Rewrite/optimize algebra tree
     */
    laroot = PFalgopt (laroot, false, NULL, opt_args);
    PFla_xml (stdout, laroot, prop_args);

    PFmem_destroy ();

    exit (EXIT_SUCCESS);
 }
}

/* vim:set shiftwidth=4 expandtab: */
