/**
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

#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* alternative definitions for strdup and strndup
 * and some helper functions
 */
#include "shred_helper.h"
/* hashtable support */
#include "hash.h"
/* guides */
/* main shredding */
#include "encoding.h"
/* ... SHoops */
#include "oops.h"

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"


#if HAVE_GETOPT_H && HAVE_GETOPT_LONG
#include <getopt.h>

/**
 * long (GNU-style) command line options and their abbreviated
 * one-character variants.  Keep this list SORTED in ascending
 * order of one-character option names.
 */
static struct option long_options[] = {
    { "suppress_attributes", no_argument,       NULL, 'a' },
    { "in-file",             required_argument, NULL, 'f' },
    { "out-file",            required_argument, NULL, 'o' },
    { "format",              required_argument, NULL, 'F' },
    { "sql",                 no_argument,       NULL, 's' },
    { "help",                no_argument,       NULL, 'h' },
};

/**
 * character buffer large enough to hold longest
 * command line option plus some extra formatting space
 */
static char opt_buf[sizeof ("print-abstract-syntax-tree") + 8];

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
                                        cmp_opt)))
        return snprintf (buf, sizeof (opt_buf), t, l->name), buf;
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

#define SQL_FORMAT "%e, %s, %l, %k, %n, %t, %g";

shred_state_t *status;  

/* Print help message */
static void
print_help (char *progname)
{
    printf ("Pathfinder XML-Shredder\n");
    printf ("encode XML-Documents in different encodings.\n");
    printf ("(c) Database Group, Technische Universitaet Muenchen\n\n");

    printf ("Usage: %s [OPTION] -f [FILE] -o [FILE]\n\n", progname);            

    printf ("  -h%s: print this help message\n",
            long_option (opt_buf, ", --%s", 'h'));
    printf ("  -f filename%s: parse this XML-Document\n",
            long_option (opt_buf, ", --%s=filename", 'f'));
    printf ("  -o filename%s: writes output to this file(s)\n",
            long_option (opt_buf, ", --%s=filename", 'o'));
    printf ("  -a%s: suppress attributes\n",
            long_option (opt_buf, ", --%s", 'a'));
    printf ("  -s%s: sql encoding supported by pathfinder\n"
                "\t\t(that is probably what you want)\n",
                long_option (opt_buf, ", --%s", 's'));
}

#define MAIN_EXIT(rtn) \
        do { \
                   exit (rtn); \
           } while (0)
int
main (int argc, char **argv)
{
    static char *progname = NULL;

    status = malloc (sizeof (shred_state_t));

	FILE * shout    = NULL;
	FILE * attout   = NULL;
	FILE * guideout = NULL;

    /*
     * Determine basename(argv[0]) and dirname(argv[0]) on
     * of argv[0] as both functions may modify their arguments.
     */
    progname = strndup (argv[0], FILENAME_MAX);

    /* parse command line using getopt library */
    while (true) {
        int c; 

        #define OPT_STRING "af:o:F:sh"

#if HAVE_GETOPT_H && HAVE_GETOPT_LONG
        int option_index = 0;
        c = getopt_long (argc, argv, OPT_STRING, 
                         long_options, &option_index);
#else
        c = getopt (argc, argv, OPT_STRING);
#endif
        status->format = SQL_FORMAT; 

        if (c == -1)
            break;
        switch (c) {
            case 'a':
                status->suppress_attributes = true;
                break;
            case 'F':
                status->format = strdup (optarg);
                break;
            case 'f':
                status->infile = strndup (optarg, FILENAME_MAX);
                if (!SHreadable (status->infile)) {
                    SHoops (SH_FATAL, "This file doesn't exists or "
                                      "you don't have the permission "
                                      "to read it.\n");
                    goto failure;
                }
                status->infile_given = (status->infile != NULL);
                break;
            case 'o':
                status->outfile = strndup (optarg, FILENAME_MAX);
                status->outfile_given = (status->outfile != NULL);
                break;
            case 's':
                status->sql = true;
                status->format = "%e, %s, %l, %k, %n, %t, %g";
                break;
            case 'h':
                print_help (progname);
                exit (0);
        }
    }

    if (!status->infile_given) {
        SHoops (SH_FATAL, "Input filename required.\n");
        print_help (progname);
        goto failure;
    }

    if (!status->outfile_given) {
        SHoops (SH_FATAL, "Output filename required.\n");
        print_help (progname);
        goto failure;
    }


    /* attribute file */
	char attoutfile[FILENAME_MAX];
	snprintf (attoutfile, FILENAME_MAX,
	          (!status->sql)?"%s_atts":"%s_names",
			  status->outfile);

    /* guide file */
    char guideoutfile[FILENAME_MAX];
    snprintf (guideoutfile, FILENAME_MAX,
              "%s_guide.xml", status->outfile);

    /* Open files */ 
	shout = SHopen_write (status->outfile);
	attout = SHopen_write (attoutfile);

    if (status->sql) guideout = SHopen_write (guideoutfile);

    /* shred the files */
    if (SHshredder (status->infile, shout, attout, guideout, status) < 0)
        goto failure;

    fclose (shout);
	fclose (attout);
	if (status->sql) fclose (guideout);

    free (status);

    MAIN_EXIT (EXIT_SUCCESS);
failure:
    free (status);

    MAIN_EXIT (EXIT_FAILURE);
}
