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

#include "pf_config.h"
#include <assert.h>
#include <stdio.h>
#ifdef HAVE_STDBOOL_H
#include <stdbool.h>
#endif
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
    { "format",              required_argument, NULL, 'F' },
    { "attributes-separate", no_argument,       NULL, 'a' },
    { "in-file",             required_argument, NULL, 'f' },
    { "help",                no_argument,       NULL, 'h' },
    { "names-inline",        no_argument,       NULL, 'n' },
    { "out-file",            required_argument, NULL, 'o' },
    { NULL,                  no_argument,       NULL, 0   }
};
/* also see definition of OPT_STRING below */

/**
 * character buffer large enough to hold longest
 * command line option plus some extra formatting space
 */
static char opt_buf[sizeof ("attributes-separate") + 8];

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

#define OPT_STRING "F:af:hno:"

#define SQL_FORMAT "%e, %s, %l, %k, %n, %t, %g"

shred_state_t status;  

/* print help message */
static void
print_help (char *progname)
{
    printf ("Pathfinder XML Shredder\n");
    printf ("(c) Database Group, Technische Universitaet Muenchen\n\n");
    printf ("Produces relational encodings of XML input documents, one node/tuple\n\n");

    printf ("Usage: %s [OPTION] -f [FILE] -o [PREFIX]\n\n", progname);            

    printf ("  -f filename%s: encode given input XML document\n",
            long_option (opt_buf, ", --%s=filename", 'f'));
    printf ("  -o prefix%s: writes encoding to file <prefix>.csv\n",
            long_option (opt_buf, ", --%s=prefix", 'o'));
    printf ("  -F format%s: format selected encoding components\n"
            "\t(default: '%s')\n"
            "\t%%e: node preorder rank\n"
            "\t%%o: node postorder rank\n"
            "\t%%E: node preorder rank in stretched pre/post plane\n"
            "\t%%O: node postorder rank in stretched pre/post plane\n"
            "\t%%s: size of subtree below node\n"
            "\t%%l: length of path from root to node (level)\n"
            "\t%%k: node kind\n"
            "\t%%p: preorder rank of parent node\n"
            "\t%%P: preorder rank of parent node in stretched pre/post plane\n"
            "\t%%n: element/attribute name\n"
            "\t%%t: text node content\n"
            "\t%%g: guide node for node (also writes dataguide to file <prefix>_guide.xml)\n",
            long_option (opt_buf, ", --%s=format", 'F'), SQL_FORMAT);
    printf ("  -a%s: attributes separate (default: attributes inline)\n"
            "\twrites attribute encoding to file <prefix>_atts.csv\n",
            long_option (opt_buf, ", --%s", 'a'));
    printf ("  -n%s: tag/attribute names inline (default: names separate)\n"
            "\twrites name encoding to file <prefix>_names.csv\n",
            long_option (opt_buf, ", --%s", 'n'));
    printf ("  -h%s: print this help message\n",
            long_option (opt_buf, ", --%s", 'h'));
}

#define MAIN_EXIT(rtn) \
        do { \
                   exit (rtn); \
           } while (0)
int
main (int argc, char **argv)
{
    char *progname = NULL;

    FILE *shout    = NULL;
    FILE *attout   = NULL;
    FILE *namesout = NULL;
    FILE *guideout = NULL;

    /*
     * Determine basename(argv[0]) and dirname(argv[0]) on
     * of argv[0] as both functions may modify their arguments.
     */
    progname = strndup (argv[0], FILENAME_MAX);

    status.format = SQL_FORMAT; 
    status.infile = NULL;
    status.outfile = NULL;
    status.statistics = true;
    status.names_separate = true;
    status.attributes_separate = false;

    /* parse command line using getopt library */
    while (true) {
        int c; 

#if HAVE_GETOPT_H && HAVE_GETOPT_LONG
        int option_index = 0;
        opterr = 1;
        c = getopt_long (argc, argv, OPT_STRING, 
                         long_options, &option_index);
#else
        c = getopt (argc, argv, OPT_STRING);
#endif

        if (c == -1)
            break;
        switch (c) {
            case 'a':
                status.attributes_separate = true;
                break;
            case 'n':
                status.names_separate = false;
                break;
            case 'F':
                status.format = strdup (optarg);
                status.statistics = false;
                
                for (unsigned int i = 0; status.format[i+1]; i++)
                    if (status.format[i] == '%' &&
                        status.format[i+1] == 'g') {
                        status.statistics = true;
                        break;
                    }
                break;
            case 'f':
                status.infile = strndup (optarg, FILENAME_MAX);
                if (!SHreadable (status.infile)) 
                    SHoops (SH_FATAL, "input XML file not readable\n");
                if (status.infile)
                    status.doc_name = status.infile;
                else
                    status.doc_name = "";
                break;
            case 'o':
                status.outfile = strndup (optarg, FILENAME_MAX);
                break;
            case 'h':
                print_help (progname);
                exit (0);
            default:
                SHoops (SH_FATAL, "try `%s -h'\n", progname);
        }
    }

    if (!status.outfile &&
        (status.attributes_separate ||
         status.names_separate ||
         status.statistics)) {
        SHoops (SH_FATAL, "output filename required\n");
        print_help (progname);
        goto failure;
    }
    
    if (status.outfile) {
        /* open files */ 
        if (status.attributes_separate) {
            /* attribute file */
            char attoutfile[FILENAME_MAX];
            snprintf (attoutfile, FILENAME_MAX, "%s_atts.csv", status.outfile);
            attout = SHopen_write (attoutfile);
        }
    
        if (status.names_separate) {
            /* names file */
            char namesoutfile[FILENAME_MAX];
            snprintf (namesoutfile, FILENAME_MAX, "%s_names.csv", status.outfile);
            namesout = SHopen_write (namesoutfile);
        }
    
        if (status.statistics) {
            /* guide file */
            char guideoutfile[FILENAME_MAX];
            snprintf (guideoutfile, FILENAME_MAX, "%s_guide.xml", status.outfile);
            guideout = SHopen_write (guideoutfile);
        }


        char outfile[FILENAME_MAX];
        snprintf (outfile, FILENAME_MAX, "%s.csv", status.outfile);
        shout = SHopen_write (outfile);
    }
    else
        shout = stdout;


    /* shred the files */
    if (SHshredder (status.infile,
                    shout, attout, namesout, guideout,
                    &status) < 0)
        goto failure;

    if (status.outfile)             fclose (shout);
    if (status.attributes_separate) fclose (attout);
    if (status.names_separate)      fclose (namesout);
    if (status.statistics)          fclose (guideout);

    MAIN_EXIT (EXIT_SUCCESS);

failure:
    MAIN_EXIT (EXIT_FAILURE);
}
