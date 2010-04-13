/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Entry point to Pathfinder SQL Code Generator.
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

#include "pf_config.h"
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
    { "help",                      no_argument,       NULL, 'h' },
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
             "The Pathfinder SQL Code Generator experienced "
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
#include "map_names.h"
#include "properties.h"
#include "lalg2sql.h"
#include "sql_opt.h"
#include "sqlprint.h"
#include "alg_cl_mnemonic.h"


#define MAIN_EXIT(rtrn) \
        fputs (PFerrbuf, stderr);\
        exit (rtrn);




void
print_property (PFla_pb_item_property_t property, unsigned int nest)
{

	char *nestSpaces;
	nestSpaces = (char *) PFmalloc (nest + 1);
	for(unsigned int i = 0; i < nest; i++)
	{
		nestSpaces[i] = ' ';
	}
	nestSpaces[nest] = '\0';

	fprintf (stdout, "%s<property name=\"%s\"",
	    nestSpaces, property.name);
	if (property.value)
	{
	    fprintf (stdout, " value=\"%s\"",
			property.value);
	}
	if (property.properties)
	{
		fprintf (stdout, ">\n");
		for (unsigned int i = 0;
				i < PFarray_last (property.properties);
	            i++)
		{
			PFla_pb_item_property_t subProperty =
				*((PFla_pb_item_property_t*) PFarray_at (
												property.properties, i));
			print_property (subProperty, nest+2);
		}
		fprintf (stdout, "%s</property>\n", nestSpaces);
	}
	else
	{
		fprintf (stdout, "/>\n");
	}

}



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
        c = getopt_long (argc, argv, "h",
                         long_options, &option_index);
#else
        c = getopt (argc, argv, "h");
#endif

        if (c == -1)
            break;            /* end of command line */
        switch (c) {
            case 'h':
                printf ("\n         Pathfinder SQL Code Generator\n\n"
                        "Compiles Logical Algebra Dag in XML Format to SQL.\n\n"
                        "($Revision$, $Date$)\n");
                printf ("(c) Database Systems Group, ");
                printf (    "Eberhard Karls Universitaet Tuebingen\n\n");
                printf ("Usage: %s [OPTION] [FILE]\n\n", argv[0]);
                printf ("  Reads from standard input if FILE is omitted.\n\n");
                printf ("  -h%s: print this help message\n",
                        long_option (opt_buf, ", --%s", 'h'));
                printf ("\n");
                printf ("Enjoy.\n");
                exit (0);

            default:
                PFoops (OOPS_CMDLINEARGS, "try `%s -h'", progname);
                break;

        }           /* end of switch */
    }           /* end of while */

    /* We either have a plan bundle in @a lapb
       or a logical query plan in @a laroot. */
    PFla_pb_t  *lapb    = NULL;
    PFla_op_t  *laroot  = NULL;
    PFsql_t    *sqlroot = NULL;

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
        lapb = PFxml2la_importXMLFromFile (ctx, argv[optind]);
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
        lapb = PFxml2la_importXMLFromMemory (ctx, xml, strlen (xml));
    }

    /* detect whether we have a plan bundle or only a single plan */
    if (PFla_pb_size (lapb) == 1 && PFla_pb_id_at (lapb, 0) == -1) {
       laroot = PFla_pb_op_at (lapb, 0);
       lapb = NULL;
    }

    unsigned int i = 0, c;
    /* plan bundle emits SQL code wrapped in XML tags */ 
    if (lapb)
        fprintf (stdout, "<query_plan_bundle>\n");
    /* If we have a plan bundle we have to generate
       a SQL query for each query plan. */
    do {
        /* plan bundle emits SQL code wrapped in XML tags
           with additional column and linking information */ 
        if (lapb) {
            laroot = PFla_pb_op_at (lapb, i);
            assert (laroot->kind == la_serialize_rel);

            fprintf (stdout, "<query_plan id=\"%i\"",
                     PFla_pb_id_at (lapb, i));
            if (PFla_pb_idref_at (lapb, i) != -1)
                fprintf (stdout, " idref=\"%i\" colref=\"%i\"",
                         PFla_pb_idref_at (lapb, i),
                         PFla_pb_colref_at (lapb, i));
            fprintf (stdout, ">\n");

            if (PFla_pb_properties_at (lapb, i))
			{
				fprintf (stdout, "  <properties>\n");
				for (unsigned int propertyID = 0;
					propertyID < PFarray_last (PFla_pb_properties_at (lapb, i));
					propertyID++)
				{
					PFla_pb_item_property_t property =
						*((PFla_pb_item_property_t*) PFarray_at
								(PFla_pb_properties_at (lapb, i), propertyID));
					print_property (property, 4);
				}
				fprintf (stdout, "  </properties>\n");
			}

            fprintf (stdout,
                     "<schema>\n"
                     "  <column name=\"%s\" function=\"iter\"/>\n",
                     PFcol_str (laroot->sem.ser_rel.iter));
            for (c = 0; c < clsize (laroot->sem.ser_rel.items); c++)
                fprintf (stdout,
                         "  <column name=\"%s\" new=\"false\""
                                  " function=\"item\""
                                  " position=\"%u\"/>\n",
                         PFcol_str (clat (laroot->sem.ser_rel.items, c)),
                         c);
            fprintf (stdout, "</schema>\n"
                             "<query>"
                             "<![CDATA[\n");
            i++;
        }

        if (!PFcol_is_name_unq(laroot->schema.items[0].name))
            laroot = PFmap_unq_names (laroot);

        /* Infer properties required by the SQL Code Generation and
           generate the SQL code. */
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
                      true  /* name origin */,
                      laroot, NULL);

        sqlroot = PFlalg2sql (laroot);
        sqlroot = PFsql_opt (sqlroot);
        PFsql_print (stdout, sqlroot);

        /* plan bundle emits SQL code wrapped in XML tags */ 
        if (lapb)
            fprintf (stdout, "]]>"
                             "</query>\n"
                             "</query_plan>\n");

    /* iterate over the plans in the plan bundle */
    } while (lapb && i < PFla_pb_size (lapb));

    /* plan bundle emits SQL code wrapped in XML tags */ 
    if (lapb)
        fprintf (stdout, "</query_plan_bundle>\n");

    PFmem_destroy ();

    exit (EXIT_SUCCESS);
 }
}

/* vim:set shiftwidth=4 expandtab: */
