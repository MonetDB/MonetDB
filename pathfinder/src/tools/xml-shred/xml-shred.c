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

#include <getopt.h>

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"

#define STACK_MAX 100
#define PROPSIZE 32

#define MIN(a,b) ((a) < (b) ? (a) : (b))

typedef long int nat;

enum kind_t {
      elem
    , attr
    , text
    , comm
    , pi
    , doc
};
typedef enum kind_t kind_t;

typedef struct node_t node_t;
struct node_t {
    nat       pre;
    nat       post;
    nat       pre_stretched;
    nat       post_stretched;
    nat       size;
    int       level;
    node_t   *parent;
    kind_t    kind;
    xmlChar  *prop;
};

static node_t stack[STACK_MAX];
static int level;
static int max_level;
static nat pre;
static nat post;
static nat rank;
static nat att_id;

static char *format = "%e, %o, %p, %k, %t";
FILE *out;
FILE *out_attr;
char  filename[FILENAME_MAX];
bool  filename_given = false;
char  outfile[FILENAME_MAX];
char  outfile_atts[FILENAME_MAX];
bool  outfile_given = false;
bool  suppress_attributes = false;

static void print_tuple (node_t tuple);
static void flush_buffer (void);

static xmlChar buf[PROPSIZE + 1];
static int bufpos;

char *strndup(const char *s, size_t n);
char *strdup(const char *s);

static void
start_document (void *ctx)
{
    pre = 0;
    post = 0;
    rank = 0;
    level = 0;
    max_level = 0;
    att_id = 0;

    stack[level] = (node_t) {
        .pre            = pre,
        .post           = 0,
        .pre_stretched  = rank,
        .post_stretched = 0,
        .size           = 0,
        .level          = level,
        .parent         = NULL,
        .kind           = doc,
        .prop           = filename_given ? strndup (filename, PROPSIZE)
                                         : ""
    };
}

static void
end_document (void *ctx)
{
    flush_buffer ();

    assert (level == 0);

    post++;
    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    print_tuple (stack[level]);

    free (stack[level].prop);
}

static void
start_element (void *ctx, const xmlChar *tagname, const xmlChar **atts)
{
    flush_buffer ();

    pre++;
    rank++;
    level++;

    if (level > max_level)
        max_level = level;

    assert (level < STACK_MAX);

    stack[level] = (node_t) {
        .pre            = pre,
        .post           = 0,
        .pre_stretched  = rank,
        .post_stretched = 0,
        .size           = 0,
        .level          = level,
        .parent         = stack + level - 1,
        .kind           = elem,
        .prop           = strndup (tagname, PROPSIZE)
    };

    /*
     * FIXME: handle attributes here
     */
    if (!suppress_attributes && atts)
        while (*atts) {
            fprintf (out_attr, "%lu, %lu, \"%s\", \"%s\"\n", att_id++, pre,
                     atts[0], atts[1]);
            atts += 2;
        }
}

static void
end_element ()
{
    flush_buffer ();

    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    print_tuple (stack[level]);

    post++;
    free (stack[level].prop);
    level--;
    assert (level >= 0);
}

void
characters (void *ctx, const xmlChar *chars, int n)
{
    if (bufpos < PROPSIZE) {
        snprintf (buf + bufpos, MIN (n, PROPSIZE - bufpos) + 1, "%s", chars);
        bufpos += MIN (n, PROPSIZE - bufpos);
    }

    buf[PROPSIZE] = '\0';
}

static void
flush_buffer (void)
{
    if (buf[0]) {
        pre++;
        rank += 2;
        level++;

        if (level > max_level)
            max_level = level;

        stack[level] = (node_t) {
            .pre            = pre,
            .post           = post,
            .pre_stretched  = rank - 1,
            .post_stretched = rank,
            .size           = 0,
            .level          = level,
            .parent         = stack + level - 1,
            .kind           = text,
            .prop           = buf,
        };

        post++;

        print_tuple (stack[level]);

        level--;
    }

    buf[0] = '\0';
    bufpos = 0;
}

static xmlSAXHandler saxhandler = {
      .startDocument         = start_document
    , .endDocument           = end_document
    , .startElement          = start_element
    , .endElement            = end_element
    , .characters            = characters
    , .processingInstruction = NULL
    , .comment               = NULL
    , .error                 = NULL
    , .cdataBlock            = NULL
    , .internalSubset        = NULL
    , .isStandalone          = NULL
    , .hasInternalSubset     = NULL
    , .hasExternalSubset     = NULL
    , .resolveEntity         = NULL
    , .getEntity             = NULL
    , .entityDecl            = NULL
    , .notationDecl          = NULL
    , .attributeDecl         = NULL
    , .elementDecl           = NULL
    , .unparsedEntityDecl    = NULL
    , .setDocumentLocator    = NULL
    , .reference             = NULL
    , .ignorableWhitespace   = NULL
    , .warning               = NULL
    , .fatalError            = NULL
    , .getParameterEntity    = NULL
    , .externalSubset        = NULL
    , .initialized           = false
};

static void
print_help (int argc, char **argv)
{
    printf ("%s - encode XML document in pre/post encoding\n"
            "Usage: %s -h             print this help screen\n"
            "       %s -f <filename>  parse XML file <filename>\n"
            "       %s -o <filename>  output filename\n"
            "       %s -a             suppress attributes\n",
            argv[0], argv[0], argv[0], argv[0], argv[0]);
}

static void
print_tuple (node_t tuple)
{
    for (unsigned int i = 0; format[i]; i++)
        if (format[i] == '%') {
            i++;
            switch (format[i]) {
                case 'e':  fprintf (out, "%lu", tuple.pre); break;
                case 'o':  fprintf (out, "%lu", tuple.post); break;
                case 'E':  fprintf (out, "%lu", tuple.pre_stretched); break;
                case 'O':  fprintf (out, "%lu", tuple.post_stretched); break;
                case 's':  fprintf (out, "%lu", tuple.size); break;
                case 'l':  fprintf (out, "%u", tuple.level); break;

                case 'p':  if (tuple.parent)
                               fprintf (out, "%lu", tuple.parent->pre);
                           else
                               fprintf (out, "NULL");
                           break;

                case 'P':  if (tuple.parent)
                               fprintf (out, "%lu",tuple.parent->pre_stretched);
                           else
                               fprintf (out, "NULL");
                           break;
                case 'k': 
                           switch (tuple.kind) {
                               case elem: putc ('1', out); break;
                               case attr: putc ('2', out); break;
                               case text: putc ('3', out); break;
                               case comm: putc ('4', out); break;
                               case pi:   putc ('5', out); break;
                               case doc:  putc ('6', out); break;
                               default: assert (0);
                           }
                           break;
                case 't':
                    putc ('"', out);
                    for (unsigned int i = 0; i < PROPSIZE && tuple.prop[i]; i++)
                        switch (tuple.prop[i]) {
                            case '\n': putc (' ', out); break;
                            case '"':  putc ('"', out); putc ('"', out);
                                       break;
                            default:   putc (tuple.prop[i], out);
                        }
                    putc ('"', out);
                    break;

                default:   putc (format[i], out); break;
            }
        }
        else
            putc (format[i], out);

    putc ('\n', out);
}

int
main (int argc, char **argv)
{
    xmlParserCtxtPtr  ctx;

    suppress_attributes = false;
    outfile_given = false;

    /* parse command line using getopt library */
    while (true) {
        int c = getopt (argc, argv, "F:af:ho:");

        if (c == -1)
            break;

        switch (c) {

            case 'a':
                suppress_attributes = true;
                break;

            case 'F':
                format = strdup (optarg);
                break;

            case 'f':
                strncpy (filename, optarg, FILENAME_MAX);
                filename_given = true;
                break;

            case 'o':
                strncpy (outfile, optarg, FILENAME_MAX);
                outfile_given = true;
                break;

            case 'h':
                print_help (argc, argv);
                exit (0);

        }
    }

    if (!outfile_given && !suppress_attributes) {
        fprintf (stderr, "Attribute generation requires output filename.\n");
        print_help (argc, argv);
        exit (EXIT_FAILURE);
    }

    if (outfile_given) {

        out = fopen (outfile, "w");
        snprintf (outfile_atts, FILENAME_MAX, "%s_atts", outfile);
        out_attr =  fopen (outfile_atts, "w");

        if (!out || !out_attr) {
            fprintf (stderr, "error opening output file(s).\n");
            exit (EXIT_FAILURE);
        }
    }
    else
        out = stdout;

    /* did we get a filename? */
    if (!filename_given) {
        fprintf (stderr, "No filename given.\n");
        print_help (argc, argv);
        exit (EXIT_FAILURE);
    }

    /* start XML parsing */
    ctx = xmlCreateFileParserCtxt (filename);
    ctx->sax = &saxhandler;

    (void) xmlParseDocument (ctx);

    if (! ctx->wellFormed) {
        fprintf (stderr, "XML input not well-formed\n");
        exit (EXIT_FAILURE);
    }

    fprintf (stderr, "tree height was %i\n", max_level);

    return 0;
}
