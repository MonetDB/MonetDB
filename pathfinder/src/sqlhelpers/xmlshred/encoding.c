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
#include <stdio.h>
#include <string.h>

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"

#include "encoding.h"
#include "guides.h"
#include "oops.h"
#include "hash.h"
#include "shred_helper.h"

#include <assert.h>

#define MIN(a,b) ((a) < (b) ? (a) : (b))

#define PROPSIZE 32000 
#define TEXT_SIZE 32000
#define TAG_SIZE 32 

#define NAME_ID 0

#define BAILOUT(...) do { SHoops (SH_FATAL, __VA_ARGS__); \
                          free_hash (hash_table);         \
                          exit(1);                        \
                        } while (0)

FILE *out;
FILE *out_attr;
FILE *out_names;
FILE *guide_out;

typedef struct node_t node_t;
struct node_t {
    nat           pre;
    nat           post;
    nat           pre_stretched;
    nat           post_stretched;
    node_t       *parent;
    nat           size;
    int           level;
    kind_t        kind;
    xmlChar      *name;
    int           name_id;
    xmlChar      *value;
    guide_tree_t *guide;
};

static shred_state_t shredstate;

/* hash table */
static hashtable_t hash_table;

static xmlChar buf[PROPSIZE + 1];
static int bufpos;

static node_t stack[STACK_MAX];
static int level;
static int max_level;
static nat pre;
static nat post;
static nat rank;
static nat att_id;

/**
 * Print the encoded kind.
 */
void
print_kind (FILE *f, kind_t kind)
{
    switch (kind) {
       case elem: putc ('1', f); break;
       case attr: putc ('2', f); break;
       case text: putc ('3', f); break;
       case comm: putc ('4', f); break;
       case pi:   putc ('5', f); break;
       case doc:  putc ('6', f); break;
       default: assert (!"kind unknown");
    }
}

static void
print_tuple (node_t tuple)
{
    const char *format = shredstate.format;
    
    unsigned int i;
    for (i = 0; format[i]; i++)
         if (format[i] == '%') {
             i++;
             assert (format[i]);
             switch (format[i]) {
                 case 'e': fprintf (out, SSZFMT, tuple.pre); break;
                 case 'o': fprintf (out, SSZFMT, tuple.post); break;
                 case 'E': fprintf (out, SSZFMT, tuple.pre_stretched); break;
                 case 'O': fprintf (out, SSZFMT, tuple.post_stretched); break;
                 case 's': fprintf (out, SSZFMT, tuple.size); break;
                 case 'l': fprintf (out, SSZFMT, tuple.level); break;
                 case 'k': print_kind (out, tuple.kind); break;
                 case 'p':
                     if (tuple.parent)
                         fprintf (out, SSZFMT, tuple.parent->pre);
                     break;
                 case 'P':
                     if (tuple.parent)
                         fprintf (out, SSZFMT,tuple.parent->pre_stretched);
                     break;
                 case 'n':
                     if (tuple.name_id != -1) {
                         if (shredstate.names_separate)
                             fprintf(out, "%i", tuple.name_id);
                         else
                             fprintf(out, "%s", tuple.name);
                     }
                     break;
                 case 't':
                     if (tuple.value) {
                         unsigned int j;
                         putc ('"', out);
                         for (j = 0; j < PROPSIZE && tuple.value[j]; j++)
                             switch (tuple.value[j]) {
                                 case '\n': 
                                     putc (' ', out); break;
                                 case '"':
                                     putc ('"', out); putc ('"', out); break;
                                 default:
                                     putc (tuple.value[j], out);
                             }
                             putc ('"', out);
                      }
                      break;    
                   case 'g':  fprintf (out, SSZFMT, tuple.guide->guide); break;
                   default:   putc (format[i], out); break;
             }
         }
         else
             putc (format[i], out);
    putc ('\n', out);
}

static int
generate_name_id (const xmlChar *name)
{
    static unsigned int global_name_id = NAME_ID;
    int name_id;
   
    if (!name)
        return -1;

    name_id = find_element (hash_table, (char *) name);

    /* key not found */
    if (NOKEY(name_id)) {
        /* create a new id */
        name_id = global_name_id++;
        /* add the pair into the hashtable */
        hashtable_insert (hash_table, (char *) name, name_id);
        /* print the name binding if necessary */
        if (shredstate.names_separate)
            fprintf (out_names, "%i, \"%s\"\n", name_id, name);
    }

    return name_id;
}

static void
flush_node (kind_t kind, const xmlChar *name, const xmlChar *value)
{
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
        .parent         = stack + level - 1,
        .size           = 0,
        .level          = level,
        .kind           = kind,
        .name           = (xmlChar *) name,
        .name_id        = generate_name_id (name),
        .value          = (xmlChar *) value,
        .guide          = insert_guide_node (name,
                                             stack[level-1].guide,
                                             kind)
    };

    post++;

    print_tuple (stack[level]);

    level--;
}

static void
flush_buffer (void)
{
    /* check if text is larger than TEXT_SIZE characters */
    if (xmlStrlen (buf) > TEXT_SIZE)
        BAILOUT ("We support text with length <= %i", TEXT_SIZE);

    if (buf[0])
        flush_node (text, NULL, buf);

    buf[0] = '\0';
    bufpos = 0;
}

static void
start_document (void *ctx)
{
    /* calling convention */
    (void) ctx;

    /* initialize everything with zero */
    pre       = 0;
    post      = 0;
    rank      = 0;
    level     = 0;
    max_level = 0;
    att_id    = 0;

    /* create a new node */
    stack[level] = (node_t) {
          .pre            = pre
        , .post           = 0
        , .pre_stretched  = rank
        , .post_stretched = 0
        , .parent         = NULL
        , .size           = 0
        , .level          = level
        , .kind           = doc
        , .name           = NULL
        , .name_id        = -1
        , .value          = xmlStrdup ((xmlChar *) shredstate.doc_name)
        , .guide          = insert_guide_node (
                                xmlCharStrndup (
                                    shredstate.doc_name,
                                    FILENAME_MAX),
                                NULL,
                                doc)
    };

}

static void
end_document (void *ctx)
{
    /* calling convention */
    (void) ctx;

    flush_buffer ();

    assert (level == 0);

    post++;
    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    print_tuple (stack[level]);

    assert (stack[level].guide->guide == GUIDE_INIT);

    adjust_guide_min_max (stack[level].guide);
    stack[level].guide->max = 1;
}

static void
start_element (void *ctx, const xmlChar *tagname, const xmlChar **atts)
{
    /* calling convention */
    (void) ctx;

    /* check if tagname is larger than TAG_SIZE characters */
    if (xmlStrlen(tagname) > TAG_SIZE)
        BAILOUT ("We support only tagnames with length <= %i", TAG_SIZE);
     
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
        .parent         = stack + level - 1,
        .size           = 0,
        .level          = level,
        .kind           = elem,
        .name           = xmlStrdup (tagname),
        .name_id        = generate_name_id (tagname), 
        .value          = (xmlChar *) NULL,
        .guide          = insert_guide_node (
                              tagname,
                              stack[level-1].guide,
                              elem)
    };

    if (atts) {
        if (shredstate.attributes_separate) {
            if (shredstate.names_separate)
                while (*atts) {
                    fprintf (out_attr,
                             SSZFMT ", " SSZFMT ", \"" SSZFMT "\", \"%s\","
                             SSZFMT "\n",
                             att_id++, pre, generate_name_id (atts[0]), atts[1],
                             insert_guide_node (atts[0],
                                                stack[level].guide,
                                                attr)->guide);
                    atts += 2;
                }
            else
                while (*atts) {
                    fprintf (out_attr,
                             SSZFMT ", " SSZFMT ", \"%s\", \"%s\"," SSZFMT "\n",
                             att_id++, pre, atts[0], atts[1],
                             insert_guide_node (atts[0],
                                                stack[level].guide,
                                                attr)->guide);
                    atts += 2;
                }
        } else /* handle attributes like children */
            while (*atts) {
                /* check if tagname is larger than TAG_SIZE characters */
                if (xmlStrlen (atts[0]) > TAG_SIZE)
                    BAILOUT ("We support only attributes with "
                             "length <= %i", TAG_SIZE);

                /* check if value is larger than TEXT_SIZE characters */
                if (xmlStrlen (atts[1]) > TEXT_SIZE)
                    BAILOUT ("We support only attribute content"
                             " with length <= %i", TEXT_SIZE);

                flush_node (attr, atts[0], atts[1]);
                atts += 2;
            }
     }
}

static void
end_element (void *ctx, const xmlChar *name)
{
    (void) ctx;
    (void) name;

    flush_buffer ();

    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    print_tuple (stack[level]);

    adjust_guide_min_max (stack[level].guide);

    post++;
    level--;
    assert (level >= 0);
}

static void
processing_instruction (void *ctx, const xmlChar *target, const xmlChar *chars)
{
    /* calling convention */
    (void) ctx;
        
    flush_buffer ();

    flush_node (pi, target, chars);
}

void
comment (void *ctx, const xmlChar *chars)
{
    /* calling convention */
    (void) ctx;

    flush_buffer ();

    flush_node (comm, NULL, chars);
}

void
characters (void *ctx, const xmlChar *chars, int n)
{
    /* calling convention */
    (void) ctx;

    if (bufpos < PROPSIZE) {
        snprintf ((char *) buf + bufpos,
                  MIN (n, PROPSIZE - bufpos) + 1,
                  "%s",
                  (char *) chars);
        bufpos += MIN (n, PROPSIZE - bufpos);
    }

    buf[bufpos] = '\0';
}

static void
error (void *ctx, const char *msg, ...)
{
    va_list az;
    int buf_size = 4096;
    char buf[buf_size];
        
    (void) ctx;

    va_start (az, msg);
    vsnprintf (buf, buf_size, msg, az);
    fprintf (stderr, "%s", buf);
    va_end (az);

    BAILOUT ("libxml error");
}

static xmlSAXHandler saxhandler = {
      .startDocument         = start_document
    , .endDocument           = end_document 
    , .startElement          = start_element 
    , .endElement            = end_element 
    , .characters            = characters
    , .processingInstruction = processing_instruction
    , .comment               = comment
    , .error                 = error
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

/**
 * Main shredding procedure.
 */
int
SHshredder (const char *s, 
            FILE *shout,
            FILE *attout,
            FILE *namesout,
            FILE *guideout,
            shred_state_t *status)
{
    /* XML parser context */
    xmlParserCtxtPtr ctx;
    shredstate = *status;

    assert (shout);

    /* bind the different output files to global variables
       to make them accessible inside the callback functions */
    out       = shout;
    out_attr  = attout;
    out_names = namesout;

    hash_table = new_hashtable (); 

    /* start XML parsing */
    ctx = xmlCreateFileParserCtxt (s);
    ctx->sax = &saxhandler;

    (void) xmlParseDocument (ctx);
    
    if (!ctx->wellFormed) {
        SHoops (SH_FATAL, "XML input not well-formed\n");
    }

    /* print statistics if required */
    if (shredstate.statistics)
        print_guide_tree (guideout, stack[0].guide, 0);

    free_hash (hash_table);

    return 0;
}
