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

#define TAG_SIZE  100
#define BUF_SIZE 4096 
#define NAME_ID 0

#define BAILOUT(...) do { SHoops (SH_FATAL, __VA_ARGS__); \
                          free_hash (hash_table);         \
                          exit(1);                        \
                        } while (0)

FILE *out;
FILE *out_attr;
FILE *out_names;
FILE *guide_out;
FILE *err;

unsigned int text_size;

/* count the tags/text being truncated
 * during the shredding process */
unsigned int tag_stripped;
unsigned int text_stripped;


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

/* guide wrapper to ensure that guide information is only collected if needed */
#define insert_guide_node_(n,p,k) ((shredstate.statistics)           \
                                  ? insert_guide_node (n,p,k)        \
                                  : NULL)                            
#define guide_val_(g)             ((shredstate.statistics)           \
                                  ? (g)->guide                       \
                                  : 0)

/* hash table */
static hashtable_t hash_table;

static xmlChar buf[BUF_SIZE + 1];
static int bufpos;

static node_t stack[STACK_MAX];
static int level;
static int max_level;
static nat pre;
static nat post;
static nat rank;
static nat att_id;

void
print_text (FILE *f, char *buf, size_t len) {
    if (len)
        if (fwrite (buf, (size_t) 1, len, f) < len)
            BAILOUT ("write failed");
}

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

void
print_number (FILE *f, node_t tuple)
{
    if (tuple.value) {
        char *num_str = (char *) tuple.value;
        char *src = num_str;
        double num = strtod (num_str, &src);
        /* ensure that the number represents neither the infinity
           nor NaN (num_str[0] < 'a') and that the whole string
           can be converted into a number (src is the empty string) */
        if (num_str[0] < 'a' && !src[0])
            fprintf(f, "%20.10f", num);
    }
}

void
print_value (FILE *f, node_t tuple)
{
    /* escape quotes in quoted string */
    if (tuple.value) {
        char *src = (char *) tuple.value;
        unsigned int start = 0, end;
        putc ('"', f);
        for (end = 0; end < text_size && tuple.value[end]; end++)
            /* escape quotes */
            if (tuple.value[end] == '"') {
                print_text (f, &src[start], end - start);
                start = end + 1;
                putc ('"', f); putc ('"', f);
            }
        if (start < end)
            print_text (f, &src[start], end - start);
        putc ('"', f);
    }
}

void (*print_name) (FILE *, node_t);
    
void
print_name_str (FILE *f, node_t tuple)
{
    fprintf (f, "%i", tuple.name_id);
}

void
print_name_id (FILE *f, node_t tuple)
{
    fprintf (f, "\"%s\"", (char *) tuple.name);
}

static void
print_tuple (node_t tuple)
{
    const char  *format = shredstate.format;
    unsigned int i;
    
    if (shredstate.fastformat) {
        /* Ensure that in main.c the format string in shredstate.format
           is compared with the string FAST_FORMAT and that FAST_FORMAT
           describes the output printed here. */

        fprintf (out, SSZFMT ", " SSZFMT ", %i, ",
                 tuple.pre, tuple.size, tuple.level);
        print_kind (out, tuple.kind);
        print_text (out, ", ", 2);
        if (tuple.name_id != -1)
            print_name (out, tuple);
        print_text (out, ", ", 2);
        print_number (out, tuple);
        print_text (out, ", ", 2);
        print_value (out, tuple);
        putc ('\n', out);
        return;
    }
    
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
                 case 'l': fprintf (out, "%i",   tuple.level); break;
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
                     if (tuple.name_id != -1)
                         print_name (out, tuple);
                     break;
                 case 'd':  print_number (out, tuple); break;    
                 case 't':  print_value (out, tuple); break;    
                 case 'g':  fprintf (out, SSZFMT, guide_val_(tuple.guide)); break;
                 case '%':  putc ('%', out); break;
                 default:   SHoops (SH_FATAL,
                                    "Unexpected formatting character '%c'",
                                    format[i]);
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
            fprintf (out_names, "%i, \"%s\"\n", name_id, (char*)name);
    }

    return name_id;
}

static void
flush_node (kind_t kind, const xmlChar *name, const xmlChar *value)
{
    int valStrLen = -1;

    pre++;
    rank += 2;
    level++;

    if (level > max_level)
        max_level = level;

    /* check if tagname is larger than TAG_SIZE characters */
    if (name && xmlStrlen (name) > TAG_SIZE)
        BAILOUT ("we allow only attributes not greater as %u characters", TAG_SIZE);

    /* check if value is larger than text_size characters */
    if (value && (valStrLen = xmlStrlen (value)) >= 0 &&
        (unsigned int) valStrLen > text_size)
        text_stripped++;

    stack[level] = (node_t) {
        .pre            = pre,
        .post           = post,
        .pre_stretched  = rank - 1,
        .post_stretched = rank,
        .parent         = stack + level - 1,
        .size           = 0,
        .level          = level,
        .kind           = kind,
        .name           = xmlStrdup (name),
        .name_id        = generate_name_id (name),
        .value          = xmlStrndup (value,
                                      MIN ((unsigned int ) xmlStrlen (value),
                                           text_size)),
        .guide          = insert_guide_node_ (name,
                                              stack[level-1].guide,
                                              kind)
    };

    post++;

    print_tuple (stack[level]);

    if (stack[level].name)  xmlFree (stack[level].name);
    if (stack[level].value) xmlFree (stack[level].value);
    level--;
}

static void
flush_buffer (void)
{
    if (buf[0]) {
        flush_node (text, NULL, buf);
    }

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

    if (strlen (shredstate.doc_name) > MIN (FILENAME_MAX, text_size))
        text_stripped++;

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
        , .guide          = insert_guide_node_ (
                                xmlCharStrndup (
                                    shredstate.doc_name,
                                    MIN (FILENAME_MAX, text_size)),
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

    if (shredstate.statistics) {
        assert (guide_val_ (stack[level].guide) == GUIDE_INIT);
        adjust_guide_min_max (stack[level].guide);
        stack[level].guide->max = 1;
    }
    if (stack[level].value) xmlFree (stack[level].value);
}

static void
start_element (void *ctx, const xmlChar *tagname, const xmlChar **atts)
{
    /* calling convention */
    (void) ctx;

    flush_buffer ();

    pre++;
    rank++;
    level++;

    if (level > max_level)
        max_level = level;

    assert (level < STACK_MAX);
    
    if (xmlStrlen (tagname) > TAG_SIZE)
        BAILOUT ("we allow only tag-names not greater as %u characters", TAG_SIZE);

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
        .guide          = insert_guide_node_ (tagname,
                                              stack[level-1].guide,
                                              elem)
    };

    if (atts) {
        if (shredstate.attributes_separate) {
            if (shredstate.names_separate)
                while (*atts) {
                    fprintf (out_attr,
                             SSZFMT ", " SSZFMT ", %i, \"%s\"",
                             att_id++,
                             pre,
                             generate_name_id (atts[0]),
                             (char*)atts[1]);
                    if (shredstate.statistics)
                        fprintf (out_attr, "," SSZFMT,
                                 guide_val_ (
                                     insert_guide_node_ (atts[0],
                                                         stack[level].guide,
                                                         attr)));
                    putc ('\n', out_attr);
                    atts += 2;
                }
            else
                while (*atts) {
                    fprintf (out_attr,
                             SSZFMT ", " SSZFMT ", \"%s\", \"%s\"",
                             att_id++,
                             pre,
                             (char*)atts[0],
                             (char*)atts[1]);
                    if (shredstate.statistics)
                        fprintf (out_attr, "," SSZFMT,
                                 guide_val_ (
                                     insert_guide_node_ (atts[0],
                                                         stack[level].guide,
                                                         attr)));
                    putc ('\n', out_attr);
                    atts += 2;
                }
        } else /* handle attributes like children */
            while (*atts) {
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

/* Enable the following lines if your code generation produces
   code that collapses path steps and directly following text
   value lookups into a single path step. */
#if 0
    /* copy the text value of a text node if it is the only child */
    if (buf[0] && pre == stack[level].pre)
        stack[level].value = xmlStrndup (buf, text_size);
#endif

    flush_buffer ();

    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    print_tuple (stack[level]);

    if (shredstate.statistics)
        adjust_guide_min_max (stack[level].guide);

    post++;
    /* free the memory allocated for the element name and the text value */
    if (stack[level].name)  xmlFree (stack[level].name);
    if (stack[level].value) xmlFree (stack[level].value);
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

    if (bufpos < 0 || (unsigned int) bufpos < text_size) {
        snprintf ((char *) buf + bufpos,
                  MIN (n, BUF_SIZE - bufpos) + 1,
                  "%s",
                  (char *) chars);
        bufpos += MIN (n, BUF_SIZE - bufpos);
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
    fprintf (err, "%s", buf);
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

static void
report (void)
{
    if (text_stripped > 0) {
        fprintf (err, "%u Values were stripped to %u "
                      "character(s).\n", text_stripped, text_size);
    }
}

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
    err       = stderr;

    /* how many characters should be stored in
     * the value column */
    text_size = status->strip_values;

    text_stripped = 0;
    tag_stripped  = 0;

    /* initialize hashtable */
    hash_table = new_hashtable (); 

    /* Decide whether to print the node names
       or its corresponding name ids only once. */
    if (shredstate.names_separate)
        print_name = print_name_id;
    else
        print_name = print_name_str;

    /* start XML parsing */
    ctx = xmlCreateFileParserCtxt (s);
    ctx->sax = &saxhandler;

    (void) xmlParseDocument (ctx);
    
    if (!ctx->wellFormed) {
        SHoops (SH_FATAL, "XML input not well-formed\n");
    }

    /* print statistics if required */
    if (shredstate.statistics) {
        print_guide_tree (guideout, stack[0].guide, 0);
        free_guide_tree (stack[0].guide);
    }

    free_hash (hash_table);

    xmlCleanupParser ();

    if (!status->quiet)
        report ();

    return 0;
}

/* vim:set shiftwidth=4 expandtab: */

