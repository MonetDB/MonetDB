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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pf_config.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>

/* libxml SAX2 parser internals */
#include "libxml/parserInternals.h"

#include "encoding.h"
#include "guides.h"
#include "oops.h"
#include "hash.h"
#include "shred_helper.h"

#ifndef HAVE_SAX2
 #error "libxml2 SAX2 interface required to compile the XML shredder `pfshred'"
#endif

FILE *out;
FILE *out_attr;
FILE *out_names;
FILE *out_uris;
FILE *guide_out;
FILE *err;

unsigned int text_size;

/* count the tags/text being truncated
 * during the shredding process */
unsigned int tag_stripped;
unsigned int text_stripped;

static shred_state_t shredstate;

/* Wrapper to ensure that guide information is only collected if needed */
#define insert_guide_node_(u,n,p,k) ((shredstate.statistics)            \
                                  ? insert_guide_node ((u),(n),(p),(k)) \
                                  : NULL)                            
#define guide_val_(g)             ((shredstate.statistics) \
                                  ? (g)->guide             \
                                  : 0)

/* localname and URI hash tables */
static hashtable_t localname_hash;
static hashtable_t uris_hash;

#define BAILOUT(...) do { SHoops (SH_FATAL, __VA_ARGS__);   \
                          free_hashtable (localname_hash);  \
                          free_hashtable (uris_hash);       \
                          exit (1);                         \
                        } while (0)


/* id of empty namespace prefix */
#define EMPTY_NS 0

/* maximum localname/URI and text buffer sizes */
#define TAG_SIZE 100
#define BUF_SIZE 4096 
                              
/* buffer for XML node contents/values */
static xmlChar buf[BUF_SIZE + 1];
static int bufpos;

static node_t stack[STACK_MAX];
static int level;
static int max_level;
static nat pre;
static nat post;
static nat rank;
static nat att_id;
             
/* encoding format compilation */

/* maximum and actual number of 
 * formatting instructions `%.' in `-F' argument 
 */
#define FMT_MAX 32
static unsigned int fmts;

/* type of a node formatting function */
typedef void (fmt_lambda_t) (node_t);

/* formatting instructions and their separating strings */
static fmt_lambda_t *fmt_funs[FMT_MAX];
static char         *fmt_seps[FMT_MAX];

void
print_text (FILE *f, char *buf, size_t len) {
    if (len)
        if (fwrite (buf, (size_t) 1, len, f) < len)
            BAILOUT ("write failed (print_text)");
}

/**
 * Print the decoded kind
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
       default: assert (!"XML node kind unknown (print_kind)");
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

void (*print_localname) (FILE *, node_t);
void (*print_uri) (FILE *, node_t);
    
void
print_localname_id (FILE *f, node_t tuple)
{
    fprintf (f, "%i", tuple.localname_id);
}

void
print_localname_str (FILE *f, node_t tuple)
{
    fprintf (f, "\"%s\"", (char *) tuple.localname);
}
    
void
print_uri_id (FILE *f, node_t tuple)
{
    fprintf (f, "%i", tuple.uri_id);
}

void
print_uri_str (FILE *f, node_t tuple)
{
    fprintf (f, "\"%s\"", (char *) tuple.uri);
}

/* implementation of formatting functions */
static void lambda_e (node_t n) 
{ fprintf (out, SSZFMT, n.pre); }

static void lambda_o (node_t n) 
{ fprintf (out, SSZFMT, n.post); }

static void lambda_E (node_t n) 
{ fprintf (out, SSZFMT, n.pre_stretched); }

static void lambda_O (node_t n) 
{ fprintf (out, SSZFMT, n.post_stretched); }

static void lambda_s (node_t n) 
{ fprintf (out, SSZFMT, n.size); }

static void lambda_l (node_t n) 
{ fprintf (out, "%i", n.level); }

static void lambda_k (node_t n) 
{ print_kind (out, n.kind); }

static void lambda_p (node_t n) 
{ if (n.parent) lambda_e (*(n.parent)); }

static void lambda_P (node_t n) 
{ if (n.parent) lambda_E (*(n.parent)); }

static void lambda_n (node_t n) 
{ if (n.localname_id != -1) print_localname (out, n); }

static void lambda_u (node_t n) 
{ if (n.uri_id != -1) print_uri (out, n); }

static void lambda_d (node_t n) 
{ print_number (out, n); }

static void lambda_t (node_t n) 
{ print_value (out, n); }

static void lambda_g (node_t n) 
{ fprintf (out, SSZFMT, guide_val_ (n.guide)); }

static void lambda_percent (node_t n) 
{ (void) n; putc ('%', out); } 
           
/* binding formatting functions to formatting instructions `%.' */
static fmt_lambda_t *fmt_lambdas[] = {
    ['e'] = lambda_e 
  , ['o'] = lambda_o 
  , ['E'] = lambda_E 
  , ['O'] = lambda_O 
  , ['s'] = lambda_s 
  , ['l'] = lambda_l 
  , ['k'] = lambda_k
  , ['p'] = lambda_p
  , ['P'] = lambda_P
  , ['n'] = lambda_n
  , ['u'] = lambda_u
  , ['d'] = lambda_d
  , ['t'] = lambda_t
  , ['g'] = lambda_g
  , ['%'] = lambda_percent
  , ['~'] = NULL
};

static void
apply_fmt (node_t tuple)
{   
    unsigned int fn;

    /* alternate: 
     * - print separator string
     * - execute formatting instruction
     * - print separator string
     * - ...
     */
    fputs (fmt_seps[0], out);

    for (fn = 0; fn < fmts; fn++) { 
        (*fmt_funs[fn]) (tuple);
        fputs (fmt_seps[fn + 1], out);
    } 
    
    fputc ('\n', out);
}

static void
compile_fmt (char *fmt)
{
    char *sep;
    fmt_lambda_t *lambda;

    fmts = 0;
    
    while (fmts < FMT_MAX && (sep = strsplit (&fmt, "%"))) {
        fmt_seps[fmts] = sep;
        if (fmt) {
            lambda = fmt_lambdas[(int)(*fmt)];
            if (!lambda)
               SHoops (SH_FATAL,
                       "unknown formatting instruction `%%%c' in argument to -F",
                       *fmt);

            fmt_funs[fmts] = lambda;

            fmts++;
            /* skip over formatting instruction character */
            fmt++;
        } 
    }
}

static int
generate_localname_id (const xmlChar *localname)
{
    static unsigned int global_localname_id = 0;
    int localname_id;
   
    if (!localname)
        return -1;
                     
    localname_id = hashtable_find (localname_hash, localname);

    if (NOKEY (localname_id)) {
        /* key not found, create a new name id */
        localname_id = global_localname_id++;
        /* add the (localname, localname_id) pair into the hash table */
        hashtable_insert (localname_hash, localname, localname_id);
        /* print the name binding if necessary */
        if (shredstate.names_separate)
            fprintf (out_names, "%i, \"%s\"\n", localname_id, (char*) localname);
    }

    return localname_id;
}

static int
generate_uri_id (const xmlChar *URI)
{  
    static unsigned int global_uri_id = 0;
    int uri_id;
    
    if (!URI)
        return -1;
        
    uri_id = hashtable_find (uris_hash, URI);

    if (NOKEY (uri_id)) {
        /* key not found, create a new URI id */
        uri_id = global_uri_id++;
        /* add the (URI, uri_id) pair to the hash table */
        hashtable_insert (uris_hash, URI, uri_id);
        /* print the URI binding if necessary */
        if (shredstate.names_separate)
            fprintf (out_uris, "%i, \"%s\"\n", uri_id, (char*) URI);
    }

    return uri_id;
}

static void
flush_node (kind_t kind, 
            const xmlChar *URI, const xmlChar *localname, 
            const xmlChar *value)
{
    int valStrLen = -1;

    pre++;
    rank++;
    level++;

    max_level = MAX(level, max_level);

    /* check if tagname is larger than TAG_SIZE characters */
    if (localname && xmlStrlen (localname) > TAG_SIZE)
        BAILOUT ("attribute local name `%s' exceeds %u characters", 
                 localname, TAG_SIZE);
    
    if (URI && xmlStrlen (URI) > TAG_SIZE)
        BAILOUT ("namespace URI `%s' exceeds length of %u characters", 
                 URI, TAG_SIZE);

    /* check if value is larger than text_size characters */
    if (value && (valStrLen = xmlStrlen (value)) >= 0 &&
        (unsigned int) valStrLen > text_size)
        text_stripped++;

    stack[level] = (node_t) {
        .pre            = pre
      , .post           = post
      , .pre_stretched  = rank
      , .post_stretched = rank + 1
      , .parent         = stack + level - 1
      , .size           = 0
      , .level          = level
      , .kind           = kind
      , .localname      = xmlStrdup (localname)
      , .localname_id   = generate_localname_id (localname)
      , .uri            = xmlStrdup (URI)
      , .uri_id         = generate_uri_id (URI)
      , .value          = xmlStrndup (value,
                                      MIN ((unsigned int) xmlStrlen (value),
                                           text_size))
      , .guide          = insert_guide_node_ (URI, localname,
                                              stack[level-1].guide,
                                              kind)
    };

    post++;
    rank++;
    
    apply_fmt (stack[level]);

    if (stack[level].localname) xmlFree (stack[level].localname);
    if (stack[level].uri)       xmlFree (stack[level].uri);
    if (stack[level].value)     xmlFree (stack[level].value);

    level--;
}

static void
flush_buffer (void)
{
    if (buf[0]) {
        flush_node (text, NULL, NULL, buf);
    }

    buf[0] = '\0';
    bufpos = 0;
}

static void
start_document (void *ctx)
{
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
        , .localname      = NULL
        , .localname_id   = -1
        , .uri            = NULL
        , .uri_id         = -1
        , .value          = xmlStrdup ((xmlChar *) shredstate.doc_name)
        , .guide          = insert_guide_node_ (
                                NULL,
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
    (void) ctx;

    flush_buffer ();

    assert (level == 0);

    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    apply_fmt (stack[level]);

    post++;

    if (shredstate.statistics) 
        guide_occurrence (stack[level].guide);

    if (stack[level].value) 
        xmlFree (stack[level].value);
}

static void
start_element (void *ctx, 
               const xmlChar *localname, 
               const xmlChar *prefix, const xmlChar *URI, 
               int nb_namespaces, const xmlChar **namespaces,
               int nb_attributes, int nb_defaulted, 
               const xmlChar **atts)
{  
    (void) ctx;
    (void) prefix;
    (void) nb_namespaces;
    (void) namespaces;
    (void) nb_defaulted;
                
    xmlChar *attv;
    
    flush_buffer ();

    pre++;
    rank++;
    level++;
    
    assert (level < STACK_MAX);
    max_level = MAX(level, max_level);

    assert (localname);
    if (xmlStrlen (localname) > TAG_SIZE)
        BAILOUT ("tag name `%s' exceeds length of %u characters", 
                 localname, TAG_SIZE);
        
    /* establish empty namespace prefix */
    if (!URI)
        URI = (xmlChar *) "";        
    if (xmlStrlen (URI) > TAG_SIZE)
        BAILOUT ("Namespace URI `%s' exceeds length of %u characters", 
                 URI, TAG_SIZE);

    stack[level] = (node_t) {
        .pre            = pre
      , .post           = 0
      , .pre_stretched  = rank
      , .post_stretched = 0
      , .parent         = stack + level - 1
      , .size           = 0
      , .level          = level
      , .kind           = elem
      , .localname      = xmlStrdup (localname)
      , .localname_id   = generate_localname_id (localname) 
      , .uri            = xmlStrdup (URI)
      , .uri_id         = generate_uri_id (URI)
      , .value          = (xmlChar *) NULL
      , .guide          = insert_guide_node_ (URI, localname,
                                              stack[level-1].guide,
                                              elem)
    };

    if (nb_attributes) {
        if (shredstate.attributes_separate) {
            while (nb_attributes--) {
                /* atts[0]: localname
                   atts[1]: prefix
                   atts[2]: URI
                   atts[3]: value
                   atts[4]: end of value
                 */    
                /* establish empty namespace prefix */
                if (!atts[2])
                    atts[2] = (xmlChar *) "";   
                    
                if (shredstate.names_separate)
                    fprintf (out_attr,
                             SSZFMT ", " SSZFMT ", %i, %i, \"%.*s\"",
                             att_id++,
                             pre,
                             generate_uri_id (atts[2]),
                             generate_localname_id (atts[0]),
                             (int) (atts[4] - atts[3]),
                             (char*) atts[3]);
                else                           
                    fprintf (out_attr,
                             SSZFMT ", " SSZFMT ", \"%s\", \"%s\", \"%.*s\"",
                             att_id++,
                             pre,
                             (char*) atts[2],
                             (char*) atts[0],
                             (int) (atts[4] - atts[3]),
                             (char*) atts[3]);
                             
                if (shredstate.statistics)
                    fprintf (out_attr, "," SSZFMT,
                             guide_val_ (
                                 insert_guide_node_ (atts[2], atts[0],
                                                     stack[level].guide,
                                                     attr)));
                putc ('\n', out_attr);
                atts += 5;
            }
        } else /* handle attributes like children */
            while (nb_attributes--) {      
                /* establish empty namespace prefix */
                if (!atts[2])
                    atts[2] = (xmlChar *) "";   

                /* extract attribute value */
                attv = xmlStrndup (atts[3], atts[4] - atts[3]);

                flush_node (attr, atts[2], atts[0], attv);

                xmlFree (attv);
                atts += 5;
            }
     }
}

static void
end_element (void *ctx,
             const xmlChar *localname,
             const xmlChar *prefix, const xmlChar *URI)
{
    (void) ctx;
    (void) localname;
    (void) prefix;
    (void) URI;
    
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

    apply_fmt (stack[level]);

    if (shredstate.statistics)
        guide_occurrence (stack[level].guide);

    post++;

    /* free the memory allocated for the element name and the text value */
    if (stack[level].localname) 
        xmlFree (stack[level].localname);
    if (stack[level].uri)       
        xmlFree (stack[level].uri);
    if (stack[level].value)     
        xmlFree (stack[level].value);

    level--;
    assert (level >= 0);    
}   

static void
processing_instruction (void *ctx, 
                        const xmlChar *target, const xmlChar *chars)
{
    (void) ctx;
        
    flush_buffer ();

    /* XPath node tests may refer to the target of an XML 
       processing instruction: processing-instruction(target) */
    flush_node (pi, NULL, target, chars);
}

void
comment (void *ctx, const xmlChar *chars)
{
    (void) ctx;

    flush_buffer ();

    flush_node (comm, NULL, NULL, chars);
}

void
characters (void *ctx, const xmlChar *chars, int n)
{
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
    buf[buf_size - 1] = 0;
    fprintf (err, "%s", buf);
    va_end (az);

    BAILOUT ("libxml error");
}

static xmlSAXHandler saxhandler = {
      .initialized           = XML_SAX2_MAGIC  /* select SAX2 interface */
    , .startDocument         = start_document
    , .endDocument           = end_document 
    , .startElementNs        = start_element
    , .endElementNs          = end_element 
    , .startElement          = NULL
    , .endElement            = NULL
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
};

static void
report (void)
{
    if (text_stripped > 0) {
        fprintf (err, "%u text node/attribute values were stripped to %u "
                      "character(s).\n", text_stripped, text_size);
    }
}

/**
 * Main shredding procedure.
 */
void
SHshredder (const char *s, 
            FILE *shout,
            FILE *attout,
            FILE *namesout,
            FILE *urisout,
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
    out_uris  = urisout;
    err       = stderr;

    /* how many characters should be stored in
     * the value column */
    text_size = shredstate.strip_values;

    text_stripped = 0;
    tag_stripped  = 0;

    /* compile the -F format string */
    compile_fmt (shredstate.format);
    
    /* initialize localname and URI hashes */
    localname_hash = new_hashtable (); 
    uris_hash = new_hashtable ();
    /* pre-insert entry for empty namespace URIs */
    generate_uri_id ((xmlChar *) "");
    
    /* Whether to print the node localnames and URIs
       or the corresponding name ids */
    if (shredstate.names_separate) {
        print_localname = print_localname_id;
        print_uri       = print_uri_id; 
    } else {
        print_localname = print_localname_str;
        print_uri       = print_uri_str;
    }
    
    /* start XML parsing via SAX2 interface */
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

    free_hashtable (localname_hash);
    free_hashtable (uris_hash);
    
    xmlCleanupParser ();

    if (!status->quiet)
        report ();
}

/* vim:set shiftwidth=4 expandtab: */

