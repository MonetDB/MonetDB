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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2011 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

#include "monetdb_config.h"

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
FILE *out_prefixes;
FILE *out_uris;
FILE *guide_out;
FILE *err;

unsigned int text_size;

/* count the tags/text being truncated
 * during the shredding process */
unsigned int prefix_stripped;
unsigned int name_stripped;
unsigned int uri_stripped;
unsigned int text_stripped;

static shred_state_t shredstate;

/* Wrapper to ensure that guide information is only collected if needed */
#define insert_guide_node_(u,n,p,k) ((shredstate.statistics)                   \
                                  ? insert_guide_node (                        \
                                        (u),(n),                               \
                                        generate_uri_id ((xmlChar *) u),       \
                                        generate_localname_id ((xmlChar *) n), \
                                        (p),(k))                               \
                                  : NULL)                            
#define guide_val_(g)             ((shredstate.statistics) \
                                  ? (g)->guide             \
                                  : 0)

/* localname, prefix, and URI hash tables */
static hashtable_t localname_hash;
static hashtable_t prefixes_hash;
static hashtable_t uris_hash;
/* localname, prefix, and URI counters */
static unsigned int global_localname_id;
static unsigned int global_prefix_id;
static unsigned int global_uri_id;

#define BAILOUT(...) do { SHoops (SH_FATAL, __VA_ARGS__);   \
                          free_hashtable (localname_hash);  \
                          free_hashtable (prefixes_hash);   \
                          free_hashtable (uris_hash);       \
                          free (buffer);                    \
                          exit (1);                         \
                        } while (0)


/* buffer for XML node contents/values */
static unsigned int bufsize;
static xmlChar *buffer;
static unsigned int bufpos;

static node_t stack[STACK_MAX];
static int level;
static int max_level;
static nat root;
static nat pre;
static nat post;
static nat rank;
static nat att_id;
static guide_tree_t *guide;
             
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
    if (tuple.value)
        fputs (tuple.value, f);
}

void
print_value_escaped (FILE *f, node_t tuple)
{
    /* escape quotes in quoted string */
    if (tuple.value) {
        char *src = (char *) tuple.value;
        unsigned int start = 0, end;
        for (end = 0; tuple.value[end]; end++)
            /* escape quotes */
            if (tuple.value[end] == '"') {
                print_text (f, &src[start], end - start);
                start = end + 1;
                putc ('"', f); putc ('"', f);
            }
        if (start < end)
            print_text (f, &src[start], end - start);
    }
}

void
print_localname_id (FILE *f, node_t tuple)
{
    fprintf (f, "%i", tuple.localname_id);
}

void
print_localname_str (FILE *f, node_t tuple)
{
    fputs ((char *) tuple.localname, f);
}
    
void
print_prefix_id (FILE *f, node_t tuple)
{
    fprintf (f, "%i", tuple.prefix_id);
}

void
print_prefix_str (FILE *f, node_t tuple)
{
    fputs ((char *) tuple.prefix, f);
}

void
print_uri_id (FILE *f, node_t tuple)
{
    fprintf (f, "%i", tuple.uri_id);
}

void
print_uri_str (FILE *f, node_t tuple)
{
    fputs ((char *) tuple.uri, f);
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

static void lambda_h (node_t n) 
{ if (n.parent) {
      lambda_h (*(n.parent));
      fprintf (out, SSZFMT, n.parent->children + 1);
  }
  fprintf (out, "/");
}

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

static void lambda_x (node_t n) 
{ print_prefix_id (out, n); }

static void lambda_X (node_t n) 
{ print_prefix_str (out, n); }

static void lambda_n (node_t n) 
{ print_localname_id (out, n); }

static void lambda_N (node_t n) 
{ print_localname_str (out, n); }

static void lambda_u (node_t n) 
{ print_uri_id (out, n); }

static void lambda_U (node_t n) 
{ print_uri_str (out, n); }

static void lambda_d (node_t n) 
{ print_number (out, n); }

static void lambda_r (node_t n) 
{ fprintf (out, SSZFMT, n.root); }

void (*print_textnode) (FILE *, node_t);

static void lambda_t (node_t n) 
{ print_textnode (out, n); }

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
  , ['h'] = lambda_h 
  , ['s'] = lambda_s 
  , ['l'] = lambda_l 
  , ['k'] = lambda_k
  , ['p'] = lambda_p
  , ['P'] = lambda_P
  , ['n'] = lambda_n
  , ['N'] = lambda_N
  , ['x'] = lambda_x
  , ['X'] = lambda_X
  , ['u'] = lambda_u
  , ['U'] = lambda_U
  , ['d'] = lambda_d
  , ['r'] = lambda_r
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
generate_id (const xmlChar *str, unsigned int *global_id,
             hashtable_t ht, FILE *f)
{
    int id;
   
    if (!str)
        return 0; /* we ensured in initialize() that there is
                     always an entry 0 */
                     
    id = hashtable_find (ht, (char *) str);

    if (NOKEY (id)) {
        /* key not found, create a new name id */
        id = (*global_id)++;
        /* add the (str, id) pair into the hash table */
        hashtable_insert (ht, (char *) str, id);
        /* print the name binding if necessary */
        if (shredstate.names_separate)
            fprintf (f, "%i, \"%s\"\n", id, (char*) str);
    }

    return id;
}
#define generate_localname_id(str) \
        generate_id((str),&global_localname_id,localname_hash,out_names)
#define generate_prefix_id(str) \
        generate_id((str),&global_prefix_id,prefixes_hash,out_prefixes)
#define generate_uri_id(str) \
        generate_id((str),&global_uri_id,uris_hash,out_uris)

static void
flush_node (kind_t kind, 
            const xmlChar *localname, 
            const xmlChar *prefix, const xmlChar *URI, 
            const xmlChar *value)
{
    char *short_value    = NULL,
         *prefix_copy    = NULL,
         *localname_copy = NULL,
         *URI_copy       = NULL;

    pre++;
    rank++;
    level++;

    max_level = MAX(level, max_level);

    /* check if prefix is larger than text_size characters */
    if (text_size && prefix && (unsigned int) xmlStrlen (prefix) > text_size) {
        prefix_stripped++;
        prefix_copy = strndup ((char *) prefix, text_size);
    } else if (prefix)
        prefix_copy = strdup ((char *) prefix);
    else
        prefix_copy = strdup ("");
        
    /* check if tagname is larger than text_size characters */
    if (text_size && localname &&
        (unsigned int) xmlStrlen (localname) > text_size) {
        name_stripped++;
        localname_copy = strndup ((char *) localname, text_size);
    } else if (localname)
        localname_copy = strdup ((char *) localname);
    else
        localname_copy = strdup ("");
    
    /* check if uri is larger than text_size characters */
    if (text_size && URI && (unsigned int) xmlStrlen (URI) > text_size) {
        name_stripped++;
        URI_copy = strndup ((char *) URI, text_size);
    } else if (URI)
        URI_copy = strdup ((char *) URI);
    else
        URI_copy = strdup ("");

    /* check if value is larger than text_size characters */
    if (text_size && value && (unsigned int) xmlStrlen (value) > text_size) {
        text_stripped++;
        short_value = strndup ((char *) value, text_size);
    }

    assert (value);

    stack[level] = (node_t) {
        .root           = root
      , .pre            = pre
      , .post           = post
      , .pre_stretched  = rank
      , .post_stretched = rank + 1
      , .parent         = stack + level - 1
      , .size           = 0
      , .children       = 0
      , .level          = level
      , .kind           = kind
      , .prefix         = prefix_copy
      , .prefix_id      = generate_prefix_id (prefix)
      , .localname      = localname_copy
      , .localname_id   = generate_localname_id (localname)
      , .uri            = URI_copy
      , .uri_id         = generate_uri_id (URI)
      , .value          = short_value ? short_value : (char *) value
      , .guide          = insert_guide_node_ ((char *) URI, (char *) localname,
                                              stack[level-1].guide,
                                              kind)
    };

    post++;
    rank++;
    
    apply_fmt (stack[level]);

    if (prefix_copy) {
        free (prefix_copy);
        stack[level].prefix = NULL;
    }
    if (localname_copy) {
        free (localname_copy);
        stack[level].localname = NULL;
    }
    if (URI_copy) {   
        free (URI_copy);
        stack[level].uri = NULL;
    }
    if (short_value) {
        free (short_value);
        stack[level].value = NULL;
    }

    level--;
    /* add one more child node */
    stack[level].children++;
}

static void
flush_buffer (void)
{
    if (bufpos) {
        flush_node (text, NULL, NULL, NULL, buffer);
    }

    buffer[0] = '\0';
    bufpos = 0;
}

static void
start_document (void *ctx)
{
    char *doc_name;

    (void) ctx;

    root = pre;

    if (text_size && strlen (shredstate.doc_name) > text_size) {
        text_stripped++;
        doc_name = strndup (shredstate.doc_name, text_size);
    } else
        doc_name = strdup (shredstate.doc_name);


    /* create a new node */
    stack[level] = (node_t) {
          .root           = root
        , .pre            = pre
        , .post           = 0
        , .pre_stretched  = rank
        , .post_stretched = 0
        , .parent         = NULL
        , .size           = 0
        , .children       = 0
        , .level          = level
        , .kind           = doc
        , .prefix         = NULL
        , .prefix_id      = 0
        , .localname      = NULL
        , .localname_id   = 0
        , .uri            = NULL
        , .uri_id         = 0
        , .value          = doc_name
        , .guide          = guide
    };
    /* extend guide counter */
    if (guide)
        guide->count++;
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

    pre++;
    post++;

    if (shredstate.statistics) 
        guide_occurrence (stack[level].guide);

    if (stack[level].value) {
        free (stack[level].value);
        stack[level].value = NULL;
    }
}

static void
start_element (void *ctx, 
               const xmlChar *localname, 
               const xmlChar *prefix, const xmlChar *URI, 
               int nb_namespaces, const xmlChar **namespaces,
               int nb_attributes, int nb_defaulted, 
               const xmlChar **atts)
{  
    char *prefix_copy    = NULL,
         *localname_copy = NULL,
         *URI_copy       = NULL;

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

    /* establish empty namespace prefix */
    if (!prefix)
        prefix = (xmlChar *) "";        
    if (!URI)
        URI = (xmlChar *) "";        

    /* check if prefix is larger than text_size characters */
    if (text_size && (unsigned int) xmlStrlen (prefix) > text_size) {
        prefix_stripped++;
        prefix_copy = strndup ((char *) prefix, text_size);
    } else
        prefix_copy = strdup ((char *) prefix);
        
    /* check if tagname is larger than text_size characters */
    if (text_size && (unsigned int) xmlStrlen (localname) > text_size) {
        name_stripped++;
        localname_copy = strndup ((char *) localname, text_size);
    } else
        localname_copy = strdup ((char *) localname);
        
    /* check if uri is larger than text_size characters */
    if (text_size && (unsigned int) xmlStrlen (URI) > text_size) {
        name_stripped++;
        URI_copy = strndup ((char *) URI, text_size);
    } else
        URI_copy = strdup ((char *) URI);

    stack[level] = (node_t) {
        .root           = root
      , .pre            = pre
      , .post           = 0
      , .pre_stretched  = rank
      , .post_stretched = 0
      , .parent         = stack + level - 1
      , .size           = 0
      , .children       = 0
      , .level          = level
      , .kind           = elem
      , .prefix         = prefix_copy
      , .prefix_id      = generate_prefix_id (prefix) 
      , .localname      = localname_copy
      , .localname_id   = generate_localname_id (localname) 
      , .uri            = URI_copy
      , .uri_id         = generate_uri_id (URI)
      , .value          = NULL
      , .guide          = insert_guide_node_ ((char *) URI, (char *) localname,
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
                if (!atts[1])
                    atts[1] = (xmlChar *) "";        
                if (!atts[2])
                    atts[2] = (xmlChar *) "";   
                    
                if (shredstate.names_separate)
                    fprintf (out_attr,
                             SSZFMT ", " SSZFMT ", %i, %i, %i, \"%.*s\"",
                             att_id++,
                             pre,
                             generate_prefix_id (atts[1]),
                             generate_uri_id (atts[2]),
                             generate_localname_id (atts[0]),
                             (int) (atts[4] - atts[3]),
                             (char*) atts[3]);
                else                           
                    fprintf (out_attr,
                             SSZFMT ", " SSZFMT 
                             ", \"%s\", \"%s\", \"%s\", \"%.*s\"",
                             att_id++,
                             pre,
                             (char*) atts[1],
                             (char*) atts[2],
                             (char*) atts[0],
                             (int) (atts[4] - atts[3]),
                             (char*) atts[3]);
                             
                if (shredstate.statistics)
                    fprintf (out_attr, "," SSZFMT,
                             guide_val_ (
                                 insert_guide_node_ ((char *) atts[2], 
                                                     (char *) atts[0],
                                                     stack[level].guide,
                                                     attr)));
                putc ('\n', out_attr);
                atts += 5;
            }
        } else /* handle attributes like children */
            while (nb_attributes--) {      
                /* establish empty namespace prefix */
                if (!atts[1])
                    atts[1] = (xmlChar *) "";        
                if (!atts[2])
                    atts[2] = (xmlChar *) "";   

                /* extract attribute value */
                attv = xmlStrndup (atts[3], atts[4] - atts[3]);

                flush_node (attr, atts[0], atts[1], atts[2], attv);

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
    /* copy the text value of a text node if it is the only child */
    if (bufpos && pre == stack[level].pre)
        stack[level].value = !text_size || bufpos < text_size 
                           ? strdup ((char *) buffer)
                           : strndup ((char *) buffer, text_size);

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
    if (stack[level].prefix) {
        free (stack[level].prefix);
        stack[level].prefix = NULL;
    }
    if (stack[level].localname) {
        free (stack[level].localname);
        stack[level].localname = NULL;
    }
    if (stack[level].uri) {   
        free (stack[level].uri);
        stack[level].uri = NULL;
    }
    if (stack[level].value) {
        free (stack[level].value);
        stack[level].value = NULL;
    }

    level--;
    /* add one more child node */
    stack[level].children++;
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
    flush_node (pi, target, NULL, NULL, chars);
}

void
comment (void *ctx, const xmlChar *chars)
{
    (void) ctx;

    flush_buffer ();

    flush_node (comm, NULL, NULL, NULL, chars);
}

void
characters (void *ctx, const xmlChar *chars, int n)
{
    (void) ctx;

    while (bufpos + n >= bufsize) {
        char *buf = malloc (2*bufsize);
        memcpy (buf, (char *) buffer, bufpos);
        free (buffer);
        buffer = (xmlChar *) buf;
        bufsize *= 2;
    }
    memcpy ((char *) buffer + bufpos, (char *) chars, n);
    bufpos += n;
    buffer[bufpos] = '\0';
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
    if (prefix_stripped > 0) {
        fprintf (err, "%u attribute/element namespace prefixes were stripped "
                      "to %u character(s).\n", prefix_stripped, text_size);
    }
    if (name_stripped > 0) {
        fprintf (err, "%u local attribute/element names were stripped to %u "
                      "character(s).\n", name_stripped, text_size);
    }
    if (uri_stripped > 0) {
        fprintf (err, "%u attribute/element namespace uris were stripped to %u "
                      "character(s).\n", uri_stripped, text_size);
    }
    if (text_stripped > 0) {
        fprintf (err, "%u text node/attribute values were stripped to %u "
                      "character(s).\n", text_stripped, text_size);
    }
}

static void
initialize (FILE *shout,
            FILE *attout,
            FILE *namesout,
            FILE *prefixesout,
            FILE *urisout)
{
    /* bind the different output files to global variables
       to make them accessible inside the callback functions */
    out          = shout;
    out_attr     = attout;
    out_names    = namesout; 
    out_prefixes = prefixesout; 
    out_uris     = urisout;
    err          = stderr;

    /* how many characters should be stored in
     * the value column */
    text_size = shredstate.strip_values;

    text_stripped = 0;
    name_stripped = 0;
    uri_stripped  = 0;

    /* compile the -F format string */
    compile_fmt (shredstate.format);
    
    /* initialize localname, prefix, and URI hashes */
    localname_hash = new_hashtable (); 
    prefixes_hash = new_hashtable ();
    uris_hash = new_hashtable ();
    /* initialize localname, prefix, and URI counters */
    global_localname_id = 0;
    global_prefix_id    = 0;
    global_uri_id       = 0;

    /* pre-insert entries */
    generate_localname_id ((xmlChar *) "");
    generate_prefix_id ((xmlChar *) "");
    generate_uri_id ((xmlChar *) "");

    /* ensure that there is at least one id
       for localname, prefix, and URI */
    assert (global_localname_id == 1 &&
            global_prefix_id == 1 &&
            global_uri_id == 1);
    
    /* Whether to escape the quotes in the textnode content */
    if (shredstate.escape_quotes)
        print_textnode = print_value_escaped;
    else
        print_textnode = print_value;

    bufsize = 4096;
    buffer = malloc (bufsize);
    bufpos = 0;

    /* initialize everything with zero */
    pre       = 0;
    post      = 0;
    rank      = 0;
    level     = 0;
    max_level = 0;
    att_id    = 0;

    /* initialize global guide node shared for all document nodes */
    guide = insert_guide_node_ (NULL, shredstate.doc_name, NULL, doc);

    /* reset guide document counter */
    if (guide)
        guide->count = 0;
}

/**
 * Main shredding procedure.
 */
void
SHshredder (const char *s, 
            FILE *shout,
            FILE *attout,
            FILE *namesout,
            FILE *prefixesout,
            FILE *urisout,
            FILE *guideout,
            shred_state_t *status)
{
    /* XML parser context */
    xmlParserCtxtPtr ctx;
    shredstate = *status;

    assert (shout);
    initialize (shout, attout, namesout, prefixesout, urisout);

    /* start XML parsing via SAX2 interface */
    ctx = xmlCreateFileParserCtxt (s);

    /* as we replace the SAX handler by our own one
       we need to free it to avoid memory leaks */
    if (ctx->sax)
        xmlFree (ctx->sax);
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
    free_hashtable (prefixes_hash);
    free_hashtable (uris_hash);
    
    free (buffer);

    /* we don't want to free our SAX handler */
    ctx->sax = NULL;
    xmlFreeParserCtxt (ctx);

    xmlCleanupParser ();

    if (!status->quiet)
        report ();
}

/**
 * Table shredding procedure.
 */
void
SHshredder_table (const char *s, 
                  FILE *shout,
                  FILE *attout,
                  FILE *namesout,
                  FILE *prefixesout,
                  FILE *urisout,
                  FILE *guideout,
                  FILE *tableout,
                  shred_state_t *status)
{
    /* XML parser context */
    xmlParserCtxtPtr ctx;
    shredstate = *status;

    /* table handling initialization */
    FILE *table_file,
         *xml_file = NULL;
    int len = 1024,
        length,
        num = 0;
    long int start;
    char line[len],
         *str,
         *delimiter,
         *file,
         *file_old = NULL,
         *xml;

    assert (shout);
    initialize (shout, attout, namesout, prefixesout, urisout);

    table_file = fopen (s, "r");
    do {
        /* assume that our table encoding contains no newlines in a row */
        str = fgets (line, len, table_file);

        /* we're at the end or hit an error */
        if (str != line)
            break;

        if (str[strlen(str)-1] != '\n')
            SHoops (SH_FATAL, "line in table layout too long\n");

        /* look for a ',' as delimiter */
        delimiter = strchr(str,',');
        /* split string */
        *delimiter = 0;
        file = str;

        str = delimiter+1;
        /* look for a ',' as delimiter */
        delimiter = strchr(str,',');
        /* split string */
        *delimiter = 0;
        start = atol (str);
        length = atoi (delimiter+1);

        /* the first time we need to get a file handle */
        if (!file_old) {
            xml_file = fopen (file, "r");
            if (!xml_file)
                SHoops (SH_FATAL, "could not read file '%s'\n", file);
            file_old = strdup (file);
        }
        /* we need to exchange our file handle if the file changes */
        else if (strcmp(file, file_old)) {
            fclose (xml_file);
            xml_file = fopen (file, "r");
            if (!xml_file)
                SHoops (SH_FATAL, "could not read file '%s'\n", file);
            free (file_old);
            file_old = strdup (file);
        }

        /* jump to the correct place */
        if (fseek (xml_file, start, SEEK_SET))
            SHoops (SH_FATAL, "seek error\n");

        /* collect the XML file */
        xml = malloc (length+1);
        if (fread (xml, 1, length, xml_file) != (size_t) length)
            SHoops (SH_FATAL, "read error\n");

        /* start XML parsing via SAX2 interface */
        ctx = xmlCreateMemoryParserCtxt (xml, length);

        /* as we replace the SAX handler by our own one
           we need to free it to avoid memory leaks */
        if (ctx->sax)
            xmlFree (ctx->sax);
        ctx->sax = &saxhandler;

        (void) xmlParseDocument (ctx);
        
        if (!ctx->wellFormed) {
            SHoops (SH_FATAL, "XML at offset %l input not well-formed\n",
                    start);
        }

        /* we don't want to free our SAX handler */
        ctx->sax = NULL;
        xmlFreeParserCtxt (ctx);

        free (xml);

        /* fill references table */
        fprintf (tableout, "%i, " SSZFMT "\n", num++, root);

    } while (true);

    /* release remaining file handle */
    if (file_old) {
        fclose (xml_file);
        free (file_old);
    }

    /* bail out */
    if (ferror (table_file))
        SHoops (SH_FATAL, "problem in reading file %s\n", s);

    fclose (table_file);

    /* This frees the libxml library resources */
    xmlCleanupParser ();

    /* print statistics if required */
    if (shredstate.statistics) {
        print_guide_tree (guideout, stack[0].guide, 0);
        free_guide_tree (stack[0].guide);
    }

    free_hashtable (localname_hash);
    free_hashtable (prefixes_hash);
    free_hashtable (uris_hash);
    
    free (buffer);

    if (!status->quiet)
        report ();
}

/* vim:set shiftwidth=4 expandtab: */
