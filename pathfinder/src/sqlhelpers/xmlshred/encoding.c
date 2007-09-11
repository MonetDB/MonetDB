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

FILE * out;
FILE * out_attr;
FILE * guide_out;

typedef struct node_t node_t;
struct node_t {
    nat        pre;
    nat        apre;
    nat        post;
    nat        pre_stretched;
    nat        post_stretched;
    nat        size;
    int        level;
    int        name_id;
    node_t   * parent;
    kind_t     kind;
    xmlChar  * prop;
    nat        guide;
};

/* print a tuple */
static void print_tuple (node_t tuple, const char *format);
static void flush_buffer (void);

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

/* return a brand new name_id */
static int new_nameid(void)
{
    static unsigned int name = NAME_ID;
    return name++;
}

static void
start_document (void *ctx)
{
    /* calling convention */
    (void)ctx;

    /* initialize everything with zero */
    pre       = 0;
    post      = 0;
    rank      = 0;
    level     = 0;
    max_level = 0;
    att_id    = 0;

    /* insert guide node */
    current_guide_node = insert_guide_node (xmlCharStrndup (shredstate.infile,
                                                    FILENAME_MAX),
                                            NULL, doc);

    /* create a new node */
    stack[level] = (node_t) {
          .pre            = pre
        , .post           = 0
        , .pre_stretched  = rank
        , .post_stretched = 0
        , .size           = 0
        , .level          = level
        , .parent         = NULL
        , .kind           = doc
        , .prop           = (shredstate.infile_given)? 
                            xmlCharStrndup(shredstate.infile,
                                           (size_t)PROPSIZE) :
                            xmlCharStrndup("",strlen(""))
        , .guide          = current_guide_node->guide
    };

}

static void
end_document (void *ctx)
{
    /* calling convention */
        (void)ctx;

    flush_buffer ();

    assert (level == 0);

    post++;
    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    print_tuple (stack[level], shredstate.format);

    free (stack[level].prop);

    assert (current_guide_node->guide == GUIDE_INIT);
}

static void
start_element (void *ctx, const xmlChar *tagname, const xmlChar **atts)
{


    /* calling convention */
    (void)ctx;

    guide_tree_t *attr_guide_node = NULL;
    current_guide_node = insert_guide_node(tagname,
	                             current_guide_node, elem);

    /* check if tagname is larger than TAG_SIZE characters */
    if (xmlStrlen(tagname) > TAG_SIZE) {
        SHoops (SH_FATAL, "We support only tagnames with length <= %i\n", 
                          TAG_SIZE);
        free_hash (hash_table);
        exit(1);
    }
     
    flush_buffer ();

    pre++;
    rank++;
    level++;

    if (level > max_level)
        max_level = level;

    assert (level < STACK_MAX);

    /* try to find the tagname in the
     * hashtable */
    int name_id = -1; 

    if(shredstate.sql) { 
        name_id = find_element(hash_table, (char*)tagname);

        /* key not found */
        if (NOKEY(name_id)) {
            /* create a new id */
            name_id = new_nameid();     
            hashtable_insert(hash_table, (char*)tagname, name_id);
            fprintf (out_attr, "%i, \"%s\"\n", name_id, 
                     strndup((char*)tagname, PROPSIZE));
        }
    }


    stack[level] = (node_t) {
        .pre            = pre,
        .apre           = -1,
        .post           = 0,
        .pre_stretched  = rank,
        .post_stretched = 0,
        .size           = 0,
        .level          = level,
        .parent         = stack + level - 1,
        .name_id        = name_id, 
        .kind           = elem,
        .prop           = (!shredstate.sql)?
                          xmlStrndup (tagname, (size_t)TAG_SIZE):
                          (xmlChar *)NULL,
        .guide          = current_guide_node->guide
    };

    if (!shredstate.suppress_attributes && atts) {
        if (!shredstate.sql)
            while (*atts) {
                attr_guide_node = insert_guide_node(atts[0],
                    current_guide_node, attr);
                fprintf (out_attr, "%lu, %lu, \"%s\", \"%s\", %lu\n", 
                         att_id++, pre, atts[0], atts[1], attr_guide_node->guide);
                atts += 2;
            }
        /* handle attributes as we need for sql generation */
        else
            while (*atts) {

                    /* check if tagname is larger than TEXT_SIZE characters */
                    if (xmlStrlen(atts[1]) > TEXT_SIZE) {
                        SHoops (SH_FATAL, "We support only attribute content"
                                          " with length <= %i\n", TEXT_SIZE);
                        free_hash(hash_table);
                        exit(1);
                    }
                    /* check if tagname is larger than TAG_SIZE characters */
                    if (strlen((char*)atts[0]) > TAG_SIZE) {
                        fprintf(stderr, "We support only attributes with "
                                        "length <= %i\n", TAG_SIZE);

                             free_hash(hash_table);
                                  exit(1);
                        }
 
                        /* try to find the tagname in the
                         * hashtable */
                        name_id = find_element(hash_table, (char*)atts[0]);

                        /* key not found */
                        if(NOKEY(name_id)) {

                            /* create a new id */
                            name_id = new_nameid();     

                            hashtable_insert(hash_table, (char*)atts[0], name_id);
                            fprintf (out_attr, "%i, \"%s\"\n", name_id,
	    	    			         strndup((char*)atts[0],PROPSIZE));
                        }

                     attr_guide_node = insert_guide_node(atts [0], 
                                                         current_guide_node, attr);

                     pre++;
                     print_tuple ((node_t) {
                                      .pre = pre,
                                      .apre = -1,
                                      .post = 0,
                                      .pre_stretched = 0,
                                      .post_stretched = 0,
                                      .size = 0,
                                      .level = level + 1,
                                      .parent = 0,
                                      .name_id = name_id,
                                      .kind = attr,
                                      .prop = (xmlChar*)atts[1],
                                      .guide = attr_guide_node->guide,
                                   }, shredstate.format);
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

    print_tuple (stack[level], shredstate.format);

    post++;
    free (stack[level].prop);
    level--;
    assert (level >= 0);

    current_guide_node = current_guide_node->parent;
}

static void
processing_instruction (void *ctx, const xmlChar *target,  const xmlChar *chars)
{
    /* calling convention */
    (void)ctx;
    (void)chars;
        
    leaf_guide_node = insert_guide_node(target, current_guide_node, pi);

    if (bufpos < PROPSIZE) {
        snprintf ((char*)buf + bufpos, MIN ((int)strlen((char*)target),
		          PROPSIZE - bufpos) + 1, 
                  "%s", (char*)target);
        bufpos += MIN ((int)strlen((char*)target), PROPSIZE - bufpos);
    }

    buf[PROPSIZE] = '\0';

    flush_buffer();
}

void
comment (void *ctx,  const xmlChar *chars)
{
    /* calling convention */
        (void)ctx;

    leaf_guide_node = insert_guide_node(NULL, current_guide_node, comm);

    if (bufpos < PROPSIZE) {
        snprintf ((char*)buf + bufpos, MIN ((int)strlen((char*)chars),
		          PROPSIZE - bufpos) + 1, 
            "%s", (char*)chars);
        bufpos += MIN ((int)strlen((char*)chars), PROPSIZE - bufpos);
    }

    buf[PROPSIZE] = '\0';

    flush_buffer();
}

void
characters (void *ctx, const xmlChar *chars, int n)
{
    /* calling convention */
    (void)ctx;

    if (bufpos < PROPSIZE) {
        snprintf ((char*)buf + bufpos, MIN (n, PROPSIZE - bufpos) + 1, "%s", (char *)chars);
        bufpos += MIN (n, PROPSIZE - bufpos);
    }

    buf[PROPSIZE] = '\0';

    leaf_guide_node = insert_guide_node(NULL, current_guide_node, text);
}

static void
flush_buffer (void)
{
    /* check if tagname is larger than TEXT_SIZE characters */
    if (xmlStrlen (buf) > TEXT_SIZE) {
        free_hash (hash_table); 
        SHoops (SH_FATAL, "We support text with length <= %i\n", TEXT_SIZE);
        exit(1);
    }

    if (buf[0]) {
        pre++;
        rank += 2;
        level++;

        if (level > max_level)
            max_level = level;
        
        stack[level] = (node_t) {
            .pre            = pre,
            .apre           = -1,
            .post           = post,
            .pre_stretched  = rank - 1,
            .post_stretched = rank,
            .size           = 0,
            .level          = level,
            .parent         = stack + level - 1,
            .name_id        = -1,
            .kind           = leaf_guide_node->kind,
            .prop           = buf,
            .guide          = leaf_guide_node->guide,
        };

        post++;

        print_tuple (stack[level], shredstate.format);

        level--;
    }

    buf[0] = '\0';
    bufpos = 0;
}

static void
print_tuple (node_t tuple, const char * format)
{
    unsigned int i;
    for (i = 0; format[i]; i++)
         if (format[i] == '%') {
             i++;
             switch (format[i]) {
                 case 'e':
                                     if (tuple.pre != -1)
                         fprintf (out, "%lu", tuple.pre);
                     break;
                 case 'o':  
                                         fprintf (out, "%lu", tuple.post); break;
                 case 'E':
                                     fprintf (out, "%lu", tuple.pre_stretched); break;
                 case 'O':
                                     fprintf (out, "%lu", tuple.post_stretched); break;
                 case 's':
                                     fprintf (out, "%lu", tuple.size); break;
                 case 'l':
                                     fprintf (out, "%u",  tuple.level); break;
                 case 'p':
                                     if (tuple.parent)
                         fprintf (out, "%lu", tuple.parent->pre);
                     else
                         fprintf (out, "NULL");
                     break;
                 case 'P':
                                     if (tuple.parent)
                         fprintf (out, "%lu",tuple.parent->pre_stretched);
                     else
                         fprintf (out, "NULL");
                     break;
                 case 'k': 
                     print_kind (out, tuple.kind);
                     break;
                 case 'n':
                     if (tuple.name_id != -1)
                         fprintf(out, "%i", tuple.name_id);
                                     break;
                 case 'a':
                     if (tuple.apre != -1)
                         fprintf(out, "%lu", tuple.apre);
                                     break;
                 case 't':
                     if (tuple.prop) {
                         unsigned int i;
                         putc ('"', out);
                         for (i = 0; i < PROPSIZE && tuple.prop[i]; i++)
                             switch (tuple.prop[i]) {
                                 case '\n': 
                                                                     putc (' ', out); break;
                                 case '"':
                                                                     putc ('"', out); putc ('"', out); break;
                                 default:
                                                                     putc (tuple.prop[i], out);
                             }
                             putc ('"', out);
                      }
                      else {
                          fprintf(out, "NULL");
                      }
                      break;    
                   case 'g':  fprintf (out, "%lu", tuple.guide); break;
                   default:   putc (format[i], out); break;
             }
         }
         else
             putc (format[i], out);
    putc ('\n', out);
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

static xmlSAXHandler saxhandler = {
      .startDocument         = start_document
    , .endDocument           = end_document 
    , .startElement          = start_element 
    , .endElement            = end_element 
    , .characters            = characters
    , .processingInstruction = processing_instruction
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

/**
 * Main shredding procedure.
 */
int
SHshredder (const char *s, 
            FILE *shout,
			FILE *attout,
            FILE *guideout,
			shred_state_t *status)
{
    assert (s);
	assert (shout);
	assert (attout);
    assert (guideout);

    /* XML parser context */
    xmlParserCtxtPtr  ctx;
    shredstate = *status;

	/* output should be given in parameterlist,
	 * instead creating within this module */
	out = shout;
	out_attr = attout;

    /* if we need sql encoding we need to initialize
	 * the hashtable */
    if (shredstate.sql)
        hash_table = new_hashtable (); 

    /* start XML parsing */
    ctx = xmlCreateFileParserCtxt (s);
    ctx->sax = &saxhandler;

    (void) xmlParseDocument (ctx);
    
    if (!ctx->wellFormed) {
        SHoops (SH_FATAL, "XML input not well-formed\n");
    }

    if (shredstate.sql)
        print_guide_tree (guideout, current_guide_node, 0);

    return 0;
}
