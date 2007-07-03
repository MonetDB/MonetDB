/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Create a guide tree from a given file. The DTD of the guide tree is 
 * hardcoded in this file and is: 
 *      <!ELEMENT node (EMPTY | node*)>
 *      <!ATTLIST node guide CDATA #REQUIRED>
 *      <!ATTLIST node count CDATA #REQUIRED>
 *      <!ATTLIST node kind (1|2|3|4|5|6) #REQUIRED>
 *      <!ATTLIST node name CDATA #IMPLIED>
 *
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */


#include "pathfinder.h"	

#include <stdio.h>
#include <stdbool.h>
#include <string.h>

/* SAX parser interface (libxml2) */
#include <libxml/parser.h>
#include <libxml/parserInternals.h>

#include "oops.h"
#include "load_stats.h"
#include "mem.h"
#include "options.h"

/* Current node in the XML file */
PFguide_tree_t  *current_guide_node = NULL;
/* the root element of the guide tree */
PFguide_tree_t  *root_guide_node = NULL;

/* Add a new child to the parent */
static void add_guide_child(PFguide_tree_t *parent, PFguide_tree_t *child);

/* start of a XML element */
static void
start_element (void *ctx, const xmlChar *tagname, const xmlChar **atts)
{
    PFguide_tree_t  *new_guide_node = NULL;

    unsigned int guide = 0;     /* the guide number of the node */
    unsigned int count = 0;     /* count of same nodes */
    PFguide_kind_t kind = 0;    /* kind if the guide node */
    char *tag_name = NULL;      /* tag name of the guide node */

    (void) ctx;                 /* pacify compiler */
    (void) tagname;             /* pacify compiler */

    /* get attributes */
    if((atts != NULL) && *atts) {
        guide = atoi((char *)atts[1]);

        atts += 2;
        count = atoi((char *)atts[1]); 

        atts += 2;
        kind = atoi((char *)atts[1]) - 1;

        atts += 2;
        tag_name = (*atts != NULL) ? (char*)atts[1] : NULL;
    }

    /* create the first guide node */
    if(root_guide_node == NULL) {
        new_guide_node = (PFguide_tree_t*)PFmalloc(sizeof(PFguide_tree_t));
        *new_guide_node = (PFguide_tree_t) {
            .guide = guide,
            .count = count,
            .kind  = kind,
            .tag_name = tag_name != NULL ?
                (char*)PFmalloc(sizeof(char)*(strlen(tag_name)+1)) :
                NULL, 
            .parent = NULL,
            .child_list = NULL,
        };

        /* copy the string, otherwise it will be lost */
        if(new_guide_node->tag_name != NULL) 
            new_guide_node->tag_name = strncpy(new_guide_node->tag_name, 
                tag_name, strlen(tag_name));

        /* set root guide and current guide */
        root_guide_node = new_guide_node;
        current_guide_node = new_guide_node;
    } else {
        /* create all other guide nodes */
        new_guide_node = (PFguide_tree_t*)PFmalloc(sizeof(PFguide_tree_t));
        *new_guide_node = (PFguide_tree_t) {
            .guide = guide,
            .count = count,
            .kind  = kind,
            .tag_name = tag_name != NULL ? 
                (char*)PFmalloc(sizeof(char)*(strlen(tag_name)+1)) : 
                NULL,
            .parent = current_guide_node,
            .child_list = NULL,
        };

        /* copy the string, otherwise it will be lost */
        if(new_guide_node->tag_name != NULL)
            new_guide_node->tag_name = strncpy(new_guide_node->tag_name, 
                tag_name, strlen(tag_name));

        /* create association between parent and child */
        add_guide_child(current_guide_node, new_guide_node);
        /* set actual node as current node */
        current_guide_node = new_guide_node;        
    }
}

/* end of a XML element */
static void
end_element ()
{
    /* go a level higher in the tree */
    if(current_guide_node->parent != NULL) 
        current_guide_node = current_guide_node->parent;

}

static xmlSAXHandler saxhandler = {
      .startDocument         = NULL
    , .endDocument           = NULL
    , .startElement          = start_element
    , .endElement            = end_element
    , .characters            = NULL
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

/* create the guide tree and return it */
PFguide_tree_t* 
PFguide_tree()
{

    /* guide filename */
    char  *filename = NULL;
    /* Read guide filename as option */
    PFarray_t *options = NULL;
    
    /* get the filename */
    options = PFenv_lookup (PFoptions,
        PFqname (PFns_lib, "guide"));

    if (!options) {
        PFlog ("pf:guide not set.\n");
        return NULL;
    } else {   
        /* it will be used the first item only to create the guide */
        filename = *((char **) PFarray_at (options, 0));
    } 

    /* DTD of the guide XML file */
    char *dtd = "<!ELEMENT node (EMPTY | node*)>"
                "<!ATTLIST node guide CDATA #REQUIRED>"
                "<!ATTLIST node count CDATA #REQUIRED>"
                "<!ATTLIST node kind (1|2|3|4|5|6) #REQUIRED>"
                "<!ATTLIST node name CDATA #IMPLIED>";

    /* Context pointer */
    xmlValidCtxtPtr          validCtxt;
    /* DTD for evaluating a file */
    xmlDtdPtr                dtdPtr;
    /* to read the DTD from memory */
    xmlParserInputBufferPtr  inputBuffer;
    /* the resulting document tree */    
    xmlDocPtr doc;  
    /* result of parsing */
    int result = 0;

    /* Allocate a validation context structure */
    validCtxt = xmlNewValidCtxt();

    /* document to be validated */
    doc = xmlRecoverFile(filename);

    /* Create a buffered parser input from memory */
    inputBuffer = xmlParserInputBufferCreateMem(dtd, strlen(dtd), 
        XML_CHAR_ENCODING_NONE);

    /* Load and parse a DTD */
    dtdPtr = xmlIOParseDTD(NULL, inputBuffer, 
        XML_CHAR_ENCODING_NONE);

    /* Try to validate the document against the dtd */
    result = xmlValidateDtd(validCtxt, doc, dtdPtr);
    
    if(result == 0) { 
        PFlog ("XML input invalid\t");  
        return NULL; 
    }

    /* start XML parsing */
    xmlParserCtxtPtr   ctx;

    ctx = xmlCreateFileParserCtxt (filename);
    ctx->sax = &saxhandler;

    (void) xmlParseDocument (ctx);

    /* Document is not well-formed */
    if (! ctx->wellFormed) {
        PFlog ("XML input not well-formed. Exit\n");
        exit (EXIT_FAILURE);
    }

    return root_guide_node;
}

/* create association between parent and child */
static void
add_guide_child(PFguide_tree_t *parent, PFguide_tree_t *child)
{
    
    if((parent == NULL) || (child == NULL))
        return;

    /* insert new guide node in the child list */
    if(parent->child_list == NULL) {
        parent->child_list = PFarray(sizeof(PFguide_tree_t**));
    }
    child->parent = parent;
    *(PFguide_tree_t**) PFarray_add(parent->child_list) = child;

    return;
}
/* vim:set shiftwidth=4 expandtab filetype=c: */

