/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Create a guide tree from a given file. The DTD of the guide tree is
 * hardcoded in this file and is:
 *      <!ELEMENT node (EMPTY | node*)>
 *      <!ATTLIST node guide CDATA #REQUIRED>
 *      <!ATTLIST node count CDATA #REQUIRED>
 *      <!ATTLIST node min CDATA #REQUIRED>
 *      <!ATTLIST node max CDATA #REQUIRED>
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */


#include "pathfinder.h" 

#include <stdio.h>
#ifdef HAVE_STDBOOL_H
#include <stdbool.h>
#endif
#include <string.h>
#include <assert.h>

/* SAX parser interface (libxml2) */
#include <libxml/parser.h>
#include <libxml/parserInternals.h>

#include "oops.h"
#include "load_stats.h"
#include "mem.h"
#include "options.h"

/* Current node in the XML file */
static PFguide_tree_t *current_guide_node;

/* Root guide node in the XML file */
static PFguide_tree_t *root_guide_node;

static int level;

/* start of a XML element */
static void
start_element (void *ctx, const xmlChar *tagname, const xmlChar **atts)
{
    PFguide_tree_t  *new_guide_node = NULL;

    unsigned int   guide    = 0;    /* the guide number of the node */
    unsigned int   count    = 0;    /* count of same nodes */
    unsigned int   min      = 0;    /* miniumum number of occurrences
                                       of the same nodes */
    unsigned int   max      = 0;    /* maximum number of occurrences
                                       of the same nodes */
    unsigned int   kind_int = 0;    /* read kind as integer */
    PFguide_kind_t kind     = text; /* kind if the guide node */
    char          *tag_uri  = NULL; /* tag uri of the guide node */
    char          *tag_name = NULL; /* tag name of the guide node */
    char          *attribute_name;

    (void) ctx;                 /* pacify compiler */
    (void) tagname;             /* pacify compiler */

    /* get attributes */
    if ((atts != NULL) && *atts) {
        while (*atts) {
            /* get attribute name */
            attribute_name = (char*)atts[0];

            if (strcmp (attribute_name, "guide") == 0)
                guide = (unsigned int) atoi((char *)atts[1]);

            if (strcmp (attribute_name, "count") == 0)
                count = (unsigned int) atoi((char *)atts[1]);

            if (strcmp (attribute_name, "min") == 0)
                min = (unsigned int) atoi((char *)atts[1]);

            if (strcmp (attribute_name, "max") == 0)
                max = (unsigned int) atoi((char *)atts[1]);

            if (strcmp (attribute_name, "kind") == 0) {
                kind_int = (unsigned int) atoi ((char *) atts[1]);
                switch(kind_int) {
                    case 1: kind = elem; break;
                    case 2: kind = attr; break;
                    case 3: kind = text; break;
                    case 4: kind = comm; break;
                    case 5: kind = pi;   break;
                    case 6: kind = doc;  break;
                    default: assert (0); break;
                }
            }

            if (strcmp (attribute_name, "uri") == 0)
                tag_uri = (char*) atts[1];

            if (strcmp (attribute_name, "name") == 0)
                tag_name = (char*) atts[1];

            atts += 2;
        }
    }

    /* consistency checks */
    switch (kind) {
        case elem:
        case attr:
            if (!tag_uri)
                 PFoops (OOPS_FATAL, "Guide optimization - "
                        "uri of a guide is NULL when it is required\n");
        case doc:
        case pi:
            if (!tag_name)
                 PFoops (OOPS_FATAL, "Guide optimization - "
                        "tag_name of a guide is NULL when it is required\n");
            break;
        default:
            break;
    }

    /* create all other guide nodes */
    new_guide_node = (PFguide_tree_t *) PFmalloc (sizeof (PFguide_tree_t));
    *new_guide_node = (PFguide_tree_t) {
        .guide = guide,
        .count = count,
        .min   = min,
        .max   = max,
        .level = level,
        .kind  = kind,
        .name  = PFqname (PFns_wild, NULL),
        .parent = current_guide_node,
        .child_list = NULL,
    };

    /* copy the strings, otherwise they will be lost */
    if (tag_name != NULL) {
        PFns_t ns;
        char *name_copy;
        name_copy = (char *) PFmalloc (sizeof (char) * (strlen (tag_name) + 1));
        name_copy = strncpy (name_copy, tag_name, strlen (tag_name));

        if (tag_uri) {
            ns.prefix = "";
            char *uri_copy;
            uri_copy = (char *) PFmalloc (sizeof (char) *
                                          (strlen (tag_uri) + 1));
            uri_copy = strncpy (uri_copy, tag_uri, strlen (tag_uri));
            ns.uri = uri_copy;
        }
        else
            ns = PFns_wild;

        new_guide_node->name = PFqname (ns, name_copy);
    }

    /* create association between parent and child */
    if (current_guide_node) {
        new_guide_node->parent = current_guide_node;

        /* insert new guide node in the child list */
        if (!current_guide_node->child_list)
            current_guide_node->child_list =
                PFarray (sizeof (PFguide_tree_t**), 10);

        *(PFguide_tree_t**)
            PFarray_add (current_guide_node->child_list) =
                new_guide_node;
    } else {
        root_guide_node = new_guide_node;
    }

    /* set actual node as current node */
    current_guide_node = new_guide_node;
    /* increase level */
    level++;
}

/* end of a XML element */
static void
end_element (void *ctx, const xmlChar *name)
{
    (void) ctx;
    (void) name;

    /* go a level higher in the tree */
    current_guide_node = current_guide_node->parent;
    /* decrease level */
    level--;
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
        return NULL;
    } else {
        /* it will be used the first item only to create the guide */
        filename = *((char **) PFarray_at (options, 0));
    }

    /* DTD of the guide XML file */
    char *dtd = "<!ELEMENT node (EMPTY | node*)>"
                "<!ATTLIST node guide CDATA #REQUIRED>"
                "<!ATTLIST node count CDATA #REQUIRED>"
                "<!ATTLIST node min CDATA #REQUIRED>"
                "<!ATTLIST node max CDATA #REQUIRED>"
                "<!ATTLIST node kind (1|2|3|4|5|6) #REQUIRED>"
                "<!ATTLIST node uri CDATA #IMPLIED>"
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

    if (!result)
        PFoops (OOPS_FATAL, "Guide optimization - XML input is invalid.\n");

    /* start XML parsing */
    xmlParserCtxtPtr   ctx;

    ctx = xmlCreateFileParserCtxt (filename);
    ctx->sax = &saxhandler;

    /* initialize global variables */
    current_guide_node = NULL;
    root_guide_node    = NULL;
    level              = 0;

    (void) xmlParseDocument (ctx);

    /* Document is not well-formed */
    if (!ctx->wellFormed)
        PFoops (OOPS_FATAL, "Guide optimization - "
                "XML input is not well-formed.\n");

    return root_guide_node;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
