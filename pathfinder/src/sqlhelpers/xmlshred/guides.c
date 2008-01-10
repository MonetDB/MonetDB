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
#include <assert.h>
#include <limits.h>

#include "pf_config.h"
#include "guides.h"

/**
 * Insert an XML node into the guide tree 
 */
guide_tree_t * 
insert_guide_node (const xmlChar *URI, const xmlChar *localname, 
                   guide_tree_t *parent, kind_t kind)
{
    static nat    guide_count    = 1;
    child_list_t *child_list     = NULL;
    guide_tree_t *guide_node     = NULL;

    /* try to find a matching guide node */
    if (parent) {
        /* search all children to find a node with 
           identical characteristics */
        for (child_list = parent->child_list;
             child_list != NULL;
             child_list = child_list->next_element) {
             guide_node = child_list->node;
             assert (guide_node);
             
             /* identical characteristics:
                (1) same kind
                (2) same tag name (if applicable) */
             if (guide_node->kind != kind)      
                 continue;
             
             switch (kind) {
             case elem:
             case attr:
                assert (guide_node->uri);
                assert (URI);
                if (xmlStrcmp (guide_node->uri, URI) != 0)
                    continue; 
                /* fall through */    
             case doc:
             case pi:
                assert (guide_node->localname);
                assert (localname);
                if (xmlStrcmp (guide_node->localname, localname) != 0)
                    continue;
             default:
                ;
             }
                            
             /* node with identical charactistics found */
             guide_node->count++;    
             guide_node->occur++;

             return guide_node;
        }
    }

    /* no matching guide node was found -- create a new one */
    guide_node = (guide_tree_t *) malloc (sizeof (guide_tree_t));

    *guide_node = (guide_tree_t) {
        .uri        = xmlStrdup (URI)
      , .localname  = xmlStrdup (localname)
      , .count      = 1
      , .occur      = 1
      , .parent     = parent
      , .child_list = NULL 
      , .last_child = NULL
      , .guide      = guide_count++
      , .kind       = kind
    };

    if (parent) {
        /* we know nothing about minimum/maximum occurrences yet */
        guide_node->min_occur = INT_MAX;
        guide_node->max_occur = INT_MIN;
    } else {
        /* minimum/maximum occurence for the document node always is 1/1
           (this will not be updated further) */
        guide_node->min_occur = 1;
        guide_node->max_occur = 1;
    }

    /* associate child with the parent */
    if (parent) {
        child_list = (child_list_t *) malloc (sizeof (child_list_t));
        *child_list = (child_list_t) { .next_element = NULL,
                                       .node = guide_node };

        if (parent->last_child)
            parent->last_child->next_element = child_list;
        else
            parent->child_list = child_list;

        parent->last_child = child_list;
    }
    
    return guide_node;
}

/**
 * Adjust minimum and maximum occurrences of all child nodes
 * (called once we've seen all children of the node associated
 * with this guide, i.e., in end_element and end_document)
 */
void
guide_occurrence (guide_tree_t *parent)
{
    child_list_t *child_list;
    guide_tree_t *guide = NULL;

    assert (parent);
        
    for (child_list = parent->child_list;
         child_list != NULL;
         child_list = child_list->next_element) {

        guide = child_list->node; 
        
        assert (guide);
        
        guide->min_occur = MIN(guide->min_occur, guide->occur);
        guide->max_occur = MAX(guide->max_occur, guide->occur);
        
        guide->occur = 0;
    } 
}

#define GUIDE_PADDING_COUNT 2

/**
 * Serialize the guide tree in an XML format
 */
void 
print_guide_tree (FILE *guide_out, guide_tree_t *guide, int tree_depth)
{
    child_list_t  *child_list;
    bool           print_end_tag = true;
    int            i;

    assert (guide);
    assert (guide_out);
    
    /* print the padding */
    for (i = 0; i < tree_depth * GUIDE_PADDING_COUNT; i++)
        fputc (' ', guide_out);

    /* print the node self */
    fprintf (guide_out, 
             "<node guide=\"" SSZFMT "\" count=\"" SSZFMT "\""
             " min=\"" SSZFMT "\" max=\"" SSZFMT "\" kind=\"",
             guide->guide, guide->count, 
             guide->min_occur, guide->max_occur);
    
    print_kind (guide_out, guide->kind);
    fputc ('\"', guide_out);

    if (guide->uri)
        fprintf (guide_out, " uri=\"%s\"", (char *) guide->uri);

    if (guide->localname)
        fprintf (guide_out, " name=\"%s\"", (char *) guide->localname);

    if (!guide->child_list) {
        fputc ('/', guide_out);
        print_end_tag = false;
    }

    fprintf (guide_out, ">\n");

    /* invoke printing recursively */
    child_list = guide->child_list;
    while (child_list != NULL) {
       print_guide_tree (guide_out, child_list->node, tree_depth+1);
       child_list = child_list->next_element;
    }

    /* print the end tag */
    if (print_end_tag) {
        /* print the padding */
        for (i = 0; i < tree_depth * GUIDE_PADDING_COUNT; i++)
            fputc (' ', guide_out);
        fprintf(guide_out, "</node>\n");
    }
}

/**
 * Free the memory allocated for the guide tree
 */
void 
free_guide_tree (guide_tree_t *guide)
{
    child_list_t  *child_list, *child_list_free;
    
    assert (guide);
    
    /* recursively free the memory allocated
       for the children */
    child_list = guide->child_list;
    while (child_list) {
       free_guide_tree (child_list->node);
       child_list_free = child_list;
       child_list = child_list->next_element;
       /* free child list item */
       free (child_list_free);
    }

    /* free the copied URI and localname */
    if (guide->uri) 
        xmlFree (guide->uri);
    if (guide->localname) 
        xmlFree (guide->localname);
    /* free the current guide node */
    free (guide);
}
