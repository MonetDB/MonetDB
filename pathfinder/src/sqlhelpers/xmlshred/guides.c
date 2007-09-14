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
#include "guides.h"

#include <stdio.h>
#include <assert.h>

guide_tree_t * 
insert_guide_node (const xmlChar *tag_name, guide_tree_t *parent, kind_t kind)
{
    static nat    guide_count    = GUIDE_INIT;
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
            
             if (kind == doc || kind == elem || kind == attr || kind == pi) {
                 assert (guide_node->tag_name);
                 assert (tag_name);        
                 if (xmlStrcmp (guide_node->tag_name, tag_name) != 0)
                     continue;
             }
                
             /* node with identical charactistics found */
             guide_node->count++;    
             guide_node->rel_count++;

             return guide_node;
        }
    }

    /* no matching guide node was found -- create a new one */
    guide_node = (guide_tree_t *) malloc (sizeof (guide_tree_t));
    *guide_node = (guide_tree_t) {
        .tag_name   = xmlStrdup (tag_name),
        .count      = 1,
        .rel_count  = 1,
        .min        = parent ? (parent->rel_count > 1 ? 0 : 1) : 1,
        .max        = 0,
        .parent     = parent,
        .child_list = NULL, 
        .last_child = NULL,
        .guide      = guide_count++,
        .kind       = kind,
    };

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
 * Adjust the minimum and maximum value of all children.
 */
void
adjust_guide_min_max (guide_tree_t *parent)
{
    child_list_t *child_list;
    guide_tree_t *guide = NULL;

    assert (parent);
        
    for (child_list = parent->child_list;
         child_list != NULL;
         child_list = child_list->next_element) {

        guide = child_list->node;
        /* if this is the first min/max assignement and the
           guide node was not missing before (guide->min != 0)
           we can use the relative count as the minimum value */
        if (!guide->max && guide->min)
            guide->min = guide->rel_count;
        else
            /* use the minimum */
            guide->min = guide->min < guide->rel_count
                         ? guide->min : guide->rel_count;

        /* use the maximum */
        guide->max = guide->max > guide->rel_count
                     ? guide->max : guide->rel_count;

        /* reset the relative count */
        guide->rel_count = 0;
    } 
}

#define GUIDE_PADDING_COUNT 2

void 
print_guide_tree (FILE *guide_out, guide_tree_t *guide, int tree_depth)
{
    child_list_t  *child_list, *child_list_free;
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
             guide->guide, guide->count, guide->min, guide->max);
    
    print_kind (guide_out, guide->kind);
    fputc ('\"', guide_out);

    if (guide->tag_name != NULL)
        fprintf (guide_out, " name=\"%s\"", (char*)guide->tag_name);

    if (!guide->child_list) {
        fputc ('/', guide_out);
        print_end_tag = false;
    }

    fprintf (guide_out, ">\n");

    /* invoke printing recursively */
    child_list = guide->child_list;
    while (child_list != NULL) {
       print_guide_tree (guide_out, child_list->node, tree_depth+1);
       child_list_free = child_list;
       child_list = child_list->next_element;
       free (child_list_free);
    }

    /* print the end tag */
    if (print_end_tag) {
        /* print the padding */
        for (i = 0; i < tree_depth * GUIDE_PADDING_COUNT; i++)
            fputc (' ', guide_out);
        fprintf(guide_out, "</node>\n");
    }

    free (guide);
}
