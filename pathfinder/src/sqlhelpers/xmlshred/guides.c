#include "pf_config.h"
#include <stdio.h>
#include <assert.h>

#include "guides.h"
#include "encoding.h"
#include <stdio.h>
#include <assert.h>

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"

/* current guide count */
nat guide_count = GUIDE_INIT;

void 
add_guide_child(guide_tree_t *parent, guide_tree_t *child)
{
     if((parent == NULL) || (child == NULL))
         return;

     /* insert new child in the list */
     child_list_t *new_child_list = (child_list_t*)malloc(sizeof(child_list_t));
     *new_child_list = (child_list_t){
         .next_element = NULL,
         .node  = child,
     };

     if(parent->child_list == NULL) {
         parent->child_list = new_child_list;
     } else {
         parent->last_child->next_element = new_child_list;
     }

     parent->last_child = new_child_list;
     child->parent = parent;

     return;
}

guide_tree_t* 
insert_guide_node(const xmlChar *tag_name, guide_tree_t *parent, kind_t kind)
{
    child_list_t *child_list = NULL;
    guide_tree_t *child_node = NULL;
    guide_tree_t *new_guide_node = NULL;


#define HAS_TAG(k) \
  (((k) == doc) || ((k) == elem) || ((k) == attr) || ((k) == pi))
  
    if (parent != NULL) {
      /* search all children to find a node with 
         identical characteristics */
      for (child_list = parent->child_list;
           child_list != NULL;
           child_list = child_list->next_element) {
        child_node = child_list->node;

        assert (child_node);
        
        /* identical characteristics:
           (1) same kind
           (2) same tag name (if applicable) */
        if (child_node->kind != kind)      
          continue;

        if (HAS_TAG (kind)) {
          assert (child_node->tag_name);
          assert (tag_name);        
          if (xmlStrcmp (child_node->tag_name, tag_name) != 0)
            continue;
        } 
           
        /* node with identical charactistics found */
        child_node->count++;    
        child_node->rel_count++;
        return child_node;
      }
    }

    /* create a new guide node */
    new_guide_node = (guide_tree_t*)malloc(sizeof(guide_tree_t));
    *new_guide_node = (guide_tree_t) {
        .tag_name = xmlStrdup (tag_name),
        .count = 1,
        .rel_count = 1,
        .min       = parent ? (parent->rel_count > 1 ? 0 : 1) : 1,
        .max       = 0,
        .parent = parent,
        .child_list = NULL, 
        .last_child = NULL,
        .guide = guide_count,
        .kind = kind,
    };
    /* increment the guide count */ 
    guide_count++;

    /* associate child with the parent */
    add_guide_child(parent, new_guide_node);
    
    return new_guide_node;
}

void
adjust_guide_min_max (guide_tree_t *guide)
{
    if (guide) {
        /* Adjust the minimum and maximum value of all children */
        child_list_t *child_list = guide->child_list;
        guide_tree_t *node = NULL;
            
        while(child_list != NULL) {
            node = child_list->node;
            /* if this is the first min/max assignement and the
               node was not missing before (node->min != 0)
               we can use the relative count as the minimum value */
            if (!node->max && node->min)
                node->min = node->rel_count;
            else
                /* use the minimum */
                node->min = node->min < node->rel_count
                            ? node->min : node->rel_count;
            /* use the maximum */
            node->max = node->max>node->rel_count ? node->max:node->rel_count;
            /* reset the relative count */
            node->rel_count = 0;
            child_list = child_list->next_element;
        } 
    }
}

void 
print_guide_tree(FILE *guide_out, guide_tree_t *root, int tree_depth)
{
    child_list_t  *child_list = root->child_list;
    child_list_t  *child_list_free = NULL;
    bool           print_end_tag = true;
    int            i;

    assert (guide_out);

    /* print the padding */
    for(i = 0; i < tree_depth * GUIDE_PADDING_COUNT; i++)
      fprintf(guide_out, " ");

    print_end_tag = child_list == NULL ? false : true;

    /* print the node self */
    fprintf (
        guide_out, 
        "<node guide=\"" SSZFMT "\" count=\"" SSZFMT "\""
             " min=\"" SSZFMT "\" max=\"" SSZFMT "\" kind=\"",
        root->guide, root->count, root->min, root->max);
    print_kind (guide_out, root->kind);
    fprintf (guide_out, "\"");
    if (root->tag_name != NULL)
        fprintf (guide_out, " name=\"%s\"", root->tag_name);
    fprintf (guide_out, "%s>\n", print_end_tag ? "" : "/"); 

    while(child_list != NULL) {
       print_guide_tree(guide_out, child_list->node, tree_depth+1);
       child_list_free = child_list;
       child_list = child_list->next_element;
       if(child_list_free != NULL)
           free(child_list_free);
    }

    /* print the end tag */
    if (print_end_tag) {
        for(i = 0; i < tree_depth * GUIDE_PADDING_COUNT; i++)
            fprintf(guide_out, " ");

        fprintf(guide_out, "</node>\n");
    }

    free(root);
    root = NULL;
}
