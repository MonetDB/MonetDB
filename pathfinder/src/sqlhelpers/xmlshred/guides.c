#include "guides.h"
#include "encoding.h"
#include <stdio.h>

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"


/* current guide count */
nat guide_count = GUIDE_INIT;

/** current guide node in the guide tree */
guide_tree_t *current_guide_node = NULL;
/** current leaf in guide_tree */
guide_tree_t *leaf_guide_node = NULL;

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


    if (parent != NULL) {
        /* Search all children and check if the node already exist */
        child_list = parent->child_list;
            
        while(child_list != NULL) {
            child_node = child_list->node;

            #define TAG(k) \
                  (((k) == (doc)) || ((k) == (elem)) \
                   || ((k) == (pi)) || ((k) == (attr))) 

            if (((!TAG(kind)) && child_node->kind==kind) ||
                 (TAG(kind) && (child_node->kind == kind) &&
                  xmlStrcmp (child_node->tag_name, tag_name)==0)) {
                child_node->count = child_node->count + 1;
                return child_node;
            }
            child_list = child_list->next_element;
        } 
    }

    /* create a new guide node */
    new_guide_node = (guide_tree_t*)malloc(sizeof(guide_tree_t));
    *new_guide_node = (guide_tree_t) {
        .tag_name = xmlStrdup (tag_name),
        .count = 1,
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
print_guide_tree(FILE *guide_out, guide_tree_t *root, int tree_depth)
{
    child_list_t  *child_list = root->child_list;
    child_list_t  *child_list_free = NULL;
    bool           print_end_tag = true;
    int            i;

    /* print the padding */
    for(i = 0; i < tree_depth * GUIDE_PADDING_COUNT; i++)
      fprintf(guide_out, " ");

    print_end_tag = child_list == NULL ? false : true;

    /* print the node self */
    fprintf (
        guide_out, 
        "<node guide=\"" SSZFMT "\" count=\"" SSZFMT "\" kind=\"",
        root->guide,
        root->count);
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
