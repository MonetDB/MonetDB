#ifndef GUIDES_H__
#define GUIDES_H__

#include "shred_helper.h"
#include "encoding.h"
#include <stdio.h>

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"

#define GUIDE_PADDING_COUNT 4
#define GUIDE_INIT 1

typedef struct guide_tree_t guide_tree_t;

typedef struct child_list_t child_list_t;
struct child_list_t {
    child_list_t    *next_element;
    guide_tree_t    *node;
};

struct guide_tree_t {
    xmlChar        * tag_name;
    nat              count;
    guide_tree_t   * parent;
    child_list_t   * child_list;
    child_list_t   * last_child;
    nat              guide;
    kind_t           kind;
};

/* current guide node in the guide tree */
extern guide_tree_t * current_guide_node;
extern guide_tree_t * leaf_guide_node;

/* add a guide child to the parent */
void add_guide_child(guide_tree_t *parent, guide_tree_t *child);

/* insert a node in the guide tree */
guide_tree_t* insert_guide_node(const xmlChar *tag_name, guide_tree_t 
    *parent, kind_t kind);

/* print the guide tree */
void print_guide_tree(FILE *guide_out, guide_tree_t *root, int tree_depth);

#endif /* GUIDES_H__ */
