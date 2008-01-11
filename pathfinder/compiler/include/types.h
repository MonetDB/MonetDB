/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder's implementation of the XQuery static type system.
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
 *
 *
 *
 * A diagram of Pathfinder's internal type system (`qn' denotes QName,
 * possibly using wildcards `ns:*', `*:loc').  
 *
 * @verbatim
                                         +- untypedAtomic          
                                         |
                                         |           +- decimal -- integer
                                         |           |
                                         +- numeric -+
                                         |           |
                                         |           +- double
                                         |
                              +- atomic -+- boolean
                              |          |
                              |          +- string
                              |          |
                              |          +- QName
                              |          
                              |           
                              |            +- comm
                              |            |
       +- untypedAny          |            +- p-i
       |                      |            |
       +- item ---------------+            +- text
       |                      |            |
       +- t&t                 +- node -----+- doc { t }
       |                                   |
       +- t|t                              +- attr * { t }/attr qn { t }
       |                                   |
       +- t,t                              +- elem * { t }/elem qn { t }
       | 
   t --+- t*
       |
       +- t+
       |
       +- t?
       |
       +- empty/() (epsilon)
       |
       +- none (empty set)
       |
       +- named qn (reference to named type)
       |
       +- stmt
@endverbatim
 *
 * 
 * Notes: 
 *
 *  - Below `atomic' we can attach the complete XML Schema
 *    (Part 2, Datatypes) builtin type hierarchy, if this is desired.
 *
 *  - The `all group' operator & is n-ary.
 *
 *  - Mapping from XQuery SequenceTypes to Pathfinder internal type system:
 *
 * @verbatim
       [[ t* ]]                       = [[ t ]]*
       [[ t+ ]]                       = [[ t ]]+
       [[ t? ]]                       = [[ t ]]?
       [[ xs:integer ]]               = integer
       [[ xs:string ]]                = string
                                     ...
       [[ xs:anyType ]]               = item*
       [[ xs:anyItem ]]               = item
       [[ xs:anyNode ]]               = node
       [[ xs:anySimpleType ]]         = numeric | string | boolean
       [[ xs:anyElement ]]            = elem * { [[ xs:anyType ]] }
       [[ xs:anyAttribute ]]          = attr * { atomic* }
       [[ xdt:untypedAtomic ]]        = untypedAtomic
       [[ xdt:untypedAny ]]           = untypedAny
       [[ qn ]]                       = [[ named qn ]]
       [[ empty ]]                    = ()
       [[ node ]]                     = node
       [[ processing-instruction ]]   = p-i
       [[ comment ]]                  = comm
       [[ text ]]                     = text
       [[ document ]]                 = doc { elem { item* }* }
       [[ item ]]                     = item
       [[ element ]]                  = elem * { item* }
       [[ attribute ]]                = attr * { atomic }
       [[ element qn ]]               = elem qn { item* }
       [[ attribute qn ]]             = attr qn { atomic }
       [[ element of type qn ]]       = elem * { named qn }
       [[ attribute of type qn ]]     = attr * { named qn }
       [[ element qn of type qn' ]]   = elem qn { named qn' }
       [[ attribute qn of type qn' ]] = attr qn { named qn' }
@endverbatim
 *    
 *  - unsupported (SchemaContext):
 *
 *      - element qn context qn'/qn''/...  
 *      - attribute qn context qn'/qn''/... 
 *     
 *  - The type `stmt' is a Pathfinder specific extension to accomodate
 *    for our update language.  Any (update) function with side effects
 *    has type `stmt'.  An input query is allowed to either have a
 *    subtype of stmt* (i.e., be a pure update statement), or be disjoint
 *    from stmt (i.e., be a side effect free query).  With this
 *    restriction we can avoid semantical problems that might occur
 *    if an input contains a mix of query and update.
 *    
 *  - Similar things also hold for Pathfinder's `docmgmt' type.  This
 *    type is assigned to (built-in) functions that modify the document
 *    collection, i.e., add or remove documents from the collection.
 *    Such statements may neither be mixed with update statements of
 *    type `stmt' (which update subtrees *within* an item in the
 *    document collection), nor with regular values.
 */


#ifndef TYPES_H
#define TYPES_H

#include "qname.h"
#include "env.h"

/**
 * Encoding of internal types and type constructors.  
 */
enum PFtytype_t {
  /* ty_none is not part of the XQuery type system per se, but
   * is introduced to facilitate type-checking and subtyping
   */
  ty_none,                     /**< none (empty set)            */
  ty_empty,                    /**< empty (epsilon)             */
  ty_named,                    /**< reference to named type     */
  ty_opt,                      /**< t?                          */
  ty_plus,                     /**< t+                          */
  ty_star,                     /**< t*                          */
  ty_seq,                      /**< t,t                         */
  ty_choice,                   /**< t|t                         */
  ty_all,                      /**< t&t                         */
  ty_item,                     /**< item                        */
  ty_untypedAny,               /**< untypedAny                  */
  ty_atomic,                   /**< atomic                      */
  ty_untypedAtomic,            /**< untypedAtomic               */
  ty_numeric,                  /**< numeric                     */
  ty_integer,                  /**< integer                     */
  ty_decimal,                  /**< decimal                     */
  ty_double,                   /**< double                      */
  ty_string,                   /**< string                      */
  ty_boolean,                  /**< boolean                     */
  ty_qname,                    /**< QName                       */
  ty_node,                     /**< node                        */
  ty_elem,                     /**< elem                        */
  ty_attr,                     /**< attr                        */
  ty_doc,                      /**< doc                         */
  ty_text,                     /**< text                        */
  ty_pi,                       /**< pi                          */
  ty_comm,                     /**< comm                        */
  ty_stmt,                     /**< update statement            */
  ty_docmgmt,                  /**< document management         */

  ty_types                     /**< # of types                  */
};

typedef enum PFtytype_t PFtytype_t;

/**
 * Maximum number of types a type constructor can take.
 */
#define PFTY_MAXCHILD 2

/** 
 * Representation of internal types.
 */
typedef struct PFty_t PFty_t;

struct PFty_t {
  PFtytype_t  type;                  /**< encoded type (constructor) */
  PFqname_t   name;                  /**< name of a named type (ty_named),
                                          or element/attribute name
                                          (ty_elem/ty_attr), if this is a
                                          wildcard (elem * { t }/attr * { t }),
                                          ensure name.loc == 0 */
  PFenv_t     *sym_space;            /**< symbol space of named type */
  PFty_t      *child[PFTY_MAXCHILD]; /**< type constructor arguments */
};

#define PFty_wildcard(ty) (((ty).type == ty_elem            \
                            || (ty).type == ty_attr         \
                            || (ty).type == ty_pi) &&       \
                           PFQNAME_WILDCARD ((ty).name))

/**
 * Types (internal).
 */
PFty_t PFty_none (void);
PFty_t PFty_empty (void);
PFty_t PFty_item (void);
PFty_t PFty_untypedAny (void);
PFty_t PFty_atomic (void);
PFty_t PFty_untypedAtomic (void);
PFty_t PFty_numeric (void);
PFty_t PFty_node (void);
PFty_t PFty_text (void);
PFty_t PFty_pi (char *);
PFty_t PFty_comm (void);
PFty_t PFty_stmt (void);
PFty_t PFty_docmgmt (void);

/** 
 * Type constructors (internal).
 */
PFty_t PFty_one (PFty_t t);
PFty_t PFty_opt (PFty_t t);
PFty_t PFty_plus (PFty_t t);
PFty_t PFty_star (PFty_t t);
PFty_t PFty_seq (PFty_t t1, PFty_t t2);
PFty_t PFty_choice (PFty_t t1, PFty_t t2);
PFty_t PFty_all (PFty_t t1, PFty_t t2);

PFty_t PFty_elem (PFqname_t qn, PFty_t t);
PFty_t PFty_attr (PFqname_t qn, PFty_t t);
PFty_t PFty_doc (PFty_t t);

PFty_t PFty_named (PFqname_t qn);
PFty_t PFty_named_elem (PFqname_t qn);
PFty_t PFty_named_attr (PFqname_t qn);
PFty_t PFty_named_group (PFqname_t qn);
PFty_t PFty_named_attrgroup (PFqname_t qn);

/**
 * XML Schema (Part 2, Datatypes) types.  
 */
PFty_t PFty_xs_integer (void);
PFty_t PFty_xs_string (void);
PFty_t PFty_xs_boolean (void);
PFty_t PFty_xs_decimal (void);
PFty_t PFty_xs_double (void);
PFty_t PFty_xs_QName (void);

PFty_t PFty_xs_anyType (void);
PFty_t PFty_xs_anyItem (void);
PFty_t PFty_xs_anyNode (void);
PFty_t PFty_xs_anySimpleType (void);
PFty_t PFty_xs_anyElement (void);
PFty_t PFty_xs_anyAttribute (void);

/**
 * XPath data types
 */
PFty_t PFty_xdt_untypedAtomic (void);
PFty_t PFty_xdt_untypedAny (void);

/**
 * The XML Schema symbol spaces.
 */
extern PFenv_t *PFtype_defns;
extern PFenv_t *PFelem_decls;
extern PFenv_t *PFattr_decls;
extern PFenv_t *PFgroup_defns;
extern PFenv_t *PFattrgroup_defns;


void PFty_import (PFty_t, PFty_t);
PFty_t *PFty_schema (PFty_t);
void PFty_predefined (void);

/**
 * Unfold all named type references in a (recursive) type.
 */
PFty_t PFty_defn (PFty_t);

/**
 * Return QName of an element or attribute type (`element qn { type }').
 */
PFqname_t PFty_name (PFty_t t);

/**
 * Return the child of a #PFty_t type with exactly one child.
 */
PFty_t PFty_child (PFty_t t);

/**
 * Return the left child of a #PFty_t type.
 */
PFty_t PFty_lchild (PFty_t t);

/**
 * Return the right child of a #PFty_t type.
 */
PFty_t PFty_rchild (PFty_t t);

/**
 * String representation of given type.
 */
char *PFty_str (PFty_t t);

#endif

/* vim:set shiftwidth=4 expandtab: */
