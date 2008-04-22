/**
 * @file
 *
 * Declarations for relational algebra, generic for logical and
 * physical algebra.
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

#ifndef ALGEBRA_H
#define ALGEBRA_H

#include "qname.h"

/* ............... atomic values ............... */

/**
 * Simple atomic types that our algebra knows.
 *
 * Actual attribute types can be any combination of types from here
 * (polymorphism). We represent these polymorphic algebra types with
 * help of a bit-vector.  Each of the bits corresponds to one of the
 * const values here.
 *
 * Observe that the type #aat_node has four bits set, as nodes are split
 * in MonetDB into attribute nodes (#aat_anode) and other nodes (#aat_pnode).
 * Both node kinds require three bits each, to represented the nodes using a
 * node id (#aat_pre), a document fragment (#aat_frag), and based on the
 * node kind either the attribute id (#aat_attr) or a kind identifier
 * (#aat_nkind).
 *
 * @note
 *   The bits for #aat_frag must be lower than the bit
 *   for #aat_pre and the bit for #aat_pre must be lower
 *   than the bit for #aat_attr, respectively. Our sort
 *   implementation will first sort by the frag BAT,
 *   then by the pre BAT, and then by the attribute id
 *   this way. This implements document order across
 *   documents and attributes correctly.
 */
#define aat_nat         0x00000001 /**< algebra simple atomic type natural number */
#define aat_int         0x00000002 /**< algebra simple atomic type integer */
#define aat_str         0x00000004 /**< algebra simple atomic type string  */
#define aat_dec         0x00000008 /**< algebra simple atomic type decimal */
#define aat_dbl         0x00000010 /**< algebra simple atomic type double  */
#define aat_bln         0x00000020 /**< algebra simple atomic type boolean  */
#define aat_uA          0x00000040 /**< algebra simple atomic type untypedAtomic  */

/* qname representation in MIL and SQL */ 
#define aat_qname_id    0x00000100 /**< MIL id column representing a QName */
#define aat_qname_loc   0x00000100 /**< SQL local name column representing a QName */
#define aat_qname_cont  0x00000200 /**< MIL container column representing a QName */
#define aat_qname_uri   0x00000200 /**< SQL uri column representing a QName */
/* qname representation in the algebra */
#define aat_qname       0x00000300 /**< algebra simple atomic type QName  */

/* node representation in MIL and SQL */
#define aat_frag        0x00001000 /**< node fragment */
#define aat_pre         0x00002000 /**< pre value */
#define aat_attr        0x00004000 /**< attr value */
#define aat_nkind       0x00008000 /**< node kind indicating that
                                        a node is not an attribute */
/* attribute node representation in the algebra */
#define aat_anode       0x00007000 /**< algebra simple atomic type attribute */
/* element/document/text/pi/comment node representation in the algebra */
#define aat_pnode       0x0000B000 /**< algebra simple atomic type 
                                        representing all other nodes */
/* node representation in the algebra */
#define aat_node        0x0000F000 /**< algebra simple atomic type node */

#define aat_charseq     0x00100000 /**< this type represents the CHAR type in SQL */
#define aat_update      0x00100000 /**< and it represents the update kind in MIL */
/**
 * The following types represent the first parameter of an update function
 * (which is a always of kind node). This allows an update item to correctly
 * encode all information in separate types: update + node1 + str|qname|node
 * and to transport this (triple) information to the update tape
 * at the serialize operator.
 *
 * @note
 *    The bits encoding the node1 information #aat_frag1, #aat_pre1, #aat_attr1,
 *    and #aat_nkind1 represent the normal node information shifted 4 bits
 *    to the left.
 */
/* node representation in MIL and SQL */
#define aat_frag1       0x00010000 /**< node fragment */
#define aat_pre1        0x00020000 /**< pre value */
#define aat_attr1       0x00040000 /**< attr value */
#define aat_nkind1      0x00080000 /**< node kind indicating that
                                        a node is not an attribute */
/* attribute node representation in the algebra */
#define aat_anode1      0x00070000 /**< algebra simple atomic type attribute */
/* element/document/text/pi/comment node representation in the algebra */
#define aat_pnode1      0x000B0000 /**< algebra simple atomic type 
                                        representing all other nodes */
/* node representation in the algebra */
#define aat_node1       0x000F0000 /**< algebra simple atomic type node */

/**
 * The following types are for the document management functions. We introduce
 * one new type, aat_docmgmt, to signify document management queries, and 3 
 * new types for path, document name and collection name.
 */
#define aat_docmgmt  0x01000000 /**< represents the doc management type */
#define aat_path     0x02000000 /**< the path where a document resides*/
#define aat_docnm    0x04000000 /**< the name of the document */
#define aat_colnm    0x08000000 /**< the name of the collection */

/** Simple atomic types in our algebra */
typedef unsigned int PFalg_simple_type_t;

#define monomorphic(a) ((a) == aat_nat || (a) == aat_int || (a) == aat_str \
                        || (a) == aat_dec || (a) == aat_dbl || (a) == aat_bln \
                        || (a) == aat_qname || (a) == aat_uA \
                        || (a) == aat_anode || (a) == aat_pnode \
                        || ((a) == aat_pre || (a) == aat_frag) \
                        || ((a) == aat_attr || (a) == aat_nkind) \
                        || ((a) == 0))

typedef unsigned int nat;

/** atomic algebra values */
union PFalg_atom_val_t {
    nat           nat_;    /**< value for natural number atoms (#aat_nat) */
    long long int int_;    /**< value for integer atoms (#aat_int) */
    char         *str;     /**< value for string and untyped atoms (#aat_str) */
    double        dec_;     /**< value for decimal atoms (#aat_dec) */
    double        dbl;     /**< value for double atoms (#aat_dbl) */
    bool          bln;     /**< value for boolean atoms (#aat_bln) */
    PFqname_t     qname;
};
/** algebra values */
typedef union PFalg_atom_val_t PFalg_atom_val_t;

/** typed atomic value representation in our algebra */
struct PFalg_atom_t {
    PFalg_simple_type_t type; /**< type of this atom */
    PFalg_atom_val_t    val;  /**< value */
};
/** typed atomic value representation in our algebra */
typedef struct PFalg_atom_t PFalg_atom_t;


/* ............... tuples of atomic values ............... */

/** a tuple is an array of atoms, with length specified in `count' */
struct PFalg_tuple_t {
    unsigned int  count;   /**< number of atoms in this tuple */
    PFalg_atom_t *atoms;   /**< array containing the atoms */
};

typedef struct PFalg_tuple_t PFalg_tuple_t;

/** direction of sorting */
#define DIR_DESC true
#define DIR_ASC false

/* ................ algebra attribute lists ................ */

/** An attribute (name) is represented by constants
        (as these are bit vectors we don't use an enum) */
#define att_NULL    0x00000000    /**< cope with empty partions */
#define att_iter    0x00000001    /**< iter column */
#define att_item    0x00000002    /**< item column */
#define att_pos     0x00000004    /**< pos column */
#define att_iter1   0x00000008    /**< iter1 column */
#define att_item1   0x00000010    /**< item1 column */
#define att_pos1    0x00000020    /**< pos1 column */
#define att_inner   0x00000040    /**< inner column */
#define att_outer   0x00000080    /**< outer column */
#define att_sort    0x00000100    /**< sort column */
#define att_sort1   0x00000200    /**< sort column 1 */
#define att_sort2   0x00000400    /**< sort column 2 */
#define att_sort3   0x00000800    /**< sort column 3 */
#define att_sort4   0x00001000    /**< sort column 4 */
#define att_sort5   0x00002000    /**< sort column 5 */
#define att_sort6   0x00004000    /**< sort column 6 */
#define att_sort7   0x00008000    /**< sort column 7 */
#define att_ord     0x00010000    /**< ord column */
#define att_iter2   0x00020000    /**< iter column 2 */
#define att_iter3   0x00040000    /**< iter column 3 */
#define att_iter4   0x00080000    /**< iter column 4 */
#define att_iter5   0x00100000    /**< iter column 5 */
#define att_iter6   0x00200000    /**< iter column 6 */
#define att_res     0x00400000    /**< res column */
#define att_res1    0x00800000    /**< res1 column */
#define att_cast    0x01000000    /**< cast column */
#define att_item2   0x02000000    /**< item2 column */
#define att_subty   0x04000000    /**< subty column */
#define att_itemty  0x08000000    /**< itemty column */
#define att_notsub  0x10000000    /**< notsub column */
#define att_isint   0x20000000    /**< isint column */
#define att_isdec   0x40000000    /**< isdec column */
#define att_item3   0x80000000    /**< item3 column */

/** attribute names */
typedef unsigned int PFalg_att_t;

/** A list of attributes (actually: attribute names) */
struct PFalg_attlist_t {
    unsigned int count;    /**< number of items in this list */
    PFalg_att_t *atts;     /**< array that holds the actual list items */
};
typedef struct PFalg_attlist_t PFalg_attlist_t;

/* ............. algebra schema specification .............. */

/** An algebra schema item is a (name, type) pair */
struct PFalg_schm_item_t {
    PFalg_att_t         name;
    PFalg_simple_type_t type;
};
typedef struct PFalg_schm_item_t PFalg_schm_item_t;

/** A schema is then a list of schema items */
struct PFalg_schema_t {
    unsigned int               count;  /**< number of items in the list */
    struct PFalg_schm_item_t  *items;  /**< array holding the schema items */
};
typedef struct PFalg_schema_t PFalg_schema_t;


/** item in a projection list, an (new-name,old-name) pair */
struct PFalg_proj_t {
    PFalg_att_t new;   /**< new attribute name to assign */
    PFalg_att_t old;   /**< old attribute name */
};
typedef struct PFalg_proj_t PFalg_proj_t;



/* ....... path step specs (semantic infos of step operators) ....... */

/** location steps */
enum PFalg_axis_t {
      alg_anc          /**< ancestor axis */
    , alg_anc_s        /**< ancestor-or-self axis */
    , alg_attr         /**< attribute axis */
    , alg_chld         /**< child axis */
    , alg_desc         /**< descendant axis */
    , alg_desc_s       /**< descendant-or-self axis */
    , alg_fol          /**< following axis */
    , alg_fol_s        /**< following-sibling axis */
    , alg_par          /**< parent axis */
    , alg_prec         /**< preceding axis */
    , alg_prec_s       /**< preceding-sibling axis */
    , alg_self         /**< self axis */
};
/** location steps */
typedef enum PFalg_axis_t PFalg_axis_t;

enum PFalg_node_kind_t {
      node_kind_elem   /**< elements */
    , node_kind_attr   /**< attributes */
    , node_kind_text   /**< text nodes */
    , node_kind_pi     /**< processing instructions */
    , node_kind_comm   /**< comments */
    , node_kind_doc    /**< document nodes */
    , node_kind_node   /**< any XML tree node */
};
typedef enum PFalg_node_kind_t PFalg_node_kind_t;

struct PFalg_step_spec_t {
    PFalg_axis_t      axis;    /**< represented axis */
    PFalg_node_kind_t kind;    /**< node kind to test for */
    PFqname_t         qname;   /**< for name tests */
};
typedef struct PFalg_step_spec_t PFalg_step_spec_t;

/** function call result occurrence indicator */
enum PFalg_occ_ind_t {
      alg_occ_unknown            /**< unknown result size */
    , alg_occ_zero_or_one        /**< zero or one tuple per iteration */
    , alg_occ_exactly_one        /**< exactly one tuple per iteration */
    , alg_occ_one_or_more        /**< one or more tuples per iteration */
};
typedef enum PFalg_occ_ind_t PFalg_occ_ind_t;

/** function call representatives */
enum PFalg_fun_call_t {
      alg_fun_call_pf_documents            /**< pf:documents               */
    , alg_fun_call_pf_documents_unsafe     /**< pf:documents_unsafe        */
    , alg_fun_call_pf_documents_str        /**< pf:documents_str           */
    , alg_fun_call_pf_documents_str_unsafe /**< pf:documents_str_unsafe    */
    , alg_fun_call_pf_docname              /**< pf:docname                 */
    , alg_fun_call_pf_collections          /**< pf:collections             */
    , alg_fun_call_pf_collections_unsafe   /**< pf:collections_unsafe      */
    , alg_fun_call_pf_collection           /**< pf:collection */
    , alg_fun_call_xrpc                    /**< XRPC function call         */
    , alg_fun_call_xrpc_helpers            /**< func call for XRPC helpers */
    , alg_fun_call_tijah                   /**< Tijah function call        */
};
typedef enum PFalg_fun_call_t PFalg_fun_call_t;

/** function representatives */
enum PFalg_fun_t {
      alg_fun_num_add             /**< arithmetic plus operator */
    , alg_fun_num_subtract        /**< arithmetic minus operator */
    , alg_fun_num_multiply        /**< arithmetic times operator */
    , alg_fun_num_divide          /**< arithmetic divide operator */
    , alg_fun_num_modulo          /**< arithmetic modulo operator */
    , alg_fun_fn_abs              /**< fn:abs */
    , alg_fun_fn_ceiling          /**< fn:ceiling */
    , alg_fun_fn_floor            /**< fn:floor */
    , alg_fun_fn_round            /**< fn:round */
    , alg_fun_fn_concat           /**< fn:concat */
    , alg_fun_fn_substring        /**< fn:substring */
    , alg_fun_fn_substring_dbl    /**< fn:substring with length specified*/
    , alg_fun_fn_string_length    /**< fn:string-length */
    , alg_fun_fn_normalize_space  /**< fn:normalize-space */
    , alg_fun_fn_upper_case       /**< fn:upper-case */
    , alg_fun_fn_lower_case       /**< fn:lower-case */
    , alg_fun_fn_translate        /**< fn:translate */
    , alg_fun_fn_contains         /**< fn:contains */
    , alg_fun_fn_starts_with      /**< fn:starts-with */
    , alg_fun_fn_ends_with        /**< fn:ends-with */
    , alg_fun_fn_substring_before /**< fn:substring-before */
    , alg_fun_fn_substring_after  /**< fn:substring-after */
    , alg_fun_fn_matches          /**< fn:matches */
    , alg_fun_fn_matches_flag     /**< fn:matches with flags */
    , alg_fun_fn_replace          /**< fn:replace */
    , alg_fun_fn_replace_flag     /**< fn:replace with flags */
    , alg_fun_fn_name             /**< fn:name */
    , alg_fun_fn_local_name       /**< fn:local-name */
    , alg_fun_fn_namespace_uri    /**< fn:namespace-uri */
    , alg_fun_fn_number           /**< fn:number */
    , alg_fun_fn_number_lax       /**< fn:number (ignoring NaN) */
    , alg_fun_fn_qname            /**< fn:QName */
    , alg_fun_fn_doc_available    /**< fn:doc-available */
    , alg_fun_pf_fragment         /**< #pf:fragment */
    , alg_fun_pf_supernode        /**< #pf:supernode */
    , alg_fun_pf_add_doc_str      /**< pf:add-doc */
    , alg_fun_pf_add_doc_str_int  /**< pf:add-doc */
    , alg_fun_pf_del_doc          /**< pf:del-doc */
    , alg_fun_upd_rename          /**< upd:rename */
    , alg_fun_upd_delete          /**< upd:delete */
    , alg_fun_upd_insert_into_as_first    /**< upd:insertIntoAsFirst */
    , alg_fun_upd_insert_into_as_last     /**< upd:insertIntoAsLast */
    , alg_fun_upd_insert_before           /**< upd:insertBefore */
    , alg_fun_upd_insert_after            /**< upd:insertAfter */
    , alg_fun_upd_replace_value_att       /**< upd:replaceValue */
    , alg_fun_upd_replace_value           /**< upd:replaceValue */
    , alg_fun_upd_replace_element /**< upd:replaceElementContent */
    , alg_fun_upd_replace_node    /**< upd:replaceNode */
};
typedef enum PFalg_fun_t PFalg_fun_t;

/* ............. document fields specification .............. */

enum PFalg_doc_t {
      doc_atext        /**< attribute content > */
    , doc_text         /**< content of a text node > */
    , doc_comm         /**< content of a comment node > */
    , doc_pi_text      /**< content of a pi node > */
/*    , doc_name   */      /**< name of element node > */
/*    , doc_local  */      /**< local part of an element node name > */
/*    , doc_uri    */      /**< uri part of an element node name > */
};
typedef enum PFalg_doc_t PFalg_doc_t;

/* ............ complex selection specification ............. */

enum PFalg_comp_t {
      alg_comp_eq      /**< == comparison > */
    , alg_comp_gt      /**<  > comparison > */
    , alg_comp_ge      /**< >= comparison > */
    , alg_comp_lt      /**<  < comparison > */
    , alg_comp_le      /**< =< comparison > */
    , alg_comp_ne      /**< != comparison > */
};
typedef enum PFalg_comp_t PFalg_comp_t;

struct PFalg_sel_t {
    PFalg_comp_t comp;  /**< comparison > */
    PFalg_att_t  left;  /**< left selection column > */
    PFalg_att_t  right; /**< right selection column > */
};
typedef struct PFalg_sel_t PFalg_sel_t;

/* ***************** Constructors ******************* */

/** construct literal natural number (atom) */
PFalg_atom_t PFalg_lit_nat (nat value);

/** construct literal integer (atom) */
PFalg_atom_t PFalg_lit_int (long long int value);

/** construct literal string (atom) */
PFalg_atom_t PFalg_lit_str (char *value);

/** construct literal float (atom) */
/* FIXME: Wouter: should be double? */
PFalg_atom_t PFalg_lit_dec (float value);

/** construct literal double (atom) */
PFalg_atom_t PFalg_lit_dbl (double value);

/** construct literal boolean (atom) */
PFalg_atom_t PFalg_lit_bln (bool value);

/** construct literal QName (atom) */
PFalg_atom_t PFalg_lit_qname (PFqname_t value);


/**
 * Construct a literal table tuple, a list of atoms.
 * (actually just a wrapper for #PFalg_tuple_()).
 *
 * Functions with a variable number of arguments need a mechanism to
 * detect the end of the argument list. We therefore wrap the actual
 * worker #PFalg_tuple_() into this macro. The macro detects the
 * number of arguments passed (using arithmetics with sizeof()) and
 * prepends a count information to the actual argument list.
 */
#define PFalg_tuple(...)                                       \
    PFalg_tuple_ ((sizeof ((PFalg_atom_t[]) { __VA_ARGS__ })   \
                      / sizeof (PFalg_atom_t)),                \
                  (PFalg_atom_t[]) { __VA_ARGS__ } )

/** Worker to construct a literal table tuple */
PFalg_tuple_t PFalg_tuple_ (unsigned int count, PFalg_atom_t *atoms);

/**
 * Construct an attribute list (list of attribute names only).
 */
#define PFalg_attlist(...)                                     \
    PFalg_attlist_ ((sizeof ((PFalg_att_t[]) { __VA_ARGS__ })   \
                       / sizeof (PFalg_att_t)),                \
                   (PFalg_att_t[]) { __VA_ARGS__ })
PFalg_attlist_t PFalg_attlist_ (unsigned int count, PFalg_att_t *atts);


/* Returns the original schema without the attributes given in the list */
#define PFalg_schema_diff(schema, ...) \
    PFalg_schema_diff_ (schema, \
            (sizeof ((PFalg_att_t[]) { __VA_ARGS__ })   \
                       / sizeof (PFalg_att_t)), \
            (PFalg_att_t[]) {__VA_ARGS__})
PFalg_schema_t PFalg_schema_diff_(PFalg_schema_t schema,
                        unsigned int count, PFalg_att_t *atts);

/** 
 * Constructor for an empty schema with iter|pos|item and item of type item_t
 */
PFalg_schema_t PFalg_iter_pos_item_schema(PFalg_simple_type_t item_t);

/** 
 * Constructor for an empty schema with iter|item and item of type item_t
 */
PFalg_schema_t PFalg_iter_item_schema(PFalg_simple_type_t item_t);

/** Constructor for projection list item */
PFalg_proj_t PFalg_proj (PFalg_att_t new, PFalg_att_t old);

/**
 * Test if two atomic values are comparable
 */
bool PFalg_atom_comparable (PFalg_atom_t a, PFalg_atom_t b);

/**
 * Compare two atomic values (if possible)
 */
int PFalg_atom_cmp (PFalg_atom_t a, PFalg_atom_t b);

/**
 * Print attribute name
 */
char * PFatt_str (PFalg_att_t att);

/**
 * Checks whether a name is unique or not.
 */
bool PFalg_is_unq_name(PFalg_att_t att);

/**
 * Create a unique name based on an id @a id and
 * an original name @a ori that retains the usage information
 * of the new variable (iter, pos or item).
 */
PFalg_att_t PFalg_unq_name (PFalg_att_t ori, unsigned int id);

/**
 * Create an original column name based on an unique name @a unq
 * and a list of free original variables @a free.
 */
PFalg_att_t PFalg_ori_name (PFalg_att_t unq, PFalg_att_t free);

/**
 * Print XPath axis
 */
char * PFalg_axis_str (PFalg_axis_t axis);

/**
 * Print node kind
 */
char * PFalg_node_kind_str (PFalg_node_kind_t kind);

/**
 * Print simple type name
 */
char * PFalg_simple_type_str (PFalg_simple_type_t att);

/**
 * Print function call kind
 */
char * PFalg_fun_call_kind_str (PFalg_fun_call_t kind);

/**
 * Print function name
 */
char * PFalg_fun_str (PFalg_fun_t fun);

/**
 * Construct a predicate.
 */
PFalg_sel_t PFalg_sel (PFalg_comp_t comp,
                       PFalg_att_t left,
                       PFalg_att_t right);

#ifdef HAVE_PFTIJAH

/*
 * PFTIJAH defines used by the pftijah funcall interface. In the future
 * the parameters will be implemented using their private context structure
 * instead of decoding info in the function name.
 */

#define PFTIJAH_NODEKIND  aat_pnode
#define DOCMGMTTYPE aat_docmgmt

#define PFT_FUN(F)              (strncmp(F,"pftijah_",8)==0)

#define PFT_QUERY_N_XX "pftijah_query_n_xx"
#define PFT_QUERY_N_SX "pftijah_query_n_sx"
#define PFT_QUERY_N_XO "pftijah_query_n_xo"
#define PFT_QUERY_N_SO "pftijah_query_n_so"
#define PFT_QUERY_I_XX "pftijah_query_i_xx"
#define PFT_QUERY_I_SX "pftijah_query_i_sx"
#define PFT_QUERY_I_XO "pftijah_query_i_xo"
#define PFT_QUERY_I_SO "pftijah_query_i_so"

#define PFT_FUN_QUERY(F)        (strncmp(F,"pftijah_query_",14)==0)

#define PTF_QUERY_NODES(N)      (N[14]=='n')
#define PTF_QUERY_STARTNODES(N) (N[16]=='s')
#define PTF_QUERY_OPTIONS(N)    (N[17]=='o')


#define PFT_MANAGE_FTI_C_XX "pftijah_manage_fti_c_xx"
#define PFT_MANAGE_FTI_C_CX "pftijah_manage_fti_c_cx"
#define PFT_MANAGE_FTI_C_XO "pftijah_manage_fti_c_xo"
#define PFT_MANAGE_FTI_C_CO "pftijah_manage_fti_c_co"
#define PFT_MANAGE_FTI_E_CX "pftijah_manage_fti_e_cx"
#define PFT_MANAGE_FTI_E_CO "pftijah_manage_fti_e_co"
#define PFT_MANAGE_FTI_R_XX "pftijah_manage_fti_r_xx"
#define PFT_MANAGE_FTI_R_XO "pftijah_manage_fti_r_xo"

#define PFT_FUN_MANAGE(F)        (strncmp(F,"pftijah_manage_",15)==0)

#define PFT_FUN_MANAGE_KIND(F)   (F[19])
#define PFT_FUN_MANAGE_COLL(F)   (F[21] == 'c')
#define PFT_FUN_MANAGE_OPT(F)    (F[22] == 'o')

#define PFT_SCORE      "pftijah_score"
#define PFT_NODES      "pftijah_nodes"
#define PFT_INFO       "pftijah_info"
#define PFT_TOKENIZE   "pftijah_tokenize"
#define PFT_RESSIZE    "pftijah_ressize"

#endif /* HAVE_PFTIJAH */

#endif  /* ALGEBRA_H */

/* vim:set shiftwidth=4 expandtab: */
