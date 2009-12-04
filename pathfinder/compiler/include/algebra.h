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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2009 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
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
 * Actual column types can be any combination of types from here
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
#define aat_qname_id    0x00000080 /**< MIL id column representing a QName */
#define aat_qname_loc   0x00000080 /**< SQL local name column representing a QName */
#define aat_qname_cont  0x00000100 /**< MIL container column representing a QName */
#define aat_qname_uri   0x00000100 /**< SQL uri column representing a QName */
/* qname representation in the algebra */
#define aat_qname       0x00000180 /**< algebra simple atomic type QName  */

#ifdef HAVE_GEOXML
/* geo xml type */
#define aat_wkb         0x00000200 /**< algebra simple atomic type wkb  */
#endif

/* date time types */
#define aat_dtime       0x00000400 /**< dateTime                    */
#define aat_date        0x00000800 /**< date                        */
#define aat_time        0x00001000 /**< time                        */
#define aat_gymonth     0x00002000 /**< gYearMonth                  */
#define aat_gyear       0x00004000 /**< gYear                       */
#define aat_gmday       0x00008000 /**< gMonthDay                   */
#define aat_gmonth      0x00010000 /**< gMonth                      */
#define aat_gday        0x00020000 /**< gDay                        */
#define aat_duration    0x00040000 /**< duration                    */
#define aat_ymduration  0x00080000 /**< yearMonthDuration           */
#define aat_dtduration  0x01000000 /**< dayTimeDuration             */

#define aat_unused13    0x02000000
#define aat_unused14    0x04000000
#define aat_unused15    0x08000000

/* node representation in MIL and SQL */
#define aat_frag        0x00100000 /**< node fragment */
#define aat_pre         0x00200000 /**< pre value */
#define aat_attr        0x00400000 /**< attr value */
#define aat_nkind       0x00800000 /**< node kind indicating that
                                        a node is not an attribute */
/* attribute node representation in the algebra */
#define aat_anode       0x00700000 /**< algebra simple atomic type attribute */
/* element/document/text/pi/comment node representation in the algebra */
#define aat_pnode       0x00B00000 /**< algebra simple atomic type 
                                        representing all other nodes */
/* node representation in the algebra */
#define aat_node        0x00F00000 /**< algebra simple atomic type node */

#define aat_error       0x10000000 /**< this type represents an error */
#define aat_charseq     0x20000000 /**< this type represents the CHAR type in SQL */

/* Indicators for the return type: These two type bits
   provide the overloading functionality of the types. */
#define aat_update      0x40000000 /**< and it represents the update kind in MIL */
#define aat_docmgmt     0x80000000 /**< represents the doc management type */

/**
 * The following types are for the update functions. The following four types
 * represent the first parameter of an update function (which is a always of
 * kind node). This allows an update item to correctly encode all information
 * in separate types: update + node1 + str|qname|node * and to transport this
 * (triple) information to the update tape at the serialize operator.
 *
 * @note
 *    The bits encoding the node1 information #aat_frag1, #aat_pre1, #aat_attr1,
 *    and #aat_nkind1 represent the normal node information shifted 4 bits
 *    to the left.
 *
 * @note
 *    The update types reuse the type bits of the normal (query) types for
 *    nodes, strings, and QNames.
 *
 * @note
 *    Update types may not be mixed with with query (here atomic and node types).
 *    Therefore normal querying can overload the following bit ranges.
 */
/* node representation in MIL and SQL */
#define aat_frag1       0x01000000 /**< node fragment */
#define aat_pre1        0x02000000 /**< pre value */
#define aat_attr1       0x04000000 /**< attr value */
#define aat_nkind1      0x08000000 /**< node kind indicating that
                                        a node is not an attribute */
/* attribute node representation in the algebra */
#define aat_anode1      0x07000000 /**< algebra simple atomic type attribute */
/* element/document/text/pi/comment node representation in the algebra */
#define aat_pnode1      0x0B000000 /**< algebra simple atomic type 
                                        representing all other nodes */
/* node representation in the algebra */
#define aat_node1       0x0F000000 /**< algebra simple atomic type node */

/**
 * The following types are for the document management functions. We introduce
 * one new type, aat_docmgmt, to signify document management queries, and 3 
 * new types for path, document name and collection name. As atomic types and
 * document management types may not be mixed we can overload the lower bits.
 */
#define aat_path        0x00000001 /**< the path where a document resides*/
#define aat_docnm       0x00000002 /**< the name of the document */
#define aat_colnm       0x00000004 /**< the name of the collection */

/** Simple atomic types in our algebra */
typedef unsigned int PFalg_simple_type_t;

#define monomorphic(a) ((a) == aat_nat || (a) == aat_int || (a) == aat_str \
                        || (a) == aat_dec || (a) == aat_dbl || (a) == aat_bln \
                        || (a) == aat_qname || (a) == aat_uA \
                        || (a) == aat_anode || (a) == aat_pnode \
                        || (a) == aat_dtime || (a) == aat_date \
                        || (a) == aat_time || (a) == aat_gymonth \
                        || (a) == aat_gyear || (a) == aat_gmday \
                        || (a) == aat_gmonth || (a) == aat_gday \
                        || (a) == aat_duration || (a) == aat_ymduration \
                        || (a) == aat_dtduration \
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

/* ................ algebra column lists ................ */

/** An column (name) is represented by constants
        (as these are bit vectors we don't use an enum) */
#define col_NULL    0x00000000    /**< cope with empty partions */
#define col_iter    0x00000001    /**< iter column */
#define col_item    0x00000002    /**< item column */
#define col_pos     0x00000004    /**< pos column */
#define col_iter1   0x00000008    /**< iter1 column */
#define col_item1   0x00000010    /**< item1 column */
#define col_pos1    0x00000020    /**< pos1 column */
#define col_inner   0x00000040    /**< inner column */
#define col_outer   0x00000080    /**< outer column */
#define col_sort    0x00000100    /**< sort column */
#define col_sort1   0x00000200    /**< sort column 1 */
#define col_sort2   0x00000400    /**< sort column 2 */
#define col_sort3   0x00000800    /**< sort column 3 */
#define col_sort4   0x00001000    /**< sort column 4 */
#define col_sort5   0x00002000    /**< sort column 5 */
#define col_sort6   0x00004000    /**< sort column 6 */
#define col_sort7   0x00008000    /**< sort column 7 */
#define col_ord     0x00010000    /**< ord column */
#define col_iter2   0x00020000    /**< iter column 2 */
#define col_iter3   0x00040000    /**< iter column 3 */
#define col_iter4   0x00080000    /**< iter column 4 */
#define col_iter5   0x00100000    /**< iter column 5 */
#define col_iter6   0x00200000    /**< iter column 6 */
#define col_res     0x00400000    /**< res column */
#define col_res1    0x00800000    /**< res1 column */
#define col_cast    0x01000000    /**< cast column */
#define col_item2   0x02000000    /**< item2 column */
#define col_subty   0x04000000    /**< subty column */
#define col_itemty  0x08000000    /**< itemty column */
#define col_notsub  0x10000000    /**< notsub column */
#define col_item3   0x20000000    /**< item3 column */
#define col_score1  0x40000000    /**< score1 column */
#define col_score2  0x80000000    /**< score2 column */

/** column names */
typedef unsigned int PFalg_col_t;

/** A list of columns (actually: column names) */
#define PFalg_collist_t               PFarray_t
/** Constructor for a column list */
#define PFalg_collist(size)           PFarray (sizeof (PFalg_col_t), (size))
#define PFalg_collist_copy(cl)        PFarray_copy ((cl))
/** Positional access to a column list */
#define PFalg_collist_at(cl,i)        *(PFalg_col_t *) PFarray_at ((cl), (i))
#define PFalg_collist_top(cl)         *(PFalg_col_t *) PFarray_top ((cl))
/** Append to a column list */
#define PFalg_collist_add(cl)         *(PFalg_col_t *) PFarray_add ((cl))
#define PFalg_collist_concat(cl1,cl2) PFarray_concat ((cl1), (cl2))
/** Size of a column list */
#define PFalg_collist_size(cl)        PFarray_last ((cl))

/* ............. algebra schema specification .............. */

/** An algebra schema item is a (name, type) pair */
struct PFalg_schm_item_t {
    PFalg_col_t         name;
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
    PFalg_col_t new;   /**< new column name to assign */
    PFalg_col_t old;   /**< old column name */
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
    , alg_so_select_narrow /**< StandOff select-narrow axis (non-standard) */
    , alg_so_select_wide   /**< StandOff select-wide axis (non-standard) */
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
    , alg_fun_call_pf_collections          /**< pf:collections             */
    , alg_fun_call_pf_collections_unsafe   /**< pf:collections_unsafe      */
    , alg_fun_call_xrpc                    /**< XRPC function call         */
    , alg_fun_call_xrpc_helpers            /**< func call for XRPC helpers */
    , alg_fun_call_tijah                   /**< Tijah function call        */
    , alg_fun_call_cache                   /**< caching function call      */
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
    , alg_fun_pf_log              /**< pf:log */
    , alg_fun_pf_sqrt             /**< pf:sqrt */
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
    , alg_fun_pf_nid              /**< pf:nid */
    , alg_fun_pf_docname          /**< pf:docname */
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
    , alg_fun_fn_year_from_datetime
    , alg_fun_fn_month_from_datetime
    , alg_fun_fn_day_from_datetime
    , alg_fun_fn_hours_from_datetime
    , alg_fun_fn_minutes_from_datetime
    , alg_fun_fn_seconds_from_datetime
    , alg_fun_fn_year_from_date
    , alg_fun_fn_month_from_date
    , alg_fun_fn_day_from_date
    , alg_fun_fn_hours_from_time
    , alg_fun_fn_minutes_from_time
    , alg_fun_fn_seconds_from_time
    , alg_fun_add_dur          /**< arithmetic plus operator for duration */
    , alg_fun_subtract_dur     /**< arithmetic minus operator for duration */
    , alg_fun_multiply_dur     /**< arithmetic times operator for duration */
    , alg_fun_divide_dur       /**< arithmetic div operator for duration */
#ifdef HAVE_GEOXML
    , alg_fun_geo_wkb             /**< geoxml:wkb */
    , alg_fun_geo_point           /**< geoxml:point */
    , alg_fun_geo_distance        /**< geoxml:distance */
    , alg_fun_geo_geometry        /**< geoxml:geometry */
    , alg_fun_geo_relate          /**< geoxml:geometry */
    , alg_fun_geo_intersection    /**< geoxml:geometry */
#endif
};
typedef enum PFalg_fun_t PFalg_fun_t;


/** doc table operator kinds */
enum PFalg_doc_tbl_kind_t {
      alg_dt_doc   = 1
    , alg_dt_col   = 2
};
typedef enum PFalg_doc_tbl_kind_t PFalg_doc_tbl_kind_t;

/* ................ aggregate specification ................. */

enum PFalg_aggr_kind_t {
      alg_aggr_dist   /**< aggregate of a column that functionally
                           depends on the partitioning column */
    , alg_aggr_count  /**< count aggregate */
    , alg_aggr_min    /**< minimum aggregate */
    , alg_aggr_max    /**< maximum aggregate */
    , alg_aggr_avg    /**< average aggregate */
    , alg_aggr_sum    /**< sum aggregate */
    , alg_aggr_seqty1 /**< sequence type matching for `1' occurrence */
    , alg_aggr_all    /**< all existential quantifier */
    , alg_aggr_prod   /**< product aggregate */
};
typedef enum PFalg_aggr_kind_t PFalg_aggr_kind_t;

struct PFalg_aggr_t {
    PFalg_aggr_kind_t kind; /**< aggregate kind */
    PFalg_col_t       res;  /**< result column */
    PFalg_col_t       col;  /**< input column */
};
typedef struct PFalg_aggr_t PFalg_aggr_t;

/* ............. document fields specification .............. */

enum PFalg_doc_t {
      doc_atext        /**< attribute content > */
    , doc_text         /**< content of a text node > */
    , doc_comm         /**< content of a comment node > */
    , doc_pi_text      /**< content of a pi node > */
    , doc_qname        /**< QName of a node > */
    , doc_atomize      /**< string-value of an element or document node */
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
    PFalg_col_t  left;  /**< left selection column > */
    PFalg_col_t  right; /**< right selection column > */
};
typedef struct PFalg_sel_t PFalg_sel_t;

/* ***************** Constructors ******************* */

/** construct literal natural number (atom) */
PFalg_atom_t PFalg_lit_nat (nat value);

/** construct literal integer (atom) */
PFalg_atom_t PFalg_lit_int (long long int value);

/** construct literal string (atom) */
PFalg_atom_t PFalg_lit_str (char *value);

/** construct literal untypedAtomic (atom) */
PFalg_atom_t PFalg_lit_uA (char *value);

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

#define PFalg_collist_worker(...)                              \
    PFalg_collist_ ((sizeof ((PFalg_col_t[]) { __VA_ARGS__ })  \
                       / sizeof (PFalg_col_t)),                \
                   (PFalg_col_t[]) { __VA_ARGS__ })
/**
 * Construct an column list (list of column names only).
 */
PFalg_collist_t * PFalg_collist_ (unsigned int count, PFalg_col_t *cols);

/** 
 * Constructor for an empty schema with iter|pos|item and item of type item_t
 */
PFalg_schema_t PFalg_iter_pos_item_schema(PFalg_simple_type_t item_t);
PFalg_schema_t PFalg_iter_pos_item_score_schema(PFalg_simple_type_t item_t);

/** 
 * Constructor for an empty schema with iter|item and item of type item_t
 */
PFalg_schema_t PFalg_iter_item_schema(PFalg_simple_type_t item_t);

/** Constructor for projection list item */
PFalg_proj_t PFalg_proj (PFalg_col_t new, PFalg_col_t old);

/**
 * Merge adjacent projection lists.
 */
PFalg_proj_t * PFalg_proj_merge (PFalg_proj_t *upper_proj,
                                 unsigned int upper_count,
                                 PFalg_proj_t *lower_proj,
                                 unsigned int lower_count);
/**
 * Create a projection list based on a schema.
 */
PFalg_proj_t * PFalg_proj_create (PFalg_schema_t schema);

/**
 * Test if two atomic values are comparable
 */
bool PFalg_atom_comparable (PFalg_atom_t a, PFalg_atom_t b);

/**
 * Compare two atomic values (if possible)
 */
int PFalg_atom_cmp (PFalg_atom_t a, PFalg_atom_t b);

/**
 * Print column name
 */
char * PFcol_str (PFalg_col_t col);

/**
 * Initialize the column name counter.
 */
void PFalg_init (void);

/**
 * Checks whether a name is unique or not.
 */
bool PFcol_is_name_unq (PFalg_col_t col);

/**
 * Create a new unique column name (based on an original bit-encoded 
 * or unique column name @a col) that retains the usage information
 * of the new variable (iter, pos or item).
 */
PFalg_col_t PFcol_new (PFalg_col_t col);

/**
 * Create an unique name based on an id @a id (and an original
 * or unique column name @a col) that retains the usage information
 * of the new variable (iter, pos or item).
 */
PFalg_col_t PFcol_new_fixed (PFalg_col_t col, unsigned int id);

/**
 * Create an original column name based on an unique name @a unq
 * and a list of free original variables @a free.
 */
PFalg_col_t PFcol_ori_name (PFalg_col_t unq, PFalg_col_t free);

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
char * PFalg_simple_type_str (PFalg_simple_type_t col);

/**
 * Print function call kind
 */
char * PFalg_fun_call_kind_str (PFalg_fun_call_t kind);

/**
 * Print function name
 */
char * PFalg_fun_str (PFalg_fun_t fun);

/**
 * Print aggregate kind
 */
char * PFalg_aggr_kind_str (PFalg_aggr_kind_t kind);

/**
 * Construct an aggregate entry.
 */
PFalg_aggr_t PFalg_aggr (PFalg_aggr_kind_t kind,
                         PFalg_col_t       res,
                         PFalg_col_t       col);

/**
 * Construct a predicate.
 */
PFalg_sel_t PFalg_sel (PFalg_comp_t comp,
                       PFalg_col_t left,
                       PFalg_col_t right);

#ifdef HAVE_PFTIJAH

/*
 * PFTIJAH defines used by the pftijah funcall interface. In the future
 * the parameters will be implemented using their private context structure
 * instead of decoding info in the function name.
 */

#define PFTIJAH_NODEKIND  aat_pnode
#define DOCMGMTTYPE aat_docmgmt

#define PFT_FUN(F)              (strncmp(F,"pftijah_",8)==0)

#define PFT_QUERY_N_XX  "pftijah_query_n_xx"
#define PFT_QUERY_N_SX  "pftijah_query_n_sx"
#define PFT_QUERY_N_XO  "pftijah_query_n_xo"
#define PFT_QUERY_N_SO  "pftijah_query_n_so"
#define PFT_QUERY_I_XX  "pftijah_query_i_xx"
#define PFT_QUERY_I_SX  "pftijah_query_i_sx"
#define PFT_QUERY_I_XO  "pftijah_query_i_xo"
#define PFT_QUERY_I_SO  "pftijah_query_i_so"

#define PFT_FTFUN_N_SOI "pftijah_ftfun_n_soi"
#define PFT_FTFUN_B_SXX "pftijah_ftfun_b_sxx"
#define PFT_FTFUN_B_SOI "pftijah_ftfun_b_soi"

#define PFT_FUN_QUERY(F)        (strncmp(F,"pftijah_query_",14)==0)
#define PFT_FUN_FTFUN(F)        (strncmp(F,"pftijah_ftfun_",14)==0)

#define PTF_QUERY_NODES(N)      (N[14]=='n')
#define PTF_QUERY_STARTNODES(N) (N[16]=='s')
#define PTF_QUERY_OPTIONS(N)    (N[17]=='o')

#define PTF_FTFUN_RESNODE(N)    (N[14]=='n')
#define PTF_FTFUN_RESBOOL(N)    (N[14]=='b')
#define PTF_FTFUN_STARTNODES(N) (N[16]=='s')
#define PTF_FTFUN_OPTIONS(N)    (N[17]=='o')
#define PTF_FTFUN_IGNORES(N)    (N[18]=='i')

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

#define PFT_TERMS      "pftijah_terms"
#define PFT_TERMS_O    "pftijah_terms_o"
#define PFT_TFALL      "pftijah_tfall"
#define PFT_TFALL_O    "pftijah_tfall_o"
#define PFT_TF         "pftijah_tf"
#define PFT_TF_O       "pftijah_tf_o"
#define PFT_FBTERMS    "pftijah_fbterms"
#define PFT_FBTERMS_O  "pftijah_fbterms_o"

#endif /* HAVE_PFTIJAH */

#endif  /* ALGEBRA_H */

/* vim:set shiftwidth=4 expandtab: */
