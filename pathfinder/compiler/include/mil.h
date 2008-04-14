/**
 * @file
 *
 * Declarations for MIL tree structure
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

#ifndef MIL_H
#define MIL_H

/** maximum number of children of a MIL tree node */
#define MIL_MAXCHILD 12

/** MIL oid's are unsigned integers */
typedef unsigned int oid;

/** MIL identifiers are ints */
typedef unsigned int PFmil_ident_t;

/* reserved identifiers. */
#define PF_MIL_VAR_UNUSED       0
#define PF_MIL_VAR_WS           1
#define PF_MIL_VAR_WS_CONT      2
#define PF_MIL_VAR_KIND_DOC     3
#define PF_MIL_VAR_KIND_ELEM    4
#define PF_MIL_VAR_KIND_TEXT    5
#define PF_MIL_VAR_KIND_COM     6
#define PF_MIL_VAR_KIND_PI      7
#define PF_MIL_VAR_KIND_REF     8

#define PF_MIL_VAR_GENTYPE      9

#define PF_MIL_VAR_ATTR        10
#define PF_MIL_VAR_ELEM        11

#define PF_MIL_VAR_STR         13
#define PF_MIL_VAR_INT         14
#define PF_MIL_VAR_DBL         15
#define PF_MIL_VAR_DEC         16
#define PF_MIL_VAR_BOOL        17

#define PF_MIL_VAR_PRE_SIZE    20
#define PF_MIL_VAR_PRE_LEVEL   21
#define PF_MIL_VAR_PRE_KIND    22
#define PF_MIL_VAR_PRE_PROP    23
#define PF_MIL_VAR_PRE_CONT    24
#define PF_MIL_VAR_PRE_NID     25
#define PF_MIL_VAR_NID_RID     26
#define PF_MIL_VAR_FRAG_ROOT   27
#define PF_MIL_VAR_ATTR_OWN    28
#define PF_MIL_VAR_ATTR_QN     29
#define PF_MIL_VAR_ATTR_PROP   30
#define PF_MIL_VAR_ATTR_CONT   31
#define PF_MIL_VAR_QN_LOC      32
#define PF_MIL_VAR_QN_URI      33
#define PF_MIL_VAR_QN_URI_LOC  34
#define PF_MIL_VAR_QN_PREFIX   35
#define PF_MIL_VAR_PROP_VAL    36
#define PF_MIL_VAR_PROP_TEXT   37
#define PF_MIL_VAR_PROP_COM    38
#define PF_MIL_VAR_PROP_INS    39
#define PF_MIL_VAR_PROP_TGT    40

#define PF_MIL_VAR_LE          42
#define PF_MIL_VAR_LT          43
#define PF_MIL_VAR_EQ          44
#define PF_MIL_VAR_GT          45
#define PF_MIL_VAR_GE          46

#define PF_MIL_VAR_TRACE_OUTER 50
#define PF_MIL_VAR_TRACE_INNER 51
#define PF_MIL_VAR_TRACE_ITER  52
#define PF_MIL_VAR_TRACE_MSG   53
#define PF_MIL_VAR_TRACE_ITEM  54
#define PF_MIL_VAR_TRACE_TYPE  55
#define PF_MIL_VAR_TRACE_REL   56

#ifdef HAVE_PFTIJAH
#define PF_MIL_TIJAH_SCORE_DB  58
#define PF_MIL_TIJAH_FTI_TAPE  59
#endif

#define PF_MIL_VAR_AXIS_ANC    60
#define PF_MIL_VAR_AXIS_ANC_S  61
#define PF_MIL_VAR_AXIS_CHLD   62
#define PF_MIL_VAR_AXIS_DESC   63
#define PF_MIL_VAR_AXIS_DESC_S 64
#define PF_MIL_VAR_AXIS_FOL    65
#define PF_MIL_VAR_AXIS_FOL_S  66
#define PF_MIL_VAR_AXIS_PAR    67
#define PF_MIL_VAR_AXIS_PREC   68
#define PF_MIL_VAR_AXIS_PREC_S 69
#define PF_MIL_VAR_AXIS_SELF   70
#define PF_MIL_VAR_AXIS_ATTR   71

#define PF_MIL_VAR_CODE_NONE   73
#define PF_MIL_VAR_CODE_KIND   74
#define PF_MIL_VAR_CODE_NS     75
#define PF_MIL_VAR_CODE_LOC    76
#define PF_MIL_VAR_CODE_NSLOC  77
#define PF_MIL_VAR_CODE_TARGET 78

#define PF_MIL_VAR_TIME_LOAD   80
#define PF_MIL_VAR_TIME_QUERY  81
#define PF_MIL_VAR_TIME_PRINT  82

#define PF_MIL_RES_VAR_COUNT (PF_MIL_VAR_TIME_PRINT + 1)

/** Node kinds for MIL tree representation */
enum PFmil_kind_t {
      m_lit_int      /**< literal integer */
    , m_lit_lng      /**< literal long integer */
    , m_lit_oid      /**< literal oid */
    , m_lit_str      /**< literal string */
    , m_lit_dbl      /**< literal double */
    , m_lit_bit      /**< literal boolean */

    , m_nil          /**< MonetDB's special value `nil' */

    , m_type         /**< MIL type */
    , m_var          /**< MIL variable */

    , m_seq          /**< Sequence of two MIL statements */

    , m_if           /**< if-then-else blocks */
    , m_while        /**< while statement */

    , m_assgn        /**< assignment statement + declaration (`var ... :=') */
#if 0
    , m_reassgn      /**< assignment statement alone (`:=') */
#endif

    , m_new          /**< MIL new() operator (creates new BATs) */
    , m_sseqbase     /**< MIL seqbase() function */
    , m_seqbase      /**< MIL seqbase() function */
    , m_key          /**< MIL key() function */
    , m_insert       /**< MIL insert() function to insert single BUNs */
    , m_binsert      /**< MIL insert() function to insert a BAT at once */
    , m_bappend      /**< MIL append(): Append one BAT[void,any] to another */
    , m_order        /**< MIL order() function (destructively orders a BAT) */

    , m_select       /**< MIL select(a,b) function */
    , m_select2      /**< MIL select(a,b,c) function */
    , m_uselect      /**< MIL uselect(a,b) function */
    , m_exist        /**< MIL exist(a,b) function */

    , m_project      /**< MIL project() function */
    , m_mark         /**< MIL mark() function */
    , m_hmark        /**< MIL hmark() function */
    , m_tmark        /**< MIL tmark() function */
    , m_mark_grp     /**< MIL mark_grp() function */
    , m_fetch        /**< MIL fetch() function */
    , m_access       /**< change access restrictions to a BAT */

    , m_cross        /**< MIL join operator */
    , m_join         /**< MIL join operator */
    , m_leftjoin     /**< MIL leftjoin operator */
    , m_leftfetchjoin/**< MIL leftfetchjoin operation */
    , m_thetajoin    /**< MIL thetajoin operator */
    , m_unq2_tjoin   /**< MIL htordered_unique_thetajoin PROC
                          applying a unique on both iter columns */
    , m_unq1_tjoin   /**< MIL ll_htordered_unique_thetajoin PROC
                          aligning the iter columns and evaluating
                          a unique operation over the single resulting
                          column. */
    , m_zip_nodes    /**< MIL combine_node_info PROC that compresses
                          the fragment, pre, and attribute ids into
                          a single filed. */

    , m_reverse      /**< MIL reverse operator, swap head/tail */
    , m_mirror       /**< MIL mirror() operator, mirror head into tail */
    , m_copy         /**< MIL copy, returns physical copy of a BAT */
    , m_kunique      /**< MIL kunique() operator, make BAT unique in head */
    , m_tunique      /**< MIL tunique() operator, unique BAT tails in head */
    , m_kunion       /**< MIL kunion() operator */
    , m_kdiff        /**< MIL kdiff() operator */
    , m_kintersect   /**< MIL kintersect() operator */
    , m_sintersect   /**< MIL sintersect() operator */
    , m_mc_intersect /**< multi column intersection */

    , m_merged_union /**< MIL merged_union() function (from the pathfinder
                          runtime module */
    , m_multi_mu     /**< MIL multi_merged_union() function (from the pathfinder
                          runtime module */

    , m_sort         /**< MIL sort function */
    , m_sort_rev     /**< MIL sort_rev function */
    , m_ctgroup      /**< MIL CTgroup function */
    , m_ctmap        /**< MIL CTmap function */
    , m_ctextend     /**< MIL CTextend function */
    , m_ctrefine     /**< MIL CTrefine function */
    , m_ctrefine_rev /**< MIL CTrefine_rev function */
    , m_ctderive     /**< MIL CTderive function */

    , m_cast         /**< typecast */
    , m_mcast        /**< multiplexed typecast */

    , m_add          /**< arithmetic add */
    , m_madd         /**< multiplexed arithmetic add */
    , m_sub          /**< arithmetic subtract */
    , m_msub         /**< multiplexed arithmetic subtract */
    , m_mmult        /**< multiplexed arithmetic multiply */
    , m_div          /**< arithmetic divide */
    , m_mdiv         /**< multiplexed arithmetic divide */
    , m_mmod         /**< multiplexed arithmetic modulo */
    , m_mmax         /**< multiplexed arithmetic maximum */

    , m_mabs         /**< multiplexed operator abs */
    , m_mceiling     /**< multiplexed operator ceil */
    , m_mfloor       /**< multiplexed operator floor */
    , m_mround_up    /**< multiplexed operator round_up */

    , m_gt           /**< greater than */
    , m_eq           /**< equal */
    , m_meq          /**< multiplexed comparison (equality) */
    , m_mgt          /**< multiplexed comparison (greater than) */
    , m_mge          /**< multiplexed comparison (greater equal) */
    , m_mlt          /**< multiplexed comparison (less than) */
    , m_mle          /**< multiplexed comparison (less equal) */
    , m_mne          /**< multiplexed comparison (inequality) */

    , m_not          /**< boolean negation `not' */
    , m_mnot         /**< multiplexed boolean negation `[not]' */
    , m_mneg         /**< multiplexed numeric negation `[-]' */

    , m_mand         /**< multiplexed boolean operator `and' */
    , m_mor          /**< multiplexed boolean operator `or' */

    , m_enum         /**< MIL enumerate() function */
    , m_count        /**< MIL count() function */
    , m_gcount       /**< Grouped count() function `{count}()' */
    , m_egcount      /**< Grouped count() function `{count}()' */
    , m_avg          /**< MIL avg() function */
    , m_gavg         /**< Grouped avg() function `{avg}()' */
    , m_max          /**< MIL max() function */
    , m_gmax         /**< Grouped max() function `{max}()' */
    , m_min          /**< MIL min() function */
    , m_gmin         /**< Grouped min() function `{min}()' */
    , m_sum          /**< MIL sum() function */
    , m_gsum         /**< Grouped sum() function `{sum}()' */
    , m_egsum        /**< Grouped sum() function `{sum}()' */

    , m_declare      /**< declare variable */
    , m_nop          /**< `no operation', do nothing.
                          (This may be produced during compilation.) */
    , m_arg          /**< helper node to list arguments for variable length
                          argument functions (e.g., merged_union) */

    , m_usec         /**< Get the current time */
    , m_new_ws       /**< Create a new (empty) working set */
    , m_destroy_ws   /**< Free an existing working set */
    , m_mposjoin     /**< Positional multijoin with a working set */
    , m_mvaljoin     /**< Multijoin with a working set */
    , m_bat          /**< MonetDB bat() function */
    , m_catch        /**< MonetDB catch function + assignment
                          'a0000 := CATCH()' */
    , m_error        /**< MonetDB ERROR() function */

    , m_isnil        /**< Operator `isnil()' */
    , m_misnil       /**< Multiplexed isnil() operator `[isnil]()' */
    , m_mifthenelse  /**< Multiplexed ifthenelse() operator `[ifthenelse]()' */
    , m_msearch      /**< Multiplexed search() function `[search](a,b)' */
    , m_mstring      /**< Multiplexed string() function `[string](a,b)' */
    , m_mstring2     /**< Multiplexed string() function `[string](a,b,c)' */
    , m_mstarts_with /**< Multiplexed startsWith()
                          function `[startsWith](a,b)' */
    , m_mends_with   /**< Multiplexed endsWith() function `[endsWith](a,b)' */
    , m_mlength      /**< Multiplexed length() function `[length](a)' */
    , m_mtoUpper     /**< Multiplexed toUpper() function `[toUpper](a)' */
    , m_mtoLower     /**< Multiplexed toLower() function `[toLower](a)' */
    , m_mtranslate   /**< Multiplexed translate() func `[translate](a,b,c)' */

    , m_mnorm_space       /**< Multiplexed normSpace() function */
    , m_mpcre_match       /**< Multiplexed pcre_match() function */
    , m_mpcre_match_flag  /**< Multiplexed pcre_match() function w flags*/
    , m_mpcre_replace     /**< Multiplexed pcre_replace() function */

    , m_step

    , m_merge_adjacent
    , m_string_join

    , m_get_fragment
    , m_set_kind
    , m_materialize

    , m_assert_order /**< MIL assert_order() function. Fixes the order
                          property of the tail in MonetDB BAT headers.
                          It is a work-around to tell MonetDB about
                          sortedness whenever it is not able to infer
                          it itself. */

    , m_chk_order    /**< MIL chk_order() function. Fixes order properties
                          in MonetDB BAT headers. We should try to get rid
                          of these completely, and fix/implement MonetDB
                          operators that are sufficiently order-aware to
                          correctly set order properties right away. */

    , m_sc_desc      /**< Staircase Join descendant axis */

    , m_doc_tbl      /**< doc_tbl() function (Pathfinder runtime) */

    , m_attr_constr  /**< attr_constr() function (Pathfinder runtime) */

    , m_elem_constr  /**< elem_constr() function (Pathfinder runtime) */

    , m_elem_constr_e  /**< elem_constr_empty() function
                          (Pathfinder runtime) */

    , m_text_constr  /**< text_constr() function (Pathfinder runtime) */

    , m_add_qname    /**< add_qname() function (Pathfinder runtime) */

    , m_add_qnames   /**< add_qnames() function (Pathfinder runtime) */

    , m_add_content  /**< add_conten() function (Pathfinder runtime) */

    , m_chk_qnames   /**< invalid_qname() function (Pathfinder runtime) */

    , m_print        /**< MIL print() function */

    , m_col_name     /**< assign BAT column name (for debugging only) */

    , m_comment      /**< MIL comment (for debugging only) */

    , m_serialize    /**< serialization function.
                          The serialization function must be provided by the
                          pathfinder extension module. For now we just have
                          a simple MIL script that does the work mainly for
                          debugging purposes. */

    , m_trace        /**< trace function.
                          The trace function must be provided by the
                          pathfinder extension module. For now we just have
                          a simple MIL script that does the work mainly for
                          debugging purposes. */

    , m_module       /**< module loading */

    , m_update_tape  /**< play update tape function  */ 
    , m_docmgmt_tape /**< play docmgmt tape function */
    , m_ws_collection_root /**< mil document function for collection */
    , m_ws_documents       /**< mil document function for documents  */
    , m_ws_documents_str   /**< mil document function
                                for documents in a specified collection */
    , m_ws_docname         /**< mil document function for  docname      */
    , m_ws_collections     /**< mil document function for collections   */
#ifdef HAVE_PFTIJAH
    , m_tj_pfop
    , m_tj_tokenize
    , m_tj_query_handler
    , m_tj_query_score
    , m_tj_query_nodes
    , m_tj_add_fti_tape
    , m_tj_docmgmt_tape
#endif
};
typedef enum PFmil_kind_t PFmil_kind_t;

/* enum values must not be zero (zero is used for some error detection) */
enum PFmil_type_t {
      mty_oid   = 1
    , mty_void  = 2
    , mty_int   = 3
    , mty_str   = 4
    , mty_lng   = 5
    , mty_dbl   = 6
    , mty_bit   = 7
    , mty_chr   = 8
    , mty_bat   = 9
};
typedef enum PFmil_type_t PFmil_type_t;

enum PFmil_access_t {
      BAT_WRITE     /**< full read/write access to this BAT */
    , BAT_READ      /**< BAT is read-only */
    , BAT_APPEND    /**< BUNs may be inserted, but no updates or deletions */
};
typedef enum PFmil_access_t PFmil_access_t;

/* enum for update types */
enum PFmil_update_t {
      UPDATE_INSERT_FIRST     = 1
    , UPDATE_INSERT_LAST      = 2
    , UPDATE_INSERT_BEFORE    = 3
    , UPDATE_INSERT_AFTER     = 4
    , UPDATE_REPLACECONTENT   = 5
    , UPDATE_REPLACENODE      = 6
    , UPDATE_DELETE           = 7
    , UPDATE_RENAME           = 8
    , UPDATE_REPLACE          = 9
};
typedef enum PFmil_update_t PFmil_update_t;

/** semantic content for MIL tree nodes */
union PFmil_sem_t {
    int           i;       /**< literal integer */
    long long int l;       /**< literal long integer */
    oid           o;       /**< literal oid */
    char         *s;       /**< literal string */
    double        d;       /**< literal double */
    bool          b;       /**< literal boolean */

    PFmil_type_t  t;       /**< MIL type (for #m_type nodes) */
    PFmil_ident_t ident;   /**< MIL identifier */
    PFmil_access_t access; /**< BAT access specifier, only for #m_access nodes*/
};
typedef union PFmil_sem_t PFmil_sem_t;

/** MIL tree node */
struct PFmil_t {
    PFmil_kind_t     kind;
    PFmil_sem_t      sem;
    struct PFmil_t  *child[MIL_MAXCHILD];
};
typedef struct PFmil_t PFmil_t;

/** a literal integer */
PFmil_t * PFmil_lit_int (int i);

/** a literal long integer */
PFmil_t * PFmil_lit_lng (long long int l);

/** a literal string */
PFmil_t * PFmil_lit_str (const char *s);

/** a literal oid */
PFmil_t * PFmil_lit_oid (oid o);

/** a literal dbl */
PFmil_t * PFmil_lit_dbl (double d);

/** a literal bit */
PFmil_t * PFmil_lit_bit (bool b);

/** a MIL variable */
PFmil_t * PFmil_var (PFmil_ident_t name);

/** return the variable name as string */
char * PFmil_var_str (PFmil_ident_t name);

/** MIL type */
PFmil_t * PFmil_type (PFmil_type_t);

/** MIL `no operation' (statement that does nothing) */
PFmil_t * PFmil_nop (void);

/** MIL keyword `nil' */
PFmil_t * PFmil_nil (void);

/** shortcut for MIL variable `unused' */
#define PFmil_unused() PFmil_var (PF_MIL_VAR_UNUSED)

/** MIL new() statement */
PFmil_t * PFmil_new (const PFmil_t *, const PFmil_t *);

/** if-then-else clauses */
PFmil_t * PFmil_if (const PFmil_t *, const PFmil_t *, const PFmil_t *);

/** while statement */
PFmil_t * PFmil_while (const PFmil_t *, const PFmil_t *);
/**
 * Assignment statement including declaration:
 * Declare variable v and assign expression e to it.
 */
PFmil_t * PFmil_assgn (const PFmil_t *v, const PFmil_t *e);

/** MIL seqbase() function */
PFmil_t * PFmil_seqbase_lookup (const PFmil_t *);

/** MIL seqbase() function */
PFmil_t * PFmil_seqbase (const PFmil_t *, const PFmil_t *);

/** MIL key() function */
PFmil_t * PFmil_key (const PFmil_t *, bool);

/** MIL order() function (destructively re-orders a BAT by its head) */
PFmil_t * PFmil_order (const PFmil_t *);

/** MIL select() function */
PFmil_t * PFmil_select (const PFmil_t *, const PFmil_t *);

/** MIL select() function */
PFmil_t * PFmil_select2 (const PFmil_t *, const PFmil_t *, const PFmil_t *);

/** MIL uselect() function */
PFmil_t * PFmil_uselect (const PFmil_t *, const PFmil_t *);

/** MIL exist() function */
PFmil_t * PFmil_exist (const PFmil_t *, const PFmil_t *);

/** MIL insert() function to insert a single BUN (3 arguments) */
PFmil_t * PFmil_insert (const PFmil_t *, const PFmil_t *, const PFmil_t *);

/** MIL insert() function to insert a whole BAT at once (2 arguments) */
PFmil_t * PFmil_binsert (const PFmil_t *, const PFmil_t *);

/** MIL append() function to append a BAT[void,any] to another */
PFmil_t * PFmil_bappend (const PFmil_t *, const PFmil_t *);

/** MIL project() function */
PFmil_t * PFmil_project (const PFmil_t *, const PFmil_t *);

/** MIL mark() function */
PFmil_t * PFmil_mark (const PFmil_t *, const PFmil_t *);

/** MIL hmark() function */
PFmil_t * PFmil_hmark (const PFmil_t *, const PFmil_t *);

/** MIL tmark() function */
PFmil_t * PFmil_tmark (const PFmil_t *, const PFmil_t *);

/** MIL mark_grp() function */
PFmil_t * PFmil_mark_grp (const PFmil_t *, const PFmil_t *);

/** MIL fetch() function */
PFmil_t * PFmil_fetch (const PFmil_t *, const PFmil_t *);

/** Set access restrictions for a BAT */
PFmil_t * PFmil_access (const PFmil_t *, PFmil_access_t);

/** MIL cross() operator */
PFmil_t * PFmil_cross (const PFmil_t *, const PFmil_t *);

/** MIL join() operator */
PFmil_t * PFmil_join (const PFmil_t *, const PFmil_t *);

/** MIL leftjoin() operator ensures ordering by left operand */
PFmil_t * PFmil_leftjoin (const PFmil_t *, const PFmil_t *);

/** MIL leftfetchjoin() operator */
PFmil_t * PFmil_leftfetchjoin (const PFmil_t *, const PFmil_t *);

/** MIL thetajoin() operator */
PFmil_t * PFmil_thetajoin (const PFmil_t *, const PFmil_t *,
                           const PFmil_t *, const PFmil_t *);

/** MIL htordered_unique_thetajoin PROC */
PFmil_t * PFmil_unq2_thetajoin (const PFmil_t *, const PFmil_t *,
                                const PFmil_t *);

/** MIL ll_htordered_unique_thetajoin PROC */
PFmil_t * PFmil_unq1_thetajoin (const PFmil_t *, const PFmil_t *,
                                const PFmil_t *, const PFmil_t *,
                                const PFmil_t *);

/** MIL combine_node_info PROC */
PFmil_t * PFmil_zip_nodes (const PFmil_t *, const PFmil_t *,
                           const PFmil_t *, const PFmil_t *,
                           const PFmil_t *, const PFmil_t *);

/** MIL reverse() function, swap head/tail */
PFmil_t * PFmil_reverse (const PFmil_t *);

/** MIL mirror() function, mirror head into tail */
PFmil_t * PFmil_mirror (const PFmil_t *);

/** MIL kunique() function, make BAT unique in head */
PFmil_t * PFmil_kunique (const PFmil_t *);

/** MIL tunique() function, unique BAT tails in head */
PFmil_t * PFmil_tunique (const PFmil_t *);

/** MIL kunion() function */
PFmil_t * PFmil_kunion (const PFmil_t *, const PFmil_t *);

/** MIL kdiff() function */
PFmil_t * PFmil_kdiff (const PFmil_t *, const PFmil_t *);

/** MIL kintersect() function */
PFmil_t * PFmil_kintersect (const PFmil_t *, const PFmil_t *);

/** MIL sintersect() function */
PFmil_t * PFmil_sintersect (const PFmil_t *, const PFmil_t *);

/** Multi column intersection (MIL ds_link() proc from the mkey module) */
PFmil_t * PFmil_mc_intersect (const PFmil_t *);

/** MIL merged_union() function (from the pathfinder runtime module) */
PFmil_t * PFmil_merged_union (const PFmil_t *);

/** MIL multi_merged_union() function (from the pathfinder runtime module) */
PFmil_t * PFmil_multi_merged_union (const PFmil_t *);

/** list arguments in a variable argument list function */
PFmil_t * PFmil_arg (const PFmil_t *, const PFmil_t *);

/** MIL copy operator, returns physical copy of a BAT */
PFmil_t * PFmil_copy (const PFmil_t *);

/** MIL sort function */
PFmil_t * PFmil_sort (const PFmil_t *, bool dir_desc);

/** MIL CTgroup function */
PFmil_t * PFmil_ctgroup (const PFmil_t *);

/** MIL CTmap function */
PFmil_t * PFmil_ctmap (const PFmil_t *);

/** MIL CTextend function */
PFmil_t * PFmil_ctextend (const PFmil_t *);

/** MIL CTrefine function */
PFmil_t * PFmil_ctrefine (const PFmil_t *, const PFmil_t *, bool dir_desc);

/** MIL CTderive function */
PFmil_t * PFmil_ctderive (const PFmil_t *, const PFmil_t *);

/** enumerate operator, return sequence of integers */
PFmil_t * PFmil_enumerate (const PFmil_t *, const PFmil_t *);
    
/** MIL count() function, return number of BUNs in a BAT */
PFmil_t * PFmil_count (const PFmil_t *);

/** Grouped count function `{count}()' (aka. ``pumped count'') */
PFmil_t * PFmil_gcount (const PFmil_t *);

/** Grouped count function `{count}()' with two parameters
    (aka. ``pumped count'') */
PFmil_t * PFmil_egcount (const PFmil_t *, const PFmil_t *);

/** MIL avg() function */
PFmil_t * PFmil_avg (const PFmil_t *);

/** Grouped avg function `{avg}()' (aka. ``pumped avg'') */
PFmil_t * PFmil_gavg (const PFmil_t *);

/** MIL max() function */
PFmil_t * PFmil_max (const PFmil_t *);

/** Grouped max function `{max}()' (aka. ``pumped max'') */
PFmil_t * PFmil_gmax (const PFmil_t *);

/** MIL min() function */
PFmil_t * PFmil_min (const PFmil_t *);

/** Grouped min function `{min}()' (aka. ``pumped min'') */
PFmil_t * PFmil_gmin (const PFmil_t *);

/** MIL sum() function */
PFmil_t * PFmil_sum (const PFmil_t *);

/** Grouped sum function `{sum}()' (aka. ``pumped sum'') */
PFmil_t * PFmil_gsum (const PFmil_t *);

/** Grouped sum function `{sum}()' with two parameters
    (aka. ``pumped sum'') */
PFmil_t * PFmil_egsum (const PFmil_t *, const PFmil_t *);

/** typecast */
PFmil_t * PFmil_cast (const PFmil_t *, const PFmil_t *);

/** multiplexed typecast */
PFmil_t * PFmil_mcast (const PFmil_t *, const PFmil_t *);

/** MIL add operator */
PFmil_t * PFmil_add (const PFmil_t *, const PFmil_t *);

/** MIL multiplexed add operator */
PFmil_t * PFmil_madd (const PFmil_t *, const PFmil_t *);

/** MIL subtract operator */
PFmil_t * PFmil_sub (const PFmil_t *, const PFmil_t *);

/** MIL multiplexed subtract operator */
PFmil_t * PFmil_msub (const PFmil_t *, const PFmil_t *);

/** MIL multiplexed multiply operator */
PFmil_t * PFmil_mmult (const PFmil_t *, const PFmil_t *);

/** MIL divide operator */
PFmil_t * PFmil_div (const PFmil_t *, const PFmil_t *);

/** MIL multiplexed divide operator */
PFmil_t * PFmil_mdiv (const PFmil_t *, const PFmil_t *);

/** MIL multiplexed modulo operator */
PFmil_t * PFmil_mmod (const PFmil_t *, const PFmil_t *);

/** MIL multiplexed maximum operator */
PFmil_t * PFmil_mmax (const PFmil_t *, const PFmil_t *);

/** MIL multiplexed operator abs */
PFmil_t * PFmil_mabs (const PFmil_t *);

/** MIL multiplexed operator ceil */
PFmil_t * PFmil_mceil (const PFmil_t *);

/** MIL multiplexed operator floor */
PFmil_t * PFmil_mfloor (const PFmil_t *);

/** MIL multiplexed operator round_up */
PFmil_t * PFmil_mround_up (const PFmil_t *);

/** greater than */
PFmil_t * PFmil_gt (const PFmil_t *, const PFmil_t *);

/** equal */
PFmil_t * PFmil_eq (const PFmil_t *, const PFmil_t *);

/** Multiplexed comparison operator (equality) */
PFmil_t * PFmil_meq (const PFmil_t *, const PFmil_t *);

/** Multiplexed comparison operator (greater than) */
PFmil_t * PFmil_mgt (const PFmil_t *, const PFmil_t *);

/** Multiplexed comparison operator (greater equal) */
PFmil_t * PFmil_mge (const PFmil_t *, const PFmil_t *);

/** Multiplexed comparison operator (less than) */
PFmil_t * PFmil_mlt (const PFmil_t *, const PFmil_t *);

/** Multiplexed comparison operator (less equal) */
PFmil_t * PFmil_mle (const PFmil_t *, const PFmil_t *);

/** Multiplexed comparison operator (inequality) */
PFmil_t * PFmil_mne (const PFmil_t *, const PFmil_t *);

/** MIL boolean negation */
PFmil_t * PFmil_not (const PFmil_t *);

/** MIL multiplexed boolean negation */
PFmil_t * PFmil_mnot (const PFmil_t *);

/** MIL multiplexed numeric negation */
PFmil_t * PFmil_mneg (const PFmil_t *);

/** Multiplexed boolean operator `and' */
PFmil_t * PFmil_mand (const PFmil_t *, const PFmil_t *);

/** Multiplexed boolean operator `or' */
PFmil_t * PFmil_mor (const PFmil_t *, const PFmil_t *);

/** Operator `[isnil]' */
PFmil_t * PFmil_isnil (const PFmil_t *);

/** Multiplexed isnil operator `[isnil]' */
PFmil_t * PFmil_misnil (const PFmil_t *);

/** Multiplexed ifthenelse operator `[ifthenelse]' */
PFmil_t * PFmil_mifthenelse (const PFmil_t *, const PFmil_t *, const PFmil_t *);

/** Multiplexed search() function `[search](a,b)' */
PFmil_t * PFmil_msearch (const PFmil_t *, const PFmil_t *);

/** Multiplexed string() function `[string](a,b)' */
PFmil_t * PFmil_mstring (const PFmil_t *, const PFmil_t *);

/** Multiplexed string() function `[string](a,b,c)' */
PFmil_t * PFmil_mstring2 (const PFmil_t *, const PFmil_t *, const PFmil_t *);

/** Multiplexed startsWith() function `[startsWith](a,b)' */
PFmil_t * PFmil_mstarts_with (const PFmil_t *, const PFmil_t *);

/** Multiplexed endsWith() function `[endsWith](a,b)' */
PFmil_t * PFmil_mends_with (const PFmil_t *, const PFmil_t *);

/** Multiplexed length() function `[length](a)' */
PFmil_t * PFmil_mlength (const PFmil_t *);

/** Multiplexed toUpper() function `[toUpper](a)' */
PFmil_t * PFmil_mtoUpper (const PFmil_t *);

/** Multiplexed toLower() function `[toLower](a)' */
PFmil_t * PFmil_mtoLower (const PFmil_t *);

/** Multiplexed translate() function `[translate](a,b,c)' */
PFmil_t * PFmil_mtranslate (const PFmil_t *, const PFmil_t *, const PFmil_t *);

/** Multiplexed normSpace() function `[normSpace](a)' */
PFmil_t * PFmil_mnorm_space (const PFmil_t *);

/** Multiplexed pcre_match() function `[pcre_match](a,b)' */
PFmil_t * PFmil_mpcre_match (const PFmil_t *, const PFmil_t *);

/** Multiplexed pcre_match() function `[pcre_match](a,b,c)' */
PFmil_t * PFmil_mpcre_match_flag (const PFmil_t *, const PFmil_t *,
                                                   const PFmil_t *);

/** Multiplexed pcre_replace() function `[pcre_replace](a,b,c,d)' */
PFmil_t * PFmil_mpcre_replace (const PFmil_t *, const PFmil_t *,
                               const PFmil_t *, const PFmil_t *);

PFmil_t * PFmil_usec (void);
PFmil_t * PFmil_new_ws (void);
PFmil_t * PFmil_destroy_ws (const PFmil_t *ws);
PFmil_t * PFmil_mposjoin (const PFmil_t *, const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_mvaljoin (const PFmil_t *, const PFmil_t *, const PFmil_t *);

PFmil_t * PFmil_bat (const PFmil_t *);
PFmil_t * PFmil_catch (const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_error (const PFmil_t *);

/* ---------- general purpose staircase join ---------- */
PFmil_t * PFmil_step (const PFmil_t *axis, const PFmil_t *test,
                      const PFmil_t *iter, const PFmil_t *frag,
                      const PFmil_t *pre,  const PFmil_t *attr,
                      const PFmil_t *ws,   const PFmil_t *ord,
                      const PFmil_t *kind, const PFmil_t *ns,
                      const PFmil_t *loc,  const PFmil_t *tgt);

PFmil_t * PFmil_merge_adjacent (const PFmil_t *, const PFmil_t *,
                                const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_string_join (const PFmil_t *, const PFmil_t *);

PFmil_t * PFmil_get_fragment (const PFmil_t *);
PFmil_t * PFmil_set_kind (const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_materialize (const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_assert_order (const PFmil_t *);
PFmil_t * PFmil_chk_order (const PFmil_t *);


/** Staircase join for descendant axis */
PFmil_t * PFmil_sc_desc (const PFmil_t *ws, const PFmil_t *iter,
                         const PFmil_t *item, const PFmil_t *live);

PFmil_t * PFmil_doc_tbl (const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_attribute (const PFmil_t *, const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_element (const PFmil_t *, const PFmil_t *,
                         const PFmil_t *, const PFmil_t *,
                         const PFmil_t *, const PFmil_t *,
                         const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_empty_element (const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_textnode (const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_add_qname (const PFmil_t *, const PFmil_t *,
                           const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_add_qnames (const PFmil_t *, const PFmil_t *,
                            const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_add_content (const PFmil_t *, const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_check_qnames (const PFmil_t *);

PFmil_t * PFmil_declare (const PFmil_t *);

PFmil_t * PFmil_print (const PFmil_t *);
PFmil_t * PFmil_col_name (const PFmil_t *, const PFmil_t *);
PFmil_t * PFmil_comment (const char *, ...)
      __attribute__ ((format (printf, 1, 2)));

PFmil_t * PFmil_ser (const PFmil_t *);

PFmil_t * PFmil_trace (const PFmil_t *);

PFmil_t * PFmil_module (const PFmil_t *);

PFmil_t * PFmil_upd (const PFmil_t *);

PFmil_t * PFmil_docmgmt (const PFmil_t *);

/** 
 * mil document function:
 * ws_collection_root(BAT[void,BAT], BAT[void,str]) : BAT[oid,oid]  
 */
PFmil_t * PFmil_ws_collection_root (const PFmil_t *, const PFmil_t *);

/** 
 * mil document function:
 * ws_documents(BAT[void,BAT], bit) : BAT[void,oid]
 */
PFmil_t * PFmil_ws_documents (const PFmil_t *, const PFmil_t *);

/** 
 * mil document function:
 * ws_documents(BAT[void,BAT], BAT[any,str], bit) : BAT[oid,oid] 
 */
PFmil_t * PFmil_ws_documents_str (const PFmil_t *, const PFmil_t *,
                                  const PFmil_t *);

/** 
 * mil document function:
 * ws_docname(BAT[void,BAT], BAT[void,oid], BAT[void,oid], BAT[void,int])
 *                                                             : BAT[oid,str]
 */
PFmil_t * PFmil_ws_docname (const PFmil_t *, const PFmil_t *,
                            const PFmil_t *, const PFmil_t *);

/** 
 * mil document function:
 * ws_collections(BAT[void,BAT], bit) : BAT[void,oid]
 */
PFmil_t * PFmil_ws_collections (const PFmil_t *, const PFmil_t *);

#define PFmil_seq(...) \
    PFmil_seq_ (sizeof ((PFmil_t *[]) { __VA_ARGS__} ) / sizeof (PFmil_t *), \
                (const PFmil_t *[]) { __VA_ARGS__ } )
PFmil_t *PFmil_seq_ (int count, const PFmil_t **stmts);

#ifdef HAVE_PFTIJAH

PFmil_t * PFmil_tj_query_handler (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c, const PFmil_t *d, const PFmil_t *e, const PFmil_t *f, const PFmil_t *g);

PFmil_t * PFmil_tj_query_score (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c, const PFmil_t *d);

PFmil_t * PFmil_tj_query_nodes (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c);

PFmil_t * PFmil_tj_pfop (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c, const PFmil_t *d);

PFmil_t * PFmil_tj_docmgmt_tape (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c, const PFmil_t *d, const PFmil_t *e, const PFmil_t *f);

PFmil_t * PFmil_tj_add_fti_tape (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c, const PFmil_t *d, const PFmil_t *e, const PFmil_t *f);

PFmil_t * PFmil_tj_tokenize (const PFmil_t *a);
#endif


#endif   /* MIL_H */

/* vim:set shiftwidth=4 expandtab: */
