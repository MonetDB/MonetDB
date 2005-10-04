/**
 * @file
 *
 * Mnemonic abbreviations for logical algebra constructors.
 *
 * $Id$
 */

/* Also import generic algebra stuff */
#include "algebra_mnemonic.h"

/** literal table construction */
#define lit_tbl(...)    PFla_lit_tbl (__VA_ARGS__)

/** empty table construction */
#define empty_tbl(atts) PFla_empty_tbl (atts)

/** cartesian product */
#define cross(a,b)      PFla_cross ((a),(b))

/** equi-join */
#define eqjoin(a,b,c,d) PFla_eqjoin ((a),(b),(c),(d))

/** dummy node creation */
#define dummy()         PFla_dummy ()

/** staircase join */
#define scjoin(a,b,c)   PFla_scjoin ((a),(b),(c))

/** document table */
#define doc_tbl(a)      PFla_doc_tbl((a))

/** disjoint union */
#define disjunion(a,b)  PFla_disjunion ((a),(b))

/** intersection */
#define intersect(a,b)  PFla_intersect ((a),(b))

/** difference */
#define difference(a,b) PFla_difference ((a),(b))

/** projection operator */
#define project(...)    PFla_project (__VA_ARGS__)

/** rownum operator */
#define rownum(a,b,c,d) PFla_rownum ((a),(b),(c),(d))

/* selection operator */
#define select_(a,b)    PFla_select ((a),(b))

/** type test operator */
#define type(a,b,c,d)   PFla_type ((a),(b),(c),(d))

/* type cast operator */
#define cast(a,b,c)     PFla_cast ((a),(b),(c))

/* addition operator */
#define add(a,b,c,d)      PFla_add ((a),(b),(c),(d))

/* subtraction operator */
#define subtract(a,b,c,d) PFla_subtract ((a),(b),(c),(d))

/* multiplication operator */
#define multiply(a,b,c,d) PFla_multiply ((a),(b),(c),(d))

/* division operator */
#define divide(a,b,c,d)   PFla_divide ((a),(b),(c),(d))

/* modulo operator */
#define modulo(a,b,c,d)   PFla_modulo ((a),(b),(c),(d))

/* numeric equal operator */
#define eq(a,b,c,d)    PFla_eq ((a),(b),(c),(d))

/* numeric greater-than operator */
#define gt(a,b,c,d) PFla_gt ((a),(b),(c),(d))

/* numeric negation operator */
#define neg(a,b,c)        PFla_neg ((a),(b),(c))

/* boolean AND operator */
#define and(a,b,c,d)      PFla_and ((a),(b),(c),(d))

/* boolean OR operator */
#define or(a,b,c,d)       PFla_or ((a),(b),(c),(d))

/* boolean NOT operator */
#define not(a,b,c)        PFla_not ((a),(b),(c))

/* operator forming (partitioned) sum of a column */
#define sum(a,b,c,d)      PFla_sum ((a),(b),(c),(d))

/* (partitioned) row counting operator */
#define count(a,b,c)      PFla_count ((a),(b),(c))

/* algebra seqty1 operator (see PFla_seqty1()) */
#define seqty1(a,b,c,d)   PFla_seqty1((a), (b), (c), (d))

/* all operator (see PFla_all()) */
#define all(a,b,c,d)      PFla_all((a), (b), (c), (d))

/* duplicate elimination operator */
#define distinct(a)       PFla_distinct ((a))

/* element-constructing operator */
#define element(a,b,c)    PFla_element ((a),(b),(c))

/* attribute-constructing operator */
#define attribute(a,b)    PFla_attribute ((a),(b))

/* text node-constructing operator */
#define textnode(a)       PFla_textnode ((a))

/* document node-constructing operator */
#define docnode(a,b)      PFla_docnode ((a),(b))

/* comment-constructing operator */
#define comment(a)        PFla_comment ((a))

/* processing instruction-constructing operator */
#define processi(a)       PFla_processi ((a))

/* constructor for fs:item-sequence-to-node-sequence() functionality */
#define strconcat(a)      PFla_strconcat ((a))

/* constructor for pf:merge-adjacent-text-nodes() functionality */
#define merge_adjacent(a,b) PFla_pf_merge_adjacent_text_nodes ((a),(b))

#define doc_access(a,b,c,d) PFla_doc_access ((a), (b), (c), (d))
#define string_join(a,b)  PFla_string_join ((a), (b))

#define cast_item(o)      PFla_cast_item ((o))

/** serialization */
#define serialize(a,b)    PFla_serialize ((a),(b))

/** constructor for algebraic representation of newly ceated xml nodes */
#define roots(a)          PFla_roots ((a))

/** constructor for a new fragment, containing newly ceated xml nodes */
#define fragment(a)       PFla_fragment ((a))

/** constructor for an empty fragment */
#define empty_frag()      PFla_empty_frag ()

/* vim:set shiftwidth=4 expandtab: */
