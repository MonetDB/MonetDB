/** atomic natural number construction */
#define lit_nat(n)      PFalg_lit_nat (n)

/** atomic integer construction */
#define lit_int(i)      PFalg_lit_int (i)

/** atomic string construction */
#define lit_str(s)      PFalg_lit_str (s)

/** atomic float construction */
#define lit_dec(f)      PFalg_lit_dec (f)

/** atomic double construction */
#define lit_dbl(d)      PFalg_lit_dbl (d)

/** atomic boolean construction */
#define lit_bln(b)      PFalg_lit_bln (b)

/** atomic QName construction */
#define lit_qname(b)      PFalg_lit_qname (b)

/** tuple construction */
#define tuple(...)      PFalg_tuple (__VA_ARGS__)

/** (schema) attribute construction */
#define att(n,t)        PFalg_att ((n),(t))

/** attribute list construction */
#define attlist(...)    PFalg_attlist (__VA_ARGS__)

/** a sort specification list is just another attribute list */
#define sortby(...)     PFalg_attlist (__VA_ARGS__)

/** literal table construction */
#define lit_tbl(...)    PFalg_lit_tbl (__VA_ARGS__)

/** empty table construction */
#define empty_tbl(atts) PFalg_empty_tbl (atts)

/** cartesian product */
#define cross(a,b)      PFalg_cross ((a),(b))

/** equi-join */
#define eqjoin(a,b,c,d) PFalg_eqjoin ((a),(b),(c),(d))

/** dummy node creation */
#define dummy()         PFalg_dummy ()

/** staircase join */
#define scjoin(a,b,c)   PFalg_scjoin ((a),(b),(c))

/** document table */
#define doc_tbl(a)      PFalg_doc_tbl((a))

/** disjoint union */
#define disjunion(a,b)  PFalg_disjunion ((a),(b))

/** intersection */
#define intersect(a,b)  PFalg_intersect ((a),(b))

/** difference */
#define difference(a,b) PFalg_difference ((a),(b))

/** projection operator */
#define project(...)    PFalg_project (__VA_ARGS__)

/** item in the projection list */
#define proj(a,b)       PFalg_proj ((a),(b))

/** rownum operator */
#define rownum(a,b,c,d) PFalg_rownum ((a),(b),(c),(d))

/* selection operator */
#define select_(a,b)    PFalg_select ((a),(b))

/** type test operator */
#define type(a,b,c,d)   PFalg_type ((a),(b),(c),(d))

/* type cast operator */
#define cast(a,b,c)     PFalg_cast ((a),(b),(c))

/* addition operator */
#define add(a,b,c,d)      PFalg_add ((a),(b),(c),(d))

/* subtraction operator */
#define subtract(a,b,c,d) PFalg_subtract ((a),(b),(c),(d))

/* multiplication operator */
#define multiply(a,b,c,d) PFalg_multiply ((a),(b),(c),(d))

/* division operator */
#define divide(a,b,c,d)   PFalg_divide ((a),(b),(c),(d))

/* modulo operator */
#define modulo(a,b,c,d)   PFalg_modulo ((a),(b),(c),(d))

/* numeric equal operator */
#define eq(a,b,c,d)    PFalg_eq ((a),(b),(c),(d))

/* numeric greater-than operator */
#define gt(a,b,c,d) PFalg_gt ((a),(b),(c),(d))

/* numeric negation operator */
#define neg(a,b,c)        PFalg_neg ((a),(b),(c))

/* boolean AND operator */
#define and(a,b,c,d)      PFalg_and ((a),(b),(c),(d))

/* boolean OR operator */
#define or(a,b,c,d)       PFalg_or ((a),(b),(c),(d))

/* boolean NOT operator */
#define not(a,b,c)        PFalg_not ((a),(b),(c))

/* operator forming (partitioned) sum of a column */
#define sum(a,b,c,d)      PFalg_sum ((a),(b),(c),(d))

/* (partitioned) row counting operator */
#define count(a,b,c)      PFalg_count ((a),(b),(c))

/* algebra seqty1 operator (see PFalg_seqty1()) */
#define seqty1(a,b,c,d)   PFalg_seqty1((a), (b), (c), (d))

/* all operator (see PFalg_all()) */
#define all(a,b,c,d)      PFalg_all((a), (b), (c), (d))

/* duplicate elimination operator */
#define distinct(a)       PFalg_distinct ((a))

/* element-constructing operator */
#define element(a,b,c)    PFalg_element ((a),(b),(c))

/* attribute-constructing operator */
#define attribute(a,b)    PFalg_attribute ((a),(b))

/* text node-constructing operator */
#define textnode(a)       PFalg_textnode ((a))

/* document node-constructing operator */
#define docnode(a,b)      PFalg_docnode ((a),(b))

/* comment-constructing operator */
#define comment(a)        PFalg_comment ((a))

/* processing instruction-constructing operator */
#define processi(a)       PFalg_processi ((a))

/* constructor for fs:item-sequence-to-node-sequence() functionality */
#define strconcat(a)      PFalg_strconcat ((a))

/* constructor for pf:merge-adjacent-text-nodes() functionality */
#define merge_adjacent(a,b) PFalg_pf_merge_adjacent_text_nodes ((a),(b))

#define string_value(a,b) PFalg_string_value ((a), (b))

#define cast_item(o)      PFalg_cast_item ((o))

/** serialization */
#define serialize(a,b)    PFalg_serialize ((a),(b))

/** constructor for algebraic representation of newly ceated xml nodes */
#define roots(a)          PFalg_roots ((a))

/** constructor for a new fragment, containing newly ceated xml nodes */
#define fragment(a)       PFalg_fragment ((a))

/** constructor for an empty fragment */
#define empty_frag()      PFalg_empty_frag ()
