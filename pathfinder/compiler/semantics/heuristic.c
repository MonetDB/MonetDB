/**
 * @file
 *
 * The "push-selections-down" heuristic, for general equi-comparisions, 
 * enhanced with run-time index plan selection ("smarts without stats").
 *
 * Brought to you by Peter Boncz (CWI), summer holiday 2007.
 *
 * Initially developed for use in the MonetDB backends (mps and algebra). 
 * An adaptation to the SQL backend should be possible as well.
 *
 *
 * Recognized Patterns:
 * --------------------
 *
 * (1)  EXPR0/PATH1[PRED(.) = EXPR1]
 * (2)  for $x in EXPR0/PATH1
 *      where PRED($x) = EXPR1 
 *      [ order by EXPR3($x),.. ]
 *      return EXPR2($x)
 *
 * with PRED($x) being $x/PATH2/@ns:id, $x/PATH2/text() or $x/PATH2
 * - left and right '=' arguments may be swapped (i.e. EXPR1 = PRED($x)). 
 * - the PRED($x) = EXPR1 equality test may be enclosed in multiple (directly 
 *   nested) boolean and-operations (i.e. conjunctive predicates)
 * Note that for (1) $x is just "."
 * For (2), the restriction on PRED($x) holds, that $x is never used except as 
 * listed above (the start of the text() or attribute() expression being 
 * equi-compared).
 *
 * For both (1) and (2), I then generate:
 *
 * let $var1 := EXPR0, (: our context nodes *)
 *
 *     (: atomize lookup values first - doing this in MonetDB would be a pain :)
 *     $var2 := for $var3 in EXPR1
 *              return if ($var3 instance of node()) 
 *                     then data($var3) else $var3,
 *
 *     (: use pf:text/pf:attribute index-lookup builtins to get candidates    :)
 *     $var4 := for $var5 in $var2
 *              (: viz pf:attribute($var1, "el_ns","el_loc","at_ns","at_loc", :)
 *              return pf:text($var1, string($var5)) 
 *
 * return
 *     (: if we have candidates, use them, otherwise use the original plan :)
 *     if (some $var6 in $var3 satisfies $var6 instance of document-node())
 *     then
 *          (: original plan :)
 *          for $x in EXPR0/PATH1
 *          where PATH2/text() = $var2 (: re-use atomization effort :)
 *          [ order by EXPR3($x) ]
 *          return EXPR2($x)
 *     else
 *          (: index plan :)
 *          for $x in ($var4/UP/PATH2_REV)
 *                    [some $var7 in pf:supernode(./PATH1_REV),
 *                          $var8 in $var1 satisfies $var7 is $var8]
 *          where $x/child::b/text() = $var2
 *          [ order by EXPR3($x) ]
 *          return EXPR2($x)
 *
 * Notes:
 * - for pattern (1), EXPR2 is just $x
 * - PATH1 and PATH2 are (possibly empty) location path sequences, with
 *   [pred] predicates allowed in between
 * - REV_PATH1 and REV_PATH2 are the reverse paths (inverse steps in reverse 
 *   order) of PATH1 resp. PATH2.
 * - EXPR0 and EXPR2($x),.. can be any XQuery expression
 * - in the final return pattern we use PRED($x)=$var2 instead of PRED($x)=EXPR1
 *   such that even if at runtime the index is not used, we did not waste effort 
 *   on atomizing node values in EXPR1 twice ($var2 contains atomized values).
 * - UP = . for attribute tests. For PRED($x)/text() = EXPR1 tests, 
 *   UP = parent::nsloc, to go from text nodes to element nodes. Finally, 
 *   UP = ancestor::nsloc, for PRED(x) = EXPR1 tests (ie on fn:data).
 *   Last match is tricky: the first non-space text node will produce
 *   the match.
 *
 * The run-time decision whether to use the index or not is encoded in the $var6
 * test for document nodes!! When the index should not be used, the pf:text() 
 * or pf:attribute() return a single result: a document node (1@0).
 * Note that the access path selection is done *per iteration*!!
 *
 * Note I repeat the EXPR0 in the original plan, so potentially it is executed
 * twice (instead of re-using $var1 there). This was done because mps loop-lifts
 * $var1 even if it is loop-independent, making PATH1 evaluation much more 
 * expensive. Also, first the index- and original- alternatives used the same
 *   where PRED(x) = EXPR1 order by EXPR3($x) return EXPR2($x)
 * part, and had the if-then-else in its for-binding, but the result was that 
 * joins with an outer for-loop and the original query pattern were no longer 
 * recognized (the index else-case then causes the join expressions to depend).
 * 
 *
 * Example:
 * --------
 *
 *  doc("foo")/descendant::a[./child::b/@id = "1"]/ret
 *
 * becomes (after core simplicifation of $var2 and $var4):
 *
 * let $var1 := doc("foo"),
 *     $var2 := "1",
 *     $var4 := pf:attribute($var1, "*", "b", "*", "id", $var2),
 * return
 *     if (some $var6 in $var4 satisfies $var6 instance of document-node())
 *     then (: original plan :)
 *          doc("foo")/descendant::a[./child::b/@id = $var2]/ret
 *     else (: index plan :)
 *          for $var9 in ($var3/parent::b/parent::*)
 *                        [some $var7 in pf:supernode(./self::a/ancestor::*),
 *                              $var8 in $var1 satisfies $var7 is $var8]
 *          where data($var9/child::b/text()) = $var2
 *          return $var8/ret)
 *
 *
 * New XQuery Primitives:
 * ----------------------
 *
 * I use the following pathfinder-specific builtin extensions that support 
 * equi-value index-based lookup:
 * - pf:supernode($ctx as node()*) as node()*
 * - pf:attribute($frag as node()*, $ns as xs:string, $loc as xs:string, 
 *                $val as xs:string) as node()*
 * - pf:text($frag as node()*, $val as xs:string) as node()*
 *
 * For each document node in $ctx, pf:supernode() adds its collection-supernode 
 * to the result. Collection supernodes are returned by pf:collection(), and 
 * provide a single parent to all documents in a collection, to start navigating 
 * from. Because normal upwards paths do *not* return supernodes, $x/PATH1_REV 
 * would miss it, and that is why the pf:supernode() had to be introduced.
 *
 * pf:attribute() and pf:text() perform equi-lookup using an index. The index 
 * stores xs:double hash values that are derived from the xs:string text- and 
 * attribute- values in the document at indexing time, and maps them to NIDs.
 * I use a special hash function that maps all strings that start with a numeric 
 * (after skipping spaces) to their double cast. Also, "true" hashes on 1.0, 
 * "false" on 0.0 and other strings to a random value (internally, these doubles
 * are compressed into 32-bits integer bit patterns again to save space). 
 *
 * This special hash function is required for proper XQuery equality semantics
 * (an attribute with value x="   4.2E1 foo" must match [@x = 42], etc etc).
 *
 *
 * Runtime Aspects:
 * ----------------
 * 
 * As mentioned, the MonetDB indexing system may decide at *run-time* that index
 * access is not possible or not useful.  To signal this, the pf:attribute() and 
 * pf:text() built-ins return a single document node (such a node cannot be 
 * returned by pf:attribute or pf:text() normally, as they return text nodes 
 * or element nodes respectively).
 *
 * In the MonetDB implementation, an index is *not* used (run-time decision) if:
 * - the context nodes have too few descendants on average (<1000)
 *   In that case all iterations won't use the index (global decision). 
 * - a context node (EXPR0) is a newly constructed node (because the temporary 
 *   document container is not indexed)
 * - the selection provides "too many" hits and therefore was pruned from the 
 *   index (index keeps a tombstone to detect this)
 * - the total amount of nodes (over all iterations) is too "large" (>1000).
 * 
 *
 * Fragilities:
 * ------------
 *
 * The main fragility of these patterns is in the requirement for PATH1 and 
 * PATH2 to be formulated as a concatenation of steps. So, let-bindings chopping
 * up an otherwise adjacent sequence of steps (as in the normalized Core 
 * representation), make me lose track. Same holds for the use of function 
 * calls. Pattern (2) has a another syntactic fragility. Namely, the where-
 * condition with the string equality must start with the variable ($x) that is 
 * bound to its *own* FLWR block. Note that index-access is also useful if $x 
 * stems from an outer for-loop. That is, if the equality predicate is (part of) 
 * a join condition. So, I only detect such joins if the equi-test text/attribute 
 * access starts in the inner join expression.
 *
 *
 * Future Work:
 * ------------
 *
 * As the index maps (hashes) values on doubles, we can also use it to handle 
 * range queries on numeric values. The fact that at this point in the pathfinder
 * compiler we do not have type info could be mitigated by a run-time hack: 
 * e.g. we could let the index give up at runtime on range-patterns if EXPR1
 * is not fully numeric.
 *
 * This approach could handle the patterns:
 * - PRED($x) comp EXPR1, comp in union(lcomp,gcomp), lcomp={<,<=}, gcomp={>,>=}
 * - (PRED($x) gcomp EXPR1) and (PRED($x) lcomp EXPR2)
 * - (PRED1($x) gcomp EXPR1) and (PRED1($x) lcomp EXPR2) or
 *   (PRED2($x) gcomp EXPR1) and (PRED2($x) lcomp EXPR2)
 *
 * The latter two patterns test point-intersect-region, and region-overlaps.
 * Such support for region tests is arguably more powerful than the standoff
 * work by Wouter Alink. It will also make MonetDB the champion of the so-
 * called "Guido" query set, whose scalability heavily hinges on band-joins.
 *
 * The (adapted) pf:text/pf:attribute for this would need a band-join to handle 
 * this and it would then make sense to switch to pre-sorted doubles with a 
 * delta table for updates, instead of a hash-table. 
 *
 * A small extension to the value=>double mapping (hashing?) would also permit
 * to index date/time values in such a way that range queries can be supported.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 *
 * $Id$
 */ 

#include "pathfinder.h"
#include "normalize.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "oops.h"
#include "abssyn.h"
#include "qname.h"
#include "mem.h"

/* some abssyn tree helper macros and constructor functions
 */
#define L(p)  ((p)->child[0])
#define R(p)  ((p)->child[1])
#define LL(p) L(L(p))
#define LR(p) R(L(p))
#define RL(p) L(R(p))
#define RR(p) R(R(p))

#define nil               p_leaf (p_nil, p->loc)
#define dot               p_leaf (p_dot, p->loc)
#define flwr(a,b)         p_wire2(p_flwr, p->loc, a, b)
#define let(a,b)          p_wire2(p_let, p->loc, a, b)
#define binds(a,b)        p_wire2(p_binds, p->loc, a, b) 
#define bind(a,b)         p_wire2(p_bind, p->loc, a, b)
#define vars(a)           p_wire2(p_vars, p->loc, var_type(a), nil)
#define var_type(a)       p_wire2(p_var_type, p->loc, a, nil)
#define req_ty(a,b)       p_wire2(p_req_ty, p->loc, a, b)
#define args(a,b)         p_wire2(p_args, p->loc, a, b)
#define where(a,b)        p_wire2(p_where, p->loc, a, b)  
#define locpath(a,b)      p_wire2(p_locpath, p->loc, a, b) 
#define if_then(a,b)      p_wire2(p_if, p->loc, a, b)  
#define then_else(a,b)    p_wire2(p_then_else, p->loc, a, b)  
#define instof(a,b)       p_wire2(p_instof, p->loc, a, b)  
#define ord_ret(a,b)      p_wire2(p_ord_ret, p->loc, a, b)  
#define pred(a,b)         p_wire2(p_pred, p->loc, a, b)  
#define is(a,b)           p_wire2(p_is, p->loc, a, b)  
#define eq(a,b)           p_wire2(p_eq, p->loc, a, b)  
#define some(a,b)         p_wire2(p_some, p->loc, a, b)  

/* these cannot be handled with a macro */
#define apply(x,y,a)      apply_(p->loc, x, y, a)
#define node_ty(x,y)      node_ty_(p->loc, x, y) 
#define var(x)            var_(p->loc, x) 
#define lit_str(x)        lit_str_(p->loc, x?x:"") 
#define step(a,b)         step_(p->loc, a, b) 
#define seq_ty(a)         seq_ty_(p->loc, a) 

static PFpnode_t* 
var_(PFloc_t loc, int varnum) 
{ 
    PFpnode_t *r = p_leaf(p_varref, loc);  
    if (r) {
        r->sem.qname_raw.prefix = "#pf";
        r->sem.qname_raw.loc = (char*) PFmalloc(8);
        if (r->sem.qname_raw.loc)
            snprintf(r->sem.qname_raw.loc, 8, "heur%03d", varnum);
    }
    return r; 
}

static PFpnode_t* 
node_ty_(PFloc_t loc, PFpkind_t kind, PFpnode_t *n) 
{ 
    PFpnode_t *r = p_wire1(p_node_ty, loc, n);
    if (r) r->sem.kind = kind; 
    return r; 
}

static PFpnode_t* 
lit_str_(PFloc_t loc, char* val) 
{ 
    PFpnode_t *r = p_leaf(p_lit_str, loc);  
    if (r) r->sem.str = val;
    return r; 
}

static PFpnode_t* 
step_(PFloc_t loc, PFpaxis_t axis, PFpnode_t *a) 
{ 
    PFpnode_t *r = p_wire1(p_step, loc, a);  
    if (r) r->sem.axis = axis;
    return r; 
}

static PFpnode_t* 
apply_(PFloc_t loc, char *ns, char* fcn, PFpnode_t *a) 
{ 
    PFpnode_t *r = p_wire1(p_fun_ref, loc, a);
    if (r) {
        r->sem.qname_raw.prefix = ns;
        r->sem.qname_raw.loc = fcn;
    }
    return r;
}

static PFpnode_t* 
seq_ty_(PFloc_t loc, PFpnode_t *a) { 
    PFpnode_t *r = p_wire1(p_seq_ty, loc, a);  
    if (r) r->sem.kind = p_kind_node;
    return r; 
}

#define cpy(cur)            cpy_subst(cur,NULL,NULL,NULL)
#define subst(cur,src,dst)  cpy_subst(cur,src,dst,NULL)
#define dot2var(cur,dst)    cpy_subst(cur,NULL,dst,NULL)

/* copy a abssyn tree, performing (possibly) some substitutions 
 */
static PFpnode_t *cpy_subst(PFpnode_t*, PFpnode_t *, PFpnode_t *, int*);
static PFpnode_t *
cpy_subst(PFpnode_t* cur, PFpnode_t* src, PFpnode_t* dst, int *haspath)
{
    int haspath1 = 0, haspath2 = 0, subst_dots = (dst && src == NULL); 
    PFpnode_t *n = (PFpnode_t*) PFmalloc(sizeof(PFpnode_t));

    if (cur == src) return dst; /* perform simple substitution */
    if (n) {
        *n = *cur;
        if (L(n)) L(n) = cpy_subst(L(n), src, dst, &haspath1);
        if (subst_dots && cur->kind == p_pred) dst = NULL;
        if (R(n)) R(n) = cpy_subst(R(n), src, dst, &haspath2);
    }
    if (haspath) { /* post traversal check whether we have seen a locpath */
        *haspath = haspath1 || haspath2 || (cur->kind == p_locpath);
        if (*haspath) subst_dots = 0;
    }
    /* special case: dot substitution */
    return (subst_dots && cur->kind == p_dot)?cpy(dst):n;
}

#define TST_ATTR  1 /* comparison with @attribute */
#define TST_TEXT  2 /* comparison with /text() */
#define TST_DATA  3 /* comparison with node (i.e. fn:data(.)) */

/* check+analyze the eq predicate to see if we can use it for indexed access 
 */
static int 
check_predicate(PFpnode_t *r, PFpnode_t **req_name)
{
    int tst = 0;
    PFpnode_t *n = r;
    if (r && r->kind == p_locpath  && L(r) && L(r)->kind == p_step) {

        /* check for 'PATH/text() = VAL' pattern */ 
        if (L(r)->sem.axis == p_child && LL(r) && 
            LL(r)->kind == p_node_ty &&
            LL(r)->sem.kind == p_kind_text) 
        {
            tst = TST_TEXT;
            n = R(r); /* for text(), the previous step matters for the qname */ 
        } 

        /* try to collect a qualified name for the destination nodes */
        if (L(n) && LL(n) && LL(L(n)) && LL(LL(n)) &&
            LL(n)->kind == p_node_ty && 
            LL(L(n))->kind == p_req_ty &&
            LL(LL(n))->kind == p_req_name) 
        {
            /* heuristic: attribute and fn:data() selections must have qname */
            if (!tst) {
                if (L(r)->sem.axis == p_attribute) {
                    tst = TST_ATTR; /* 'PATH/@attr = VAL' pattern */
                } else {
                    tst = TST_DATA; /* 'PATH = VAL' pattern, ie fn:data(PATH) */
                }
            }
            if (tst) *req_name = cpy(LL(LL(n)));
        }
    }
    return tst;
}

/* reverse axis mapping; 
 * - value 'p_attribute' is used to signal irreversible axes 
 * - some *very* crude 'benefit' cost metric to try to avoid
 *   reverse paths that are more expensive than the originals
 *   The benefits are multiples of 2, because benefits are slashed  
 *   in half for wildcards /step::* (we prefer /step::ns:loc).
 */
typedef struct {
 int benefit;
 PFpaxis_t id;
} rev_axis_t;

rev_axis_t rev_axis[] = {
    { -8, p_descendant },      
    { -8, p_descendant_or_self },   
    {  0, p_attribute },
    {  2, p_parent },
    {  8, p_ancestor },
    {  8, p_ancestor_or_self }, 
    { -8, p_preceding },
    { -4, p_preceding_sibling },
    { -2, p_child },
    { -8, p_following },
    { -4, p_following_sibling },
    {  0, p_self },
    {  0, p_attribute },
    {  0, p_attribute },
    {  0, p_attribute },
    {  0, p_attribute } 
};

#define skip_over_emptyseq(p) skip(p,0)
#define skip_to_locpath(p)    skip(p,1)
static PFpnode_t*
skip(PFpnode_t *p, int skippred) {
    while((p->kind == p_exprseq && R(p)->kind == p_empty_seq) ||
          (skippred && p->kind == p_pred)) p = L(p);
    return p;
}

static PFpnode_t *
revert_locpath(PFpnode_t **root, PFpnode_t* ctx, int gen_preds)
{
    PFpaxis_t axis = p_self;
    PFpnode_t *r = ctx;
    PFpnode_t *p = *root;
    PFpnode_t *c = NULL;
    int benefit = 0;

    while(p && L(p)) {
        if (c == NULL && p->kind == p_pred) {
            /* cannot use indices in case of positional predicates!*/
            if (!PFposition_safe_predicate(R(p), true)) return NULL; 

            c = R(p); /* save condition in c */
            p = skip_over_emptyseq(L(p)); 
            continue;
        }
        if (p->kind != p_locpath || L(p)->kind != p_step) break;

        rev_axis_t a = rev_axis[L(p)->sem.kind];
        if (a.id == p_attribute) return NULL;

        /* protect (a bit) against reverted locpaths that are 
           more expensive than the originals */
        int delta = a.benefit;
        if (LL(p)->kind == p_node_ty && LL(L(p)) &&
            LL(L(p))->kind == p_req_ty && LL(LL(p)) &&
            LL(LL(p))->kind == p_req_name && LL(LL(p))->sem.qname_raw.loc && 
            LL(LL(p))->sem.qname_raw.loc[0]) 
        {
           delta /= 2; /* specific path is cheaper than a wildcard path */
        }
        benefit += delta;

        /* for UP::x/self::x omit the first self::x */
        if (r != ctx || ctx->kind != p_locpath || axis != p_self) 
            r = locpath(step(axis, cpy(LL(p))), r);

        p = skip_over_emptyseq(R(p));
        if (c && gen_preds) { /* apply saved condition */
            r = pred(r, cpy(c));
            c = NULL;
        }
        axis = a.id;
    }
    if (benefit < 0) return NULL;

    /* finish the reverse path, return the path context expression in 'root' */
    *root = p;
    return locpath(step(axis, node_ty (gen_preds?p_kind_node:p_kind_elem, nil)), r);
}

/* check whether an expression does not use certain variables (directly)
 */
static int var_independent(PFpnode_t *p, PFpnode_t *ignore, PFpnode_t* binds);
static int
var_independent(PFpnode_t *p, PFpnode_t *ignore, PFpnode_t *binds) 
{
    if (p != ignore && p->kind == p_varref) {
        PFpnode_t* b;
        for(b = binds; b->kind == p_binds; b = R(b)) {
            PFpnode_t *v = (L(b)->kind == p_let)?LL(L(b)):LL(LL(b));
            if (!PFqname_raw_eq(p->sem.qname_raw, v->sem.qname_raw)) return 0;
        }
    }
    if (L(p) && !var_independent(L(p), ignore, binds)) return 0;
    if (R(p) && !var_independent(R(p), ignore, binds)) return 0;
    return 1;
}

/* look for a variable in a series of nested bindings 
 */
static PFpnode_t* 
var_findbind(PFpnode_t *v, PFpnode_t *EXPR2) {
    while(v && v->kind == p_binds) {
        if (L(v) && L(v)->kind == p_bind && 
            LL(v) && LL(v)->kind == p_vars && 
            LL(L(v)) && LL(L(v))->kind == p_var_type && 
            LL(LL(v)) && LL(LL(v))->kind == p_varref &&
            !PFqname_raw_eq(LL(LL(v))->sem.qname_raw, EXPR2->sem.qname_raw)) return v;
        v = R(v);
    }
    return NULL; 
}

/* main rewrite function
 */
static int
try_rewrite(PFpnode_t *p, PFpnode_t **stack, int depth, int curvar)
{
    /* my rewrite introduces 9 temporary variables */
    PFpnode_t *VAR1 = var(curvar+1);
    PFpnode_t *VAR2 = var(curvar+2);
    PFpnode_t *VAR3 = var(curvar+3);
    PFpnode_t *VAR4 = var(curvar+4);
    PFpnode_t *VAR5 = var(curvar+5);
    PFpnode_t *VAR6 = var(curvar+6);
    PFpnode_t *VAR7 = var(curvar+7);
    PFpnode_t *VAR8 = var(curvar+8);
    PFpnode_t *VAR9 = var(curvar+9);

    /* check txt/attr predicate and ensure it is the left child of eq */
    PFpnode_t *req_name = nil;
    int tst = check_predicate(L(p), &req_name);
    if (!tst) {
        tst = check_predicate(R(p), &req_name);
        if (tst) {
            PFpnode_t *swap = L(p);
            L(p) = R(p);
            R(p) = swap;
        } else {
           return 0;
        }
    }

    /* I do support conjunctions in PRED, EXPR1 */
    while(depth > 0 && stack[depth-1]->kind == p_and) depth--;

    /* right-hand side cannot refer to position(), first(), last() */
    if (!PFposition_safe_predicate(R(p), true)) return 0; 

    /* text steps need to go upwards from the text-nodes given by the idx */
    PFpnode_t *UP  = cpy(VAR4); 
    if (tst != TST_ATTR) {
        UP = locpath(step((tst == TST_TEXT)?p_parent:p_ancestor, 
                     node_ty(p_kind_elem, req_ty(req_name, nil))), UP);
    }

    /* declare the protagonist expressions in our patterns */
    PFpnode_t *BINDS = nil;
    PFpnode_t *VBIND = NULL;
    PFpnode_t *COND  = subst(stack[depth], R(p), cpy(VAR2));
    PFpnode_t *EXPR0 = NULL; 
    PFpnode_t *EXPR1 = R(p);
    PFpnode_t *EXPR2 = (tst == TST_DATA)?L(p):LR(p); 
    PFpnode_t *EXPR3 = nil; 
    PFpnode_t *PATH1_REV = NULL;
    PFpnode_t *PATH2_REV = revert_locpath(&EXPR2, UP, 0);
    if (PATH2_REV == NULL) return 0;

    if (stack[depth-1]->kind == p_exprseq && /* matches pattern (1) */
        stack[depth-2]->kind == p_pred && 
        EXPR2->kind == p_dot) 
    {
        EXPR0 = L(stack[depth-2]);
        EXPR2 = var(curvar+9);
        COND = dot2var(COND, VAR9); /* replace occurrences of '.' by VAR9 */
    } else if (EXPR2->kind == p_varref && /* matches pattern (2) */
               stack[depth-1]->kind == p_where && 
               stack[depth-2]->kind == p_flwr) 
    {
        /* for-loop may introduce other var bindings, look for ours (EXPR2) */
        BINDS = L(stack[depth-2]);
        VBIND = var_findbind(BINDS, EXPR2);
        if (VBIND && var_independent(stack[depth], EXPR2, VBIND)) {
            /* instead of $var9 use the original for-loop var $x (EXPR2) */
            VAR9->sem = EXPR2->sem;
            EXPR0 = LR(VBIND);
            EXPR2 = RR(stack[depth-1]);
            EXPR3 = RL(stack[depth-1]);
        }
    }
    if (EXPR0) { /* we have a pattern hit! */
        EXPR0 = skip_over_emptyseq(EXPR0);
        PATH1_REV = revert_locpath(&EXPR0, dot, 1); 
    }
    if (PATH1_REV) {
        /* get the parameters for the pf:text/pf:attribute call */
        PFpnode_t *arg = args(VAR1, nil);
        PFpnode_t *h = skip_to_locpath((LR(p)->kind == p_dot)?EXPR0:LR(p));
        char *prefix = "*", *loc = "*";
        
        if (h->kind == p_locpath && 
            L(h) && L(h)->kind == p_step &&
            LL(h) && LL(h)->kind == p_node_ty &&
            L(LL(h)) && L(LL(h))->kind == p_req_ty &&
            LL(LL(h)) && LL(LL(h))->kind == p_req_name)
        {
            /* get the element type just before the text/attribute step */
            loc = LL(LL(h))->sem.qname_raw.loc;
            prefix = LL(LL(h))->sem.qname_raw.prefix;
        }
        if (tst == TST_ATTR) {  
            /* pf:attribute() has additional parameters wrt pf:text() */
            arg = args(lit_str(req_name->sem.qname_raw.loc), 
                       args(lit_str(req_name->sem.qname_raw.prefix), 
                            args(lit_str(loc), 
                                 args(lit_str(prefix), arg))));
        }
 
        /* create the bindings of our new enclosing flwr block */
        h = VBIND?VBIND:BINDS;         /* h points into the BIND list at $x */
        if (VBIND) VBIND = cpy(VBIND); /* VBIND contains all bindings from $x on */

        *h = /* overwrite $x, thus replace it and further binds by our new vars */
        *binds(
           let(var_type(cpy(VAR1)),EXPR0),
          binds(
           let(var_type(cpy(VAR2)),
               flwr(binds(bind(vars(cpy(VAR3)),EXPR1), nil),
               where(nil, ord_ret(nil,
                     if_then(instof(cpy(VAR3), seq_ty(node_ty(p_kind_node, nil))),
                     then_else(apply("fn", "data", args(cpy(VAR3), nil)),
                               cpy(VAR3))))))),
          binds(
           let(var_type(cpy(VAR4)),
               flwr(binds(bind(vars(cpy(VAR5)), cpy(VAR2)), nil),
               where(nil, ord_ret(nil,
                     apply("pf",(tst == TST_ATTR)?"attribute":"text", 
                           args(apply("fn", "string", args(cpy(VAR5), nil)),
                                arg)))))), nil)));


        /* pattern(2): put in VBIND any remaining variable bindings after $x */
        if (VBIND) { 
            BINDS = cpy(BINDS); 
            *L(stack[depth-2]) = *VBIND; /* replace binds in orig plan to $x and beyond only */
            VBIND = cpy(R(VBIND));
        }

        /* creatively edited 'pf -Pas10' of target pattern */
        *stack[depth-2] =
        *flwr(BINDS,
         where(nil, 
         ord_ret(nil,
              if_then(some(binds(bind(vars(cpy(VAR6)), cpy(VAR4)), nil),
                      instof(cpy(VAR6), seq_ty(node_ty(p_kind_doc, nil)))),
              then_else(
                  /* original plan */
                  subst(stack[depth-2], R(p), cpy(VAR2)),
                  /* index plan */
                  flwr(binds(bind(vars(cpy(VAR9)),
                             pred(PATH2_REV,
                                  some(binds(bind(vars(cpy(VAR7)),
                                                  apply("pf", "supernode",
                                                         args(PATH1_REV, nil))),
                                                     nil),
                                  some(binds(bind(vars(cpy(VAR8)),
                                                  cpy(VAR1)), nil),
                                  is(cpy(VAR7), cpy(VAR8)))))), 
                             VBIND?VBIND:nil), /* rest of binds, after $x */
                  where(COND,
                  ord_ret(cpy(EXPR3), cpy(EXPR2)))))))));

        return depth-2;
    }
    return 0;
}

#define HEURISTIC_MAXDEPTH 1023

/**
 * The heuristic index selection push-down rewrite.
 *
 * We look leftdeep from the root for a string equality predicate.
 * To maintain context, we keep a traversal stack (node + child pointer). 
 *
 * The single left-deep traversal ensures that *if* we substitute a predicate 
 * using our heuristics,the pasted-in result pattern will not be checked again 
 * for a potential match. This is fast and prevents an endless matching loop.
 * 
 * @param root root of the abstract syntax tree 
 * @return the rewritten abstract syntax tree 
 */
 
PFpnode_t* 
PFheuristic_index (PFpnode_t *root)
{
    PFpnode_t *stack[HEURISTIC_MAXDEPTH+1];
    char child[HEURISTIC_MAXDEPTH+1];
    int curvar = 0, depth = 0;
    stack[0] = root;
    child[0] = 0;

    while(depth >= 0 && depth < HEURISTIC_MAXDEPTH) {
        int next_child = child[depth]++;
        PFpnode_t *r = stack[depth]; 

        if (r && next_child < 2) { /* PUSH child left-deep-first */
            stack[++depth] = r->child[next_child]; 
            child[depth] = 0; 
        } else {
            if (r && r->kind == p_eq && depth > 2) { /* now try to rewrite */
                int rewritten = try_rewrite(r, stack, depth, curvar);
                if (rewritten) { depth = rewritten; curvar += 9; }
            } 
            depth--; /* POP */
        }
    }
    return root;
}
/* vim:set shiftwidth=4 expandtab filetype=c: */
