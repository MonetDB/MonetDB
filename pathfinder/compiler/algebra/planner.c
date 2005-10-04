/**
 * @file
 *
 * Compile a logical algebra tree into a physical execution plan. 
 *
 * @section planning Physical Plan Generation
 *
 * For any algebra subexpression, we derive all plans that appear
 * interesting so far. We consider a plan to be interesting if
 *
 *  - There is no other plan that returns the same (or better)
 *    ordering in a cheaper way.
 *
 * For each logical algebra subexpression, we thus return a
 * @em set of plans, each plan with another ordering.  At the
 * end, we pick the cheapest of all the plans we found for the
 * input query.
 *
 * This approach is a rather classical one.  However, in contrast
 * to classical relational systems, we do @em not operate on
 * algebra @em trees.  The input for our planner is a directed
 * acyclic graph (DAG).  While this is not a critical issue for
 * ``normal'' expressions (the worst case would be that we
 * unneccessarily compute a subexpression result twice), we
 * may run into trouble with expressions that contribute new
 * live node sets (document table access, node construction, etc.)
 *
 * @section live_nodes Live Node Set Handling
 *
 * In contrast to a standard relational algebra, our algebra
 * is not completely side-effect free.  Side-effects originate
 * in XQuery's node construction operators that introduce
 * side-effects in XQuery.
 *
 * In particular, we must evaluate expressions that construct
 * nodes, exactly as many times as they appear in the original
 * query.  In our logical algebra, this is reflected by the way
 * we generate the algebra DAGs:
 *
 *  - Generate an expression tree for each node constructor.
 *    In case an expression result is assigned to a variable
 *    that occurs several times, all these occurrences
 *    reference the same algebra expression tree (implemented
 *    as a C pointer).
 *    .
 *  - Two algebra expressions that construct nodes are @em never
 *    considered identical during algebra CSE.  This means that
 *    subexpressions that construct nodes are shared iff created
 *    as described in the above point, i.e., if they stem from
 *    the same XQuery subexpression.
 *
 * Node construction expressions may be referenced several times
 * in the logical tree.  They will at least be referenced by
 * a @c FRAGs and a @c ROOTS node that extract the two sub-results
 * (the newly introduced live node set and the logical query
 * result, respectively).
 *
 * If we produced multiple plans for one construction operator,
 * the different references might pick different plans, thus
 * evaluating the construction more than once.  To guarantee
 * correct consideration of side-effects, we thus produce
 * exactly one plan for each construction operator.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include "oops.h"

#include <assert.h>
#include <stdio.h>

#include "planner.h"

/* We need PFmalloc. */
#include "mem.h"

/* We use array to handle lists of plan candidates. */
#include "array.h"

/* We compile from logical algebra into physical algebra. */
#include "logical.h"
#include "physical.h"
#include "physical_mnemonic.h"

/* We will need a notion of ``ordering''. */
#include "ordering.h"

/* short-hands */
#define L(p) ((p)->child[0])
#define R(p) ((p)->child[1])

/**
 * A ``plan'' is actually a physical algebra operator tree.
 */
typedef PFpa_op_t plan_t;

/* ensure some ordering on a given plan */
static PFplanlist_t *ensure_ordering (const plan_t *unordered,
                                      PFord_ordering_t required);
/* test if a is cheaper than b (only look at costs) */
static bool costless (const plan_t *a, const plan_t *b);
/* test if a is a better plan than b (look at orderings and costs) */
static bool better_or_equal (const plan_t *a, const plan_t *b);
/* given a list of plans, prune all plans that are not interesting */
static PFplanlist_t *prune_plans (PFplanlist_t *planlist);

/**
 * Create a new (empty) plan list.
 *
 * When searching for plans for some logical algebra subtree,
 * we start with an empty list of possible plans. We then add
 * each plan we find to this list.
 */
static PFplanlist_t *
new_planlist (void)
{
    return PFarray (sizeof (plan_t *));
}

/**
 * Add a plan to a plan list.
 *
 * We collect each ``interesting'' plan we find in a list of
 * plans that we will return as the compilation result for a
 * logical algebra subexpression.
 */
static void
add_plan (PFplanlist_t *list, const plan_t *plan)
{
    *((plan_t **) PFarray_add (list)) = (plan_t *) plan;
}

/**
 * Add a set of plans to a plan list.
 *
 * We collect each ``interesting'' plan we find in a list of
 * plans that we will return as the compilation result for a
 * logical algebra subexpression.
 */
static void
add_plans (PFplanlist_t *list, PFplanlist_t *plans)
{
    PFarray_concat (list, plans);
}


/**
 * Generate possible physical plans for literal table construction.
 *
 * There's actually just the one literal table constructor in
 * our physical algebra.  In some cases, however, we can avoid
 * constructing the table at all, e.g. by using the `attach'
 * operator.
 *
 * The literal table operator will try to infer some orderings
 * from the input tuples (see #PFpa_lit_tbl).
 */
static PFplanlist_t *
plan_lit_tbl (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();
    plan_t        *plan = NULL;

    PFalg_attlist_t attlist;

    attlist.count = n->schema.count;
    attlist.atts  = PFmalloc (attlist.count * sizeof (*attlist.atts));

    for (unsigned int i = 0; i < attlist.count; i++)
        attlist.atts[i] = n->schema.items[i].name;

    /*
     * There is exactly this one plan.
     */
    plan = lit_tbl (attlist, n->sem.lit_tbl.count, n->sem.lit_tbl.tuples);

    /* Add this plan to the return set of plans. */
    add_plan (ret, plan);

    return ret;
}

/**
 * Generate physical plan for the logical `project' operator.
 *
 * There's just one physical operator for `project' that we
 * call from here.  The resulting plans will contain all
 * those orderings of @a n (or prefixes thereof), for which
 * we still have all attributes left in the projection result.
 */
static PFplanlist_t *
plan_project (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_project);
    assert (n->child[0]); assert (n->child[0]->plans);

    /* consider each plan in R */
    for (unsigned int r = 0; r < PFarray_last (n->child[0]->plans); r++)
        add_plan (ret,
                  project (*(plan_t **) PFarray_at (n->child[0]->plans, r),
                           n->sem.proj.count, n->sem.proj.items));

    return ret;
}

/**
 * Worker for plan_cross(): adds plans for @a a x @a b to the
 * plan list @a ret.  In addition to the standard cross product,
 * this function also considers the case when @a a is a literal
 * table. In that case, the cross product may be implemented
 * @em much cheaper with the ColumnAttach operator.
 */
static void
cross_worker (PFplanlist_t *ret, const plan_t *a, const plan_t *b)
{
    /* add the cross product plan */
    add_plan (ret, cross (a, b));

    /* if a is a literal table, consider the ColumnAttach operator */
    if (a->kind == pa_lit_tbl && a->sem.lit_tbl.count == 1) {

        PFalg_tuple_t  t    = a->sem.lit_tbl.tuples[0];
        plan_t        *plan = (plan_t *) b;

        for (unsigned int i = 0; i < t.count; i++) {
            plan = attach (plan, a->schema.items[i].name, t.atoms[i]);
        }

        add_plan (ret, plan);
    }
}

/**
 * Generate possible physical plans for cross product.
 *
 * For the logical operation R x S, this will return
 * both combinations, R x S and S x R, for the physical plan.
 *
 * If relations R and S follow the orderings O_r and O_s, 
 * R x S will have the ordering O_r + O_s (see #PFpa_cross).
 */
static PFplanlist_t *
plan_cross (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_cross);
    assert (n->child[0]); assert (n->child[0]->plans);
    assert (n->child[1]); assert (n->child[1]->plans);

    /* combine each plan in R with each plan in S */
    for (unsigned int r = 0; r < PFarray_last (n->child[0]->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (n->child[1]->plans); s++) {
            cross_worker (ret, *(plan_t **) PFarray_at (n->child[0]->plans, r),
                               *(plan_t **) PFarray_at (n->child[1]->plans, s));
            cross_worker (ret, *(plan_t **) PFarray_at (n->child[1]->plans, s),
                               *(plan_t **) PFarray_at (n->child[0]->plans, r));
        }

    return ret;
}

/**
 * Worker for plan_eqjoin().
 */
static void
join_worker (PFplanlist_t *ret,
             PFalg_att_t att1, PFalg_att_t att2,
             const plan_t *a, const plan_t *b)
{
    /*
     * try ``standard'' EqJoin, which does not give us any
     * ordering guarantees.
     */
    add_plan (ret, eqjoin (att1, att2, a, b));

    /*
     * LeftJoin may be more expensive, but gives us some ordering
     * guarantees.
     */
    add_plan (ret, leftjoin (att1, att2, a, b));
    add_plan (ret, leftjoin (att2, att1, b, a));

#if 0
    PFalg_att_t    att_a = NULL;
    PFalg_att_t    att_b = NULL;
    PFplanlist_t  *sorted_a;
    PFplanlist_t  *sorted_b;

    /* We can always apply the nested loops join */
    add_plan (ret, nljoin (att1, att2, a, b));

    /* test which of the two attributes is in which argument */
    for (unsigned int i = 0; i < a->schema.count; i++)
        if (a->schema.items[i].name == att1) {
            att_a = att1;
            break;
        }
        else if (a->schema.items[i].name == att2) {
            att_a = att2;
            break;
        }

    for (unsigned int i = 0; i < b->schema.count; i++)
        if (b->schema.items[i].name == att1) {
            att_b = att1;
            break;
        }
        else if (b->schema.items[i].name == att2) {
            att_b = att2;
            break;
        }

    assert (att_a && att_b);

    /* We can use MergeJoin after sorting both arguments on the
     * join attributes.
     */
    sorted_a = prune_plans (
                   ensure_ordering (a, PFord_refine (PFordering (), att_a)));
    sorted_b = prune_plans (
                   ensure_ordering (b, PFord_refine (PFordering (), att_b)));

    /* apply MergeJoin for all the resulting plans */
    for (unsigned int i = 0; i < PFarray_last (sorted_a); i++)
        for (unsigned int j = 0; j < PFarray_last (sorted_a); j++)
            add_plan (ret, merge_join (att_a, att_b,
                                       *(plan_t **) PFarray_at (sorted_a, i),
                                       *(plan_t **) PFarray_at (sorted_b, j)));
#endif
}

/**
 * Generate physical plans for equi-join.
 */
static PFplanlist_t *
plan_eqjoin (const PFla_op_t *n)
{
    PFplanlist_t  *ret       = new_planlist ();

    assert (n); assert (n->kind == la_eqjoin);
    assert (n->child[0]); assert (n->child[0]->plans);
    assert (n->child[1]); assert (n->child[1]->plans);

    /* combine each plan in R with each plan in S */
    for (unsigned int r = 0; r < PFarray_last (n->child[0]->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (n->child[1]->plans); s++) {
            join_worker (ret, n->sem.eqjoin.att1, n->sem.eqjoin.att2,
                              *(plan_t **) PFarray_at (n->child[0]->plans, r),
                              *(plan_t **) PFarray_at (n->child[1]->plans, s));
            join_worker (ret, n->sem.eqjoin.att1, n->sem.eqjoin.att2,
                              *(plan_t **) PFarray_at (n->child[1]->plans, s),
                              *(plan_t **) PFarray_at (n->child[0]->plans, r));
        }

    return ret;
}

/**
 * Helper function: Determine type of attribute @a att in schema
 * @a schema. Used by plan_disjunion().
 */
static PFalg_simple_type_t
type_of (PFalg_schema_t schema, PFalg_att_t att)
{
    for (unsigned int i = 0; i < schema.count; i++)
        if (schema.items[i].name == att)
            return schema.items[i].type;

    PFoops (OOPS_FATAL, "unable to find attribute %s in schema",
            PFatt_print (att));

    return 0;  /* pacify compilers */
}

/**
 * Generate physical plans for disjoint union.
 *
 * Available implementations are MergeUnion and AppendUnion.
 * MergeUnion is order-aware.  It expects both input relations
 * to be in some given ordering, and will propagate that ordering
 * to its output.  For two equal values in the ordering attribute,
 * MergeUnion will pick tuples from the left relation first, then
 * from the right relation.
 *
 * The alternative is AppendUnion, which simply appends two
 * relations.  AppendUnion is slightly cheaper and does not
 * require any specific input ordering.  Its output ordering,
 * however, is typically not valuable.
 *
 * @bug
 *   Due to the restrictions that MonetDB's binary table model
 *   gives us, we only support orderings on a single column in
 *   MergeUnion.
 */
static PFplanlist_t *
plan_disjunion (const PFla_op_t *n)
{
    PFplanlist_t  *ret       = new_planlist ();

    /* Consider each combination of plans in R and S */
    for (unsigned int r = 0; r < PFarray_last (n->child[0]->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (n->child[1]->plans); s++) {

            plan_t *R = *(plan_t **) PFarray_at (n->child[0]->plans, r);
            plan_t *S = *(plan_t **) PFarray_at (n->child[1]->plans, s);

            /* common orderings of R and S */
            PFord_set_t common
                = PFord_unique (PFord_intersect (R->orderings, S->orderings));

            /*
             * unfortunately, we can only support MergeJoin with
             * one-column orderings in our MIL translation (and it
             * even has to be monomorphic).
             */
            PFord_set_t prefixes = PFord_set ();

            for (unsigned int i = 0; i < PFord_set_count (common); i++) {

                PFalg_att_t att = PFord_order_at (PFord_set_at (common, i), 0);

                PFalg_simple_type_t tyR = type_of (R->schema, att);
                PFalg_simple_type_t tyS = type_of (S->schema, att);

                if (tyR == tyS
                    && (tyR == aat_nat || tyR == aat_int || tyR == aat_str
                        || tyR == aat_dec || tyR == aat_dbl))

                    PFord_set_add (prefixes, PFord_refine (PFordering (), att));
            }

            /* kick out the duplicates */
            prefixes = PFord_unique (prefixes);
                
            /* and generate plans */
            for (unsigned int i = 0; i < PFord_set_count (prefixes); i++)
                add_plan (ret, merge_union (R, S, PFord_set_at (prefixes, i)));

            /* of course we can always do AppendUnion */
            add_plan (ret, append_union (R, S));

#if 0
            /* compute common prefixes of orderings in R and S */
            PFord_set_t prefixes
                = PFord_prefixes (
                        PFord_unique (
                            PFord_intersect (R->orderings, S->orderings)));

            for (unsigned int i = 0; i < PFord_set_count (prefixes); i++)
                add_plan (ret, merge_union ( R, S, PFord_set_at (prefixes, i)));
#endif
        }

    return ret;
}


/**
 * Return possible plans that guarantee the result from @a unordered
 * to be ordered according to @a required.
 *
 * We have three choices:
 *  - If a plan already fulfills @a required, we can just copy it
 *    to the result plan list.
 *  - If a plan partially fulfills @a required (i.e., it has a
 *    common ordering prefix with @a required), we can apply
 *    RefineSort that is much cheaper than our last-resort operator
 *    StdSort.
 *  - If we cannot benefit from any existing order, we apply
 *    StdSort.
 *
 * This function plans all those choices (if applicable).  The
 * subsequent prune_plans() call from plan_subexpression() will
 * remove inefficient plans.
 */
static PFplanlist_t *
ensure_ordering (const plan_t *unordered, PFord_ordering_t required)
{
    PFplanlist_t *ret           = new_planlist ();

    /* the best suitable ordering in `unordered' we found so far */
    PFord_ordering_t best_order = PFordering ();

    /*
     * See if we can benefit from some existing sorting
     */
    for (unsigned int j = 0; j < PFord_set_count (unordered->orderings); j++) {

        /* the existing order we currently look at */
        PFord_ordering_t existing = PFord_set_at (unordered->orderings, j);
        PFord_ordering_t prefix   = PFordering ();

        /* collect the common prefix of required and existing ordering */
        for (unsigned int k = 0;
             k < PFord_count (required) && k < PFord_count (existing)
             && PFord_order_at (required, k) == PFord_order_at (existing, k);
             k++)
            prefix = PFord_refine (prefix, PFord_order_at (existing, k));

        /*
         * If this prefix is better that what we already found
         * so far, remember it.
         */
        if (PFord_count (prefix) > PFord_count (best_order))
            best_order = prefix;

        /*
         * If this existing ordering already implies the required
         * ordering, we reached the optimum we can get and thus
         * stop searching for better prefixes.
         */
        if (PFord_implies (existing, required))
            break;
    }

    /*
     * If the input already provides the order we want,
     * just add the input to the plan list as it is.
     */
    if (PFord_implies (best_order, required))
        add_plan (ret, unordered);

    /*
     * The ``best ordering'' of `unordered' is now in `best_order'
     * (i.e., the longest common prefix of an existing ordering in
     * `unordered' and the required ordering.  We can use that to
     * apply RefineSort.
     */
    if (PFord_count (best_order) > 0)
        add_plan (ret, refine_sort (unordered, best_order, required));

    /* We can always apply StandardSort */
    add_plan (ret, std_sort (unordered, required));

    return ret;
}


/**
 * Generate physical plans for the logical `rownum' operator.
 *
 * The logical `rownum' operator is a rather complex operator.  In
 * our physical algebra, we make it more explicit and separate its
 * two processing steps:
 *
 *  1. Sort the input relation in a useful manner.  We can easily
 *     do this here with the help of ensure_ordering().
 *  2. On sorted input, row number generation is rather easy.  Still,
 *     we have two choices left:
 *      - `MergeRowNumber' can apply row numbers in a single scan,
 *        with no buffer requirements.  For this, we require a
 *        primary sorting on the rownum operator's partitioning
 *        argument. (We enforce that with ensure_ordering() here.)
 *      - `HashRowNumber' uses a hash table to do rownum's
 *        partitioning.  This requires only a small amount of
 *        memory, but has less requirements on the input ordering.
 *
 * We currently only consider the MergeRowNumber implementation.
 * HashRowNumber requires the input to be sorted according to the
 * rownum parameter within the partitions also given as the rownum
 * parameters.  We haven't implemented the full order framework
 * yet to describe this sort of ordering information, so we ignore
 * HashRowNumber for now.
 */
static PFplanlist_t *
plan_rownum (const PFla_op_t *n)
{
    PFplanlist_t *ret    = new_planlist ();
    PFplanlist_t *sorted = new_planlist ();
    PFord_ordering_t ord;

    assert (n); assert (n->kind == la_rownum);
    assert (n->child[0]); assert (n->child[0]->plans);

    /*
     * Build up the ordering that we require for MergeRowNumber
     */
    ord = PFordering ();

    /* the partitioning attribute must be the primary ordering */
    if (n->sem.rownum.part)
        ord = PFord_refine (ord, n->sem.rownum.part);

    /* then we refine by all the attributes in the sortby parameter */
    for (unsigned int i = 0; i < n->sem.rownum.sortby.count; i++)
        ord = PFord_refine (ord, n->sem.rownum.sortby.atts[i]);

    /* ensure correct input ordering for MergeRowNumber */
    for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (n->child[0]->plans, i), ord));

    /* throw out those plans that are too expensive */
    sorted = prune_plans (sorted);

    /* for each remaining plan, generate a MergeRowNumber operator */
    for (unsigned int i = 0; i < PFarray_last (sorted); i++)
        for (unsigned int j = 0; j < PFarray_last (n->child[0]->plans); j++)
            add_plan (ret,
                      merge_rownum (*(plan_t **) PFarray_at (sorted, i),
                                    n->sem.rownum.attname, n->sem.rownum.part));

    return ret;
}

/**
 * Create physical plan for Distinct operator (duplicate elimination).
 *
 * AFAIK, MonetDB only can do SortDistinct, i.e., sort its input, then
 * use the sort information to eliminate the duplicates.  The plans we
 * build are currently brute-force: generate all possible permutations
 * for orderings on the input relation, sort according to them, then
 * do duplicate elimination.
 */
static PFplanlist_t *
plan_distinct (const PFla_op_t *n)
{
    PFplanlist_t      *ret = new_planlist ();
    PFord_ordering_t   ord = PFordering ();
    PFord_set_t        perms;
    PFplanlist_t      *sorted;

    for (unsigned int i = 0; i < n->schema.count; i++)
        ord = PFord_refine (ord, n->schema.items[i].name);

    perms = PFord_permutations (ord);

    /* consider all possible orderings (permutations) */
    for (unsigned int p = 0; p < PFord_set_count (perms); p++) {

        sorted = new_planlist ();

        /* consider all input plans and sort them */
        for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
            add_plans (sorted,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (n->child[0]->plans, i),
                           PFord_set_at (perms, p)));

        /* throw away those that are too expensive */
        sorted = prune_plans (sorted);

        /* on each of the remaining, do a SortDistinct */
        for (unsigned int i = 0; i < PFarray_last (sorted); i++)
            add_plan (ret,
                      sort_distinct (*(plan_t **) PFarray_at (sorted, i),
                                     PFord_set_at (perms, p)));
    }

    return ret;
}

/**
 * Create physical operator that allows the access to the
 * string content of the loaded documents
 */ 
static PFplanlist_t *
plan_doc_access (const PFla_op_t *n)
{
    PFplanlist_t *ret    = new_planlist ();

    assert (n); assert (n->kind == la_doc_access);
    assert (n->child[0]); assert (n->child[0]->plans);
    assert (n->child[1]); assert (n->child[1]->plans);

    /* for each plan, generate a doc_access operator */
    for (unsigned int i = 0; i < PFarray_last (n->child[1]->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (n->child[0]->plans); j++)
            add_plan (ret,
                      doc_access (
                          *(plan_t **) PFarray_at (n->child[0]->plans, j),
                          *(plan_t **) PFarray_at (n->child[1]->plans, i),
                          n->sem.doc_access.att,
                          n->sem.doc_access.doc_col));

    return ret;
}

/**
 * Create physical equivalent for the string_join operator
 * (concatenates sets of strings using a seperator for each set)
 *
 * The string_join operator expects its first argument (strings)
 * sorted by iter|pos and its second argument (seperators) sorted
 * by iter. The output is sorted by iter|item.
 */
static PFplanlist_t *
plan_string_join (const PFla_op_t *n)
{
    PFplanlist_t *ret       = new_planlist ();
    PFplanlist_t *sorted_n1 = new_planlist ();
    PFplanlist_t *sorted_n2 = new_planlist ();

    assert (n); assert (n->kind == la_string_join);
    assert (n->child[0]); assert (n->child[0]->plans);
    assert (n->child[1]); assert (n->child[1]->plans);

    /* The string_join operator requires its inputs to be properly sorted. */
    for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
        add_plans (sorted_n1,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (n->child[0]->plans, i),
                       PFord_refine (PFord_refine (PFordering (), att_iter),
                                     att_pos)));

    for (unsigned int i = 0; i < PFarray_last (n->child[1]->plans); i++)
        add_plans (sorted_n2,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (n->child[1]->plans, i),
                       PFord_refine (PFordering (), att_iter)));

    /* for each remaining plan, generate a string_join operator */
    for (unsigned int i = 0; i < PFarray_last (sorted_n1); i++)
        for (unsigned int j = 0; j < PFarray_last (sorted_n2); j++)
            add_plan (ret,
                      serialize (
                          *(plan_t **) PFarray_at (sorted_n1, i),
                          *(plan_t **) PFarray_at (sorted_n2, j)));
    return ret;
}

/**
 * Create physical equivalent of our `serialize' operator.
 *
 * The physical Serialize operator expects its input to be sorted
 * by iter|pos (actually, iter should be constant).
 */
static PFplanlist_t *
plan_serialize (const PFla_op_t *n)
{
    PFplanlist_t *ret    = new_planlist ();
    PFplanlist_t *sorted = new_planlist ();

    assert (n); assert (n->kind == la_serialize);
    assert (n->child[0]); assert (n->child[0]->plans);
    assert (n->child[1]); assert (n->child[1]->plans);

    /* The serialize operator requires its input to be properly sorted. */
    for (unsigned int i = 0; i < PFarray_last (n->child[1]->plans); i++)
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (n->child[1]->plans, i),
                       PFord_refine (PFord_refine (PFordering (), att_iter),
                                     att_pos)));

    /* throw out those plans that are too expensive */
    sorted = prune_plans (sorted);

    /* for each remaining plan, generate a Serialize operator */
    for (unsigned int i = 0; i < PFarray_last (sorted); i++)
        for (unsigned int j = 0; j < PFarray_last (n->child[0]->plans); j++)
            add_plan (ret,
                      serialize (
                          *(plan_t **) PFarray_at (n->child[0]->plans, j),
                          *(plan_t **) PFarray_at (sorted, i)));

    return ret;
}

/**
 * Generate physical plan for document table access.
 *
 * @note
 *   Following the ideas in @ref live_nodes, we generate exactly
 *   one plan for this operator.  (Document table access is not
 *   precisely an operator with side-effects.  But it returns a
 *   live node set and we treat it analogously.)
 */
static PFplanlist_t *
plan_doc_tbl (const PFla_op_t *n)
{
    PFplanlist_t  *ret                = new_planlist ();
    PFplanlist_t  *ordered            = new_planlist ();
    plan_t        *cheapest_unordered = NULL;
    plan_t        *cheapest_ordered   = NULL;

    /* find the cheapest plan for our argument */
    for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
        if (!cheapest_unordered
            || costless (*(plan_t **) PFarray_at (n->child[0]->plans, i),
                         cheapest_unordered))
            cheapest_unordered
                = *(plan_t **) PFarray_at (n->child[0]->plans, i);

    /* an ordering by `iter' is typically helpful -> sort all plans */
    for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
        add_plans (ordered,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (n->child[0]->plans, i),
                       PFord_refine (PFordering (), att_iter)));

    for (unsigned int i = 0; i < PFarray_last (ordered); i++)
        if (!cheapest_ordered
            || costless (*(plan_t **) PFarray_at (ordered, i),
                         cheapest_ordered))
            cheapest_ordered = *(plan_t **) PFarray_at (ordered, i);

    /*
     * The plan `cheapest_unordered' is always the cheapest possible
     * plan.  However, an ordered plan could still be benefitial,
     * even if it is slightly more expensive on its own.  We assume
     * that the ordering makes up the cost of an unordered plan, if
     * its cost is at most 1.5 times the cost of an unordered plan.
     */
    if (cheapest_ordered->cost * 3 <= cheapest_unordered->cost * 2)
        add_plan (ret, doc_tbl (cheapest_ordered));
    else
        add_plan (ret, doc_tbl (cheapest_unordered));

    return ret;
}

/**
 * Helper function to plan binary operators (arithmetic, comparison,
 * Boolean). If either operand column is known to be constant, plan
 * for a more specific implementation.
 */
static PFplanlist_t *
plan_binop (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    static PFpa_op_t * (*op_atom[]) (const PFpa_op_t *, const PFalg_att_t,
                                     const PFalg_att_t, const PFalg_atom_t)
        = {
              [la_num_add]      = PFpa_num_add_atom
            , [la_num_multiply] = PFpa_num_mult_atom
            , [la_num_eq]       = PFpa_eq_atom
            , [la_num_subtract] = PFpa_num_sub_atom
            , [la_num_divide]   = PFpa_num_div_atom
            , [la_num_modulo]   = PFpa_num_mod_atom
            , [la_num_gt]       = PFpa_gt_atom
        };

    static PFpa_op_t * (*op[]) (const PFpa_op_t *, const PFalg_att_t,
                                const PFalg_att_t, const PFalg_att_t)
        = {
              [la_num_add]      = PFpa_num_add
            , [la_num_multiply] = PFpa_num_mult
            , [la_num_eq]       = PFpa_eq
            , [la_num_subtract] = PFpa_num_sub
            , [la_num_divide]   = PFpa_num_div
            , [la_num_modulo]   = PFpa_num_mod
            , [la_num_gt]       = PFpa_gt
        };


    switch (n->kind) {

        case la_num_add:
        case la_num_multiply:
        case la_num_eq:

            assert (op_atom[n->kind]);

            /* consider NumAddConst if attribute att1 is known to be constant */
            if (PFprop_const (n->child[0]->prop, n->sem.binary.att1))
                for (unsigned int i = 0;
                        i < PFarray_last (n->child[0]->plans); i++)
                    add_plan (ret,
                        op_atom[n->kind] (
                            *(plan_t **) PFarray_at (n->child[0]->plans,i),
                            n->sem.binary.res,
                            n->sem.binary.att2,
                            PFprop_const_val (n->child[0]->prop,
                                              n->sem.binary.att1)));

            /* fall through */

        case la_num_subtract:
        case la_num_divide:
        case la_num_modulo:
        case la_num_gt:

            assert (op_atom[n->kind]);

            /* consider NumAddConst if attribute att2 is known to be constant */
            if (PFprop_const (n->child[0]->prop, n->sem.binary.att2))
                for (unsigned int i = 0;
                        i < PFarray_last (n->child[0]->plans); i++)
                    add_plan (ret,
                        op_atom[n->kind] (
                            *(plan_t **) PFarray_at (n->child[0]->plans,i),
                            n->sem.binary.res,
                            n->sem.binary.att1,
                            PFprop_const_val (n->child[0]->prop,
                                              n->sem.binary.att2)));

            /* fall through */

        default:

            assert (op[n->kind]);

            for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
                add_plan (ret,
                        op[n->kind] (
                            *(plan_t **) PFarray_at (n->child[0]->plans, i),
                            n->sem.binary.res,
                            n->sem.binary.att1,
                            n->sem.binary.att2));
    }

    return ret;
}

/**
 * Helper function to plan unary operators (numeric/Boolean negation)
 */
static PFplanlist_t *
plan_unary (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
        switch (n->kind) {

            case la_num_neg:
                add_plan (ret,
                          num_neg (
                            *(plan_t **) PFarray_at (n->child[0]->plans, i),
                            n->sem.unary.res, n->sem.unary.att));
                break;

            case la_bool_not:
                add_plan (ret,
                          bool_not (
                            *(plan_t **) PFarray_at (n->child[0]->plans, i),
                            n->sem.unary.res, n->sem.unary.att));
                break;

            default:
                PFoops (OOPS_FATAL, "error in plan_unary");
        }

    return ret;
}

/**
 * Generate physical plan for the logical `cast' operator.
 *
 * There's just one physical operator for `cast' that we
 * call from here.  The resulting plans will contain all
 * orderings of @a n.
 */
static PFplanlist_t *
plan_cast (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_cast);
    assert (n->child[0]); assert (n->child[0]->plans);

    /* consider each plan in R */
    for (unsigned int r = 0; r < PFarray_last (n->child[0]->plans); r++)
        add_plan (ret,
                  cast (*(plan_t **) PFarray_at (n->child[0]->plans, r),
                        n->sem.cast.att,
                        n->sem.cast.ty));

    return ret;
}

/**
 * Generate physical plan for the logical `select' operator.
 *
 * There's just one physical operator for `select' that we
 * call from here.  The resulting plans will contain all
 * orderings of @a n.
 */
static PFplanlist_t *
plan_select (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_select);
    assert (n->child[0]); assert (n->child[0]->plans);

    /* consider each plan in R */
    for (unsigned int r = 0; r < PFarray_last (n->child[0]->plans); r++)
        add_plan (ret,
                  select_ (*(plan_t **) PFarray_at (n->child[0]->plans, r),
                           n->sem.select.att));

    return ret;
}

/**
 * Generate physical plan for the logical `difference' operator.
 *
 * @todo
 *     We currently only have a Difference operator that can handle
 *     one column tables only.
 */
static PFplanlist_t *
plan_difference (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_difference);
    assert (n->child[0]); assert (n->child[0]->plans);
    assert (n->child[1]); assert (n->child[1]->plans);

    /* consider each plan in L */
    for (unsigned int l = 0; l < PFarray_last (n->child[0]->plans); l++)
        /* and each plan in R */
        for (unsigned int r = 0; r < PFarray_last (n->child[1]->plans); r++)
            add_plan (ret,
                      difference (
                          *(plan_t **) PFarray_at (n->child[0]->plans, l),
                          *(plan_t **) PFarray_at (n->child[1]->plans, r)));

    return ret;
}

/**
 * Generate physical plan for the logical `Count' operator.
 *
 * Currently we only provide HashCount, which neither benefits from
 * any input ordering, nor does it guarantee any output ordering.
 *
 * FIXME:
 *   Is there a means to implement an order-aware Count operator
 *   in MonetDB?
 */
static PFplanlist_t *
plan_count (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_count);
    assert (n->child[0]); assert (n->child[0]->plans);

    /* consider each plan in n */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  hash_count (
                      *(plan_t **) PFarray_at (L(n)->plans, i),
                      n->sem.count.res, n->sem.count.part));

    return ret;
}


/**
 * Create physical plan for the staircase join operator (XPath location
 * steps). Uses helper functions plan_scj_XXX.
 */
static PFplanlist_t *
plan_scjoin (const PFla_op_t *n)
{
    PFplanlist_t *ret     = new_planlist ();
    PFpa_op_t* (*llscj) (const PFpa_op_t *, const PFpa_op_t *, const PFty_t,
                         const PFord_ordering_t, const PFord_ordering_t) = NULL;

    switch (n->sem.scjoin.axis) {
        case alg_anc:       llscj = PFpa_llscj_anc;         break;
        case alg_anc_s:     llscj = PFpa_llscj_anc_self;    break;
        case alg_attr:      llscj = PFpa_llscj_attr;        break;
        case alg_chld:      llscj = PFpa_llscj_child;       break;
        case alg_desc:      llscj = PFpa_llscj_desc;        break;
        case alg_desc_s:    llscj = PFpa_llscj_desc_self;   break;
        case alg_fol:       llscj = PFpa_llscj_foll;        break;
        case alg_fol_s:     llscj = PFpa_llscj_foll_sibl;   break;
        case alg_par:       llscj = PFpa_llscj_parent;      break;
        case alg_prec:      llscj = PFpa_llscj_prec;        break;
        case alg_prec_s:    llscj = PFpa_llscj_prec_sibl;   break;
        case alg_self:      assert (0);
    }

    /*
     * Consider loop-lifted staircase join variants.
     *
     * (For top-level XPath expressions, we'd not actually need
     * loop-lifted staircase joins, but could fall back to the
     * more efficient ``standard'' staircase join.
     */

    /*
     * Loop-lifted staircase join can handle two different orderings
     * for input and output: iter|item or item|iter. In the invocation
     * of the staircase join operators in MIL, this information is
     * encoded in a single integer value.
     */
    const PFord_ordering_t in[2]
            = { PFord_refine (PFord_refine (PFordering (), att_iter),
                              att_item),
                PFord_refine (PFord_refine (PFordering (), att_item),
                              att_iter) };
    const PFord_ordering_t out[2]
            = { PFord_refine (PFord_refine (PFordering (), att_iter),
                              att_item),
                PFord_refine (PFord_refine (PFordering (), att_item),
                              att_iter) };

    /* consider the two possible input orderings */
    for (unsigned short i = 0; i < 2; i++) {

        PFplanlist_t *ordered = new_planlist ();

        /* sort all plans according to this input ordering */
        for (unsigned int j = 0; j < PFarray_last (n->child[1]->plans); j++) {
            add_plans (ordered,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (n->child[1]->plans, j),
                           in[i]));
        }

        /* generate plans for each input and each output ordering */

        for (unsigned int k = 0; k < PFarray_last (ordered); k++)
            for (unsigned int l = 0; l < PFarray_last (n->child[0]->plans); l++)
                /* the evaluation of the attribute axis keeps the input order */
                if (n->sem.scjoin.axis == alg_attr)
                    add_plan (
                        ret,
                        llscj (*(plan_t **) PFarray_at (n->child[0]->plans, l),
                               *(plan_t **) PFarray_at (ordered, k),
                               n->sem.scjoin.ty,
                               in[i],
                               out[i]));
                else
                    for (unsigned short o = 0; o < 2; o++)
                        add_plan (
                            ret,
                            llscj (*(plan_t **) PFarray_at (n->child[0]->plans,
                                                            l),
                                   *(plan_t **) PFarray_at (ordered, k),
                                   n->sem.scjoin.ty,
                                   in[i],
                                   out[o]));
    }

    return ret;
}

/**
 * `fragment' operators in the logical algebra just get a 1:1 mapping
 * into the physical Fragment operator.
 */
static PFplanlist_t *
plan_fragment (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
        add_plan (ret,
                  fragment (*(plan_t **) PFarray_at (n->child[0]->plans, i)));

    return ret;
}

/**
 * `roots' operators in the logical algebra just get a 1:1 mapping
 * into the physical Roots operator.
 */
static PFplanlist_t *
plan_roots (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
        add_plan (ret, roots (*(plan_t **) PFarray_at (n->child[0]->plans, i)));

    return ret;
}

/**
 * `frag_union' operators in the logical algebra just get a 1:1 mapping
 * into the physical FragUnion operator.
 */
static PFplanlist_t *
plan_frag_union (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (n->child[0]->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (n->child[1]->plans); j++)
            add_plan (ret,
                      frag_union (
                          *(plan_t **) PFarray_at (n->child[0]->plans, i),
                          *(plan_t **) PFarray_at (n->child[1]->plans, j)));

    return ret;
}

/**
 * `empty_frag' operators in the logical algebra just get a 1:1 mapping
 * into the physical EmptyFrag operator.
 */
static PFplanlist_t *
plan_empty_frag (const PFla_op_t *n __attribute__((unused)))
{
    PFplanlist_t *ret    = new_planlist ();

    (void) n; /* pacify picky compilers that do not understand
                 "__attribute__((unused))" */

    add_plan (ret, empty_frag ());

    return ret;
}

/**
 * Prune all those plans from a plan list that are superseded
 * by some other plan in the list.
 */
static PFplanlist_t *
prune_plans (PFplanlist_t *planlist)
{
    PFplanlist_t *ret = new_planlist ();

    /* Iterate over all plans. Add it to the return value if there's
     * not yet a better plan.
     */
    for (unsigned int i = 0; i < PFarray_last (planlist); i++) {

        /* assume there is no better plan */
        bool found_better = false;
    
        /* but maybe there is one already in our return list */
        for (unsigned int j = 0; j < PFarray_last (ret); j++)
            if (better_or_equal (*(plan_t **) PFarray_at (ret, j),
                                 *(plan_t **) PFarray_at (planlist, i))) {
                found_better = true;
                break;
            }

        /* if not, is there a better plan to follow in `planlist'? */
        if (!found_better)
            for (unsigned int j = i + 1; j < PFarray_last (planlist); j++)
                if (better_or_equal (*(plan_t **) PFarray_at (planlist, j),
                                     *(plan_t **) PFarray_at (planlist, i))) {
                    found_better = true;
                    break;
                }

        /*
         * If we know there is a better plan (either in the result
         * set, or lateron in the plan list), skip this plan.
         * Otherwise, add it to the result set.
         */
        if (!found_better)
            *(plan_t *) PFarray_add (ret)
                = *(plan_t *) PFarray_at (planlist, i);
    }

    return ret;
}

/**
 * Test if @a a is a ``better'' plan than @a b:
 *
 *  - Plan @a a is cheaper than plan @a b.
 *  - Plan @a a provides all the orderings that @a b also provides.
 *    (Or even more orderings/more strict ones.)
 */
static bool
better_or_equal (const plan_t *a, const plan_t *b)
{
    /* If a does not cost less than b, return false. */
    if (costless (b, a))
        return false;

    /* a is only better than b if it satisfies *all* the orderings in b. */
    for (unsigned int i = 0; i < PFord_set_count (b->orderings); i++) {

        bool satisfied = false;

        for (unsigned int j = 0; j < PFord_set_count (a->orderings); j++) {
            /*
             * If the this order in a implies the current order in b,
             * we are satisfied.
             */
            if (PFord_implies (
                        PFord_set_at (a->orderings, j),
                        PFord_set_at (b->orderings, i))) {
                satisfied = true;
                break;
            }
        }

        /* If one ordering in b cannot be satisfied, a is not better than b. */
        if (! satisfied)
            return false;
    }

    return true;
}

/**
 * Test is plan @a a is cheaper than plan @a b.
 *
 * We don't really have a suitable cost model (yet), so this is
 * actually just the integer comparison operator.
 */
static bool
costless (const plan_t *a, const plan_t *b)
{
    return (a->cost < b->cost);
}


/**
 * Compute all interesting plans that execute @a n and store them
 * in the @a plans field of @a n.
 *
 * Generates plans (through recursive invocation) bottom-up. Actual
 * planning is distributed to the @c plan_XXX function that
 * corresponds to the current node kind. Afterwards, we immediately
 * prune un-interesting plans (to reduce the search space as early
 * as possible).
 *
 * Logical algebra properties (column constantness) are propagated
 * to the corresponding physical plans for possible optimizations.
 *
 * As what we translate is actually a DAG (not a tree), the current
 * node @a n already might have been translated earlier. We thus
 * check if there is already a plan for this logical expression and
 * skip planning in that case.
 *
 * @param n subexpression to translate
 */
static void
plan_subexpression (PFla_op_t *n)
{
    PFplanlist_t *plans = NULL;

    /*
     * Skip if we already compiled this subexpression.
     * (Note that we actually compile a DAG, not a tree.)
     */
    if (n->plans)
        return;

    /* translate bottom-up */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD; i++)
        if (n->child[i])
            plan_subexpression (n->child[i]);

    /* Compute possible plans. */
    switch (n->kind) {
        case la_lit_tbl:      plans = plan_lit_tbl (n);     break;
        case la_project:      plans = plan_project (n);     break;
        case la_cross:        plans = plan_cross (n);       break;
        case la_eqjoin:       plans = plan_eqjoin (n);      break;
        case la_disjunion:    plans = plan_disjunion (n);   break;
        case la_rownum:       plans = plan_rownum (n);      break;
        case la_distinct:     plans = plan_distinct (n);    break;
                                                            
        case la_num_add:                                    
        case la_num_subtract:                               
        case la_num_multiply:                               
        case la_num_divide:                                 
        case la_num_modulo:                                 
        case la_num_eq:                                     
        case la_num_gt:                                     
                              plans = plan_binop (n);       break;
                                                            
        case la_num_neg:                                    
        case la_bool_not:                                   
                              plans = plan_unary (n);       break;
                                                            
        case la_cast:         plans = plan_cast (n);        break;
        case la_select:       plans = plan_select (n);      break;
        case la_difference:   plans = plan_difference (n);  break;
        case la_count:        plans = plan_count (n);       break;
                                                            
        case la_scjoin:       plans = plan_scjoin (n);      break;
        case la_doc_tbl:      plans = plan_doc_tbl (n);     break;
                                                            
        case la_fragment:     plans = plan_fragment (n);    break;
        case la_roots:        plans = plan_roots (n);       break;
        case la_frag_union:   plans = plan_frag_union (n);  break;
        case la_empty_frag:   plans = plan_empty_frag (n);  break;
                                                            
        case la_doc_access:   plans = plan_doc_access (n);  break;
        case la_string_join:  plans = plan_string_join (n); break;
                                                            
        case la_serialize:    plans = plan_serialize (n);   break;

        default:
            PFoops (OOPS_FATAL,
                    "physical algebra equivalent for logical algebra "
                    "node kind %u not implemented, yet", n->kind);
    }

    assert (plans);
    assert (PFarray_last (plans) > 0);

    /* Prune un-interesting plans. */
    plans = prune_plans (plans);

    /* Keep the computed plans in the logical algebra node. */
    n->plans = plans;

    /* assign logical properties to physical node as well */
    for (unsigned int plan = 0; plan < PFarray_last (plans); plan++)
        (*(plan_t **) PFarray_at (plans, plan))->prop = n->prop;
}

/**
 * Compute physical algebra plan for logical algebra expression
 * tree rooted at @a root.
 *
 * For each sub-expression in the logical algebra tree, we compute
 * all ``interesting'' (possible) plans, ignoring those plans that
 * are superseded by a cheaper plan that guarantees the same (or
 * even stricter) ordering. From all the plans that execute
 * @a root, we pick the cheapest.
 *
 * While traversing the logical algebra tree bottom-up, we store
 * a list containing each ``interesting'' plan equivalent in
 * each logical algebra subexpression root node.
 *
 * @param root Root of the logical algebra tree to be compiled.
 * @return ``Best'' plan to execute the algebra expression rooted
 *         at @a root.
 */
PFpa_op_t *
PFplan (PFla_op_t *root)
{
    PFpa_op_t *ret = NULL;

    /* compute all interesting plans for root */
    plan_subexpression (root);

    /* Now pick the cheapest one. */
    assert (root->plans);
    assert (PFarray_last (root->plans) > 0);

    ret = *(plan_t **) PFarray_at (root->plans, 0);

    for (unsigned int i = 1; i < PFarray_last (root->plans); i++)
        if (costless (*(plan_t **) PFarray_at (root->plans, i), ret))
            ret = *(plan_t **) PFarray_at (root->plans, i);

    return ret;
}

/* vim:set shiftwidth=4 expandtab: */
