/**
 * @file
 *
 * Compile Algebra expression tree into MIL tree.
 *
 * $Id$
 */

#include <assert.h>

/* strlen et al. */
#include <string.h>

#include "pathfinder.h"
#include "milgen.h"

/** twig-generated node type identifiers */
#include "milgen.symbols.h"

/** twig: type of tree node */
#define TWIG_NODE PFalg_op_t

/** twig: max number of children under a parse tree node */
#define TWIG_MAXCHILD PFALG_OP_MAXCHILD

static int TWIG_ID[] = {
      [aop_lit_tbl]   lit_tbl    /**< literal table */
    , [aop_disjunion] disjunion  /**< union two relations with same schema */
    , [aop_cross]     cross      /**< cross product (Cartesian product) */
    , [aop_project]   project    /**< projection and renaming operator */
    , [aop_rownum]    rownum     /**< consecutive number generation */

    , [aop_serialize] serialize  /**< serialize algebra expression below
                                      (This is mainly used explicitly match
                                      the expression root during the Twig
                                      pass.) */
};

/** twig: setup twig */
#include "twig.h"

/* undefine the twig node ids because we introduce
 * MIL tree constructor functions of the same name below
 */
#undef lit_tbl
#undef disjunion
#undef cross
#undef project
#undef rownum
#undef serialize

/* ----------------------- End of twig setup -------------------- */

#include "mil_mnemonic.h"

/* forward declarations */
static void clear_usectr (PFalg_op_t *n);
static void zero_refctr (PFalg_op_t *n);
static void inc_refctr (PFalg_op_t *n);
static void deallocate (PFalg_op_t *n, int count);
static PFmil_ident_t new_var (void);
static PFmil_ident_t bat (PFmil_ident_t, PFalg_att_t, PFalg_simple_type_t);

static PFmil_t * literal (PFalg_atom_t atom);

/** MIL implementation types for algebra types */
static PFmil_type_t impl_types[] = {
      [aat_nat]    m_oid
    , [aat_int]    m_int
    , [aat_str]    m_str
    , [aat_node]   m_oid
};

/** implementation type for a given algebra type, as a MIL node */
#define implty(n) type (impl_types[n])

/**
 * Set usage counter of all algebra nodes to 0.
 *
 * @param n Current node.
 */
static void
clear_usectr (PFalg_op_t *n)
{
    int i;

    n->usectr = 0;

    for (i = 0; i < PFALG_OP_MAXCHILD && n->child[i]; i++)
        clear_usectr (n->child[i]);
}

/**
 * Set reference counter of all algebra nodes to 0. See #set_refctr().
 *
 * @param n Current node.
 */
static void
zero_refctr (PFalg_op_t *n)
{
    int i;

    n->refctr = 0;

    for (i = 0; i < PFALG_OP_MAXCHILD && n->child[i]; i++)
        zero_refctr (n->child[i]);
}

/**
 * Walk through algebra tree and increment the reference counter of
 * each node. Common subtrees will be visited more than once, resulting
 * in a reference count non-equal to 1. (Note that physically the algebra
 * is not actually a tree.)
 *
 * @param n Current node.
 */
static void
inc_refctr (PFalg_op_t *n)
{
    int i;

    n->refctr++;

    for (i = 0; i < PFALG_OP_MAXCHILD && n->child[i]; i++)
        inc_refctr (n->child[i]);
}

/**
 * Set reference counter of each algebra tree node.
 *
 * While on the logic level, we think of the algebra as an expression
 * @b tree, we physically implement it as a directed @b graph. Generated
 * algebra expressions will contain many common subexpressions. During
 * the generation, we will map all common subtrees to the same physical
 * node. When generating the MIL code from the algebra, we will evaluate
 * each subtree only once. Later references to that expression will use
 * that same result. A reference counter gives us the information, how
 * many references we have to each node - an information we will use to
 * correctly clean up variables that are no longer used in MIL.
 *
 * @param root Root of the algebra tree.
 */
static void
set_refctr (PFalg_op_t *root)
{
    /* first set all reference counters to zero (play safe) */
    zero_refctr (root);

    /* now, during a tree-walk, increment the counter for each visit. */
    inc_refctr (root);
}

/**
 * We collect the MIL program during compilation here.
 */
static PFmil_t *milprog = NULL;

/**
 * ``Execute'' MIL code (i.e., collect it in variable #milprog).
 *
 * Twig has been designed as a code generator, aiming at immediate
 * printing of generated code. Instead of printing the code directly,
 * we collect all the generated snippets via calls to execute().
 * Effectively, execute() builds up a MIL tree representing a long
 * expression sequence, with the tree root in #milprog.
 *
 * Call execute() with an arbitrary number of MIL commands (as
 * #PFmil_t pointers).
 *
 * In other Twig passes, compilation results have been collected by
 * references within the source tree (e.g., the ``core'' member variable
 * in struct #PFpnode_t). The algebra, however, is not necessarily a
 * real tree, but may be any directed graph. For each sub-expression,
 * we want to compute its value only once, when the expression is needed
 * the first time. For subsequent uses, we reference the ``old'' variable,
 * before finally deleting the MIL variable after the sub-expression has
 * been referenced the last time.
 *
 * If we built up a MIL tree parallel to the algebra tree, we could not
 * really ensure that computation is done before the first reference,
 * and that destroying is done after the last. By collecting code via
 * execute(), we are sure that MIL code will be output and processed
 * in exactly the same order as we generated it during compilation.
 */
#define execute(...) milprog = seq (milprog, __VA_ARGS__)


/**
 * ``Invent'' new MIL variable name.
 *
 * This function will return a unique string on each call that can
 * be used as a valid Monet identifier.
 */
static PFmil_ident_t
new_var (void)
{
    static unsigned int varno = 0;
    PFmil_ident_t ret = PFmalloc (sizeof ("a0000"));

    sprintf ((char *) ret, "a%04u", varno++);

    return ret;
}

/**
 * Construct Monet BAT name to represent an attribute of a
 * relation. The name is constructed based on a prefix, the
 * attribute name, and the attribute's type. These three parts
 * are concatenated, separated by an underscore, to form the
 * BAT name.
 *
 * The prefix refers to the relation this BAT belongs to. The
 * attribute obviously specifies the attribute. As all attributes
 * may have a polymorphic type, an attribute might be represented
 * by more than one BAT. Appending the implementation type to the
 * BAT name resolves the ambiguity.
 *
 * @param prefix     BAT prefix (``table name'')
 * @param attribute  Attribute name, will be used as suffix.
 * @param type       Monet implementation type, will also be part
 *                   of the BAT name.
 * @return A MIL identifier of the form "<prefix>_<attribute>_<type>"
 */
static PFmil_ident_t
bat (PFmil_ident_t prefix, PFalg_att_t attribute, PFalg_simple_type_t type)
{
    PFmil_ident_t ret;
    static const char *type_ident[] = {
          [m_oid]    "oid"
        , [m_int]    "int"
        , [m_str]    "str" };

    /* allocate for prefix + attribute + max type + underscores + '\0' */
    ret = PFmalloc (strlen (prefix) + strlen (attribute) + sizeof ("int") + 3);

    sprintf (ret, "%s_%s_%s", prefix, attribute, type_ident[impl_types[type]]);

    return ret;
}

static PFmil_t *
literal (PFalg_atom_t atom)
{
    switch (atom.type) {

        case aat_int:  return lit_int (atom.val.int_);
        case aat_str:  return lit_str (atom.val.str);
        case aat_node: return lit_oid (atom.val.node);

        default:
                       PFoops (OOPS_FATAL,
                               "A relational algebra type has not yet "
                               "been implemented.");
                       break;
    }
}

/**
 * ``Deallocate'' node @a count times.
 *
 * Increment the usage counter of @a n by @a count. (``@a n has been
 * used @a count times.'') If the usage counter reaches the reference
 * counter, we will no longer need the expression result and can free
 * it.
 */
static void
deallocate (PFalg_op_t *n, int count)
{
    int i;
    PFalg_simple_type_t t;

    assert (n);

    /* increment usage counter accordingly */
    n->usectr += count;

    /* free BAT if the usage counter has reached the reference counter
     * (but only if BAT is actually instantiated and bat_prefix != NULL) */
    if (n->bat_prefix && n->usectr >= n->refctr)
        for (i = 0; i < n->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & n->schema.items[i].type)
                    execute (assgn (var (bat (n->bat_prefix,
                                              n->schema.items[i].name,
                                              t)),
                             unused ()));

    /* clear the bat_prefix field, so we won't accidentally re-use this BAT */
    n->bat_prefix = NULL;
}

/**
 * Find monomorphic column in @a n. If no such column is found,
 * return -1.
 */
static int
mono_col (PFalg_op_t *n)
{
    int                 col;
    PFalg_simple_type_t t;

    for (col = 0; col < n->schema.count; col++)
        for (t = 1; t; t <<= 1)
            if (t == n->schema.items[col].type)
                return col;

    return -1;
}

/**
 * Compile Algebra expression tree into MIL tree
 *
 * @param a Algebra expression tree
 */
PFmil_t *
PFmilgen (PFalg_op_t *a)
{
    assert (a);

    /* set reference counters and clear usage counters */
    set_refctr (a);
    clear_usectr (a);

    /* initialize variable milprog, with a `no operation' node */
    milprog = nop ();

    /* invoke compilation */
    (void) rewrite (a, 0);

    /* return MIL code collected in variable milprog */
    return milprog;
}
