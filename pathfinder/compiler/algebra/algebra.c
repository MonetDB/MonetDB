/**
 * @file
 *
 * Functions related to algebra tree construction.
 * (Generic stuff for logical and physical algebra.)
 */

/*
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

/**
 * @page compilation Relational XQuery Compilation
 *
 * Pathfinder basically implements the relational XQuery compilation
 * scheme we presented at TDM 2004 <a href='#fn1'>[1]</a>. Compilation
 * from XQuery Core to our back-end MonetDB thus happens in four steps:
 *
 * -# Compilation into a <b>logical algebra</b>. This is actually what
 *    is described in the TDM publication.
 *    .
 * -# Generation of a <b>physical algebra</b> plan. This makes the
 *    decision on physical operators more explicit and, most importantly,
 *    respects table <b>orderings</b> and introduces a <b>cost model</b>.
 *    .
 * -# This plan is then translated into our internal representation of
 *    a MIL program. You may think of this as the abstract syntax tree
 *    of the MIL program.
 *    .
 * -# Finally, we serialize the MIL code into the ASCII representation
 *    readable by MonetDB.
 *
 * @section core2alg Compiling into Logical Algebra
 *
 * The step into the relational world happens in @c core2alg.brg, where
 * normalized, typed, and possibly optimized XQuery Core code is compiled
 * into our <b>relational algebra</b>. Basically, this is an
 * implementation of the TDM paper (plus, of course, rules that we did
 * not mention in the paper).
 *
 * This compilation step is based on Burg, our tree pattern matcher.
 * The file @c core2alg.brg contains a tree grammar for our XQuery Core
 * language, and rules to translate it into our relational algebra. For
 * a lineup of these translation rules, please see @c core2alg.brg.
 *
 * Compilation into the logical algebra is done bottom-up (with the
 * exception of few rules that require top-down processing; see
 * @c core2alg.brg). The output of each rule is a @em pair consisting
 * of
 *
 * -# the logical algebra equivalent of the XQuery Core expression and
 *    .
 * -# a set of XML fragments that includes all nodes that might be
 *    referenced in the result of the logical algebra expression.
 *
 * There is an important difference to the procedure described in the
 * TDM paper that affects the handling of live node sets!
 *
 * The TDM paper describes a procedure where the live node set is
 * passed through the compilation top-down, effectively as a side
 * effect that collects any live node fragments during compilation.
 * As an initialization, the live node set contains the persistent
 * document nodes @c doc.
 *
 * Our compilation entirely produces the live node set bottom-up. Any
 * compilation output "carries" a set of XML fragments that may appear
 * in the result of the algebraic expression. Persistent documents will
 * be introduced when XQuery's document accessor function @c fn:doc is
 * compiled. Other expressions will always carry an empty node set (as
 * their result will never contain nodes, such as, e.g. arithmetic
 * expressions), or discard their input fragment sets as soon as they
 * have set up a new fragment set, which is the case for all node
 * construction functions (note the <em>copy semantics</em> of XQuery's
 * node constructors). For several XQuery expressions, the compilation
 * will just pass on the live node set(s) of their argument, or @em union
 * two live node sets as it is the case for XQuery's sequence construction.
 *
 * All in all, this leads to the situation that any XQuery operation that
 * needs access to the live node sets will always see the @em minimal live
 * node set that is relevant for its argument. Future development may
 * use this information to optimize algebraic operators by restricting
 * them to only small live node sets. Minimal live node sets may also
 * become handy when we want to do cost or result size estimations on
 * the algebra plans.
 *
 * So in contrast to the compilation rules in the TDM paper, our rules
 * only require two input parameters:
 *
 * -# a variable environment @c env (\f$\Gamma\f$ in the TDM paper) that
 *    collects variable bindings at <em>compile time</em> and
 *    .
 * -# the @c loop relation.
 *
 * Both parameters are implemented as global variables in @c core2alg.brg.
 *
 * @subsection udf User-Defined Functions
 *
 * User-defined functions are essentially @em unfolded during compilation
 * into the logical algebra. If compilation encounters a user-defined
 * function call, it adds variable bindings for the function parameters
 * to the variable environment @c env, then continues compilation directly
 * in the function body.
 *
 * Though this nicely avoids any function call overhead, be aware that
 * this technique prohibits recursive user-defined functions. In fact,
 * such functions result in an infinite loop at compile time!
 *
 * @subsection builtins Built-In Functions
 *
 * File xquery_fo.c lists all built-in functions known to the
 * Pathfinder compiler. For any built-in function, we should prepare an
 * algebraic equivalent to be inserted whenever we encounter a call of
 * such a function (we don't have the algebraic representations for all
 * of them, yet).
 *
 * Algebraic equivalents are listed in the file builtins.c. The algebra
 * expression templates are implemented as C functions. A pointer to
 * the function is stored in the function's #PFfun_t struct (in
 * xquery_fo.c). Compilation to the algebra (@c core2alg.brg)
 * passes compilation to the corresponding C function in builtins.c
 * whenever it encounters a call to a built-in function.
 *
 * @subsection log_impl Logical Algebra Implementation
 *
 * Our logical algebra tree is implemented through #PFla_op_t nodes
 * ("logical algebra operator"). Constructors are listed in logical.c.
 *
 * Each node contains information about its resulting relational
 * @em schema, a list of name/type pairs. While names are implemented
 * as C strings, types are implemented with help of the enum
 * #PFalg_simple_type_t. As we need to deal with polymorphic columns,
 * the type field is actually a bit vector where we set those bits
 * that may appear in the respective columns (this is automagically
 * handled by the constructor functions).
 *
 * The simple type @c node plays a special role: Lateron, we will
 * implement XML tree nodes with help of @em two columns: @em pre and
 * @em kind. Enum value @c aat_node thus sets @em two bits; the two
 * sub-parts are available as @c aat_pre and @c aat_kind. (See also
 * @ref table_representation below.)
 *
 * @subsection algopt Optimizing Logical Algebra Trees
 *
 * Our logical algebra tree (DAG, to be precise; see below) provides the
 * first important hook for interesting query rewrite and optimization
 * techniques.
 *
 * The current code for that purpose is mainly a stub for a full
 * implementation. Here's what the current code does:
 *
 * - Infer information about table columns that are known to be constant.
 *   This is mainly a stub for the annotations that Torsten describes
 *   in his XIME-P 2005 submission <a href='#fn2'>[2]</a>. Property
 *   inference is implemented in properties.c. The logical algebra
 *   optimizer (algopt.brg, see below) invokes the inference.
 *   .
 *   Derived properties may also be helpful for later processing. And
 *   so will the physical algebra planner consider that information,
 *   plus it will transfer the information also to the physical plans.
 *   .
 * - Eliminate any literal empty tables, as well as some other minor
 *   tree rewrites. This happens in algopt.brg, which is supposed to
 *   become our actual optimizer.
 *   .
 * - Perform common subexpression elimination on the algebra tree/DAG.
 *   It is known that plans that come out of our compilation procedure 
 *   have a high degree of sharing. The way core2alg.brg is
 *   implemented, its output will thus already be a DAG (not a tree).
 *   A distinct CSE phase in algebra_cse.c catches remaining
 *   subexpressions and produces the minimal DAG for our logical algebra
 *   expression.
 *
 *
 * @section planner Compilation into a Physical Algebra Plan
 *
 * The planner (planner.c) compiles the logical algebra into
 * a physical algebra plan. The physical algebra makes several issues
 * more explicit:
 *
 * - For many logical operators we have a choice of different physical
 *   implementations, depending, e.g., on input and/or output orderings,
 *   etc.
 *   .
 * - The physical algebra is <em>order aware</em>. We keep track of the
 *   ordering each (sub)expression has. Several physical operators are
 *   known to guarantee a certain order.
 *   .
 * - The physical algebra includes a notion of <em>costs</em>. The
 *   cheapest possible plan will be searched during planning. Note that
 *   we do @b not yet have a sensible cost model; current cost estimation
 *   is just ment as a stub for a real cost model.
 *
 * @subsection operators Physical Operators
 *
 * Our physical algebra makes many operators more explicit. Row numbering,
 * e.g., is split up into sorting and the actual numbering here. Be aware,
 * though, that these operators are not necessarily those that will
 * actually be executed on the back-end. MonetDB's tactical optimization
 * may decide to use other implementations at runtime, given that they
 * produce the same output.
 *
 * @subsection ordering Order Awareness
 *
 * During planning, we track information on ordering properties that we
 * can guarantee for certain (sub) plans. The ordering framework in
 * ordering.c provides the necessary tools to test order implications, etc.
 *
 * The generated orderings produced by specific physical operators are
 * determined in the corresponding <em>constructor function</em> in
 * physical.c. In a sense, each operator "knows" about its own
 * ordering guarantees.
 *
 * Note that Pathfinder does @b not (yet) implement an extended ordering
 * framework as proposed in our Technical Report <a href='#fn3'>[3]</a>.
 *
 * @subsection costs Physical Algebra Plan Costs
 *
 * Much like in case of the ordering properties, physical costs are
 * determined in the respective constructor function in physical.c.
 *
 * The costs are currently implemented as simple integer values, where
 * cost formulas have been chosen ad hoc. We should definitely implement
 * a real cost model here!
 *
 * @subsection finding_plans Finding Physical Algebra Plans
 *
 * The planner is implemented in planner.c. Function #plan_subexpression()
 * distributes planning to a number of functions that compute plans
 * for specific logical algebra operators. Possible plans for each
 * logical node are stored in the logical algebra node's @c plans field.
 *
 * For each logical node, we produce all possible physical plans
 * (considering all available plans for its arguments). Some of these
 * plans may be "un-interesting", as we already found a plan that
 * produces (at least) the same ordering at a lower cost. We prune
 * such plans immediately from the search space for each subexpression.
 * (Note that this may be particularly effective, as our algebra heavily
 * relies on @em orderings. We frequently require one specific ordering,
 * which implies that we only need to keep a single cheapest plan for
 * that ordering.)
 *
 * The input to our planner is actually a DAG (not a tree). During the
 * bottom-up translation, we will thus frequently encounter subexpressions
 * that we have already translated before. In that case, we skip
 * re-planning for that subexpression altogether.
 *
 * There's one issue where we need to be careful during planning: Our
 * logical algebra DAG does not only share subexpressions for performance
 * reasons. Furthermore, we expect shared node constructors to be
 * evaluated exactly once to guarantee correct node identity semantics.
 *
 * In general, we might find more than one plan for a node constructor
 * node. If two different parents of such a node constructor now picked
 * different physical plans for that constructor, we would end up with
 * the construction evaluated twice (which ultimately would violate
 * XQuery semantics).
 *
 * As a workaround, our planner thus makes sure that it produces
 * @em exactly one plan for any node construction operator.
 *
 * @subsection planner_output Output of the Planner
 *
 * After planning the entire logical algebra DAG, #PFplan() picks the
 * single cheapest plan for the top-level logical algebra node (the
 * @c serialize node). This makes the output of the planner to be a
 * single physical plan---again a @em DAG.
 *
 * @subsection debugging_algebra Debugging/Investigating the Algebra
 *   Compilation Process
 *
 * The Pathfinder compiler provides hooks to print algebra tree
 * structures in a format readable by the AT&T dot utility.  Tree
 * annotations may optionally be added to the dot output.  For more
 * information please refer to the @ref commandline section on the
 * main page (main.c).
 *
 * @section milgen Compiling Plans into Internal MIL Code Representation
 *
 * Generation into MIL code is again implemented based on a Burg pattern
 * matcher (file @c milgen.brg). Its output is an internal representation
 * of the MIL program that will be serialized to ASCII MIL code afterwards.
 *
 * @subsection table_representation Table Representation in MIL
 *
 * We implement multi-column algebra tables as a number of Binary
 * Association Tables (BATs), one for each column. BATs are aligned
 * with their @c void heads.
 *
 * Due to XQuery's polymorphism, our implementation depends on polymorphic
 * table columns. We implement this as follows:
 *
 * - For each XQuery data type that may be present in the table's column,
 *   we introduce a BAT, with tail type according to the XQuery data type.
 *   We still keep these BATs head-<code>void</code> with a @c nil value
 *   for each row where the value takes some other data type.
 *   .
 *   A column <code>[42, "foo", 17, "bar"]</code> would thus be
 *   implemented as
 *   @verbatim
            void | int        void |  str
           ------+-----      ------+-------
             0@0 |  42         0@0 |  nil
             1@0 | nil         1@0 | "foo"
             2@0 |  17         2@0 |  nil
             3@0 | nil         3@0 | "bar"
@endverbatim
 *   .
 * - XML tree nodes are not implemented as a single column, but as a
 *   pre/kind pair. This is adapted from Jan's "summer branch"
 *   implementation and allows to re-use lots of things in the runtime
 *   module.
 *
 * The special role, where nodes are represented as two columns, while
 * other types only occupy one column, is nicely captured by our bit
 * vector encoding of algebra types (see @ref log_impl).
 *
 * @subsection mil_varnames MIL Variable Names
 *
 * Output of the MIL generator will be a typical, assignment based, MIL
 * program. Variable names will automatically be generated as neccessary
 * and follow the pattern "a0000", where @c 0000 is replaced by unique
 * numbers. In the resulting MIL program, all these variables will be
 * initialized in the beginning of the script. Whenever the value of a
 * variable is no longer needed, the variable is set to @c nil.
 *
 * In order to reduce the total number of variables in use, the MIL
 * generator will try to re-use variables as much as possible. For this,
 * we keep a list of variables available (@c mvars in @c milgen.brg).
 * For each variable we maintain a <em>pin counter</em> that we increase
 * whenever a variable is still in use. Whenever the compilation requests
 * a new variable via new_var(), we search for existing variables with
 * @a pin = 0, or generate a new variable name if neccessary.
 *
 * In order to link variable names to the node/attribute/type combination
 * that they implement, each physical plan node carries an environment
 * @c env, with the mapping from attribute/type to the implementing
 * BAT. (Note that XML tree nodes are implemented by two BATs. This is
 * captured by two bindings in the environment, one for @a pre, one for
 * @a kind.)
 *
 * @subsection mil_generation Generating MIL Code
 *
 * MIL Code generation is implemented through a bottom-up Burg matcher.
 * The action code calls the macro #execute(), which appends a piece
 * of MIL code to the MIL program in the global variable @a milprog.
 *
 * After finishing the compilation, we know the names (or, in other words,
 * the number) of all MIL variables that we needed, and add a declaration
 * for each of them at the beginning of the MIL code.
 *
 * The MIL code finally calls the serializer in the Pathfinder runtime
 * module. As this expects values to be represented in "Jan's" value
 * container scheme, we produce such a representation at the very end
 * of compilation. Future versions might directly serialize the value
 * encoding used by the "algebra" MIL generator.
 *
 * @section milprint Serializing MIL
 *
 * The file @c milgen.brg produces an <em>internal tree representation</em>
 * of the generated MIL program. (Reason for this is that we might do
 * rewrites to the MIL code afterwards this way, comparable to the
 * mil_opt.c optimizer in the "summer branch").
 *
 * The ASCII representation parsable by MonetDB is generated in
 * milprint.c. In a sense, this file implements a grammar for the
 * internal tree structure. Code is printed into a string array, before
 * finally printing it to stdout. The implementation as a sort of grammar
 * at the same time "guarantees" that the produced code is actually valid
 * MIL code.
 * 
 *
 * <a name='fn1'>[1]</a> Torsten Grust, Jens Teubner.
 *   <a href='http://www.pathfinder-xquery.org/files/algebra-mapping.pdf'>
 *   Relational Algebra: Mother Tongue---XQuery: Fluent</a>. <em>TDM 2004</em>.
 *
 * <a name='fn2'>[2]</a> Torsten Grust.
 *   <a href='http://www.pathfinder-xquery.org/files/relational-flwors.pdf'>
 *   Purely Relational FLWORs</a>. <em>XIME-P 2005</em>.
 *
 * <a name='fn3'>[3]</a> Peter Boncz, Torsten Grust, Stefan Manegold,
 *   Jan Rittinger, and Jens Teubner.
 *   <a href='http://www.pathfinder-xquery.org/files/pathfinder-tr.pdf'>
 *   Pathfinder: Relational XQuery Over Multi-Gigabyte XML Inputs In
 *   Interactive Time</a>. Technical Report, CWI, 2005.
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

/** handling of variable argument lists */
#include <stdarg.h>
/** strcpy, strlen, ... */
#include <string.h>
#include <stdio.h>
/** assert() */
#include <assert.h>

#include "oops.h"
#include "mem.h"
#include "array.h"

#include "algebra.h"

/** include mnemonic names for constructor functions */
#include "algebra_mnemonic.h"


/** construct literal integer (atom) */
PFalg_atom_t
PFalg_lit_nat (nat value)
{
    return (PFalg_atom_t) { .type = aat_nat, .val = { .nat = value } };
}

/** construct literal integer (atom) */
PFalg_atom_t
PFalg_lit_int (int value)
{
    return (PFalg_atom_t) { .type = aat_int, .val = { .int_ = value } };
}

/** construct literal string (atom) */
PFalg_atom_t
PFalg_lit_str (char *value)
{
    return (PFalg_atom_t) { .type = aat_str, .val = { .str = value } };
}

/** construct literal decimal (atom) */
PFalg_atom_t
PFalg_lit_dec (float value)
{
    return (PFalg_atom_t) { .type = aat_dec, .val = { .dec = value } };
}

/** construct literal double (atom) */
PFalg_atom_t
PFalg_lit_dbl (double value)
{
    return (PFalg_atom_t) { .type = aat_dbl, .val = { .dbl = value } };
}

/** construct literal boolean (atom) */
PFalg_atom_t
PFalg_lit_bln (bool value)
{
    return (PFalg_atom_t) { .type = aat_bln, .val = { .bln = value } };
}

/** construct literal QName (atom) */
PFalg_atom_t
PFalg_lit_qname (PFqname_t value)
{
    return (PFalg_atom_t) { .type = aat_qname, .val = { .qname = value } };
}


/**
 * Construct a tuple for a literal table.
 *
 * @see PFla_lit_tbl_()
 *
 * @param count Number of values in the tuple that follow
 * @param atoms Values of type #PFalg_atom_t that form the tuple.
 *              The array must be exactly @a count items long.
 *
 * @note
 *   You should never need to call this function directly. Use the
 *   wrapper macro #PFalg_tuple instead (which is available as
 *   #tuple if you have included the mnemonic constructor names in
 *   algebra_mnemonic.h). This macro will detect the @a count
 *   argument on its own, so you only need to pass the tuple's
 *   atoms.
 *
 * @b Example:
 *
 * @code
   PFalg_tuple_t t = tuple (lit_int (1), lit_str ("foo"));
@endcode
 */
PFalg_tuple_t
PFalg_tuple_ (unsigned int count, PFalg_atom_t *atoms)
{
    return (PFalg_tuple_t) {.count = count,
                            .atoms = memcpy (PFmalloc (count * sizeof (*atoms)),
                                             atoms, count * sizeof (*atoms)) };
}


#if 0
/**
 * Test the equality of two schema specifications.
 *
 * @param a Schema to test against schema @a b.
 * @param b Schema to test against schema @a a.
 * @return Boolean value @c true, if the two schemata are equal.
 */
static bool
schema_eq (PFalg_schema_t a, PFalg_schema_t b)
{
    int i, j;

    /* schemata are not equal if they have a different number of attributes */
    if (a.count != b.count)
        return false;

    /* see if any attribute in a is also available in b */
    for (j = 0; i < a.count; i++) {
        for (j = 0; j < b.count; j++)
            if ((a.items[i].type == b.items[j].type)
                && !strcmp (a.items[i].name, b.items[j].name))
                break;
        if (j == b.count)
            return false;
    }

    return true;
}
#endif


/**
 * Constructor for an item in an algebra projection list;
 * a pair consisting of the new and old attribute name.
 * Particularly useful in combination with the constructor
 * function for the algebra projection operator (see
 * #PFalg_project() or its wrapper macro #project()).
 *
 * @param new Attribute name after the projection
 * @param old ``Old'' attribute name in the argument of
 *            the projection operator.
 */
PFalg_proj_t
PFalg_proj (PFalg_att_t new, PFalg_att_t old)
{
    return (PFalg_proj_t) { .new = strcpy (PFmalloc (strlen (new) + 1), new),
                            .old = strcpy (PFmalloc (strlen (old) + 1), old) };
}


/**
 * Constructor for attribute lists (e.g., for literal table
 * construction, or sort specifications in the rownum operator).
 *
 * @param count Number of array elements that follow.
 * @param atts  Array of attribute names.
 *              Must be exactly @a count elements long.
 *
 * @note
 *   You typically won't need to call this function directly. Use
 *   the wrapper macro #PFalg_attlist() (or its abbreviation #attlist(),
 *   if you have included algebra_mnemonic.h). It will determine
 *   @a count on its own, so you only have to pass an arbitrary
 *   number of attribute names.
 *
 * @b Example:
 *
 * @code
   PFalg_attlist_t s = attlist ("iter", "pos");
@endcode
 */
PFalg_attlist_t
PFalg_attlist_ (unsigned int count, PFalg_att_t *atts)
{
    PFalg_attlist_t ret;
    unsigned int    i;

    ret.count = count;
    ret.atts  = PFmalloc (count * sizeof (*(ret.atts)));

    for (i = 0; i < count; i++)
        ret.atts[i] = strcpy (PFmalloc (strlen (atts[i]) + 1), atts[i]);

    return ret;
}

/**
 * Test if two atomic values are comparable
 */
bool
PFalg_atom_comparable (PFalg_atom_t a, PFalg_atom_t b)
{
    return a.type == b.type && !(a.type & aat_node);
}

/**
 * Compare two atomic values (if possible)
 */
int PFalg_atom_cmp (PFalg_atom_t a, PFalg_atom_t b)
{
    assert (PFalg_atom_comparable (a, b));

    switch (a.type) {
        case aat_nat:   return (a.val.nat == b.val.nat ? 0
                                : (a.val.nat < b.val.nat ? -1 : 1));
        case aat_int:   return a.val.int_ - b.val.int_;
        case aat_str:   return strcmp (a.val.str, b.val.str);
        case aat_dec:   return a.val.dec - b.val.dec;
        case aat_dbl:   return a.val.dbl - b.val.dbl;
        case aat_bln:   return a.val.bln - b.val.bln;
        case aat_qname: return PFqname_eq (a.val.qname, b.val.qname);
        case aat_node: 
        case aat_pre:
        case aat_kind:
                        break; /* error */
    }
    
    PFoops (OOPS_FATAL, "error comparing literal values");

    assert(0); /* never reached due to "exit" in PFoops */
    return 0; /* pacify picky compilers */
}

/* vim:set shiftwidth=4 expandtab: */
