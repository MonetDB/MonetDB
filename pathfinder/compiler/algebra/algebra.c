/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

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
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
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
 * distinct attribute nodes and other nodes. In both cases we will
 * implement XML tree nodes with help of @em two to three columns:
 * @em pre, @em frag and possibly @em attr. Enum value @c aat_pnode
 * thus sets @em three bits; the three sub-parts are available as
 * @c aat_pre, @c aat_frag and @c aat_nkind. The last bit is only used
 * to decide if we have non-attribute nodes. Accordingly the enum value
 * @c aat_anode sets the bits for the sub-parts @c aat_pre, @c aat_attr
 * and @c aat_frag. (See also @ref table_representation below.)
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
 * @section milgenOverview Compiling Plans into Internal MIL Code Representation
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
 *   pre/attr/frag triple.
 *
 * The special role, where nodes are represented as three columns, while
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
 * For more specific information on the MIl generation please refer
 * to the @ref milgenDetail page in milgen.brg.
 *
 * @section milprint Serializing MIL
 *
 * The file @c milgen.brg produces an <em>internal tree representation</em>
 * of the generated MIL program. Reason for this is that we apply a dead code
 * elimination on the MIL tree afterwards. The MIL dead code elimination in
 * #mil_dce.c removes all variables that are not referenced.
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

/* always include pf_config.h first! */
#include "pf_config.h"
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
    return (PFalg_atom_t) { .type = aat_nat, .val = { .nat_ = value } };
}

/** construct literal integer (atom) */
PFalg_atom_t
PFalg_lit_int (long long int value)
{
    return (PFalg_atom_t) { .type = aat_int, .val = { .int_ = value } };
}

/** construct literal string (atom) */
PFalg_atom_t
PFalg_lit_str (char *value)
{
    return (PFalg_atom_t) { .type = aat_str, .val = { .str = value } };
}

/** construct literal untypedAtomic (atom) */
PFalg_atom_t
PFalg_lit_uA (char *value)
{
    return (PFalg_atom_t) { .type = aat_uA, .val = { .str = value } };
}

/** construct literal decimal (atom) */
PFalg_atom_t
PFalg_lit_dec (float value)
{
    return (PFalg_atom_t) { .type = aat_dec, .val = { .dec_ = value } };
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

    /* schemata are not equal if they have a different number of columns */
    if (a.count != b.count)
        return false;

    /* see if any column in a is also available in b */
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
 * a pair consisting of the new and old column name.
 * Particularly useful in combination with the constructor
 * function for the algebra projection operator (see
 * #PFla_project_() or its wrapper macro #project()).
 *
 * @param new Attribute name after the projection
 * @param old ``Old'' column name in the argument of
 *            the projection operator.
 */
PFalg_proj_t
PFalg_proj (PFalg_col_t new, PFalg_col_t old)
{
    return (PFalg_proj_t) { .new = new, .old = old };
}

/**
 * Merge adjacent projection lists. The new projection list will
 * be as wide as the upper projection (@a upper_count).
 */
PFalg_proj_t *
PFalg_proj_merge (PFalg_proj_t *upper_proj, unsigned int upper_count,
                  PFalg_proj_t *lower_proj, unsigned int lower_count)
{
    unsigned int  i,
                  j;
    PFalg_proj_t *proj = PFmalloc (upper_count * sizeof (*(proj)));

    for (i = 0; i < upper_count; i++) {
        for (j = 0; j < lower_count; j++)
            if (upper_proj[i].old ==
                lower_proj[j].new) {
                proj[i] = PFalg_proj (upper_proj[i].new,
                                      lower_proj[j].old);
                break;
            }
        if (j == lower_count)
            PFoops (OOPS_FATAL,
                    "unreferenced column in algebra plan");
    }
    return proj;
}

/**
 * Create a projection list based on a schema. The new projection
 * list will be as wide as the upper projection (@a upper_count).
 */
PFalg_proj_t *
PFalg_proj_create (PFalg_schema_t schema)
{
    unsigned int  i;
    PFalg_proj_t *proj = PFmalloc (schema.count * sizeof (*(proj)));

    for (i = 0; i < schema.count; i++) {
        proj[i] = PFalg_proj (schema.items[i].name,
                              schema.items[i].name);
    }
    return proj;
}

/**
 * Constructor for column lists (e.g., for literal table
 * construction, or sort specifications in the rownum operator).
 *
 * @param count Number of array elements that follow.
 * @param cols  Array of column names.
 *              Must be exactly @a count elements long.
 *
 * @note
 *   You typically won't need to call this function directly. Use
 *   the wrapper macro #PFalg_collist_worker() (or its abbreviation
 *   #collist(), if you have included algebra_mnemonic.h). It will
 *   determine @a count on its own, so you only have to pass an
 *   arbitrary number of column names.
 *
 * @b Example:
 *
 * @code
   PFalg_collist_t *s = collist (col_iter, col_pos);
@endcode
 */
PFalg_collist_t *
PFalg_collist_ (unsigned int count, PFalg_col_t *cols)
{
    PFalg_collist_t *ret = PFalg_collist (count);
    unsigned int     i;

    for (i = 0; i < count; i++)
        PFalg_collist_add (ret) = cols[i];

    return ret;
}

/**
 * Return a schema of iter|pos|item where item type is item_t
 */
PFalg_schema_t
PFalg_iter_pos_item_schema(PFalg_simple_type_t item_t)
{
    PFalg_schema_t schema;
    schema.count = 3;
    schema.items = PFmalloc (3 * sizeof (PFalg_schema_t));

    schema.items[0].name = col_iter;
    schema.items[0].type = aat_nat;
    schema.items[1].name = col_pos;
    schema.items[1].type = aat_nat;
    schema.items[2].name = col_item;
    schema.items[2].type = item_t;

    return schema;
}

/**
 * Return a schema of iter|pos|item|score where item type is item_t
 */
PFalg_schema_t
PFalg_iter_pos_item_score_schema(PFalg_simple_type_t item_t)
{
    PFalg_schema_t schema;
    schema.count = 4;
    schema.items = PFmalloc (4 * sizeof (PFalg_schema_t));

    schema.items[0].name = col_iter;
    schema.items[0].type = aat_nat;
    schema.items[1].name = col_pos;
    schema.items[1].type = aat_nat;
    schema.items[2].name = col_item;
    schema.items[2].type = item_t;
    schema.items[3].name = col_score1;
    schema.items[3].type = aat_dbl;

    return schema;
}

/**
 * Return a schema of iter|item where item type is item_t
 */
PFalg_schema_t
PFalg_iter_item_schema(PFalg_simple_type_t item_t)
{
    PFalg_schema_t schema;
    schema.count = 2;
    schema.items = PFmalloc (2 * sizeof (PFalg_schema_t));

    schema.items[0].name = col_iter;
    schema.items[0].type = aat_nat;
    schema.items[1].name = col_item;
    schema.items[1].type = item_t;

    return schema;
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
int
PFalg_atom_cmp (PFalg_atom_t a, PFalg_atom_t b)
{
    assert (PFalg_atom_comparable (a, b));

    switch (a.type) {
        case aat_nat:   return (a.val.nat_ == b.val.nat_ ? 0
                                : (a.val.nat_ < b.val.nat_ ? -1 : 1));
        case aat_int:   return a.val.int_ - b.val.int_;
        case aat_uA:
        case aat_str:   return strcmp (a.val.str, b.val.str);
        case aat_dec:   return (a.val.dec_ == b.val.dec_ ? 0
                                : (a.val.dec_ < b.val.dec_ ? -1 : 1));
        case aat_dbl:   return (a.val.dbl == b.val.dbl ? 0
                                : (a.val.dbl < b.val.dbl ? -1 : 1));
        case aat_bln:   return a.val.bln - b.val.bln;
        case aat_qname: return PFqname_eq (a.val.qname, b.val.qname);
        default:
            PFoops (OOPS_FATAL, "error comparing literal values");
            break;
    }

    assert(0); /* never reached due to "exit" in PFoops */
    return 0; /* pacify picky compilers */
}

/**
 * Print simple type name
 */
char *
PFalg_simple_type_str (PFalg_simple_type_t type) {
    switch (type) {
        /* the logical types */
        case aat_nat:   return "nat";
        case aat_int:   return "int";
        case aat_str:   return "str";
        case aat_dec:   return "dec";
        case aat_dbl:   return "dbl";
        case aat_bln:   return "bool";
        case aat_uA:    return "uA";
        case aat_qname: return "qname";
        case aat_node:  return "node";
        case aat_anode: return "attr";
        case aat_pnode: return "pnode";

        /* the date time types */
        case aat_dtime:      return "dateTime";
        case aat_date:       return "date";
        case aat_time:       return "time";
        case aat_gymonth:    return "gYearMonth";
        case aat_gyear:      return "gYear";
        case aat_gmday:      return "gMonthDay";
        case aat_gmonth:     return "gMonth";
        case aat_gday:       return "gDay";
        case aat_duration:   return "duration";
        case aat_ymduration: return "yearMonthDuration";
        case aat_dtduration: /* or aat_frag1 */
            /* overloaded with update queries */
            if (type & aat_update)
                return "target_frag";
            return "dayTimeDuration";

        /* the bit representation */
        case aat_qname_id:   return "qname_id";
        case aat_qname_cont: return "qname_cont";
        case aat_frag:       return "frag";
        case aat_pre:        return "pre";
        case aat_attr:       return "attr";
        case aat_pre1:       return "target_pre";
        case aat_attr1:      return "target_attr";
        case aat_docmgmt:    return "docmgmt_type";
        case aat_error:      return "error";
#ifdef HAVE_GEOXML
        case aat_wkb:        return "wkb";
#endif
        default:
            if (type & aat_update)
                return "update";
            else if (type & aat_docmgmt)
                return "docmgmt";
            else
                PFoops (OOPS_FATAL, "unknown column simple type (%i)", type);
    }
    return NULL;
}

static unsigned int highest_col_name_id;
/* define an internal unique score column identifier
   (mix of col_pos and col_item) */
#define col_score 0x00000003

/**
 * Initialize the column name counter.
 */
void
PFalg_init (void)
{
    highest_col_name_id = 1;
}

/**
 * Checks whether a name is unique or not.
 */
bool
PFcol_is_name_unq (PFalg_col_t col)
{
    return ((1 << 3) & col) && (col & 7);
}

/**
 * Create an unique name based on an id @a id and
 * an unique name @a unq that retains the usage information
 * of the new variable (iter, pos or item).
 */
static PFalg_col_t
col_unq_unq (PFalg_col_t col, unsigned int id)
{
    return (id << 4) | (1 << 3) | (col & 7);
}
    
/**
 * Create an unique name based on an id @a id and
 * an original name @a ori that retains the usage information
 * of the new variable (iter, pos or item).
 */
static PFalg_col_t
col_ori_unq (PFalg_col_t ori, unsigned int id)
{
    PFalg_col_t unq = col_NULL;

    if (PFcol_is_name_unq(ori))
        PFoops (OOPS_FATAL,
                "bit-encoded column name expected");

    switch (ori) {
        case col_iter:
        case col_iter1:
        case col_inner:
        case col_outer:
        case col_iter2:
        case col_iter3:
        case col_iter4:
        case col_iter5:
        case col_iter6:
            unq = col_iter;
            break;

        case col_pos:
        case col_pos1:
        case col_sort:
        case col_sort1:
        case col_sort2:
        case col_sort3:
        case col_sort4:
        case col_sort5:
        case col_sort6:
        case col_sort7:
        case col_ord:
            unq = col_pos;
            break;

        case col_item:
        case col_item1:
        case col_res:
        case col_res1:
        case col_cast:
        case col_item2:
        case col_item3:
        case col_subty:
        case col_itemty:
        case col_notsub:
            unq = col_item;
            break;

        case col_score1:
        case col_score2:
            unq = col_score;
            break;

        default:
            PFoops (OOPS_FATAL,
                    "Mapping variable to an unique name failed. "
                    "(bit-encoded name: %s, id: %u)",
                    PFcol_str (ori), id);
    }

    return (id << 4) | (1 << 3) | unq;
}

/**
 * Create a new unique column name (based on an original bit-encoded 
 * or unique column name @a col) that retains the usage information
 * of the new variable (iter, pos or item).
 */
PFalg_col_t
PFcol_new (PFalg_col_t col)
{
    if (PFcol_is_name_unq (col))
        return col_unq_unq (col, highest_col_name_id++);
    else
        return col_ori_unq (col, highest_col_name_id++);
}

/**
 * Create an unique name based on an id @a id (and an original
 * or unique column name @a col) that retains the usage information
 * of the new variable (iter, pos or item).
 */
PFalg_col_t
PFcol_new_fixed (PFalg_col_t col, unsigned int id)
{
    if (id >= highest_col_name_id)
        highest_col_name_id = id + 1;

    if (PFcol_is_name_unq (col))
        return col_unq_unq (col, id);
    else
        return col_ori_unq (col, id);
}

/**
 * Create an original column name based on an unique name @a unq
 * and a list of free original variables @a free.
 */
PFalg_col_t
PFcol_ori_name (PFalg_col_t unq, PFalg_col_t free)
{
    switch (unq & (col_iter | col_pos | col_item || col_score)) {
        case col_iter:
            if (free & col_iter)   return col_iter;
            if (free & col_iter1)  return col_iter1;
            if (free & col_iter2)  return col_iter2;
            if (free & col_iter3)  return col_iter3;
            if (free & col_iter4)  return col_iter4;
            if (free & col_iter5)  return col_iter5;
            if (free & col_iter6)  return col_iter6;
            if (free & col_inner)  return col_inner;
            if (free & col_outer)  return col_outer;
            /* If we have relations whose schema has more than
               10 columns of the same kind we may also use names
               from another group. */

        case col_pos:
            if (free & col_pos)    return col_pos;
            if (free & col_pos1)   return col_pos1;
            if (free & col_sort)   return col_sort;
            if (free & col_sort1)  return col_sort1;
            if (free & col_sort2)  return col_sort2;
            if (free & col_sort3)  return col_sort3;
            if (free & col_sort4)  return col_sort4;
            if (free & col_sort5)  return col_sort5;
            if (free & col_sort6)  return col_sort6;
            if (free & col_sort7)  return col_sort7;
            if (free & col_ord)    return col_ord;

        case col_item:
            if (free & col_item)   return col_item;
            if (free & col_item1)  return col_item1;
            if (free & col_item2)  return col_item2;
            if (free & col_item3)  return col_item3;
            if (free & col_subty)  return col_subty;
            if (free & col_itemty) return col_itemty;
            if (free & col_notsub) return col_notsub;
            if (free & col_res)    return col_res;
            if (free & col_res1)   return col_res1;
            if (free & col_cast)   return col_cast;

        case col_score:
            if (free & col_score1) return col_score1;
            if (free & col_score2) return col_score2;

            /* repeat iter and pos columns to allow
               other names for item columns as well */
            if (free & col_iter)   return col_iter;
            if (free & col_iter1)  return col_iter1;
            if (free & col_iter2)  return col_iter2;
            if (free & col_iter3)  return col_iter3;
            if (free & col_iter4)  return col_iter4;
            if (free & col_iter5)  return col_iter5;
            if (free & col_iter6)  return col_iter6;
            if (free & col_inner)  return col_inner;
            if (free & col_outer)  return col_outer;
            if (free & col_pos)    return col_pos;
            if (free & col_pos1)   return col_pos1;
            if (free & col_sort)   return col_sort;
            if (free & col_sort1)  return col_sort1;
            if (free & col_sort2)  return col_sort2;
            if (free & col_sort3)  return col_sort3;
            if (free & col_sort4)  return col_sort4;
            if (free & col_sort5)  return col_sort5;
            if (free & col_sort6)  return col_sort6;
            if (free & col_sort7)  return col_sort7;
            if (free & col_ord)    return col_ord;
            break;

        default:
            break;
    }

    PFoops (OOPS_FATAL,
            "Mapping unique name to an original name failed. "
            "(unique =%s, free=%s)", PFcol_str(unq), PFcol_str(free));

    return 0; /* in case a compiler does not understand PFoops */
}

/**
 * Print column name
 */
char *
PFcol_str (PFalg_col_t col) {
    switch (col) {
        case col_NULL:    return "(NULL)";
        case col_iter:    return "iter";
        case col_item:    return "item";
        case col_pos:     return "pos";
        case col_iter1:   return "iter1";
        case col_item1:   return "item1";
        case col_pos1:    return "pos1";
        case col_inner:   return "inner";
        case col_outer:   return "outer";
        case col_sort:    return "sort";
        case col_sort1:   return "sort1";
        case col_sort2:   return "sort2";
        case col_sort3:   return "sort3";
        case col_sort4:   return "sort4";
        case col_sort5:   return "sort5";
        case col_sort6:   return "sort6";
        case col_sort7:   return "sort7";
        case col_ord:     return "ord";
        case col_iter2:   return "iter2";
        case col_iter3:   return "iter3";
        case col_iter4:   return "iter4";
        case col_iter5:   return "iter5";
        case col_iter6:   return "iter6";
        case col_res:     return "res";
        case col_res1:    return "res1";
        case col_cast:    return "cast";
        case col_item2:   return "item2";
        case col_item3:   return "item3";
        case col_subty:   return "item4";
        case col_itemty:  return "item5";
        case col_notsub:  return "item6";
        case col_score1:  return "score1";
        case col_score2:  return "score2";
        default:
            if (col & (1 << 3)) {
                unsigned int id     = col >> 4,
                             tmp_id = id;
                size_t       len = sizeof ("iter");
                char        *res;

                while (tmp_id > 0) {
                    tmp_id /= 10;
                    len++;
                }
                res = PFmalloc (len+1);

                if ((col & col_iter) == col_iter)
                    snprintf (res, len, "%s%u", "iter", id);
                else if ((col & col_pos) == col_pos)
                    snprintf (res, len, "%s%u", "pos", id);
                else if ((col & col_item) == col_item)
                    snprintf (res, len, "%s%u", "item", id);
                else if ((col & col_score) == col_score)
                    snprintf (res, len, "%s%u", "score", id);
                res[len] = 0;

                return res;
            }
            else
                PFoops (OOPS_FATAL, "unknown column name (%i)", col);
    }
    return NULL;
}

/**
 * Print XPath axis
 */
char *
PFalg_axis_str (PFalg_axis_t axis)
{
    switch (axis) {
        case alg_anc:    return "ancestor";
        case alg_anc_s:  return "ancestor-or-self";
        case alg_attr:   return "attribute";
        case alg_chld:   return "child";
        case alg_desc:   return "descendant";
        case alg_desc_s: return "descendant-or-self";
        case alg_fol:    return "following";
        case alg_fol_s:  return "following-sibling";
        case alg_par:    return "parent";
        case alg_prec:   return "preceding";
        case alg_prec_s: return "preceding-sibling";
        case alg_self:   return "self";
        case alg_so_select_narrow:   return "select-narrow";
        case alg_so_select_wide:   return "select-wide";
    }
    return NULL;
}

/**
 * Print node kind
 */
char *
PFalg_node_kind_str (PFalg_node_kind_t kind)
{
    switch (kind) {
        case node_kind_elem: return "element";
        case node_kind_attr: return "attribute";
        case node_kind_text: return "text";
        case node_kind_pi:   return "processing-instruction";
        case node_kind_comm: return "comment";
        case node_kind_doc:  return "document-node";
        case node_kind_node: return "node";
    }
    return NULL;
}

/**
 * Print function call kind
 */
char *
PFalg_fun_call_kind_str (PFalg_fun_call_t kind)
{
    switch (kind) {
        case alg_fun_call_pf_documents:          return "pf:documents";
        case alg_fun_call_pf_documents_unsafe:   return "pf:documents_unsafe";
        case alg_fun_call_pf_documents_str:      return "pf:documents_str";
        case alg_fun_call_pf_documents_str_unsafe: 
                                               return "pf:documents_str_unsafe";
        case alg_fun_call_pf_collections:        return "pf:collections";
        case alg_fun_call_pf_collections_unsafe: return "pf:collections_unsafe";
        case alg_fun_call_xrpc:                  return "XRPC";
        case alg_fun_call_xrpc_helpers:          return "XRPC helper";
        case alg_fun_call_tijah:                 return "Tijah";
        case alg_fun_call_cache:                 return "Query Cache";
    }
    return NULL;
}

/**
 * Print function name
 */
char *
PFalg_fun_str (PFalg_fun_t fun)
{
    switch (fun) {
        case alg_fun_num_add:             return "add";
        case alg_fun_num_subtract:        return "subtract";
        case alg_fun_num_multiply:        return "multiply";
        case alg_fun_num_divide:          return "divide";
        case alg_fun_num_modulo:          return "modulo";
        case alg_fun_fn_abs:              return "fn:abs";
        case alg_fun_fn_ceiling:          return "fn:ceiling";
        case alg_fun_fn_floor:            return "fn:floor";
        case alg_fun_fn_round:            return "fn:round";
        case alg_fun_pf_sqrt:             return "pf:sqrt";
        case alg_fun_pf_log:              return "pf:log";
        case alg_fun_fn_concat:           return "fn:concat";
        case alg_fun_fn_substring:        return "fn:substring";
        case alg_fun_fn_substring_dbl:    return "fn:substring3";
        case alg_fun_fn_string_length:    return "fn:string-length";
        case alg_fun_fn_normalize_space:  return "fn:normalize-space";
        case alg_fun_fn_upper_case:       return "fn:upper-case";
        case alg_fun_fn_lower_case:       return "fn:lower-case";
        case alg_fun_fn_translate:        return "fn:translate";
        case alg_fun_fn_contains:         return "fn:contains";
        case alg_fun_fn_starts_with:      return "fn:starts-with";
        case alg_fun_fn_ends_with:        return "fn:ends-with";
        case alg_fun_fn_substring_before: return "fn:substring-before";
        case alg_fun_fn_substring_after:  return "fn:substring-after";
        case alg_fun_fn_matches:          return "fn:matches";
        case alg_fun_fn_matches_flag:     return "fn:matches";
        case alg_fun_fn_replace:          return "fn:replace";
        case alg_fun_fn_replace_flag:     return "fn:replace";
        case alg_fun_fn_name:             return "fn:name";
        case alg_fun_fn_local_name:       return "fn:local-name";
        case alg_fun_fn_namespace_uri:    return "fn:namespace-uri";
        case alg_fun_fn_number:           return "fn:number";
        case alg_fun_fn_number_lax:       return "fn:number";
        case alg_fun_fn_qname:            return "fn:QName";
        case alg_fun_fn_doc_available:    return "fn:doc-available";
        case alg_fun_pf_fragment:         return "#pf:fragment";
        case alg_fun_pf_supernode:        return "#pf:supernode";
        case alg_fun_pf_add_doc_str:      return "pf:add-doc";
        case alg_fun_pf_add_doc_str_int:  return "pf:add-doc";
        case alg_fun_pf_del_doc:          return "pf:del-doc";
        case alg_fun_pf_nid:              return "pf:nid";
        case alg_fun_pf_docname:          return "pf:docname";
        case alg_fun_upd_rename:          return "upd:rename";
        case alg_fun_upd_delete:          return "upd:delete";
        case alg_fun_upd_insert_into_as_first:  return "upd:insertIntoAsFirst";
        case alg_fun_upd_insert_into_as_last:   return "upd:insertIntoAsLast";
        case alg_fun_upd_insert_before:         return "upd:insertBefore";
        case alg_fun_upd_insert_after:          return "upd:insertAfter";
        case alg_fun_upd_replace_value_att:     return "upd:replaceValue";
        case alg_fun_upd_replace_value:         return "upd:replaceValue";
        case alg_fun_upd_replace_element: return "upd:replaceElementContent";
        case alg_fun_upd_replace_node:    return "upd:replaceNode";
        case alg_fun_fn_year_from_datetime:     return "fn:year-from-datetime";
        case alg_fun_fn_month_from_datetime:    return "fn:month-from-datetime";
        case alg_fun_fn_day_from_datetime:      return "fn:day-from-datetime";
        case alg_fun_fn_hours_from_datetime:    return "fn:hours-from-datetime";
        case alg_fun_fn_minutes_from_datetime:return "fn:minutes-from-datetime";
        case alg_fun_fn_seconds_from_datetime:return "fn:seconds-from-datetime";
        case alg_fun_fn_year_from_date:         return "fn:year-from-date";
        case alg_fun_fn_month_from_date:        return "fn:month-from-date";
        case alg_fun_fn_day_from_date:          return "fn:day-from-date";
        case alg_fun_fn_hours_from_time:        return "fn:hours-from-time";
        case alg_fun_fn_minutes_from_time:      return "fn:minutes-from-time";
        case alg_fun_fn_seconds_from_time:      return "fn:seconds-from-time";
        case alg_fun_add_dur:                   return "add-duration";
        case alg_fun_subtract_dur:              return "subtract-duration";
        case alg_fun_multiply_dur:              return "multiply-duration";
        case alg_fun_divide_dur:                return "divide-duration";
#ifdef HAVE_GEOXML
        case alg_fun_geo_wkb:          return "geoxml:wkb";
        case alg_fun_geo_point:        return "geoxml:point";
        case alg_fun_geo_distance:     return "geoxml:distance";
        case alg_fun_geo_geometry:     return "geoxml:geometry";
        case alg_fun_geo_relate:       return "geoxml:relate";
        case alg_fun_geo_intersection: return "geoxml:intersection";
#endif
    }
    PFoops (OOPS_FATAL, "unknown algebraic function name (%i)", fun);
    return NULL;
}

/**
 * Print aggregate kind
 */
char *
PFalg_aggr_kind_str (PFalg_aggr_kind_t kind)
{
    switch (kind) {
        case alg_aggr_dist:    return "distinct";
        case alg_aggr_count:   return "count";
        case alg_aggr_min:     return "min";
        case alg_aggr_max:     return "max";
        case alg_aggr_avg:     return "avg";
        case alg_aggr_sum:     return "sum";
        case alg_aggr_seqty1:  return "seqty1";
        case alg_aggr_all:     return "all";
        case alg_aggr_prod:    return "prod";
    }
    PFoops (OOPS_FATAL, "unknown aggregate kind (%i)", kind);
    return NULL;
}

/**
 * Construct an aggregate entry.
 */
PFalg_aggr_t
PFalg_aggr (PFalg_aggr_kind_t kind, PFalg_col_t res, PFalg_col_t col)
{
    return (PFalg_aggr_t) { .kind = kind, .res = res, .col = col };
}

/**
 * Construct a predicate.
 */
PFalg_sel_t PFalg_sel (PFalg_comp_t comp,
                       PFalg_col_t left,
                       PFalg_col_t right) {
    return (PFalg_sel_t) { .comp = comp, .left = left, .right = right };
}

/* vim:set shiftwidth=4 expandtab: */
