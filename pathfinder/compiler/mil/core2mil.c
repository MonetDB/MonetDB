/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Map an XQuery Core expression to our internal representation of
 * a MIL program. The mapping is described in @ref mil_mapping.
 *
 * This file contains (static) constructor functions for all
 * node kinds in the MIL tree, our internal tree structure to
 * represent a MIL program. This constructor functions are named
 * after the node they create.
 *
 * A second group of static functions does the actual mapping
 * from Core to MIL. These functions are all named
 * <code>map_*</code>, each of them mapping one Core node kind.
 * All of these mapping functions get a variable, as well as
 * a Core node as their arguments and return a MIL tree. The
 * general mapping function that maps any Core expression to
 * MIL is core2mil(). It basically dispatches the actual work
 * to the various map_* functions. Some simple mappings are
 * directly done in core2mil().
 *
 * Many Core operators operate on atoms. Atoms can be mapped
 * to MIL as they are, that is, they don't have to be mapped
 * as assignments. This mapping is done in atom2mil().
 *
 * All MIL nodes have an implementation type annotation. This
 * implementation type is represented by a #PFmty_t struct
 * that carries a MIL simple type and a flag telling if the
 * type is the simple type itself or a sequence thereof. All
 * constructor functions do a sanity check on the
 * implementation types of their arguments (as far as this is
 * possible) and set the implementation type of their return
 * value.
 *
 * Before the actual MIL mapping, the Core tree is annotated
 * with the MIL implementation type of each expression. This
 * is done in PFmiltype(). The entry point to the MIL mapping
 * (PFcore2mil() in this file) will invoke PFmiltype(), then
 * map the Core expression as
 *
 * @verbatim
     result := [[ core-expression ]]
@endverbatim
 *
 * (The variable @c result is created before in PFcore2mil().)
 *
 * The final MIL program will then be this assignment, followed
 * by a <code>print (result)</code>
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2003 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

/**
 * @page mil_mapping Mapping to MIL
 *
 * In the last stages of our compiler, our internal subset "Core"
 * of the XML Query language is mapped to a MIL program (or a
 * Pathfinder internal tree structure representing this program,
 * to be precise). This mapping is done by PFcore2mil() and its
 * helper functions in core2mil.c. PFmilprint() can generate the
 * actual ASCII representation of the MIL program, correctly
 * indented and enriched with color markup describing data types,
 * if requested by the user.
 *
 * The mapping strategy we use is described in the following
 * Section @ref mapping_strategy. The implications and the
 * requirements we have on the Pathfinder runtime module follow
 * in Section @ref runtime_requirements.
 *
 * @section mapping_core_to_mil Mapping from Core to MIL
 *
 * @subsection mapping_strategy Mapping Strategy - The Original Idea
 *
 * XML Query is a fully functional language, in contrast to MIL.
 * This makes some kinds of XML Query expressions hard to map
 * straight to MIL. An example are the XML Query @c for clauses.
 * The MIL way to express iterations is the @c batloop command.
 * <code>batloop</code>s, however, do not have a return type.
 * So nested expressions are hard to implement in MIL as nested
 * MIL commands.
 *
 * The approach we took was therefore not to map XML Query
 * expressions as expressions, but always as assignment
 * statements. I. e. our mapping routines always map something
 * like
 *
 * @verbatim
   v := [[ e ]]
@endverbatim
 *
 * The whole XML Query entered by the user is therefore mapped
 * to the MIL program
 *
 * @verbatim
   result := [[ core-tree ]];
   print (result);
@endverbatim
 *
 * It will sometimes be necessary to introduce new variables
 * during the mapping to MIL. An example are @c for clauses:
 *
 * @verbatim
   v := [[ for $x in $y return e ]]

   ==

   v := <new BAT>;
   y@batloop {
       x := $t;           (bind $x)
       w := [[ e ]];      (map recursively)
       v.insert (w);      (insert result of e in v)
   }
                          (v has correct content after the batloop)
@endverbatim
 *
 * This example also makes clear that the mapping demands the
 * Core expression to be in a normalized form. In particular
 * this means that non-"atomic" expressions may only occur in
 * rare situations in the Core tree, while most operators only
 * operate on "atoms". "Atoms" we call everything that is either
 * a literal constant (like string or integer constants) or a
 * variable. @c y in the above example therefore must be a
 * variable (an atom, to be precise; but everything else than
 * a variable will be eliminated by an optimization phase), and
 * the above @c batloop makes sense.
 *
 * @subsection mapping_strategy_advanced A More Advanced Mapping
 *
 * The above approach is a consistent mapping of the functional
 * nature of "Core" to MIL. There are cases, however, where the
 * mapping would lead to very inefficient MIL code. The generated
 * MIL code will - due to the SSA form - never update any existing
 * variables, but always create new instances. Particularly in
 * sequence construction and @c for iterations, this results in
 * heavy copy and creation of BATs.
 *
 * We therefore extended the above idea:
 *
 *  - Right before MIL code generation, we leave the normalized form
 *    that only allows atoms in most places. Instead, we unfold many
 *    @c let clauses. This brings - for instance - sequences back to
 *    their original look-alike <code>seq (atom, seq (atom, seq (...)))</code>
 *    and we can identify them later on.
 *
 *  - We do no longer restrict our mapping to <code>v := [[ e ]]</code>
 *    like expressions, but also allow <code>v.insert ([[ e ]])</code>.
 *    In the source code this is reflected in the parameter @a assgn_fn
 *    in core2mil() and other functions.
 *
 *  - Several mapping rules now allow to propagate the ``final
 *    destination'' of an expression down in the recursion, by
 *    specifying insert() as the ``assignment function''.
 *
 *
 * @subsection mil_types Types in MIL
 *
 * The above subsection silently used some details of the data
 * structures and types we use in our MIL programs. This
 * subsection clarifies these details.
 *
 * <b>Simple Types</b>
 *
 * To achieve maximum performance, we try to use simple data types
 * as much as possible. This has mainly two consequences on the
 * data structures we use:
 *
 *  -# If we know the data type of an expression precisely enough
 *     (from our static typing information), we use the
 *     corresponding Monet type to represent it:
 *     @verbatim
         xs:integer     - int
         xs:double      - dbl
         xs:boolean     - bit
         xs:string      - str
         xs:anyNode     - oid
@endverbatim
 *     Note that our implementation currently cannot deal with
 *     <code>xs:decimal</code> values. The extension would be
 *     straightforward.\n
 *     Internally, we use the datatype @c node for nodes. In the
 *     actual MIL code, however, we use the Monet type @c oid.
 *     The main reason for this is Monet's @c void type that only
 *     works with @c oid and gives performance advantages. Within
 *     Pathfinder, we still know ``what kind'' of @c oid we have
 *     at hand.
 *     .
 *  -# The XML Query semantic does not distinguish between a
 *     single item and a sequence containing exactly this item (see
 *     <a href='http://www.w3.org/TR/2002/WD-xquery-20021115/#id-basics'>``XQuery
 *     1.0: An XML Query Language, Section 2''</a>). Such a
 *     sequence is also called a <i>singleton sequence</i>.\n
 *     .
 *     For performance reasons, Pathfinder does <i>not</i> implement
 *     all values as sequences (that possibly contain only one item).
 *     If we know (from static typing) that an expression will never
 *     evaluate to a sequence with more than one item, our mapping
 *     will use that actual item.
 *
 * <b>Boxing</b>
 *
 * The XML Query type system is quite flexible and allows situations
 * where the static typing information is not sufficient to decide
 * on a Monet implementation type as specified above at compile time.
 *
 * In such a case we use a technique that compiler constructors call
 * <i>boxing</i>. Instead of the value itself we use a ``pointer''
 * to the value, making it possible to find the actual value and its
 * type. For most compiler people, ``pointer'' actually means a
 * pointer, pointing to a memory location in the machine's RAM. Our
 * MIL code does something similar:
 *
 *  - We keep a number of global BATs that store the actual values.
 *    We need one of these BATs for each of the above Monet types
 *    (@c int, @c dbl, @c bit, @c str, @c node). The values are
 *    stored in the tail value of the corresponding global BAT.
 *    .
 *  - The head of these global BATs always has the Monet type
 *    @c oid. Internally, we use the type @c item here. When generating
 *    MIL output, however, we use @c oid to take advantage of Monet's
 *    @c void type. The @c oid value serves as a lookup value for
 *    the actual values.
 *    .
 *    In our MIL program, everywhere we are unsure about the dynamic
 *    type, we use the Monet type @c oid. Each @c oid key is unique
 *    over <i>all</i> the above mentioned global BATs. To find the
 *    actual value for any @c oid value, we perform a search in
 *    all the global BATs.
 *    .
 *    That last sentence seems to make this @c oid type horribly
 *    expensive, as five BATs have to be searched for each @c oid
 *    lookup. Pathfinder will use a bit encoding in the @c oid
 *    type with which we can immediately decide in which BAT to
 *    look for the value.
 *
 * <b>Sequences</b>
 *
 * As stated above, we will try to use simple values as much as
 * possible. Of course, sequence types will still dominate our MIL
 * programs. Sequences will be implemented as BATs in Monet. The
 * head type is @c void and will always contain @c nil values. In
 * fact, a sequence would actually only need one column. To save
 * memory, we don't use the head column, simply by setting it
 * @c void/@c nil.
 *
 * The tail column of our sequence BATs has one of the above
 * mentioned six types. Either one of the five simple types, or
 * @c item.
 *
 * A note on empty sequences: The BAT representation allows for a
 * straightforward implementation of the empty sequence: an empty
 * BAT. As we try to use simple values as often as possible, we
 * decided on another representation of the empty sequence. If
 * the (static typed) quantifier of an expression is <code>?</code>,
 * we also use a simple type to represent this value. If it actually
 * is the empty sequence, we use Monet's special value @c nil to
 * indicate this.
 *
 * Another small note: The decision sequence or not sequence is
 * done based on the static typing information. Of course, an
 * expression can evaluate to a singleton sequence when the quantifier
 * of its static type is, say, <code>+</code>. Our MIL code will still
 * use a BAT to represent this singleton sequence.
 *
 * @section runtime_requirements Requirements on the Pathfinder Runtime Module
 *
 * The mapping strategy described in the above section demands some
 * features from the Pathfinder runtime module that must be loaded
 * before executing Pathfinder generated MIL code.
 *
 * @subsection module_datatypes Data Types
 *
 * The Pathfinder extension module must provide cast functions to
 * cast between different types or representations:
 *
 * @verbatim
     int_bit    - convert an int to a bit
     str_bit    - convert a str to a bit
     dbl_bit    - convert a dbl to a bit
     item_bit   - unbox an item and convert it to a bit
     bit_int    - convert a bit to an int
     str_int    - convert a str to an int
     dbl_int    - convert a dbl to an int
     item_int   - unbox an item and convert it to an int
     bit_str    - convert a bit to a str
     int_str    - convert an int to a str
     dbl_str    - convert a dbl to a str
     item_str   - unbox an item and convert it to a str
     bit_dbl    - convert a bit to a dbl
     int_dbl    - convert an int to a dbl
     str_dbl    - convert a str to a dbl
     item_dbl   - unbox an item and convert it to a dbl
     item_node  - unbox an item and convert it to a node
     bit_item   - box a bit
     int_item   - box a int
     str_item   - box a str
     dbl_item   - box a dbl
     node_item  - box a node
@endverbatim
 *
 *
 * @subsection module_functions Functions
 *
 * The generated MIL program will contain various function calls
 * either to built-in functions that have to be provided by the
 * extension module, or to user functions that will be defined in
 * the generated MIL code as well.
 *
 * As XML Query uses QNames for function names, all of our built-in
 * functions are described below with QNames. The PFmilprint()
 * function will contain a mechanism to map QNames to valid MIL
 * function identifier. The precise mapping, however, is not yet
 * clear.
 *
 * @todo How do we map QNames to MIL identifiers?
 *
 * <b>Overloaded <code>insert</code> Function</b>
 *
 * Our mapping generates sequences with statements like
 *
 * @verbatim
   v.insert (x)  .
@endverbatim
 *
 * To make the Pathfinder source code, as well as the generated MIL
 * code more readable, we always use @c insert with only one
 * argument (here: @c x), regardless if we want to insert a single
 * value or a BAT. If we want to insert a single value, we actually
 * want to insert a BUN with @c nil head and the value in the tail,
 * iff the value is not @c nil. (Remember that @c nil represents the
 * empty sequence.) To achieve this, the @c insert function has to
 * be overloaded like
 *
 * @verbatim
   insert (BAT[void, any::1] a, any::1 b) : BAT[void, any::1] := {
       if (!isnil (b)) {
           a.insert (nil, b);
       }
   } 
@endverbatim
 *
 * <b>XPath Accessor Functions</b>
 *
 * The extension module has to provide the functions
 *
 * @verbatim
     pf:ancestor (context) : BAT[void, node]
     pf:ancestor-name (str, str, context) : BAT[void, node]
     pf:ancestor-or-self (context) : BAT[void, node]
     pf:ancestor-or-self-name (str, str, context) : BAT[void, node]
     pf:attribute (context) : BAT[void, node]
     pf:attribute-name (str, str, context) : BAT[void, node]
     pf:child (context) : BAT[void, node]
     pf:child-name (str, str, context) : BAT[void, node]
     pf:descendant (context) : BAT[void, node]
     pf:descendant-name (str, str, context) : BAT[void, node]
     pf:descendant-or-self (context) : BAT[void, node]
     pf:descendant-or-self-name (str, str, context) : BAT[void, node]
     pf:following (context) : BAT[void, node]
     pf:following-name (str, str, context) : BAT[void, node]
     pf:following-sibling (context) : BAT[void, node]
     pf:following-sibling-name (str, str, context) : BAT[void, node]
     pf:parent (context) : BAT[void, node]
     pf:parent-name (str, str, context) : BAT[void, node]
     pf:preceding (context) : BAT[void, node]
     pf:preceding-name (str, str, context) : BAT[void, node]
     pf:preceding-sibling (context) : BAT[void, node]
     pf:preceding-sibling-name (str, str, context) : BAT[void, node]
     pf:self (context) : BAT[void, node]
     pf:self-name (str, str, context) : BAT[void, node]  .
@endverbatim
 *
 * That evaluate the 12 XPath axes; each of them with and without
 * a built-in name test. All of them must be overloaded to allow
 * either a <code>BAT[void, node]</code> or a single @c node. (In
 * the latter case, a @c nil value must evaluate to an empty BAT.)
 *
 * The variants with built-in nametest carry <i>two</i> string
 * arguments. The first one is the queried namespace (as a full
 * URI), the second the local name.
 *
 * @todo Should we make sure that XPath steps always get unboxed
 *       nodes or should we allow boxed values as argument and
 *       do unboxing within the XPath step?
 *
 * @todo How do we implement ``don't care'' arguments here? In
 *       XML Query they are specified as `<code>*</code>'. Note
 *       that a <code>*</code> is different from, e. g., the
 *       empty namespace.
 *
 * We also need some functions that perform a test on the node
 * kinds. The semantic of each of them is to return all those
 * entries in @c context that have the requested node kind.
 *
 * @verbatim
     pf:comment-filter (context) : BAT[void, node]
     pf:text-filter (context) : BAT[void, node]
     pf:processing-instruction-filter (context) : BAT[void, node]
     pf:document-filter (context) : BAT[void, node]
     pf:element-filter (context) : BAT[void, node]
     pf:attribute-filter (context) : BAT[void, node]
@endverbatim
 *
 * The @c root function has to return the root node of the
 * document. (This function might get an argument in the future,
 * meaning the root node of the document containing the nodes
 * in @c context.) The return type is a sequence of nodes, all
 * of which have (XQuery semantics) type @c document.
 *
 * @verbatim
     pf:root () : BAT[void, node]
@endverbatim
 *
 * The following functions (effectively implementing an
 * <code>instance of</code> operator) have to be defined for
 * arguments of types @c item or <code>BAT[void, item]</code>.
 * If called with a BAT argument, the functions must only
 * return @c true if <i>all</i> items in the sequence have
 * the respective type. Possible implementations would be
 * lookups in the global BATs or evaluation of the lower
 * bits of the @c item type (see @ref mil_types above).
 *
 * @verbatim
     pf:is-boolean (...) : bit
     pf:is-integer (...) : bit
     pf:is-string (...) : bit
     pf:is-double (...) : bit
@endverbatim
 *
 * The following functions have analoguous semantic, but must
 * be defined for @c item, @c node and sequences thereof.
 *
 * @verbatim
     pf:is-text-node (...) : bit
     pf:is-processing-instruction-node (...) : bit
     pf:is-document-node (...) : bit
     pf:is-element (...) : bit
     pf:is-attribute (...) : bit
@endverbatim
 *
 * <b>Other Functions</b>
 *
 * The extension module will also have to provide some more
 * built-in functions and operators. It is not yet clear, however,
 * which of them.
 *
 * @subsection module_memory Memory Considerations
 *
 * Our compiler currently produces a chain of assignments, most
 * of them to temporary variables, until the final query result
 * is assigned to @c result and then printed. Currently we do
 * not generate any form of @c free() statements that may help
 * the extension module to clean up its data structures correctly.
 *
 * The scoping information that is necessary to generate these
 * calls is available, however, in the Pathfinder compiler. We'll
 * definitely use this knowlegde at some point to generate
 * necessary memory cleanup calls. Currently we do without these
 * calls to keep MIL and Pathfinder code simple.
 *
 */

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "pathfinder.h"
#include "core2mil.h"

#include "miltype.h"
#include "subtyping.h"
#include "mem.h"
#include "oops.h"

#define wildcard(qname) \
 ((qname).loc == NULL && (qname).ns.ns == NULL && (qname).ns.uri == NULL)

/**
 * Function pointer type for `assignment like' functions. Sensible
 * functions are assgn() and insert().
 */
typedef PFmnode_t * (*assgn_fn_t) (PFmnode_t *, PFmnode_t *);


/* ******** forward declarations ******** */

/* generic node construction functions */
static PFmnode_t * leaf (PFmtype_t, PFmty_t);
static PFmnode_t * wire1 (PFmtype_t, PFmty_t, PFmnode_t *);
static PFmnode_t * wire2 (PFmtype_t, PFmty_t, PFmnode_t *, PFmnode_t *);
static PFmnode_t * wire3 (PFmtype_t, PFmty_t,
                          PFmnode_t *, PFmnode_t *, PFmnode_t *);

/* these two are deprecated; use leaf() and wire1() instead */
static PFmnode_t * leaf_ (PFmtype_t);
static PFmnode_t * wire1_ (PFmtype_t, PFmnode_t *);

/* forward declaration of constructor functions */
static PFmnode_t * lit_int (int);
static PFmnode_t * lit_bit (bool);
static PFmnode_t * lit_str (char *);
static PFmnode_t * nil (void);
static PFmnode_t * var (PFvar_t *);
static PFmnode_t * assgn (PFmnode_t *, PFmnode_t *);
static PFmnode_t * batloop (PFmnode_t *, PFmnode_t *);
static PFmnode_t * fetch (PFmnode_t *, PFmnode_t *);
static PFmnode_t * comm_seq (PFmnode_t *, PFmnode_t *);
static PFmnode_t * ifthenelse (PFmnode_t *, PFmnode_t *, PFmnode_t *);
static PFmnode_t * ifthenelse_ (PFmnode_t *, PFmnode_t *, PFmnode_t *);
static PFmnode_t * insert (PFmnode_t *, PFmnode_t *);
static PFmnode_t * new_ (PFmnode_t *, PFmnode_t *);
static PFmnode_t * plus (PFmnode_t *, PFmnode_t *);
static PFmnode_t * equals (PFmnode_t *, PFmnode_t *);
static PFmnode_t * or (PFmnode_t *, PFmnode_t *);
static PFmnode_t * isnil (PFmnode_t *);
static PFmnode_t * count (PFmnode_t *);
static PFmnode_t * error (PFmnode_t *);
static PFmnode_t * type (PFmty_simpl_t);

static PFmnode_t * args (PFmnode_t *, PFmnode_t *);
static PFmnode_t * apply (PFqname_t, PFmnode_t *);

/* other helper functions */

/* create an empty sequence */
static PFmnode_t * new_seq (PFmty_t);
/* allocate a new variable */
static PFvar_t * new_var (PFmty_t impl_ty);

/* construct the empty sequence */
static PFmnode_t * empty (PFmty_t t);

/* generic mapping function, dispatching work to map_* functions */
static PFmnode_t * core2mil (assgn_fn_t, PFvar_t *, PFcnode_t *);
/* map any atom to a MIL atom */
static PFmnode_t * atom2mil (PFcnode_t * c);


/** create a MIL tree leaf node */
static PFmnode_t *
leaf (PFmtype_t kind, PFmty_t t)
{
    PFmnode_t *ret;

    ret = (PFmnode_t *) PFmalloc (sizeof (PFmnode_t));

    ret->kind = kind;
    ret->mty = t;

    return ret;
}

/**
 * Old implementation of leaf node construction function.
 *
 * @deprecated Use leaf() instead
 */
static PFmnode_t *
leaf_ (PFmtype_t kind)
{
    PFmnode_t *ret;

    ret = (PFmnode_t *) PFmalloc (sizeof (PFmnode_t));

    ret->kind = kind;

    return ret;
}

/** create a MIL tree node with one child */
static PFmnode_t *
wire1 (PFmtype_t kind, PFmty_t t, PFmnode_t * n)
{
    PFmnode_t * ret = leaf (kind, t);

    assert (ret); assert (n);
    ret->child[0] = n;

    return ret;
}

/**
 * Old implementation of node construction function.
 *
 * @deprecated Use wire1() instead
 */
static PFmnode_t *
wire1_ (PFmtype_t kind, PFmnode_t * n)
{
    PFmnode_t * ret = leaf_ (kind);

    assert (ret); assert (n);
    ret->child[0] = n;

    return ret;
}

/** create a MIL tree node with two children */
static PFmnode_t *
wire2 (PFmtype_t kind, PFmty_t t, PFmnode_t * n1, PFmnode_t * n2)
{
    PFmnode_t * ret = wire1 (kind, t, n1);

    assert (ret); assert (n2);
    ret->child[1] = n2;

    return ret;
}


/** create a MIL tree node with three children */
static PFmnode_t *
wire3 (PFmtype_t kind, PFmty_t t,
       PFmnode_t * n1, PFmnode_t * n2, PFmnode_t * n3)
{
    PFmnode_t * ret = wire2 (kind, t, n1, n2);

    assert (ret); assert (n3);
    ret->child[2] = n3;

    return ret;
}

/** Generate a MIL node representing a literal integer */
static PFmnode_t *
lit_int (int i)
{
    PFmnode_t * ret = leaf (m_lit_int,
                            (PFmty_t) { .ty = mty_int, .quant = mty_simple });
    ret->sem.num = i;

    return ret;
}

/** Generate a MIL node representing a literal boolean */
static PFmnode_t *
lit_bit (bool b)
{
    PFmnode_t * ret = leaf (m_lit_bit,
                            (PFmty_t) { .ty = mty_bit, .quant = mty_simple });
    ret->sem.tru = b;

    return ret;
}

/** Generate a MIL node representing a literal string */
static PFmnode_t *
lit_str (char * s)
{
    PFmnode_t * ret = leaf (m_lit_str,
                             (PFmty_t) { .ty = mty_str, .quant = mty_simple });
    ret->sem.str = PFstrdup (s);

    return ret;
}

/** create a new BAT according to our typing idea */
static PFmnode_t *
new_seq (PFmty_t t)
{
    /* new (void, t) */
    return new_ (type (mty_void), type (t.ty));
}

/** create a MIL nil node */
static PFmnode_t *
nil (void)
{
    return leaf (m_nil,
                 (PFmty_t) { .ty = mty_oid, .quant = mty_simple} );
}

/** MIL tree representation of a MIL variable */
static PFmnode_t *
var (PFvar_t * v)
{
    PFmnode_t * ret = NULL;

    assert (v);
    ret = leaf (m_var, v->impl_ty);
    ret->sem.var = v;

    return ret;
}

/**
 * Allocate a new variable and generate a unique name for it.
 *
 * @param impl_ty The MIL implementation type to set
 * @return Pointer to the newly allocated variable.
 */
static PFvar_t *
new_var (PFmty_t impl_ty)
{
    PFvar_t            *w = NULL;
    static unsigned int count = 0;
    char               *varname = NULL;


    varname = PFmalloc (sizeof("w_000"));
    snprintf (varname, sizeof("w_000"), "w_%03u", count++);

    w = PFnew_var (PFqname (PFns_pf, varname));
    w->impl_ty = impl_ty;

    return w;
}


/**
 * Constructor for `assignment' MIL nodes.
 *
 * Return implementation type is set to @c oid.
 *
 * @param v variable to assign the expression @a e to
 * @param e expression to assign to the variable @a v
 * @return MIL tree representation of <code>v := e</code>
 */
static PFmnode_t *
assgn (PFmnode_t * v, PFmnode_t * e)
{
    PFmnode_t *rhs = e;

    if (v->mty.quant == mty_simple) {

        if (e->mty.quant != mty_simple) {
            /* v->mty.quant == mty_simple && e->mty.quant == mty_sequence */
            PFinfo (OOPS_WARNING,
                    "casting sequence to simple value in assignment to $%s",
                    PFqname_str (v->sem.var->qname));
            rhs = fetch (rhs, lit_int (0));
        }

        if (v->mty.ty != e->mty.ty) {
            rhs = wire1 (m_cast, v->mty, rhs);
            rhs->sem.mty = v->mty;
        }

    } else {

        /* left hand side is sequence */

        if (v->mty.ty != e->mty.ty) {
            rhs = wire1 (e->mty.quant == mty_simple ? m_cast : m_fcast,
                         (PFmty_t) { .ty = v->mty.ty, .quant = rhs->mty.quant },
                         rhs);
            rhs->sem.mty = (PFmty_t) {.ty = v->mty.ty, .quant = rhs->mty.quant};
        }

        if (e->mty.quant != mty_sequence)
            rhs = wire2 (m_insert, v->mty, new_seq (v->mty), rhs);
    }

    return wire2 (m_assgn,
                  (PFmty_t) { .ty = mty_oid, .quant = mty_simple },
                  v, rhs);
}

/**
 * Constructor for `batloop' MIL nodes.
 *
 * Return implementation type is set to @c oid.
 *
 * @param bat MIL node of kind `variable' that should be iterated over
 * @param e   Expression to evaluate for each iteration
 * @return MIL tree representation of <code>bat@batloop { e }</code>
 */
static PFmnode_t *
batloop (PFmnode_t * bat, PFmnode_t * e)
{
    assert (bat->kind == m_var);

    if (bat->mty.quant != mty_sequence)
        PFoops (OOPS_FATAL, "Illegal types in batloop() constructor");

    return wire2 (m_batloop,
                  (PFmty_t) { .ty = mty_oid, .quant = mty_simple },
                  bat, e);
}

/**
 * Constructor for `fetch function' MIL nodes.
 *
 * Return implementation is the simple type variant of @a bat's
 * type. @a bat needs to have a sequence type.
 *
 * @param bat the BAT to fetch a value from. This argument
 *            must have a sequence type.
 * @param idx the index to fetch. This index starts from 0 and
 *            must be an integer.
 * @return MIL tree representation of <code>bat.fetch (idx)</code>
 */
static PFmnode_t *
fetch (PFmnode_t * bat, PFmnode_t * idx)
{
    if (bat->mty.quant != mty_sequence || idx->mty.quant != mty_simple
            || idx->mty.ty != mty_int)
        PFoops (OOPS_FATAL, "Illegal types in fetch() constructor");

    return wire2 (m_fetch,
                  (PFmty_t) { .ty = bat->mty.ty, .quant = mty_sequence },
                  bat, idx);
}

/**
 * Constructor for `command sequence' MIL nodes.
 *
 * Return implementation type is set to @c oid.
 *
 * @param e1 first MIL statement to execute
 * @param e2 second MIL statement to execute
 * @return MIL tree representation of <code>e1 ; e2</code>
 */
static PFmnode_t *
comm_seq (PFmnode_t * e1, PFmnode_t * e2)
{
    return wire2 (m_comm_seq,
                  (PFmty_t) { .ty = mty_oid, .quant = mty_simple },
                  e1, e2);
}

/**
 * Constructor for `if-then-else' MIL nodes.
 *
 * Return implementation type is set to @c oid.
 *
 * @param condition condition to meet for the `then' clause
 * @param e_then statement to execute if @a condition is @c true
 * @param e_else statement to execute if @a condition is @c false
 * @return MIL tree representation of
 *         <code>if (condition) { e_then } else { e_else }</code>
 */
static PFmnode_t *
ifthenelse (PFmnode_t * condition, PFmnode_t * e_then, PFmnode_t * e_else)
{
    if (condition->mty.ty != mty_bit || condition->mty.quant != mty_simple)
        PFoops (OOPS_FATAL, "condition in if-then-else has no boolean type");

    if (e_then->mty.ty != mty_oid || e_then->mty.quant != mty_simple)
        PFinfo (OOPS_WARNING,
                "`then' part in if-then-else does not have statement type");

    if (e_else->mty.ty != mty_oid || e_else->mty.quant != mty_simple)
        PFinfo (OOPS_WARNING,
                "`else' part in if-then-else does not have statement type");

    return wire3 (m_ifthenelse,
                  (PFmty_t) { .ty = mty_oid, .quant = mty_simple },
                  condition, e_then, e_else);
}


/**
 * Constructor for `ifthenelse_' MIL nodes. (This
 * is the ifthenelse() function of Monet's arith
 * module that implements C's 'condition ? e1 : e2'.)
 *
 * The implementation type of both operands needs
 * to be the same. (The function is declared
 * <code>ifthenelse (bit, any::1, any::1) : any::1</code>.)
 * The return implementation type is also set to that
 * same type.
 *
 * @param condition condition to meet for the `then' clause
 * @param e1 statement to return if @a condition is @c true
 * @param e2 statement to return if @a condition is @c false
 * @return MIL tree representation of
 *         <code>ifthenelse (condition, e1, e2)</code>
 */
static PFmnode_t *
ifthenelse_ (PFmnode_t * condition, PFmnode_t * e1, PFmnode_t * e2)
{
    if (condition->mty.ty != mty_bit || condition->mty.quant != mty_simple)
        PFoops (OOPS_FATAL, "condition in if-then-else has no boolean type");

    if (e1->mty.ty != e2->mty.ty || e1->mty.quant != e2->mty.quant)
        PFoops (OOPS_FATAL,
                "return conditions in boolean switch `ifthenelse (a, b, c)' "
                "must have the same type.");

    return wire3 (m_ifthenelse_,
                  e1->mty,
                  condition, e1, e2);
}

/**
 * Constructor for `insert' MIL nodes.
 *
 * Return implementation type is set to <code>BAT[void, t]</code>, where
 *   @c t is the type of @a bat.
 *
 * @param bat   MIL node of kind `variable' or `new' that the value should
 *              be inserted in.
 * @param value the value to insert into @a bat
 * @return MIL tree representation of <code>bat.insert (value)</code>
 */
static PFmnode_t *
insert (PFmnode_t * bat, PFmnode_t * value)
{
    PFmnode_t *rhs = value;

    assert (bat->kind == m_var || bat->kind == m_new);
    assert (bat->mty.quant == mty_sequence);

    if (bat->mty.ty != value->mty.ty) {
        rhs = wire1 (value->mty.quant == mty_simple ? m_cast : m_fcast,
                (PFmty_t) { .ty = bat->mty.ty, .quant = rhs->mty.quant },
                rhs);
        rhs->sem.mty = (PFmty_t) { .ty = bat->mty.ty, .quant = rhs->mty.quant };
    }

    return wire2 (m_insert, bat->mty, bat, rhs);
}

/**
 * Constructor for `new' MIL nodes.
 *
 * Return implementation type is set to <code>BAT[void, t]</code>, where
 *   @c t is the type of the new BAT.
 *
 * @param head MIL node of kind `type' specifying the head type
 *             of the new BAT
 * @param tail MIL node of kind `type' specifying the tail type
 *             of the new BAT
 * @return MIL tree representation of <code>new (head, tail)</code>
 */
static PFmnode_t *
new_ (PFmnode_t * head, PFmnode_t * tail)
{
    assert (head->kind == m_type && tail->kind == m_type);

    /*
     * Type nodes contain the type they represent in their
     * _semantic value_. Their expression type is always `oid'.
     */
    return wire2 (m_new,
                  (PFmty_t) { .ty = tail->sem.mty.ty, .quant = mty_sequence },
                  head, tail);
}

/**
 * Constructor for `arithmetic plus' MIL nodes.
 *
 * Return implementation type is type of parameter @a e1. Both
 * parameters must have the same implementation type and must
 * be simple values.
 *
 * @param e1 right side of the `+' operator
 * @param e2 left side of the `+' operator
 * @return MIL tree representation of <code>e1 + e2</code>
 */
static PFmnode_t *
plus (PFmnode_t * e1, PFmnode_t * e2)
{
    if (e1->mty.quant != mty_simple || e2->mty.quant != mty_simple
            || e1->mty.ty != e2->mty.ty)
        PFoops (OOPS_FATAL, "Illegal types in plus() constructor");

    return wire2 (m_plus, e1->mty, e1, e2);
}

/**
 * Constructor for `equality' MIL nodes.
 *
 * Return implementation type is boolean. Both
 * parameters must have the same implementation type and must
 * be simple values.
 *
 * @param e1 right side of the `=' operator
 * @param e2 left side of the `=' operator
 * @return MIL tree representation of <code>e1 = e2</code>
 */
static PFmnode_t *
equals (PFmnode_t * e1, PFmnode_t * e2)
{
    if (e1->mty.quant != mty_simple || e2->mty.quant != mty_simple
            || e1->mty.ty != e2->mty.ty)
        PFoops (OOPS_FATAL, "Illegal types in equals() constructor");

    return wire2 (m_equals,
                  (PFmty_t) { .ty = mty_bit, .quant = mty_simple },
                  e1, e2);
}

/**
 * Constructor for `or' MIL nodes.
 *
 * Return implementation type, as well as the implementation
 * type of both arguments must be boolean (simple valued).
 *
 * @param e1 right side of the `or' operator
 * @param e2 left side of the `or' operator
 * @return MIL tree representation of <code>e1 || e2</code>
 */
static PFmnode_t *
or (PFmnode_t * e1, PFmnode_t * e2)
{
    if (e1->mty.ty != mty_bit || e1->mty.quant != mty_simple)
        PFoops (OOPS_FATAL, "left hand type in `or' is not boolean");

    if (e2->mty.ty != mty_bit || e2->mty.quant != mty_simple)
        PFoops (OOPS_FATAL, "right hand type in `or' is not boolean");

    return wire2 (m_or, 
                  (PFmty_t) { .ty = mty_bit, .quant = mty_simple },
                  e1, e2);
}

/**
 * Constructor for `isnil' MIL nodes.
 *
 * Return implementation type is boolean. The operand must
 * have a simple value type.
 *
 * @param e operand
 * @return MIL tree representation of <code>isnil (e)</code>
 */
static PFmnode_t *
isnil (PFmnode_t * e)
{
    if (e->mty.quant != mty_simple)
        PFoops (OOPS_FATAL,
                "operand of isnil() function is not a simple value");

    return wire1 (m_isnil, 
                  (PFmty_t) { .ty = mty_bit, .quant = mty_simple },
                  e);
}

/**
 * Constructor for `count' MIL nodes.
 *
 * Monet's count() function counts the number of BUNs in a BAT.
 * The argument @a bat must therefore have a sequence type, the
 * return type will be a simple integer type.
 *
 * @param bat Operand to the count() function
 * @return MIL tree representation of <code>count (bat)</code>
 */
static PFmnode_t *
count (PFmnode_t * bat)
{
    if (bat->mty.quant != mty_sequence)
        PFoops (OOPS_FATAL,
                "argument in count() constructor must be a sequence");

    return wire1 (m_count,
                  (PFmty_t) { .ty = mty_int, .quant = mty_simple },
                  bat);
}

/**
 * Constructor for `error' MIL nodes.
 *
 * Monet's error() function generates a runtime error. The
 * string passed as an argument is printed. The implementation
 * type of the argument @a msg must therefore be a simple string
 * type. Although the error function does actually not return,
 * its return type is set to the statement type.
 *
 * @param msg Operand to the error() function
 * @return MIL tree representation of <code>error (msg)</code>
 */
static PFmnode_t *
error (PFmnode_t * msg)
{
    if (msg->mty.ty != mty_str || msg->mty.quant != mty_simple)
        PFoops (OOPS_FATAL,
                "argument in error() constructor must be a string");

    return wire1 (m_error,
                  (PFmty_t) { .ty = mty_oid, .quant = mty_simple },
                  msg);
}

/**
 * Constructor for `type' MIL nodes.
 *
 * Return implementation type is set to @c oid.
 *
 * @param t MIL simple type to represent
 * @return MIL tree representation of the type
 */
static PFmnode_t *
type (PFmty_simpl_t t)
{
    PFmnode_t * ret = leaf (m_type,
                            (PFmty_t) { .ty = mty_oid, .quant = mty_simple } );

    ret->sem.mty = (PFmty_t) { .ty = t, .quant = mty_simple };

    return ret;
}

/**
 * Create a MIL tree node representing a function call
 *
 * @param qname name of the function to call
 * @param args  argument list (a chain of `args' nodes constructed with
 *              args())
 * @return MIL tree representing the function call
 */
static PFmnode_t *
apply (PFqname_t qname, PFmnode_t * args)
{
    PFarray_t   *funs  = NULL;  /* "list" of functions bound to that QName */
    PFfun_t     *fun   = NULL;  /* function bound to that QName */
    unsigned int i     = 0;     /* count number of actual arguments */
    PFmnode_t   *arg   = args;  /* iterate over arguments */
    PFmnode_t   *ret   = NULL;  /* construct return value here */

    /* try to lookup this function in the function environment */
    funs = PFenv_lookup (PFfun_env, qname);

    if (funs == NULL)
        PFoops (OOPS_FATAL,
                "undefined function `%s' in mapping to MIL",
                PFqname_str (qname));

    /*
     * funs is a list of functions bound to that QName, we take
     * the first and only one
     */
    fun = *((PFfun_t **) PFarray_at (funs, 0));
    assert (fun);

    /* safety check: see if the number of arguments is correct */
    while (arg->kind != m_nil) {
        arg = arg->child[1];
        i++;
    }

    if (i != fun->arity)
        PFoops (OOPS_FATAL,
                "illegal arity for `%s' in mapping to MIL, "
                "expected %i, got %i", PFqname_str (qname), fun->arity, i);

    /*
     * The return type in the XQuery type system is encoded in
     * the PFfun_t struct. We map it to the MIL implementation
     * type here.
     */
    ret = wire1 (m_apply, PFty2mty (fun->ret_ty), args);

    ret->sem.fun = fun;

    return ret;
}

/**
 * Construct a MIL `args' node (to form chains of function arguments)
 */
static PFmnode_t *
args (PFmnode_t *a, PFmnode_t *b)
{
    if (b->kind != m_args && b->kind != m_nil)
        PFoops (OOPS_FATAL,
                "right child of `args' node is neither `args' node "
                "nor `nil' node");

    return wire2 (m_args,
                  (PFmty_t) { .ty = mty_oid, .quant = mty_simple },
                  a, b);
}


/**
 * Cast a MIL type to another one. Note that it does not
 * make sense to cast a sequence to a simple value (while it
 * does make sense the other way round).
 *
 * @todo Our implementation of the insert operator in Monet (either
 *       in MIL or in the Pathfinder extension module) has to take
 *       care not to insert @c nil values. It should be implemented
 *       like
 * <pre>
 * insert (BAT[void, any::1] a, any::1 b) : BAT[void, any::1] :=
 * {
 *     if (!isnil (b)) { a.insert (nil, b); }
 * }
 * </pre>
 *
 * @param from the original type
 * @param to   the destination type
 * @param e    the expression to cast
 * @return A MIL node representing the correctly casted expression
 */
static PFmnode_t *
cast (PFmty_t from, PFmty_t to, PFmnode_t * e)
{
    PFmnode_t * ret = e;

    if (from.ty != to.ty) {
        ret = wire1_ ((from.quant == mty_simple ? m_cast : m_fcast), ret);
        ret->mty.ty = to.ty;
        ret->mty.quant = from.quant;
    }

    if (from.quant != to.quant) {
        if (from.quant == mty_simple && to.quant == mty_sequence) {
            ret = insert (new_seq (to), ret);
        }
        else
            ret = ifthenelse_ (equals (count (e), lit_int (0)),
                    wire1 (m_cast,
                           (PFmty_t) { .ty = to.ty, .quant = mty_simple },
                           nil ()),
                    ifthenelse_ (equals (count (e), lit_int (1)),
                        fetch (e, lit_int (0)),
                        error (lit_str ("Sequences containing more than "
                                        "one item cannot be cast to simple "
                                        "values.")))
                    );
    }

    return ret;
}


/**
 * Create the appropriate representation for an empty sequence.
 * (For singleton sequences: a simple nil value, casted to the
 * correct type; for "real" sequences: an empty sequence of
 * correct type.)
 *
 * @param t The implementation type of the empty sequence.
 */
static PFmnode_t *
empty (PFmty_t t)
{
    PFmnode_t * ret = NULL;

    if (t.quant == mty_simple) {
        ret = wire1_ (m_cast, nil ());
        ret->mty = t;
    } else {
        ret = new_seq (t);
    }

    return ret;
}



/* ---------------------- mapping functions ----------------------- */

/**
 * Map a Core sequence construction to its MIL equivalent
 */
static PFmnode_t *
map_seq (assgn_fn_t assgn_fn, PFvar_t * v, PFcnode_t * n)
{
    PFmnode_t * ret = NULL;
    unsigned short i;

    /* we can only insert if v is represented as a BAT (i.e. it is a sequence */
    assert ((assgn_fn == assgn) || (v->impl_ty.quant == mty_sequence));

    if (v->impl_ty.quant == mty_sequence) {

        /* if this is an assignment, we need to create v */
        if (assgn_fn == assgn)
            ret = assgn (var (v), empty (n->impl_ty));

        /* then we can insert both operands */
        for (i =0; i <= 1; i++) {
            if (!PFty_subtype (n->child[i]->type, PFty_empty ())) {
                if (ret)
                    ret = comm_seq (ret,
                                    core2mil (insert, v, n->child[i]));
                else
                    ret = core2mil (insert, v, n->child[i]);
            }
        }

        assert (ret);
    }
    else {
        if (PFty_subtype (n->child[0]->type, PFty_empty ())) {
            ret = core2mil (assgn_fn, v, n->child[1]);
        } else {
            ret = core2mil (assgn_fn, v, n->child[0]);
        }
        assert (ret);
    }

#if 0
    if (n->impl_ty.quant == mty_sequence) {

        /* v := new (void, xxx)  --  Construct empty BAT */
        ret = assgn (var (v), empty (n->impl_ty));

        /* Now insert both operands */
        for (i = 0; i <= 1; i++) {

            /* if this is not the (static) empty sequence, insert its result */
            if (!PFty_subtype (n->child[i]->type, PFty_empty ())) {

                /* map the subexpression */
                ins = atom2mil (n->child[i]);

                ins = insert (var (v),
                              cast (ins->mty,
                                    (PFmty_t) { .ty = n->impl_ty.ty,
                                                .quant = ins->mty.quant },
                                    ins));

                ret = comm_seq (ret, ins);

            }   /* is operand a statically typed empty sequence? */
        }   /* iterate over operands */

    } else {

        /*
         * The whole thing has quantifier 1 or ?.
         * --> One argument must be the statically typed empty sequence.
         * --> Simply return mapping of the other one.
         */

        if (PFty_subtype (n->child[0]->type, PFty_empty ())) {
            if (PFty_subtype (n->child[1]->type, PFty_empty ())) {
                PFoops (OOPS_FATAL,
                        "`seq' applied to two empty sequences");
            }
            ret = core2mil (assgn, v, n->child[1]);
        } else {
            ret = core2mil (assgn, v, n->child[0]);
        }

    }  /* is the result a sequence or a simple type? */
#endif

    return ret;
}

/**
 * Map XQuery for-expressions that iterate over at most one item
 * to their MIL equivalent. map_for() dispatches the mapping of
 * for-expressions to either this function or to map_for_seq()
 * that maps "real" for's that iterate over (possibly) more than
 * one item.
 *
 * Note that this kind of for-expressions could be removed by a
 * query rewriting rule like
 *
 * @verbatim
     for $x [at $p] in a1 { item? } return e
   ==>
     typeswitch (a1)
         case item as $x return let $p := 1 return e
         case empty return ()
@endverbatim
 *
 * This function maps
 *
 * @verbatim
   v := for $x [at $p] in a1 return e
@endverbatim
 * 
 * to
 *
 * @verbatim
   v := new (void, <type>) / nil;    (depending on v's implementation type)
   if (not (isnil (a1))) {
     p := 1;                         (position is always one)
     x := a1;                        (a1 must be an atom)
     [[ v := e ]];                   (map recursively)
   }
@endverbatim
 *
 */
static PFmnode_t *
map_singleton_for (PFvar_t * v, PFcnode_t * c)
{
    PFmnode_t * ret = NULL;

    ret = comm_seq (assgn (var (c->child[0]->sem.var),
                           cast (c->child[2]->impl_ty,    /* x := a1 */
                                 c->child[0]->impl_ty,
                                 atom2mil (c->child[2]))),
                    core2mil (assgn, v, c->child[3]));    /* [[ v:=e ]] */

    /* only make assignment 'p := 1' if $p is given in core expression */
    if (c->child[1]->kind != c_nil)
        ret = comm_seq (assgn (var (c->child[1]->sem.var), lit_int (1)), ret);
    
    ret = comm_seq (assgn (var (v), empty (v->impl_ty)),
                    ifthenelse (wire1_ (m_not, wire1_ (m_isnil,
                                                       atom2mil (c->child[2]))),
                                ret,
                                nil ()));

    ret->child[1]  /* if-then-else node */
         ->child[0]  /* condition (not) */
           ->mty = (PFmty_t) { .ty = mty_bit, .quant = mty_simple };

    ret->child[1]  /* if-then-else node */
         ->child[0]  /* condition (not) */
           ->child[0]  /* condition (isnil) */
             ->mty = (PFmty_t) { .ty = mty_bit, .quant = mty_simple };

    return ret;
}

/**
 * Map an XQuery for-expression to its MIL equivalent
 * 
 * @verbatim
                                                        for
   v: = for $x [at $p] in a1 return e                /  |  |  \
                                                   $x  $p  a1  e
@endverbatim
 *
 * maps to
 *
 * @verbatim
   p := 0;
   v := new (void, <type>);
   a1@batloop {
     x = $t;
     p++;
     [[ w := e ]];
     v.insert (w);
   }
@endverbatim
 *
 */
static PFmnode_t *
map_sequence_for (assgn_fn_t assgn_fn, PFvar_t * v, PFcnode_t * c)
{
  /*PFvar_t   * w   = NULL;*/  /* newly created variable for mapping */ /* StM: unused! */
    PFvar_t   * x   = c->child[0]->sem.var; /* bind variable */
    PFvar_t   * p   = NULL;    /* positional variable (if available) */
    PFmnode_t * ret = NULL;    /* return expression */
    PFmnode_t * tail = NULL;   /* $t (tail) */

    /* allocate variable w */
    /*w = new_var (c->child[3]->impl_ty);*/ /* StM: unused! */

    /* construct expressiont `$t' (Monet's tail function) and set its type */
    tail = leaf_ (m_tail);
    tail->mty = (PFmty_t) { .ty = c->child[2]->impl_ty.ty,
                            .quant = mty_simple };

    if (c->child[1]->kind != c_nil) {     /* do we have a positional var? */

        p = c->child[1]->sem.var;

        /* batloop */
        ret = comm_seq (assgn_fn == assgn
                         ? comm_seq (assgn (var (p), lit_int (0)),
                                     assgn (var (v), new_seq (c->impl_ty)))
                         : assgn (var (p), lit_int (0)),
                        batloop (atom2mil (c->child[2]),
                                 comm_seq (
                                     comm_seq (
                                         assgn (var (x), tail),
                                         assgn (var (p), plus (var (p),
                                                               lit_int (1)))),
                                     core2mil (insert, v, c->child[3]))));
        /*
        ret = comm_seq (comm_seq (assgn (var (p), lit_int (0)),
                                  assgn (var (v), new_seq (c->impl_ty))),
                        batloop (atom2mil (c->child[2]),
                                 comm_seq (
                                     comm_seq (
                                         comm_seq (
                                             assgn (var (x), tail),
                                             assgn (var (p),
                                                    plus (var (p),
                                                          lit_int (1)))),
                                         core2mil (assgn, w, c->child[3])),
                                     insert (var (v), var (w)))));
         */
    } else {

        ret = batloop (atom2mil (c->child[2]),
                       comm_seq (assgn (var (x), tail),
                                 core2mil (insert, v, c->child[3])));

        if (assgn_fn == assgn)
            comm_seq (assgn (var (v), new_seq (c->impl_ty)),
                      ret);

        /*
        ret = comm_seq (assgn (var (v), new_seq (c->impl_ty)),
                        batloop (atom2mil (c->child[2]),
                                 comm_seq (
                                     comm_seq (assgn (var (x), tail),
                                               core2mil (assgn,
                                                         w, c->child[3])),
                                     insert (var (v), var (w)))));
        */
    }
    /* FIXME: Set types for both `insert' statements */

    return ret;
}

/**
 * Maps for-nodes to their MIL representation. Dispatches the actual
 * work to map_singleton_for() or map_sequence_for(), depending on
 * the type of the expression iterated over.
 */
static PFmnode_t *
map_for (assgn_fn_t assgn_fn, PFvar_t * v, PFcnode_t * c)
{
    if (c->child[2]->impl_ty.quant == mty_simple)
        return map_singleton_for (v, c);
    else
        return map_sequence_for (assgn_fn, v, c);
}

/**
 * Map if-then-else clauses to MIL.
 *
 * @verbatim
   v <- if a1 then e1 else e2
   @endverbatim
 *
 * is mapped to
 *
 * @verbatim
   v := nil;                declare v
   if a1 {
     [[ x1 <- e1 ]];        map recursively
     v := <cast>(x);        cast as required
   } else {
     [[ x2 <- e2 ]];        map recursively
     v := <cast>(x);        cast as required
   }
@endverbatim
 *
 */
static PFmnode_t *
map_ifthenelse (assgn_fn_t assgn_fn, PFvar_t * v, PFcnode_t * c)
{
    /**** map then-part first ****/

    return ifthenelse (atom2mil (c->child[0]),
                       core2mil (assgn_fn, v, c->child[1]),
                       core2mil (assgn_fn, v, c->child[2]));
}


/**
 * Map @c typeswitch clauses to MIL
 *
 * The @c typeswitch semantics described in the W3C drafts
 * 
 */
static PFmnode_t *
map_typesw (PFvar_t * v, PFcnode_t * c)
{
    (void)v; (void)c;
    assert (!"map_typesw() not implemented yet");
    return NULL;
}


/**
 * Find the XML Query type that closest corresponds to a given MIL
 * implementation type.
 * 
 * See @ref mil_types for the mapping table.
 */
static PFty_t
mty2ty (PFmty_simpl_t mty_ty)
{
    switch (mty_ty) {
        case mty_oid:
        case mty_void:  PFoops (OOPS_FATAL,
                                "illegal MIL implementation type encountered");
        case mty_bit:   return PFty_xs_boolean ();
        case mty_int:   return PFty_xs_integer ();
        case mty_str:   return PFty_xs_string ();
        case mty_dbl:   return PFty_xs_double ();
        case mty_node:  return PFty_xs_anyNode ();
        case mty_item:  return PFty_xs_anyItem ();
    }

    PFoops (OOPS_FATAL, "enum value %i not handled in switch", mty_ty);
}

/**
 * Map <code>instance of</code> clauses to MIL
 *
 * @note
 *   The <code>instance of</code> operator is actually a redundant
 *   operator in the Core language. It can be expressed as a
 *   @c typeswitch clause. During our experimental stage of MIL
 *   code generation, we will use a <code>instance of</code> in
 *   Core anyway to play around with conditions on XQuery types.
 *   The <code>instance of</code> should be removed from Core
 *   in the future
 *
 * The generated code followes the
 * <a href='http://www.w3.org/TR/xquery/#id-sequencetype-matching'>SequenceType
 * Matching</a> algorithm described in the
 * <a href='http://www.w3.org/TR/xquery/'>XQuery 1.0: An XML Query
 * Language</a> specification. Here described in a somewhat different
 * fashion.
 *
 * -# If the expression is the empty sequence then
 *    .
 *    -# If @c empty is a subtype of the SequenceType, we return @c true.
 *    -# If @c empty is not a subtype of the SequenceType, we return @c false.
 *    Note that these two cases are determined at compile time, while
 *    the test for the expression to be the empty sequence will be a
 *    runtime test.
 *    .
 * -# If the SequenceType is the empty sequence, we return @c false.\n
 *    (Note that <code>() instance of empty</code> is already handled
 *    above.)\n
 *    This is a compile time test.
 *    .
 * -# Do a test on the quantifiers: If the SequenceType is a subtype
 *    of <code>xs:anyItem?</code> and the quantifier of the expression
 *    is @c mty_sequence (so far a compile time decision), then return
 *    @c false, if <code>count (e)</code> does not equal 1.
 *    .
 * -# Do a test on the prime types. Note that the function calls need
 *    to be folded into the BAT if the expression has a sequence type.
 *    In some cases it might be necessary to do an explicit @c batloop.
 *    .
 *    -# If the XQuery type corresponding with the MIL implementation
 *       type is a subtype of the SequenceType, return @c true. The
 *       corresponding XQuery type is determined using mty2ty() using
 *       the mapping
 *       @verbatim
           bit   - xs:boolean
           int   - xs:integer
           str   - xs:string
           dbl   - xs:double
           node  - xs:anyNode
           item  - xs:anyItem
         @endverbatim
 *       (The MIL types @c oid and @c void are illegal here and will
 *       result in a runtime error.)\n
 *       Note that this is a decision that is taken at compile time.
 *       .
 *    -# If the implementation type is @c bit, @c int, @c str or @c dbl,
 *       return @c false (Compile time decision).
 *       .
 *    -# If the implementation type is @c item and SequenceType is not
 *       a subtype of <code>xs:anyNode</code> then return, corresponding
 *       to the SequenceType
 *       @verbatim
           SequenceType <: xs:boolean  --> is-boolean (e)
           SequenceType <: xs:integer  --> is-integer (e)
           SequenceType <: xs:string   --> is-string (e)
           SequenceType <: xs:double   --> is-double (e)
           SequenceType <: numeric     --> is-integer (e) || is-double (e)
         @endverbatim
 *       (Compile time decision)
 *       .
 *    -# If the SequenceType is @c text then return
 *       <code>is-text-node (e)</code>.
 *    -# If the SequenceType is @c processing-instruction then return
 *       <code>is-processing-instruction_node (e)</code>.
 *       (Processing Instructions may actually also have a target
 *       name. We'll simply omit this for the sake of simplicity.)
 *    -# If the SequenceType is @c comment then return
 *       <code>is-comment-node (e)</code>.
 *    -# If the SequenceType is @c document-node then return
 *       <code>is-document-node (e)</code>.
 *    -# If the SequenceType is a subtype of @c element then
 *       - If the SequenceType is @c element return
 *         <code>is-element-node (e)</code>.
 *       - If the SequenceType is <code>element <i>n</i></code>,
 *         where <code><i>n</i></code> is a QName, return
 *         <code>node-name-eq (n.ns, n.loc, e)</code>.
 *       - Otherwise return @c false.\n
 *         (This might seem strange on the first view, as we did not
 *         handle the SequenceType that restricts an element on its
 *         type annotation. But as we won't map @c validate expressions
 *         for now, we won't have any type annotations, so the
 *         condition here must evaluate to @c false.)
 *    -# Handle SequenceTypes that are a subtype of @c attribute
 *       equivalently. The node name of an attribute is preceded
 *       by the @@ sign.
 *    .
 *    For this to work, the above mentioned access functions must be
 *    overloaded, so that they take <code>node</code>s and
 *    <code>item</code>s.
 *    .
 *    (Remember that the SequenceType must be one of
 *    .
 *     - @c text, @c processing-instruction, @c comment,
 *       @c document-node
 *     - @c element
 *       (This may be further specified by a QName and/or a type.)
 *     - @c attribute
 *       (This may be further specified by a QName and/or a type.),
 *    .
 *    all the other cases have already been handled in the above
 *    steps.)
 *
 * @param v variable to assign the result of the <code>instance of</code>
 *          expression to. Will have a @c bit implementation type.
 * @param c the <code>instance of</code> clause to map.
 * @return MIL representation the <code>instance of</code> clause.
 */
static PFmnode_t *
map_instof (PFvar_t * v, PFcnode_t * c)
{
    PFcnode_t *e = c->child[0];
    bool       empty_allowed;
    PFmnode_t *ret = NULL;
    PFty_t     t = PFty_defn (c->child[1]->sem.type);
    PFty_t prime = PFty_prime (t);

    /*
     * Read this function bottom-up, as we first
     * construct the inner parts of our MIL code!
     */

    /*
     * If the SequenceType is the empty sequence, we return false.
     * (Note that '() instance of empty' will be handled below.
     */
    if (PFty_eq (prime, PFty_empty ())) 
        ret = assgn (var (v), lit_bit (false));
    /*
     * If the XQuery type corresponding with the MIL implementation
     * type is a subtype of the SequenceType, return true.
     */
    else if (PFty_subtype (mty2ty (e->impl_ty.ty), prime)) {
        PFinfo (OOPS_OK,
                "type of e: %s; type of prime: %s",
                PFty_str (mty2ty (e->impl_ty.ty)), PFty_str (prime));
        ret = assgn (var (v), lit_bit (true));
    }
    /*
     * If the implementation type is bit, int, str or dbl, return false.
     */
    else if (e->impl_ty.ty == mty_bit || e->impl_ty.ty == mty_int
             || e->impl_ty.ty == mty_str || e->impl_ty.ty == mty_dbl) {
#if 0
        PFinfo (OOPS_OK,
                "type of e: %s; type of prime: %s",
                PFty_str (mty2ty (e->impl_ty.ty)), PFty_str (prime));
#endif
        ret = assgn (var (v), lit_bit (false));
    }
    /*
     * If the implementation type is item and SequenceType is not
     * a subtype of xs:anyNode then return a function corresponding
     * to the SequenceType
     */
    else if (e->impl_ty.ty == mty_item
             && !PFty_subtype (prime, PFty_xs_anyNode ())) {
        /* SequenceType <: xs:boolean  --> is-boolean (e) */
        if (PFty_subtype (prime, PFty_xs_boolean ()))
            ret = assgn (var (v), apply (PFqname (PFns_pf, "is-boolean"),
                                         args (atom2mil (e), nil ())));
        /* SequenceType <: xs:integer  --> is-integer (e) */
        else if (PFty_subtype (prime, PFty_xs_integer ()))
            ret = assgn (var (v), apply (PFqname (PFns_pf, "is-integer"),
                                         args (atom2mil (e), nil ())));
        /* SequenceType <: xs:string  --> is-string (e) */
        else if (PFty_subtype (prime, PFty_xs_string ()))
            ret = assgn (var (v), apply (PFqname (PFns_pf, "is-string"),
                                         args (atom2mil (e), nil ())));
        /* SequenceType <: xs:double  --> is-double (e) */
        else if (PFty_subtype (prime, PFty_xs_double ()))
            ret = assgn (var (v), apply (PFqname (PFns_pf, "is-double"),
                                         args (atom2mil (e), nil ())));
        /* SequenceType <: numeric  --> is-integer || is-double (e) */
        else if (PFty_subtype (prime, PFty_numeric ()))
            ret = assgn (var (v),
                         or (apply (PFqname (PFns_pf, "is-integer"),
                                    args (atom2mil (e), nil ())),
                             apply (PFqname (PFns_pf, "is-double"),
                                    args (atom2mil (e), nil ()))));
        else
            PFoops (OOPS_FATAL,
                    "type %s not handled in instof-mapping",
                    PFty_str (prime));
    }
    /*
     * If the SequenceType is 'text' then return is-text-node (e)
     */
    else if (PFty_eq (prime, PFty_text ()))
        ret = assgn (var (v), apply (PFqname (PFns_pf, "is-text-node"),
                                     args (atom2mil (e), nil ())));
    /*
     * If the SequenceType is 'processing-instruction' then
     * return is-processing-instruction-node (e)
     */
    else if (PFty_eq (prime, PFty_pi ()))
        ret = assgn (var (v), apply (PFqname (PFns_pf,
                                              "is-processing-instruction-node"),
                                     args (atom2mil (e), nil ())));
    /*
     * If the SequenceType is 'comment' then return is-comment-node (e)
     */
    else if (PFty_eq (prime, PFty_comm ()))
        ret = assgn (var (v), apply (PFqname (PFns_pf, "is-comment-node"),
                                     args (atom2mil (e), nil ())));
    /*
     * If the SequenceType is 'document-node' then return is-document-node (e)
     */
    else if (PFty_eq (prime, PFty_doc (PFty_xs_anyType ())))
        ret = assgn (var (v), apply (PFqname (PFns_pf, "is-document-node"),
                                     args (atom2mil (e), nil ())));
    /*
     * If the SequenceType is a subtype of 'element', then
     * decide on the more detailed type specifications.
     */
    else if (PFty_subtype (prime, PFty_xs_anyElement ())) {

        /*
         * If the SequenceType is 'element' return `is-element-node (e)'
         * (prime <: element && element <: prime ==> prime == element)
         */
        if (PFty_subtype (PFty_xs_anyElement (), prime)) {
            ret = assgn (var (v), apply (PFqname (PFns_pf, "is-element"),
                                         args (atom2mil (e), nil ())));
        }

        /*
         * If the SequenceType is 'element n', where n is a QName,
         * return `node-name-eq (n.ns, n.loc, e)'
         */
        else if (!wildcard (PFty_qname (prime))
                 && PFty_subtype (PFty_xs_anyType (), PFty_child (prime))) {
            ret = assgn (var (v),
                    apply (PFqname (PFns_pf, "node-name-eq"),
                        args (lit_str (PFqname_uri (PFty_qname (prime))),
                            args (lit_str (PFqname_loc (PFty_qname (prime))),
                                args (atom2mil (e), nil ())))));
        }

        /*
         * Otherwise return false, as we haven't implemented
         * validation yet.
         */
        else {
            PFinfo (OOPS_WARNING,
                    "An `instance of' test was statically set to false.");
            PFinfo (OOPS_WARNING,
                    "The probable cause is a XML Schema type along with "
                    "an `element ...' type.");
            PFinfo (OOPS_WARNING,
                    "This might also indicate a bug in the `instance of' "
                    "mapping to MIL.");
            ret = assgn (var (v), lit_bit (false));
        }
    }
    else if (PFty_subtype (prime, PFty_xs_anyAttribute ())) {
        PFoops (OOPS_FATAL,
                "`instance of' not implemented for attribute types yet");
    }

    if (ret == NULL) {
        PFinfo (OOPS_FATAL, "problem in `instance of' mapping");
        PFinfo (OOPS_FATAL, "possible cause: unexpected type encountered");
        PFinfo (OOPS_FATAL, "this could also indicate a missing (not yet "
                            "implemented) optimization rule");
        PFinfo (OOPS_FATAL, "expression: e <%s> instance of %s",
                            PFty_str (mty2ty (e->impl_ty.ty)),
                            PFty_str (prime));
        PFoops (OOPS_FATAL, "prime type is: %s", PFty_str (prime));
    }

    /*
     * Test on the quantifiers: If the SequenceType is a subtype
     * of xs:anyItem? and the quantifier of the expression
     * mty_sequence (so far a compile time decision), then return
     * false, if count (e) does not equal 1. Otherwise go on with
     * our tests.
     */
    if (e->impl_ty.quant == mty_sequence
        && PFty_subtype (t, PFty_opt (PFty_xs_anyItem ()))) {
        ret = ifthenelse (equals (count (atom2mil (e)), lit_int (1)),
                          ret,
                          assgn (var (v), lit_bit (false)));
    }

    assert (ret);

    /*
     * Handle the empty sequence.
     *
     * In the resulting MIL program, this will actually be the
     * first check.
     */

    /*
     * is it actually possible (according to our static typing results)
     * to encounter the empty sequence here?
     */
    if (PFty_subtype (PFty_empty (), e->type)) {
        empty_allowed = PFty_subtype (PFty_empty (), t);

        if (e->impl_ty.quant == mty_simple)
            ret = ifthenelse (isnil (atom2mil (e)),
                              assgn (var (v), lit_bit (empty_allowed)),
                              ret);
        else
            ret = ifthenelse (equals (count (atom2mil (e)), lit_int (0)),
                              assgn (var (v), lit_bit (empty_allowed)),
                              ret);
    }

    return ret;
}


/**
 * Map a location step (with name/node test) to MIL
 *
 * @param step The Core node representing the XPath axis. It has one child
 *             that specifies name or node tests.
 * @param ctx  Context that this XPath step operates on.
 * @return A MIL expression representing this XPath step. (Note that
 *         we actually return an expression that has to be assigned
 *         somewhere, not a MIL statement.
 */
static PFmnode_t *
step2mil (PFcnode_t *step, PFcnode_t *ctx)
{
    /* our MIL functions that evaluate XPath steps without name tests */
    static char *simpl[] = {
          [c_ancestor]                "ancestor"
        , [c_ancestor_or_self]        "ancestor-or-self"
        , [c_attribute]               "attribute"
        , [c_child]                   "child"
        , [c_descendant]              "descendant"
        , [c_descendant_or_self]      "descendant-or-self"
        , [c_following]               "following"
        , [c_following_sibling]       "following-sibling"
        , [c_parent]                  "parent"
        , [c_preceding]               "preceding"
        , [c_preceding_sibling]       "preceding-sibling"
        , [c_self]                    "self"
    };

    /* MIL functions with a built-in name test */
    static char *named[] = {
          [c_ancestor]                "ancestor-nametest"
        , [c_ancestor_or_self]        "ancestor-or-self-nametest"
        , [c_attribute]               "attribute-nametest"
        , [c_child]                   "child-nametest"
        , [c_descendant]              "descendant-nametest"
        , [c_descendant_or_self]      "descendant-or-self-nametest"
        , [c_following]               "following-nametest"
        , [c_following_sibling]       "following-sibling-nametest"
        , [c_parent]                  "parent-nametest"
        , [c_preceding]               "preceding-nametest"
        , [c_preceding_sibling]       "preceding-sibling-nametest"
        , [c_self]                    "self-nametest"
    };

    /* MIL functions that do the kind tests */
    static char *kindt[] = {
          [c_kind_comment]         "comment-filter"
        , [c_kind_text]            "text-filter"
        , [c_kind_pi]              "processing-instruction-filter"
        , [c_kind_doc]             "document-filter"
        , [c_kind_elem]            "element-filter"
        , [c_kind_attr]            "attribute-filter"
    };

    PFmnode_t *ret = NULL;

    /* now create function calls with one or two arguments */
    if (step->child[0]->kind == c_namet)
        ret = apply (PFqname (PFns_pf, named[step->kind]),
                args (lit_str (PFqname_uri (step->child[0]->sem.qname)),
                    args (lit_str (PFqname_loc (step->child[0]->sem.qname)),
                        args (atom2mil (ctx), nil ()))));
    else {
        ret = apply (PFqname (PFns_pf, simpl[step->kind]),
                     args (atom2mil (ctx), nil ()));

        if (step->child[0]->kind != c_kind_node)
            ret = apply (PFqname (PFns_pf, kindt[step->child[0]->kind]),
                         args (ret, nil ()));
    }

    return ret;
}

/**
 * Map a locstep node from Core to MIL
 *
 * @verbatim
          locsteps
         /        \
      <axis>     context
       /
   <nodetest>
@endverbatim
 *
 */
static PFmnode_t *
map_locsteps (assgn_fn_t assgn_fn, PFvar_t *v, PFcnode_t *c)
{
    return assgn_fn (var (v), step2mil (c->child[0], c->child[1]));
}


/**
 * Map the empty sequence from Core to MIL.
 *
 * This is actually not as easy as it seems. We have two principal
 * ways to represent the empty sequence:
 *
 *  - We use Monet's @c nil value if the result should have the
 *    quantifiers @c ?.
 *  - We use an empty BAT if the result should have the quantifiers
 *    @c + or @c *.
 * 
 * Both variants, however, need to be correctly typed. (@c nil values
 * also have a type, and BATs need to be typed anyway.) However, as
 * the static type of the expression that should be mapped is usually
 * the empty sequence, we cannot determine a type.
 *
 * We get out of this problems by using the variable @a v to determine
 * the return type and rely on our calling function to enrich @a v's
 * data structure with the correct implementation type. <b>So be careful
 * when calling this function!</b>
 *
 * Note that the empty sequence constructor will not occur very often
 * in an optimized XQuery expression, as it will usually be thrown
 * away by the optimizer. A valid occurence might be:
 *
 * @verbatim
   if e1 then () else e2 .
@endverbatim
 *
 * @param v The variable the result expression should be assigned to.
 *          <b>Be sure to set the correct implementation type here!</b>
 * @param c The core expression to map. Should always be of kind
 *          @c c_empty.
 * @return A MIL tree fragment, that assigns the correct empty sequence
 *         representation (@c nil value or empty BAT) with correct type
 *         to the variable @a v.
 */
static PFmnode_t *
map_empty (assgn_fn_t assgn_fn, PFvar_t * v, PFcnode_t * c)
{
    return assgn_fn (var (v), empty (c->impl_ty));
}


/**
 * Represent an atom (literal constant or a variable) in MIL.
 * To be used for example in function calls, etc.
 */
static PFmnode_t *
atom2mil (PFcnode_t * c)
{
    PFmnode_t * ret = NULL;

    switch (c->kind)
    {
        case c_var:     ret = var (c->sem.var);
                        ret->mty = c->sem.var->impl_ty;
                        return ret;

        case c_lit_int: ret = leaf_ (m_lit_int);
                        ret->sem.num = c->sem.num;
                        ret->mty
                            = (PFmty_t) { .ty = mty_int, .quant = mty_simple };
                        return ret;

        case c_lit_str: ret = leaf_ (m_lit_str);
                        ret->sem.str = c->sem.str;
                        ret->mty
                            = (PFmty_t) { .ty = mty_str, .quant = mty_simple };
                        return ret;

        case c_lit_dec: PFinfo (OOPS_NOTICE,
                                "Implementing decimal as double: %g",
                                c->sem.dec);
                        ret = leaf_ (m_lit_dbl);
                        ret->sem.dbl = c->sem.dec;
                        ret->mty
                            = (PFmty_t) { .ty = mty_dbl, .quant = mty_simple };
                        return ret;

        case c_lit_dbl: ret = leaf_ (m_lit_dbl);
                        ret->sem.dbl = c->sem.dbl;
                        ret->mty
                            = (PFmty_t) { .ty = mty_dbl, .quant = mty_simple };
                        return ret;

        case c_true:
        case c_false:   ret = leaf_ (m_lit_bit);
                        ret->sem.tru = (c->kind == c_true);
                        ret->mty
                            = (PFmty_t) { .ty = mty_bit, .quant = mty_simple };
                        return ret;

        default:        break;
    }

    PFoops (OOPS_FATAL, "illegal core node type");
}

/**
 * Map a core expression to its MIL representation. This function
 * dispatches the actual work to other static functions within this
 * file.
 *
 * @param v variable the result should be assigned to
 * @param c core expression to map
 * @result MIL representation of the expression <b>@a $v := @a c</b>
 *
 * @bug Most of this is not implemented yet.
 */
static PFmnode_t *
core2mil (assgn_fn_t assgn_fn, PFvar_t * v, PFcnode_t * c)
{
    assert (v); assert (c);

    switch (c->kind) {

        case c_let:                     /* let binding */
            return comm_seq (core2mil (assgn,
                                       c->child[0]->sem.var,
                                       c->child[1]),
                             core2mil (assgn_fn, v, c->child[2]));

        case c_var:                     /* variable */
        case c_lit_int:                 /* integer literal */
        case c_lit_str:                 /* string literal */
        case c_lit_dec:                 /* decimal literal */
        case c_lit_dbl:                 /* double literal */
        case c_true:                    /* Built-in function 'true' */
        case c_false:                   /* Built-in function 'false' */
            return assgn_fn (var (v),
                             cast ((PFmty_t) { .ty = c->impl_ty.ty,
                                               .quant = c->impl_ty.quant },
                                   (PFmty_t) { .ty = v->impl_ty.ty,
                                               .quant = c->impl_ty.quant },
                                   atom2mil (c)));

        case c_empty:                   /* Built-in function 'empty' */
            return map_empty (assgn_fn, v, c);

        case c_seq:                     /* sequence construction */
            return map_seq (assgn_fn, v, c);

        case c_for:                     /* for binding */
            return map_for (assgn_fn, v, c);

        case c_ifthenelse:              /* if-then-else conditional */
            return map_ifthenelse (assgn_fn, v, c);

        case c_instof:                  /* 'instance of' operator */
            return map_instof (v, c);

        case c_locsteps:                /* path of location steps only */
            return map_locsteps (assgn_fn, v, c);

        case c_typesw:                  /* typeswitch clause */
            return map_typesw (v, c);

        case c_root:                    /* Built-in function 'root' */
            return assgn_fn (var (v),
                             apply (PFqname (PFns_pf, "root"), nil ()));

        case c_apply:                   /* function application */
        case c_arg:                     /* function argument (list) */

        case c_nil:                     /* end-of-sequence marker */
        case c_cases:                   /* case concatenation for typesw */
        case c_case:                    /* single case for typeswitch */
        case c_seqtype:                 /* a SequenceType*/
        case c_seqcast:                 /* cast along <: */
        case c_proof:                   /* type checker ony: prove <: rel. */


        /* xpath axes */
        case c_ancestor:
        case c_ancestor_or_self:
        case c_attribute:
        case c_child:
        case c_descendant:
        case c_descendant_or_self:
        case c_following:
        case c_following_sibling:
        case c_parent:
        case c_preceding:
        case c_preceding_sibling:
        case c_self:

        case c_kind_node:
        case c_kind_comment:
        case c_kind_text:
        case c_kind_pi:
        case c_kind_doc:
        case c_kind_elem:
        case c_kind_attr:

        case c_namet:                   /* name test */

        case c_error:                   /* Built-in function 'error' */

        case c_int_eq:                  /* Equal operator for integers */


            assert (!"Not implemented yet!");
    }

    PFoops (OOPS_FATAL, "illegal core tree node");
}

/** Map core expression @a c to a MIL program @a m. */
void
PFcore2mil (PFcnode_t * c, PFmnode_t ** m)
{
    PFvar_t * v;

    /* Tag each core tree node with MIL implementation type */
    PFmiltype (c);

#if 0
    /* This is for debugging only */
    if (PFty_subtype (PFty_integer (),  PFty_opt (PFty_item ())))
        PFoops (OOPS_NOTICE, "xs:integer <: xs:AnyItem?");
    else
        PFoops (OOPS_NOTICE, "xs:integer !<: xs:AnyItem?");
#endif

    /* the query result will be assigned to v */
    v = PFnew_var (PFqname (PFns_pf, "result"));
    v->impl_ty = c->impl_ty;

    /**
     * @bug We will run into problems if we map the statically typed
     *      empty sequence. We should treat this case separately.
     */

    /* start MIL mapping */
    *m = core2mil (assgn, v, c);

    *m = comm_seq (*m, wire1_ (m_print, var (v)));

    assert (*m);
}

/* vim:set shiftwidth=4 expandtab: */
