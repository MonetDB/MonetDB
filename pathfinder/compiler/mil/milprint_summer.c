/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */
/**
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
 * $Id$
 */

/*
 * IMPORTANT NOTE:
 *
 *   THE milprint_summer MODULE IS MAKING FREQUENT USE OF
 *   *DIRECT* ACCESS TO THE INTERNAL OF PATHFINDER'S TYPE
 *   SYSTEM.  THIS IS (AND ALWAYS WAS) A DEPRECATED MEANS
 *   TO INSPECT PATHFINDER'S STRUCTURED TYPES.
 *
 *   UPCOMING CHANGES OF THE INTERNALS OF OUR TYPE SYSTEM
 *   *WILL* BREAK ANY CODE THAT DOES SUCH DIRECT INSPECTION
 *   OF THE CONTENTS OF PFty_t STRUCTS!  MOST IMPORTANTLY,
 *   THIS AFFECTS THE ENUM VALUES IN THE type FIELD OF THE
 *   PFty_t STRUCT.  ALL CODE THAT DIRECTLY READS THE VALUES
 *   OF THIS FIELD NEEDS TO BE FIXED.
 *
 *   TO INDICATE PROBLEMATIC CASES, I HAVE TAGGED ALL INVALID
 *   ACCESSES WITH #ifdef PREPROCESSOR MACROS.  ONCE THE
 *   CLEANUP OF PATHFINDER'S TYPE SYSTEM WILL BE COMMITTED,
 *   THIS MACRO WILL BE #undef'd.
 *
 *   TO ACCESS TYPE INFORMATION IN A PROPER MANNER, PLEASE
 *   USE THE SUBTYPING ROUTINE PFty_subtype() AND OTHER
 *   ACCESSORS PROVIDED BY types.c AND subtyping.c (E.G.,
 *   PFty_prime()).  IF YOU NEED FURTHER ASSISTANCE,
 *   CONSULT THE ARCHIVES OF THE monetdb-devel LIST OR
 *   ASK ME (JENS).
 */
#define USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM 1

#include "pathfinder.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "milprint_summer.h"
#include "compile_interface.h"

#include "mem.h"
#include "array.h"
#include "pfstrings.h"
#include "oops.h"
#include "timer.h"
#include "subtyping.h"
#include "mil_opt.h"

/* format to print a long long */
#if defined(HAVE___INT64) | defined(__MINGW32__)
#define LLFMT "%I64d"           
#else
#define LLFMT "%lld"            
#endif

/* accessors to left and right child node */
#define LEFT_CHILD(p)  ((p)->child[0])
#define RIGHT_CHILD(p) ((p)->child[1])
/* accessor to the type of the node */
#define TY(p) ((p)->type)

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) (LEFT_CHILD(p))
/** starting from p, make a step right */
#define R(p) (RIGHT_CHILD(p))
/** starting from p, make a step down */
#define D(p) (LEFT_CHILD(p))
/** starting from p, make two steps left */
#define LL(p) L(L(p))
/** starting from p, make a step left, then a step right */
#define LR(p) R(L(p))
/** starting from p, make a step right, then a step left */
#define RL(p) L(R(p))
/** starting from p, make two steps right */
#define RR(p) R(R(p))
/** starting from p, make a step right, then a step down */
#define RD(p) D(R(p))
/** starting from p, make a step down, then a step left */
#define DL(p) L(L(p))
/* ... and so on ... */
#define DRL(p) L(R(L(p)))
#define RLL(p) L(L(R(p)))
#define RRL(p) L(R(R(p)))
#define RLR(p) R(L(R(p)))
#define LLR(p) R(L(L(p)))
#define RRR(p) R(R(R(p)))
#define LLL(p) L(L(L(p)))

#define RRRL(p) L(R(R(R(p))))
#define RRRRL(p) L(R(R(R(R(p)))))

/* starting level of user-defined functions */ 
#define UDF_LEV 1024
/* starting level of global variables */
#define GLO_LEV -1

/**
 * mps_error calls PFoops with more text, since something is wrong
 * in the translation, if this message appears.
 */
static void mps_error(const char *format, ...)
{
    int j, i = strlen(format) + 80;
    char *msg = PFmalloc(i);
    va_list ap;

    /* take in a block of MIL statements */
    va_start(ap, format);
    j = vsnprintf(msg, i, format, ap);
    va_end (ap);
    while (j < 0 || j > i) {
            if (j > 0)      /* C99 */
                    i = j + 1;
            else            /* old C */
                    i *= 2;
            msg = PFrealloc(i, msg);
            va_start(ap, format);
            j = vsnprintf(msg, i, format, ap);
            va_end (ap);
    } 
    PFoops (OOPS_FATAL,
            "The Pathfinder compiler experienced \n"
            "an internal problem in the MIL code generation.\n"
            "You may want to report this problem to the Pathfinder \n"
            "development team (pathfinder@pathfinder-xquery.org).\n\n"
            "When reporting problems, please attach your XQuery input,\n"
            "as well as the following information:\n"
            "%s\n\n"
            "We apologize for the inconvenience...\n", msg);
}

static int
translate2MIL (opt_t *f, int code, int cur_level, int counter, PFcnode_t *c);
static int 
var_is_used (PFvar_t *v, PFcnode_t *e);
int PFqueryType(PFcnode_t *c);

/* throw away out-of date flwr information. return the relevant level */
static void 
add_flwr_level(opt_t *f, int cur_level) 
{
    f->flwr_level[cur_level] = -1; /* init the current level; but keep innerjoins */
    f->flwr_depth = cur_level+1;
}

#if 0 /* DISABLED */
static int 
get_flwr_level(opt_t *f, int cur_level) 
{
    /* look up the highest still active flwr */
    int i = (cur_level < f->flwr_depth)?cur_level:f->flwr_depth-1; 
    for(; i >= 0; i--)
        if (f->flwr_level[i] > 0) return f->flwr_level[i];
    return 0;
}
#endif

/* tests type equality (not only structural 'PFty_eq' 
   and without hierarchy 'PFty_subtype' */
#define TY_EQ(t1,t2) (PFty_equality (t1,t2))

/**
 * Test if two types are castable into each other
 *
 * @param input_type the type from which it is casted
 * @param cast_type the type to which it is casted
 * @return the result of the castable test
 */
static bool
castable (PFty_t input_type, PFty_t cast_type)
{
    return ((PFty_subtype (cast_type, PFty_opt (PFty_xs_boolean ())) ||
             PFty_subtype (cast_type, PFty_opt (PFty_xs_integer ())) ||
             PFty_subtype (cast_type, PFty_opt (PFty_xs_decimal ())) ||
             PFty_subtype (cast_type, PFty_opt (PFty_xs_double ())) ||
             PFty_subtype (cast_type, PFty_opt (PFty_untypedAtomic ())) ||
             PFty_subtype (cast_type, PFty_opt (PFty_xs_string ()))) &&
             PFty_subtype (input_type, PFty_opt (PFty_atomic ())));
}

/* (return) code, which holds the 
   information about the interface chosen */
#define ITEM_ORDER -1 /* scj interface `item|iter' representation */
#define NORMAL      0 /* normal `iter|pos|item|kind' interface */
#define VALUES      1 /* direct value interface `iter|pos|item-value|kind' */
#define NODE       32 /* element/attribute construction interface */
#define BOOL  3
#define INT   4
#define DEC   5
#define DBL   6
#define STR   7
#define U_A   8

/**
 * val_join returns the string, which maps the
 * reference values in item to their values
 * according to a given kind
 *
 * @param i the kind of the values, which are joined
 * @result the string mapping the references to
 *         their values
 */
static char *
val_join (int i)
{
    if (i == INT)
        return ".leftfetchjoin(int_values)";
    else if (i == DEC)
        return ".leftfetchjoin(dec_values)";
    else if (i == DBL)
        return ".leftfetchjoin(dbl_values)";
    else if (i == STR)
        return ".leftfetchjoin(str_values)";
    else if (i == U_A)
        return ".leftfetchjoin(str_values)";
    else if (i == NODE)
        mps_error ("val_join: NODE is no valid reference.");
    else if (i == VALUES)
        mps_error ("val_join: VALUES is no valid reference.");
    else
        mps_error ("val_join: no valid reference.");

    return ""; /* to pacifiy compilers */
}

/**
 * kind_str returns a string containing an 
 * extension for the item name according to their
 * kind (to avoid item bats with different types)
 *
 * @param i the kind of the values
 * @result the string mapping the references to
 *         their values
 */
static char *
kind_str (int i)
{
    if (i == NORMAL)
        return "";
    else if (i == BOOL)
        return "";
    else if (i == INT)
        return "_int_";
    else if (i == DEC)
        return "_dec_";
    else if (i == DBL)
        return "_dbl_";
    else if (i == STR)
        return "_str_";
    else if (i == U_A)
        return "_str_";
    else if (i == NODE)
        mps_error ("kind_str: NODE is no valid reference.");
    else if (i == VALUES)
        mps_error ("kind_str: VALUES is no valid reference.");
    else
        mps_error ("kind_str: no valid reference (%i).", i);

    return ""; /* to pacifiy compilers */
}

/**
 * get_kind returns the kind of a core node using the type
 * annotation
 *
 * @param t the type of a core node
 * @result the number indicating the kind extracted from
 *         the type information
 */
static int
get_kind (PFty_t t)
{
    if (PFty_subtype (t, PFty_star (PFty_xs_integer ())))
        return INT;
    else if (PFty_subtype (t, PFty_star (PFty_xs_string ())))
        return STR;
    else if (PFty_subtype (t, PFty_star (PFty_xs_double ())))
        return DBL;
    else if (PFty_subtype (t, PFty_star (PFty_xs_decimal ())))
        return DEC;
    else if (PFty_subtype (t, PFty_star (PFty_untypedAtomic ())))
        return U_A;
    else if (PFty_subtype (t, PFty_star (PFty_xs_boolean ())))
        return NORMAL;
    else if (PFty_subtype (t, PFty_star (PFty_node ())))
        return NORMAL;
    else
        mps_error ("get_kind: can't recognize type '%s'.",
                   PFty_str(t));

    return 0; /* to pacifiy compilers */
}

/**
 * combinable tests wether two types can be merged 
 * (e.g. in a sequence operator) without using the
 * references to the values and returns their common
 * kind.
 * 
 * @param t the type of a core node
 * @result the number indicating the kind extracted from
 *         the type information
 */
static int
combinable (PFty_t t1, PFty_t t2)
{
#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
    int rc;
    if ((PFty_prime(PFty_defn(t1))).type == ty_choice ||
        (PFty_prime(PFty_defn(t2))).type == ty_choice)
        return NORMAL;
    else if ((rc = get_kind (t1)) == get_kind (t2))
        return rc;
    else
#else
    (void) t1; (void) t2;
#endif
        return NORMAL;
}

/* container for type information */
struct type_co {
    int  kind;         /* kind of the container */
    char *table;       /* variable name of the corresponding table of values */
    char *mil_type;    /* implementation type */
    char *mil_cast;    /* the MIL constant representing the type */
    char *name;        /* the full name of the type to generate error messages */
};
typedef struct type_co type_co;

static type_co
int_container ()
{
    type_co new_co;
    new_co.kind = INT;
    new_co.table = "int_values";
    new_co.mil_type = "lng";
    new_co.mil_cast = "INT";
    new_co.name = "integer";

    return new_co;
}

static type_co
dbl_container ()
{
    type_co new_co;
    new_co.kind = DBL;
    new_co.table = "dbl_values";
    new_co.mil_type = "dbl";
    new_co.mil_cast = "DBL";
    new_co.name = "double";

    return new_co;
}

static type_co
dec_container ()
{
    type_co new_co;
    new_co.kind = DEC;
    new_co.table = "dec_values";
    new_co.mil_type = "dbl";
    new_co.mil_cast = "DEC";
    new_co.name = "decimal";

    return new_co;
}

static type_co
str_container ()
{
    type_co new_co;
    new_co.kind = STR;
    new_co.table = "str_values";
    new_co.mil_type = "str";
    new_co.mil_cast = "STR";
    new_co.name = "string";

    return new_co;
}

static type_co
u_A_container ()
{
    type_co new_co;
    new_co.kind = U_A;
    new_co.table = "str_values";
    new_co.mil_type = "str";
    new_co.mil_cast = "U_A";
    new_co.name = "untypedAtomic";

    return new_co;
}

static type_co
bool_container ()
{
    type_co new_co;
    new_co.kind = BOOL;
    new_co.table = 0;
    new_co.mil_type = "bit";
    new_co.mil_cast = "BOOL";
    new_co.name = "boolean";

    return new_co;
}

/**
 * kind_container returns a the container
 * according to the kind value
 *
 * @param i the kind of the values
 * @result the string mapping the references to
 *         their values
 */
static type_co
kind_container (int i)
{
    if (i == INT)
        return int_container();
    else if (i == DEC)
        return dec_container();
    else if (i == DBL)
        return dbl_container();
    else if (i == STR)
        return str_container();
    else if (i == U_A)
        return u_A_container();
    else if (i == BOOL)
        return bool_container();
    else if (i == NODE)
        mps_error ("kind_container: NODE is no valid reference.");
    else if (i == VALUES)
        mps_error ("kind_container: VALUES is no valid reference.");
    else
        mps_error ("kind_container: no valid reference (%i).", i);

    return int_container(); /* to pacifiy compilers */
}



/**
 * Worker function for saveResult, which can
 * cope with item bat, holding `real' values
 * (saveResult_ should be used in pairs with deleteResult_
 * since the first one opens a scope and the latter one closes it)
 *
 * @param f the Stream the MIL code is printed to
 * @param counter the actual offset of saved variables
 * @param rcode the kind of the item bat
 */
static void
saveResult_ (opt_t *f, int counter, int rcode)
{
    char *item_ext = kind_str(rcode); /* item_ext = "" */

    milprintf(f, "{ # saveResult%i () : int\n", counter);
    milprintf(f, "var ipik%03u := ipik;\n", counter);
    milprintf(f, "var iter%03u := iter;\n", counter);
    milprintf(f, "var pos%03u := pos;\n", counter);
    milprintf(f, "var item%s%03u := item%s;\n", 
              item_ext, counter, item_ext);
    milprintf(f, "var kind%03u := kind;\n", counter);
    milprintf(f, "# end of saveResult%i () : int\n", counter);
}

/**
 * saveResult binds a intermediate result to a set of
 * variables, which are not used
 * (saveResult should be used in pairs with deleteResult
 * since the first one opens a scope and the latter one closes it)
 *
 * @param f the Stream the MIL code is printed to
 * @param counter the actual offset of saved variables
 */
static void
saveResult (opt_t *f, int counter)
{
   saveResult_ (f, counter, NORMAL);
}

/**
 * saveResult_node binds a intermediate result to a new
 * set of variables using the node interface
 * (saveResult should be used in pairs with deleteResult
 * since the first one opens a scope and the latter one closes it)
 *
 * @param f the Stream the MIL code is printed to
 * @param counter the actual offset of saved variables
 */
static void
saveResult_node (opt_t *f, int counter)
{
    milprintf(f, "{ # saveResult_node%i () : int\n", counter);
    milprintf(f,
            "var _elem_iter%03u   := _elem_iter  ;\n"
            "var _elem_size%03u   := _elem_size  ;\n"
            "var _elem_level%03u  := _elem_level ;\n"
            "var _elem_kind%03u   := _elem_kind  ;\n"
            "var _elem_prop%03u   := _elem_prop  ;\n"
            "var _elem_cont%03u   := _elem_cont  ;\n"
            "var _attr_iter%03u   := _attr_iter  ;\n"
            "var _attr_qn%03u     := _attr_qn    ;\n"
            "var _attr_prop%03u   := _attr_prop  ;\n"
            "var _attr_cont%03u   := _attr_cont  ;\n"
            "var _attr_own%03u    := _attr_own   ;\n"
            "var _r_attr_iter%03u := _r_attr_iter;\n"
            "var _r_attr_qn%03u   := _r_attr_qn  ;\n"
            "var _r_attr_prop%03u := _r_attr_prop;\n"
            "var _r_attr_cont%03u := _r_attr_cont;\n",
            counter, counter, counter, counter, counter, counter,
            counter, counter, counter, counter, counter,
            counter, counter, counter, counter);
    milprintf(f, "# end of saveResult_node%i () : int\n", counter);
}

/**
 * Worker function for deleteResult, which can
 * cope with item bat, holding `real' values
 * (deleteResult_ should be used in pairs with saveResult_
 * since the first one opens a scope and the latter one closes it)
 *
 * @param f the Stream the MIL code is printed to
 * @param counter the actual offset of saved variables
 * @param rcode the kind of the item bat
 */
static void
deleteResult_ (opt_t *f, int counter, int rcode)
{
    (void) rcode;
    milprintf(f, "} # end of deleteResult%i ()\n", counter);
}

/**
 * deleteResult deletes a saved intermediate result and
 * gives free the offset to be reused (return value)
 * (deleteResult should be used in pairs with saveResult
 * since the first one opens a scope and the latter one closes it)
 *
 * @param f the Stream the MIL code is printed to
 * @param counter the actual offset of saved variables
 */
static void
deleteResult (opt_t *f, int counter)
{
   deleteResult_ (f, counter, NORMAL);
}

/**
 * deleteResult_node frees the variables bound
 * to an intermediate result
 * (deleteResult should be used in pairs with saveResult
 * since the first one opens a scope and the latter one closes it)
 *
 * @param f the Stream the MIL code is printed to
 * @param counter the actual offset of saved variables
 */
static void
deleteResult_node (opt_t *f, int counter)
{
    milprintf(f, "} # end of deleteResult_node%i ()\n", counter);
}

/**
 * addValues inserts values into a table which are not already in
 * in the table (where the tail is supposed to be key(true)) and gives
 * back the offsets for all values
 *
 * @param f the Stream the MIL code is printed to
 * @param t_co t_co.table the variable name where the entries are inserted
 *             t_co.mil_type the type of the entered values
 * @param varname the variable which holds the input entries and
 *        gets the offsets of the added items
 * @param var_type the type of the entered values
 * @param result_var the variable, to which the result is bound
 */
static void
addValues (opt_t *f, 
           type_co t_co,
           char *varname,
           char *result_var)
{
    /* get the offsets of the values */
    milprintf(f, "%s := %s.addValues(%s).tmark(0@0);\n", result_var, t_co.table, varname);
}

/**
 * add_empty_strings adds an empty string for each missing iteration
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the kind of the item bat
 * @param cur_level the level of the for-scope
 */
static void
add_empty_strings (opt_t *f, int rc, int cur_level)  
{
    char *item_ext = kind_str(rc);
    milprintf(f,
            /* test qname and add "" for each empty item */
            "if (iter.count() != loop%03u.count())\n"
            "{ # add empty strings\n"
            "var difference := reverse(tdiff(loop%03u,iter)).hmark(0@0);\n"
            "var res_mu := merged_union(iter.chk_order(), difference.chk_order(), item%s, %s);\n"
            "item%s := res_mu.fetch(1);\n" /* CONST? */
            "} # end of add empty strings\n",
            cur_level, cur_level,
            item_ext,
            (rc)?"\"\"":"EMPTY_STRING",
            item_ext);
}

/**
 * Worker function for translateEmpty, which can
 * cope with item bat, holding `real' values
 * 
 * @param f the Stream the MIL code is printed to
 * @param rcode the kind of the item bat
 */
static void
translateEmpty_ (opt_t *f, int rcode)
{
    char *item_ext = kind_str(rcode);

    milprintf(f,
            "# translateEmpty ()\n"
            "ipik := empty_bat;\n"
            "iter := empty_bat;\n"
            "pos := empty_bat;\n"
            "item%s := empty%s_bat;\n"
            "kind := empty_kind_bat;\n",
            item_ext, item_ext);
}

/**
 * translateEmpty translates the empty sequence and gives back 
 * empty bats for the intermediate result (iter|pos|item|kind)
 * 
 * @param f the Stream the MIL code is printed to
 */
static void
translateEmpty (opt_t *f)
{
    translateEmpty_ (f, NORMAL);
}

/**
 * translateEmpty_node translates the empty sequence and gives back 
 * empty bats for the intermediate result of the node interface
 * (elem: iter|size|level|kind|cont, attr: iter|qn|prop|cont|own,
 *  root attributes: iter|qn|prop|cont)
 * 
 * @param f the Stream the MIL code is printed to
 */
static void
translateEmpty_node (opt_t *f)
{
    milprintf(f,
            "{ # translateEmpty_node ()\n"
            "_elem_iter  := empty_bat;\n"
            "_elem_size  := empty_bat.project(int_nil);\n"
            "_elem_level := empty_bat.project(chr_nil);\n"
            "_elem_kind  := empty_bat.project(chr_nil);\n"
            "_elem_prop  := empty_bat;\n"
            "_elem_cont  := empty_bat;\n"
            "_attr_iter  := empty_bat;\n"
            "_attr_qn    := empty_bat;\n"
            "_attr_prop  := empty_bat;\n"
            "_attr_cont  := empty_bat;\n"
            "_attr_own   := empty_bat;\n"
            "_r_attr_iter := empty_bat;\n"
            "_r_attr_qn   := empty_bat;\n"
            "_r_attr_prop := empty_bat;\n"
            "_r_attr_cont := empty_bat;\n"
            "} # end of translateEmpty_node ()\n");
}

/**
 * translateElemContent is a wrapper which ensures that
 * the NODE interface isn't used where it is unknown

 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the Core node containing the rest of the subtree
 * @result the kind indicating, which result interface is chosen
 */
static int
translateElemContent (opt_t *f, int code, int cur_level, int counter, PFcnode_t *c)
{
    if (c->kind == c_elem  ||
        c->kind == c_attr  ||
        c->kind == c_text  ||
        c->kind == c_seq   ||
        c->kind == c_empty ||
        (c->kind == c_apply &&
         (!PFqname_eq(c->sem.fun->qname,
                      PFqname (PFns_pf,"item-sequence-to-node-sequence")) ||
          !PFqname_eq(c->sem.fun->qname,
                      PFqname (PFns_pf,"merge-adjacent-text-nodes")))))
        return translate2MIL (f, code, cur_level, counter, c);
   else
        return translate2MIL (f, NORMAL, cur_level, counter, c);
}

/**
 * map2NODE_interface maps nodes from the iter|pos|item|kind
 * interface to the NODE interface
 * (r_attr: iter|qn|prop|cont,
 *  elem:   iter|size|level|kind|prop|cont,
 *  attr:   iter|qn|prop|cont|own)
 *
 * @param f the Stream the MIL code is printed to
 */
static void
map2NODE_interface (opt_t *f)
{
    milprintf(f,
            "{ # map2NODE_interface (counter)\n"
            /* there can be only nodes and attributes */

            /* create the attribute root entries */
            "kind := kind.materialize(ipik);\n"
            "var attr := kind.get_type(ATTR).hmark(0@0);\n"
            "var attr_iter := attr.leftfetchjoin(iter).materialize(attr);\n"
            "var attr_item := attr.leftfetchjoin(item).materialize(attr);\n"
            "var attr_cont := attr.leftfetchjoin(kind).get_container();\n"
            "attr := nil;\n"
            "_r_attr_iter := attr_iter;\n"
            "_r_attr_qn   := mposjoin(attr_item, attr_cont, ws.fetch(ATTR_QN));\n"
            "_r_attr_prop := mposjoin(attr_item, attr_cont, ws.fetch(ATTR_PROP));\n"
            "_r_attr_cont := mposjoin(attr_item, attr_cont, ws.fetch(ATTR_CONT));\n"
            "attr_iter := nil;\n"
            "attr_item := nil;\n"
            "attr_cont := nil;\n"


            "var nodes := kind.get_type(ELEM);\n"
            /* if no nodes are found we jump right to the end and only
               have to execute the stuff for the root construction */
            "if (nodes.count() != 0) {\n"
    
            "var oid_oid := nodes.hmark(0@0);\n"
            "nodes := nil;\n"
            "var node_items := oid_oid.leftfetchjoin(item).materialize(oid_oid);\n"
            "var node_conts := oid_oid.leftfetchjoin(kind).get_container();\n"
            /* set iter to a distinct list and therefore don't
               prune any node */
            "var iter_input := oid_oid.mirror();\n"

            /* get all subtree copies */
            "var res_scj := loop_lifted_descendant_or_self_step"
            "(iter_input, node_items, constant2bat(node_conts), ws, 0);\n"

            "iter_input := nil;\n"
            /* variables for the result of the scj */
            "var res_iter := res_scj.fetch(0);\n" /* CONST? */
            "var res_item := res_scj.fetch(1);\n" /* CONST? */
            "var res_cont := res_scj.fetch(2);\n" /* CONST? */
            /* res_ec is the iter|dn table resulting from the scj */

            /* create subtree copies for all bats except content_level */
            "_elem_iter  := res_iter.leftfetchjoin(oid_oid).leftfetchjoin(iter).materialize(res_item).chk_order();\n"
            "_elem_size  := mposjoin(res_item, res_cont, ws.fetch(PRE_SIZE));\n"
            "_elem_size  := correct_sizes(res_iter, res_item, _elem_size);\n"
            "_elem_kind  := mposjoin(res_item, res_cont, ws.fetch(PRE_KIND));\n"
            "_elem_prop  := mposjoin(res_item, res_cont, ws.fetch(PRE_PROP));\n"
            "_elem_cont  := mposjoin(res_item, res_cont, ws.fetch(PRE_CONT));\n"

            /* change the level of the subtree copies */
            /* get the level of the content root nodes */
            "var temp_ec_item := res_iter.leftfetchjoin(node_items).materialize(res_item);\n"
            "var temp_ec_cont := res_iter.leftfetchjoin(node_conts);\n"
            "nodes := res_item.mark(0@0);\n"
            "var root_level := mposjoin(temp_ec_item, "
                                       "temp_ec_cont, "
                                       "ws.fetch(PRE_LEVEL));\n"
            "root_level := nodes.leftfetchjoin(root_level);\n"

            "temp_ec_item := res_item;\n"
            "temp_ec_cont := res_cont;\n"
            "var content_level := mposjoin(temp_ec_item, temp_ec_cont, "
                                          "ws.fetch(PRE_LEVEL));\n"
            "content_level := nodes.leftfetchjoin(content_level);\n"
            "content_level := content_level.[-](root_level);\n"
            "root_level := nil;\n"
            /* join is made after the multiplex, because the level has to be
               change only once for each dn-node. With the join the multiplex
               is automatically expanded */
            "content_level := content_level.tmark(0@0);\n"

            "_elem_level := content_level;\n"

            /* get the attributes of the subtree copy elements */
            "{ # create attribute subtree copies\n"
            "var temp_attr := get_attr_own(ws, mposjoin(res_item, res_cont, ws.fetch(PRE_NID)), res_cont);\n"
            "var oid_attr := temp_attr.tmark(0@0);\n"
            "var oid_cont;\n"
            "if (is_constant(res_cont)) {\n"
            "    oid_cont := res_cont;\n"
            "} else {\n"
            "    oid_cont := temp_attr.reverse().leftfetchjoin(res_cont);\n"
            "    oid_cont := oid_cont.tmark(0@0);\n"
            "}\n"
            "_attr_qn   := mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_QN));\n"
            "_attr_prop := mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_PROP));\n"
            "_attr_cont := mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_CONT)).materialize(_attr_prop);\n"
            "_attr_own  := temp_attr.hmark(0@0);\n"
            "_attr_iter := _attr_own.leftfetchjoin(_elem_iter);\n"
            "temp_attr := nil;\n"
            "oid_attr := nil;\n"
            "oid_cont := nil;\n"
            "} # end of create attribute subtree copies\n"

            "} else { # if (nodes.count() != 0) ...\n"
            "_elem_iter  := empty_bat;\n"
            "_elem_size  := empty_bat.project(int_nil);\n"
            "_elem_level := empty_bat.project(chr_nil);\n"
            "_elem_kind  := empty_bat.project(chr_nil);\n"
            "_elem_prop  := empty_bat;\n"
            "_elem_cont  := empty_bat;\n"
            "_attr_iter  := empty_bat;\n"
            "_attr_qn    := empty_bat;\n"
            "_attr_prop  := empty_bat;\n"
            "_attr_cont  := empty_bat;\n"
            "_attr_own   := empty_bat;\n"
            "}  # end of else in 'if (nodes.count() != 0)'\n"
            "} # end of map2NODE_interface (counter)\n");
}

/**
 * translateConst translates the loop-lifting of a Constant
 * (before calling a variable 'itemID' with an oid has to be bound)
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param kind the kind of the item
 */
static void
translateConst (opt_t *f, int cur_level, char *kind)
{
    milprintf(f,
            "# translateConst (kind)\n"
            "iter := loop%03u.tmark(0@0);\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "item := itemID;\n"
            "kind := %s;\n",
            cur_level, kind);
}

/**
 * Worker function for translateSeq, which can
 * cope with item bat, holding `real' values
 * 
 * @param f the Stream the MIL code is printed to
 * @param i the offset of the first intermediate result
 * @param rcode the kind of the item bat
 */
static void
translateSeq_ (opt_t *f, int i, int rcode)
{
    char *item_ext = kind_str(rcode);

    /* pruning of the two cases where one of
       the intermediate results is empty */
    milprintf(f,
            "if (ipik.count() = 0) {\n"
            "        ipik := ipik%03u;\n"
            "        iter := iter%03u;\n"
            "        pos := pos%03u;\n"
            "        item%s := item%s%03u;\n"
            "        kind := kind%03u;\n",
            i, i, i, item_ext, item_ext, i, i);
    milprintf(f, 
            "} else { if (ipik%03u.count() != 0)\n",
            i);
    milprintf(f,
            "{ # translateSeq (counter)\n"
            /* FIXME: tests if input is sorted is needed because of merged union*/
            "var merged_result := merged_union "
            "(iter%03u.chk_order(), iter.chk_order(), item%s%03u, item%s, kind%03u, kind);\n",
            i, item_ext, i, item_ext, i);
    milprintf(f,
            "iter := merged_result.fetch(0);\n"
            "item%s := merged_result.fetch(1);\n"
            "kind := merged_result.fetch(2);\n"
            "ipik := iter;\n"
            "pos := tmark_grp_unique(iter, ipik);\n"
            "} # end of translateSeq (counter)\n"
            "}\n",
            item_ext);
}

/**
 * translateSeq combines two intermediate results and saves 
 * the intermediate result (iter|pos|item|kind) sorted by 
 * iter (under the condition, that the incoming iters of each
 * input where sorted already)
 * 
 * @param f the Stream the MIL code is printed to
 * @param i the offset of the first intermediate result
 * @param item_ext the extended item name
 */
static void
translateSeq (opt_t *f, int i)
{
    translateSeq_ (f, i, NORMAL);
}

/**
 * translateSeq_node combines two intermediate results of 
 * the NODE interface (under the condition, that the 
 * incoming iters of each input where sorted already)
 * 
 * @param f the Stream the MIL code is printed to
 * @param i the offset of the first intermediate result
 */
static void
translateSeq_node (opt_t *f, int i)
{
    /* first combine the root attribute sets */
    /* pruning of the two cases where one of
       the intermediate results is empty */
    milprintf(f,
            "# translateSeq_node (f, counter)\n"
            "if (_r_attr_iter.count() = 0) {\n"
            "        _r_attr_iter := _r_attr_iter%03u;\n"
            "        _r_attr_qn := _r_attr_qn%03u;\n"
            "        _r_attr_prop := _r_attr_prop%03u;\n"
            "        _r_attr_cont := _r_attr_cont%03u;\n"
            "} else { if (_r_attr_iter%03u.count() != 0)\n",
            i, i, i, i, i);
    milprintf(f,
            "{ # combine attribute roots\n"
            "var merged_result := merged_union "
            "(_r_attr_iter%03u.chk_order(), _r_attr_iter.chk_order(), "
             "_r_attr_qn%03u, _r_attr_qn, "
             "_r_attr_prop%03u, _r_attr_prop, "
             "_r_attr_cont%03u, _r_attr_cont);\n"
            "_r_attr_iter := merged_result.fetch(0);\n" /* CONST? */
            "_r_attr_qn := merged_result.fetch(1);\n" /* CONST? */
            "_r_attr_prop := merged_result.fetch(2);\n" /* CONST? */
            "_r_attr_cont := merged_result.fetch(3);\n" /* CONST? */
            "}} # end of combine attribute roots\n",
            i, i, i, i);
    /* now combine the element result sets */
    /* pruning of the two cases where one of
       the intermediate results is empty */
    milprintf(f,
            "if (_elem_iter.count() = 0) {\n"
            "_elem_iter  := _elem_iter%03u  ;\n"
            "_elem_size  := _elem_size%03u  ;\n"
            "_elem_level := _elem_level%03u ;\n"
            "_elem_kind  := _elem_kind%03u  ;\n"
            "_elem_prop  := _elem_prop%03u  ;\n"
            "_elem_cont  := _elem_cont%03u  ;\n"
            "_attr_iter  := _attr_iter%03u  ;\n"
            "_attr_qn    := _attr_qn%03u    ;\n"
            "_attr_prop  := _attr_prop%03u  ;\n"
            "_attr_cont  := _attr_cont%03u  ;\n"
            "_attr_own   := _attr_own%03u   ;\n"
            "} else { if (_elem_iter%03u.count() != 0)\n",
            i, i, i, i, i, i,
            i, i, i, i, i,
            i);
    milprintf(f,
            "{ # combine element nodes\n"
            /* FIXME: tests if input is sorted is needed because of merged union*/
            "var seqb := oid(count(_elem_size) + int(_elem_size.seqbase()));\n"
            "var shift_factor := int(seqb) - int(_elem_size%03u.seqbase());\n"
            "var merged_result := merged_union ("
            "_elem_iter%03u.chk_order(), _elem_iter.chk_order(), "
            "_elem_size%03u, _elem_size, "
            "_elem_level%03u, _elem_level, "
            "_elem_kind%03u, _elem_kind, "
            "_elem_prop%03u, _elem_prop, "
            "_elem_cont%03u, _elem_cont, "
            "_elem_size%03u.mark(seqb), _elem_size.mirror());\n"
            "_elem_iter := merged_result.fetch(0);\n" /* CONST? */
            "_elem_size := merged_result.fetch(1);\n" /* CONST? */
            "_elem_level:= merged_result.fetch(2);\n" /* CONST? */
            "_elem_kind := merged_result.fetch(3);\n" /* CONST? */
            "_elem_prop := merged_result.fetch(4);\n" /* CONST? */
            "_elem_cont := merged_result.fetch(5);\n" /* CONST? */
            "var preNew_preOld := merged_result.fetch(6);\n" /* CONST? */
            "merged_result := nil;\n"

            "_attr_own%03u := _attr_own%03u.[int]().[+](shift_factor).[oid]();\n"
            "merged_result := merged_union ("
            "_attr_iter%03u, _attr_iter, "
            "_attr_qn%03u, _attr_qn, "
            "_attr_prop%03u, _attr_prop, "
            "_attr_cont%03u, _attr_cont, "
            "_attr_own%03u, _attr_own);\n"
            "_attr_iter := merged_result.fetch(0);\n" /* CONST? */
            "_attr_qn   := merged_result.fetch(1);\n" /* CONST? */
            "_attr_prop := merged_result.fetch(2);\n" /* CONST? */
            "_attr_cont := merged_result.fetch(3);\n" /* CONST? */
            "_attr_own  := merged_result.fetch(4);\n" /* CONST? */
            "_attr_own := _attr_own.leftjoin(preNew_preOld.reverse())"
                                  ".tmark(seqbase(_attr_own));\n"
            /* we need to help MonetDB, since we know that the order
               preserving join (above) creates and deletes no rows */
            "_attr_own := _attr_own.tmark(0@0);\n"
            "}} # combine element nodes\n"
            "# end of translateSeq_node (f, counter)\n",
            i,
            i, i, i, i, i, i, i,
            i, i,
            i, i, i, i, i);
}

/**
 * translateSequence translates the sequence core node '(a, b)'
 * and provides different translations for the different 
 * interfaces
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the Core node containing the sequence
 * @result the kind indicating, which result interface is chosen
 */
static int
translateSequence (opt_t *f, int code, int cur_level, int counter, PFcnode_t *c)
{
    int rc1, rc2, kind;
    char *item_ext;

    /* nodes are combined with an extended merged_union */
    if (code == NODE)
    {
        rc1 = translateElemContent (f, code, cur_level, counter, L(c));
        if (rc1 == NORMAL)
            map2NODE_interface (f);

        counter++;
        saveResult_node (f, counter);
    
        rc2 = translateElemContent (f, code, cur_level, counter, R(c));
        if (rc2 == NORMAL)
            map2NODE_interface (f);
    
        translateSeq_node (f, counter);
        deleteResult_node (f, counter);
        return code;
    }
    /* locstep codes shouldn't occur in sequence construction */
    else if (code == ITEM_ORDER)
        mps_error ("item|iter interface was chosen at the wrong place "
                   "(sequence construction).");
    /* returns the kind as values */
    else if (code == VALUES && (kind = combinable(TY(L(c)),TY(R(c)))))
    {
        item_ext = kind_str(kind);
        rc1 = translate2MIL (f, code, cur_level, counter, L(c));
        if (rc1 == NORMAL)
            milprintf(f, "item%s := item%s;\n", 
                      item_ext, val_join(kind));
        counter++;
        saveResult_ (f, counter, kind);

        rc2 = translate2MIL (f, code, cur_level, counter, R(c));
        if (rc2 == NORMAL)
            milprintf(f, "item%s := item%s;\n", 
                      item_ext, val_join(kind));

        if (rc1 != NORMAL && rc2 != NORMAL && rc1 != rc2) 
            mps_error ("we get a type mismatch (%i != %i) "
                       "in sequence construction (using the value interface).",
                       rc1, rc2); 

        translateSeq_ (f, counter, kind);
        deleteResult_ (f, counter, kind);
        return kind; 
    }
    /* main case return the result using the normal interface */
    translate2MIL (f, NORMAL, cur_level, counter, L(c));
    counter++;
    saveResult (f, counter);

    translate2MIL (f, NORMAL, cur_level, counter, R(c));

    translateSeq (f, counter);
    deleteResult (f, counter);
    return NORMAL;
}

/**
 * translateVar looks up a variable in the 
 * actual scope and binds it values to the intermediate
 * result (iter|pos|item|kind)
 * 
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param v the variable to be looked up
 */
static void
translateVar (opt_t *f, int cur_level, PFvar_t *v)
{
    milprintf(f, "{ # translateVar (%s)\n", PFqname_str(v->qname));
    milprintf(f, "var vid := v_vid%03u.ord_uselect(%i@0);\n", 
            cur_level, f->module_base + v->vid);
    milprintf(f, "vid := vid.hmark(0@0);\n");
    milprintf(f, "iter := vid.leftfetchjoin(v_iter%03u).assert_order();\n", cur_level);
    milprintf(f, "pos := vid.leftfetchjoin(v_pos%03u);\n", cur_level);
    milprintf(f, "item := vid.leftfetchjoin(v_item%03u);\n", cur_level);
    milprintf(f, "kind := vid.leftfetchjoin(v_kind%03u);\n", cur_level);
    milprintf(f, "ipik := iter;\n");
    milprintf(f, "} # end of translateVar (%s)\n", PFqname_str(v->qname));
}

/**
 * append appends the information of a variable to the 
 * corresponding column of the variable environment
 *
 * @param f the Stream the MIL code is printed to
 * @param name the columnname
 * @param type_ the tail type of the column
 * @param level the actual level of the for-scope
 */
static void
append (opt_t *f, char *name, int level)
{
    milprintf(f, "{ # append (%s, level)\n", name);
    milprintf(f, "%s := %s.materialize(ipik);\n", name, name);
    milprintf(f, "v_%s%03u := v_%s%03u.append(%s);\n", name, level, name, level, name);
    milprintf(f, "} # append (%s, level)\n", name);
}

/**
 * insertVar adds a intermediate result (iter|pos|item|kind) to
 * the variable environment in the actual for-scope (let-expression)
 * 
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param vid the key value of the intermediate result which is later used to
 *        look it up again 
 */
static void
insertVar (opt_t *f, int cur_level, int vid)
{
    milprintf(f,
            "{ # insertVar (vid)\n"
            "var vid := project(ipik,%i@0);\n",
            f->module_base + vid);

    append (f, "vid", cur_level);
    append (f, "iter", cur_level);
    append (f, "pos", cur_level); 
    append (f, "item", cur_level);
    append (f, "kind", cur_level);

    /*
    milprintf(f, 
            "print(\"testoutput in insertVar(%i@0) expanded to level %i\");\n",
            vid, cur_level);
    milprintf(f, 
            "print (v_vid%03u, v_iter%03u, v_pos%03u, v_kind%03u);\n",
            cur_level, cur_level, cur_level, cur_level);
    */
    milprintf(f, "} # end of insertVar (vid)\n");
}

/**
 * createEnumeration creates the Enumeration needed for the
 * changes item and inserts if needed
 * the int values to 'int_values'
 *
 * @param f the Stream the MIL code is printed to
 */
static void
createEnumeration (opt_t *f, int cur_level)
{
    milprintf(f,
            "{ # createEnumeration ()\n"
            /* the head of item has to be void */
            "var ints_cE := tmark_grp_unique(outer%03u, outer%03u).[lng]();\n",
            cur_level, cur_level);
    addValues (f, int_container(), "ints_cE", "item");
    milprintf(f,
            "iter := inner%03u.tmark(0@0);\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            /* change kind information to int */
            "kind := INT;\n"
            "} # end of createEnumeration ()\n",
            cur_level);
}

/**
 * project creates the variables for the next for-scope
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 */
static void
project (opt_t *f, int cur_level)
{
    /* create an order that points back to the last flwro block */
    milprintf(f, "# project ()\n");
    milprintf(f, "iter := iter.materialize(ipik);\n");
    milprintf(f, "var order_%03u := iter;\n", cur_level);

#if 0 /* DISABLED */
    int cur, lim = get_flwr_level(f, cur_level);
    if (lim == 0 || f->flwr_level[lim-1] == -1) 
    for(cur=cur_level; cur > lim; cur--)
        if (cur >= f->flwr_depth || f->flwr_level[cur] == -1) 
            milprintf(f, "order_%03u := order_%03u.leftjoin(reverse(inner%03u)).leftfetchjoin(outer%03u);\n", cur_level, cur_level, cur-1, cur-1);
#endif

    /* create a new loop / outer|inner relation
       and adjust iter, pos from for loop binding */
    milprintf(f, "var outer%03u := iter;\n", cur_level);
    milprintf(f, "iter := iter.mark(1@0);\n");
    milprintf(f, "var inner%03u := iter;\n", cur_level);
    milprintf(f, "pos := 1@0;\n");
    milprintf(f, "var loop%03u := inner%03u;\n", cur_level, cur_level);

    /* create a new set of variables, which contain
       the variable environment of the next scope */
    milprintf(f, "var v_vid%03u;\n", cur_level);
    milprintf(f, "var v_iter%03u;\n", cur_level);
    milprintf(f, "var v_pos%03u;\n", cur_level);
    milprintf(f, "var v_item%03u;\n", cur_level);
    milprintf(f, "var v_kind%03u;\n", cur_level);
}

/**
 * getExpanded looks up the variables with which are expanded
 * (because needed) in the next deeper for-scope nesting 
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param fid the number of the for-scope to look up
 *        the list of variables which have to be expanded
 */
static void
getExpanded (opt_t *f, int cur_level, int fid)
{
    milprintf(f, 
            "{ # getExpanded (fid)\n"
            "var vu_nil := vu_fid.ord_uselect(%i@0);\n",
            f->module_base + fid);
    milprintf(f,
            "var vid_vu := vu_vid.reverse();\n"
            "var oid_nil := vid_vu.leftjoin(vu_nil);\n"
            "expOid := v_vid%03u.leftjoin(oid_nil);\n",
            /* the vids from the nesting before are looked up */
            cur_level - 1);
    milprintf(f,
            "expOid := expOid.mirror();\n"
            "} # end of getExpanded (fid)\n");
}

/**
 * expand joins inner_outer and iter and sorts out the
 * variables which shouldn't be expanded by joining with expOid
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 */
static void
expand (opt_t *f, int cur_level)
{
    milprintf(f,
            "{ # expand ()\n"
            "var expOid_iter := expOid.leftfetchjoin(v_iter%03u);\n",
            /* the iters from the nesting before are looked up */
            cur_level - 1); 

    milprintf(f,
            "expOid := nil;\n"
            "var iter_expOid := expOid_iter.reverse();\n"
            "expOid_iter := nil;\n"
            "var oidMap_expOid := outer%03u.chk_order().leftjoin(iter_expOid.chk_order());\n",
            cur_level);

    /* if we don't have a stable sort, we should be sure, that the
       mapping relation is refined on the tail */
    milprintf(f,
            "if (not(ordered(reverse(outer%03u)) and ordered(iter_expOid) and ordered(reverse(iter_expOid)))) {\n"
            "  var temp_sort := oidMap_expOid.hmark(0@0)"
                                          ".CTrefine(oidMap_expOid.tmark(0@0))"
                                          ".mirror();\n"
            "  oidMap_expOid := temp_sort.leftfetchjoin("
                                        "oidMap_expOid.hmark(0@0))"
                                      ".reverse()"
                                              ".leftfetchjoin("
                                                "oidMap_expOid.tmark(0@0))\n;"
            "}\n",
            cur_level, cur_level);
   
    milprintf(f,
            "iter_expOid := nil;\n"
            "var expOid_oidMap := oidMap_expOid.reverse();\n"
            "oidMap_expOid := nil;\n"
            "expOid_iter := expOid_oidMap.leftfetchjoin(inner%03u);\n",
            cur_level);
    milprintf(f,
            "expOid_oidMap := nil;\n"
            "v_iter%03u := expOid_iter;\n",
            cur_level);
    /* oidNew_expOid is the relation which maps from old scope to the
       new scope */
    milprintf(f,
            "oidNew_expOid := expOid_iter.hmark(0@0);\n"
            "expOid_iter := nil;\n"
            "} # end of expand ()\n");
}

/**
 * join maps the five columns (vid|iter|pos|item|kind) to the next scope
 * and reserves the double size in the bats for inserts from let-expressions
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 */
static void
join (opt_t *f, int cur_level)
{
    milprintf(f, "# join ()\n");
    milprintf(f, "var cnt := count(v_iter%03u)*2;\n", cur_level);
    milprintf(f, "var new_v_iter := v_iter%03u;\n", cur_level);
    milprintf(f, "v_iter%03u := bat(void,oid,cnt)"
                                ".seqbase(0@0).access(BAT_APPEND).append(new_v_iter);\n", cur_level);
    milprintf(f, "new_v_iter := nil;\n");

    milprintf(f, "var new_v_vid := oidNew_expOid.leftjoin(v_vid%03u);\n", cur_level-1);
    milprintf(f, "v_vid%03u := bat(void,oid,cnt)"
                                ".seqbase(0@0).access(BAT_APPEND).insert(new_v_vid);\n", cur_level);
    milprintf(f, "new_v_vid := nil;\n");

    milprintf(f, "var new_v_pos := oidNew_expOid.leftjoin(v_pos%03u);\n", cur_level-1);
    milprintf(f, "v_pos%03u := bat(void,oid,cnt)"
                                ".seqbase(0@0).access(BAT_APPEND).insert(new_v_pos);\n", cur_level);
    milprintf(f, "new_v_pos := nil;\n");

    milprintf(f, "var new_v_item := oidNew_expOid.leftjoin(v_item%03u);\n", cur_level-1);
    milprintf(f, "v_item%03u := bat(void,oid,cnt)"
                                ".seqbase(0@0).access(BAT_APPEND).insert(new_v_item);\n", cur_level);
    milprintf(f, "new_v_item := nil;\n");

    milprintf(f, "var new_v_kind := oidNew_expOid.leftjoin(v_kind%03u);\n", cur_level-1);
    milprintf(f, "v_kind%03u := bat(void,int,cnt)"
                                ".seqbase(0@0).access(BAT_APPEND).insert(new_v_kind);\n", cur_level);
    milprintf(f, "new_v_kind := nil;\n");

    milprintf(f, "oidNew_expOid := nil;\n");
    milprintf(f, "# end of join ()\n");
}

/**
 * mapBack joins back the intermediate result to their old
 * iter values after the execution of the body of the for-expression
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 */
static void
mapBack (opt_t *f, int cur_level)
{
    milprintf(f,
            "{ # mapBack ()\n"
            /* the iters are mapped back to the next outer scope */
            "var iter_oidMap := inner%03u.reverse();\n",
            cur_level);
    milprintf(f,
            "var oid_oidMap := iter.leftjoin(iter_oidMap).tmark(0@0);\n"
            "iter_oidMap := nil;\n"
            "iter := oid_oidMap.leftfetchjoin(outer%03u);\n",
            cur_level);
    milprintf(f,
            "oid_oidMap := nil;\n"
            "pos := tmark_grp_unique(iter,ipik);\n"
            "# item := item;\n"
            "# kind := kind;\n"
            "# ipik := ipik;\n"
            "} # end of mapBack ()\n"
           );
}

/**
 * cleanUpLevel sets all variables needed for 
 * a new scope introduced by a for-expression
 * back to nil
 * 
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 */
static void
cleanUpLevel (opt_t *f, int cur_level)
{
    milprintf(f, "# cleanUpLevel ()\n");
    milprintf(f, "inner%03u := nil;\n", cur_level);
    milprintf(f, "outer%03u := nil;\n", cur_level);
    milprintf(f, "order_%03u := nil;\n", cur_level);
    milprintf(f, "loop%03u := nil;\n", cur_level);

    milprintf(f, "v_vid%03u := nil;\n", cur_level);
    milprintf(f, "v_iter%03u := nil;\n", cur_level);
    milprintf(f, "v_pos%03u := nil;\n", cur_level);
    milprintf(f, "v_item%03u := nil;\n", cur_level);
    milprintf(f, "v_kind%03u := nil;\n", cur_level);
}
                                                                                                                                                        
/**
 * createNewVarTable creates new bats for the next for-scope
 * in case no variables will be expanded
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 */
static void
createNewVarTable (opt_t *f, int cur_level)
{
    milprintf(f, "# createNewVarTable ()\n");
    milprintf(f,
            "v_iter%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n",
            cur_level);
    milprintf(f,
            "v_vid%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n",
            cur_level);
    milprintf(f,
            "v_pos%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n",
            cur_level);
    milprintf(f,
            "v_item%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n",
            cur_level);
    milprintf(f,
            "v_kind%03u := bat(void,int).seqbase(0@0).access(BAT_APPEND);\n",
            cur_level);
}

/*
 * translateIfThen translates either then or else block of an if-then-else
 *
 * to avoid more than one expansion of the subtree for each 
 * branch three branches (PHASES) are added in MIL. They
 * avoid the expansion of the variable environment and of the 
 * subtree if the if-clause produces either only true or only
 * false values. If then- or else-clause is empty (c_empty)
 * the function will only be called for the other.
 *
 *  '-' = not      |   skip  |  c_empty 
 *        executed | 0  1  2 | then  else
 *  PHASE 1 (then) |    -  - |  -
 *  PHASE 2 (then) |       - |  -
 *  PHASE 3 (then) |    -  - |  -
 *  PHASE 1 (else) |    -  - |        -
 *  PHASE 2 (else) |    -    |        -
 *  PHASE 3 (else) |    -  - |        -
 *
 * Stefan.Manegold@cwi.nl, 03.11.2006:
 *
 * fixing BUG #1562868 XQ: complex q fails with error in "batbat_lng_add_inplace":
 * 
 * trading in optimization (performance?) for correctness:
 * 
 * The optimization to omit the eveluation of empty if-then-else branches
 * is broken, and since noone has been able to fix it, yet, we simply
 * disable this optimization --- the performance "penalty" should not be
 * too big, since the inpu to the respective branch is empty.
 * 
 * See
 * http://sourceforge.net/tracker/index.php?func=detail&aid=1562868&group_id=56967&atid=482468
 * and "fixing BUG #1562868" below for details.
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the then/else expression
 * @param then is the boolean saving if the branch (then/else)
 * @param bool_res the number, where the result of the if-clause
 *        is saved 
 * @result the kind indicating, which result interface is chosen
 */
static int
translateIfThen (opt_t *f, int code, int cur_level, int counter,
                 PFcnode_t *c, int then, int bool_res)
{
    int rc;

    cur_level++;
    milprintf(f, "{ # translateIfThen\n");

    /* initial setting of new 'scope' */
    milprintf(f, "var loop%03u := loop%03u;\n", cur_level, cur_level-1);
    milprintf(f, "var inner%03u := inner%03u;\n", cur_level, cur_level-1);
    milprintf(f, "var outer%03u := outer%03u;\n", cur_level, cur_level-1);
    milprintf(f, "var order_%03u := order_%03u;\n", cur_level, cur_level-1);
    milprintf(f, "var v_vid%03u := v_vid%03u;\n", cur_level, cur_level-1);
    milprintf(f, "var v_iter%03u := v_iter%03u;\n", cur_level, cur_level-1);
    milprintf(f, "var v_pos%03u := v_pos%03u;\n", cur_level, cur_level-1);
    milprintf(f, "var v_item%03u := v_item%03u;\n", cur_level, cur_level-1);
    milprintf(f, "var v_kind%03u := v_kind%03u;\n", cur_level, cur_level-1);

    /* 1. PHASE: create all mapping stuff to next 'scope' */
    /* milprintf(f, "if (skip = 0)\n{\n"); *//* see "fixing BUG #1562868" above */
    milprintf(f, "if (true)\n{\n");
    /* output for debugging
    milprintf(f, "\"PHASE 1 of %s-clause active\".print();\n",then?"then":"else");
    */

    /* get the right set of sequences, which have to be processed */
    if (!then)
            milprintf(f, "selected := item%03u.ord_uselect(0@0);\n", bool_res);

    milprintf(f, "iter := selected.mirror().join(iter%03u);\n", bool_res);
    milprintf(f, "iter := iter.tmark(0@0);\n");
    milprintf(f, "outer%03u := iter;\n", cur_level);
    milprintf(f, "var inner := inner%03u.tsort();\n", cur_level-1);
    milprintf(f, "order_%03u := outer%03u.leftfetchjoin(inner.hmark(min(inner)))"
                                        ".leftfetchjoin(order_%03u);\n",
              cur_level, cur_level, cur_level-1);
    milprintf(f, "iter := iter.mark(1@0);\n");
    milprintf(f, "inner%03u := iter;\n", cur_level);
    milprintf(f, "loop%03u := inner%03u;\n", cur_level, cur_level);
    milprintf(f, "iter := nil;\n");

    /* - in a first version no variables are pruned
         at an if-then-else node 
       - if-then-else is executed more or less like a for loop */
    milprintf(f, "var expOid := v_iter%03u.mirror();\n", cur_level);
    milprintf(f, "var oidNew_expOid;\n");
    expand (f, cur_level);
    join (f, cur_level);

    milprintf(f, "}\n");

    /* 2. PHASE: execute then/else expression if there are 
       true/false values in the boolean expression */
    if (then)
            milprintf(f, "if (skip != 1)\n{\n");
    else
            milprintf(f, "if (skip != 2)\n{\n");
    /* output for debugging
    milprintf(f, "\"PHASE 2 of %s-clause active\".print();\n",then?"then":"else");
    */

    /* get rc and apply it to the empty result
       to get the same item variable */
    rc = translate2MIL (f, code, cur_level, counter, c);
    milprintf(f, "} else {\n");
    translateEmpty_ (f, rc);
    milprintf(f, "}\n");

    /* 3. PHASE: create all mapping stuff from to actual 'scope' */
    /* milprintf(f, "if (skip = 0)\n{\n"); *//* see "fixing BUG #1562868" above */
    milprintf(f, "if (true)\n{\n");
    /* output for debugging
    milprintf(f, "\"PHASE 3 of %s-clause active\".print();\n",then?"then":"else");
    */
    mapBack (f, cur_level);
    milprintf(f, "}\n");

    cleanUpLevel (f, cur_level);
    milprintf(f, "} # end of translateIfThen\n");
    cur_level--;
    return rc;
}

/**
 * translateIfThenElse translates the if-then-else core node
 * and provides different translations for the different 
 * interfaces
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param then_ the Core node containing the then expression
 * @param else_ the Core node containing the else expression
 * @result the kind indicating, which result interface is chosen
 */
static int
translateIfThenElse (opt_t *f, int code, int cur_level, int counter, 
                     PFcnode_t *then_, PFcnode_t *else_)
{
    int bool_res, rcode, rc1, rc2;
    char *item_ext;

    counter ++;
    saveResult (f, counter);
    bool_res = counter;
    milprintf(f, "{ # ifthenelse-translation\n");
    /* idea:
    select trues
    if (trues = count) or (trues = 0)
         only give back one of the results
    else
         do the whole stuff
    */
    /* see "fixing BUG #1562868" above */
    milprintf(f, "item%03u := item%03u.materialize(ipik);\n", bool_res, bool_res);
    milprintf(f,
            "var selected;\n"
            "var skip;\n"
            "if (type(item%03u) = bat) {"
            "  selected := item%03u.ord_uselect(1@0);\n"
            "  var cnt := selected.count();\n"
            "  if (item%03u.count() = cnt) {\n"
            "   skip := 2;\n"
            "  } else { \n"
            "   skip := int(cnt = 0);\n"
            "  }\n"
            "} else {\n"
            "  skip := 1 + int(item%03u); # handle constants efficiently\n"
            "}\n",
            bool_res, bool_res, 
            bool_res, bool_res);
    /* if at compile time one argument is already known to
       be empty don't do the other */
    if (else_->kind == c_empty)
    {
        rcode = translateIfThen (f, code, cur_level, counter, 
                                 then_, 1, bool_res);
    }
    else if (then_->kind == c_empty)
    {
        rcode = translateIfThen (f, code, cur_level, counter,
                                 else_, 0, bool_res);
    }
    else if (code == VALUES && 
             (rcode = combinable(TY(then_),TY(else_))))
    {
        item_ext = kind_str(rcode);
        rc1 = translateIfThen (f, code, cur_level, counter,
                               then_, 1, bool_res);
        if (rc1 == NORMAL)
            milprintf(f, "item%s := item%s;\n", 
                      item_ext, val_join(rcode));
        counter++;
        saveResult_ (f, counter, rcode);

        rc2 = translateIfThen (f, code, cur_level, counter,
                               else_, 0, bool_res);
        if (rc2 == NORMAL)
            milprintf(f, "item%s := item%s;\n", 
                      item_ext, val_join(rcode));

        if (rc1 != NORMAL && rc2 != NORMAL && rc1 != rc2) 
            mps_error ("we get a type mismatch (%i != %i) "
                       "in the sequence construction (using the value interface).",
                       rc1, rc2); 

        translateSeq_ (f, counter, rcode);
        deleteResult_ (f, counter, rcode);
    }
    else
    {

    /* main case return the result using the normal interface */
        translateIfThen (f, NORMAL, cur_level, counter,
                         then_, 1, bool_res);
        counter++;
        saveResult (f, counter);
        translateIfThen (f, NORMAL, cur_level, counter,
                         else_, 0, bool_res);
        translateSeq (f, counter);
        deleteResult (f, counter);
        rcode = NORMAL;
    }
    milprintf(f, "} # end of ifthenelse-translation\n");
    deleteResult (f, bool_res);
    return rcode;
}

/**
 * translateTypeswitch generates a boolean list for all
 * iterations, which match the sequence type and the default
 * types
 * Only a small part of the possible sequence types is 
 * covered. The parts not supported stop the processing
 * at compile time.
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param input_type the input type of the typeswitch expression
 * @param seq_type the the sequence type used for splitting the cases
 */
static void
translateTypeswitch (opt_t *f, int cur_level, PFty_t input_type, PFty_t seq_type)
{
    char *kind;
    PFty_t exp_type;
    /* '()?' = 0; '()' = 1; '()*' = 2; '()+' = 3; */
    int qualifier = 1;

    exp_type = PFty_defn (seq_type);

#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
    if (exp_type.type == ty_opt)
    {
        qualifier = 0;
        exp_type = PFty_child (exp_type);
    }
    else if (exp_type.type == ty_star)
    {
        qualifier = 2;
        exp_type = PFty_child (exp_type);
    }
    else if (exp_type.type == ty_plus)
    {
        qualifier = 3;
        exp_type = PFty_child (exp_type);
    }
#endif

    milprintf(f, "{ # typeswitch\n");

    if (TY_EQ(exp_type, PFty_empty ()))
    {
        milprintf(f,
                "var unique_iter := iter.tunique();\n"
                "item := loop%03u.outerjoin(project(unique_iter,false)).[isnil]().[oid]();\n"
                "iter := loop%03u;\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := BOOL;\n"
                "} # end of typeswitch\n",
                cur_level, cur_level);
        return;
    }
    else if (TY_EQ (exp_type, PFty_xs_integer ()))
        kind = "INT";
    else if (TY_EQ (exp_type, PFty_xs_decimal ()))
        kind = "DEC";
    else if (TY_EQ (exp_type, PFty_xs_string ()))
        kind = "STR";
    else if (TY_EQ (exp_type, PFty_xs_double ()))
        kind = "DBL";
    else if (TY_EQ (exp_type, PFty_xs_boolean ()))
        kind = "BOOL";
    else if (TY_EQ (exp_type, PFty_untypedAtomic ()))
        kind = "U_A";
    else if (TY_EQ (exp_type, PFty_xs_anyNode ())) {
        milprintf(f,
            "kind := kind.[>](ATOMIC);\n");
        kind = "true";
    }
    else if (TY_EQ (exp_type, PFty_xs_anyAttribute ())) {
        milprintf(f,
            "kind := kind.get_types();\n");
        kind = "ATTR";
    }
    else if (TY_EQ (exp_type, PFty_doc (PFty_xs_anyType ()))) {
        milprintf(f,
            "var oid_node := kind.get_type(ELEM).mark(0@0).reverse();\n"
            "var oid_item := oid_node.leftfetchjoin(item);\n"
            "var oid_cont := oid_node.leftfetchjoin(kind).get_container();\n"
            "var oid_kind := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND));\n"
            "oid_kind := oid_node.reverse().leftfetchjoin(oid_kind);\n"
            "kind := kind.mirror().outerjoin(oid_kind);\n");
        kind = "DOCUMENT";
    }
    else if (TY_EQ (exp_type, PFty_xs_anyElement ())) {
        milprintf(f,
            "var oid_node := kind.get_type(ELEM).mark(0@0).reverse();\n"
            "var oid_item := oid_node.leftfetchjoin(item);\n"
            "var oid_cont := oid_node.leftfetchjoin(kind).get_container();\n"
            "var oid_kind := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND));\n"
            "oid_kind := oid_node.reverse().leftfetchjoin(oid_kind);\n"
            "kind := kind.mirror().outerjoin(oid_kind);\n");
        kind = "ELEMENT";
    }
    else if (TY_EQ (exp_type, PFty_text ())) {
        milprintf(f,
            "var oid_node := kind.get_type(ELEM).mark(0@0).reverse();\n"
            "var oid_item := oid_node.leftfetchjoin(item);\n"
            "var oid_cont := oid_node.leftfetchjoin(kind).get_container();\n"
            "var oid_kind := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND));\n"
            "oid_kind := oid_node.reverse().leftfetchjoin(oid_kind);\n"
            "kind := kind.mirror().outerjoin(oid_kind);\n");
        kind = "TEXT";
    }
    else if (TY_EQ (exp_type, PFty_comm ())) {
        milprintf(f,
            "var oid_node := kind.get_type(ELEM).mark(0@0).reverse();\n"
            "var oid_item := oid_node.leftfetchjoin(item);\n"
            "var oid_cont := oid_node.leftfetchjoin(kind).get_container();\n"
            "var oid_kind := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND));\n"
            "oid_kind := oid_node.reverse().leftfetchjoin(oid_kind);\n"
            "kind := kind.mirror().outerjoin(oid_kind);\n");
        kind = "COMMENT";
    }
    else if (TY_EQ (exp_type, PFty_pi (NULL))) {
        milprintf(f,
            "var oid_node := kind.get_type(ELEM).mark(0@0).reverse();\n"
            "var oid_item := oid_node.leftfetchjoin(item);\n"
            "var oid_cont := oid_node.leftfetchjoin(kind).get_container();\n"
            "var oid_kind := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND));\n"
            "oid_kind := oid_node.reverse().leftfetchjoin(oid_kind);\n"
            "kind := kind.mirror().outerjoin(oid_kind);\n");
        kind = "PI";
    }
    else
        PFoops (OOPS_TYPECHECK,
                "couldn't solve typeswitch at compile time "
                "(runtime checks only support atomic types);"
                " don't know if '%s' is subtype of '%s'",
                PFty_str(input_type), PFty_str(seq_type));

    /* match one/multiple values per iteration */
    if (qualifier == 0 || qualifier == 1)
        milprintf(f,
                "var single_iters := histogram(iter).[=](1).select(true).mirror();\n"
                "single_iters := single_iters.leftjoin(iter.reverse());\n"
                "var matching_iters := single_iters.leftfetchjoin(kind).[=](%s).select(true);\n"
                "single_iters := nil;\n",
                kind);
    else
        milprintf(f,
                "var iter_count := histogram(iter);\n"
                "var iter_kind := iter.reverse().leftfetchjoin(kind);\n"
                "var kind_count := histogram(iter_kind.[=](%s).select(true).reverse());\n"
                "iter_kind := nil;\n"
                "var matching_iters := iter_count.[=](kind_count).select(true);\n"
                "iter_count := nil;\n"
                "kind_count := nil;\n",
                kind);

    /* create result for empty iterations */
    if (qualifier == 0 || qualifier == 2)
        milprintf(f,
                "iter := iter.tunique().mirror();\n"
                "item := iter.outerjoin(matching_iters).[isnil]().select(true);\n"
                "item := loop%03u.outerjoin(item).[isnil]().[oid]();\n",
                cur_level);
    else
        milprintf(f,
                "item := loop%03u.outerjoin(matching_iters).[isnil]().[not]().[oid]();\n",
                cur_level);

    milprintf(f,
            "matching_iters := nil;\n"
            "iter := loop%03u;\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := BOOL;\n"
            "} # end of typeswitch\n",
            cur_level);
}

/**
 * loop_liftedSCJ is the loop-lifted version of the staircasejoin,
 * which translates the attribute step and calls the iterative version
 * of the loop-lifted staircasejoin for the other axis
 *
 * @param f the Stream the MIL code is printed to
 * @param axis the string containing the axis information
 * @param kind the string containing the kind information
 * @param ns the string containing the qname namespace information
 * @param loc the string containing the qname local part information
 * @param rev_in an integer indicating the input order (item|iter/iter|item)
 * @param rev_out an integer indicating the output order (item|iter/iter|item)
 */
static void
loop_liftedSCJ (opt_t *f, 
                char *axis, char *kind, char *ns, char *loc,
                int rev_in, int rev_out)
{
    /* iter|pos|item input contains only nodes (kind=ELEM) */
    milprintf(f, "# loop_liftedSCJ (axis, kind, ns, loc)\n");
    milprintf(f, "if ([and](reverse(tunique(kind)),63).texist(ATTR))"
                  "ERROR (\"path steps are only supported "
                          "starting from non-attribute nodes\");\n");


    if (!strcmp (axis, "attribute"))
    {
        if (rev_in || rev_out) 
            mps_error ("can't support item|iter order in attribute step "
                       "of loop-lifted staircase join (loop_liftedSCJ"
                       " in %i, out %i).", rev_in, rev_out);

        milprintf(f,
            /* get the attribute ids from the pre values */
            "{ # attribute axis\n"
            "var oid_iter := iter;\n"
            "var oid_item := item.materialize(ipik);\n"
            "var oid_cont := kind.get_container();\n"
            "var temp1 := get_attr_own (ws, mposjoin(oid_item, oid_cont, ws.fetch(PRE_NID)), oid_cont);\n"
            "oid_item := nil;\n"
            "oid_cont := temp1.hmark(0@0).leftfetchjoin(oid_cont);\n"
            "var oid_attr := temp1.tmark(0@0);\n"
            "oid_iter := temp1.hmark(0@0).leftfetchjoin(oid_iter);\n"
            "temp1 := nil;\n"
            "var temp1_str; # only needed for name test\n"
           );

        if (ns)
        {
            milprintf(f,
                    "temp1_str := mposjoin(mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_QN)), "
                                          "mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_CONT)), "
                                          "ws.fetch(QN_URI));\n"
                    "temp1 := temp1_str.ord_uselect(\"%s\");\n"
                    "temp1_str := nil;\n",
                    ns);
            milprintf(f,
                    "temp1 := temp1.hmark(0@0);\n"
                    "oid_attr := temp1.leftfetchjoin(oid_attr);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "temp1 := nil;\n");
        }
        if (loc)
        {
            milprintf(f,
                    "temp1_str := mposjoin(mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_QN)), "
                                      "mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_CONT)), "
                                      "ws.fetch(QN_LOC));\n"
                    "temp1 := temp1_str.ord_uselect(\"%s\");\n"
                    "temp1_str := nil;\n",
                    loc);
            milprintf(f,
                    "temp1 := temp1.hmark(0@0);\n"
                    "oid_attr := temp1.leftfetchjoin(oid_attr);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "temp1 := nil;\n");
        }

        /* add '.tmark(0@0)' to be sure that the head of 
           the results is void */
        milprintf(f,
                "iter := oid_iter.tmark(0@0);\n"
                "item := oid_attr.tmark(0@0);\n"
                "kind := oid_cont.tmark(0@0);\n"
                "if (type(iter) = bat) {\n"
                "    ipik := iter;\n"
                "} else {\n"
                "    if (type(item) = bat) {\n"
                "        ipik := item;\n"
                "    } else {\n"
                "        ipik := kind;\n"
                "    }\n"
                "}\n"
                "} # end of attribute axis\n");
    }
    else if (!strcmp (axis, "self"))
    {
        if (rev_in || rev_out) 
            mps_error ("can't support item|iter order in self step "
                       "of loop-lifted staircase join (loop_liftedSCJ"
                       " in %i, out %i).", rev_in, rev_out);

        milprintf(f,
            "{ # self axis\n"
            "var oid_iter := iter.materialize(ipik);\n"
            "var oid_item := item.materialize(ipik);\n"
            "var oid_cont := kind.get_container();\n"
           );

        if (kind)
        {
            milprintf(f,
                    "var temp1 := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND)).ord_uselect(%s).hmark(0@0);\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_item := temp1.leftfetchjoin(oid_item);\n"
                    "temp1 := nil;\n",
                    kind);
        }
        else if (ns && loc)
        {
            milprintf(f,
                    "var temp1 := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND)).ord_uselect(ELEMENT).hmark(0@0);\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_item := temp1.leftfetchjoin(oid_item);\n"
                    "temp1 := nil;\n");
            milprintf(f,
                    "var temp_str := mposjoin(mposjoin(oid_item, oid_cont, ws.fetch(PRE_PROP)), "
                                             "mposjoin(oid_item, oid_cont, ws.fetch(PRE_CONT)), "
                                             "ws.fetch(QN_URI_LOC));\n"
                    "temp1 := temp_str.ord_uselect(\"%s:%s\").hmark(0@0);\n"
                    "temp_str := nil;\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_item := temp1.leftfetchjoin(oid_item);\n"
                    "temp1 := nil;\n",
                    ns,loc);
        }
        else if (loc)
        {
            milprintf(f,
                    "var temp1 := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND)).ord_uselect(ELEMENT).hmark(0@0);\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_item := temp1.leftfetchjoin(oid_item);\n"
                    "temp1 := nil;\n");
            milprintf(f,
                    "var temp_str := mposjoin(mposjoin(oid_item, oid_cont, ws.fetch(PRE_PROP)), "
                                             "mposjoin(oid_item, oid_cont, ws.fetch(PRE_CONT)), "
                                             "ws.fetch(QN_LOC));\n"
                    "temp1 := temp_str.ord_uselect(\"%s\").hmark(0@0);\n"
                    "temp_str := nil;\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_item := temp1.leftfetchjoin(oid_item);\n"
                    "temp1 := nil;\n",
                    loc);
        }
        else if (ns)
        {
            milprintf(f,
                    "var temp1 := mposjoin(oid_item, oid_cont, ws.fetch(PRE_KIND)).ord_uselect(ELEMENT).hmark(0@0);\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_item := temp1.leftfetchjoin(oid_item);\n"
                    "temp1 := nil;\n");
            milprintf(f,
                    "var temp_str := mposjoin(mposjoin(oid_item, oid_cont, ws.fetch(PRE_PROP)), "
                                             "mposjoin(oid_item, oid_cont, ws.fetch(PRE_CONT)), "
                                             "ws.fetch(QN_URI));\n"
                    "temp1 := temp_str.ord_uselect(\"%s\").hmark(0@0);\n"
                    "temp_str := nil;\n"
                    "oid_iter := temp1.leftfetchjoin(oid_iter);\n"
                    "oid_cont := temp1.leftfetchjoin(oid_cont);\n"
                    "oid_item := temp1.leftfetchjoin(oid_item);\n"
                    "temp1 := nil;\n",
                    ns);
        }
        
        milprintf(f,
                "iter := oid_iter.tmark(0@0);\n"
                "item := oid_item.tmark(0@0);\n"
                "kind := oid_cont.tmark(0@0);\n"
                "ipik := iter;\n"
                "} # end of self axis\n");
    }
    else
    {
        /* code item_order into one integer:
           0 = input and output in iter|item order
           1 = input in item|iter and output in iter|item order
           2 = input in iter|item and output in item|iter order
           3 = input and output in item|iter order */
        int item_order = ((rev_out)?2:0) + ((rev_in)?1:0);
        
        milprintf(f,"item := item.materialize(ipik);\n");
        milprintf(f,"iter := iter.materialize(ipik);\n");
        if (kind && loc)
        {
            milprintf(f,
                    "res_scj := loop_lifted_%s_step_with_target_test"
                    "(iter, item, constant2bat(kind.get_container()), ws, %i, \"%s\");\n",
                    axis, item_order, loc);
        }
        else if (kind)
        {
            milprintf(f,
                    "res_scj := loop_lifted_%s_step_with_kind_test"
                    "(iter, item, constant2bat(kind.get_container()), ws, %i, %s);\n",
                    axis, item_order, kind);
        }
        else if (ns && loc)
        {
            milprintf(f,
                    "res_scj := loop_lifted_%s_step_with_nsloc_test"
                    "(iter, item, constant2bat(kind.get_container()), ws, %i, \"%s\", \"%s\");\n",
                    axis, item_order, ns, loc);
        }
        else if (loc)
        {
            milprintf(f,
                    "res_scj := loop_lifted_%s_step_with_loc_test"
                    "(iter, item, constant2bat(kind.get_container()), ws, %i, \"%s\");\n",
                    axis, item_order, loc);
        }
        else if (ns)
        {
            milprintf(f,
                    "res_scj := loop_lifted_%s_step_with_ns_test"
                    "(iter, item, constant2bat(kind.get_container()), ws, %i, \"%s\");\n", 
                    axis, item_order, ns);
        }
        else
        {
            milprintf(f,
                    "res_scj := loop_lifted_%s_step"
                    "(iter, item, constant2bat(kind.get_container()), ws, %i);\n", 
                    axis, item_order);
        }
    }
}

/**
 * translateLocsteps finds the right parameters for the staircasejoin
 * and calls it with this parameters
 *
 * @param f the Stream the MIL code is printed to
 * @param rev_in an integer indicating the input order (item|iter/iter|item)
 * @param rev_out an integer indicating the output order (item|iter/iter|item)
 * @param c the Core node containing the step information
 */
static void
translateLocsteps (opt_t *f, int rev_in, int rev_out, PFcnode_t *c)
{
    char *axis, *ns, *loc, *kind = "ELEM";
    PFty_t in_ty;

    milprintf(f, 
            /* variable for the (iterative) scj */
            "{ # translateLocsteps (c)\n"
            "var res_scj;"
           );

    switch (c->kind)
    {
        case c_ancestor:
            axis = "ancestor";
            break;
        case c_ancestor_or_self:
            axis = "ancestor_or_self";
            break;
        case c_attribute:
            axis = "attribute";
            kind = "ATTR";
            break;
        case c_child:
            axis = "child";
            break;
        case c_descendant:
            axis = "descendant";
            break;
        case c_descendant_or_self:
            axis = "descendant_or_self";
            break;
        case c_following:
            axis = "following";
            break;
        case c_following_sibling:
            axis = "following_sibling";
            break;
        case c_parent:
            axis = "parent";
            break;
        case c_preceding:
            axis = "preceding";
            break;
        case c_preceding_sibling:
            axis = "preceding_sibling";
            break;
        case c_self:
            axis = "self";
            break;
/* [STANDOFF] */
        case c_select_narrow:
            axis = "select_narrow";
            break;
        case c_select_wide:
            axis = "select_wide";
            break;
        case c_reject_narrow:
            axis = "reject_narrow";
            break; 
        case c_reject_wide:
            axis = "reject_wide";
            break; 
/* [/STANDOFF] */
        default:
            PFoops (OOPS_FATAL, "XPath axis is not supported in MIL-translation");
    }

    /* FIXME: here we have to include new seqtypes */
    if (L(c)->kind != c_seqtype)
    {
        mps_error ("Path step expects sequence type as kind test.");
        in_ty = PFty_none (); /* keep compilers happy */
    }
    else
    {
        in_ty = L(c)->sem.type;
    }

    if (TY_EQ (in_ty, PFty_xs_anyNode ()))
    {
        loop_liftedSCJ (f, axis, 0, 0, 0, rev_in, rev_out);
    }
    else if (TY_EQ (in_ty, PFty_comm ()))
    {
        loop_liftedSCJ (f, axis, "COMMENT", 0, 0, rev_in, rev_out);
    }
    else if (TY_EQ (in_ty, PFty_text ()))
    {
        loop_liftedSCJ (f, axis, "TEXT", 0, 0, rev_in, rev_out);
    }
    else if (PFty_subtype (in_ty, PFty_pi (NULL)))
    {
        char *target = PFqname_loc (PFty_name (in_ty));
        loop_liftedSCJ (f, axis, "PI", 0, target, rev_in, rev_out);
    }
    else if (PFty_subtype (in_ty, PFty_doc (PFty_xs_anyType ())))
    {
        loop_liftedSCJ (f, axis, "DOCUMENT", 0, 0, rev_in, rev_out);
    }
    else if (PFty_subtype (in_ty, PFty_xs_anyElement ()))
    {
        ns = PFqname_uri (PFty_name (in_ty));
        loc = PFqname_loc (PFty_name (in_ty));

        /*
         * wildcard ns/local part is represented as a NULL pointer
         */
        if (!ns && !loc)
        {
            loop_liftedSCJ (f, axis, "ELEMENT", 0, 0, rev_in, rev_out);
        }
        else
        {
            loop_liftedSCJ (f, axis, 0, ns, loc, rev_in, rev_out); 
        }

        /* test pattern we don't support */
        if (!TY_EQ (PFty_child(in_ty), PFty_xs_anyType ()))
        {
            PFlog ("element body %s in %s step ignored", 
                   PFty_str(in_ty),
                   axis);
        }
    }
    else if (PFty_subtype (in_ty, PFty_xs_anyAttribute ()))
    {
        if (strcmp (axis, "attribute"))
        {
            milprintf(f,
                    "ipik := empty_bat;\n"
                    "iter := empty_bat;\n"
                    "pos := empty_bat;\n"
                    "item := empty_bat;\n"
                    "kind := empty_kind_bat;\n"
                    "} # end of translateLocsteps (c)\n"
                   );
            return;
        }

        ns = PFqname_uri (PFty_name (in_ty));
        loc = PFqname_loc (PFty_name (in_ty));

        /*
         * wildcard ns/local part is represented as a NULL pointer
         */

        loop_liftedSCJ (f, axis, 0, ns, loc, rev_in, rev_out); 

        /* test pattern we don't support */
        if (!TY_EQ (PFty_child(in_ty), PFty_star (PFty_atomic ())))
        {
            PFlog ("attribute body %s in %s step ignored", 
                   PFty_str(in_ty),
                   axis);
        }
    }
    else
    {
        PFoops (OOPS_FATAL, "illegal node test in MIL-translation");
    }

    if (strcmp (axis, "attribute") && strcmp (axis, "self"))
    {
        /* res_scj = iter|item bat */
        milprintf(f,
                "iter := res_scj.fetch(0);\n"
                "item := res_scj.fetch(1);\n"
                "pos  := tmark_grp_unique(iter,ipik);\n"
                "kind := res_scj.fetch(2).set_kind(%s);\n"
                "ipik := item;\n"
                ,kind);
    } else {
        milprintf(f, "kind := kind.set_kind(%s);\n", kind);
    }

    milprintf(f,
            "} # end of translateLocsteps (c)\n"
           );
}

/**
 * castQName casts strings to QNames
 * - only strings are allowed 
 * - doesn't test text any further 
 * - translates only into string into local part
 *
 * @param f the Stream the MIL code is printed to
 */
static void
castQName (opt_t *f, int rc)
{
    char *item_ext = kind_str(rc);

    milprintf(f,
            "{ # castQName ()\n"
            "kind := kind.materialize(ipik);\n"
            "var qnames := kind.get_type(QNAME);\n"
            "var counted_items := kind.count();\n"
            "var counted_qn := qnames.count();\n"
            "if (counted_items != counted_qn)\n"
            "{\n"
            "var strings := kind.ord_uselect(STR);\n"
            "if (counted_items != (strings.count() + counted_qn)) "
            "{ ERROR (\"err:XPTY0004: name expression expects only string, "
            "untypedAtomic, or qname value.\"); }\n"
            "counted_items := nil;\n"

            "var oid_oid := strings.hmark(0@0);\n"
            "strings := nil;\n"
            "var oid_item := oid_oid.leftfetchjoin(item%s).materialize(oid_oid);\n"
            /* get all the unique strings */
            "strings := oid_item.tunique().hmark(0@0);\n"
            "var oid_str := strings%s;\n"
            "strings := invalid_qname(oid_str);\n"
            "if (not(isnil(strings)))"
            "{ ERROR (\"err:XPTY0004: illegal qname '%%s'\", strings); }",
            item_ext, (rc)?"":val_join(STR));

    milprintf(f,
            /* string name is only translated into local name, because
               no URIs for the namespace are available */
            "var prop_name := ws.fetch(QN_PREFIX_URI_LOC).fetch(WS);\n"

            /* find all strings which are not in the qnames of the WS */
            "var oid_str_ := [+](\"::\", oid_str);\n"
            "oid_str_ := oid_str_.tdiff(prop_name);\n"
            "oid_str := oid_str.fetch(oid_str_).tmark(0@0);\n"
            "oid_str_ := nil;\n"
            "prop_name := nil;\n"
            /* add the strings as local part of the qname into the working set */
            "ws.fetch(QN_LOC).fetch(WS).append(oid_str);\n"
            "oid_str := [+](\":\",oid_str);\n"
            "ws.fetch(QN_URI_LOC).fetch(WS).append(oid_str);\n"
            "oid_str := [+](\":\",oid_str);\n"
            "ws.fetch(QN_PREFIX_URI_LOC).fetch(WS).append(oid_str);\n"
            "oid_str := oid_str.project(\"\");\n"
            "ws.fetch(QN_URI).fetch(WS).append(oid_str);\n"
            "ws.fetch(QN_PREFIX).fetch(WS).append(oid_str);\n"
            "oid_str := nil;\n"

            /* get all the possible matching names from the updated working set */
            "var prop_id := ws.fetch(QN_PREFIX).fetch(WS).ord_uselect(\"\");\n"
            "prop_id := prop_id.mirror().leftfetchjoin(ws.fetch(QN_URI).fetch(WS)).ord_uselect(\"\");\n"
            "prop_name := prop_id.mirror().leftfetchjoin(ws.fetch(QN_LOC).fetch(WS));\n"
            "prop_id := nil;\n"

            "oid_str := oid_item%s;\n"
            "oid_item := nil;\n"
            /* get property ids for each string */
            "var oid_prop := oid_str.leftjoin(prop_name.reverse());\n"
            "oid_str := nil;\n"
            "prop_name := nil;\n"
            /* oid_prop now contains the items with property ids
               which were before strings */
            "if (counted_qn = 0) {\n"
            /* the only possible input kind is string -> oid_oid=void|void */
            "    item := oid_prop.tmark(0@0);\n"
            "} else {\n"
            /* qnames and newly generated qnames are merged 
               (first 2 parameters are the oids for the sorting) */
            "    var res_mu := merged_union"
                        "(oid_oid, "
                         "qnames.hmark(0@0), "
                         "oid_prop.tmark(0@0), "
                         "qnames.hmark(0@0).leftfetchjoin(item));\n"
            "    item := res_mu.fetch(1);\n" /* CONST? */
            "}\n"
            "kind := QNAME;\n"
            /* "ipik := item;\n" */
            "}\n"
            "} # end of castQName ()\n",
            (rc)?"":val_join(STR));
    }

/**
 * loop_liftedElemConstr creates new elements with their 
 * element and attribute subtree copies as well as the attribute 
 * contents
 *
 * @param f the Stream the MIL code is printed to
 * @param i the counter of the actual saved result (elem name)
 */
static void
loop_liftedElemConstr (opt_t *f, int rcode, int rc, int i)
{
    if (rc != NORMAL && rc != NODE)
        mps_error ("unexpected interface chosen in element construction.");

    if (rc != NORMAL)
    {
        milprintf(f,
                "{ # loop_liftedElemConstr (counter)\n"
                /* every element must have a name, but elements don't need
                   content. Therefore the second argument of the grouped
                   count has to be from the names result */
                "var iter_size := _elem_iter.reverse().leftfetchjoin(_elem_size);\n"
                "iter_size := {count}(iter_size, iter%03u.tunique(), FALSE);\n"
                "var root_iter  := iter_size.hmark(0@0).chk_order();\n"
                "var root_size  := iter_size.tmark(0@0);\n"
                "var root_prop  := iter%03u.reverse().leftfetchjoin(item%03u);\n"
                "if (not(is_constant(root_prop))) {\n"
                "    root_prop  := root_prop.tmark(0@0);\n"
                "}\n",
                i, i, i);
    
        milprintf(f,
                /* merge union root and nodes */
                "{\n"
                "var merged_result := merged_union ("
                "root_iter,  _elem_iter.chk_order(), "
                "root_size,  _elem_size, "
                "chr(0), _elem_level.[+](chr(1)), "
                "ELEMENT,  _elem_kind, "
                "root_prop,  _elem_prop, "
                "WS,  _elem_cont, "
     /* attr */ "root_iter.project(nil),  _elem_iter.mirror());\n"
    
                "root_iter  := nil;\n"
                "root_size  := nil;\n"
                "root_prop  := nil;\n"
                "_elem_iter  := merged_result.fetch(0);\n" /* CONST? */
                "_elem_size  := merged_result.fetch(1);\n" /* CONST? */
                "_elem_level := merged_result.fetch(2);\n" /* CONST? */
                "_elem_kind  := merged_result.fetch(3);\n" /* CONST? */
                "_elem_prop  := merged_result.fetch(4);\n" /* CONST? */
                "_elem_cont  := merged_result.fetch(5);\n" /* CONST? */
     /* attr */ "var preNew_preOld := merged_result.fetch(6);\n" /* CONST? */
                "merged_result := nil;\n"
     /* attr */ "_attr_own := _attr_own.leftjoin(preNew_preOld.reverse());\n"
                /* we need to help MonetDB, since we know that the order
                   preserving join (above) creates and deletes no rows */
                "_attr_own := _attr_own.tmark(0@0);\n"
     /* attr */ "preNew_preOld := nil;\n"
                "}\n"
                /* create attribute root entries */
                "{ # create attribute root entries\n"
                "var root_item := _elem_level.ord_uselect(chr(0));\n"
                "root_item := root_item.hmark(0@0);\n"
                /* root_item is aligned to iter%03u */
                "var iter_item := iter%03u.reverse().leftfetchjoin(root_item);\n"
                "root_item := nil;\n"
                "var attr_own := _r_attr_iter.leftjoin(iter_item);\n"
                "iter_item := nil;\n"
                /* insert root attribute entries to the other attributes */
                /* use iter, qn and cont to find unique combinations */
                "if (_r_attr_iter.count() != 0) { # test uniqueness\n"
                "var sorting := _r_attr_iter.tsort();\n"
                "sorting := sorting.CTrefine(mposjoin(_r_attr_qn,_r_attr_cont,ws.fetch(QN_URI_LOC)));\n"
                "var unq_attrs := sorting.tunique();\n"
                "sorting := nil;\n"
                /* test uniqueness */
                "if (unq_attrs.count() != _r_attr_iter.count())\n"
                "{\n"
                "   item%03u := materialize(item%03u,ipik%03u);\n"
                "   if (item%03u.count() > 0) {\n"
                "      ERROR (\"err:XQDY0025: attribute names are not unique "
                "in constructed element '%%s'.\",\n"
                "             item%03u.leftfetchjoin(ws.fetch(QN_LOC).fetch(WS)).fetch(0));\n"
                "   } else {\n"
                "      ERROR (\"err:XQDY0025: attribute names are not unique "
                "in constructed element.\");\n"
                "   }\n"
                "}\n"
                "unq_attrs := nil;\n"
                "} # end of test uniqueness\n"
                /* avoid inserting into `empty_bat' and because it's empty
                   just assign root attributes (they are the only ones) */
                "if (_attr_iter.count() = 0) {\n"
                "_attr_iter := _r_attr_iter;\n"
                "_attr_qn   := _r_attr_qn;\n"
                "_attr_prop := _r_attr_prop;\n"
                "_attr_cont := _r_attr_cont;\n"
                "_attr_own  := attr_own.tmark(0@0);\n"
                "} else {\n"
                "var merged_result := merged_union ("
                "_attr_iter, _r_attr_iter, "
                "_attr_qn  , _r_attr_qn,   "
                "_attr_prop, _r_attr_prop, "
                "_attr_cont, _r_attr_cont, "
                "_attr_own , attr_own);\n"
                "_attr_iter := merged_result.fetch(0);\n" /* CONST? */
                "_attr_qn   := merged_result.fetch(1);\n" /* CONST? */
                "_attr_prop := merged_result.fetch(2);\n" /* CONST? */
                "_attr_cont := merged_result.fetch(3);\n" /* CONST? */
                "_attr_own  := merged_result.fetch(4);\n" /* CONST? */
                "merged_result := nil;\n"
                "}\n"
                "_r_attr_iter := empty_bat;\n"
                "_r_attr_qn   := empty_bat;\n"
                "_r_attr_prop := empty_bat;\n"
                "_r_attr_cont := empty_bat;\n"
                "} # end of create attribute root entries\n",
                i, i, i, i, i, i);
    }
    else
    {
        milprintf(f,
                "{ # loop_liftedElemConstr (counter)\n"
                "var root_level;\n"
                "var root_size;\n"
                "var root_kind;\n"
                "var root_cont;\n"
                "var root_prop;\n"
    
     /* attr */ "var preNew_preOld;\n"
     /* attr */ "var preNew_cont;\n"
     /* attr */ "kind := kind.materialize(ipik);\n"
     /* attr */ "var attr := kind.get_type(ATTR).hmark(0@0);\n"
     /* attr */ "var attr_iter := attr.leftfetchjoin(iter).materialize(attr);\n"
     /* attr */ "var attr_item := attr.leftfetchjoin(item).materialize(attr);\n"
     /* attr */ "var attr_cont := attr.leftfetchjoin(kind).get_container();\n"
     /* attr */ "attr := nil;\n"
    
                /* there can be only nodes and attributes - everything else
                   should cause an error */
    
                "kind := kind.materialize(ipik);\n"
                "var nodes := kind.get_type(ELEM);\n"
                /* if no nodes are found we jump right to the end and only
                   have to execute the stuff for the root construction */
                "if (nodes.count() != 0) {\n"
        
                "var oid_oid := nodes.hmark(0@0);\n"
                "nodes := nil;\n"
                "var node_items := oid_oid.leftfetchjoin(item).materialize(oid_oid);\n"
                "var node_conts := oid_oid.leftfetchjoin(kind).get_container();\n"
                /* set iter to a distinct list and therefore don't
                   prune any node */
                "var iter_input := oid_oid.mirror();\n"
    
                /* get all subtree copies */
                "var res_scj := loop_lifted_descendant_or_self_step"
                "(iter_input, node_items, constant2bat(node_conts), ws, 0);\n"
    
                "iter_input := nil;\n"
                /* variables for the result of the scj */
                "var res_iter := res_scj.fetch(0);\n" /* CONST? */
                "var res_item := res_scj.fetch(1);\n" /* CONST? */
                "var res_cont := res_scj.fetch(2);\n" /* CONST? */
                "res_scj := nil;\n"
                /* res_ec is the iter|dn table resulting from the scj */
                /* create content_iter as sorting argument for the merged union */
                "var content_iter := res_iter.leftfetchjoin(oid_oid).leftfetchjoin(iter).chk_order();\n"
    
                /* create subtree copies for all bats except content_level */
                "var content_size := mposjoin(res_item, res_cont, "
                                             "ws.fetch(PRE_SIZE));\n"
                /* StM (more guessing than knowing...):
                 * Fixing the subtree sizes via correct_sizes() as for
                 * _elem_size in map2NODE_interface() above does not seem to
                 * work, here; res_item does not seem to contain complete
                 * subtrees!??
                 * Apparently, we're only dealing with container 0@0, here,
                 * i.e., the transient document, which does not contain any
                 * holes, and hence, does fixing the size is not necessary
                 * at all... !??
                 * "content_size  := correct_sizes(res_iter, res_item, content_size);\n"
                 */
                "var content_prop := mposjoin(res_item, res_cont, "
                                             "ws.fetch(PRE_PROP));\n"
                "var content_kind := mposjoin(res_item, res_cont, "
                                             "ws.fetch(PRE_KIND));\n"
                "var content_cont := mposjoin(res_item, res_cont, "
                                             "ws.fetch(PRE_CONT));\n"
    
     /* attr */ /* content_pre is needed for attribute subtree copies */
     /* attr */ "var content_pre := res_item;\n"
     /* attr */ /* as well as content_cont_pre */
     /* attr */ "var content_cont_pre := res_cont;\n"
    
                /* change the level of the subtree copies */
                /* get the level of the content root nodes */
                "var temp_ec_item := res_iter.leftfetchjoin(node_items).materialize(res_item);\n"
                "var temp_ec_cont := res_iter.leftfetchjoin(node_conts);\n"
                "nodes := res_item.mark(0@0);\n"
                "var contentRoot_level := mposjoin(temp_ec_item, "
                                                  "temp_ec_cont, "
                                                  "ws.fetch(PRE_LEVEL));\n"
                "contentRoot_level := nodes.leftfetchjoin(contentRoot_level);\n"
    
                "temp_ec_item := res_item;\n"
                "temp_ec_cont := res_cont;\n"
                "var content_level := mposjoin(temp_ec_item, temp_ec_cont, "
                                              "ws.fetch(PRE_LEVEL));\n"
                "content_level := nodes.leftfetchjoin(content_level);\n"
                "content_level := content_level.[-](contentRoot_level);\n"
                "contentRoot_level := nil;\n"
                "content_level := content_level.[+](chr(1));\n"
                /* join is made after the multiplex, because the level has to be
                   change only once for each dn-node. With the join the multiplex
                   is automatically expanded */
                "content_level := content_level.tmark(0@0);\n"
    
                /* printing output for debugging purposes */
                /*
                "print(\"content\");\n"
                "print(content_iter, content_size, [int](content_level), "
                "[int](content_kind), content_prop, content_pre, content_cont_pre);\n"
                */
    
                /* calculate the sizes for the root nodes */
                "var contentRoot_size := mposjoin(node_items, node_conts, "
                                                 "ws.fetch(PRE_SIZE)).[+](1);\n"
                /* StM (more guessing than knowing...):
                 * Fixing the subtree sizes via correct_sizes() as for
                 * _elem_size in map2NODE_interface() above does not seem to
                 * work, here; res_item does not seem to contain complete
                 * subtrees!??
                 * Apparently, we're only dealing with container 0@0, here,
                 * i.e., the transient document, which does not contain any
                 * holes, and hence, does fixing the size is not necessary
                 * at all... !??
                 * "contentRoot_size  := correct_sizes(node_iters, node_items, contentRoot_size);\n"
                 */
                "var size_oid := contentRoot_size.reverse();\n"
                "contentRoot_size := nil;\n"
                "size_oid := size_oid.leftfetchjoin(oid_oid);\n"
                "oid_oid := nil;\n"
                "var size_iter := size_oid.leftfetchjoin(iter.materialize(ipik));\n"
                "size_oid := nil;\n"
                "var iter_size := size_iter.reverse();\n"
                "size_iter := nil;\n"
                /* sums up all the sizes into an size for each iter */
                /* every element must have a name, but elements don't need
                   content. Therefore the second argument of the grouped
                   sum has to be from the names result */
               "iter_size := {sum}(iter_size, iter%03u.tunique());\n",
               i);
    
        milprintf(f,
                "root_level := chr(0);\n"
                "root_size := iter_size;\n"
                "root_kind := ELEMENT;\n"
                "root_prop := iter%03u.materialize(ipik%03u).reverse();\n"
                "root_prop := root_prop.leftfetchjoin(item%03u).materialize(root_prop);\n"
                "root_cont := WS;\n",
                i, i, i);
    
        milprintf(f,
                "root_size := root_size.tmark(0@0);\n"
                "root_prop := root_prop.tmark(0@0);\n"
                "var root_iter := iter_size.hmark(0@0).chk_order();\n"
                "iter_size := nil;\n"
    
     /* attr */ /* root_pre is a dummy needed for merge union with content_pre */
     /* attr */ "var root_pre := oid_nil;\n"
     /* attr */ /* as well as root_cont_pre */
     /* attr */ "var root_cont_pre := oid_nil;\n"
    
                /* merge union root and nodes */
                "{\n"
                "var merged_result := merged_union ("
                "root_iter, content_iter, root_size, content_size, "
                "root_level, content_level, root_kind, content_kind, "
                "root_prop, content_prop, root_cont, content_cont, "
     /* attr */ "root_pre, content_pre, root_cont_pre, content_cont_pre);\n"
                "root_iter := nil;\n"
                "content_iter := nil;\n"
                "root_size := merged_result.fetch(1);\n" /* CONST? */
                "content_size := nil;\n"
                "root_level := merged_result.fetch(2);\n" /* CONST? */
                "content_level := nil;\n"
                "root_kind := merged_result.fetch(3);\n" /* CONST? */
                "content_kind := nil;\n"
                "root_prop := merged_result.fetch(4);\n" /* CONST? */
                "content_prop := nil;\n"
                "root_cont := merged_result.fetch(5);\n" /* CONST? */
                "content_cont := nil;\n"
                "root_pre := merged_result.fetch(6);\n" /* CONST? */
                "content_pre := nil;\n"
                "root_cont_pre := merged_result.fetch(7);\n" /* CONST? */
                "content_cont_pre := nil;\n"
                "merged_result := nil;\n"
                /* printing output for debugging purposes */
                /* 
                "merged_result.print();\n"
                "print(\"merged (root & content)\");\n"
                "print(root_size, [int](root_level), [int](root_kind), root_prop);\n"
                */
                "}\n"
       
        
     /* attr */ /* preNew_preOld has in the tail old pre */
     /* attr */ /* values merged with nil values */
     /* attr */ "preNew_preOld := root_pre;\n"
     /* attr */ "root_pre := nil;\n"
     /* attr */ "preNew_cont := root_cont_pre;\n"
     /* attr */ "root_cont_pre := nil;\n"
    
                "} else { # if (nodes.count() != 0) ...\n"
               );
    
        milprintf(f, "item%03u := item%03u.materialize(ipik%03u);\n", i, i, i);
        milprintf(f, "root_level := item%03u.project(chr(0));\n", i);
        milprintf(f, "root_size := item%03u.project(0);\n", i);
        milprintf(f, "root_kind := item%03u.project(ELEMENT);\n", i);
        milprintf(f, "root_prop := item%03u;\n", i);
        milprintf(f, "root_cont := item%03u.project(WS);\n", i);
    
     /* attr */ milprintf(f,
     /* attr */ "preNew_preOld := item%03u.project(oid_nil);\n", i);
     /* attr */ milprintf(f,
     /* attr */ "preNew_preOld := preNew_preOld.tmark(0@0);\n"
     /* attr */ "preNew_cont := preNew_preOld.tmark(0@0);\n"
    
                "root_level := root_level.tmark(0@0);\n"
                "root_size := root_size.tmark(0@0);\n"
                "root_kind := root_kind.tmark(0@0);\n"
                "root_prop := root_prop.tmark(0@0);\n"
                "root_cont := root_cont.tmark(0@0);\n"
    
                "}  # end of else in 'if (nodes.count() != 0)'\n"
    
                /* set the offset for the new created trees */
                "{\n"
                "var seqb := oid(count(ws.fetch(PRE_SIZE).fetch(WS))"
                                "+ int(ws.fetch(PRE_SIZE).fetch(WS).seqbase()));\n"
                "root_level := root_level.seqbase(seqb);\n"
                "root_size := root_size.seqbase(seqb);\n"
                "root_kind := root_kind.seqbase(seqb);\n"
                "root_prop := root_prop.seqbase(seqb);\n"
                "root_cont := root_cont.seqbase(seqb);\n"
                /* get the new pre values */
     /* attr */ "preNew_preOld := preNew_preOld.seqbase(seqb);\n"
     /* attr */ "preNew_cont := preNew_cont.seqbase(seqb);\n"
                "seqb := nil;\n"
                "}\n"
                /* insert the new trees into the working set */
                "ws.fetch(PRE_LEVEL).fetch(WS).insert(root_level);\n"
                "ws.fetch(PRE_SIZE).fetch(WS).insert(root_size);\n"
                "ws.fetch(PRE_NID).fetch(WS).insert(root_size.mirror());\n"
                "ws.fetch(NID_RID).fetch(WS).insert(root_size.mirror());\n"
                "ws.fetch(PRE_KIND).fetch(WS).insert(root_kind);\n"
                "ws.fetch(PRE_PROP).fetch(WS).insert(root_prop);\n"
                "ws.fetch(PRE_CONT).fetch(WS).insert(root_cont);\n"
    
                /* printing output for debugging purposes */
                /*
                "print(\"actual working set\");\n"
                "print(Tpre_size, [int](Tpre_level), [int](Tpre_kind), Tpre_prop);\n"
                */
    
                /* save the new roots for creation of the intermediate result */
                "var roots := root_level.ord_uselect(chr(0));\n"
                "roots := roots.hmark(0@0);\n"
    
                /* resetting the temporary variables */
                "root_level := nil;\n"
                "root_size := nil;\n"
                "root_prop := nil;\n"
                "root_kind := nil;\n"
                "root_cont := nil;\n"
    
                /* adding the new constructed roots to the FRAG_ROOT bat of the
                   working set, that a following (preceding) step can check
                   the fragment boundaries */
                "# adding new fragments to the _FRAG_ROOT bat\n"
                "ws.fetch(FRAG_ROOT).fetch(WS).insert(reverse(reverse(roots).project(oid_nil)));\n"
               );
    
                /* return the root elements in iter|pos|item|kind representation */
                /* should contain for each iter exactly 1 root element
                   unless there is a thinking error */
        milprintf(f,
                "iter := iter%03u;\n"
                "item := roots;\n"
                "ipik := item;\n"
                "pos := ipik.mark(1@0);\n"
                "kind := ELEM;\n",
                i);
    
     /* attr */ /* attr translation */
     /* attr */ /* 1. step: add subtree copies of attributes */
        milprintf(f,
                "{ # create attribute subtree copies\n"
                /* get the attributes of the subtree copy elements */
                /* because also nil values from the roots are used for matching
                   and 'select(nil)' inside mvaljoin gives back all the attributes
                   not bound to a pre value first all root pre values have to
                   be thrown out */
                "var content_preNew_preOld := preNew_preOld.ord_select(oid_nil,oid_nil);\n"
                "var oid_preOld := content_preNew_preOld.tmark(0@0);\n"
                "var oid_preNew := content_preNew_preOld.hmark(0@0);\n"
                "content_preNew_preOld := nil;\n"
                "var oid_cont := oid_preNew.leftfetchjoin(preNew_cont);\n"
                "var temp_attr := get_attr_own(ws, mposjoin(oid_preOld, oid_cont, ws.fetch(PRE_NID)), oid_cont);\n"
                "oid_preOld := nil;\n"
                "var oid_attr := temp_attr.tmark(0@0);\n"
                "oid_cont := temp_attr.reverse().leftfetchjoin(oid_cont);\n"
                "oid_cont := oid_cont.tmark(0@0);\n"
                "oid_preNew := temp_attr.reverse().leftfetchjoin(oid_preNew);\n"
                "# oid_preNew := oid_preNew.tmark(0@0);\n"
                "temp_attr := nil;\n"
    
                "var seqb := oid(ws.fetch(ATTR_QN).fetch(WS).count());\n"
    
                /* get the values of the QN/OID offsets for the reference to the
                   string values */
                "var attr_qn := mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_QN));\n"
                "var attr_oid := mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_PROP));\n"
                "oid_cont := mposjoin(oid_attr, oid_cont, ws.fetch(ATTR_CONT));\n"
                "oid_attr := nil;\n"
                "attr_qn := attr_qn.seqbase(seqb);\n"
                "attr_oid := attr_oid.seqbase(seqb);\n"
                "oid_preNew := oid_preNew.tmark(seqb);\n"
                "oid_cont := oid_cont.seqbase(seqb);\n"
                "seqb := nil;\n"
    
                /* insert into working set WS the attribute subtree copies 
                   only 'offsets' where to find strings are copied 
                   (QN/CONT, OID/CONT) */
                "ws.fetch(ATTR_QN).fetch(WS).insert(attr_qn);\n"
                "ws.fetch(ATTR_PROP).fetch(WS).insert(attr_oid);\n"
                "ws.fetch(ATTR_OWN).fetch(WS).insert(oid_preNew);\n"
                "ws.fetch(ATTR_CONT).fetch(WS).insert(oid_cont);\n"
                "} # end of create attribute subtree copies\n"
               );
    
     /* attr */ /* 2. step: add attribute bindings of new root nodes */
        milprintf(f,
                "{ # create attribute root entries\n"
                /* use iter, qn and cont to find unique combinations */
                "var attr_qn_ := mposjoin(attr_item, attr_cont, ws.fetch(ATTR_QN));\n"
                "var attr_qn_cont := mposjoin(attr_item, attr_cont, ws.fetch(ATTR_CONT));\n"
                "var sorting := attr_iter.tsort();\n"
                "sorting := sorting.CTrefine(mposjoin(attr_qn_,attr_qn_cont,ws.fetch(QN_URI_LOC)));\n"
                "var unq_attrs := sorting.tunique();\n"
                "attr_qn_ := nil;\n"
                "sorting := nil;\n"
                /* test uniqueness */
                "if (unq_attrs.count() != attr_iter.count())\n"
                "{\n"
                "   if (item%03u.count() > 0) {\n"
                "      ERROR (\"err:XQDY0025: attribute names are not unique "
                "in constructed element '%%s'.\",\n"
                "             item%03u.leftfetchjoin(ws.fetch(QN_LOC).fetch(WS)).fetch(0));\n"
                "   } else {\n"
                "      ERROR (\"err:XQDY0025: attribute names are not unique "
                "in constructed element.\");\n"
                "   }\n"
                "}\n"
                "unq_attrs := nil;\n",
                i, i);
    
                /* insert it into the WS after everything else */
        milprintf(f,
                "var seqb := oid(ws.fetch(ATTR_QN).fetch(WS).count());\n"
                /* get old QN reference and copy it into the new attribute */
                "var attr_qn := mposjoin(attr_item, attr_cont, ws.fetch(ATTR_QN)).seqbase(seqb);\n"
                /* get old OID reference and copy it into the new attribute */
                "var attr_oid := mposjoin(attr_item, attr_cont, ws.fetch(ATTR_PROP)).seqbase(seqb);\n"
                /* get the iters and their corresponding new pre value (roots) and
                   multiply them for all the attributes */
                "var attr_own := iter%03u.reverse().leftfetchjoin(roots);\n"
                "roots := nil;\n"
                "attr_own := attr_iter.leftjoin(attr_own);\n"
                "attr_iter := nil;\n"
                "attr_own := attr_own.tmark(seqb);\n",
                i);
                /* use the old CONT values as reference */
        milprintf(f,
                "attr_cont := attr_cont.tmark(seqb);\n"
                "seqb := nil;\n"
      
                "ws.fetch(ATTR_QN).fetch(WS).insert(attr_qn);\n"
                "attr_qn := nil;\n"
                "ws.fetch(ATTR_PROP).fetch(WS).insert(attr_oid);\n"
                "attr_oid := nil;\n"
		"if (count(attr_own) > 0) {\n"
		"# When attr_own is empty, and the tail type is physically 'void'\n"
		"# it 'sometimes' happens that the tail's seqbase is not set (i.e., 'nil'),\n"
		"# which makes the tail logically 'NIL' iso. 'VID' or 'OID',\n"
		"# and hence causes the insert to fail due to tail-type mismatch.\n"
		"# Since I haven't found the place where the empty attr_own 'looses' its tail seqbase,\n"
		"# and since neither the tail-seqbase nor the insert has any impact if attr_own is empty,\n"
		"# we simply avoid the problem by skipping the 'NOP'-insert completely in that case.\n"
		"# Stefan.Manegold@cwi.nl\n"
                "ws.fetch(ATTR_OWN).fetch(WS).insert(attr_own);\n"
		"}\n"
                "attr_own := nil;\n"
                "ws.fetch(ATTR_CONT).fetch(WS).insert(attr_cont);\n"
                "attr_qn_cont := nil;\n"
                "attr_cont := nil;\n"
      
                "} # end of create attribute root entries\n"
      
                /* printing output for debugging purposes */
                /*
                "print(\"Theight\"); Theight.print();\n"
                "print(\"Tdoc_pre\"); Tdoc_pre.print();\n"
                "print(\"Tdoc_name\"); Tdoc_name.print();\n"
                */
      
                "} # end of loop_liftedElemConstr (counter)\n"
               );

        if (rcode != NORMAL)
            mps_error ("expected iter|pos|item|kind interface in element "
                       "construction (got %i).", rcode);
        else
            return;
    }
    if (rcode != NORMAL)
    {
        milprintf(f, "} # end of loop_liftedElemConstr (counter)\n");
    }
    else
    {
        milprintf(f,
            /* set the offset for the new created trees */
            "{\n"
            "var seqb := oid(count(ws.fetch(PRE_SIZE).fetch(WS))"
                            "+ int(ws.fetch(PRE_SIZE).fetch(WS).seqbase()));\n"
            /* get the new pre values */
 /* attr */ "var preOld_preNew := _elem_size.mark(seqb);\n"
 /* attr */ "_attr_own := _attr_own.leftfetchjoin(preOld_preNew);\n"
 /* attr */ "preOld_preNew := nil;\n"
            "_elem_size  := _elem_size.tmark(seqb);\n"
            "_elem_level := _elem_level.tmark(seqb);\n"
            "_elem_kind  := _elem_kind.tmark(seqb);\n"
            "_elem_prop  := _elem_prop.tmark(seqb);\n"
            "_elem_cont  := _elem_cont.tmark(seqb);\n"
            "seqb := nil;\n"
            "}\n"
            /* insert the new trees into the working set */
            "ws.fetch(PRE_SIZE).fetch(WS).insert(_elem_size);\n"
            "ws.fetch(PRE_NID).fetch(WS).insert(_elem_size.mirror());\n"
            "ws.fetch(NID_RID).fetch(WS).insert(_elem_size.mirror());\n"
            "ws.fetch(PRE_LEVEL).fetch(WS).insert(_elem_level);\n"
            "ws.fetch(PRE_KIND).fetch(WS).insert(_elem_kind);\n"
            "ws.fetch(PRE_PROP).fetch(WS).insert(_elem_prop);\n"
            "ws.fetch(PRE_CONT).fetch(WS).insert(_elem_cont);\n"

            /* save the new roots for creation of the intermediate result */
            "var roots := _elem_level.ord_uselect(chr(0));\n"
            "roots := roots.hmark(0@0);\n"

            /* resetting the temporary variables */
            "_elem_level := nil;\n"
            "_elem_size := nil;\n"
            "_elem_prop := nil;\n"
            "_elem_kind := nil;\n"
            "_elem_cont := nil;\n"

            /* adding the new constructed roots to the FRAG_ROOT bat of the
               working set, that a following (preceding) step can check
               the fragment boundaries */
            "# adding new fragments to the _FRAG_ROOT bat\n"
            "ws.fetch(FRAG_ROOT).fetch(WS).insert(reverse(reverse(roots).project(oid_nil)));\n"

            /* return the root elements in iter|pos|item|kind representation */
            /* should contain for each iter exactly 1 root element
               unless there is a thinking error */
            "iter := iter%03u;\n"
            "item := roots;\n"
            "ipik := item;\n"
            "pos := ipik.mark(1@0);\n"
            "kind := ELEM;\n"

            "{ # add attribute subtree copies to WS\n"
            "var seqb := oid(ws.fetch(ATTR_QN).fetch(WS).count());\n"
            "_attr_qn   := _attr_qn  .tmark(seqb);\n"
            "_attr_prop := _attr_prop.tmark(seqb);\n"
            "_attr_own  := _attr_own .tmark(seqb);\n"
            "_attr_cont := _attr_cont.tmark(seqb);\n"
            "seqb := nil;\n"

            /* insert into working set WS the attribute subtree copies 
               only 'offsets' where to find strings are copied 
               (QN/CONT, OID/CONT) */
            "ws.fetch(ATTR_QN).fetch(WS).insert(_attr_qn);\n"
            "ws.fetch(ATTR_PROP).fetch(WS).insert(_attr_prop);\n"
            "ws.fetch(ATTR_OWN).fetch(WS).insert(_attr_own);\n"
            "ws.fetch(ATTR_CONT).fetch(WS).insert(_attr_cont);\n"
            "_attr_qn   := nil;\n"
            "_attr_prop := nil;\n"
            "_attr_own  := nil;\n"
            "_attr_cont := nil;\n"
            "} # end of add attribute subtree copies to WS\n"
            "} # end of loop_liftedElemConstr (counter)\n",
            i);
    }
}


/**
 * loop-liftedAttrConstr creates new attributes which
 * are not connected to element nodes
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param i the counter of the actual saved result (attr name)
 */
static void
loop_liftedAttrConstr (opt_t *f, int rcode, int rc, int cur_level, int i)
{
    char *item_ext = kind_str(rc);
    char *str_values = (rc)?"":val_join(STR);

    milprintf(f,
            /* test qname and add "" for each empty item */
            "{ # loop_liftedAttrConstr (int i)\n"
            "if (iter%03u.count() != loop%03u.count())\n"
            "{ ERROR (\"err:XPTY0004: name expression expects only string, "
            "untypedAtomic, or qname value (got empty sequence).\"); }\n"
            "if (iter.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter, difference, item%s, %s);\n"
            "item%s := res_mu.fetch(1);\n" /* CONST? */
            "}\n",
            i, cur_level, cur_level, cur_level,
            item_ext,
            (rc)?"\"\"":"EMPTY_STRING",
            item_ext);

    milprintf(f,
            "var ws_prop_val := ws.fetch(PROP_VAL).fetch(WS);\n"
            /* add strings to PROP_VAL table (but keep the tail of PROP_VAL
               unique) */
            "var unq := item%s.tunique().hmark(0@0);\n"
            "var unq_str := unq%s;\n"
            "unq := nil;\n"
            "var str_unq := reverse(unq_str.tdiff(ws_prop_val));\n"
            "var seqb := oid(int(ws_prop_val.seqbase()) + ws_prop_val.count());\n"
            "unq_str := str_unq.hmark(seqb);\n"
            "str_unq := nil;\n"
            "seqb := nil;\n"
            "ws_prop_val := ws_prop_val.insert(unq_str);\n"
            "unq_str := nil;\n"
            /* get the property values of the strings */
            "var strings := item%s.materialize(loop%03u);\n"
            "var attr_oid := strings.leftjoin(ws_prop_val.reverse());\n"
            "strings := nil;\n",
            item_ext, str_values,
            (rc)?item_ext:val_join(STR), cur_level);
    if (rcode != NORMAL)
    {
        translateEmpty_node (f);
        milprintf(f,
                "attr_oid := attr_oid.tmark(0@0);\n"
                "_r_attr_iter := iter%03u;\n"
                "_r_attr_qn   := item%03u.materialize(ipik%03u);\n"
                "_r_attr_prop := attr_oid;\n"
                "_r_attr_cont := attr_oid.project(WS);\n"
                "attr_oid := nil;\n"
                "} # end of loop_liftedAttrConstr (int i)\n",
                i, i, i);
    }
    else
    {
        milprintf(f,
                "seqb := oid(ws.fetch(ATTR_OWN).fetch(WS).count());\n"
                "attr_oid := attr_oid.tmark(seqb);\n"
                /* add the new attribute properties */
                "ws.fetch(ATTR_PROP).fetch(WS).insert(attr_oid);\n"
                "attr_oid := nil;\n"
                "item%03u := item%03u.materialize(ipik%03u);\n"
                "var qn := item%03u.tmark(seqb);\n"
                "ws.fetch(ATTR_QN).fetch(WS).insert(qn);\n"
                "ws.fetch(ATTR_CONT).fetch(WS).insert(qn.project(WS));\n"
                "ws.fetch(ATTR_OWN).fetch(WS).insert(qn.project(oid_nil));\n"
                /* get the intermediate result */
                "iter := iter%03u;\n"
                "pos := pos%03u;\n"
                "item := iter%03u.mark(seqb);\n"
                "kind := ATTR;\n"
                "ipik := ipik%03u;\n"
                "} # end of loop_liftedAttrConstr (int i)\n",
                i, i, i, i, i, i, i, i);
    }
}

/**
 * loop_liftedTextConstr takes strings and creates new text 
 * nodes out of it and adds them to the working set
 *
 * @param f the Stream the MIL code is printed to
 */
static void
loop_liftedTextConstr (opt_t *f, int rcode, int rc)
{
    char *item_ext = kind_str(STR);

    milprintf(f,
            "{ # adding new strings to text node content and create new nodes\n"
            "var ws_prop_text := ws.fetch(PROP_TEXT).fetch(WS);\n");
    if (rc != NORMAL)
    {
        milprintf(f,
                "var unq_str := item%s.tunique().hmark(0@0);\n",
                item_ext);
    }
    else
    {    
        milprintf(f,
                "var unq := item.tunique().hmark(0@0);\n"
                "var unq_str := unq.leftfetchjoin(str_values);\n"
                "unq := nil;\n");
    }
    milprintf(f,
            "var str_unq := reverse(unq_str.tdiff(ws_prop_text));\n"
            "unq_str := nil;\n"
            "var seqb := oid(int(ws_prop_text.seqbase()) + ws_prop_text.count());\n"
            "unq_str := str_unq.hmark(seqb);\n"
            "str_unq := nil;\n"
            "seqb := nil;\n"
            "ws_prop_text := ws_prop_text.insert(unq_str);\n"
            "unq_str := nil;\n"
            /* get the property values of the strings; */
            /* we invest in sorting ws_prop_text &  X_strings/item%s on the TYPE_str join columns */
            /* as the mergejoin proved to be faster and more robust with large BATs than a hashjoin */
            "var ws_text_prop := ws_prop_text.reverse().sort();\n"
            "item%s := item%s.materialize(ipik);\n"
            "var X_item := item%s.hmark(0@0);\n"
            "var X_strings := item%s.tmark(0@0).tsort();\n"
            "var X_prop := X_strings.leftjoin(ws_text_prop);\n"
            "X_strings := nil;\n"
            "ws_text_prop := nil;\n"
            "var newPre_prop := X_item.reverse().leftjoin(X_prop);\n"
            "X_item := nil;\n"
            "X_prop := nil;\n",
            item_ext, (rc)?item_ext:val_join(STR),
            (rc)?item_ext:val_join(STR), (rc)?item_ext:val_join(STR));

    if (rcode != NORMAL)
    {
        translateEmpty_node (f);
        milprintf(f,
                "newPre_prop := newPre_prop.tmark(0@0);\n"
                "_elem_iter  := iter;\n"
                "_elem_size  := newPre_prop.project(0);\n"
                "_elem_level := newPre_prop.project(chr(0));\n"
                "_elem_kind  := newPre_prop.project(TEXT);\n"
                "_elem_prop  := newPre_prop;\n"
                "_elem_cont  := newPre_prop.project(WS);\n");
    }
    else
    {
        milprintf(f,
                "seqb := oid(count(ws.fetch(PRE_KIND).fetch(WS))"
                            "+ int(ws.fetch(PRE_KIND).fetch(WS).seqbase()));\n"
                "newPre_prop := newPre_prop.tmark(seqb);\n"
                "ws.fetch(PRE_PROP).fetch(WS).insert(newPre_prop);\n"
                "ws.fetch(PRE_SIZE).fetch(WS).insert(newPre_prop.project(0));\n"
                "ws.fetch(PRE_NID).fetch(WS).insert(newPre_prop.mirror());\n"
                "ws.fetch(NID_RID).fetch(WS).insert(newPre_prop.mirror());\n"
                "ws.fetch(PRE_LEVEL).fetch(WS).insert(newPre_prop.project(chr(0)));\n"
                "ws.fetch(PRE_KIND).fetch(WS).insert(newPre_prop.project(TEXT));\n"
                "ws.fetch(PRE_CONT).fetch(WS).insert(newPre_prop.project(WS));\n"
                "item := item%s.mark(seqb);\n"
                "kind := ELEM;\n"
       
                /* add the new constructed roots to the FRAG_ROOT bat of the working set, and adapt max height */ 
                "ws.fetch(FRAG_ROOT).fetch(WS).insert(reverse(item.project(oid_nil)));\n",
                kind_str(rc));
    }
    milprintf(f,
            "} # end of adding new strings to text node content and create new nodes\n");

}

/**
 * testCastComplete tests if the result of a Cast
 * also contains empty sequences and produces an
 * error if empty sequences are found
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param type_ the type to which it should be casted
 */
static void
testCastComplete (opt_t *f, int cur_level, PFty_t type_)
{
    milprintf(f,
            "if (iter.tunique().count() != loop%03u.count())\n"
            "{    ERROR(\"err:XPTY0004: cast to '%s' does not allow empty sequences to be casted.\"); }\n",
            cur_level, PFty_str (type_));
}


/**
 * evaluateCast casts from a given type to the target_type type
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the number indicating, which result interface is chosen
 * @param rc the number indicating, which interface the input uses
 * @param ori the struct containing the information about the 
 *        input type
 * @param target the struct containing the information about 
 *        the cast type
 * @param cast the command to execute the cast
 */
static void
evaluateCast (opt_t *f,
              int rcode, int rc,
              type_co ori,
              type_co target,
              char *cast)
{
    char *item_ext = kind_str(rcode);

    if (rc != NORMAL)
        milprintf(f,
                "var cast_val := item%s.%s;\n", kind_str(rc), cast);
    else if (ori.kind != BOOL)
        milprintf(f,
                "var cast_val := item.leftfetchjoin(%s).%s;\n",
                ori.table, cast);
    else
        milprintf(f, "var cast_val := item.%s;\n", cast);

    milprintf(f,
            "if (cast_val.texist(%s_nil))\n"
            "{    ERROR (\"err:FORG0001: could not cast value from %s to %s.\"); }\n",
            target.mil_type, ori.name, target.name);

    if (target.kind == BOOL)
        milprintf(f, "item := cast_val.[oid]();\n");
    else if (rcode == NORMAL)
        addValues (f, target, "cast_val", "item");
    else
        milprintf(f, "item%s := cast_val;\n", item_ext);

        milprintf(f,
                "cast_val := nil;\n"
                "kind := %s;\n",
                target.mil_cast);
}

/**
 * evaluateCastAny casts a (statically) heterogeneously typed sequence
 * however, in milprint_summer, kind/item may both still be runtime constants
 **/
static void 
evaluateCastAny(opt_t *f, int rc, int rcode, type_co tpe, char* str_cast) 
{
    if (rc != NORMAL) 
        mps_error ("forgot to cope with type '%s' in cast to %s.", kind_container(rc).name, tpe.name);

    milprintf(f, 
	"# cast a type-heterogeneous expression (according to static typing)\n"
	"var _val := nil;\n"
	"var _k := tunique(kind);\n"
        "if (count(_k) != 1) {\n"
	"  _val := bat(oid,%s,count(kind));\n"
	"  if (_k.exist(INT)) {\n"
        "    _val.insert(kind.ord_uselect(INT).mirror().leftfetchjoin(item).leftfetchjoin(int_values).[%s]());\n"
	"  }\n"
	"  if (_k.exist(BOOL)) {\n"
	"    _val.insert(kind.ord_uselect(BOOL).mirror().leftfetchjoin(item).[bit]().[%s]());\n"
	"  }\n"
	"  if (_k.exist(DEC) or _k.exist(DBL)) {\n"
	"    _val.insert(kind.ord_uselect(DEC,DBL).mirror().leftfetchjoin(item).leftfetchjoin(dbl_values).[%s]());\n"
	"  }\n"
	"  if (_k.exist(STR) or _k.exist(U_A)) {\n"
	"    _val.insert(kind.ord_uselect(STR,U_A).mirror().leftfetchjoin(item).leftfetchjoin(str_values).%s);\n"
	"  }\n"
	"  _val := _val.tmark(0@0).project(%s_nil).access(BAT_WRITE).replace(_val).access(BAT_READ);\n"
	"} else {\n"
	"  # at run-time, it turns out to be homogeneous\n"
	"  kind := reverse(_k).fetch(0);\n", tpe.mil_type, tpe.mil_type, tpe.mil_type, tpe.mil_type, str_cast, tpe.mil_type);

    if (rcode != NORMAL || strcmp(tpe.mil_type,"bit"))
        milprintf(f, 
	"  if (kind = BOOL) {\n"
	"    _val := [bit](item).[%s]();\n"
	"  }\n", tpe.mil_type);

    if (rcode != NORMAL || strcmp(tpe.mil_type,"lng"))
        milprintf(f, 
	"  if (kind = INT) {\n"
	"    _val := item.leftfetchjoin(int_values).[%s]();\n"
	"  }\n", tpe.mil_type);

    if (rcode != NORMAL || strcmp(tpe.mil_type,"dbl"))
        milprintf(f, 
	"  if ((kind = DEC) or (kind = DBL)) {\n"
	"    _val := item.leftfetchjoin(dbl_values).[%s]();\n"
	"  }\n", tpe.mil_type);

    if (rcode != NORMAL || strcmp(tpe.mil_type,"str"))
        milprintf(f, 
	"  if ((kind = STR) or (kind = U_A)) {\n"
	"    _val := item.leftfetchjoin(str_values).%s;\n"
	"  }\n", str_cast);

    milprintf(f, 
	"}\n"
        "if (type(_val) = void) { # idempotent cast leaves (_val = item)\n"
	"  _val := item;\n"
        "} else {\n"
        "  if (_val.texist(%s_nil))\n"
        "    ERROR (\"err:FORG0001: could not cast value to %s.\");\n", tpe.mil_type, tpe.name);

    if (tpe.kind == BOOL) {
        milprintf(f, 
	"}\nitem%s := [oid](_val);\n", kind_str(rcode));
    } else if (rcode != NORMAL) {
        milprintf(f, 
	"}\nitem%s := _val;\n", kind_str(rcode));
    } else {
        addValues(f, tpe, "_val", "item");
        milprintf(f, "}\n");
    }
    milprintf(f, 
	"kind := %s;\n", tpe.mil_cast);
}


/**
 * translateCast2INT takes an intermediate result
 * and casts all possible types to INT. It produces
 * an error if an iteration contains more than one
 * value
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the number indicating, which result interface is chosen
 * @param rc the number indicating, which interface the input uses
 * @param input_type the type of the input expression
 */
static void
translateCast2INT (opt_t *f, int rcode, int rc, PFty_t input_type)
{
    if (TY_EQ (input_type, PFty_xs_integer ()))
    {
        if (rcode != NORMAL && rc == NORMAL)
            milprintf(f,
                    "item%s := item%s;\n",
                    kind_str(rcode), val_join(rcode));
        else if (rcode == NORMAL && rc != NORMAL)
        {   /* we'r unlucky and have to add the values to the references */
            char *item = (char *) PFmalloc (strlen ("item") + strlen (kind_str(rc)) + 1);
            item = strcat (strcpy (item, "item"), kind_str(rc));

            addValues(f, int_container(), item, "item");
        }
    }
    else if (TY_EQ (input_type, PFty_xs_decimal ()))
        evaluateCast (f, rcode, rc, dec_container(), int_container(), "[lng]()");
    else if (TY_EQ (input_type, PFty_xs_double ()))
        evaluateCast (f, rcode, rc, dbl_container(), int_container(), "[lng]()");
    else if (TY_EQ (input_type, PFty_xs_string ()) ||
             TY_EQ (input_type, PFty_untypedAtomic ()))
        evaluateCast (f, rcode, rc, str_container(), int_container(), "[lng]()");
    else if (TY_EQ (input_type, PFty_xs_boolean ()))
        evaluateCast (f, rcode, rc, bool_container(), int_container(), "[lng]()");
    else /* handles the choice type */
	evaluateCastAny(f, rc, rcode, int_container(), "[lng]()");  
}

/**
 * translateCast2DEC takes an intermediate result
 * and casts all possible types to DEC. It produces
 * an error if an iteration contains more than one
 * value
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the number indicating, which result interface is chosen
 * @param rc the number indicating, which interface the input uses
 * @param input_type the type of the input expression
 */
static void
translateCast2DEC (opt_t *f, int rcode, int rc, PFty_t input_type)
{
    if (TY_EQ (input_type, PFty_xs_integer ()))
        evaluateCast (f, rcode, rc, int_container(), dec_container(), "[dbl]()");
    else if (TY_EQ (input_type, PFty_xs_decimal ()))
    {
        if (rcode != NORMAL && rc == NORMAL)
            milprintf(f,
                    "item%s := item%s;\n",
                    kind_str(rcode), val_join(rcode));
        else if (rcode == NORMAL && rc != NORMAL)
        {   /* we'r unlucky and have to add the values to the references */
            char *item = (char *) PFmalloc (strlen ("item") + strlen (kind_str(rc)) + 1);
            item = strcat (strcpy (item, "item"), kind_str(rc));

            addValues(f, dec_container(), item, "item");
        }
    }
    else if (TY_EQ (input_type, PFty_xs_double ()))
        evaluateCast (f, rcode, rc, dbl_container(), dec_container(), "[dbl]()");
    else if (TY_EQ (input_type, PFty_xs_string ()) ||
             TY_EQ (input_type, PFty_untypedAtomic ()))
        evaluateCast (f, rcode, rc, str_container(), dec_container(), "[dbl]()");
    else if (TY_EQ (input_type, PFty_xs_boolean ()))
        evaluateCast (f, rcode, rc, bool_container(), dec_container(), "[dbl]()");
    else /* handles the choice type */ 
	evaluateCastAny(f, rc, rcode, dec_container(), "[dbl]()");  
}

/**
 * translateCast2DBL takes an intermediate result
 * and casts all possible types to DBL. It produces
 * an error if an iteration contains more than one
 * value
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the number indicating, which result interface is chosen
 * @param rc the number indicating, which interface the input uses
 * @param input_type the type of the input expression
 */
static void
translateCast2DBL (opt_t *f, int rcode, int rc, PFty_t input_type)
{
    if (TY_EQ (input_type, PFty_xs_integer ()))
        evaluateCast (f, rcode, rc, int_container(), dbl_container(), "[dbl]()");
    else if (TY_EQ (input_type, PFty_xs_decimal ()))
        evaluateCast (f, rcode, rc, dec_container(), dbl_container(), "[dbl]()");
    else if (TY_EQ (input_type, PFty_xs_double ()))
    {
        if (rcode != NORMAL && rc == NORMAL)
            milprintf(f,
                    "item%s := item%s;\n",
                    kind_str(rcode), val_join(rcode));
        else if (rcode == NORMAL && rc != NORMAL)
        {   /* we'r unlucky and have to add the values to the references */
            char *item = (char *) PFmalloc (strlen ("item") + strlen (kind_str(rc)) + 1);
            item = strcat (strcpy (item, "item"), kind_str(rc));

            addValues(f, dbl_container(), item, "item");
        }
    }
    else if (TY_EQ (input_type, PFty_xs_string ()) || 
             TY_EQ (input_type, PFty_untypedAtomic ()))
        evaluateCast (f, rcode, rc, str_container(), dbl_container(), "[dbl]()");
    else if (TY_EQ (input_type, PFty_xs_boolean ()))
        evaluateCast (f, rcode, rc, bool_container(), dbl_container(), "[dbl]()");
    else /* handles the choice type */ 
	evaluateCastAny(f, rc, rcode, dbl_container(), "[dbl]()");  
}

/**
 * translateCast2STR takes an intermediate result
 * and casts all possible types to STR. It produces
 * an error if an iteration contains more than one
 * value
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the number indicating, which result interface is chosen
 * @param rc the number indicating, which interface the input uses
 * @param input_type the type of the input expression
 */
static void
translateCast2STR (opt_t *f, int rcode, int rc, PFty_t input_type)
{
    type_co cast_container = (rcode == STR)?str_container():u_A_container();

    if (TY_EQ (input_type, PFty_xs_integer ()))
        evaluateCast (f, rcode, rc, int_container(), cast_container, "[str]()");
    else if (TY_EQ (input_type, PFty_xs_decimal ()))
        evaluateCast (f, rcode, rc, dec_container(), cast_container, "[str]()");
    else if (TY_EQ (input_type, PFty_xs_double ()))
        evaluateCast (f, rcode, rc, dbl_container(), cast_container, "[str]()");
    else if (TY_EQ (input_type, PFty_xs_string ()) ||
             TY_EQ (input_type, PFty_untypedAtomic ()) ||
             /* cope with special case introduced by simplifyCoreTree in
                combination with setting the input type of fn:concat as atomic?
                (see ``FuncArgList: args (Expr, FuncArgList)'' in fs.brg) */
             (TY_EQ (input_type, PFty_atomic ()) && rc == U_A))
    {
        if (rcode != NORMAL && rc == NORMAL)
            milprintf(f,
                    "item%s := item%s;\n",
                    kind_str(rcode), val_join(rcode));
        else if (rcode == NORMAL && rc != NORMAL)
        {   /* we'r unlucky and have to add the values to the references */
            char *item = (char *) PFmalloc (strlen ("item") + strlen (kind_str(rc)) + 1);
            item = strcat (strcpy (item, "item"), kind_str(rc));

            addValues(f, str_container(), item, "item");
        }

        if (TY_EQ (input_type, PFty_xs_string ()) && rcode == U_A)
            milprintf(f, "kind := U_A;\n");
        else if (TY_EQ (input_type, PFty_untypedAtomic ()) && rcode == STR)
            milprintf(f, "kind := STR;\n");
    }
    else if (TY_EQ (input_type, PFty_xs_boolean ()))
        evaluateCast (f, rcode, rc, bool_container(), cast_container, "leftfetchjoin(bool_str)");
    else /* handles the choice type */ 
	evaluateCastAny(f, rc, rcode, cast_container, "[str]()");  
}

/**
 * translateCast2BOOL takes an intermediate result
 * and casts all possible types to BOOL. It produces
 * an error if an iteration contains more than one
 * value
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the number indicating, which result interface is chosen
 * @param rc the number indicating, which interface the input uses
 * @param input_type the type of the input expression
 */
static void
translateCast2BOOL (opt_t *f, int rcode, int rc, PFty_t input_type)
{
    if (TY_EQ (input_type, PFty_xs_integer ()))
        evaluateCast (f, rcode, rc, int_container(), bool_container(), "[bit]()");
    else if (TY_EQ (input_type, PFty_xs_decimal ()))
        evaluateCast (f, rcode, rc, dec_container(), bool_container(), "[bit]()");
    else if (TY_EQ (input_type, PFty_xs_double ()))
        evaluateCast (f, rcode, rc, dbl_container(), bool_container(), "[bit]()");
    else if (TY_EQ (input_type, PFty_xs_string ()) ||
             TY_EQ (input_type, PFty_untypedAtomic ()))
        evaluateCast (f, rcode, rc, str_container(), bool_container(), "[!=](\"\")");
    else if (TY_EQ (input_type, PFty_xs_boolean ()));
    else /* handles the choice type */ 
	evaluateCastAny(f, NORMAL, BOOL, bool_container(), "[!=](\"\")");  
}

/**
 * translateCast decides wether the cast can be evaluated
 * and then calls the specific cast function
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param rc the number indicating, which interface the input uses
 * @param cur_level the level of the for-scope
 * @param c the Core node containing the rest of the subtree
 * @result the kind indicating, which result interface is chosen
 */
static int
translateCast (opt_t *f, 
               int code, int rc,
               int cur_level, PFcnode_t *c)
{
    int rcode; /* the return code */

    PFty_t cast_type = PFty_defn (L(c)->sem.type);
    PFty_t input_type = PFty_defn (TY(R(c)));
     
    input_type = PFty_prime(input_type);

    milprintf(f,
            "{ # cast from %s to %s\n", 
            PFty_str(PFty_defn (TY(R(c)))),
            PFty_str(PFty_defn (L(c)->sem.type)));

    if (PFty_subtype (cast_type, PFty_star (PFty_xs_boolean ()))) {
        rcode = NORMAL;
        translateCast2BOOL (f, rcode, rc, input_type);
    } else if (PFty_subtype (cast_type, PFty_star (PFty_xs_integer ()))) {
        rcode = (code)?INT:NORMAL;
        translateCast2INT (f, rcode, rc, input_type);
    } else if (PFty_subtype (cast_type, PFty_star (PFty_xs_decimal ()))) {
        rcode = (code)?DEC:NORMAL;
        translateCast2DEC (f, rcode, rc, input_type);
    } else if (PFty_subtype (cast_type, PFty_star (PFty_xs_double ()))) {
        rcode = (code)?DBL:NORMAL;
        translateCast2DBL (f, rcode, rc, input_type);
    } else if (PFty_subtype (cast_type, PFty_star (PFty_xs_QName ())) ||
               PFty_subtype (cast_type, PFty_star (PFty_untypedAtomic ()))) {
        rcode = (code)?U_A:NORMAL;
        translateCast2STR (f, rcode, rc, input_type);
    } else if (PFty_subtype (cast_type, PFty_star (PFty_xs_string ()))) {
        rcode = (code)?STR:NORMAL;
        translateCast2STR (f, rcode, rc, input_type);
    } else
        PFoops (OOPS_TYPECHECK,
                "can't cast type '%s' to type '%s'",
                PFty_str(TY(R(c))),
                PFty_str(L(c)->sem.type));

    if (PFty_subtype (cast_type, PFty_plus (PFty_item ())))
        testCastComplete (f, cur_level, cast_type);

    milprintf(f,
            "} # end of cast from %s to %s\n", 
            PFty_str(PFty_defn (TY(R(c)))),
            PFty_str(PFty_defn (L(c)->sem.type)));

    return rcode;
}

/**
 * evaluateOp evaluates a operation and gives back a new
 * item column with the references to the updated values.
 * It doesn't test, if the intermediate results are aligned
 * and therefore doesn't work, if either the order of iter
 * is not given or some rows (like in the optional case)
 * are missing.
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the number indicating, which result interface is chosen
 * @param rc1 the number indicating, which interface the first input uses
 * @param rc2 the number indicating, which interface the second input uses
 * @param counter the actual offset of saved variables
 * @param operator the operator which is evaluated
 * @param t_co the container containing the type information of the
 *        values
 * @param div enables the test wether a division is made
 */
static void
evaluateOp (opt_t *f, int rcode, int rc1, int rc2,
            int counter, char *operator, type_co t_co, char *div)
{
    milprintf(f, "{ # '%s' calculation\n", operator);
    if (rc2 != NORMAL)
        milprintf(f, "var val_snd := item%s;\n", kind_str(rc2));
    else
        milprintf(f, "var val_snd := item.leftfetchjoin(%s);\n", t_co.table);

    if (rc1 != NORMAL)
        milprintf(f, "var val_fst := item%s%03u;\n", kind_str(rc1), counter);
    else
        milprintf(f, "var val_fst := item%03u.leftfetchjoin(%s);\n", counter, t_co.table);

    if (div)
        milprintf(f, 
                "if (val_snd.texist(%s)) \n"
                "    { ERROR (\"err:FOAR0001: division by 0 is forbidden.\"); }\n",  div);
    milprintf(f, "val_fst := [%s](val_fst,val_snd);\n", operator);

    if (rcode == BOOL) {
        milprintf(f, "item := val_fst.[oid]();\n");
        milprintf(f, "kind := BOOL;\n");
    } else if (rcode != NORMAL) {
        milprintf(f, "item%s := val_fst;\n", kind_str(rcode)); 
    } else {
        addValues(f, t_co, "val_fst", "item"); 
    }
    milprintf(f, "} # end of '%s' calculation\n", operator);
}

/**
 * evaluateOpOpt evaluates a operation and gives back a new 
 * intermediate result. It first gets the iter values and joins
 * the two input with their iter values, which makes it possible
 * to evaluate operations where the operands are empty
 *
 * @param f the Stream the MIL code is printed to
 * @param rcode the number indicating, which result interface is chosen
 * @param rc1 the number indicating, which interface the first input uses
 * @param rc2 the number indicating, which interface the second input uses
 * @param counter the actual offset of saved variables
 * @param operator the operator which is evaluated
 * @param t_co the container containing the type information of the
 *        values
 * @param kind the type of the new intermediate result
 * @param div enables the test wether a division is made
 */
static void
evaluateOpOpt (opt_t *f, int rcode, int rc1, int rc2,
               int counter, char *operator,
               type_co t_co, char *kind, char *div)
{
    milprintf(f, "{ # '%s' calculation with optional type\n", operator);
    milprintf(f, "var iters := mergejoin(iter.materialize(ipik).chk_order(), iter%03u.materialize(ipik%03u).chk_order().reverse());\n", counter, counter);

    if (rc2 != NORMAL)
        milprintf(f, "var val_snd := iters.hmark(0@0).leftfetchjoin(item%s);\n", 
		kind_str(rc2));
    else
        milprintf(f, "var val_snd := iters.hmark(0@0).leftfetchjoin(item).leftfetchjoin(%s);\n", 
                t_co.table);

    if (rc1 != NORMAL)
        milprintf(f, "var val_fst := iters.tmark(0@0).leftfetchjoin(item%s%03u);\n", 
		kind_str(rc1), counter);
    else
        milprintf(f, "var val_fst := iters.tmark(0@0).leftfetchjoin(item%03u).leftfetchjoin(%s);\n", 
                counter, t_co.table);

    if (div)
        milprintf(f, 
                "if (val_snd.texist(%s))\n"
                "    { ERROR (\"err:FOAR0001: division by 0 is forbidden.\"); }\n", div);
    milprintf(f, "val_fst := [%s](val_fst,val_snd);\n", operator);
    milprintf(f, "iter := iters.hmark(0@0).leftfetchjoin(iter);\n");
    milprintf(f, "ipik := iter;\n");
    milprintf(f, "pos := 1@0;\n");
    milprintf(f, "kind := %s;\n", kind);

    if (rcode == BOOL) {
        milprintf(f, "item := val_fst.[oid]();\n");
    } else if (rcode != NORMAL) {
        milprintf(f, "item%s := val_fst;\n", kind_str(rcode)); 
    } else { 
        addValues(f, t_co, "val_fst", "item"); 
    }
    milprintf(f, "} # end of '%s' calculation with optional type\n", operator);
}

/**
 * translateOperation takes a operator and a core tree node
 * containing two arguments and calls according to the input
 * type a helper function, which evaluates the operation on
 * the intermediate results of the input
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param operator the operator which is evaluated
 * @args the head of the argument list
 * @param div enables the test wether a division is made
 * @result the kind indicating, which result interface is chosen
 */
static int
translateOperation (opt_t *f, int code, int cur_level, int counter, 
                    char *operator, PFcnode_t *args, bool div)
{
    int rcode, rc1, rc2;
    PFty_t expected = TY(L(args));

    /* translate the subtrees */
    rc1 = translate2MIL (f, VALUES, cur_level, counter, L(args));
    counter++;
    saveResult_ (f, counter, rc1);

    rc2 = translate2MIL (f, VALUES, cur_level, counter, RL(args));

    /* evaluate the operation */
    if (TY_EQ(expected, PFty_xs_integer()))
    {
        rcode = (code)?INT:NORMAL;
        evaluateOp (f, rcode, rc1, rc2, counter, operator,
                    int_container(), (div)?"0LL":NULL);
    }
    else if (TY_EQ(expected, PFty_xs_double()))
    {
        rcode = (code)?DBL:NORMAL;
        evaluateOp (f, rcode, rc1, rc2, counter, operator,
                    dbl_container(), (div)?"dbl(0)":NULL);
    }
    else if (TY_EQ(expected, PFty_xs_decimal()))
    {
        rcode = (code)?DEC:NORMAL;
        evaluateOp (f, rcode, rc1, rc2, counter, operator,
                    dec_container(), (div)?"dbl(0)":NULL);
    }
    else if (TY_EQ(expected, PFty_opt(PFty_xs_integer())))
    {
        rcode = (code)?INT:NORMAL;
        evaluateOpOpt (f, rcode, rc1, rc2, counter, operator,
                       int_container(), "INT", (div)?"0LL":NULL);
    }
    else if (TY_EQ(expected, PFty_opt(PFty_xs_double())))
    {
        rcode = (code)?DBL:NORMAL;
        evaluateOpOpt (f, rcode, rc1, rc2, counter, operator,
                       dbl_container(), "DBL", (div)?"dbl(0)":NULL);
    }
    else if (TY_EQ(expected, PFty_opt(PFty_xs_decimal())))
    {
        rcode = (code)?DEC:NORMAL;
        evaluateOpOpt (f, rcode, rc1, rc2, counter, operator,
                       dec_container(), "DEC", (div)?"dbl(0)":NULL);
    }
    else
    {
	rcode = NORMAL;
        mps_error ("result type '%s' is not supported in operation '%s'.",
                   PFty_str(expected), operator);
    }

    /* clear the intermediate result of the second subtree */
    deleteResult_ (f, counter, rc1);
    return rcode;
}

/**
 * evaluateComp evaluates a comparison and gives back a new
 * item column with the boolean values.
 * It doesn't test, if the intermediate results are aligned
 * and therefore doesn't work, if either the order of iter
 * is not given or some rows (like in the optional case)
 * are missing. 
 *
 * @param f the Stream the MIL code is printed to
 * @param rc1 the number indicating, which interface the first input uses
 * @param rc2 the number indicating, which interface the second input uses
 * @param counter the actual offset of saved variables
 * @param comp the comparison which is evaluated
 * @param table the name of the table where the values are saved
 */
static void
evaluateComp (opt_t *f, int rc1, int rc2,
              int counter, char *operator, type_co t_co)
{
    evaluateOp(f, BOOL, (t_co.kind != BOOL)?rc1:BOOL, (t_co.kind != BOOL)?rc2:BOOL, counter, operator, t_co, NULL);
}

/**
 * evaluateCompOpt evaluates a comparison and gives back a new 
 * intermediate result. It first gets the iter values and joins
 * the two input with their iter values, which makes it possible
 * to evaluate comparisons where the operands are empty
 *
 * @param f the Stream the MIL code is printed to
 * @param rc1 the number indicating, which interface the first input uses
 * @param rc2 the number indicating, which interface the second input uses
 * @param counter the actual offset of saved variables
 * @param comp the comparison which is evaluated
 * @param table the name of the table where the values are saved
 */
static void
evaluateCompOpt (opt_t *f, int rc1, int rc2, 
                 int counter, char *operator, type_co t_co)
{
    evaluateOpOpt(f, BOOL, (t_co.kind != BOOL)?rc1:BOOL, (t_co.kind != BOOL)?rc2:BOOL, counter, operator, t_co, "BOOL", NULL);
}

/**
 * translateComparison takes a operator and a core tree node
 * containing two arguments and calls according to the input
 * type a helper function, which evaluates the comparison on
 * the intermediate results of the input
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param comp the comparison which is evaluated
 * @args the head of the argument list
 * @result the kind indicating, which result interface is chosen (NORMAL)
 */
static int
translateComparison (opt_t *f, int cur_level, int counter, 
                     char *comp, PFcnode_t *args)
{
    int rc1, rc2;
    PFty_t expected = TY(L(args));

    /* translate the subtrees */
    rc1 = translate2MIL (f, VALUES, cur_level, counter, L(args));
    counter++;
    saveResult_ (f, counter, rc1); 

    rc2 = translate2MIL (f, VALUES, cur_level, counter, RL(args));

    /* evaluate the comparison */
    if (TY_EQ(expected, PFty_xs_integer()))
    {
        evaluateComp (f, rc1, rc2, counter, comp, int_container());
    }
    else if (TY_EQ(expected, PFty_xs_double()))
    {
        evaluateComp (f, rc1, rc2, counter, comp, dbl_container());
    }
    else if (TY_EQ(expected, PFty_xs_decimal()))
    {
        evaluateComp (f, rc1, rc2, counter, comp, dec_container());
    }
    else if (TY_EQ(expected, PFty_xs_boolean()))
    {
        evaluateComp (f, rc1, rc2, counter, comp, bool_container());
    }
    else if (TY_EQ(expected, PFty_xs_string()))
    {
        evaluateComp (f, rc1, rc2, counter, comp, str_container());
    }
    else if (TY_EQ(expected, PFty_opt(PFty_xs_integer())))
    {
        evaluateCompOpt (f, rc1, rc2, counter, comp, int_container());
    }
    else if (TY_EQ(expected, PFty_opt(PFty_xs_double())))
    {
        evaluateCompOpt (f, rc1, rc2, counter, comp, dbl_container());
    }
    else if (TY_EQ(expected, PFty_opt(PFty_xs_decimal())))
    {
        evaluateCompOpt (f, rc1, rc2, counter, comp, dec_container());
    }
    else if (TY_EQ(expected, PFty_opt(PFty_xs_boolean())))
    {
        evaluateCompOpt (f, rc1, rc2, counter, comp, bool_container());
    }
    else if (TY_EQ(expected, PFty_opt(PFty_xs_string())))
    {
        evaluateCompOpt (f, rc1, rc2, counter, comp, str_container());
    }
    else
        mps_error ("type '%s' is not supported in comparison '%s'.",
                   PFty_str(expected), comp);

    /* clear the intermediate result of the second subtree */
    deleteResult_ (f, counter, rc1);

    return NORMAL;
}

/**
 * fn:boolean translates the XQuery function fn:boolean.
 *
 * @param f the Stream the MIL code is printed to
 * @param rc the number indicating, which interface the input uses
 * @param cur_level the level of the for-scope
 * @param input_type the input type of the Core expression
 *        which has to evaluated
 */
static void
fn_boolean (opt_t *f, int rc, int cur_level, PFty_t input_type)
{
    /* first analyze how big the sequences are per iter */
    milprintf(f,
            "{ # translate fn:boolean (item*) as boolean\n"
            "iter := iter.materialize(ipik);\n"
            "var iter_count := {count}(iter.reverse(), loop%03u.reverse(), FALSE);\n"
            "var iter_pos := {min}(iter.reverse());\n"
            "var sequences := iter_count.ord_uselect(2,int_nil);\n"
            "var emptyseqs := iter_count.tmark(0@0).ord_uselect(0,0);\n", cur_level);

    /* error check. Note that kind maybe constant, leftfetchjoin and min both work here  */
    milprintf(f,
            "# incomprehensible XQuery semantics: |sequences|>1 are true, but *must* start with a node\n"
            "if (bit(count(sequences))) {\n"
            "  if (sequences.hmark(0@0).join(iter_pos).leftfetchjoin(kind).min() < NODE)\n" 
            "  {  ERROR (\"err:FORG0006: boolean() cannot handle sequences with length>1 that start with an atomic.\"); }\n"
            "}\n");

    /* again kind/item constant safe. we just get the first item of each sequence now */
    milprintf(f,
            "# get the first item of each non-empty sequence\n"
            "iter := iter_pos.hmark(0@0);\n"
            "item%s := iter_pos.leftfetchjoin(item%s).tmark(0@0);\n"
            "kind := iter_pos.leftfetchjoin(kind).tmark(0@0);\n"
            "ipik := iter;\n"
            "pos := 1@0;\n", kind_str(rc), kind_str(rc));

    /* if any of the values are of type node, replace them by a true */
    if (rc == NORMAL) /* otherwise no nodes anyway */
    milprintf(f,
            "# replace all nodes by true (also the single-node sequences!!)\n"
            "if (type(kind) = bat) {\n"
            "  sequences := kind.ord_uselect(NODE,int_nil);\n" 
            "  if (bit(count(sequences))) {\n"
            "    sequences := sequences.project(1@0);\n"
            "    item.access(BAT_WRITE).replace(sequences).access(BAT_READ);\n"
            "    sequences := sequences.project(BOOL);\n"
            "    kind.access(BAT_WRITE).replace(sequences).access(BAT_READ);\n"
            "  }\n"
            "} else { if (kind = NODE) {\n"
            "  item := 1@0;\n"
            "  kind := BOOL;\n"
            "}}\n");

    /* now that everything is a single atomic value, cast them to bool */
    translateCast2BOOL (f, NORMAL, rc, input_type);

    /* finally, if applicable, get back the empty sequences as falses. */
    milprintf(f,
            "if (bit(count(emptyseqs))) {\n"
            "  item := loop%03u.tmark(oid_nil).outerjoin(reverse(iter)).outerjoin(item).seqbase(0@0);\n"
            "  iter := loop%03u.tmark(0@0);\n"
            "  ipik := item;\n"
            "  item.access(BAT_WRITE).replace(emptyseqs.project(0@0)).access(BAT_READ);\n"
            "}\n", cur_level, cur_level);

    milprintf(f,
            "} # end of translate fn:boolean (item*) as boolean\n");
}

/**
 * combine_strings concatenates all the strings of each iter
 * and adds the newly created strings to the string container
 * 'str_values'
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param rc the number indicating, which interface the input uses
 */
static void
combine_strings (opt_t *f, int code, int rc)
{
    char *item_ext = kind_str(rc);

    milprintf(f,
            "{ # combine_strings\n"
            "var iter_item := iter.reverse().leftfetchjoin(item%s);\n"
            "var iter_str := iter_item%s.chk_order();\n"
            "iter_item := nil;\n"
            "iter_str := iter_str.string_join(iter.tunique().project(\" \"));\n"
            "iter := iter_str.hmark(0@0);\n"
            "ipik := iter;\n"
            "pos := ipik.mark(1@0);\n"
            "kind := U_A;\n",
            item_ext, (rc)?"":val_join(U_A));
    if (code)
        milprintf(f, "item%s := iter_str;\n", item_ext);
    else
        addValues (f, str_container(), "iter_str", "item");
    milprintf(f, "} # end of combine_strings\n");
}

/**
 * string_value gets the string value(s) of a node or an attribute.
 * The values are concatenated to one string (string-value function).
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param kind the string indicating, whether the result should be STR or U_A
 */
static void
string_value (opt_t *f, int code, char *kind)
{
    char *empty_string = (code)?"\"\"":"EMPTY_STRING";
    char *item_ext = (code)?kind_str(STR):"";

    /* to avoid executing to much code there are three cases:
       - only elements
       - only attributes
       - elements and attributes 
       This makes of course the code listed here bigger :) */
    milprintf(f,
            "{ # string-value\n"
            /* save input iters to return empty string for rows
               which had no text content */
            "var input_iter := iter;\n"
            "kind := kind.materialize(ipik);\n"
            "item := item.materialize(ipik);\n"
            "iter := iter.materialize(ipik);\n"
            "var kind_elem := kind.get_type(ELEM);\n"
            "var item_str;\n"
            /* only elements */
            "if (kind_elem.count() = kind.count())\n"
            "{\n"
                "kind_elem := nil;\n"
                "var cont := kind.get_container();\n"
                /* to get all text nodes a scj is performed */
                "var res_scj := "
                "loop_lifted_descendant_or_self_step_with_kind_test"
                "(iter, item, constant2bat(cont), ws, 0, TEXT);\n"
                /* variables for the result of the scj */
                "var t_iter := res_scj.fetch(0);\n" /* CONST? */
                "var t_item := res_scj.fetch(1);\n" /* CONST? */
                "var t_cont := res_scj.fetch(2);\n" /* CONST? */
                "res_scj := nil;\n"
                /* get the string values of the text nodes */
                "var t_item_str := mposjoin(mposjoin(t_item, t_cont, ws.fetch(PRE_PROP)), "
                                     "mposjoin(t_item, t_cont, ws.fetch(PRE_CONT)), "
                                     "ws.fetch(PROP_TEXT));\n"
                "t_cont := nil;\n"
                /* for the result of the scj join with the string values */
                "var t_iter_unq := t_iter.tunique();\n"
                "t_iter := t_iter.materialize(t_item);\n"
                "if (t_iter_unq.count() != t_item.count()) {\n"
                "    var iter_item := t_iter.reverse().leftfetchjoin(t_item_str).chk_order();\n"
                "    iter_item := iter_item.string_join(t_iter_unq.project(\"\"));\n"
                "    t_iter := iter_item.hmark(0@0);\n"
                "    t_item_str := iter_item.tmark(0@0);\n"
                "}\n"
                "t_iter_unq := nil;\n");

    milprintf(f,
                /* get the string value of all comment nodes */
                "var c_map := mposjoin (item, cont, ws.fetch (PRE_KIND))"
                                  ".select(COMMENT).hmark(0@0);\n"
                "if (c_map.count() > 0) { #process comments \n"
                    "var c_iter := c_map.leftfetchjoin(iter);\n"
                    "var c_item := c_map.leftfetchjoin(item);\n"
                    "var c_cont := c_map.leftfetchjoin(cont);\n"
                    /* get the string values of the comment nodes */
                    "var c_item_str := mposjoin(mposjoin(c_item, c_cont, ws.fetch(PRE_PROP)), "
                                               "mposjoin(c_item, c_cont, ws.fetch(PRE_CONT)), "
                                               "ws.fetch(PROP_COM));\n"
                    "c_item := nil;\n"
                    "c_cont := nil;\n"
                    /* merge strings from element and attribute */
                    "var res_mu := merged_union (t_iter, c_iter, t_item_str, c_item_str);\n"
                    "t_iter := res_mu.fetch(0);\n"
                    "t_item_str := res_mu.fetch(1);\n"
                "} # end of comment processing\n"
                /* get the string value of all processing-instruction nodes */
                "var pi_map := mposjoin (item, cont, ws.fetch (PRE_KIND))"
                                   ".select(PI).hmark(0@0);\n"
                "if (pi_map.count() > 0) { #process processing-instructions \n"
                    "var pi_iter := pi_map.leftfetchjoin(iter);\n"
                    "var pi_item := pi_map.leftfetchjoin(item);\n"
                    "var pi_cont := pi_map.leftfetchjoin(cont);\n"
                    /* get the string values of the processing-instruction nodes */
                    "var pi_item_str := mposjoin(mposjoin(pi_item, pi_cont, ws.fetch(PRE_PROP)), "
                                                "mposjoin(pi_item, pi_cont, ws.fetch(PRE_CONT)), "
                                                "ws.fetch(PROP_INS));\n"
                    "pi_item := nil;\n"
                    "pi_cont := nil;\n"
                    /* merge strings from element and attribute */
                    "var res_mu := merged_union (t_iter, pi_iter, t_item_str, pi_item_str);\n"
                    "t_item_str := res_mu.fetch(1);\n"
                "} # end of processing-instruction processing\n"
                "iter := t_iter;\n"
                "item_str := t_item_str;\n"
                "t_iter := nil;\n"
                "t_item_str := nil;\n");
    
    milprintf(f,
            "} else {\n"
                "var kind_attr := kind.get_type(ATTR);\n"
                /* only attributes */
                "if (kind_attr.count() = kind.count())\n"
                "{\n"
                    "kind_attr := nil;\n"
                    "var cont := kind.get_container();\n"
                    "item_str := mposjoin(mposjoin(item, cont, ws.fetch(ATTR_PROP)), "
                                         "mposjoin(item, cont, ws.fetch(ATTR_CONT)), "
                                         "ws.fetch(PROP_VAL));\n"
                    "item := nil;\n"
                "} else {\n"
                    /* handles attributes and elements */
                    /* get attribute string values */
                    "kind_attr := kind_attr.hmark(0@0);\n"
                    "var item_attr := kind_attr.leftfetchjoin(item);\n"
                    "var iter_attr := kind_attr.leftfetchjoin(iter);\n"
                    "var cont := kind_attr.leftfetchjoin(kind).get_container();\n"
                    "kind_attr := nil;\n"
                    "var item_attr_str "
                        ":= mposjoin(mposjoin(item_attr, cont, ws.fetch(ATTR_PROP)), "
                                    "mposjoin(item_attr, cont, ws.fetch(ATTR_CONT)), "
                                    "ws.fetch(PROP_VAL));\n"
                    "item_attr := nil;\n"
                    "cont := nil;\n"
                    /* get element string values */
                    "kind_elem := kind_elem.hmark(0@0);\n"
                    "iter := kind_elem.leftfetchjoin(iter).materialize(kind_elem);\n"
                    "cont := kind_elem.leftfetchjoin(kind).get_container();\n"
                    "item := kind_elem.leftfetchjoin(item).materialize(kind_elem);\n"
                    "kind_elem := nil;\n"
                    /* to get all text nodes a scj is performed */
                    "var res_scj := "
                    "loop_lifted_descendant_or_self_step_with_kind_test"
                    "(iter, item, constant2bat(cont), ws, 0, TEXT);\n"
                    /* variables for the result of the scj */
                    "var t_iter := res_scj.fetch(0);\n" /* CONST? */
                    "var t_item := res_scj.fetch(1);\n" /* CONST? */
                    "var t_cont := res_scj.fetch(2);\n" /* CONST? */
                    "res_scj := nil;\n"
                    /* get the string values of the text nodes */
                    "var t_item_str := mposjoin(mposjoin(t_item, t_cont, ws.fetch(PRE_PROP)), "
                                               "mposjoin(t_item, t_cont, ws.fetch(PRE_CONT)), "
                                               "ws.fetch(PROP_TEXT));\n"
                    "t_cont := nil;\n"
                    /* for the result of the scj join with the string values */
                    "var iter_item := t_iter.materialize(t_item).reverse().leftfetchjoin(t_item_str);\n"
                    "t_item := nil;\n"
                    "t_iter := iter_item.hmark(0@0);\n"
                    "t_item_str := iter_item.tmark(0@0);\n"
                    /* merge strings from element and attribute */
                    "var res_mu := merged_union (t_iter, iter_attr, t_item_str, item_attr_str);\n"
                    "t_iter := res_mu.fetch(0);\n" /* CONST? */
                    "t_item_str := res_mu.fetch(1);\n" /* CONST? */
                    "res_mu := nil;\n"
                    "iter_item := t_iter.reverse().leftfetchjoin(t_item_str).chk_order();\n"
                    "iter := nil;\n"
                    "item_str := nil;\n");
    milprintf(f,  
                    " { var item_unq := iter_item.reverse().tunique();\n"
                    "   if (item_unq.count() != iter_item.count())\n"
                    "     iter_item := iter_item.string_join(item_unq.project(\"\"));\n}\n");

    milprintf(f,
                    /* get the string value of all comment nodes */
                    "t_iter := iter_item.hmark(0@0);\n"
                    "var t_item_str := iter_item.tmark(0@0);\n"
                    "var c_map := mposjoin (item, cont, ws.fetch (PRE_KIND))"
                                      ".select(COMMENT).hmark(0@0);\n"
                    "if (c_map.count() > 0) { #process comments \n"
                        "var c_iter := c_map.leftfetchjoin(iter);\n"
                        "var c_item := c_map.leftfetchjoin(item);\n"
                        "var c_cont := c_map.leftfetchjoin(cont);\n"
                        /* get the string values of the comment nodes */
                        "var c_item_str := mposjoin(mposjoin(c_item, c_cont, ws.fetch(PRE_PROP)), "
                                                   "mposjoin(c_item, c_cont, ws.fetch(PRE_CONT)), "
                                                   "ws.fetch(PROP_COM));\n"
                        "c_item := nil;\n"
                        "c_cont := nil;\n"
                        /* merge strings from element and attribute */
                        "var res_mu := merged_union (t_iter, c_iter, t_item_str, c_item_str);\n"
                        "t_iter := res_mu.fetch(0);\n"
                        "t_item_str := res_mu.fetch(1);\n"
                    "} # end of comment processing\n"
                    /* get the string value of all processing-instruction nodes */
                    "var pi_map := mposjoin (item, cont, ws.fetch (PRE_KIND))"
                                       ".select(PI).hmark(0@0);\n"
                    "if (pi_map.count() > 0) { #process processing-instructions \n"
                        "var pi_iter := pi_map.leftfetchjoin(iter);\n"
                        "var pi_item := pi_map.leftfetchjoin(item);\n"
                        "var pi_cont := pi_map.leftfetchjoin(cont);\n"
                        /* get the string values of the processing-instruction nodes */
                        "var pi_item_str := mposjoin(mposjoin(pi_item, pi_cont, ws.fetch(PRE_PROP)), "
                                                    "mposjoin(pi_item, pi_cont, ws.fetch(PRE_CONT)), "
                                                    "ws.fetch(PROP_INS));\n"
                        "pi_item := nil;\n"
                        "pi_cont := nil;\n"
                        /* merge strings from element and attribute */
                        "var res_mu := merged_union (t_iter, pi_iter, t_item_str, pi_item_str);\n"
                        "t_iter := res_mu.fetch(0);\n"
                        "t_item_str := res_mu.fetch(1);\n"
                    "} # end of processing-instruction processing\n"
                    "iter := t_iter;\n"
                    "item_str := t_item_str;\n");
    milprintf(f,
                "}\n"
            "}\n");

    if (code)
    {
        milprintf(f, "item%s := item_str;\n", item_ext);
    }
    else
    {
        addValues (f, str_container(), "item_str", "item");
    }
    milprintf(f,
            "item_str := nil;\n"
            /* adds empty strings if an element had no string content */
            "if (iter.count() != input_iter.tunique().count())\n"
            "{\n"
            "var difference := reverse(input_iter.tdiff(iter));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter, difference, item%s, %s);\n"
            "iter := res_mu.fetch(0);\n"
            "item%s := res_mu.fetch(1);\n"
            "}\n"
            "input_iter := nil;\n"
            "ipik := iter;\n"
            "pos := tmark_grp_unique(iter,ipik);\n"
            "kind := %s;\n"
            "} # end of string-value\n",
            item_ext,
            empty_string,
            item_ext,
            kind);
}

/**
 * the loop lifted version of fn_data searches only for 
 * values, which are not atomic and calles string-value
 * for them and combines the both sort of types in the end
 *
 * @param f the Stream the MIL code is printed to
 */
static void
fn_data (opt_t *f)
{
    milprintf(f,
            /* split atomic from node types */
            "kind := kind.materialize(ipik);\n"
            "var atomic := kind.get_type_atomic();\n"
            "atomic := atomic.hmark(0@0);\n"
            "var iter_atomic := atomic.leftfetchjoin(iter);\n"
            "var pos_atomic := atomic.leftfetchjoin(pos);\n"
            "var item_atomic := atomic.leftfetchjoin(item);\n"
            "var kind_atomic := atomic.leftfetchjoin(kind);\n"

            "var node := kind.get_type_node();\n"
            "node := node.hmark(0@0);\n"
            "var iter_node := node.leftfetchjoin(iter);\n"
            "iter := node.mirror();\n"
            "ipik := iter;\n"
            "pos := node.leftfetchjoin(pos);\n"
            "item := node.leftfetchjoin(item);\n"
            "kind := node.leftfetchjoin(kind);\n");
    /* we are assuming that nothing is validated
       and we can use string-value */
    string_value (f, NORMAL, "U_A");
    milprintf(f,
            /* every input row of typed-value gives back exactly
               one output row - therefore a mapping is not necessary */
            "var res_mu := merged_union (node, atomic, iter_node, iter_atomic, item, item_atomic, kind, kind_atomic);\n"
            "node := nil;\n"
            "atomic := nil;\n"
            "iter_node := nil;\n"
            "iter_atomic := nil;\n"
            "item := nil;\n"
            "item_atomic := nil;\n"
            "kind := nil;\n"
            "kind_atomic := nil;\n"
            "iter := res_mu.fetch(1);\n"
            "item := res_mu.fetch(2);\n"
            "kind := res_mu.fetch(3);\n"
            "res_mu := nil;\n"
            "pos := tmark_grp_unique(iter,ipik);\n"
            "ipik := iter;\n"
            );
}

/**
 * is2ns is the translation of the item-sequence-to-node-sequence
 * function. It does basically four steps. The first step is to
 * get all text-nodes and every other nodes, as well as all other
 * atomic nodes. The second step calls 'translateCast2STR' for
 * the atomic nodes. The third step is to generate new text-nodes
 * where all the rules for building the content of an element
 * are applied within a function 'combine-text-string'. The fourth
 * step is to create text-nodes out of it and combine the other
 * nodes and the attributes with them.
 *
 * @param f the Stream the MIL code is printed to
 * @param counter the actual offset of saved variables
 * @param input_type the type of the input expression
 */
static void
is2ns (opt_t *f, int counter, PFty_t input_type)
{
    counter++;
    saveResult (f, counter);
    milprintf(f,
            "{ # item-sequence-to-node-sequence\n"
            "var nodes_order;\n"
            "{\n"
            "ipik := ipik%03u;\n"
            "iter := iter%03u;\n"
            "pos := pos%03u;\n"
            "item := item%03u;\n"
            "kind := kind%03u;\n"
	/* we have to split the attribute nodes from all other nodes */

            /* get all text-nodes */
            "kind := kind.materialize(ipik);\n"
            "var elem := kind.get_type(ELEM);\n"
            "elem := elem.hmark(0@0);\n"
            "var kind_elem := elem.leftfetchjoin(kind);\n"
            "var cont_elem := kind_elem.get_container();\n"
            "kind_elem := nil;\n"
            "var item_elem := elem.leftfetchjoin(item).materialize(elem);\n"
            "var kind_node := mposjoin (item_elem, cont_elem, ws.fetch(PRE_KIND));\n"
            "var text := kind_node.ord_uselect(TEXT).hmark(0@0);\n"
            "var item_text := text.leftfetchjoin(item_elem);\n"
            "var cont_text := text.leftfetchjoin(cont_elem);\n"
            "item_elem := nil;\n"
            "cont_elem := nil;\n"
            "var text_str := mposjoin (mposjoin (item_text, cont_text, ws.fetch(PRE_PROP)), "
                                      "mposjoin (item_text, cont_text, ws.fetch(PRE_CONT)), "
                                      "ws.fetch(PROP_TEXT));\n"
            "item_text := nil;\n"
            "cont_text := nil;\n"
            "var str_text := text_str.reverse().leftfetchjoin(text);\n"
            "text_str := nil;\n"
            "text := nil;\n"
            "var texts := str_text.leftfetchjoin(elem).reverse();\n"
            "str_text := nil;\n"
            "var texts_order := texts.hmark(0@0);\n"
            "texts := texts.tmark(0@0);\n"
            /* 1@0 is text node constant for combine_text_string */

            /* get all other nodes and create empty strings for them */
            "var nodes := kind_node.[!=](TEXT).ord_uselect(true).project(\"\");\n"
            "kind_node := nil;\n"
            "nodes := nodes.reverse().leftfetchjoin(elem).reverse();\n"
            "elem := nil;\n"
            "nodes_order := nodes.hmark(0@0);\n"
            "nodes := nodes.tmark(0@0);\n"
            /* 0@0 is node constant for combine_text_string */
            "var res_mu_is2ns := merged_union (nodes_order, texts_order, nodes, texts, 0@0, 1@0);\n"
            "nodes := nil;\n"
            "texts_order := nil;\n"
            "texts := nil;\n"
            "var input_order := res_mu_is2ns.fetch(0);\n" /* CONST? */
            "var input_str := res_mu_is2ns.fetch(1);\n" /* CONST? */
            "var input_const := res_mu_is2ns.fetch(2);\n" /* CONST? */
            "res_mu_is2ns := nil;\n"

            /* get all the atomic values and cast them to string */
            "kind := kind.materialize(ipik);\n"
            "var atomic := kind.get_type_atomic();\n"
            "atomic := atomic.hmark(0@0);\n"
            "iter := atomic.mirror();\n"
            "ipik := iter;\n"
            "pos := atomic.leftfetchjoin(pos);\n"
            "item := atomic.leftfetchjoin(item);\n"
            "kind := atomic.leftfetchjoin(kind);\n",
            counter, counter, counter, counter, counter);
    translateCast2STR (f, STR, NORMAL, input_type);
    milprintf(f,
            "res_mu_is2ns := merged_union (input_order, atomic, input_str, item%s, "
                                          /* 2@0 is string constant for combine_text_string */
                                          "input_const, 2@0);\n"
            "atomic := nil;\n"
            "input_order := res_mu_is2ns.fetch(0);\n" /* CONST? */
            "input_str := res_mu_is2ns.fetch(1);\n" /* CONST? */
            "input_const := res_mu_is2ns.fetch(2);\n" /* CONST? */
            "res_mu_is2ns := nil;\n"
            "var input_iter := input_order.leftfetchjoin(iter%03u).chk_order();\n"
            "var result_size := iter%03u.tunique().count() + nodes_order.count() + 1;\n"
            /* doesn't believe, that iter as well as input_order are ordered on h & t */
            /* apply the rules for the content of element construction */
            "var result_str := combine_text_string "
                              "(input_iter.materialize(input_str), input_const.materialize(input_str), input_str, result_size);\n"
            "input_iter := nil;\n"
            "input_const := nil;\n"
            "input_str := nil;\n"
            "result_size := nil;\n"
            "var result_order := result_str.hmark(0@0);\n"
            "result_order := result_order.leftfetchjoin(input_order);\n"
            "result_str := result_str.tmark(0@0);\n",
            kind_str(STR), counter, counter);
    /* instead of adding the values first to string, then create new text nodes
       before making subtree copies in the element construction a new type text
       nodes could be created, which saves only the offset of the string in the
       text-node table and has a different handling in the element construction.
       At least some copying of strings could be avoided :) */
    milprintf(f,
            "iter := result_order;\n"
            "ipik := iter;\n"
            "pos := ipik.mark(1@0);\n"
            "item%s := result_str;\n"
            "kind := STR;\n"
            "}\n",
            kind_str(STR));
    loop_liftedTextConstr (f, NORMAL, STR); 
    milprintf(f,
            "var res_mu_is2ns := merged_union (iter, nodes_order, "
                                              "item, nodes_order.leftfetchjoin(item%03u), "
                                              "kind, nodes_order.leftfetchjoin(kind%03u));\n"
            "nodes_order := nil;\n"
            "kind%03u := kind%03u.materialize(ipik%03u);\n"
            "var attr := kind%03u.get_type(ATTR).hmark(0@0);\n"
            "var item_attr := attr.leftfetchjoin(item%03u);\n"
            "var kind_attr := attr.leftfetchjoin(kind%03u);\n"
            "res_mu_is2ns := merged_union (res_mu_is2ns.fetch(0), attr, "
                                          "res_mu_is2ns.fetch(1), item_attr, "
                                          "res_mu_is2ns.fetch(2), kind_attr);\n"
            "attr := nil;\n"
            "item_attr := nil;\n"
            "kind_attr := nil;\n"
            "iter := res_mu_is2ns.fetch(0).leftfetchjoin(iter%03u);\n"
            "item := res_mu_is2ns.fetch(1);\n"
            "kind := res_mu_is2ns.fetch(2);\n"
            "pos := tmark_grp_unique(iter,ipik);\n"
            "ipik := item;\n"
            "} # end of item-sequence-to-node-sequence\n",
            counter, counter, counter,
            counter, counter, counter, counter, counter, counter);
    deleteResult (f, counter);
}

/**
 * is2ns_node is the translation of the item-sequence-to-node-sequence
 * function using the node interface. In comparison to is2ns it only
 * has to replace the root text-nodes by new merged root text-nodes and
 * update the pre values in the elem and attr relations.
 *
 * @param f the Stream the MIL code is printed to
 * @param counter the actual offset of saved variables
 */
static void
is2ns_node (opt_t *f, int counter)
{
    milprintf(f,
            "if (_elem_iter.count() != _elem_iter.tunique().count())\n"
            "{ # item-sequence-to-node-sequence\n");
    counter++;
    saveResult_node (f, counter);
    milprintf(f,
            "_elem_iter := _elem_iter%03u;\n"
            "_elem_size := _elem_size%03u;\n"
            "_elem_level := _elem_level%03u;\n"
            "_elem_kind := _elem_kind%03u;\n"
            "_elem_prop := _elem_prop%03u;\n"
            "_elem_cont := _elem_cont%03u;\n"
            /* select text root nodes */
            "var rootnodes := _elem_level.ord_uselect(chr(0)).mirror();\n"
            "rootnodes := rootnodes.leftfetchjoin(_elem_kind);\n"
            "var textnodes := rootnodes.ord_uselect(TEXT).hmark(0@0);\n"
            "var othernodes := _elem_level.kdiff(textnodes.reverse()).hmark(0@0);\n"
            "var elem_nodes := rootnodes.[!=](TEXT).ord_uselect(true).hmark(0@0);\n"
            "rootnodes := nil;\n"
            "{\n"

            "var text_prop := textnodes.leftfetchjoin(_elem_prop);\n"
            "var text_cont := textnodes.leftfetchjoin(_elem_cont);\n"
            "var text_str := mposjoin (text_prop, text_cont, ws.fetch(PROP_TEXT));\n"
            "text_prop := nil;\n"
            "text_cont := nil;\n"

            "var res_mu_is2ns := merged_union (elem_nodes, textnodes, \"\", text_str, 0@0, 1@0);\n"

            "textnodes := nil;\n"
            "text_str := nil;\n"
            "var input_order := res_mu_is2ns.fetch(0);\n" /* CONST? */
            "var input_str := res_mu_is2ns.fetch(1);\n" /* CONST? */
            "var input_const := res_mu_is2ns.fetch(2);\n" /* CONST? */
            "res_mu_is2ns := nil;\n"
            "var input_iter := input_order.leftfetchjoin(_elem_iter).chk_order();\n"
            "var result_size := _elem_iter.tunique().count() + elem_nodes.count() + 1;\n"
            "elem_nodes := nil;\n"
            /* doesn't believe, that iter as well as input_order are ordered on h & t */
            /* apply the rules for the content of element construction */
            "var result_str := combine_text_string "
                              "(input_iter, input_const, input_str, result_size);\n"
            "input_iter := nil;\n"
            "input_const := nil;\n"
            "input_str := nil;\n"
            "result_size := nil;\n"
            "var result_order := result_str.hmark(0@0);\n"
            "result_order := result_order.leftfetchjoin(input_order);\n"
            "result_str := result_str.tmark(0@0);\n"
    /* instead of adding the values first to string, then create new text nodes
       before making subtree copies in the element construction a new type text
       nodes could be created, which saves only the offset of the string in the
       text-node table and has a different handling in the element construction.
       At least some copying of strings could be avoided :) */
            "iter := result_order;\n"
            "ipik := iter;\n"
            "pos := ipik.mark(1@0);\n"
            "item%s := result_str;\n"
            "kind := STR;\n"
            "}\n",
            counter, counter, counter, counter, counter, counter,
            kind_str(STR));
    loop_liftedTextConstr (f, NODE, STR); 
    milprintf(f,
            "var res_mu_is2ns := merged_union ("
            "othernodes, _elem_iter,\n"
            "othernodes.leftfetchjoin(_elem_iter%03u), "
                                     "_elem_iter.leftfetchjoin(_elem_iter%03u),\n"
            "othernodes.leftfetchjoin(_elem_size%03u), "
                                     "_elem_size,\n"
            "othernodes.leftfetchjoin(_elem_level%03u), "
                                     "_elem_level,\n"
            "othernodes.leftfetchjoin(_elem_kind%03u), "
                                     "_elem_kind,\n"
            "othernodes.leftfetchjoin(_elem_prop%03u), "
                                     "_elem_prop,\n"
            "othernodes.leftfetchjoin(_elem_cont%03u), "
                                     "_elem_cont,\n"
            "othernodes.leftfetchjoin(_elem_iter%03u.mirror()), "
                                     "oid_nil);\n"
            "_elem_iter := res_mu_is2ns.fetch(1).chk_order();\n" /* CONST? */
            "_elem_size := res_mu_is2ns.fetch(2);\n" /* CONST? */
            "_elem_level:= res_mu_is2ns.fetch(3);\n" /* CONST? */
            "_elem_kind := res_mu_is2ns.fetch(4);\n" /* CONST? */
            "_elem_prop := res_mu_is2ns.fetch(5);\n" /* CONST? */
            "_elem_cont := res_mu_is2ns.fetch(6);\n" /* CONST? */
            "var preNew_preOld := res_mu_is2ns.fetch(7);\n" /* CONST? */
            /* update pre numbers */
            "_attr_own := _attr_own%03u.leftjoin(preNew_preOld.reverse());\n"
            "othernodes := nil;\n"
            "_attr_iter   := _attr_iter%03u  ;\n"
            "_attr_qn     := _attr_qn%03u    ;\n"
            "_attr_prop   := _attr_prop%03u  ;\n"
            "_attr_cont   := _attr_cont%03u  ;\n"
            "_r_attr_iter := _r_attr_iter%03u;\n"
            "_r_attr_qn   := _r_attr_qn%03u  ;\n"
            "_r_attr_prop := _r_attr_prop%03u;\n"
            "_r_attr_cont := _r_attr_cont%03u;\n",
            counter, counter, counter, counter,
            counter, counter, counter, counter,
            counter,
            counter, counter, counter, counter,
            counter, counter, counter, counter);

    deleteResult_node (f, counter);
    milprintf(f,
            "} # end of item-sequence-to-node-sequence\n");
}

/**
 * fn_string translates the built-in function 
 * fn:string (item*) as string 
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the Core expression which is translated next
 */
static int
fn_string(opt_t *f, int code, int cur_level, int counter, PFcnode_t *c)
{
    int rcode, rc;
    PFty_t input_type = *PFty_simplify(PFty_defn(TY(c)));

    rcode = (code)?STR:NORMAL;

    if (PFty_subtype (PFty_node(),input_type))
    {
        translate2MIL (f, NORMAL, cur_level, counter, c);
        string_value (f, code, "STR");
        add_empty_strings (f, rcode, cur_level);
        milprintf(f,
                "iter := loop%03u.tmark(0@0);\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := STR;\n",
                cur_level);

        return rcode;
    }
    /* handle mixed data */
    else if (!PFty_subtype (PFty_atomic(), input_type))
    {
        translate2MIL (f, NORMAL, cur_level, counter, c);
        fn_data (f);
        rc = NORMAL;
    }
    else
    {
        rc = translate2MIL (f, code, cur_level, counter, c);
    }

    translateCast2STR (f, rcode, rc, PFty_prime (input_type));
    add_empty_strings (f, rcode, cur_level);
    milprintf(f,
        "iter := loop%03u.tmark(0@0);\n"
        "ipik := iter;\n"
        "pos := 1@0;\n"
        "kind := STR;\n",
        cur_level);

    return rcode;
}

/**
 * translateAggregates translates fn:avg, fn:max, fn:min, and fn:sum
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param rc the integer indicating which kind the input has used
 * @param fun the function information
 * @param args the head of the argument list
 * @param args op the name of the aggregate function
 * @result the kind indicating, which result interface is chosen
 */
static int
translateAggregates (opt_t *f, int code, int rc,
                     PFfun_t *fun, PFcnode_t *args, char *op)
{
    assert (fun->sig_count == 1);
    int ic = get_kind(PFty_prime(PFty_defn(TY(L(args)))));
    int rcode = get_kind(fun->sigs[0].ret_ty);
    char *item_ext = (code)?kind_str(rcode):"";
    type_co t_co = kind_container(rcode);

    milprintf(f,
            "if (ipik.count() != 0) { # fn:%s\n"
            "var iter_grp  := iter.materialize(ipik);\n"
            "iter := iter_grp.tunique();\n"
            "var iter_aggr := {%s}(item%s, iter_grp, iter).tmark(0@0);\n"
            "iter := iter.hmark(0@0);\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := %s;\n",
            op, 
            op, (rc)?kind_str(rc):val_join(ic),
            t_co.mil_cast);

    if (code)
        milprintf(f, "item%s := iter_aggr;\n", item_ext);
    else 
        addValues (f, t_co, "iter_aggr", "item");

    milprintf(f,
            "} else {\n"
            "item%s := empty%s_bat;\n"
            "} # end of fn:%s\n",
            item_ext, item_ext,
            op);

    return (code)?rcode:NORMAL;
}

/**
 * fn_abs translates the builtin functions fn:abs, fn:ceiling, fn:floor,
 * and fn:round
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param rc the integer indicating which kind the input has used
 * @param op the operation, which has to be performed
 */
static void
fn_abs (opt_t *f, int code, int rc, char *op)
{
    char *item_ext = kind_str(rc);
    /* because functions are only allowed for dbl
       we need to cast integers */
    char *cast_dbl = (rc == INT)?".[dbl]()":"";
    char *cast_int = (rc == INT)?".[lng]()":"";
    type_co t_co = kind_container(rc);

    milprintf(f,
            "if (ipik.count() != 0) { # fn:%s\n"
            "var res := item%s%s.[%s]()%s;\n",
            op,
            item_ext, cast_dbl, op, cast_int);

    if (code)
        milprintf(f, "item%s := res;\n", item_ext);
    else 
        addValues (f, t_co, "res", "item");

    item_ext = (code)?item_ext:"";
    milprintf(f, "} # end of fn:%s\n", op);

}

/**
 * translateIntersect translates intersect and except
 *
 * @param f the Stream the MIL code is printed to
 * @param op the string indicating which operation has to 
 *        be evaluated
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the Core expression which is translated next
 */
static void
translateIntersect (opt_t *f, char *op, int cur_level, int counter, PFcnode_t *c)
{
    translate2MIL (f, NORMAL, cur_level, counter, L(c));
    counter++;
    saveResult (f, counter);
    translate2MIL (f, NORMAL, cur_level, counter, RL(c));

    /* we have to handle the special cases because
       otherwise max/min gets problems with empty bats */
    if (!strcmp(op,"intersect"))
    {
        milprintf(f, 
                "if (or (ipik.count() = 0,\n"
                "        ipik%03u.count() = 0)) {\n",
                counter);
        translateEmpty(f);
    }
    else
    {
        milprintf(f, "if (ipik%03u.count() = 0) {\n", counter);
        translateEmpty(f);
        milprintf(f, 
                "} else { if (ipik.count() = 0) {\n"
                "var sorting := iter%03u.tsort();\n"
                "sorting := sorting.CTrefine(kind%03u);\n"
                "sorting := sorting.CTrefine(item%03u);\n"
                "ipik := tmark_unique(sorting,ipik);\n"
                "sorting := nil;\n"
                "iter := ipik.leftfetchjoin(iter%03u);\n"
                "pos := tmark_grp_unique(iter,ipik);\n"
                "item := ipik.leftfetchjoin(item%03u);\n"
                "kind := ipik.leftfetchjoin(kind%03u);\n",
                counter, counter, counter, counter, counter, counter);
    }

    milprintf(f,
            "} else { # %s\n"
            "var min := min (kind%03u.min(), kind.min());\n"
            "var max := max (kind%03u.max(), kind.max());\n"
            "var diff := max - min;\n"
            "var list1 := iter%03u.[lng]();\n"
            "iter := iter.materialize(ipik);\n"
            "var list2 := iter.[lng]();\n"
            "if (diff > 0)\n"
            "{\n"
            /* since max and min are oids - the lowest number can be 1.
               But log(1) returns 0 - The solution is adding 0.1, which 
               is small enough to avoid shifting a bit too much */
            "    var shift := int(log(dbl(max) + 0.1LL)/log(2.0)) + 1;\n"
            "    list1 := list1.[<<](shift).[or](kind%03u.[-](min).[lng]());\n"
            "    list2 := list2.[<<](shift).[or](kind.[-](min).[lng]());\n"
            "}\n"
            "min := int (min (item%03u.min(), item.min()));\n"
            "max := int (max (item%03u.max(), item.max()));\n"
            "diff := max - min;\n"
            "if (diff > 0)\n"
            "{\n"
            /* since max and min are oids - the lowest number can be 1.
               But log(1) returns 0 - The solution is adding 0.1, which 
               is small enough to avoid shifting a bit too much */
            "    var shift := int(log(dbl(max) + 0.1LL)/log(2.0)) + 1;\n"
            "    list1 := list1.[<<](shift).[or](item%03u.[lng]().[-](min));\n"
            "    list2 := list2.[<<](shift).[or](item.[lng]().[-](min));\n"
            "}\n"
            "ipik := list1.reverse()"
                               ".k%s(list2.reverse())"
                               ".kunique()"
                               ".sort()"
                               ".tmark(0@0);\n"
            "iter := ipik.leftfetchjoin(iter%03u);\n"
            "pos := tmark_grp_unique(iter,ipik);\n"
            "item := ipik.leftfetchjoin(item%03u);\n"
            "kind := ipik.leftfetchjoin(kind%03u);\n"
            "} # end of %s\n",
            op, 
            counter, counter, counter, counter, counter, counter, counter,
            op,
            counter,
            counter,
            counter,
            op);

    if (strcmp(op,"intersect"))
    {
        milprintf(f, "}\n");
    } 
    deleteResult (f, counter);
}

/**
 * fn_id translates fn:id and fn:idref
 *
 * @param f the Stream the MIL code is printed to
 * @param op the string indicating which operation has to 
 *        be evaluated
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the Core expression which is translated next
 */
static void
fn_id (opt_t *f, char *op, int cur_level, int counter, PFcnode_t *c)
{
    char *item_ext = kind_str(STR);
    char *idref = (op)?"REF":"";
    op = (op)?"ref":"";
    int rc;

    rc  = translate2MIL (f, VALUES, cur_level, counter, L(c));
    if (rc == NORMAL)
        milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);

    counter++;
    saveResult_ (f, counter, STR);
    translate2MIL (f, NORMAL, cur_level, counter, RL(c));

    milprintf(f,
            "if (ipik.count() != 0) { # fn:id%s\n"
            "  iter%03u := iter%03u.materialize(ipik%03u);\n"
            "  var map := iter%03u.leftjoin(iter.materialize(ipik).reverse()).tmark(0@0);\n"
            "  item := map.leftfetchjoin(item);\n"
            "  kind := map.leftfetchjoin(kind);\n"
            "  var cont := kind.get_container();\n"
            "  map := ws_findnodes(ws, ID%s_NID, iter%03u, item, kind, cont, item%s%03u);\n"
            "  ipik := map.tmark(0@0);\n"
            "  map := map.hmark(0@0);\n"
            "  item := ipik;\n"
            "  iter := map.leftfetchjoin(iter%03u);\n"
            "  kind := map.leftfetchjoin(cont.set_kind(ELEM));\n"
            "  pos := tmark_grp_unique(iter, ipik);\n"
            "} else {\n",
               op, counter, counter, counter, counter, idref, counter, item_ext, counter, counter);
    translateEmpty (f);
    milprintf(f,
            "} # end of fn:id%s\n",
            op);

    deleteResult_ (f, counter, STR);
}

/**
 * prep_str_funs evaluates two function arguments, which have the return
 * type string?. They are prepared by retrieving the values (if not already
 * present) and filling the gaps (empty sequence) with empty strings.
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the function argument list
 * @return the updated counter value
 */
static int
prep_str_funs (opt_t *f, int cur_level, int counter, PFcnode_t *c)
{
    char *item_ext = kind_str(STR);
    int rc = translate2MIL (f, VALUES, cur_level, counter, L(c));
    if (rc == NORMAL)
        milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
    add_empty_strings (f, STR, cur_level);

    counter++;
    saveResult_ (f, counter, STR);
    rc = translate2MIL (f, VALUES, cur_level, counter, RL(c));
    if (rc == NORMAL)
        milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
    add_empty_strings (f, STR, cur_level);

    return counter;
}

/**
 * return_str_funs prepares the result for a function,
 * which returns a result of the type string. It uses the
 * given interface (code) and relies on a variable 'res'.
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param fn_name the name of the function, which generates this code
 */
static void
return_str_funs (opt_t *f, int code, int cur_level, char *fn_name)
{
    char *item_ext = kind_str(STR);
    if (code)
        milprintf(f, "item%s := res;\n", item_ext);
    else 
        addValues (f, str_container(), "res", "item");

    item_ext = (code)?item_ext:"";
    milprintf(f,
            "res := nil;\n"
            "iter := loop%03u;\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := STR;\n"
            "} # end of %s\n",
            cur_level,
            fn_name);
}

/**
 * fn_starts_with translates the builtin functions fn:starts-with 
 * and fn:ends-with
 *
 * @param f the Stream the MIL code is printed to
 * @param op the operation, which has to be performed
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the Core node containing the rest of the subtree
 */
static void
fn_starts_with (opt_t *f, char *op, int cur_level, int counter, PFcnode_t *c)
{
    char *item_ext = kind_str(STR);
    counter = prep_str_funs (f, cur_level, counter, c);

    milprintf(f,
            "{ # fn:%s-with\n"
            "var strings := item%s%03u;\n"
            "var search_str := item%s;\n"
            "item := [%sWith](strings, search_str).[oid]();\n"
            "iter := loop%03u;\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := BOOL;\n"
            "} # end of fn:%s-with\n",
            op, 
            item_ext, counter,
            item_ext,
            op,
            cur_level,
            op);

    deleteResult_ (f, counter, STR);
}

/**
 * fn_matches the builtin functions fn:matches
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param fun the function information
 * @param args the head of the function argument list
 */
static int
fn_matches (opt_t *f, int cur_level, int counter, 
            PFfun_t *fun, PFcnode_t *args)
{
    char* item_ext = kind_str(STR);
    int hasFlags = fun->arity == 3 ? 1 : 0;
    PFcnode_t *flags_node;

    /* translate 'input'-s */
    if (translate2MIL(f, VALUES, cur_level, counter, L(args)) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }

    counter++;
    saveResult_ (f, counter, STR);

    /* translate 'pattern'-s */
    if (translate2MIL(f, VALUES, cur_level, counter, RL(args)) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }

    counter++;
    saveResult_ (f, counter, STR);

    /* translate 'flags'-s */
    if (hasFlags)
        flags_node = RRL(args);
    else 
        flags_node = PFcore_str("");
    if (translate2MIL(f, VALUES, cur_level, counter, flags_node) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }

    milprintf(f,
            "{ # fn:matches (string?, string%s) as boolean\n"
            "var inputs;\n"
            "if (ipik%03u.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter%03u));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter%03u.chk_order(), difference, item%s%03u, \"\");\n"
            "inputs := res_mu.fetch(1);\n"
            "} else {\n"
            "inputs := item%s%03u.materialize(ipik%03u);\n"
            "}\n",
            hasFlags ? ", string":"",
            counter-1, cur_level, 
            cur_level, counter-1,
            counter-1, item_ext, counter-1,
            item_ext, counter-1, counter-1);
    milprintf(f,
            "var patterns;\n"
            "if (ipik%03u.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter%03u));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter%03u.chk_order(), difference, item%s%03u, \"\");\n"
            "patterns := res_mu.fetch(1);\n"
            "} else {\n"
            "patterns := item%s%03u.materialize(ipik%03u);\n"
            "}\n"
            "# replace empty sequences with empty strings\n"
            "inputs := outerjoin(patterns.mirror(), inputs);\n"
            "inputs := [ifthenelse]([isnil](inputs), \"\", inputs);\n",
            counter, cur_level, 
            cur_level, counter,
            counter, item_ext, counter,
            item_ext, counter, counter);
    milprintf(f,
            "var flagss;\n"
            "if (ipik.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter, difference, item%s, \"\");\n"
            "flagss := res_mu.fetch(1);\n"
            "} else {\n"
            "flagss := item%s;\n"
            "}\n"
            "var chk;\n"
            "var chkPtn := \"[^imsx]\";\n"
            "var pcre_err := CATCH({ chk := [pcre_match](flagss, const chkPtn); });\n"
            "if(not(isnil(pcre_err))) {\n"
            "   ERROR(\"Should not happen: error occurred while checking flags: \" + pcre_err);\n"
            "}\n"
            "if(chk.texist(true)){\n"
            "    ERROR(\"err:FORX0001: flags of fn:matches containing undefined character(s)\");\n"
            "}\n"
            "chk := nil;\n"
            "chkPtn := nil;\n"
            "pcre_err := nil;\n",
            cur_level,
            cur_level,
            item_ext,
            item_ext);
    milprintf(f,
            "item := [pcre_match](inputs,patterns%s).[oid]();\n"
            "iter := loop%03u.tmark(0@0);\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := BOOL;\n"
            "inputs := nil;\n"
            "patterns := nil;\n"
            "flagss := nil;\n"
            "} # end of fn:matches (string?, string%s) as boolean\n",
            hasFlags ? ", flagss": "",
            cur_level,
            hasFlags ? ", string":"");

    deleteResult_ (f, counter, STR);
    deleteResult_ (f, counter-1, STR);
    return NORMAL;
}

/**
 * fn_replace the builtin functions fn:replace
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param fun the function information
 * @param args the head of the function argument list
 */
static int
fn_replace (opt_t *f, int code, int cur_level, int counter, 
            PFfun_t *fun, PFcnode_t *args)
{
    char* item_ext = kind_str(STR);
    int hasFlags = fun->arity == 4 ? 1 : 0;
    PFcnode_t *flags_node;

    /* translate 'input'-s */
    if (translate2MIL (f, VALUES, cur_level, counter, L(args)) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }
    counter++;
    saveResult_ (f, counter, STR);

    /* translate 'pattern'-s */
    if (translate2MIL (f, VALUES, cur_level, counter, RL(args)) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }
    counter++;
    saveResult_ (f, counter, STR);

    /* translate 'replacement'-s */
    if (translate2MIL (f, VALUES, cur_level, counter, RRL(args)) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }
    counter++;
    saveResult_ (f, counter, STR);

    /* translate 'flags'-s */
    if (hasFlags)
        flags_node = RRRL(args);
    else
        flags_node = PFcore_str("");
    if (translate2MIL (f, VALUES, cur_level, counter, flags_node) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }

    milprintf(f,
            "{ # fn:replace (string?, string, string%s) as string\n"
            "var inputs;\n"
            "if (ipik%03u.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter%03u));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter%03u.chk_order(), difference, item%s%03u, \"\");\n"
            "inputs := res_mu.fetch(1);\n"
            "} else {\n"
            "inputs := item%s%03u.materialize(ipik%03u);\n"
            "}\n",
            hasFlags ? ", string" : "",
            counter-2, cur_level, 
            cur_level, counter-2,
            counter-2, item_ext, counter-2,
            item_ext, counter-2, counter-2);
    milprintf(f,
            "var patterns;\n"
            "if (ipik%03u.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter%03u));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter%03u.chk_order(), difference, item%s%03u, \"\");\n"
            "patterns := res_mu.fetch(1);\n"
            "} else {\n"
            "patterns := item%s%03u.materialize(ipik%03u);\n"
            "}\n"
            "# replace empty sequences with empty strings\n"
            "inputs := outerjoin(patterns.mirror(), inputs);\n"
            "inputs := [ifthenelse]([isnil](inputs), \"\", inputs);\n",
            counter-1, cur_level, 
            cur_level, counter-1,
            counter-1, item_ext, counter-1,
            item_ext, counter-1, counter-1);
    milprintf(f,
            "var replacements;\n"
            "if (ipik%03u.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter%03u));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter%03u.chk_order(), difference, item%s%03u, \"\");\n"
            "replacements := res_mu.fetch(1);\n"
            "} else {\n"
            "replacements := item%s%03u.materialize(ipik%03u);\n"
            "}\n"

            "var chk;\n"
            "var chkPtn := \"[$][0-9]\";\n"
            "var pcre_err := CATCH({ chk := [pcre_match](replacements, const chkPtn); });\n"
            "if(not(isnil(pcre_err))) {\n"
            "   ERROR(\"Should not happen: error occurred while checking replacements: \" + pcre_err);\n"
            "}\n"
            "if(chk.texist(true)){\n"
            "    ERROR(\"Variables in replacements are not supported yet\");\n"
            "}\n"
            "chk := nil;\n",
            counter, cur_level, 
            cur_level, counter,
            counter, item_ext, counter,
            item_ext, counter, counter);
    milprintf(f,
            "var flagss;\n"
            "if (ipik.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter, difference, item%s, \"\");\n"
            "flagss := res_mu.fetch(1);\n"
            "} else {\n"
            "flagss := item%s;\n"
            "}\n"
            "chkPtn := \"[^imsx]\";\n"
            "pcre_err := CATCH({ chk := [pcre_match](flagss, const chkPtn); });\n"
            "if(not(isnil(pcre_err))) {\n"
            "   ERROR(\"Should not happen: error occurred while checking flags: \" + pcre_err);\n"
            "}\n"
            "if(chk.texist(true)){\n"
            "    ERROR(\"err:FORX0001: flags of fn:replace containing undefined character(s)\");\n"
            "}\n"
            "chk := nil;\n"
            "chkPtn := nil;\n"
            "pcre_err := nil;\n",
            cur_level,
            cur_level,
            item_ext,
            item_ext);

    milprintf(f,
            "var res := [pcre_replace](inputs,patterns,replacements,flagss);\n"
            "iter := loop%03u.tmark(0@0);\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := STR;\n"
            "inputs := nil;\n"
            "patterns := nil;\n"
            "replacements := nil;\n"
            "flagss := nil;\n",
            cur_level);

    char buf[128];
    snprintf(buf, sizeof(buf),
            "fn:replace (string?, string, string%s) as string",
            hasFlags?", string":"");

    return_str_funs (f, code, cur_level, buf);
    deleteResult_ (f, counter, STR);
    deleteResult_ (f, counter-1, STR);
    deleteResult_ (f, counter-2, STR);
    return code?STR:NORMAL;
}

/**
 * fn_translate translates the builtin functions fn:translate
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param args the head of the function argument list
 */
static int
fn_translate (opt_t *f, int code, int cur_level, int counter, 
              PFcnode_t *args)
{
    char* item_ext = kind_str(STR);

    /* translate 'arg'-s */
    if (translate2MIL (f, VALUES, cur_level, counter, L(args)) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }
    counter++;
    saveResult_ (f, counter, STR);

    /* translate 'mapString'-s */
    if (translate2MIL (f, VALUES, cur_level, counter, RL(args)) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }
    counter++;
    saveResult_ (f, counter, STR);

    /* translate 'transString'-s */
    if (translate2MIL (f, VALUES, cur_level, counter, RRL(args)) == NORMAL)
    {
        milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
    }

    milprintf(f,
            "{ # fn:translate (string?, string, string) as string\n"
            "var args;\n"
            "if (ipik%03u.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter%03u));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter%03u.chk_order(), difference, item%s%03u, \"\");\n"
            "args := res_mu.fetch(1);\n"
            "} else {\n"
            "args := item%s%03u.materialize(ipik%03u);\n"
            "}\n",
            counter-1, cur_level, 
            cur_level, counter-1,
            counter-1, item_ext, counter-1,
            item_ext, counter-1, counter-1);
    milprintf(f,
            "var mapStrings;\n"
            "if (ipik%03u.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter%03u));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter%03u.chk_order(), difference, item%s%03u, \"\");\n"
            "mapStrings := res_mu.fetch(1);\n"
            "} else {\n"
            "mapStrings := item%s%03u.materialize(ipik%03u);\n"
            "}\n"
            "# replace empty sequences with empty strings\n"
            "args := outerjoin(mapStrings.mirror(), args);\n"
            "args := [ifthenelse]([isnil](args), \"\", args);\n",
            counter, cur_level, 
            cur_level, counter,
            counter, item_ext, counter,
            item_ext, counter, counter);
    milprintf(f,
            "var transStrings;\n"
            "if (ipik.count() != loop%03u.count())\n"
            "{\n"
            "var difference := reverse(loop%03u.tdiff(iter));\n"
            "difference := difference.hmark(0@0);\n"
            "var res_mu := merged_union(iter.chk_order(), difference, item%s, \"\");\n"
            "transStrings := res_mu.fetch(1);\n"
            "} else {\n"
            "transStrings := item%s;\n"
            "}\n",
            cur_level, 
            cur_level,
            item_ext,
            item_ext);

    milprintf(f,
            "var res := [translate](args,mapStrings,transStrings);\n"
            "iter := loop%03u.tmark(0@0);\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := STR;\n"
            "args := nil;\n"
            "mapStrings:= nil;\n"
            "transStrings := nil;\n",
            cur_level);

    return_str_funs (f, code, cur_level, 
            "fn:translate (string?, string, string) as string");
    deleteResult_ (f, counter, STR);
    deleteResult_ (f, counter-1, STR);
    return code?STR:NORMAL;
}

/**
 * fn_substring translates the builtin function fn:substring 
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param fun the function information
 * @param c the head of the function argument list
 */
static void
fn_substring (opt_t *f, int code, int cur_level, int counter, 
              PFfun_t *fun, PFcnode_t *c)
{
    char *item_ext, *dbl_ext;
    int str_counter, rc;

    item_ext = kind_str(STR);
    dbl_ext = kind_str(DBL);

    rc = translate2MIL (f, VALUES, cur_level, counter, L(c));
    if (rc == NORMAL)
        milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
    add_empty_strings (f, STR, cur_level);
    counter++;
    str_counter = counter;
    saveResult_ (f, str_counter, STR);

    rc = translate2MIL (f, VALUES, cur_level, counter, RL(c));
    if (rc == NORMAL)
        milprintf(f, "item%s := item.leftfetchjoin(dbl_values);\n", dbl_ext);
    counter++;
    saveResult_ (f, counter, DBL);

    /* tests wether there exists a substring lenght information */
    if (fun->arity == 3)
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, RRL(c));
        if (rc == NORMAL)
            milprintf(f, "item%s := item.leftfetchjoin(dbl_values);\n", dbl_ext);
        milprintf(f,
                "{ # fn:substring\n"
                /* calculates the offset of the start characters */
                "var start := item%s%03u.[round_up]().[int]().[-](1);\n"
                /* calculates the offset of the end characters */
                "var end := start.[+](item%s.[round_up]().[int]());\n"
                /* start from an positive character */
                "start := start.[max](0);\n"
                /* replaces the end offset by the length of the substring */
                "end := end.[-](start);\n"
                /* calculates the substring */
                "var res := [string](item%s%03u, start, end);\n"
                "start := nil;\n"
                "end := nil;\n",
                dbl_ext, counter, dbl_ext, item_ext, str_counter);
    }
    else
    {
        milprintf(f,
                "{ # fn:substring\n"
                "var start := item%s%03u.[round_up]().[int]();\n"
                "var res := [substring](item%s%03u, start);\n",
                dbl_ext, counter, item_ext, str_counter);
    }

    if (code)
        milprintf(f, "item%s := res;\n", item_ext);
    else
        addValues (f, str_container(), "res", "item");

    item_ext = (code)?item_ext:"";
    milprintf(f,
            "iter := loop%03u;\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := STR;\n"
            "} # end of fn:substring\n",
            cur_level);

    deleteResult_ (f, counter, DBL);
    deleteResult_ (f, str_counter, STR);
}

/**
 * fn_name translates the builtin functions fn:name, fn:local-name, 
 * and fn:namespace-uri
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the head of the function argument list
 * @param name the name of the built-in function
 * @result the kind indicating, which result interface is chosen
 */
static int
fn_name (opt_t *f, int code, int cur_level, int counter, PFcnode_t *c, char *name)
{
    char *item_ext = kind_str(STR);
    translate2MIL (f, NORMAL, cur_level, counter, L(c));
    milprintf(f,
            "{ # fn:%s\n"
            /* looks up the element nodes */
            "kind := kind.materialize(ipik);\n"
            "var map := kind.get_type(ELEM).hmark(0@0);\n"
            "var elem := map.leftfetchjoin(item);\n"
            "var elem_cont := map.leftfetchjoin(kind.get_container());\n"
            "var elem_oid := map;\n"
            "map := nil;\n"
            "var elem_kind := mposjoin(elem, elem_cont, ws.fetch(PRE_KIND));\n"
            "map := elem_kind.ord_uselect(ELEMENT).hmark(0@0);\n"
            "elem_kind := nil;\n"
            "elem := map.leftfetchjoin(elem);\n"
            "elem_cont := map.leftfetchjoin(elem_cont);\n"
            "elem_oid  := map.leftfetchjoin(elem_oid);\n"
            "map := nil;\n"
            /* gets the qname keys */
            "elem := mposjoin(elem, elem_cont, ws.fetch(PRE_PROP));\n"

            /* looks up the attribute nodes */
            "map := kind.get_type(ATTR).hmark(0@0);\n"
            "var attr := map.leftfetchjoin(item);\n"
            "var attr_cont := map.leftfetchjoin(kind.get_container());\n"
            "var attr_oid := map;\n"
            "map := nil;\n"
            /* gets the qname keys */
            "attr := mposjoin(attr, attr_cont, ws.fetch(ATTR_QN));\n"
            /* merges the qname keys of attributes and element nodes */
            "var res_mu := merged_union(elem_oid, attr_oid, elem, attr, elem_cont, attr_cont);\n"
            "elem := nil;\n"
            "elem_cont := nil;\n"
            "elem_oid := nil;\n"
            "attr := nil;\n"
            "attr_cont := nil;\n"
            "attr_oid := nil;\n"
            "var qname_oid  := res_mu.fetch(0);\n" /* CONST? */
            "var qname      := res_mu.fetch(1);\n" /* CONST? */
            "var qname_cont := res_mu.fetch(2);\n" /* CONST? */
            "res_mu := nil;\n",
            name);

    if (!strcmp(name,"local-name"))
        milprintf(f,
                "var res := mposjoin(qname, qname_cont, ws.fetch(QN_LOC));\n");
    else if (!strcmp(name,"namespace-uri"))
        milprintf(f,
                "var res := mposjoin(qname, qname_cont, ws.fetch(QN_URI));\n");
    else if (!strcmp(name,"name"))
        milprintf(f,
                /* creates the string representation of the qnames */
                "var prefixes := mposjoin(qname, qname_cont, ws.fetch(QN_PREFIX));\n"
                "var prefix_bool := prefixes.[=](\"\");\n"
                "var true_oid := prefix_bool.ord_uselect(true).hmark(0@0);\n"
                "var false_oid := prefix_bool.ord_uselect(false).hmark(0@0);\n"
                "prefix_bool := nil;\n"
                "prefixes := false_oid.leftfetchjoin(prefixes).[+](\":\");\n"
                "res_mu := merged_union(true_oid, false_oid, \"\", prefixes);\n"
                "true_oid := nil;\n"
                "false_oid := nil;\n"
                "prefixes := nil;\n"
                "prefixes := res_mu.fetch(1);\n" /* CONST? */
                "res_mu := nil;\n"
                "var res := prefixes.[+](mposjoin(qname, qname_cont, ws.fetch(QN_LOC)));\n"
                "prefixes := nil;\n");
    else
        mps_error ("no case for '%s' in function fn_name.",
                   name);

    milprintf(f,
            "qname := nil;\n"
            "qname_cont := nil;\n"
            "iter := qname_oid.leftfetchjoin(iter);\n"
            "qname_oid := nil;\n");

    if (code)
        milprintf(f, "item%s := res;\n", item_ext);
    else 
        addValues (f, str_container(), "res", "item");
 
    item_ext = (code)?item_ext:"";
    milprintf(f, "res := nil;\n");
    add_empty_strings (f, (code)?STR:NORMAL, cur_level);
    milprintf(f,
            "iter := loop%03u;\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := STR;\n"
            "} # end of fn:%s\n",
            cur_level,
            name);
    return (code)?STR:NORMAL;
}

/**
 * eval_join_helper prepares the input arguments for the join
 * and therefore gets the values from its containers or casts 
 * them directly from the last location step (special case)
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param number the integer contains either 1 or 2 according to 
 *        first or seconde join argument
 * @param node the join argument
 * @param special indicates wether the join argument
 *                has to be handled differently or not
 * @param res the offset of the variables storing the
 *            join argument
 * @param container the value container storing the values 
 */
static void
eval_join_helper (opt_t *f, int code, int number,
                  PFcnode_t *node, int special, int res, 
                  type_co container)
{
    if (special)
    {
        if (L(node)->kind == c_attribute)
        {
            milprintf(f,
                    "var join_item%i;\n"
                    "{\n"
                    "var join_item_str;\n"
                    "var cont := kind%03u.get_container();\n"
                    "join_item_str := mposjoin (mposjoin (item%03u, cont, ws.fetch(ATTR_PROP)), "
                                               "mposjoin (item%03u, cont, ws.fetch(ATTR_CONT)), "
                                               "ws.fetch(PROP_VAL));\n",
                    number, res, res, res);
        }
        else
        {
            milprintf(f,
                    "var join_item%i;\n"
                    "{\n"
                    "var join_item_str;\n"
                    "var cont := kind%03u.get_container();\n"
                    "join_item_str := mposjoin (mposjoin (item%03u, cont, ws.fetch(PRE_PROP)), "
                                               "mposjoin (item%03u, cont, ws.fetch(PRE_CONT)), "
                                               "ws.fetch(PROP_TEXT));\n",
                    number, res, res, res);
        }
        milprintf(f,
                "join_item%i := join_item_str.[%s]();\n"
                "join_item%i := join_item%i.materialize(ipik%03u);\n"
                "}\n"
                "if (join_item%i.texist(%s_nil))\n"
                "{    ERROR (\"err:FORG0001: could not cast value to %s.\"); }\n",
                number, container.mil_type,
                number, number, res, 
                number, container.mil_type,
                container.name);
    }
    else if (code)
    {
        /* in the evaluate_join translation every
           the item bats with values don't get
           the kind annotation */
        milprintf(f, "var join_item%i := item%03u;\n",
                number, res);
    }
    else
    {
        milprintf(f, "var join_item%i := item%03u.leftfetchjoin(%s);\n",
                number, res, container.table);
    }
}


/**
 * evaluate_join translates the recognized joins
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param args the head of the argument list
 * @result the kind indicating, which result interface is chosen
 */
static int
evaluate_join (opt_t *f, int code, int cur_level, int counter, PFcnode_t *args)
{
    unsigned int lev_fst, lev_snd,
        fst_res, snd_res, 
        snd_var, 
        cast_fst, cast_snd,
        switched_args, fid,
        rc, rc1, rc2;
    int i;
    PFcnode_t *fst, *snd, *res, *c;
    PFfun_t *fun;
    char *comp;
    char rx[32], order_snd[128];

    rx[0] = order_snd[0] = '\0';


    /* retrieve the arguments of the join function */
    fst = L(args);
    args = R(args);
    c = L(args);
    if (c->kind == c_seqtype)
    {
        cast_fst = 1;
    }
    else 
    {
        cast_fst = 0;
    }
        
    args = R(args);
    c = L(args);
    lev_fst = c->sem.num;

    args = R(args);
    snd = L(args);

    args = R(args);
    c = L(args);
    if (c->kind == c_seqtype)
    {
        cast_snd = 1;
    }
    else 
    {
        cast_snd = 0;
    }
        
    args = R(args);
    c = L(args);
    lev_snd = c->sem.num;

    args = R(args);
    c = L(args);
    fun = c->sem.fun;

    args = R(args);
    c = L(args);
    switched_args = c->sem.num;

    if (!PFqname_eq(fun->qname,PFqname (PFns_op,"eq")))
         comp = "EQ";
/*   
    else if (!PFqname_eq(fun->qname,PFqname (PFns_op,"ne")))
         comp = "NE";
*/   
    /* switches the comparison function if needed */
    else if (!PFqname_eq(fun->qname,PFqname (PFns_op,"ge")))
         comp = (switched_args)?"LE":"GE";
    else if (!PFqname_eq(fun->qname,PFqname (PFns_op,"le")))
         comp = (switched_args)?"GE":"LE";
    else if (!PFqname_eq(fun->qname,PFqname (PFns_op,"gt")))
         comp = (switched_args)?"LT":"GT";
    else if (!PFqname_eq(fun->qname,PFqname (PFns_op,"lt")))
         comp = (switched_args)?"GT":"LT";
    else
    {
         mps_error ("not supported comparison in thetajoin evaluation.");
         comp = ""; /* to pacifiy compilers */ 
    }

    args = R(args);
    res = L(args);

    /* fid selects variables used in the result */
    args = R(args);
    c = L(args);
    fid = c->sem.num;

    c = 0;

    /* create variables for intermediate results */
    /* we create them in an outer MIL scope '{ }' and therefore
       can't give the item bats containing values a special
       name according to their kind */
    counter++;
    snd_var = counter;
    milprintf(f,
            "{ # evaluate_join\n"
            "var ipik%03u;\n"
            "var iter%03u;\n"
            "var pos%03u;\n"
            "var item%03u;\n"
            "var kind%03u;\n",
            snd_var, snd_var, snd_var, snd_var, snd_var);
    counter++;
    fst_res = counter;
    milprintf(f,
            "var ipik%03u;\n"
            "var iter%03u;\n"
            "var pos%03u;\n"
            "var item%03u;\n"
            "var kind%03u;\n"
            "var match_outer%03u;\n",
            fst_res, fst_res, fst_res, fst_res, fst_res, fst_res);
    counter++;
    snd_res = counter;
    milprintf(f,
            "var ipik%03u;\n"
            "var iter%03u;\n"
            "var pos%03u;\n"
            "var item%03u;\n"
            "var kind%03u;\n"
            "var match_outer%03u;\n",
            snd_res, snd_res, snd_res, snd_res, snd_res, snd_res);

    /* create new backup scope */
    milprintf(f,
            "var jouter%03u ;\n"
            "var jorder_%03u ;\n"
            "var jinner%03u ;\n"
            "var jloop%03u  ;\n"
            "var jv_vid%03u ;\n"
            "var jv_iter%03u;\n"
            "var jv_pos%03u ;\n"
            "var jv_item%03u;\n"
            "var jv_kind%03u;\n",
            fst_res, fst_res, fst_res, fst_res, fst_res,
            fst_res, fst_res, fst_res, fst_res);


    if (lev_fst || lev_snd) /* default case */
    {
        rc1 = translate2MIL (f, VALUES, cur_level, counter, fst);

        /* introduce the correct map relation (if lev_snd == UDF_LEV) */
        if (lev_snd == UDF_LEV)
        {
            milprintf(f,
                    "{\n"
                    "var mapping := loop%03u.reverse().mirror();\n",
                    0);
            for (i = 0; i < cur_level; i++)
            {
                milprintf(f, 
                    "mapping := mapping.leftjoin(outer%03u.reverse());\n"
                    "mapping := mapping.leftfetchjoin(inner%03u);\n",
                    i+1, i+1);
            }
            milprintf(f,
                    /* tmark ensures void head */
                    "match_outer%03u := iter.leftjoin(mapping.reverse()).tmark(iter.seqbase());\n"
                    "}\n",
                    fst_res);
        } else {
            milprintf(f,
                    "var inner := inner%03u.tsort();\n"
                    "match_outer%03u := iter.leftfetchjoin(inner.hmark(min(inner)))"
                                           ".leftfetchjoin(outer%03u);\n",
                    cur_level, fst_res, cur_level);
        }

        milprintf(f,
                "ipik%03u := ipik;\n"
                "iter%03u := iter;\n"
                "pos%03u  := pos ;\n"
                "item%03u := item%s;\n" /* we need to use `item' because
                                           it was declared before */
                "kind%03u := kind;\n",
                fst_res, 
                fst_res, 
                fst_res, 
                fst_res, kind_str(rc1),
                fst_res);
        milprintf(f,
                "jouter%03u  := outer%03u ;\n"
                "jorder_%03u  := order_%03u ;\n"
                "jinner%03u  := inner%03u ;\n"
                "jloop%03u   := loop%03u  ;\n"
                "jv_vid%03u  := v_vid%03u ;\n"
                "jv_iter%03u := v_iter%03u;\n"
                "jv_pos%03u  := v_pos%03u ;\n"
                "jv_item%03u := v_item%03u;\n"
                "jv_kind%03u := v_kind%03u;\n",
                fst_res, cur_level,
                fst_res, cur_level, fst_res, cur_level,
                fst_res, cur_level, fst_res, cur_level,
                fst_res, cur_level, fst_res, cur_level,
                fst_res, cur_level, fst_res, cur_level);
    }
    else /* first join input contains only a constant
            (basically selection translation) */
    {
        milprintf(f,
                "jouter%03u  := outer%03u ;\n"
                "jorder_%03u  := order_%03u ;\n"
                "jinner%03u  := inner%03u ;\n"
                "jloop%03u   := loop%03u  ;\n"
                "jv_vid%03u  := v_vid%03u ;\n"
                "jv_iter%03u := v_iter%03u;\n"
                "jv_pos%03u  := v_pos%03u ;\n"
                "jv_item%03u := v_item%03u;\n"
                "jv_kind%03u := v_kind%03u;\n",
                fst_res, cur_level,
                fst_res, cur_level, fst_res, cur_level,
                fst_res, cur_level, fst_res, cur_level,
                fst_res, cur_level, fst_res, cur_level,
                fst_res, cur_level, fst_res, cur_level);
        milprintf(f,
                "outer%03u  := outer%03u .copy().access(BAT_WRITE);\n"
                "order_%03u  := order_%03u .copy().access(BAT_WRITE);\n"
                "inner%03u  := inner%03u .copy().access(BAT_WRITE);\n"
                "loop%03u   := loop%03u  .copy().access(BAT_WRITE);\n"
                "v_vid%03u  := v_vid%03u .copy().access(BAT_WRITE);\n"
                "v_iter%03u := v_iter%03u.copy().access(BAT_WRITE);\n"
                "v_pos%03u  := v_pos%03u .copy().access(BAT_WRITE);\n"
                "v_item%03u := v_item%03u.copy().access(BAT_WRITE);\n"
                "v_kind%03u := v_kind%03u.copy().access(BAT_WRITE);\n",
                cur_level, 0,
                cur_level, 0, cur_level, 0,
                cur_level, 0, cur_level, 0,
                cur_level, 0, cur_level, 0,
                cur_level, 0, cur_level, 0);
        rc1 = translate2MIL (f, VALUES, cur_level, counter, fst);
        milprintf(f,
                "ipik%03u := ipik;\n"
                "iter%03u := iter;\n"
                "pos%03u  := pos ;\n"
                "item%03u := item%s;\n" /* we need to use `item' because
                                           it was declared before */
                "kind%03u := kind;\n",
                fst_res, 
                fst_res, 
                fst_res, 
                fst_res, kind_str(rc1),
                fst_res);
    }

    if (!(lev_snd & ~UDF_LEV)) /* default case: lev_snd = level 0 */
    /* the above line only works because the current scope 0 is the one
       introduced by the UDF */
    {
        milprintf(f,
                "outer%03u  := outer%03u .copy().access(BAT_WRITE);\n"
                "order_%03u  := order_%03u .copy().access(BAT_WRITE);\n"
                "inner%03u  := inner%03u .copy().access(BAT_WRITE);\n"
                "loop%03u   := loop%03u  .copy().access(BAT_WRITE);\n"
                "v_vid%03u  := v_vid%03u .copy().access(BAT_WRITE);\n"
                "v_iter%03u := v_iter%03u.copy().access(BAT_WRITE);\n"
                "v_pos%03u  := v_pos%03u .copy().access(BAT_WRITE);\n"
                "v_item%03u := v_item%03u.copy().access(BAT_WRITE);\n"
                "v_kind%03u := v_kind%03u.copy().access(BAT_WRITE);\n",
                cur_level, 0,
                cur_level, 0, cur_level, 0,
                cur_level, 0, cur_level, 0,
                cur_level, 0, cur_level, 0,
                cur_level, 0, cur_level, 0);
    }
    else
    {
        /* if our join consists of two nested for loops and cur_level
           points to the the scope inside the outer for loop we still
           can savely subtract 1 to get the innermost common scope */
        milprintf(f,
                "outer%03u  := outer%03u .copy().access(BAT_WRITE);\n"
                "order_%03u  := order_%03u .copy().access(BAT_WRITE);\n"
                "inner%03u  := inner%03u .copy().access(BAT_WRITE);\n"
                "loop%03u   := loop%03u  .copy().access(BAT_WRITE);\n"
                "v_vid%03u  := v_vid%03u .copy().access(BAT_WRITE);\n"
                "v_iter%03u := v_iter%03u.copy().access(BAT_WRITE);\n"
                "v_pos%03u  := v_pos%03u .copy().access(BAT_WRITE);\n"
                "v_item%03u := v_item%03u.copy().access(BAT_WRITE);\n"
                "v_kind%03u := v_kind%03u.copy().access(BAT_WRITE);\n",
                cur_level, cur_level-1,
                cur_level, cur_level-1, cur_level, cur_level-1,
                cur_level, cur_level-1, cur_level, cur_level-1,
                cur_level, cur_level-1, cur_level, cur_level-1,
                cur_level, cur_level-1, cur_level, cur_level-1);
    }
    translate2MIL (f, NORMAL, cur_level, counter, LR(snd));
    cur_level++;
    milprintf(f, "{  # for-translation\n");
    project (f, cur_level);

    milprintf(f,
            "ipik%03u := ipik;\n"
            "iter%03u := iter;\n"
            "pos%03u  := pos ;\n"
            "item%03u := item;\n"
            "kind%03u := kind;\n",
            snd_var, snd_var, snd_var, snd_var, snd_var);

    milprintf(f, "var expOid;\n");
    getExpanded (f, cur_level, snd->sem.flwr.fid);
    milprintf(f,
            "if (expOid.count() != 0) {\n"
            "var oidNew_expOid;\n");
            expand (f, cur_level);
            join (f, cur_level);
    milprintf(f, "} else {\n");
            createNewVarTable (f, cur_level);
    milprintf(f,
            "}  # end if\n"
            "expOid := nil;\n");

    if (LLL(snd)->sem.var->used)
        insertVar (f, cur_level, LLL(snd)->sem.var->vid);
    if ((LLR(snd)->kind == c_var)
        && (LLR(snd)->sem.var->used))
    {
        createEnumeration (f, cur_level);
        insertVar (f, cur_level, LLR(snd)->sem.var->vid);
    }

    rc2 = translate2MIL (f, VALUES, cur_level, counter, R(snd));
    milprintf(f,
            "ipik%03u := ipik;\n"
            "iter%03u := iter;\n"
            "pos%03u  := pos ;\n"
            "item%03u := item%s;\n" /* we need to use `item' because
                                           it was declared before */
            "kind%03u := kind;\n"
            "match_outer%03u := iter.leftfetchjoin(inner%03u.reverse())"
                                   ".leftfetchjoin(outer%03u);\n",
            snd_res, 
            snd_res, 
            snd_res, 
            snd_res, kind_str(rc2),
            snd_res,
            snd_res, cur_level, cur_level);

    /* mapBack (f, cur_level); */
    cleanUpLevel (f, cur_level);
    milprintf(f, "}  # end of for-translation\n");
    cur_level--;

    /* overwrites values from second join parameter (not needed anymore) */
    milprintf(f,
            "outer%03u  := jouter%03u ;\n"
            "order_%03u  := jorder_%03u ;\n"
            "inner%03u  := jinner%03u ;\n"
            "loop%03u   := jloop%03u  ;\n"
            "v_vid%03u  := jv_vid%03u ;\n"
            "v_iter%03u := jv_iter%03u;\n"
            "v_pos%03u  := jv_pos%03u ;\n"
            "v_item%03u := jv_item%03u;\n"
            "v_kind%03u := jv_kind%03u;\n",
            cur_level, fst_res,
            cur_level, fst_res, cur_level, fst_res, 
            cur_level, fst_res, cur_level, fst_res, 
            cur_level, fst_res, cur_level, fst_res, 
            cur_level, fst_res, cur_level, fst_res);

    /* retrieves the join input arguments 'join_item1' and 'join_item2'
       from its value containers as well as covers the special cases
       (attribute step and text() test) */
    assert (fun->sig_count == 1);
    PFty_t input_type = (fun->sigs[0].par_ty)[0];
    if (PFty_subtype (PFty_xs_decimal (), input_type))
    {
        eval_join_helper (f, rc1, 1, fst, cast_fst, fst_res, dec_container());
        eval_join_helper (f, rc2, 2, R(snd), cast_snd, snd_res, dec_container());
    }
    /* integer is listed after decimal 
       (as type decimal is otherwise recognized as integer) */
    else if (PFty_subtype (PFty_xs_integer (), input_type))
    {
        eval_join_helper (f, rc1, 1, fst, cast_fst, fst_res, int_container());
        eval_join_helper (f, rc2, 2, R(snd), cast_snd, snd_res, int_container());
    }
    else if (PFty_subtype (PFty_xs_double (), input_type))
    {
        eval_join_helper (f, rc1, 1, fst, cast_fst, fst_res, dbl_container());
        eval_join_helper (f, rc2, 2, R(snd), cast_snd, snd_res, dbl_container());
    }
    else if (PFty_subtype (PFty_xs_string (), input_type))
    {
        eval_join_helper (f, rc1, 1, fst, cast_fst, fst_res, str_container());
        eval_join_helper (f, rc2, 2, R(snd), cast_snd, snd_res, str_container());
    }
    else if (PFty_subtype (PFty_xs_boolean (), input_type))
    {
        if (cast_fst || cast_snd)
            mps_error ("cast to boolean is not supported in thetajoin evaluation.");

        milprintf(f,
                "var join_item1 := item%03u;\n"
                "var join_item2 := item%03u;\n",
                fst_res, snd_res);
    }
    else
    {
        mps_error ("unsupported type in join evaluation.");
    }

    /* adds the iter column to the join input to avoid mapping after join 
       (relation probably is bigger afterwards) */
    milprintf(f,
            "join_item1 := join_item1.materialize(ipik%03u);\n"
            "join_item1 := join_item1.reverse().leftfetchjoin(iter%03u).reverse();\n"
            "join_item2 := join_item2.materialize(ipik%03u);\n"
            "join_item2 := join_item2.reverse().leftfetchjoin(iter%03u).reverse();\n",
            fst_res, fst_res, snd_res, snd_res);


    /* pushdown stuff */
    /* (try to) push some leftfetchjoin's below the theta-join */
    if ((LLR(snd)->kind == c_var && var_is_used (LLR(snd)->sem.var, res))
        && !(res->kind == c_var && res->sem.var == LLL(snd)->sem.var)) /* see query11 hack below */
    {
        /* cannot be pushed below theta-join, as 'snd_iter.[lng]()' is needed for 'addValues' (below) */
        snprintf(rx,32,"nil");
        snprintf(order_snd,128,
                "var order_snd := snd_iter.leftfetchjoin(iter%03u.reverse());\n",
                snd_var);
    }
    else
    {
        snprintf(rx,32,"iter%03u.reverse()",snd_var);
        snprintf(order_snd,128,
                "var order_snd := snd_iter; #.leftfetchjoin(iter%03u.reverse()); pushed below theta-join\n",
                snd_var);
    }

    if (lev_snd)
    {
        /* fill in thetajoin with scope cur_level-1 as inner-most common scope */
        /* match_outer is supposed to be aligned with join_item, even if the head of
           match_outer is completedly unrelated. */
        milprintf(f,
                "var join_result := ll_htordered_unique_thetajoin(%s, join_item1, join_item2, match_outer%03u, match_outer%03u,nil,%s);\n"
                "var snd_iter := join_result.tmark(0@0);\n"
                "var fst_iter := join_result.hmark(0@0);\n"
                "ipik := fst_iter;\n",
                comp, fst_res, snd_res, rx);
    }
    else if (lev_fst)
    {
        milprintf(f,
                "# if necessary, we (try to) push leftfetchjoin's below the theta-join\n"
                "var join_result := htordered_unique_thetajoin(%s, join_item1, join_item2, nil, %s);\n"
                "var snd_iter := join_result.tmark(0@0);\n"
                "var fst_iter := join_result.hmark(0@0);\n"
                "ipik := fst_iter;\n",
                comp, rx);
    }
    else
    {
        /* both sides are evaluated in scope 0 */
        milprintf(f,
                "# (for now,?) the mapping prohibits to push leftfetchjoin's below the theta-join\n"
                "# (unless we'd push the mapping, too, but that's a m-n join that might 'explode'...)\n"
                "var join_result := htordered_unique_thetajoin(%s, join_item1, join_item2, nil, nil);\n"
                "var snd_iter := join_result.tmark(0@0);\n"
                "var fst_iter := join_result.hmark(0@0);\n",
                comp);
        /* map forth to cur_level */
        milprintf(f,
                "{\n"
                "var mapping := outer%03u.reverse().leftfetchjoin(inner%03u);\n",
                0, 0);
        for (i = 0; i < cur_level; i++)
        {
            milprintf(f, 
                "mapping := mapping.leftjoin(outer%03u.reverse());\n"
                "mapping := mapping.leftfetchjoin(inner%03u);\n",
                i+1, i+1);
        }
        milprintf(f,
                "fst_iter := reverse(reverse(mapping).leftjoin(reverse(fst_iter)));\n"
                "}\n"
                "snd_iter := fst_iter.hmark(0@0).leftfetchjoin(snd_iter);\n"
                "fst_iter := fst_iter.tmark(0@0);\n"
                "ipik := fst_iter;\n");

        /* pushdown stuff */
        if (strcmp(rx,"nil")) {
            milprintf(f,
                "# leftfetchjoin that cannot be pushed below the theta-join (yet?)\n"
                "snd_iter := snd_iter.leftjoin(%s);\n",rx);
        }
    }

    /* pushdown stuff */
    milprintf(f,
            "# order_fst isn't needed until now\n%s", order_snd);

    /* shortcut to speed up xmark query11 */
    if (res->kind == c_var && res->sem.var == LLL(snd)->sem.var)
    {
            milprintf(f,
                    "# could also be pushed below theta-join, if order_snd wasn't needed for kind (below) ...\n"
                    "item := order_snd.leftfetchjoin(item%03u);\n"
                    "iter := fst_iter;\n"
                    "pos := 1@0;\n"
                    "# could also be pushed below theta-join, if order_snd wasn't needed for item (above) ...\n"
                    "kind := order_snd.leftfetchjoin(kind%03u);\n",
                    snd_var, snd_var);
            milprintf(f, "} # end of evaluate_join\n");
            return NORMAL;
    }

    /* result translation */
    cur_level++;
    milprintf(f, "{  # for-translation\n");

    milprintf(f, "iter := fst_iter;\n");
    project (f, cur_level);

    milprintf(f, "var expOid;\n");
    getExpanded (f, cur_level, fid);
    milprintf(f,
            "if (expOid.count() != 0) {\n"
            "var oidNew_expOid;\n");
            expand (f, cur_level);
            join (f, cur_level);
    milprintf(f, "} else {\n");
            createNewVarTable (f, cur_level);
    milprintf(f,
            "}  # end if\n"
            "expOid := nil;\n");

    if (var_is_used (LLL(snd)->sem.var, res))
    {
        milprintf(f,
                "# could also be pushed below theta-join, if order_snd wasn't needed for kind (below) ...\n"
                "item := order_snd.leftfetchjoin(item%03u);\n"
                "iter := ipik.mark(1@0);\n"
                "pos := 1@0;\n"
                "# could also be pushed below theta-join, if order_snd wasn't needed for item (above) ...\n"
                "kind := order_snd.leftfetchjoin(kind%03u);\n",
                snd_var, snd_var);
        insertVar (f, cur_level, LLL(snd)->sem.var->vid);
    }
    if (LLR(snd)->kind == c_var && var_is_used (LLR(snd)->sem.var, res))
    {
        addValues (f, int_container(), "snd_iter.[lng]()", "item");
        milprintf(f,
                "iter := ipik.mark(1@0);\n"
                "pos := 1@0;\n"
                "kind := INT;\n");
        insertVar (f, cur_level, LLR(snd)->sem.var->vid);
    }

    rc = translate2MIL (f, code, cur_level, counter, res);
        
    mapBack (f, cur_level);
    cleanUpLevel (f, cur_level);
    cur_level--;
    milprintf(f,
            "}  # end of for-translation\n"
            "} # end of evaluate_join\n");
    return rc;
}

/**
 * translateXRPCCall translates the XRPC calls to user defined functions
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param xrpc the core node containing all information we need
 *
 *              xrpc (query type)
 *             /    \
 *            URI (FunctionCall)->sem.fun
 *                   |
 *                  args
 *
 * @param apply the function application information
 * @param args the head of the argument list
 */
static void
translateXRPCCall (opt_t *f, int cur_level, int counter, PFcnode_t *xrpc)
{
    int i = 0, rc = NORMAL, updCall = PFqueryType(xrpc) == 1 ? 1 : 0;
    PFcnode_t *dsts = L(xrpc);
    PFfun_t   *fun  = R(xrpc)->sem.fun;
    PFcnode_t *args = RD(xrpc);

    if (fun->builtin){
        PFoops (OOPS_NOTSUPPORTED,
                "RPC calls to built-in functions not supported by XRPC.");
    } else if (PFqname_uri (fun->qname) == NULL) {
        PFoops (OOPS_NOTSUPPORTED,
                "RPC calls to in-line functions not supported by XRPC.");
    }

    milprintf(f, "{ # begin of XRPC function call\n");

    /* The extra parameter 'dst' of an RPC call is not listed in
     * fun->params[], so translate it separately. 
     * 'rpc_iter' contains the real total number of iterations. */
    milprintf(f, "  # begin of translate URIs\n");
    rc = translate2MIL (f, VALUES, cur_level, counter, dsts);
    if (rc == NORMAL){
        /* retrieve all URIs from str_values as indicated in item */
        milprintf(f, 
                "  item := item.materialize(ipik);\n"
                "  var rpc_dsts := item%s;\n", val_join(STR));
    } else {
        milprintf(f, 
                "  item%s := item%s.materialize(ipik);\n"
                "  var rpc_dsts := item%s;\n",
                kind_str(STR), kind_str(STR),
                kind_str(STR));
    }
    /* Assign each destination its corresponding iter. number.  rpc_dsts
     * now becomes [oid,str], with the iter numbers in * its head column
     */
    milprintf(f, 
            "  var rpc_iter := iter.materialize(ipik);\n"
            "  rpc_dsts := rpc_iter.reverse().join(rpc_dsts);\n"
            "  # end of translate URIs\n");

    milprintf(f,
            "  # begin of add args in XRPC function call\n"
            "  var fun_base%03u := proc_vid.find(\"%s\");\n"
            "  var fun_vid%03u  := bat(void,oid);\n"
            "  var fun_iter%03u := bat(void,oid);\n"
            "  var fun_item%03u := bat(void,oid);\n"
            "  var fun_kind%03u := bat(void,int);\n",
            counter, fun->sig,
            counter,
            counter,
            counter,
            counter);

    while ((args->kind != c_nil) && (fun->params[i]))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                "  iter := iter.materialize(ipik);\n"
                "  item := item.materialize(ipik);\n"
                "  kind := kind.materialize(ipik);\n"
                "  fun_vid%03u := fun_vid%03u.append(item.project(oid(fun_base%03u + %iLL)));\n"
                "  fun_iter%03u := fun_iter%03u.append(iter);\n"
                "  fun_item%03u := fun_item%03u.append(item);\n"
                "  kind := kind.materialize(ipik);\n"
                "  fun_kind%03u := fun_kind%03u.append(kind);\n\n",
                counter, counter, counter, i, 
                counter, counter, 
                counter, counter,
                counter, counter);
        args = R(args);
        i++;
    }
    milprintf(f, "  # end of add arg in XRPC function call\n");

    /* map needed global variables into the function */
    milprintf(f, "  var expOid;\n");
    getExpanded (f,
            /* we don't want to get the variables from
               the surrounding scope like for but from the current */
            cur_level+1, fun->fid);
    milprintf(f,
            "  var vid := expOid.leftfetchjoin(v_vid%03u);\n"
            "  iter    := expOid.leftfetchjoin(v_iter%03u);\n"
            "  item    := expOid.leftfetchjoin(v_item%03u);\n"
            "  kind    := expOid.leftfetchjoin(v_kind%03u);\n"
            "  ipik    := iter;\n"
            "  fun_vid%03u := fun_vid%03u.append(vid);\n"
            "  fun_iter%03u := fun_iter%03u.append(iter);\n"
            "  fun_item%03u := fun_item%03u.append(item);\n"
            "  fun_kind%03u := fun_kind%03u.append(kind);\n"
            "  expOid := nil;\n"
            "  vid := nil;\n"
            "  ipik := nil;\n"
            "  iter := nil;\n"
            "  pos := nil;\n"
            "  item := nil;\n"
            "  kind := nil;\n",
            cur_level, cur_level, cur_level, cur_level,
            counter, counter, counter, counter,
            counter, counter, counter, counter);

    milprintf(f,
            "  fun_vid%03u := fun_vid%03u.tmark(0@0);\n"
            "  fun_iter%03u := fun_iter%03u.tmark(0@0);\n"
            "  fun_item%03u := fun_item%03u.tmark(0@0);\n"
            "  fun_kind%03u := fun_kind%03u.tmark(0@0);\n",
            counter, counter, counter, counter,
            counter, counter, counter, counter);

    /* Define a variable to hold the results of a function call.
     * call rpc_sender => cont~=kind
     * extract return value (s) from the message node
     * into a iter|item|kind table
     * message node: (item=0@0, kind=~cont)
     */
    milprintf(f, 
            "  var iterc_total := count(int(rpc_iter));\n"
            /* remove the base number of fun_vid-s, in XRPC request
             * message, we need the offsets of each non-empty
             * parameters. */
            "  fun_vid%03u := ([-](fun_vid%03u.[lng](), fun_base%03u)).[oid]();\n"
            "  var res := doLoopLiftedRPC(genType,\n"
            "                             \"%s\", \"%s\", \"%s\",\n"
            "                             %d, %d, iterc_total,\n"
            "                             ws, rpc_dsts,\n"
            "                             fun_vid%03u, fun_iter%03u,\n"
            "                             fun_item%03u, fun_kind%03u,\n"
            "                             int_values, dbl_values,\n"
            "                             dec_values, str_values);\n"
            "  iter := res.fetch(0);\n"
            "  item := res.fetch(1);\n"
            "  kind := res.fetch(2);\n"
            "  if (type(iter) = bat) {\n"
            "    ipik := iter;\n"
            "  } else {\n"
            "    if (type(item) = bat) {\n"
            "      ipik := item;\n"
            "    } else {\n"
            "      ipik := kind;\n"
            "    }\n"
            "  }\n"
            "} # end of XRPC function call\n",
        counter, counter, counter,
        PFqname_uri (fun->qname), fun->atURI?fun->atURI:f->url, PFqname_loc (fun->qname),    
        updCall, fun->arity,
        counter, counter, counter, counter);
}

/**
 * translateUDF translates the user defined functions
 *
 * @param f the Stream the MIL code is printed to
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param apply the function application information
 * @param args the head of the argument list
 */
static void
translateUDF (opt_t *f, int cur_level, int counter, 
        PFfun_t *fun, PFcnode_t *args)
{
    int i = 0;

    milprintf(f, "{ # begin of UDF - function call\n");

    milprintf(f,
            "# begin of add args in UDF function call\n"
            "var fun_base%03u := proc_vid.find(\"%s\");\n"
            "var fun_vid%03u  := bat(void,oid);\n"
            "var fun_iter%03u := bat(void,oid);\n"
            "var fun_item%03u := bat(void,oid);\n"
            "var fun_kind%03u := bat(void,int);\n",
            counter, fun->sig, counter, counter, counter, counter);

    while ((args->kind != c_nil) && (fun->params[i]))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                "iter := iter.materialize(ipik);\n"
                "item := item.materialize(ipik);\n"
                "kind := kind.materialize(ipik);\n"
                "fun_vid%03u := fun_vid%03u.append(item.project(oid(fun_base%03u + %iLL)));\n"
                "fun_iter%03u := fun_iter%03u.append(iter);\n"
                "fun_item%03u := fun_item%03u.append(item);\n"
                "kind := kind.materialize(ipik);\n"
                "fun_kind%03u := fun_kind%03u.append(kind);\n\n",
                counter, counter, counter, i, 
                counter, counter, 
                counter, counter,
                counter, counter);
        args = R(args);
        i++;
    }
    milprintf(f, "# end of add arg in UDF function call\n");

    /* map needed global variables into the function */
    milprintf(f, "var expOid;\n");
    getExpanded (f,
            /* we don't want to get the variables from
               the surrounding scope like for but from the current */
            cur_level+1, fun->fid);
    milprintf(f,
            "var vid := expOid.leftfetchjoin(v_vid%03u);\n"
            "iter    := expOid.leftfetchjoin(v_iter%03u);\n"
            "item    := expOid.leftfetchjoin(v_item%03u);\n"
            "kind    := expOid.leftfetchjoin(v_kind%03u);\n"
            "ipik    := iter;\n"
            "fun_vid%03u := fun_vid%03u.append(vid);\n"
            "fun_iter%03u := fun_iter%03u.append(iter);\n"
            "fun_item%03u := fun_item%03u.append(item);\n"
            "fun_kind%03u := fun_kind%03u.append(kind);\n"
            "expOid := nil;\n"
            "vid := nil;\n"
            "ipik := nil;\n"
            "iter := nil;\n"
            "pos := nil;\n"
            "item := nil;\n"
            "kind := nil;\n",
            cur_level, cur_level, cur_level, cur_level,
            counter, counter, counter, counter,
            counter, counter, counter, counter);

    milprintf(f,
            "fun_vid%03u := fun_vid%03u.tmark(0@0);\n"
            "fun_iter%03u := fun_iter%03u.tmark(0@0);\n"
            "fun_item%03u := fun_item%03u.tmark(0@0);\n"
            "fun_kind%03u := fun_kind%03u.tmark(0@0);\n",
            counter, counter, counter, counter,
            counter, counter, counter, counter);

    /* call the proc */
    milprintf(f, PFudfMIL(),
            fun->sig,
            cur_level, cur_level, cur_level, cur_level, 
            counter, counter, counter, counter, 
            PFqname_loc (fun->qname), PFqname_loc (fun->qname));

    milprintf(f, "} # end of UDF - function call\n");
}
/**
 * translateFunction translates the builtin functions
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param fun the function information
 * @param args the head of the argument list
 * @result the kind indicating, which result interface is chosen
 */
static int
translateFunction (opt_t *f, int code, int cur_level, int counter, 
                   PFfun_t *fun, PFcnode_t *args)
{
    int rc = 0;
    char *item_ext = NULL;
    PFqname_t fnQname = fun->qname;

    if (!PFqname_eq(fnQname,PFqname (PFns_fn,"doc")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        item_ext = kind_str(rc);

        /* expects strings otherwise something stupid happens */
        milprintf(f,
                "{ # translate fn:doc (string?) as document?\n"
                "  var t := usec();\n"
                "  var r := ws_opendoc(ws, item%s.materialize(ipik));\n"
                "  kind  := r.tmark(0@0).set_kind(ELEM);\n"
                "  item  := r.hmark(0@0);\n"
                "  time_shred := time_shred + usec() - t;\n"
                "} # end of translate fn:doc (string?) as document?\n", (rc)?item_ext:val_join(STR));
        return NORMAL;
    } else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"collection")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        item_ext = kind_str(rc);
        milprintf(f, 
            "{ # translate fn:collection (string) as node*\n"
            "  var map := bat(void,oid).seqbase(0@0);\n"
            "  var ret := ws_collection(ws, item%s.materialize(ipik), map);\n"
            "  iter := map.leftfetchjoin(iter);\n"
            "  item := ret.hmark(0@0);\n"
            "  kind := ret.tmark(0@0).set_kind(ELEM);\n"
            "  ipik := item;\n"
            "  pos  := tmark_grp_unique(iter,ipik);\n"
            "} # end of translate fn:collection (string) as node*\n", (rc)?item_ext:val_join(STR));
        return NORMAL;
    } else if (!PFqname_eq(fnQname,PFqname (PFns_lib,"collection")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        item_ext = kind_str(rc);
        milprintf(f, 
            "{ # translate pf:collection (string) as node\n"
            "  var ret := ws_collection_root(ws, item%s.materialize(ipik));\n"
            "  item := ret.tmark(0@0);\n"
            "  kind := ret.hmark(0@0).set_kind(ELEM);\n"
            "  ipik := item;\n"
            "  pos  := 1@0;\n"
            "} # end of translate pf:collection (string) as node*\n", (rc)?item_ext:val_join(STR));
        return NORMAL;
    } else if (PFqname_eq(fnQname,PFqname (PFns_lib,"documents")) == 0 ||
               (rc = 1, PFqname_eq(fnQname,PFqname (PFns_lib,"documents-unsafe")) == 0))
    {
        char *consistent = rc?"false":"true";
        if (fun->arity) {
            rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
            item_ext = kind_str(rc);
            milprintf(f, 
                "{ # translate pf:documents (string*) as string*\n"
                "  var ret := ws_documents(ws, item%s.materialize(ipik),%s);\n"
                "  item := ret.tmark(0@0);\n"
                "  iter := ret.hmark(0@0).leftfetchjoin(iter);\n", (rc)?item_ext:val_join(STR), consistent);
        } else {
            milprintf(f, 
                "{ # translate pf:documents () as string*\n"
                "  var ret := reverse(loop%03u).cross(ws_documents(ws,%s));\n"
                "  iter := ret.hmark(0@0);\n"
                "  item := ret.tmark(0@0);\n", cur_level, consistent);
        }
        milprintf(f,
                "  ipik := item;\n"
                "  kind := set_kind(WS,ELEM);\n"
                "  pos  := tmark_grp_unique(iter,ipik);\n" 
                "} # end of translate fn:documents (string?) as string*\n");
        return NORMAL;
    } else if (PFqname_eq(fnQname,PFqname (PFns_lib,"collections")) == 0 ||
               (rc = 1, PFqname_eq(fnQname,PFqname (PFns_lib,"collections-unsafe")) == 0))
    {
        char *consistent = rc?"false":"true";
        milprintf(f,
                "{ # translate pf:collections () as string*\n"
                "  var ret := reverse(loop%03u).cross(ws_collections(ws, %s));\n"
                "  iter := ret.hmark(0@0);\n"
                "  item := ret.tmark(0@0);\n"
                "  ipik := item;\n"
                "  kind := set_kind(WS,ELEM);\n"
                "  pos  := tmark_grp_unique(iter,ipik);\n"
                "} # end of translate fn:collections () as string*\n", cur_level, consistent);
        return NORMAL;
    } else if (!PFqname_eq(fnQname,PFqname (PFns_lib,"mil")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        item_ext = kind_str(rc);
        milprintf(f,
                "{ # translate pf:mil (string) as item*\n"
                "  if (count(loop%03u) != 1) ERROR(\"pf:mil cannot be called from within a for-loop\");\n"
                "  kind := ELEM;\n"
                "  item := ws_mil(ws, item%s.fetch(0));\n"
                "  if (type(item) = str) {\n"
                "     item := addValues(str_values,item);\n"
                "     kind := STR;\n"
                "  }\n"
                "  kind := set_kind(WS,kind);\n"
                "  iter := item.materialize(ipik).project(1@0);\n"
                "  ipik := iter;\n"
                "  pos  := ipik.tmark(1@0);\n"
                "} # end of translate fn:mil (string) as item*\n", cur_level, rc?item_ext:val_join(STR));
        return NORMAL;
    } else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"put")))
    {
	/* this is a simple implementation that just serializes to file URLs */
        rc = translate2MIL (f, NORMAL, cur_level, counter, L(args));
        counter++;
        saveResult_ (f, counter, NORMAL);
        rc = translate2MIL (f, VALUES, cur_level, counter, RL(args));
        if (rc == NORMAL)
        {
            milprintf(f, "item_str_ := item%s;\n", val_join(STR));
        }

        /* we return an empty update list here, as put is performed on-the-fly
         * TODO: we do not detect duplicate URIs yet, as we should check across multiple calls in a query.
         *       To do that, we should use the update list. But providing ACID properties for this implementation 
         *       of fn:put (basically serializing to file) is impossible as we are handling resources outside 
         *       the DBMS control (i.e. OS files).
         */
        milprintf(f,
                "{ # translate fn:put (node, string) as stmt\n"
                "  fn_put(ws, item_str_.materialize(ipik), item%03u.materialize(ipik%03u), kind%03u.materialize(ipik%03u), int_values, dbl_values, dec_values, str_values);\n"
                "  item := bat(void,oid).seqbase(0@0);\n"
                "  kind := bat(void,int).seqbase(0@0);\n"
                "  iter := 1@0;\n"
                "  pos := 1;\n"
                "  ipik := item;\n"
                "} # end of translate fn:put (node, string) as stmt\n", counter, counter, counter, counter);
        deleteResult_ (f, counter, NORMAL);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_pf,"distinct-doc-order")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                "{ # translate pf:distinct-doc-order (node*) as node*\n"
                /* FIXME: are attribute nodes automatically filtered? */
                "var sorting;\n"
                "var simple := false;\n"
                "if (type(kind) != bat) { simple := (kind = ELEM); }\n"
                "if (not(simple)) {\n"
                "  kind := kind.materialize(ipik);\n"
                "  simple := (kind.count() = kind.get_type(ELEM).count());\n"
                "}\n"
                "if (simple) {\n"
                /* delete duplicates */
                "  sorting := iter.tsort();\n"
                "  sorting := sorting.CTrefine(kind);\n"
                "  sorting := sorting.CTrefine(item);\n"
                "} else { # cope also with attributes and sort them according to their owner\n"
                "  var elements := kind.get_type(ELEM).mirror();\n"
                "  var elem_iters := elements.leftfetchjoin(iter);\n"
                "  var elem_items := elements.leftfetchjoin(item);\n"
                "  var elem_conts := elements.leftfetchjoin(kind.get_container());\n"
                "  var elem_attrs := elements.project(oid_nil);\n"
                "  var attributes := kind.get_type(ATTR).mirror();\n"
                "  var attr_iters := attributes.leftfetchjoin(iter).materialize(attributes);\n"
                "  var attr_attrs := attributes.leftfetchjoin(item).materialize(attributes);\n"
                "  var attr_conts := attributes.leftfetchjoin(kind.get_container());\n"
                "  var attr_key := attributes.hmark(0@0);\n"
                "  var temp_attr := attr_attrs.tmark(0@0);\n"
                "  var temp_cont := attr_conts.tmark(0@0);\n"
                "  var attr_items := attr_key.reverse().leftfetchjoin("
                    "mposjoin(temp_attr, temp_cont, ws.fetch(ATTR_OWN)));\n"
                "  attr_key := nil;\n"
                "  temp_attr := nil;\n"
                "  temp_cont := nil;\n"
                "  sorting := elem_iters.union(attr_iters).tsort();\n"
                "  elem_iters := nil;\n"
                "  attr_iters := nil;\n"
                "  sorting := sorting.CTrefine(elem_conts.union(attr_conts));\n"
                "  elem_conts := nil;\n"
                "  attr_conts := nil;\n"
                "  sorting := sorting.CTrefine(elem_items.union(attr_items));\n"
                "  elem_items := nil;\n"
                "  attr_items := nil;\n"
                "  sorting := sorting.CTrefine(elem_attrs.union(attr_attrs));\n"
                "  elem_attrs := nil;\n"
                "  attr_attrs := nil;\n"
                "}\n"
                "ipik := tmark_unique(sorting,ipik);\n"
                "iter := ipik.leftfetchjoin(iter);\n"
                "pos := tmark_grp_unique(iter,ipik);\n"
                "item := ipik.leftfetchjoin(item);\n"
                "kind := ipik.leftfetchjoin(kind);\n"
                "} # end of translate pf:distinct-doc-order (node*) as node*\n"
               );
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"intersect")))
    {
        translateIntersect (f, "intersect", cur_level, counter, args);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"except")))
    {
        translateIntersect (f, "diff", cur_level, counter, args);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_lib,"nid")))
    {
        milprintf(f, "{ # translate pf:nid (node) as xs:string\n");
        translate2MIL (f, code, cur_level, counter, L(args));
        milprintf(f, "var res := [str]([lng](item.mposjoin(kind.get_container(), ws.fetch(PRE_NID))));\n");
        return_str_funs (f, code, cur_level, "pf:nid (node) as xs:string");
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"id")))
    {
        fn_id (f, NULL, cur_level, counter, args);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"idref")))
    {
        fn_id (f, "ref", cur_level, counter, args);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"root")))
    {
        if (args->kind == c_nil)
            mps_error ("missing context in function fn:root.");
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                "{ # fn:root ()\n"
                "var cont := kind.get_container();\n"
                "item := get_root(ws, item, kind, cont);\n"
                "kind := set_kind(cont,NODE);\n"
                "} # end of fn:root ()\n");
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"exactly-one")))
    {
        rc = translate2MIL (f, code, cur_level, counter, L(args));
        milprintf(f,
                "var cnt := iter.tunique().count();\n"
                "if ((cnt != loop%03u.count()) or (cnt != ipik.count()))"
                "{ ERROR (\"err:FORG0005: function fn:exactly-one expects "
                "exactly one value.\"); }\n",
                cur_level);
        return rc;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"zero-or-one")))
    {
        rc = translate2MIL (f, code, cur_level, counter, L(args));
        milprintf(f,
                "if (iter.tunique().count() != ipik.count()) "
                "{ ERROR (\"err:FORG0003: function fn:zero-or-one expects "
                "at most one value.\"); }\n");
        return rc;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"position")))
    {
        mps_error ("forgot to replace fn:position.");
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"last")))
    {
        mps_error ("forgot to replace fn:last.");
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_pf,"typed-value")))
    {
        /* we are assuming that nothing is validated
           and we can use string-value */
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        string_value (f, code, "U_A");
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,
                         PFqname (PFns_pf,"item-sequence-to-untypedAtomic")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        fn_data (f);
        translateCast2STR (f, STR, NORMAL, TY(L(args)));
        combine_strings (f, code, U_A);
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,
                         PFqname (PFns_pf,"item-sequence-to-node-sequence")) ||
             !PFqname_eq(fnQname,
                         PFqname (PFns_pf,"merge-adjacent-text-nodes")))
    {
        /* translate is2ns for node* separately */
        if (code == NODE &&
            PFty_subtype (TY(L(args)), PFty_star (PFty_node ())))
        {
            rc = translateElemContent (f, NODE, cur_level, counter, L(args));
            if (rc == NORMAL)
                map2NODE_interface (f);
            is2ns_node (f, counter);
            return NODE;
        }
        else
        {
            translate2MIL (f, NORMAL, cur_level, counter, L(args));
            is2ns (f, counter, TY(L(args)));
            return NORMAL;
        }
    }
    else if (!PFqname_eq(fnQname,
                         PFqname (PFns_fn,"distinct-values")))
    {
        rc = translate2MIL (f, code, cur_level, counter, L(args));
        item_ext = kind_str(rc);

        milprintf(f,
                "{ # translate fn:distinct-values (atomic*) as atomic*\n"
                /* delete duplicates */
                "var sorting := iter.tsort();\n"
                "sorting := sorting.CTrefine(kind);\n"
                "sorting := sorting.CTrefine(item%s);\n"
                "ipik := tmark_unique(sorting,ipik);\n"
                "sorting := nil;\n"
                "iter := ipik.leftfetchjoin(iter);\n"
                "pos := iter.mark(1@0);\n"
                "item%s := ipik.leftfetchjoin(item%s);\n"
                "kind := ipik.leftfetchjoin(kind);\n"
                "} # end of translate fn:distinct-values (atomic*) as atomic*\n",
                item_ext, item_ext, item_ext);
        return rc;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_pf,"string-value")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        string_value (f, code, "STR");
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"data")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));

        if (PFty_subtype (TY(L(args)), PFty_star (PFty_node ())))
        {
            /* this branch is probably never evaluated
               - optimization already applied in coreopt.brg */
            milprintf(f,
                      "{ # fn:data\n"
                      "var data_iter := iter;\n"
                      "iter := iter.mirror();\n");
            /* we are assuming that nothing is validated
               and we can use string-value */
            string_value (f, code, "U_A");
            milprintf(f,
                      "iter := iter.leftfetchjoin(data_iter);\n"
                      "data_iter := nil;\n"
                      "pos := iter.mark_grp(iter.tunique().project(nil), 1@0);\n"
                      "} # end of fn:data\n");
    
            rc = (code)?STR:NORMAL;
        }
        else
        {
            fn_data (f);
            rc = NORMAL;
        }
        return rc;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"string")))
    {
        return fn_string (f, code, cur_level, counter, L(args));
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"name")))
    {
        return fn_name (f, code, cur_level, counter, args, "name");
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"local-name")))
    {
        return fn_name (f, code, cur_level, counter, args, "local-name");
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"namespace-uri")))
    {
        return fn_name (f, code, cur_level, counter, args, "namespace-uri");
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"string-join")))
    {
        int rc2;
        char *item_str = kind_str(STR);
        item_ext = (code)?item_str:"";

        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_str, val_join(STR));
        }

        counter++;
        saveResult_ (f, counter, STR);

        rc2 = translate2MIL (f, VALUES, cur_level, counter, RL(args));
        if (rc2 == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_str, val_join(STR));
        }

        milprintf(f,
                "{ # string-join (string*, string)\n "
                "var iter_item_str := iter%03u.materialize(ipik%03u).reverse();\n"
                "iter_item_str := iter_item_str.leftfetchjoin(item%s%03u).materialize(iter_item_str).chk_order();\n"
                "var iter_sep_str := iter.materialize(ipik).reverse();\n"
                "iter_sep_str := iter_sep_str.leftfetchjoin(item%s).materialize(iter_sep_str);\n"
                "iter_item_str := string_join(iter_item_str, iter_sep_str);\n"
                "iter_sep_str := nil;\n"
                "iter := iter_item_str.hmark(0@0);\n"
                "iter_item_str := iter_item_str.tmark(0@0);\n",
                counter, counter, item_str, counter,
                item_str);
        if (code)
        {
            milprintf(f,"item%s := iter_item_str;\n", item_ext);
        }
        else
        {
            addValues(f, str_container(), "iter_item_str", "item");
        }
        milprintf(f,
                "iter_item_str := nil;\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := STR;\n"
                "} # end of string-join (string*, string)\n ");
        deleteResult_ (f, counter, STR);
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"contains")))
    {
        int rc2;
        item_ext = kind_str(STR);

        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
        }

        counter++;
        saveResult_ (f, counter, STR);

        rc2 = translate2MIL (f, VALUES, cur_level, counter, RL(args));
        if (rc2 == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
        }

        milprintf(f,
                "{ # fn:contains (string?, string?) as boolean\n"
                "var strings;\n"
                "if (iter%03u.count() != loop%03u.count())\n"
                "{\n"
                "var difference := reverse(loop%03u.tdiff(iter%03u));\n"
                "difference := difference.hmark(0@0);\n"
                "var res_mu := merged_union(iter%03u.chk_order(), difference, item%s%03u, \"\");\n"
                "strings := res_mu.fetch(1);\n" /* CONST? */
                "} else {\n"
                "strings := item%s%03u;\n"
                "}\n",
                counter, cur_level, 
                cur_level, counter,
                counter, item_ext, counter,
                item_ext, counter);
        milprintf(f,
                "var search_strs;\n"
                "if (iter.count() != loop%03u.count())\n"
                "{\n"
                "var difference := reverse(loop%03u.tdiff(iter));\n"
                "difference := difference.hmark(0@0);\n"
                "var res_mu := merged_union(iter, difference, item%s, \"\");\n"
                "search_strs := res_mu.fetch(1);\n" /* CONST? */
                "} else {\n"
                "search_strs := item%s;\n"
                "}\n",
                cur_level,
                cur_level,
                item_ext,
                item_ext);
        milprintf(f,
                "item := [search](strings,search_strs).[!=](-1).[oid]();\n"
                "iter := loop%03u.tmark(0@0);\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := BOOL;\n"
                "} # end of fn:contains (string?, string?) as boolean\n",
                cur_level);
        deleteResult_ (f, counter, STR);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"concat")))
    {
        int rc2;
        char *item_str = kind_str(STR);
        item_ext = (code)?item_str:"";

        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_str, val_join(STR));
        }

        counter++;
        saveResult_ (f, counter, STR);

        rc2 = translate2MIL (f, VALUES, cur_level, counter, RL(args));
        if (rc2 == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_str, val_join(STR));
        }

        milprintf(f,
                "{ # concat (string, string)\n "
                "var result := item%s%03u.[+](item%s);\n",
		item_str, counter,
                kind_str(STR));
        if (code)
        {
            milprintf(f,"item%s := result;\n", item_ext);
        }
        else
        {
            addValues(f, str_container(), "result", "item");
        }
        milprintf(f, "} # end of concat (string, string)\n ");
        deleteResult_ (f, counter, STR);
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"starts-with")))
    {
        fn_starts_with (f, "starts", cur_level, counter, args);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"ends-with")))
    {
        fn_starts_with (f, "ends", cur_level, counter, args);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"substring")))
    {
        fn_substring (f, code, cur_level, counter, fun, args);
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"matches")))
    {
        return fn_matches (f, cur_level, counter, fun, args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"replace")))
    {
        return fn_replace (f, code, cur_level, counter, fun, args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"translate")))
    {
        return fn_translate (f, code, cur_level, counter, args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"substring-before")))
    {
        item_ext = kind_str(STR);
        counter = prep_str_funs(f, cur_level, counter, args);
        milprintf(f,
                "{ # fn:substring-before\n"
                "var search_result%s := [search](item%s%03u, item%s);\n"
                "var res := [ifthenelse]([<](search_result%s, 0), \"\", "
					 "[stringleft](item%s%03u, search_result%s));\n"
                "search_result%s := nil;\n",
                item_ext, item_ext, counter, item_ext,
                item_ext,
                item_ext, counter, item_ext,
                item_ext);

        return_str_funs (f, code, cur_level, "fn:substring-before");
        deleteResult_ (f, counter, STR);
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"substring-after")))
    {
        item_ext = kind_str(STR);
        counter = prep_str_funs(f, cur_level, counter, args);
        milprintf(f,
                "{ # fn:substring-after\n"
                "var length_item%s := [length](item%s);\n"
                "var search_result%s := [search](item%s%03u, item%s);\n"
                "var res := [ifthenelse]([<](search_result%s, 0), \"\", [string](item%s%03u, "
                                        "[+](length_item%s, search_result%s)));\n"
                "length_item%s := nil;\n"
                "search_result%s := nil;\n",
                item_ext, item_ext,
                item_ext, item_ext, counter, item_ext,
		item_ext, item_ext, counter,
                item_ext, item_ext,
                item_ext,
                item_ext);
                
        return_str_funs (f, code, cur_level, "fn:substring-after");
        deleteResult_ (f, counter, STR);
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"normalize-space")))
    {
        item_ext = kind_str(STR);
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
            milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
        milprintf(f,
                "{ # fn:normalize-space\n"
                "item%s := item%s.[normSpace]();\n",
                item_ext, item_ext);
        add_empty_strings (f, STR, cur_level);
        milprintf(f, "var res := item%s;\n", item_ext);

        return_str_funs (f, code, cur_level, "fn:normalize-space");
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"lower-case")))
    {
        item_ext = kind_str(STR);
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
            milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
        milprintf(f,
                "{ # fn:lower-case\n"
                "item%s := item%s.[toLower]();\n",
                item_ext, item_ext);
        add_empty_strings (f, STR, cur_level);
        milprintf(f, "var res := item%s;\n", item_ext);

        return_str_funs (f, code, cur_level, "fn:lower-case");
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"upper-case")))
    {
        item_ext = kind_str(STR);
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
            milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
        milprintf(f,
                "{ # fn:upper-case\n"
                "item%s := item%s.[toUpper]();\n",
                item_ext, item_ext);
        add_empty_strings (f, STR, cur_level);
        milprintf(f, "var res := item%s;\n", item_ext);

        return_str_funs (f, code, cur_level, "fn:upper-case");
        return (code)?STR:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"string-length")))
    {
        char *item_ext_int = kind_str(INT);
        item_ext = kind_str(STR);

        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
            milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
        add_empty_strings (f, STR, cur_level);
        milprintf(f,
                "{ # fn:string-length\n"
                "item%s := item%s.[length]().[lng]();\n"
                "var res := item%s;\n", 
                item_ext_int, item_ext,
                item_ext_int);

        if (code)
            milprintf(f, "item%s := res;\n", item_ext_int);
        else 
            addValues (f, int_container(), "res", "item");

        item_ext_int = (code)?item_ext_int:"";
        milprintf(f,
                "iter := loop%03u;\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := INT;\n"
                "} # end of fn:string-length\n",
                cur_level);

        return (code)?INT:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"number")))
    {
        item_ext = (code)?kind_str(DBL):"";
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));

        /* cast to dbl is inlined nil represents NaN value */
        if (rc != NORMAL)
            milprintf(f,
                    "var cast_val := [dbl](item%s);\n", kind_str(rc));
        else if (PFty_subtype (TY(L(args)), PFty_xs_boolean ()))
            milprintf(f,
                    "var cast_val := [dbl](item);\n");
        else
            milprintf(f,
                    "var cast_val := [dbl](item%s);\n", kind_str(get_kind(TY(L(args)))));

        milprintf(f,
                "if (iter.count() != loop%03u.count())\n"
                "{\n"
                "var difference := reverse(loop%03u.tdiff(iter));\n"
                "difference := difference.hmark(0@0);\n"
                "var res_mu := merged_union(iter, difference, cast_val, dbl_nil);\n"
                "cast_val := res_mu.fetch(1);\n" /* CONST? */
                "}\n",
                cur_level,
                cur_level);

        /* nil as NaN value doesn't work out, because nil disappears in joins */
        milprintf(f,
                "if (cast_val.texist(dbl_nil))\n"
                "{    ERROR (\"We do not support the value NaN.\"); }\n");

        if (!code)
            addValues (f, dbl_container(), "cast_val", "item");
        else
            milprintf(f, "item%s := cast_val;\n", item_ext);

        milprintf(f,
                "cast_val := nil;\n"
                "iter := loop%03u;\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "item%s := item%s.tmark(0@0);\n"
                "kind := DBL;\n",
                cur_level, item_ext, item_ext);

        return (code)?DBL:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"to")))
    {
        int rc2;
        char *item_int = kind_str(INT);
        item_ext = (code)?item_int:"";

        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_int, val_join(INT));
        }

        counter++;
        saveResult_ (f, counter, INT);

        rc2 = translate2MIL (f, VALUES, cur_level, counter, RL(args));
        if (rc2 == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_int, val_join(INT));
        }

        milprintf(f,
                "{ # op:to (integer, integer)\n"
                /* because all values have to be there we can compare the voids
                   instead of the iter values */
                "var from := item%s%03u;\n"
                "var length := item%s.[-](from).[+](1LL).[max](0LL);\n"
                "var res := enumerate(from.materialize(ipik%03u), length.materialize(ipik%03u));\n"
                "length := nil;\n"
                "iter := res.hmark(0@0).leftfetchjoin(iter);\n"
                "res := res.tmark(0@0);\n",
		item_int, counter,
                item_int, 
		counter, counter);
        if (code)
        {
            milprintf(f,"item%s := res;\n", item_ext);
        }
        else
        {
            addValues(f, int_container(), "res", "item");
        }
        milprintf(f,
                "res := nil;\n"
                "pos := tmark_grp_unique(iter,ipik);\n"
                "ipik := iter;\n"
                "kind := INT;\n"
                "} # end of op:to (integer, integer)\n ");
        deleteResult_ (f, counter, INT);
        return (code)?INT:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"count")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));

        item_ext = (code)?kind_str(INT):"";

        milprintf(f,
                "{ # translate fn:count (item*) as integer\n"
                /* counts for all iters the number of items */
                /* uses the actual loop, to collect the iters, which are translated 
                   into empty sequences */
                "iter := iter.materialize(ipik);\n"
                "var iter_count := [lng]({count}(iter.reverse(),loop%03u.reverse(), FALSE)).tmark(0@0);\n",
                cur_level);

        if (code)
            milprintf(f, "item%s := iter_count;\n", item_ext);
        else 
            addValues (f, int_container(), "iter_count", "item");

        milprintf(f,
                "iter := loop%03u.tmark(0@0);\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := INT;\n"
                "} # end of translate fn:count (item*) as integer\n",
                cur_level);

        return (code)?INT:NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"avg")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        return translateAggregates (f, code, rc, fun, args, "avg");
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"max")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        return translateAggregates (f, code, rc, fun, args, "max");
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"min")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        return translateAggregates (f, code, rc, fun, args, "min");
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"sum")))
    {
        assert (fun->sig_count == 1);
        int rcode = get_kind(fun->sigs[0].ret_ty);
        type_co t_co = kind_container(rcode);
        item_ext = kind_str(rcode);

        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        rc = translateAggregates (f, VALUES, rc, fun, args, "sum");
        if (rc == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_ext, val_join(rcode));
        }
        counter++;
        saveResult_ (f, counter, rcode);

        if (fun->arity == 2)
        {
            rc = translate2MIL (f, VALUES, cur_level, counter, RL(args));
            if (rc == NORMAL)
            {
                milprintf(f, "item%s := item%s;\n", item_ext, val_join(rcode));
            }
        }
        else
        {
            milprintf(f, 
                    "iter := loop%03u;\n"
                    "item%s := %s(0);\n",
                    cur_level,
                    item_ext, t_co.mil_type);
        }

        milprintf(f,
                "var item_result;\n"
                "if (iter%03u.count() != loop%03u.count())\n"
                "{ # add zero values needed for fn:sum\n"
                "var difference := loop%03u.tdiff(iter%03u)"
                                           ".reverse().mirror()"
                                           ".leftjoin(reverse(iter));\n"
                "var zeros := difference.tmark(0@0).leftfetchjoin(item%s);\n"
                "difference := difference.hmark(0@0);\n"
                "var res_mu := merged_union(iter%03u, difference, item%s%03u, zeros);\n"
                "iter := res_mu.fetch(0);\n"
                "item_result := res_mu.fetch(1);\n" /* CONST? */
                "} else {\n"
                "iter := iter%03u;\n"
                "item_result := item%s%03u;\n"
                "}\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := %s;\n",
                counter, cur_level, 
                cur_level, counter,
                item_ext,
                counter,
                item_ext, counter,
                counter,
                item_ext, counter,
                t_co.mil_cast);

        if (code)
        {
            milprintf(f, "item%s := item_result;\n", item_ext);
        }
        else
        {
            addValues (f, t_co, "item_result", "item");
        }
        milprintf(f, 
                "item%s := item%s.tmark(0@0);\n",
                (code)?item_ext:"", (code)?item_ext:"");

        deleteResult_ (f, counter, rcode);
        return rcode;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"abs")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            assert (fun->sig_count == 1); 
            rc = get_kind(fun->sigs[0].ret_ty);
            milprintf(f, "item%s := item%s;\n", kind_str(rc), val_join(rc));
        }
        fn_abs (f, code, rc, "abs");
        return rc;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"ceiling")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            assert (fun->sig_count == 1); 
            rc = get_kind(fun->sigs[0].ret_ty);
            milprintf(f, "item%s := item%s;\n", kind_str(rc), val_join(rc));
        }
        fn_abs (f, code, rc, "ceil");
        return rc;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"floor")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            assert (fun->sig_count == 1); 
            rc = get_kind(fun->sigs[0].ret_ty);
            milprintf(f, "item%s := item%s;\n", kind_str(rc), val_join(rc));
        }
        fn_abs (f, code, rc, "floor");
        return rc;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"round")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            assert (fun->sig_count == 1); 
            rc = get_kind(fun->sigs[0].ret_ty);
            milprintf(f, "item%s := item%s;\n", kind_str(rc), val_join(rc));
        }
        fn_abs (f, code, rc, "round_up");
        return rc;
    }
    /* calculation functions just call an extra function with
       their operator argument to avoid code duplication */
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"plus")))
    {
        return translateOperation (f, code, cur_level, counter, "+", args, false);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"minus")))
    {
        return translateOperation (f, code, cur_level, counter, "-", args, false);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"times")))
    {
        return translateOperation (f, code, cur_level, counter, "*", args, false);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"div")))
    {
        return translateOperation (f, code, cur_level, counter, "/", args, true);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"mod")))
    {
        return translateOperation (f, code, cur_level, counter, "%", args, true);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"idiv")))
    {
        int rcode = (code)?INT:NORMAL;
        /* the semantics of idiv are a normal div operation
           followed by a cast to integer */
        if (!PFty_subtype (TY(L(args)), PFty_xs_integer ()))
        {
            rc = translateOperation (f, VALUES, cur_level, counter, "/", args, true);
            translateCast2INT (f, rcode, rc, TY(L(args)));
            testCastComplete(f, cur_level, PFty_xs_integer ());
        }
        else
            rcode = translateOperation (f, code, cur_level, counter, "/", args, true);
            
        return rcode;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"eq")))
    {
        return translateComparison (f, cur_level, counter, "=", args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"ne")))
    {
        return translateComparison (f, cur_level, counter, "!=", args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"ge")))
    {
        return translateComparison (f, cur_level, counter, ">=", args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"gt")))
    {
        return translateComparison (f, cur_level, counter, ">", args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"le")))
    {
        return translateComparison (f, cur_level, counter, "<=", args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"lt")))
    {
        return translateComparison (f, cur_level, counter, "<", args);
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"node-before")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        counter++;
        saveResult (f, counter);
        translate2MIL (f, NORMAL, cur_level, counter, RL(args));
        milprintf(f,
                "{ # translate op:node-before (node, node) as boolean\n"
                /* FIXME: in theory this should work (in practise it also
                          does with some examples), but it is assumed,
                          that the iter columns are dense and aligned */
                "var cont_before := kind%03u.[<](kind);\n"
                "var cont_equal := kind%03u.[=](kind);\n"
                "var pre_before := item%03u.[<](item);\n"
                "var node_before := cont_before.[or](cont_equal.[and](pre_before));\n"
                "item := node_before.[oid]().tmark(0@0);\n"
                "kind := BOOL;\n"
                "} # end of translate op:node-before (node, node) as boolean\n",
                counter, counter, counter);
        deleteResult (f, counter);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"node-after")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        counter++;
        saveResult (f, counter);
        translate2MIL (f, NORMAL, cur_level, counter, RL(args));
        milprintf(f,
                "{ # translate op:node-after (node, node) as boolean\n"
                /* FIXME: in theory this should work (in practise it also
                          does with some examples), but it is assumed,
                          that the iter columns are dense and aligned */
                "var cont_after := kind%03u.[>](kind);\n"
                "var cont_equal := kind%03u.[=](kind);\n"
                "var pre_after := item%03u.[>](item);\n"
                "var node_after := cont_after[or](cont_equal[and](pre_after));\n"
                "cont_after := nil;\n"
                "cont_equal := nil;\n"
                "pre_after := nil;\n"
                "item := node_after.[oid]().tmark(0@0);\n"
                "kind := BOOL;\n"
                "} # end of translate op:node-after (node, node) as boolean\n",
                counter, counter, counter);
        deleteResult (f, counter);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"is-same-node")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        counter++;
        saveResult (f, counter);
        translate2MIL (f, NORMAL, cur_level, counter, RL(args));
        milprintf(f,
                "{ # translate op:is-same-node (node, node) as boolean\n"
                /* FIXME: in theory this should work (in practise it also
                          does with some examples), but it is assumed,
                          that the iter columns are dense and aligned */
                "var cont_equal := kind%03u.[=](kind);\n"
                "var pre_equal := item%03u.[=](item);\n"
                "var node_equal := cont_equal[and](pre_equal);\n"
                "cont_equal := nil;\n"
                "pre_equal := nil;\n"
                "item := node_equal.[oid]().tmark(0@0);\n"
                "kind := BOOL;\n"
                "} # end of translate op:is-same-node (node, node) as boolean\n",
                counter, counter);
        deleteResult (f, counter);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"true")))
    {
        milprintf(f,
                "{\n"
                "var itemID := 1@0;\n");
        /* translateConst needs a bound variable itemID */
        translateConst(f, cur_level, "BOOL");
        milprintf(f, 
                "}\n");
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"false")))
    {
        milprintf(f,
                "{\n"
                "var itemID := 0@0;\n");
        /* translateConst needs a bound variable itemID */
        translateConst(f, cur_level, "BOOL");
        milprintf(f, 
                "}\n");
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"empty")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                "{ # translate fn:empty (item*) as boolean\n"
                "var iter_count := {count}(iter.reverse(),loop%03u.reverse(), FALSE);\n"
                "var iter_bool := iter_count.[=](0).[oid]();\n"
                "iter_count := nil;\n"
                "item := iter_bool.tmark(0@0);\n"
                "iter_bool := nil;\n"
                "iter := loop%03u.tmark(0@0);\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := BOOL;\n"
                "} # end of translate fn:empty (item*) as boolean\n",
                cur_level, cur_level);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"exists")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                "{ # translate fn:exists (item*) as boolean\n"
                "var iter_count := {count}(iter.reverse(),loop%03u.reverse(), FALSE);\n"
                "var iter_bool := iter_count.[!=](0).[oid]();\n"
                "item := iter_bool.tmark(0@0);\n"
                "iter := loop%03u.tmark(0@0);\n"
                "ipik := iter;\n"
                "pos := 1@0;\n"
                "kind := BOOL;\n"
                "} # end of translate fn:exists (item*) as boolean\n",
                cur_level, cur_level);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,
                         PFqname (PFns_fn,"subsequence")))
    {
        /* get offset */
        if (translate2MIL (f, VALUES, cur_level, counter, RL(args)) == NORMAL)
                milprintf(f, "item%s := item%s;\n", kind_str(DBL), val_join(DBL));
        saveResult_ (f, ++counter, DBL);

        if (fun->arity == 3) { /* get length */
                if( translate2MIL (f, VALUES, cur_level, counter, RRL(args)) == NORMAL) 
                        milprintf(f, "item%s := item%s;\n", kind_str(DBL), val_join(DBL));
                saveResult_ (f, counter+1, DBL);
        }
        /* get main table */
        rc = translate2MIL (f, code, cur_level, ++counter, L(args));
               
        milprintf(f, 
                "{ # translate fn:subsequence\n"
                "if (loop%03u.count() = 1) {\n"
                "    var lo := item_dbl_%03d.fetch(0) - 1.0LL;\n"
                "    if (lo < 1.0LL) lo := 0.0;\n", cur_level, counter-1);
        if (fun->arity == 3)
                milprintf(f, "    var hi := int(lo + item_dbl_%03d.fetch(0)) - 1;\n", counter);
        else 
                milprintf(f, "    var hi := INT_MAX;\n");

        milprintf(f, "\n"
                "    # select a slice\n"
                "    ipik := ipik.slice(int(lo),hi).tmark(0@0);\n"
                "    iter := iter.slice(int(lo),hi).tmark(0@0);\n"
                "    kind := kind.slice(int(lo),hi).tmark(0@0);\n"
                "    item%s := item%s.slice(int(lo), hi).tmark(0@0);\n"
                "    pos := ipik.mark(1@0);\n"
                "} else {\n"
                "    # evaluate selection tuple by tuple (note: fully constant-resistant code)\n"
                "    var offset_dbl := item_dbl_%03d.tmark(1@0);\n"
                "    var offset_oid := leftfetchjoin(iter, [oid](offset_dbl));\n"
                "    var sel := [>=](pos, offset_oid);\n",
                        kind_str(rc), kind_str(rc), counter-1);
        if (fun->arity == 3)
                milprintf(f,
                        "    var limit_dbl := item_dbl_%03d.tmark(1@0);\n"
			"    offset_oid := iter.leftfetchjoin([oid]([+](offset_dbl, limit_dbl)));\n"
                        "    sel := [and](sel, [<](pos, offset_oid));\n", counter);

        milprintf(f, "\n" 
                "    # carry through the selection on the table\n"
                "    if (type(sel) = bat) {\n"
                "        ipik := sel.ord_uselect(true).hmark(0@0);\n"
                "        sel := false;\n"
                "    } else {\n"
                "        if (sel = false) ipik := bat(oid,oid);\n"
                "    }\n"
                "    if (sel = false) {\n"
                "        iter := ipik.leftfetchjoin(iter);\n"
                "        kind := ipik.leftfetchjoin(kind);\n"
                "        item%s := ipik.leftfetchjoin(item%s);\n" 
                "        pos := tmark_grp_unique(iter,ipik);\n"
                "    }\n"
                "}\n", kind_str(rc), kind_str(rc));

        if (fun->arity == 3)
                deleteResult_ (f, counter, DBL);
        deleteResult_ (f, --counter, DBL);

        milprintf(f, "} # end of translate fn:subsequence\n");
        return rc;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"not")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                "# translate fn:not (boolean) as boolean\n"
                "item := item.leftfetchjoin(bool_not);\n"
               );
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"boolean")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        fn_boolean (f, rc, cur_level, TY(L(args)));
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"and")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        counter++;
        saveResult (f, counter);
        translate2MIL (f, NORMAL, cur_level, counter, RL(args));
        milprintf(f,
                "item := item.[int]().[and](item%03u.[int]()).[oid]();\n", counter);
        deleteResult (f, counter);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,PFqname (PFns_op,"or")))
    {
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        counter++;
        saveResult (f, counter);
        translate2MIL (f, NORMAL, cur_level, counter, RL(args));
        milprintf(f,
                "item := item.[int]().[or](item%03u.[int]()).[oid]();\n", counter);
        deleteResult (f, counter);
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname,
                         PFqname (PFns_pf,"join")))
    {
        return evaluate_join (f, code, cur_level, counter, args);
    }
    else if (!PFqname_eq(fnQname, PFqname (PFns_fn,"unordered")))
    {
        milprintf(f, "# ignored fn:unordered\n");
        return translate2MIL (f, code, cur_level, counter, L(args));
    }
    else if (PFqname_eq(fnQname, PFqname (PFns_upd,"delete")) == 0)
    {
        milprintf(f, "# upd:delete (node) as stmt\n");
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                  "var delitemID := int_values.addValues(UPDATE_DELETE);\n"
                  "var ids := mirror(ipik);\n"
                  "var merge1 := merged_union(ids, ids,\n"
                  "                           delitemID, item,\n"
                  "                           INT, kind,\n"
                  "                           iter, iter);\n"
                  "var merge2 := merged_union(ids, ids,\n"
                  "                           oid_nil, oid_nil,\n"
                  "                           int_nil, int_nil,\n"
                  "                           iter, iter);\n"
                  "var merge := merged_union(merge1.fetch(0), merge2.fetch(0),\n"
                  "                          merge1.fetch(1), merge2.fetch(1),\n"
                  "                          merge1.fetch(2), merge2.fetch(2),\n"
                  "                          merge1.fetch(3), merge2.fetch(3));\n"
                  "item := merge.fetch(1);\n"
                  "kind := merge.fetch(2);\n"
                  "iter := merge.fetch(3);\n"
                  "ipik := item;\n");
        return NORMAL;
    }
    else if (PFqname_eq(fnQname, PFqname (PFns_upd,"insertIntoAsFirst")) == 0 ||
             PFqname_eq(fnQname, PFqname (PFns_upd,"insertIntoAsLast")) == 0 ||
             PFqname_eq(fnQname, PFqname (PFns_upd,"insertBefore")) == 0 ||
             PFqname_eq(fnQname, PFqname (PFns_upd,"insertAfter")) == 0 ||
             PFqname_eq(fnQname, PFqname (PFns_upd,"replaceValue")) == 0 ||
             PFqname_eq(fnQname, PFqname (PFns_upd,"replaceElementContent")) == 0 ||
             PFqname_eq(fnQname, PFqname (PFns_upd,"rename")) == 0)
    {
        char *func = PFqname_loc(fnQname);
        char *update_cmd = NULL, *arg = "node";

        if (strcmp(func, "insertIntoAsLast") == 0)
            update_cmd = "UPDATE_INSERT_LAST";
        else if (strcmp(func, "insertIntoAsFirst") == 0)
            update_cmd = "UPDATE_INSERT_FIRST";
        else if (strcmp(func, "insertBefore") == 0)
            update_cmd = "UPDATE_INSERT_BEFORE";
        else if (strcmp(func, "insertAfter") == 0)
            update_cmd = "UPDATE_INSERT_AFTER";
        else if (strcmp(func, "replaceValue") == 0) {
            update_cmd = "UPDATE_REPLACE";
            arg = "str";
        } else if (strcmp(func, "replaceElementContent") == 0) {
            update_cmd = "UPDATE_REPLACECONTENT";
            arg = "str";
        } else if (strcmp(func, "rename") == 0) {
            update_cmd = "UPDATE_RENAME";
            arg = "str";
        }

        milprintf(f, "# upd:%s (node, %s) as stmt\n", func, arg);
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        counter++;
        saveResult (f, counter);
        translate2MIL (f, NORMAL, cur_level, counter, RL(args));
        if (strcmp(func, "insertIntoAsFirst") == 0 || strcmp(func, "insertAfter") == 0) {
            /* reverse the order of items within an iter for these functions so that they get inserted in the correct order */
            milprintf(f,
                      "if (count(tunique(iter)) != count(ipik)) {\n"
                      "  var revmap := iter.materialize(ipik).copy().access(BAT_WRITE).revert().reverse().ssort().reverse();\n"
                      "  item := revmap.hmark(0@0).leftfetchjoin(item);\n"
                      "  kind := revmap.hmark(0@0).leftfetchjoin(kind);\n"
                      "  iter := revmap.tmark(0@0);\n"
                      "}\n");
        }
        milprintf(f,
                  "var cmdID := int_values.addValues(%s);\n"
                  "var map := iter.materialize(ipik).leftjoin(iter%03u.reverse()).tmark(0@0);\n"
                  "kind%03u := map.leftfetchjoin(kind%03u);\n"
                  "item%03u := map.leftfetchjoin(item%03u);\n"

                  "var id1 := ipik.mirror();\n"
                  "var merge1 := merged_union(id1, id1,\n"
                  "                           cmdID, item%03u,\n"
                  "                           INT, kind%03u,\n"
                  "                           iter, iter);\n"
                  "var merge2 := merged_union(id1, id1,\n"
                  "                           item, oid_nil,\n"
                  "                           kind, int_nil,\n"
                  "                           iter, iter);\n"
                  "var merge := merged_union(merge1.fetch(0), merge2.fetch(0),\n"
                  "                          merge1.fetch(1), merge2.fetch(1),\n"
                  "                          merge1.fetch(2), merge2.fetch(2),\n"
                  "                          merge1.fetch(3), merge2.fetch(3));\n"
                  "item := merge.fetch(1);\n"
                  "kind := merge.fetch(2);\n"
                  "iter := merge.fetch(3);\n"
                  "ipik := item;\n",
                  update_cmd, counter, counter, counter, counter, counter, counter, counter);
        deleteResult (f, counter);
        return NORMAL;
    }
    else if (PFqname_eq(fnQname, PFqname (PFns_lib,"add-doc")) == 0)
    {
        milprintf(f, "# pf:add-doc (str, str [, str] [, int]) as add_stmt\n");
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        counter++;
        saveResult(f, counter);
        translate2MIL(f, NORMAL, cur_level, counter, RL(args));
        counter++;
        saveResult(f, counter);
        if (fun->arity == 2) {
            counter++;
            milprintf(f,
                      "# collection is default same as document name\n"
                      "var item%03u := item%03u;\n"
                      "var kind%03u := kind%03u;\n"
                      "# percentage default is 0\n"
                      "item := int_values.addValues(0LL);\n"
                      "kind := INT;\n",
                      counter, counter - 1, counter, counter - 1);
        } else {
            translate2MIL(f, NORMAL, cur_level, counter, RRL(args));
            counter++;
            saveResult(f, counter);
            if (fun->arity == 3) {
                milprintf(f,
                          "if (kind%03u.fetch(0) = INT) {\n"
                          "  # collection was omitted, but percentage provided\n"
                          "  kind := kind%03u;\n"
                          "  item := item%03u;\n"
                          "  kind%03u := kind%03u;\n"
                          "  item%03u := item%03u;\n"
                          "} else {\n"
                          "  item := int_values.addValues(0LL);\n"
                          "  kind := INT;\n"
                          "}\n",
                          counter, counter, counter, counter, counter - 1, counter, counter - 1);
            } else {
                translate2MIL(f, NORMAL, cur_level, counter, RRRL(args));
            }
        }
        milprintf(f,
                  "var merge1 := merged_union(ipik.mirror(), ipik.mirror(),\n"
                  "                           item%03u, item%03u,\n"
                  "                           kind%03u, kind%03u,\n"
                  "                           iter, iter);\n"
                  "var merge2 := merged_union(ipik.mirror(), ipik.mirror(),\n"
                  "                           item%03u, item,\n"
                  "                           kind%03u, kind,\n"
                  "                           iter, iter);\n"
                  "var merge := merged_union(merge1.fetch(0), merge2.fetch(0),\n"
                  "                          merge1.fetch(1), merge2.fetch(1),\n"
                  "                          merge1.fetch(2), merge2.fetch(2),\n"
                  "                          merge1.fetch(3), merge2.fetch(3));\n"
                  "item := merge.fetch(1);\n"
                  "kind := merge.fetch(2);\n"
                  "iter := merge.fetch(3);\n"
                  "ipik := item;\n",
                  counter - 2, counter - 1, counter - 2, counter - 1, counter, counter);
        if (fun->arity > 2) {
            deleteResult(f, counter);
        }
        deleteResult(f, counter - 1);
        deleteResult(f, counter - 2);
        return NORMAL;
    }
    else if (PFqname_eq(fnQname, PFqname (PFns_lib,"del-doc")) == 0)
    {
        milprintf(f, "# pf:del-doc (str) as del_stmt\n");
        translate2MIL (f, NORMAL, cur_level, counter, L(args));
        milprintf(f,
                  "var delID := int_values.addValues(-1LL);\n"
                  "var ids := mirror(ipik);\n"
                  "var merge1 := merged_union(ids, ids,\n"
                  "                           item, item,\n"
                  "                           kind, kind,\n"
                  "                           iter, iter);\n"
                  "var merge2 := merged_union(ids, ids,\n"
                  "                           item, delID,\n"
                  "                           kind, INT,\n"
                  "                           iter, iter);\n"
                  "var merge := merged_union(merge1.fetch(0), merge2.fetch(0),\n"
                  "                          merge1.fetch(1), merge2.fetch(1),\n"
                  "                          merge1.fetch(2), merge2.fetch(2),\n"
                  "                          merge1.fetch(3), merge2.fetch(3));\n"
                  "item := merge.fetch(1);\n"
                  "kind := merge.fetch(2);\n"
                  "iter := merge.fetch(3);\n"
                  "ipik := item;\n");
        return NORMAL;
    }
    else if (!PFqname_eq(fnQname, PFqname (PFns_fn,"error")))
    {
        if (fun->arity == 0)
        {
            milprintf(f, "ERROR (\"err:FOER0000\");\n");
            return NORMAL;
        }

        item_ext = kind_str(STR);

        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
        }
        milprintf(f,
                "var item_result;\n"
                "if (iter.count() != loop%03u.count())\n"
                "{ # add zero values needed for fn:sum\n"
                "var difference := reverse(loop%03u.tdiff(iter));\n"
                "difference := difference.hmark(0@0);\n"
                "var res_mu := merged_union(iter, difference, item%s, \"err:FOER0000\");\n"
                "item%s := res_mu.fetch(1);\n" /* CONST? */
                "}\n",
                cur_level, 
                cur_level,
                item_ext,
                item_ext);

        if (fun->arity == 2)
        {
            counter++;
            saveResult_ (f, counter, STR);

            rc = translate2MIL (f, VALUES, cur_level, counter, RL(args));
            if (rc == NORMAL)
            {
                milprintf(f, "item%s := item%s;\n", item_ext, val_join(STR));
            }
            milprintf(f, 
                    "ERROR(item%s%03u.fetch(0) + \": \" + item%s.fetch(0));\n",
                    item_ext, counter, item_ext);
            deleteResult_ (f, counter, STR);
        }
        else
        {
            milprintf(f, 
                    "ERROR(item%s.fetch(0));\n",
                    item_ext);
        }
        return NORMAL;
    }
#ifdef HAVE_PFTIJAH
    else if (
          ( !PFqname_eq(fnQname, PFqname (PFns_tijah,"create-ft-index"))) ||
          ( !PFqname_eq(fnQname, PFqname (PFns_tijah,"delete-ft-index")))
	)
    {
	int is_delete   = !PFqname_eq(fnQname, PFqname (PFns_tijah,"delete-ft-index"));
        int opt_counter  = 0;
        int opt_used     = 0;
        int csq_counter  = 0;
        int csq_used     = 0;
	int str_counter  = 0;
        char *item_ext = kind_str(STR);
	
	milprintf(f, 
                "{ # translate pf:tijah-create-collection\n"
	);
        /* get ft-index name string */
	rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
             milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
        add_empty_strings (f, STR, cur_level);
        saveResult_ (f, ++counter, STR);
	str_counter = counter;

        /* now check how the rest of the fun is overloaded */
        if ( fun->arity > 1 ) {
	    if ( fun->arity == 2 ) {
                if ( (TY_EQ(TY(RL(args)), PFty_star(PFty_xs_string()))) ||
		      (TY_EQ(TY(RL(args)), PFty_xs_string())) ) {
	            /* check if the type of the 2nd arg is the collseq */
	    	     csq_used = 1;
	        } else {
	    	     opt_used = 1;
		}
	    } else {
	    	opt_used = csq_used = 1;
	    }
	}

	if ( opt_used ) {
	  /* translate the options parameter*/
          if ( fun->arity == 3 )
	      translate2MIL (f, NORMAL, cur_level, counter, RRL(args));
	  else
	      translate2MIL (f, NORMAL, cur_level, counter, RL(args));
	  saveResult(f, ++counter);
	  opt_counter = counter;
        } else {
	  opt_counter = 0;
	}

	if ( csq_used ) {
	  /* translate the collection sequence parameter */
          translate2MIL (f, NORMAL, cur_level, counter, RL(args));
	  saveResult(f, ++counter);
	  csq_counter = counter;
        } else {
	  csq_counter = 0;
	}

	/* generate the serialization code */
	milprintf(f,
	        "loop%03u@batloop() { # begin batloop over collection creator\n"
                ,cur_level);
        if ( csq_counter ) {
	    milprintf(f,
                "var csqiter := iter%03u.materialize(ipik%03u).uselect(oid($t));\n"
                "var csqitem := item%03u.materialize(ipik%03u);\n"
		"var pfcollnm := csqiter.reverse().leftfetchjoin(csqitem).leftfetchjoin(str_values).tmark(0@0);\n"
		,csq_counter,csq_counter,csq_counter,csq_counter);
  
	} else {
	    milprintf(f,
	        "    var pfcollnm := new(void,str);\n"
		"    pfcollnm.append(\"*\");\n"
	    );
	}

	if ( opt_counter ) {
	    milprintf(f,
	        "    iter := iter%03u.select($t);\n"
	        "    item := item%03u.materialize(ipik%03u).semijoin(iter);\n"
	        "    kind := kind%03u.materialize(ipik%03u).semijoin(iter);\n"
	        "    iter := iter.tmark(0@0);\n"
	        "    item := item.tmark(0@0);\n"
	        "    kind := kind.tmark(0@0);\n"
		,opt_counter,opt_counter, opt_counter,opt_counter,opt_counter);
	    milprintf(f, "    var optbat := serialize_tijah_opt(ws,1,iter,iter,item,kind,int_values,dbl_values,str_values);\n");
	} else {
		milprintf(f, "    var optbat := new(str,str);\n");
	}
	/* execute tj_init_collection */
	milprintf(f,
		"    var cname := item%s%03u.fetch(int($h));\n"
		, item_ext, str_counter);
	if ( is_delete ) {
	  milprintf(f,
                "    tj_delete_collection(cname);\n"
		"}\n");
	} else {
	  milprintf(f,
#if 0
		"    optbat.print();\n"
		"    pfcollnm.print();\n"
#endif
                "    tj_init_collection(cname,optbat,pfcollnm);\n"
		"}\n");
	}

	translateEmpty(f);
	
        deleteResult_ (f, str_counter, STR);
	if ( opt_counter )
	    deleteResult(f, opt_counter);
	if ( csq_counter )
	    deleteResult(f, csq_counter);
        milprintf(f, "} # end of translate pf:tijah-ft-index\n");
        return (code)?INT:NORMAL;
    }
    else if (
          ( !PFqname_eq(fnQname, PFqname (PFns_lib,"tijah-query"))) ||
          ( !PFqname_eq(fnQname, PFqname (PFns_lib,"tijah-query-id")))
	)
    {
        int opt_counter  = 0;
	int str_counter  = 0;
	int ctx_counter  = 0;
        char *item_ext = kind_str(STR);
	
	int storeScore = !PFqname_eq(fnQname, PFqname (PFns_lib,"tijah-query-id"));
	milprintf(f, 
                "{ # translate pf:tijah-query\n"
	);
        if (fun->arity == 3) {
	  /* translate the first options parameter*/
          translate2MIL (f, NORMAL, cur_level, counter, L(args));
	  saveResult(f, ++counter);
	  opt_counter = counter;

          /* get query string */
	  rc = translate2MIL (f, VALUES, cur_level, counter, RRL(args));
          if (rc == NORMAL)
             milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
          add_empty_strings (f, STR, cur_level);
          saveResult_ (f, ++counter, STR);
	  str_counter = counter;
	  
	  /* get nodes to start from */ 
	  translate2MIL (f, code, cur_level, ++counter, RL(args));
	  saveResult(f, ++counter);
	  ctx_counter = counter;
	
	} else {
          /* get query string */
	  rc = translate2MIL (f, VALUES, cur_level, counter, RL(args));
          if (rc == NORMAL)
             milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
          add_empty_strings (f, STR, cur_level);
          saveResult_ (f, ++counter, STR);
	  str_counter = counter;
	  
	  /* get nodes to start from */ 
	  translate2MIL (f, code, cur_level, ++counter, L(args));
	  saveResult(f, ++counter);
	  ctx_counter = counter;
        }

	if ( storeScore )
	  milprintf(f,
	        "var result_id := new(void,lng).seqbase(0@0);");
        else
	  milprintf(f,
	        "var result_iter := new(void,oid).seqbase(0@0);"
	        "var result_item := new(void,oid).seqbase(0@0);"
	        "var result_pos := new(void,oid).seqbase(0@0);"
	        "var result_frag := new(void,oid).seqbase(0@0);");
	
	/* generate the serialization code */
	milprintf(f,
	        "loop%03u@batloop() { # begin batloop over queries\n"
                , cur_level);
	if (opt_counter)
	  milprintf(f,
	        "    iter := iter%03u.select($t);\n"
	        "    item := item%03u.materialize(ipik%03u).semijoin(iter);\n"
	        "    kind := kind%03u.materialize(ipik%03u).semijoin(iter);\n"
	        "    iter := iter.tmark(0@0);\n"
	        "    item := item.tmark(0@0);\n"
	        "    kind := kind.tmark(0@0);\n"
		"    var optbat := serialize_tijah_opt(ws,1,iter,iter,item,kind,int_values,dbl_values,str_values);\n"
		,opt_counter,opt_counter, opt_counter,opt_counter,opt_counter);
	else  
          milprintf(f, 
                "    var optbat := new(str,str,32);\n");

	  milprintf(f,
		"    var coll := collName;\n"
		"    if ( optbat.exist(\"collection\") ) { coll := optbat.find(\"collection\"); }\n"
		"    tijah_lock := tj_get_collection_lock(coll);\n"
		"    lock_set(tijah_lock);\n"
		"    var startNodes;\n"
	        "    iter := iter%03u.materialize(ipik%03u);\n"
		"    if (iter.count() > 0) {\n"  
		"        var iteration := iter%03u.fetch(int($h));\n"  
	        "        iter := iter.select(iteration);\n"
		"        iteration := nil;\n"
	        "        item := item%03u.materialize(ipik%03u).semijoin(iter);\n"
	        "        kind := kind%03u.materialize(ipik%03u).semijoin(iter);\n"
	        "        iter := iter.tmark(0@0);\n"
	        "        item := item.tmark(0@0);\n"
	        "        kind := kind.tmark(0@0);\n"
		"        var xdoc_name := bat(\"tj_\" + coll + \"_doc_name\");\n"
		"        var xdoc_firstpre := bat(\"tj_\" + coll + \"_doc_firstpre\");\n"
		"        var xpfpre := bat(\"tj_\" + coll + \"_pfpre\");\n"
		"        var doc_loaded := reverse(ws.fetch(OPEN_CONT)).leftfetchjoin(ws.fetch(OPEN_NAME));\n"
                "        startNodes := pf2tijah_node(xdoc_name,xdoc_firstpre,xpfpre,item,kind,doc_loaded);\n"
	        "    } else {\n"
                "    startNodes := new(void,oid); }\n"
		, ctx_counter, ctx_counter, str_counter, ctx_counter, ctx_counter, ctx_counter, ctx_counter);
          
	/* execute tijah query */
	milprintf(f,
                "    var nexi_allscores := run_tijah_query(optbat,startNodes,item%s%03u.fetch(int($h)));\n"
		"    var nexi_score;\n"
		"    if ( optbat.exist(\"returnNumber\") ) {\n"
		"        var retNum := int(optbat.find(\"returnNumber\"));\n"
		"        nexi_score := nexi_allscores.slice(0, retNum - 1);\n"
		"    } else {\n"
		"        nexi_score := nexi_allscores;\n"
		"    }\n"
		, item_ext, str_counter);
	
	    /* translate tijah-pre to pf-pre */
	    milprintf(f,
                "    var docpre := bat(\"tj_\" + collName + \"_doc_firstpre\").[oid]();\n"
                "    var pfpre :=  bat(\"tj_\" + collName + \"_pfpre\");\n"
                "    item  := nexi_score.hmark(0@0);\n"
                "    var frag := [find_lower](const docpre.reverse().mark(0@0), item);\n"
                "    item := item.join(pfpre).sort().tmark();\n"
                "    var needed_docs := bat(\"tj_\" + collName + \"_doc_name\").semijoin(frag.tunique());\n"
		"    lock_unset(tijah_lock); tijah_lock := lock_nil;\n"
                "    var loaded_docs := ws.fetch(OPEN_NAME).reverse();\n"
                "    var docs_to_load := kdiff(needed_docs.reverse(),loaded_docs).hmark(0@0);\n"
		"    ws_opendoc(ws, docs_to_load);\n"
		"    docs_to_load := nil;\n"
		"    loaded_docs := nil;\n"
		"    var doc_loaded := reverse(ws.fetch(OPEN_CONT)).leftfetchjoin(ws.fetch(OPEN_NAME));\n"
                "    var fid_pffid := needed_docs.join(doc_loaded.reverse());\n"
                "    frag := frag.join(fid_pffid).sort().tmark();\n");

	if ( storeScore ) {
	    /* store scores and nodes */
	    milprintf(f,
	        "    tID := oid(int(tID) + 1);\n" 
		"    tijah_resultsz.insert(lng(tID),lng(nexi_allscores.count()));\n"
	        "    tijah_tID.append(item.project(tID));\n"
	        "    tijah_frag.append(frag);\n"
	        "    tijah_pre.append(item);\n"
	        "    tijah_score.append(nexi_score.tmark());\n"
		"    result_id.append(lng(tID));\n"
		"} # end batloop over queries\n");

	    /* return query identifier */
            item_ext = (code)?kind_str(INT):"";
            if (code)
                milprintf(f, "item%s := result_id;\n", item_ext);
            else 
                addValues (f, int_container(), "result_id", "item");

            milprintf(f,
                "iter := loop%03u.tmark(oid(0));\n"
                "ipik := iter;\n"
                "pos := oid(1);\n"
                "kind := INT;\n"
                , cur_level);
	} else {
	    /* do not store score, return nodes instead */
	    milprintf(f,
	        "    result_iter.append(item.project($t));\n"
	        "    result_pos.append(item.mark(1@0));\n"
	        "    result_frag.append(frag);\n"
	        "    result_item.append(item);\n"
		"} # end batloop over queries\n");
	    milprintf(f,
	        "iter := result_iter;\n"
		"result_iter := nil;\n"
	        "pos := result_pos;\n"
		"result_pos := nil;\n"
	        "kind := set_kind(result_frag, ELEM);\n"
		"result_frag := nil;\n"
	        "item := result_item;\n"
		"result_item := nil;\n"
                "ipik := iter;\n"
		);
	}
	
	/* clean up */
	deleteResult(f, ctx_counter);
        deleteResult_ (f, str_counter, STR);
	if ( opt_counter )
	    deleteResult(f, opt_counter);
        milprintf(f, "} # end of translate pf:tijah_query\n");
        return (code && storeScore)?INT:NORMAL;
    }
    else if ( !PFqname_eq(fnQname, PFqname (PFns_lib,"tijah-nodes")) )
    {
        char *item_int = kind_str(INT);
        
	milprintf(f, 
                "{ # translate pf:tijah-nodes\n"
	);
        /* get query id */
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            milprintf(f, "item%s := item%s;\n", item_int, val_join(INT));
        }
        
	/* get nodes and create iter|item|.... */
	milprintf(f,
		"item := new(void,oid).seqbase(0@0);"
		"iter := new(void,oid).seqbase(0@0);"
		"pos := new(void,oid).seqbase(0@0);"
		"var frag := new(void,oid).seqbase(0@0);"
		"loop%03u@batloop() { # begin of query batloop\n"
                "    var qid := oid(item%s.fetch(int($h)));\n"
		"    var tmp := tijah_tID.ord_uselect(qid);\n"
		"    item.append(tmp.mirror().leftfetchjoin(tijah_pre));\n"
		"    iter.append(tmp.project(loop%03u.fetch(int($h))));\n"
		"    frag.append(tmp.mirror().leftfetchjoin(tijah_frag));\n"
	        "    pos.append(tmp.mark(1@0));\n"
	        "} # end of query batloop \n"
	        , cur_level, item_int, cur_level);
        
	milprintf(f,
                "kind := set_kind(frag, ELEM);\n"
                "ipik := iter;\n"
	        "} # end of translate pf:tijah_nodes\n");
        
	return NORMAL;
    }
    else if ( !PFqname_eq(fnQname, PFqname (PFns_lib,"tijah-score")) )
    {
        char *item_int = kind_str(INT);
        milprintf(f, 
                "{ # translate pf:tijah-score\n"
	);
        /* get query id */
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
	if (rc == NORMAL)
	{
	    milprintf(f, "item%s := item%s;\n", item_int, val_join(INT));
	}
	saveResult_(f, ++counter, INT);
        
	/* get node */
        rc = translate2MIL (f, code, cur_level, counter, RL(args));
        
	/* get scores */
        milprintf(f, 
		"var score := new(oid,dbl);\n"
		"var tmp := [<<]([lng](tijah_frag), const 32);\n"
		"var tijah_fragpre := [+](tmp, [lng](tijah_pre));\n"
		"tmp := nil;\n"
		"var item1_unique := item%s%03u.tunique();\n"
		"item := item.materialize(ipik);"
		"kind := kind.materialize(ipik);"
                "item1_unique@batloop() { # begin query batloop\n"
		"    var item_part := item.semijoin(item%s%03u.uselect($h));\n"
		"    var frag_part := kind.semijoin(item_part).get_container();\n"
		"    frag_part := [<<]([lng](frag_part), const 32);\n"
		"    var fragpre_part := [+](frag_part, [lng](item_part));\n"
		"    item_part := nil;\n"
		"    frag_part := nil;\n"
		"    tmp := tijah_tID.uselect(oid($h));\n"
		"    tmp := tmp.mirror().leftfetchjoin(tijah_fragpre);\n"
		"    tmp := tmp.join(fragpre_part.reverse());\n"
		"    score.insert(tmp.reverse().leftfetchjoin(tijah_score));\n"
		"} # end query batloop\n"
		"var xitem := kdiff(item,score).project(dbl(0));\n"
		"score.insert(xitem);\n"
		"xitem := nil;\n"
		"score := score.sort().tmark(0@0);\n"
		, item_int, counter, item_int, counter);
	
	/* return score */
        item_ext = (code)?kind_str(DBL):"";
        if (code)
            milprintf(f, "item%s := score;\n", item_ext);
        else 
            addValues (f, dbl_container(), "score", "item");

        milprintf(f,
                "iter := loop%03u.tmark(0@0);\n"
	        "ipik := iter;\n"
		"score := nil;\n"
                "pos := 1@0;\n"
		"kind := DBL;\n"
		, cur_level);
        
	/* clean up */
	deleteResult_(f, counter, INT);
        milprintf(f, "} # end of translate pf:tijah-score\n");
        return (code)?DBL:NORMAL;
    }
    else if ( !PFqname_eq(fnQname, PFqname (PFns_lib,"tijah-tokenize")) )
    {
        char *item_ext;
        int str_counter, rc;

        item_ext = kind_str(STR);

        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
            milprintf(f, "item%s := item.leftfetchjoin(str_values);\n", item_ext);
        add_empty_strings (f, STR, cur_level);
        counter++;
        str_counter = counter;
        saveResult_ (f, str_counter, STR);

        milprintf(f,
                "{ # pf:tijah-tokenize\n"
                "var res := [tijah_tokenize](item%s%03u);\n",
                item_ext, str_counter);

        if (code)
            milprintf(f, "item%s := res;\n", item_ext);
        else
            addValues (f, str_container(), "res", "item");

        item_ext = (code)?item_ext:"";
        milprintf(f,
            "iter := loop%03u;\n"
            "ipik := iter;\n"
            "pos := 1@0;\n"
            "kind := STR;\n"
            "} # end of pf:tijah-tokenize\n",
            cur_level);

        deleteResult_ (f, str_counter, STR);
	counter--;
        return (code)?STR:NORMAL;
    } else if (!PFqname_eq(fnQname,PFqname (PFns_lib,"tijah-resultsize")))
    {
        rc = translate2MIL (f, VALUES, cur_level, counter, L(args));
        if (rc == NORMAL)
        {
            assert (fun->sig_count == 1); 
            rc = get_kind(fun->sigs[0].ret_ty);
            milprintf(f, "item%s := item%s;\n", kind_str(rc), val_join(rc));
        }
        char *item_ext = kind_str(rc);
        /* because functions are only allowed for dbl
           we need to cast integers */
        type_co t_co = kind_container(rc);
    
        milprintf(f,
                "if (ipik.count() != 0) { # pf:tijah-resultsize\n"
                "var res := item%s.join(tijah_resultsz);\n"
		,item_ext);
        if (code)
            milprintf(f, "item%s := res;\n", item_ext);
        else 
            addValues (f, t_co, "res", "item");
    
        item_ext = (code)?item_ext:"";
        milprintf(f, "} # end of pf:tijah-resultsize\n");
        return rc;
    }
#endif /* PFTIJAH */
    PFoops(OOPS_FATAL,"function %s is not supported.", PFqname_str (fnQname));
    milprintf(f,
                "# empty intermediate result "
                "instead of unsupported function %s\n",
                PFqname_str (fnQname));
    translateEmpty (f);
    return NORMAL;
}

/**
 * translate2MIL prints the MIL-expressions, for the following
 * core nodes:
 * c_var, c_seq, c_for, c_let
 * c_lit_str, c_lit_dec, c_lit_dbl, c_lit_int
 * c_empty, c_true, c_false
 * c_locsteps, (axis: c_ancestor, ...)
 * (tests: c_namet, c_kind_node, ...)
 * c_if,
 * (constructors: c_elem, ...)
 * c_typesw, c_cases, c_case, c_seqtype, c_seqcast
 * c_nil
 * c_apply, c_arg,
 *
 * the following list is not supported so far:
 * c_error
 *
 * @param f the Stream the MIL code is printed to
 * @param code the number indicating, which result interface is preferred
 * @param cur_level the level of the for-scope
 * @param counter the actual offset of saved variables
 * @param c the Core node containing the rest of the subtree
 * @result the kind indicating, which result interface is chosen
 */
static int
translate2MIL (opt_t *f, int code, int cur_level, int counter, PFcnode_t *c)
{
    char *descending;
    int rc=NORMAL, rcode;

    assert(c);
    switch (c->kind)
    {
        case c_main:
            translate2MIL (f, code, cur_level, counter, L(c));
            rc = translate2MIL (f, code, cur_level, counter, R(c));
            break;
        case c_empty:
            if (code == NODE)
            {
                translateEmpty_node (f);
                rc = NODE;
            }
            else
            {
                translateEmpty (f);
                rc = NORMAL;
            }
            break;
        case c_lit_str:
            if (code == VALUES)
            {
                rc = STR;
                milprintf(f,
                        "iter := loop%03u.tmark(0@0);\n"
                        "ipik := iter;\n"
                        "pos := 1@0;\n"
                        "item%s := \"%s\";\n"
                        "kind := STR;\n",
                        cur_level, kind_str(rc), 
                        PFesc_string (c->sem.str));
            }
            else
            {
                rc = NORMAL;
                milprintf(f,
                        "{\n"
                        "str_values.append(\"%s\");\n"
                        "var itemID := str_values.reverse().find(\"%s\");\n",
                        PFesc_string (c->sem.str),
                        PFesc_string (c->sem.str));
                /* translateConst needs a bound variable itemID */
                translateConst(f, cur_level, "STR");
                milprintf(f, 
                        "}\n");
            }
            break;
        case c_lit_int:
            if (code == VALUES)
            {
                rc = INT;
                milprintf(f,
                        "iter := loop%03u.tmark(0@0);\n"
                        "ipik := iter;\n"
                        "pos := 1@0;\n"
                        "item%s := " LLFMT "LL;\n"
                        "kind := INT;\n",
                        cur_level, kind_str(rc), 
                        c->sem.num);
            }
            else
            {
                rc = NORMAL;
                milprintf(f,
                        "{\n"
                        "int_values.append(" LLFMT "LL);\n"
                        "var itemID := int_values.reverse().find(" LLFMT "LL);\n",
                        c->sem.num, c->sem.num);
                /* translateConst needs a bound variable itemID */
                translateConst(f, cur_level, "INT");
                milprintf(f, 
                        "}\n");
            }
            break;
        case c_lit_dec:
            if (code == VALUES)
            {
                rc = DEC;
                milprintf(f,
                        "iter := loop%03u.tmark(0@0);\n"
                        "ipik := iter;\n"
                        "pos := 1@0;\n"
                        "item%s := dbl(%gLL);\n"
                        "kind := DEC;\n",
                        cur_level, kind_str(rc), 
                        c->sem.dec);
            }
            else
            {
                rc = NORMAL;
                milprintf(f,
                        "{\n"
                        "dec_values.append(dbl(%gLL));\n"
                        "var itemID := dec_values.reverse().find(dbl(%gLL));\n",
                        c->sem.dec, c->sem.dec);
                /* translateConst needs a bound variable itemID */
                translateConst(f, cur_level, "DEC");
                milprintf(f, 
                        "}\n");
            }
            break;
        case c_lit_dbl:
            if (code == VALUES)
            {
                rc = DBL;
                milprintf(f,
                        "iter := loop%03u.tmark(0@0);\n"
                        "ipik := iter;\n"
                        "pos := 1@0;\n"
                        "item%s := dbl(%gLL);\n"
                        "kind := DBL;\n",
                        cur_level, kind_str(rc), 
                        c->sem.dbl);
            }
            else
            {
                rc = NORMAL;
                milprintf(f,
                        "{\n"
                        "dbl_values.append(dbl(%gLL));\n"
                        "var itemID := dbl_values.reverse().find(dbl(%gLL));\n",
                        c->sem.dbl, c->sem.dbl);
                /* translateConst needs a bound variable itemID */
                translateConst(f, cur_level, "DBL");
                milprintf(f, 
                        "}\n");
            }
            break;
        case c_true:
            milprintf(f,
                    "{\n"
                    "var itemID := 1@0;\n");
            /* translateConst needs a bound variable itemID */
            translateConst(f, cur_level, "BOOL");
            milprintf(f, 
                    "}\n");
            rc = NORMAL;
            break;
        case c_false:
            milprintf(f,
                    "{\n"
                    "var itemID := 0@0;\n");
            /* translateConst needs a bound variable itemID */
            translateConst(f, cur_level, "BOOL");
            milprintf(f, 
                    "}\n");
            rc = NORMAL;
            break;
        case c_seq:
            rc = translateSequence (f, code, cur_level, counter, c);
            break;
        case c_var:
            translateVar(f, cur_level, c->sem.var);
            rc = NORMAL;
            break;
        case c_let:
            if (LL(c)->sem.var->used)
            {
                translate2MIL (f, NORMAL, cur_level, counter, LR(c));
                insertVar (f, cur_level, LL(c)->sem.var->vid);
            }

            rc = translate2MIL (f, code, cur_level, counter, R(c));
            break;
        case c_for:
            translate2MIL (f, NORMAL, cur_level, counter, LR(c));
    	    add_flwr_level(f, cur_level);
            {
                 PFcnode_t *n = R(c);
                 while(n && n->kind == c_let) n = R(n);
		 if (n && n->kind == c_orderby) {
                     f->flwr_level[cur_level] = cur_level; /* this for started the flwro block */
                 }
            }
            cur_level++;

            /* not allowed to overwrite iter,pos,item */
            milprintf(f, "if (ipik.count() != 0)\n");
            milprintf(f, "{  # for-translation\n");
            project (f, cur_level); 

            milprintf(f, "var expOid;\n");
            getExpanded (f, cur_level, c->sem.flwr.fid);
            milprintf(f,
                    "if (expOid.count() != 0) {\n"
                    "var oidNew_expOid;\n");
                    expand (f, cur_level);
                    join (f, cur_level);
            milprintf(f, "} else {\n");
                    createNewVarTable (f, cur_level);
            milprintf(f, 
                    "}  # end if\n"
                    "expOid := nil;\n");

            if (LLL(c)->sem.var->used)
                insertVar (f, cur_level, LLL(c)->sem.var->vid);
            if ((LLR(c)->kind == c_var)
                && (LLR(c)->sem.var->used))
            {
                /* changes item and kind and inserts if needed
                   new int values to 'int_values' bat */
                createEnumeration (f, cur_level);
                insertVar (f, cur_level, LLR(c)->sem.var->vid);
            }
            /* end of not allowed to overwrite iter,pos,item */

            rc = translate2MIL (f, code, cur_level, counter, R(c));
            
            mapBack (f, cur_level);
            cleanUpLevel (f, cur_level);
            milprintf(f, "}  # end of for-translation\n");
            break;
        case c_if:
            translate2MIL (f, NORMAL, cur_level, counter, L(c));
            rc = translateIfThenElse (f, code, cur_level, counter, RL(c), RR(c));
            break;
        case c_typesw:
            translate2MIL (f, NORMAL, cur_level, counter, L(c));
            translateTypeswitch (f, cur_level, TY(L(c)), TY(RLL(c)));
            rc = translateIfThenElse (f, code, cur_level, counter, RLR(c), RRL(c));
            break;
        case c_locsteps:
            /* ignore result code if it's not ITEM_ORDER */
            rc = (code == ITEM_ORDER)?ITEM_ORDER:NORMAL;

            /* avoid in-/output in item order for
               self and attribute axes */
            if ((L(c)->kind == c_child ||
                 L(c)->kind == c_descendant ||
                 L(c)->kind == c_descendant_or_self) &&
                R(c)->kind == c_locsteps &&
                (RL(c)->kind == c_child ||
                 RL(c)->kind == c_descendant ||
                 RL(c)->kind == c_descendant_or_self))
            {
                translate2MIL (f, ITEM_ORDER, cur_level, counter, R(c));
                translateLocsteps (f, ITEM_ORDER, rc, L(c));
            }
            else
            {
                translate2MIL (f, NORMAL, cur_level, counter, R(c));
                translateLocsteps (f, NORMAL, rc, L(c));
            }
            /* rc = rc; */
            break;
        case c_elem:
            rcode = (code == NODE)?NODE:NORMAL;

            rc = translate2MIL (f, VALUES, cur_level, counter, L(c));
            if (L(c)->kind != c_tag)
            {
                castQName (f, rc);
            }
            counter++;
            saveResult (f, counter);

            rc = translateElemContent (f, NODE, cur_level, counter, R(c));
            if (rc == NORMAL&& rcode == NORMAL)
                loop_liftedElemConstr (f, NORMAL, NORMAL, counter);
            else if (rc == NORMAL)
            {
                map2NODE_interface (f);
                loop_liftedElemConstr (f, rcode, NODE, counter);
            }
            else
                loop_liftedElemConstr (f, rcode, NODE, counter);

            deleteResult (f, counter);
            rc = rcode;
            break;
        case c_attr:
            rcode = (code == NODE)?NODE:NORMAL;

            rc = translate2MIL (f, VALUES, cur_level, counter, L(c));
            if (L(c)->kind != c_tag)
            {
                castQName (f, rc);
            }

            counter++;
            saveResult (f, counter);

            rc = translate2MIL (f, VALUES, cur_level, counter, R(c));

            loop_liftedAttrConstr (f, rcode, rc, cur_level, counter);
            deleteResult (f, counter);
            rc = rcode;
            break;
        case c_tag:
            {
            char *prefix = PFqname_prefix (c->sem.qname);
            char *uri = PFqname_uri (c->sem.qname);
            char *loc = PFqname_loc (c->sem.qname);

            milprintf(f,
                    "{ # tagname-translation\n"
                    "var props := ws.fetch(QN_PREFIX_URI_LOC).fetch(WS);\n"
                    "var itemID;\n"
                    "if (props.texist(\"%s:%s:%s\")) {\n"
                    "    itemID := props.reverse().find(\"%s:%s:%s\");\n"
                    "} else { "
                    "    itemID := oid(props.count());\n"
                    "    ws.fetch(QN_PREFIX).fetch(WS).insert(itemID,\"%s\");\n"
                    "    ws.fetch(QN_URI).fetch(WS).insert(itemID,\"%s\");\n"
                    "    ws.fetch(QN_LOC).fetch(WS).insert(itemID,\"%s\");\n"
                    "    ws.fetch(QN_PREFIX_URI_LOC).fetch(WS).insert(itemID,\"%s:%s:%s\");\n"
                    "    ws.fetch(QN_URI_LOC).fetch(WS).insert(itemID,\"%s:%s\");\n"
                    "}\n",
                    prefix, uri, loc, 
                    prefix, uri, loc, 
                    prefix, uri, loc, 
                    prefix, uri, loc, 
                    uri, loc);

            /* translateConst needs a bound variable itemID */
            translateConst (f, cur_level, "QNAME");
            milprintf(f,
                    "} # end of tagname-translation\n"
                   );
            rc = NORMAL;
            }
            break;
        case c_text:
            rcode = (code == NODE)?NODE:NORMAL;

            rc = translate2MIL (f, VALUES, cur_level, counter, L(c));
            loop_liftedTextConstr (f, rcode, rc);
            rc = rcode;
            break;
        case c_cast:
        case c_seqcast:
            rc = translate2MIL (f, VALUES, cur_level, counter, R(c));
            rc = translateCast (f, code, rc, cur_level, c);
            break;
        case c_apply:
            if (c->sem.fun->builtin)
            {
                rc = translateFunction (f, code, cur_level, counter, 
                                        c->sem.fun, D(c));
            }
            else
            {
                translateUDF (f, cur_level, counter,
                              c->sem.fun, D(c));
                rc = NORMAL;
            }
            break;
        case c_xrpc:
            /*              xrpc
             *             /    \
             *            URI (FunctionCall)->sem.fun
             *                   |
             *                  args
             */
            translateXRPCCall(f, cur_level, counter, c);
            rc = NORMAL;
            break;
        case c_orderby:
            counter++;
            milprintf(f, "{ # order_by\n");
            milprintf(f, "var refined%03u := reverse(inner%03u).leftfetchjoin(order_%03u);\n", counter, cur_level, cur_level);

            /* evaluate orderspecs */
            translate2MIL (f, code, cur_level, counter, L(c));

            /* return expression */
            rc = translate2MIL (f, code, cur_level, counter, R(c));

            milprintf(f,
                    /* needed in case of 'stable' property */
                    "refined%03u := refined%03u.CTrefine(loop%03u.reverse());\n"
                    "var sorting := refined%03u.mirror();\n"
                    "refined%03u := nil;\n"
                    /* we need a real order preserving join here
                       otherwise the sorting inside an iteration 
                       could be mixed */
                    "sorting := sorting.leftjoin(iter.reverse()).reverse();\n"
                    /* as long as we have no complete stable sort we need
                       to refine with pos */
                    "sorting := sorting.CTrefine(pos);\n"
                    "ipik := sorting.hmark(0@0);\n"
                    "sorting := nil;\n"
                    "inner%03d := inner%03d.leftjoin(reverse(ipik.leftfetchjoin(iter))).tmark(0@0);\n"
                    "iter := ipik.mark(1@0);\n"
                    "inner%03d := inner%03d.leftfetchjoin(iter);\n"
                    "loop%03d := iter;\n"
                    "pos := ipik.leftfetchjoin(pos);\n"
                    "item%s := ipik.leftfetchjoin(item%s);\n"
                    "kind := ipik.leftfetchjoin(kind);\n"
                    "} # end of order_by\n",
                    counter, counter, cur_level,
                    counter, counter,
                    cur_level, cur_level, cur_level, cur_level, cur_level,
                    kind_str(rc), 
                    kind_str(rc));
            break;
        case c_orderspecs:
            descending = (c->sem.mode.dir == p_asc)?"":"_rev";

            rc = translate2MIL (f, VALUES, cur_level, counter, L(c));

            milprintf(f,
                    "if (iter.tunique().count() != iter.count()) {"
                    "ERROR (\"err:XPTY0004: order by expression expects "
                    "at most one value.\"); }\n"
                    "{ # orderspec\n"
                    "var order := iter.reverse().leftfetchjoin(item%s);\n"
                    "if (iter.count() != loop%03u.count()) {",
                    (rc)?kind_str(rc):val_join(get_kind(TY(L(c)))),
                    cur_level);
            if (c->sem.mode.empty == p_greatest)
            {
                milprintf(f,
                        /* generate a max value */
                        "order := order.access(BAT_APPEND);\n"
                        "order := order.insert(reverse(loop%03u.tdiff(iter))"
                                              ".project(max(order)+1));\n"
                        "order := order.access(BAT_READ);\n",
                        cur_level);
            }
            else
            {
                milprintf(f,
                        /* generate a min value */
                        "order := order.access(BAT_APPEND);\n"
                        "order := order.insert(reverse(loop%03u.tdiff(iter))"
                                              ".project(cast(nil,ttype(order))));\n"
                        "order := order.access(BAT_READ);\n",
                        cur_level);
            }
            milprintf(f,
                    "}\n"
                    "refined%03u := refined%03u.CTrefine%s(order);\n"
                    "} # end of orderspec\n",
                    counter, counter, descending);

            /* evaluate rest of orderspecs until end of list is reached */
            translate2MIL (f, code, cur_level, counter, R(c));
            rc = NORMAL; /* dummy */
            break;
        case c_fun_decls:
            translate2MIL (f, NORMAL, cur_level, counter, L(c));
            /* evaluate rest of orderspecs until end of list is reached */
            translate2MIL (f, NORMAL, cur_level, counter, R(c));
            rc = NORMAL; /* dummy */
            break;
        case c_fun_decl:
	    if (f->module_base == 0 && f->num_fun == 0) {
		/* ignore module functions */ 
  	        opt_output(f, OPT_SEC_IGNORE);
            } else { 
	        f->num_fun--;
  	        opt_output(f, OPT_SEC_EPILOGUE);
                milprintf(f, "UNDEF %s;\n", c->sem.fun->sig);
  	        opt_output(f, OPT_SEC_PROLOGUE);
            }
/* debug statement to print actual UDF parameters
"if (genType.search(\"debug\") >= 0) print(v_vid000.col_name(\"vid\"), v_iter000.col_name(\"iter\"), v_item000.col_name(\"item\"), v_kind000.col_name(\"kind\"));\n"
*/
            milprintf(f,
                    "PROC %s (bat[void,oid] loop000, "
                               "bat[void,oid] outer000, "
                               "bat[void,oid] order_000, "
                               "bat[void,oid] inner000, "
                               "bat[void,oid] v_vid000, "
                               "bat[void,oid] v_iter000, "
                               "bat[void,oid] v_item000, "
                               "bat[void,int] v_kind000) : bat[void,bat] { # fn:%s\n"
                    "var iter;\nvar pos;\nvar item;\nvar kind;\nvar ipik;\n"
                    "var v_pos000 := tmark_grp_unique(v_iter000,v_iter000);\n"
                    "v_pos000 := [oid](v_pos000).access(BAT_WRITE);\n"
                    "v_vid000 := [oid](v_vid000).access(BAT_WRITE);\n"
                    "v_iter000 := [oid](v_iter000).access(BAT_WRITE);\n"
                    "v_item000 := [oid](v_item000).access(BAT_WRITE);\n"
                    "v_kind000 := [int](v_kind000).access(BAT_WRITE);\n",
                    c->sem.fun->sig, PFqname_loc (c->sem.fun->qname));
            /* we could have multiple different calls */
            translate2MIL (f, NORMAL, 0, counter, R(c));
            milprintf(f,
                    "return bat(void,bat,4).append(iter).append(item).append(kind).access(BAT_READ);\n"
                    "} # end of PROC %s\n",
                    c->sem.fun->sig);
            opt_flush(f, 0);
  	    opt_output(f, OPT_SEC_QUERY);
            rc = NORMAL; /* dummy */
            break;
        case c_nil:
            /* don't do anything */
            rc = NORMAL; /* dummy */
            break;
        case c_letbind:
            mps_error ("Core node 'letbind' occured.");
            rc = NORMAL; /* dummy */
            break;
        case c_forbind:
            mps_error ("Core node 'forbind' occured.");
            rc = NORMAL; /* dummy */
            break;
        case c_forvars:
            mps_error ("Core node 'forvars' occured.");
            rc = NORMAL; /* dummy */
            break;
        case c_then_else:
            mps_error ("Core node 'then_else' occured.");
            rc = NORMAL; /* dummy */
            break;
        case c_seqtype:
            mps_error ("Core node 'seqtype' occured.");
            rc = NORMAL; /* dummy */
            break;
        default: 
            PFoops (OOPS_WARNING, 
                    "not supported feature is translated (kind %i)",
                    c->kind);
            break;
    }
    return rc;
}

/**
 * noConstructor tests if constructors are used in core tree
 * and report 0 if at least one is found and 1 else
 *
 * @param c the core tree, which is tested on constructor containment
 * @return 1 if no constructor is found - else 0
 */
static int
noConstructor (PFcnode_t *c)
{
    int i;
    if (c->kind == c_elem || c->kind == c_attr || c->kind == c_text ||
        c->kind == c_doc || c->kind == c_comment || c->kind == c_pi)
        return 0;
    else 
        for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
            if (!noConstructor (c->child[i]))
                return 0;

    return 1;
}

#ifdef HAVE_PFTIJAH
/**
 * noTijahFun tests if Tijah-functions are used in core tree
 * and report 0 if at least one is found and 1 else
 *
 * @param c the core tree, which is tested on Tijah-function containment
 * @return 1 if no Tijah function is found - else 0
 */
static int
noTijahFun (PFcnode_t *c)
{
    int i;
    if (c->kind == c_apply && (
	    (!PFqname_eq(c->sem.fun->qname, PFqname (PFns_lib,"tijah-query"))) ||
	    (!PFqname_eq(c->sem.fun->qname, PFqname (PFns_lib,"tijah-query-id"))) ||
	    (!PFqname_eq(c->sem.fun->qname, PFqname (PFns_lib,"tijah-nodes"))) ||
	    (!PFqname_eq(c->sem.fun->qname, PFqname (PFns_lib,"tijah-score")))))
        return 0;
    else 
        for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
            if (!noTijahFun (c->child[i]))
                return 0;

    return 1;
}
#endif

/**
 * var_only_in_for tests whether a variable is only used to iterate over
 *
 * @param v the variable which is tested
 * @param e the subtree of the variable binding
 * @return 1 if used in for-iteration (0 else)
 */
static int
var_only_in_for (PFvar_t *v, PFcnode_t *e)
{
  int i;
  int usage = 0;

  assert (v && e);

  if (e->kind == c_var && e->sem.var == v) {
      return 0;
  }
  /* if variable is used to iterate over (in a *user* written for-loop loop), ignore it as use */
  if (e->kind == c_for &&
      L(e) && L(e)->kind == c_forbind &&
      LL(e) && LL(e)->kind == c_forvars &&
      LLL(e) && LLL(e)->kind == c_var && 
      strcmp(PFqname_prefix(LLL(e)->sem.var->qname), "#pf") &&  /* not a Core-introduced loop */
      LR(e) && LR(e)->kind == c_var && LR(e)->sem.var == v)
  {
      return var_only_in_for (v, R(e)); /* we ignore L(e), then */
  }
  for (i = 0; (i < PFCNODE_MAXCHILD) && e->child[i]; i++)
      if (!var_only_in_for (v, e->child[i])) return 0;
  return 1;
}
                                                                                                                                                             
/**
 * expandable tests wether a variable can be expanded without causing
 * any problems and returns the result of the functions noConstructor and
 * noForBetween
 *
 * @param c the let-node which is tested
 * @return 1 if expandable; 0 otherwise
 */
static int
expandable (PFcnode_t *c)
{
#ifdef HAVE_PFTIJAH
    if (!noTijahFun(LR(c))) return 0;
#endif
    /* where possible, expand all pathfinder variables (introduced by XQuery Core 
     * normalization) back to where they came from; to simplify the plan again
     */
    if (strcmp(PFqname_prefix (LL(c)->sem.var->qname), "#pf") == 0) 
        return noConstructor(LR(c)); 

    /* let-variables written by the user are materialized beforehand (i.e. *not* expanded).
     * This assumes that the user 'knows what [s]he is doing' and has used the let to
     * specifically identify 'expensive' (to compute) expressions.
     *
     * The exception on this heuristic is when the variable is the potential 
     * partner in a join. In that case, the variable is better expanded in the plan
     * to defer execution (in case of a join, we then escape loop-lifting the result
     * during variable expansion). As the expansion decisions are made before
     * recognizing joins, we are not sure this actually happens, regrettably.
     */
    return var_only_in_for(LL(c)->sem.var, R(c)); 
}

/**
 * Walk core tree @a e and replace occurrences of variable @a v
 * by core tree @a a (i.e., compute e[a/v]).
 *
 * @note Only copies the single node @a a for each occurence of
 *       @a v. Only use this function if you
 *       - either call it only with atoms @a a,
 *       - or copy @a a at most once (i.e. @a v occurs at most
 *         once in @a e).
 *       Otherwise we might come into deep trouble as children
 *       of @a e would have more than one parent afterwards...
 *
 * @param v variable to replace
 * @param a core tree to insert for @a v
 * @param e core tree to walk over
 * @return modified core tree
 */
static void
replace_var (PFvar_t *v, PFcnode_t *a, PFcnode_t *e)
{
  unsigned short int i;
                                                                                                                                                             
  assert (v && a && e);
                                                                                                                                                             
  if (e->kind == c_var && e->sem.var == v)
      *e = *a;
  else
      for (i = 0; (i < PFCNODE_MAXCHILD) && e->child[i]; i++)
          replace_var (v, a, e->child[i]);
}

/**
 * var_is_used tests how often a variable is used in a tree
 * and gives back the number of usages.
 *
 * @param v variable to replace
 * @param e core tree to walk over
 */
static int
var_is_used (PFvar_t *v, PFcnode_t *e)
{
  int i;
  int usage = 0;

  assert (v && e);

  if (e->kind == c_var && e->sem.var == v)
      return 1;
  else
      for (i = 0; (i < PFCNODE_MAXCHILD) && e->child[i]; i++)
          usage += var_is_used (v, e->child[i]);

  return usage;
}


#define assert_exp(e) (assert (e), (e))

/*
static bool 
assert_exp (bool ex)
{
     assert (ex);
     return ex;
}
*/

static void 
cast_to_expected (PFcnode_t *c, PFty_t expected) 
{
     PFty_t opt_expected;
#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
     if (expected.type == ty_opt ||
         expected.type == ty_star ||
         expected.type == ty_plus)
          opt_expected = PFty_child (expected);
     else
          opt_expected = expected;
#endif
 
     if (PFty_subtype (opt_expected, PFty_atomic ()) &&
         !TY_EQ (TY(L(c)), expected) &&
         !TY_EQ (TY(L(c)), opt_expected))
     {
          if (TY_EQ (opt_expected, PFty_atomic ()))
               /* avoid dummy casts */ ;
          else if (L(c)->kind == c_seqcast
                   || L(c)->kind == c_cast)
          {
               TY(L(c))  =
                    TY(LL(c)) =
                    LL(c)->sem.type = expected;
          }
          else
          {
               L(c) = PFcore_seqcast (
                    PFcore_seqtype (expected), L(c));
               /* type new code, to avoid multiple casts */
               TY(c)     =
                    TY(L(c))  =
                    TY(LL(c)) = expected;
          }
     }
}

/**
 * simplifyCoreTree walks over a given tree and simplifies
 * some core nodes and appends extra information (e.g.
 * fake functions like 'item_sequence_to_node_sequence')
 *
 * @param c core tree to walk over
 * @return modified core tree
 */
static void
simplifyCoreTree (PFcnode_t *c)
{
    unsigned int i;
    PFfun_t *fun;
    PFcnode_t *new_node;
    PFty_t expected;
    PFty_t cast_type, input_type, opt_cast_type;

    assert(c);
    switch (c->kind)
    {
        case c_var:
            break;
        case c_seq:
            /* prunes every empty node in the sequence construction */
            simplifyCoreTree (L(c));
            simplifyCoreTree (R(c));

            if (TY_EQ(TY(L(c)), PFty_empty ()) &&
                TY_EQ(TY(R(c)), PFty_empty ()))
            {
                new_node = PFcore_empty ();
                TY(new_node) = PFty_empty ();
                *c = *new_node;
            }
            else if (TY_EQ(TY(L(c)), PFty_empty ()))
                *c = *(R(c));
            else if (TY_EQ(TY(R(c)), PFty_empty ()))
                *c = *(L(c));
            break;
        case c_let:
            /* don't do anything with global variables */
            if (LL(c)->sem.var->global)
            {
                simplifyCoreTree (LR(c));
                simplifyCoreTree (R(c));
                break;
            }
            /* Replacing one occurrence is probably the 
               most used case because let binding are
               added everywhere. To avoid multiple calls
               of simplifyCoreTree for one subtree do
               this test separately before all others. */
            else if (var_is_used (LL(c)->sem.var, R(c)) == 1 && expandable (c) )
            {
                replace_var (LL(c)->sem.var, LR(c), R(c));
                *c = *(R(c));
                simplifyCoreTree (c);
                break;
            }

            /* we need to simplify R(c) first because otherwise 
               var_is_used can contain more occurrences than necessary 
               (e.g., subtree which are pruned completely) */
            simplifyCoreTree (R(c));
            if ((i = var_is_used (LL(c)->sem.var, R(c))))
            {
                simplifyCoreTree (LR(c));

                /* remove all let statements, which are only bound to a literal
                   or another variable */
                if (LR(c)->kind == c_lit_str ||
                    LR(c)->kind == c_lit_int ||
                    LR(c)->kind == c_lit_dec ||
                    LR(c)->kind == c_lit_dbl ||
                    LR(c)->kind == c_true ||
                    LR(c)->kind == c_false ||
                    LR(c)->kind == c_empty ||
                    LR(c)->kind == c_var)
                {
                    replace_var (LL(c)->sem.var, LR(c), R(c));
                    *c = *(R(c));
                }
                /* removes let statements, which are used only once and contain
                   no constructor */
                else if (i == 1 &&
                    expandable (c))
                {
                    replace_var (LL(c)->sem.var, LR(c), R(c));
                    *c = *(R(c));
                }
                else
                {
                    /* remove let statements, whose body contains only
                       the bound variable */
                    if (R(c)->kind == c_var && 
                             R(c)->sem.var == LL(c)->sem.var)
                    {
                        *c = *(LR(c));
                    }
                }
            }
            /* removes let statement, which are not used */
            else
            {
                *c = *(R(c));
            }
            break;
        case c_for:
            /* 
                for $x in for $y in e
                          return e_y
                return e_x
              ==>
                for $y in e
                return for $x in e_y
                       return e_x

                NOTE: We have to ensure that the outer iteration has no
                      positional variable - it would get screwed up by
                      the nesting.
            */
            if (LR(c)->kind == c_for && LLR(c)->kind != c_var && R(c)->kind != c_let && R(c)->kind != c_orderby)
            {
                PFcnode_t *c_old = PFmalloc (sizeof (PFcnode_t));
                *c_old = *c;
                *c = *LR(c_old);
                LR(c_old) = R(c);
                R(c) = c_old;
                TY(R(c)) = *PFty_simplify ((PFty_quantifier (PFty_defn (TY(RLR(c))))) (TY(RR(c))));
                TY(c) = *PFty_simplify ((PFty_quantifier (PFty_defn (TY(LR(c))))) (TY(R(c))));
            }

            simplifyCoreTree (LR(c));
            simplifyCoreTree (R(c));

            input_type = PFty_defn(TY(LR(c)));

            if (R(c)->kind == c_var && 
                R(c)->sem.var == LLL(c)->sem.var)
            {
                *c = *(LR(c));
            }
            else if (TY_EQ (input_type, PFty_empty ()))
            {
                new_node = PFcore_empty ();
                TY(new_node) = PFty_empty ();
                *c = *new_node;
            }
#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
            /* removes for expressions, which loop only over one literal */
            /* FIXME: doesn't work if the following type is possible here:
                      '(integer, decimal)?' */
            else if (PFty_subtype (input_type, PFty_item ()) &&
                     input_type.type != ty_opt &&
                     input_type.type != ty_plus &&
                     input_type.type != ty_star &&
                     input_type.type != ty_choice &&
                     input_type.type != ty_all &&
                     input_type.type != ty_seq)
            {
                /* if for expression contains a positional variable
                   and it is used it is replaced by the integer 1 */
                if (LLR(c)->kind == c_var)
                {
                    new_node = PFcore_num (1);
                    TY(new_node) = PFty_xs_integer ();
                    replace_var (LLR(c)->sem.var, new_node, R(c));
                    LLR(c) = PFcore_nil ();
                    TY(LLR(c)) = PFty_none();
                }

                if (PFty_subtype (input_type, PFty_opt (PFty_atomic ())))
                {
                    replace_var (LLL(c)->sem.var, LR(c), R(c));
                    *c = *(R(c));
                }
                else 
                {
                    new_node = PFcore_let (PFcore_letbind (LLL(c),
                                                           LR(c)),
                                           R(c));
                    TY(new_node) = TY(R(c));
                    TY(L(new_node)) =
                    TY(LL(new_node)) = TY(LLR(c));
                    *c = *new_node;
                }
            }
#endif
            /* remove for expression, whose body contains only
                   the bound variable */
            break;
        case c_cast:
        case c_seqcast:
            simplifyCoreTree (R(c));
            /* debugging information */
            /*
            PFlog("input type: %s",
                  PFty_str (TY(R(c))));
            PFlog("cast type: %s",
                  PFty_str (L(c)->sem.type));
            */
            cast_type = *PFty_simplify(PFty_defn(L(c)->sem.type));
            input_type = TY(R(c));
            opt_cast_type = cast_type;

#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
            if (cast_type.type == ty_opt ||
                cast_type.type == ty_star)
                opt_cast_type = PFty_child (cast_type);
#endif

            /* if casts are nested only the most outest
               cast has to be evaluated */
            if (R(c)->kind == c_seqcast || R(c)->kind == c_cast)
            {
                assert (RR(c));
                R(c) = RR(c);
                input_type = TY(R(c));
            }

            /* get rid of unnecessary numeric cast (the only place
               where this cast is introduced are predicates) */
            if (c->kind == c_seqcast &&
                PFty_subtype (input_type, PFty_opt (PFty_numeric ())) &&
                PFty_subtype (PFty_numeric (), L(c)->sem.type) &&
                PFty_subtype (L(c)->sem.type, PFty_numeric ()))
                *c = *(R(c));
#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
            /* removes casts which are not necessary:
               - if types are the same
               - if the cast type has only a additional
                 occurence indicator compared to the
                 input type */
            else if (TY_EQ (input_type, cast_type) ||
                TY_EQ (input_type, opt_cast_type) ||
                ((cast_type.type == ty_opt ||
                  cast_type.type == ty_star) &&
                 TY_EQ (input_type, PFty_empty ())))
            {
                *c = *(R(c));
            }
            /* don't cast nodes - node type was only needed
               for static typing */
#endif
            else if (c->kind == c_seqcast &&
                     PFty_subtype (input_type, PFty_xs_anyNode ()) &&
                     PFty_subtype (cast_type, PFty_xs_anyNode ()))
            {
                *c = *(R(c));
            }
            /* tests if a type is castable */
            else if (castable (input_type, cast_type));
            else if (!PFty_subtype (input_type, cast_type))
                PFoops (OOPS_TYPECHECK,
                        "can't cast type '%s' to type '%s'",
                        PFty_str(input_type),
                        PFty_str(cast_type));
            else
            {
            /* get rid of unnecessary numeric cast (the only place
               where this cast is introduced are predicates) */
            if (c->kind == c_seqcast &&
                PFty_subtype (input_type, PFty_opt (PFty_numeric ())) &&
                PFty_subtype (PFty_numeric (), L(c)->sem.type) &&
                PFty_subtype (L(c)->sem.type, PFty_numeric ()))
                *c = *(R(c));
#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
                *c = *(R(c));
                /*
                PFlog ("cast from '%s' to '%s' ignored",
                       PFty_str (input_type),
                       PFty_str (cast_type));
                */
            }
            break;
        case c_apply:
            /* handle the promotable types explicitly by casting them */
            fun = c->sem.fun;
            {
                unsigned int i = 0;
                PFcnode_t *tmp = D(c);
#endif
                while (i < fun->arity)
                {
                    simplifyCoreTree (L(tmp));
                    tmp = R(tmp);
                    i++;
                }
            }
            if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"boolean")) 
                && assert_exp (fun->sig_count == 1)
                && PFty_subtype(TY(DL(c)), fun->sigs[0].ret_ty)) 
            {
                 /* don't use function - omit apply and arg node */
                 *c = *(DL(c));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"data")) && 
                     PFty_subtype(TY(DL(c)), PFty_star (PFty_atomic ())))
            {
                /* don't use function - omit apply and arg node */
                *c = *(DL(c));
            }
            else if ((!PFqname_eq(fun->qname,PFqname (PFns_fn,"string")) ||
                      !PFqname_eq(fun->qname,PFqname (PFns_fn,"string-join"))) &&
                     PFty_subtype(TY(DL(c)), PFty_xs_string ()))
            {
                /* don't use function - omit apply and arg node */
                *c = *(DL(c));
            }
            /* throw away merge-adjacent-text-nodes if only one element content was 
               created -> there is nothing to merge */
            /* FIXME: to find simple version this relies on other optimizations */
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"merge-adjacent-text-nodes")) &&
                 /* empty result doesn't need to be merged */ 
                (DL(c)->kind == c_empty ||
                 /* exactly one node per iteration doesn't need to be merged */
                 (PFty_subtype(TY(DL(c)), PFty_node ()) &&
                  DL(c)->kind != c_var &&
                  DL(c)->kind != c_seq) ||
                 /* merge is not needed after is2ns anymore */
                 ((DL(c)->kind == c_apply) &&
                  !PFqname_eq(DL(c)->sem.fun->qname,
                              PFqname (PFns_pf,"item-sequence-to-node-sequence"))) ||
                 /* element nodes don't contain text nodes to merge */
                 (PFty_subtype(TY(DL(c)), 
                               PFty_star (
                                   PFty_choice (
                                       PFty_choice (
                                           PFty_xs_anyElement (),
                                           PFty_doc (PFty_star (PFty_item ()))),
                                       PFty_xs_anyAttribute ())))))
                     )
            {
                /* don't use function - omit apply and arg node */
                *c = *(DL(c));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"item-sequence-to-node-sequence")) &&
                PFty_subtype(TY(DL(c)), PFty_star (PFty_node ())))
            {
                /* don't use function - omit apply and arg node */
                *c = *(DL(c));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"item-sequence-to-node-sequence")) &&
                PFty_subtype(TY(DL(c)), PFty_xs_string ()))
            {
                new_node = PFcore_wire1(c_text, DL(c));
                TY(new_node) = PFty_text();
                *c = *new_node;
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"item-sequence-to-untypedAtomic")) &&
                     assert_exp (fun->sig_count == 1) &&
                     PFty_subtype (TY(DL(c)), fun->sigs[0].ret_ty))
            {
                /* don't use function - omit apply and arg node */
                *c = *(DL(c));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"item-sequence-to-untypedAtomic")) &&
                      PFty_subtype (TY(DL(c)), PFty_atomic ()))
            {
                new_node = PFcore_seqcast (PFcore_seqtype (PFty_untypedAtomic ()),
                                           DL(c));
                TY(new_node)    =
                TY(L(new_node)) = PFty_untypedAtomic ();
                *c = *new_node;
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"item-sequence-to-untypedAtomic")) &&
                     PFty_subtype (TY(DL(c)), PFty_empty()))
            {
                new_node = PFcore_seqcast (PFcore_seqtype (PFty_untypedAtomic ()),
                                           PFcore_str (""));
                TY(new_node)    =
                TY(L(new_node)) = PFty_untypedAtomic ();
                TY(R(new_node)) = PFty_xs_string ();
                *c = *new_node;
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"distinct-doc-order")) &&
                     (PFty_subtype (TY(DL(c)), PFty_opt(PFty_node ())) ||
                      DL(c)->kind == c_locsteps))
            {
                /* don't use function - either because we only have at most
                   one node per iteration or we use scj and therefore don't need
                   to remove duplicates and sort the result */
                *c = *(DL(c));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"distinct-doc-order")) &&
                     DL(c)->kind == c_apply &&
                     !PFqname_eq((DL(c))->sem.fun->qname,PFqname (PFns_pf,"distinct-doc-order"))) 
            {
                *c = *(DL(c));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_pf,"distinct-doc-order")) &&
                     DL(c)->kind == c_apply &&
                     !PFqname_eq((DL(c))->sem.fun->qname,PFqname (PFns_pf,"distinct-doc-order")))
            {
                *c = *(DL(c));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_op, "union")))
            {
                if (TY_EQ (TY(DL(c)), PFty_empty ()))
                    new_node = DRL(c);
                else if (TY_EQ (TY(DRL(c)), PFty_empty ()))
                    new_node = DL(c);
                else
                {  /* use wire to avoid PFcore_seq() (because of IS_ATOM test) */
                   new_node =  PFcore_wire2 (c_seq, DL(c), DRL(c));
                   TY(new_node) = *PFty_simplify (PFty_seq (TY(L(new_node)), TY(R(new_node))));
                }

                R(D(c)) = PFcore_nil ();
                TY(R(D(c))) = PFty_none ();
                L(D(c)) = new_node;
                TY(L(D(c))) = TY(new_node);
                c->sem.fun = PFcore_function (PFqname (PFns_pf, "distinct-doc-order"));
            }
            /* concatentation with empty string not needed */
            else if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"concat")) &&
                     DRL(c)->kind == c_lit_str &&
                     !strcmp (DRL(c)->sem.str,""))
            {
                *c = *(DL(c));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"not")) &&
                     DL(c)->kind == c_apply &&
                     !PFqname_eq(DL(c)->sem.fun->qname,
                                 PFqname (PFns_fn,"not")))
            {
                *c = *(DL(DL(c)));
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"empty")) &&
                     DL(c)->kind == c_if &&
                     PFty_subtype (TY(RL(DL(c))),
                                   PFty_atomic ()) &&
                     PFty_subtype (TY(RR(DL(c))),
                                   PFty_empty ()))
            {
                if (L(DL(c))->kind == c_apply &&
                     !PFqname_eq(L(DL(c))->sem.fun->qname,
                                 PFqname (PFns_fn,"not")))
                {
                    *c = *DL(L(DL(c)));
                }
                else
                {
                    c->sem.fun = PFcore_function (PFqname (PFns_fn, "not"));
                    DL(c) = L(DL(c));
                    TY(D(c)) = TY(DL(c));
                }
            }
            else if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"empty")) &&
                     DL(c)->kind == c_if &&
                     PFty_subtype (TY(RL(DL(c))),
                                   PFty_empty ()) &&
                     PFty_subtype (TY(RR(DL(c))),
                                   PFty_atomic ()))
            {
                *c = *(L(DL(c)));
            }
            /*
             * fn:resolve-QName() translates a string argument into a QName
             * for computed element constructors.  The element construction
             * code handles strings there anyway, so we simply omit the
             * call to fn:resolve-URI().
             */
            else if (!PFqname_eq (fun->qname, PFqname (PFns_fn,"resolve-QName"))
                     && assert_exp (fun->sig_count == 1))
            {
                 /* don't use function - omit apply and arg node */
                 *c = *(DL(c));
            }
            else
            {
                c = D(c);
                if (fun->sig_count == 1) {
                     for (i = 0; i < fun->arity; i++, c = R(c))
                     {
                         assert (fun->sig_count == 1);
                         expected = PFty_defn((fun->sigs[0].par_ty)[i]);
                         cast_to_expected (c, expected);
                     }
                }
                else {
                     /* quick and dirty hack to support dynamic overloading */
                     assert (fun->arity == 2);
                     bool found = false;
                     /* choose one implementation. */
                     for (unsigned int i = 0; i < fun->sig_count && !found; i++) {
                          if (PFty_promotable (TY(L(c)), 
                                               fun->sigs[i].par_ty[0]) &&
                              PFty_promotable (TY(RL(c)), 
                                               fun->sigs[i].par_ty[1]))
                          {
                               cast_to_expected (c, fun->sigs[i].par_ty[0]);
                               cast_to_expected (R(c), fun->sigs[i].par_ty[0]);
                               found = true;
                          }
                     }
                     if (!found)
                          PFoops (OOPS_TYPECHECK,
                                  "no common implementation found for "
                                  "dynamically overloaded function %s",
                                  PFqname_str (fun->qname));
                }
            }
            break;
        case c_if:
            simplifyCoreTree (L(c));
            simplifyCoreTree (RL(c));
            simplifyCoreTree (RR(c));

            if (L(c)->kind == c_apply &&
                !PFqname_eq(L(c)->sem.fun->qname,
                            PFqname (PFns_fn,"not")))
            {
                L(c) = DL(L(c));
                new_node = RR(c);
                RR(c) = RL(c);
                RL(c) = new_node;
            }
            break;
        case c_locsteps:
            simplifyCoreTree (L(c));
            simplifyCoreTree (R(c));
            if (R(c)->kind != c_locsteps &&
                !PFty_subtype(TY(R(c)),PFty_opt (PFty_node ())) &&
                !(R(c)->kind == c_apply &&
                  !PFqname_eq(R(c)->sem.fun->qname,PFqname (PFns_pf,"distinct-doc-order"))))
            {
                fun = PFcore_function (PFqname (PFns_pf, "distinct-doc-order"));
                R(c) = PFcore_apply (fun, PFcore_arg (R(c), PFcore_nil ()));
            }
            else
            /* don't have to look at predicates because they are already expanded */
            if (L(c)->kind == c_child &&
                R(c)->kind == c_locsteps &&
                RL(c)->kind == c_descendant_or_self &&
                TY_EQ(TY(RLL(c)), PFty_xs_anyNode ()))
            {
                L(c)->kind = c_descendant;
                R(c) = RR(c);
            }
            else if (R(c)->kind == c_locsteps &&
                RL(c)->kind == c_self &&
                TY_EQ(TY(RLL(c)), PFty_xs_anyNode ()))
            {
                R(c) = RR(c);
            }
            break;
        case c_text:
            simplifyCoreTree (L(c));
            /* substitutes empty text nodes by empty nodes */
            if (L(c)->kind == c_empty)
                *c = *(PFcore_empty ());
            break;
        case c_orderspecs:
            simplifyCoreTree (L(c));
            simplifyCoreTree (R(c));

            input_type = PFty_prime(PFty_defn (TY(L(c))));
#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
            if (input_type.type == ty_star ||
                input_type.type == ty_plus ||
                input_type.type == ty_opt)
            {
                input_type = PFty_child(input_type);
            }

            /* we want to avoid comparison between different types */
            if (input_type.type == ty_choice)
            {
                L(c) = PFcore_seqcast (PFcore_seqtype (PFty_xs_string()), L(c));
                /* type new code, to avoid multiple casts */
                TY(c)     =
                TY(L(c))  =
                TY(LL(c)) = PFty_xs_string ();
            }
#endif
            break;
        default: 
            for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
                simplifyCoreTree (c->child[i]);
            break;
    }
}

/* variable information */
struct var_info {
    PFcnode_t *parent; /* binding expression */
    PFvar_t *id;       /* name */
    int act_lev;       /* definition level */
    PFarray_t *reflist; /* list of referenced vars */
};
typedef struct var_info var_info;

static void recognize_join(PFcnode_t *c, 
                           PFarray_t *active_vlist,
                           PFarray_t *active_vdefs,
                           int cur_level);

/**
 * create_var_info: helper function for join recognition
 * creates a new variable entry for current variable stack
 *
 * @param parent the binding expression 
 * @param id the name of the variable
 * @param act_lev the definition level
 * @return the var_info struct holding the variable information
 */
static var_info *
create_var_info (PFcnode_t *parent, PFvar_t *id, int act_lev)
{
    var_info *vi;
    vi = (var_info *) PFmalloc (sizeof (var_info));

    PFarray_t *reflist = PFarray (sizeof (var_info *));

    vi->parent = parent;
    vi->id = id;
    vi->act_lev = act_lev;
    vi->reflist = reflist;    
    return vi;
}

/**
 * var_lookup: helper function for join recognition
 * looks up a variable in the active variable stack
 * using its name as reference
 *
 * @param id the name of the variable
 * @param active_vlist the active variable stack
 * @return the var_info struct containing the variable information
 */
static var_info *
var_lookup (PFvar_t *id, PFarray_t *active_vlist)
{
    unsigned int i;
    for (i = 0; i < PFarray_last (active_vlist); i++)
    {
        if ((*(var_info **) PFarray_at (active_vlist, i))->id == id)
                return *(var_info **) PFarray_at (active_vlist, i);
    }
    return NULL;
}

/**
 * add_ref_to_vdef: helper function for join recognition
 * adds a var_info to the list of referenced variables
 *
 * @param var the new variable to add
 * @param active_vdefs the stack of active variable definitions
 */
static void
add_ref_to_vdef (var_info *var, PFarray_t *active_vdefs)
{
    unsigned int i, j, found;
    PFarray_t *reflist;

    for (i = 0; i < PFarray_last (active_vdefs); i++)
    {
        /* only add variables to the reference list
           which are defined in an outer scope */
        if ((*(var_info **) PFarray_at (active_vdefs, i))->act_lev >= var->act_lev)
        {
            /* add only variables which are not already in the list */
            found = 0;
            reflist = (*(var_info **) PFarray_at (active_vdefs, i))->reflist;
            for (j = 0; j < PFarray_last (reflist); j++)
            {
                if ((*(var_info **) PFarray_at (reflist, j))->id == var->id)
                {
                    found = 1;
                    break;
                }
            }
            if (!found)
            {
                *(var_info **) PFarray_add (reflist) = var;
            }
        }
    }
}

/**
 * collect_vars: helper function for join recognition
 * collects all active variables in a given Core expression in
 * a reference list (comparison of variables with active_vlist)
 *
 * @param c the Core expression currently searched
 * @param active_vlist the list of active variables
 * @param reflist the list of already collected variables
 * @return the updated reflist
 */
static PFarray_t *
collect_vars (PFcnode_t *c, PFarray_t *active_vlist, PFarray_t *reflist)
{
    unsigned int i;
    assert(c);
    var_info *var_struct;

    switch (c->kind)
    {
        case c_var:
            /* get var_info of current variable */
            var_struct = var_lookup (c->sem.var, active_vlist);
            /* if variable returns no variable information 
               it is irrelevant (inner scope) */
            if (var_struct)
            {
                *(var_info **) PFarray_add (reflist) = var_struct;
            }
            break;
        default:
            for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++) {
                reflist = collect_vars (c->child[i], active_vlist, reflist);
            }
            break;
    }
    return reflist;
}

/**
 * create_join_function: helper function for join recognition
 * transforms the discovered into a new core construct
 * (a function #pf:join), which represents the join pattern
 *
 * @param fst_for contains the Core expression of the left join condition
 * @param fst_cast contains a cast expression if special case has to be applied
 * @param fst_nested saves the most inner scope needed
 * @param snd_for contains the Core expression of the right join condition
 * @param snd_cast contains a cast expression if special case has to be applied
 * @param snd_nested saves the most inner scope needed
 * @param fun saves which join comparison has to be executed
 * @param switched_args saves the information wether left and right side
 *                      of the join arguments are switched
 * @param result the Core expression containing
 *               the return part of the join pattern
 */
static PFcnode_t *
create_join_function (PFcnode_t *fst_for, PFcnode_t *fst_cast, int fst_nested, 
                      PFcnode_t *snd_for, PFcnode_t *snd_cast, int snd_nested,
                      PFfun_t *fun, int switched_args, PFcnode_t *result)
{
    PFcnode_t *c, *comp;
    int fid = -1; /* dummy fid used for variable mapping of the result */

    assert(fst_for);
    assert(snd_for);
    assert(fun);
    assert(result);

    fst_cast = (!fst_cast)?PFcore_nil():fst_cast;
    snd_cast = (!snd_cast)?PFcore_nil():snd_cast;

    comp = PFcore_leaf(c_apply);
    comp->sem.fun = fun;

    PFfun_t *join = PFfun_new(PFqname (PFns_pf,"join"), /* name */
                              10,     /* arity */
                              true,   /* built-in function */
                              1,      /* sig_count */
                              NULL,   /* signatures (don't care here) */
                              NULL,   /* no algebra equivalent */
                              NULL,   /* no parameter variable names */
                              NULL);  /* no 'at'-hint URI */

    c = PFcore_nil();
    c = PFcore_arg(PFcore_num(fid),c);
    c = PFcore_arg(result, c);
    c = PFcore_arg(PFcore_num(switched_args),c);
    c = PFcore_arg(comp, c);
    c = PFcore_arg(PFcore_num(snd_nested),c);
    c = PFcore_arg(snd_cast,c);
    c = PFcore_arg(snd_for,c);
    c = PFcore_arg(PFcore_num(fst_nested),c);
    c = PFcore_arg(fst_cast,c);
    c = PFcore_arg(fst_for,c);
    c = PFcore_wire1 (c_apply, c);
    c->sem.fun = join;
    TY(c) = TY(result);
    return c;
}

/**
 * test_join_pattern:: helper function for join recognition
 * tests the join pattern and the variable independence
 *
 * @param for_node the Core expression which has to be tested
 * @param active_vlist the stack of the active variables
 * @param active_vdefs the stack of the active variable definitions
 * @param cur_level the current level
 * @return information wether subtree is already tested or not
 */
static int test_join_pattern(PFcnode_t *for_node,
                             PFarray_t *active_vlist,
                             PFarray_t *active_vdefs,
                             int cur_level)
{

    PFcnode_t *c, *if_node, *apply_node,
              *fst_inner, *snd_inner, 
              *fst_inner_cast, *snd_inner_cast,
              *res;
    int switched_args,
        found_in_fst, found_in_snd,
        max_lev_fst, max_lev_snd,
        join_found, j;
    PFarray_t *fst_reflist, *snd_reflist;
    var_info *vi, *vipos;
    PFfun_t *fun;
    unsigned int i;

    join_found = false;

    /* pattern needed:
       for $v in e_in return
           if (comp(e_1, e_2))
           then e_return
           else ()

       extended pattern for quantified expressions:
       for $v in e_in return
           if (empty(for $w in e_in return
                     if (comp(e_1, e_2)) then 1 else ()))
           then ()
           else e_return
    */

    /* test variable independence of the bound variable */
    if (cur_level)
    {
        vi = var_lookup (LLL(for_node)->sem.var, active_vlist);
        if (!vi) mps_error ("could not lookup variable '%s' in join pattern testing.",
                            PFqname_str((LLL(for_node)->sem.var)->qname)); 

        for (i = 0; i < PFarray_last (vi->reflist); i++)
        {
            /* bound variable must not reference a variable from its enclosing scope */
            if ((*(var_info **) PFarray_at (vi->reflist, i))->act_lev == vi->act_lev -1)
            {
                return join_found;
            }
        }
    }

    res = NULL;
    fst_inner = NULL;
    snd_inner = NULL;
    fst_inner_cast = NULL;
    snd_inner_cast = NULL;
    switched_args = false;

    if (R(for_node)->kind != c_if ||
        RL(for_node)->kind != c_apply)
    {
        return join_found;
    }

    if_node = R(for_node);
    apply_node = L(if_node);
    fun = apply_node->sem.fun;

    /* test quantified pattern */
    if (!PFqname_eq (fun->qname, PFqname (PFns_fn,"empty")) &&
        RL(if_node)->kind == c_empty &&
        DL(apply_node)->kind == c_for &&
        R(DL(apply_node))->kind == c_if &&
        RL(DL(apply_node))->kind == c_apply)
    {
        res = RR(if_node);

        c = DL(apply_node);
        fst_inner = PFcore_for (PFcore_wire2 (c_forbind,
                                              PFcore_forvars(LLL(c), 
                                                             LLR(c)),
                                              LR(c)),
                                PFcore_empty());
        if_node = R(c);
        apply_node = L(if_node);
        fun = apply_node->sem.fun;

        /* test quantified pattern for second comparison input */
        if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"empty")) &&
            RL(if_node)->kind == c_empty &&
            DL(apply_node)->kind == c_for &&
            R(DL(apply_node))->kind == c_if &&
            RL(DL(apply_node))->kind == c_apply)
        {

            c = DL(apply_node);
            snd_inner = PFcore_for (PFcore_wire2 (c_forbind,
                                                  PFcore_forvars(LLL(c), 
                                                                 LLR(c)),
                                                  LR(c)),
                                    PFcore_empty());
            if_node = R(c);
            apply_node = L(if_node);
            fun = apply_node->sem.fun;
        }
    }

    /* find matching comparison function */
    if (RR(if_node)->kind != c_empty ||
        (PFqname_eq(fun->qname,PFqname (PFns_op,"eq")) &&
      /* PFqname_eq(fun->qname,PFqname (PFns_op,"ne")) && (not supported by MonetDB) */
         PFqname_eq(fun->qname,PFqname (PFns_op,"le")) &&
         PFqname_eq(fun->qname,PFqname (PFns_op,"lt")) &&
         PFqname_eq(fun->qname,PFqname (PFns_op,"ge")) &&
         PFqname_eq(fun->qname,PFqname (PFns_op,"gt"))))
    {
        return join_found;
    }
 
    /* we have no quantified expression */
    if (!res)
    {
        res = RL(if_node);
        fst_inner = DL(apply_node);
        snd_inner = DRL(apply_node);
    }
    else
    {
        if (var_is_used(LLL(fst_inner)->sem.var,
                        DL(apply_node)))
        {
            fst_inner_cast = DL(apply_node);
            snd_inner_cast = DRL(apply_node);
        }
        else if (var_is_used(LLL(fst_inner)->sem.var,
                             DRL(apply_node)))
        {
            fst_inner_cast = DRL(apply_node);
            snd_inner_cast = DL(apply_node);
            switched_args = true;
        }
        else return join_found;
        
        if (snd_inner)
        {
            /* test correctness */
            if ((switched_args && 
                 var_is_used(LLL(snd_inner)->sem.var,
                             DL(apply_node))) 
                ||
                (!switched_args &&
                 var_is_used(LLL(snd_inner)->sem.var,
                             DRL(apply_node)))
               )
            {
            }
            else return join_found;
        }
        else
        {
            snd_inner = snd_inner_cast;
            snd_inner_cast = NULL;
        }

        /* now test simplifications for quantified expressions */
        /* test special pattern:
            for $v in for $w in [ e/attribute::.. | e/..::text() ]
                      return cast as untypedAtomic (string-value($w))
            return if (comp([ $v | cast as .. ($v) ],..)) then 1 else ()
           to avoid evaluation of two unnecessary loops and #pf:string-value
        */
        if (fst_inner_cast)
        {
            if (LR(fst_inner)->kind == c_for &&
                LR(LR(fst_inner))->kind == c_locsteps &&
                (L(LR(LR(fst_inner)))->kind == c_attribute ||
                 TY_EQ(TY(LL(LR(LR(fst_inner)))),
                         PFty_text ())) &&
                (R(LR(fst_inner))->kind == c_seqcast
                 || R(LR(fst_inner))->kind == c_cast) &&
                RL(LR(fst_inner))->kind == c_seqtype &&
                TY_EQ (RL(LR(fst_inner))->sem.type, PFty_untypedAtomic()) &&
                RR(LR(fst_inner))->kind == c_apply &&
                !PFqname_eq (RR(LR(fst_inner))->sem.fun->qname,
                             PFqname (PFns_pf,"string-value")) &&
                DL(RR(LR(fst_inner)))->kind == c_var &&
                DL(RR(LR(fst_inner)))->sem.var == LLL(LR(fst_inner))->sem.var &&
                (fst_inner_cast->kind == c_seqcast
                 || fst_inner_cast->kind == c_cast) &&
                R(fst_inner_cast)->kind == c_var && 
                R(fst_inner_cast)->sem.var == LLL(fst_inner)->sem.var)
            {
                fst_inner = LR(LR(fst_inner));
                fst_inner_cast = L(fst_inner_cast);

#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
                if (fst_inner_cast->sem.type.type == ty_opt)
                {
                    TY(fst_inner_cast) = PFty_child(fst_inner_cast->sem.type);
                    fst_inner_cast->sem.type = PFty_child(fst_inner_cast->sem.type);
                }
#endif
            }
            else if (fst_inner_cast->kind == c_var && fst_inner_cast->sem.var == LLL(fst_inner)->sem.var)
            {
                fst_inner = LR(fst_inner);
                fst_inner_cast = NULL;
            }
            else
            {
                R(fst_inner) = fst_inner_cast;
                fst_inner_cast = NULL;
            }
        }
        if (snd_inner_cast)
        {
            if (LR(snd_inner)->kind == c_for &&
                LR(LR(snd_inner))->kind == c_locsteps &&
                (L(LR(LR(snd_inner)))->kind == c_attribute ||
                 TY_EQ(TY(LL(LR(LR(snd_inner)))),
                         PFty_text ())) &&
                (R(LR(snd_inner))->kind == c_seqcast
                 || R(LR(snd_inner))->kind == c_cast) &&
                RL(LR(snd_inner))->kind == c_seqtype &&
                TY_EQ (RL(LR(snd_inner))->sem.type, PFty_untypedAtomic()) &&
                RR(LR(snd_inner))->kind == c_apply &&
                !PFqname_eq (RR(LR(snd_inner))->sem.fun->qname,
                             PFqname (PFns_pf,"string-value")) &&
                DL(RR(LR(snd_inner)))->kind == c_var &&
                DL(RR(LR(snd_inner)))->sem.var == LLL(LR(snd_inner))->sem.var &&
                (snd_inner_cast->kind == c_seqcast
                 || snd_inner_cast->kind == c_cast) &&
                R(snd_inner_cast)->kind == c_var && 
                R(snd_inner_cast)->sem.var == LLL(snd_inner)->sem.var)
            {
                snd_inner = LR(LR(snd_inner));
                snd_inner_cast = L(snd_inner_cast);

#ifdef USE_DEPRECATED_ACCESS_TO_TYPE_SYSTEM
                if (snd_inner_cast->sem.type.type == ty_opt)
                {
                    TY(snd_inner_cast) = PFty_child(snd_inner_cast->sem.type);
                    snd_inner_cast->sem.type = PFty_child(snd_inner_cast->sem.type);
                }
#endif
            }
            else if (snd_inner_cast->kind == c_var && snd_inner_cast->sem.var == LLL(snd_inner)->sem.var)
            {
                snd_inner = LR(snd_inner);
                snd_inner_cast = NULL;
            }
            else
            {
                R(snd_inner) = snd_inner_cast;
                snd_inner_cast = NULL;
            }
        }

    } /* end else (!res) */

    /* find referenced variables from outer scopes in first comparison side */
    fst_reflist = PFarray (sizeof (var_info *));
    fst_reflist = collect_vars(fst_inner, active_vlist, fst_reflist);
    /* search for-loop variable in first argument */
    found_in_fst = 0;
    max_lev_fst = cur_level >= UDF_LEV ? UDF_LEV : 0;
    for (i = 0; i < PFarray_last (fst_reflist); i++)
    {
        if ((*(var_info **) PFarray_at (fst_reflist, i))->act_lev == cur_level+1)
        {
            if ((*(var_info **) PFarray_at (fst_reflist, i))->id == LLL(for_node)->sem.var)
            {
                 found_in_fst = 1;
            }
            else if (LLR(for_node)->kind == c_var &&
                (*(var_info **) PFarray_at (fst_reflist, i))->id == LLR(for_node)->sem.var)
            {
                 found_in_fst = 1;
            }
        }
        else
        {
            j = (*(var_info **) PFarray_at (fst_reflist, i))->act_lev;
            max_lev_fst = (max_lev_fst > j)?max_lev_fst:j;
        }
    }

    /* find referenced variables from outer scopes in second comparison side */
    snd_reflist = PFarray (sizeof (var_info *));
    snd_reflist = collect_vars(snd_inner, active_vlist, snd_reflist);
    /* search for-loop variable in second argument */
    found_in_snd = 0;
    max_lev_snd = cur_level >= UDF_LEV ? UDF_LEV : 0;
    for (i = 0; i < PFarray_last (snd_reflist); i++)
    {
        if ((*(var_info **) PFarray_at (snd_reflist, i))->act_lev == cur_level+1)
        {
            if ((*(var_info **) PFarray_at (snd_reflist, i))->id == LLL(for_node)->sem.var)
            {
                 found_in_snd = 1;
            }
            else if (LLR(for_node)->kind == c_var &&
                (*(var_info **) PFarray_at (snd_reflist, i))->id == LLR(for_node)->sem.var)
            {
                 found_in_snd = 1;
            }
        }
        else
        {
            j = (*(var_info **) PFarray_at (snd_reflist, i))->act_lev;
            max_lev_snd = (max_lev_snd > j)?max_lev_snd:j;
        }
    }

    /* change arguments if used in switched order */
    if (found_in_fst && !found_in_snd)
    {
        c = fst_inner;
        fst_inner = snd_inner;
        snd_inner = c;
        c = fst_inner_cast;
        fst_inner_cast = snd_inner_cast;
        snd_inner_cast = c;
        i = max_lev_fst;
        max_lev_fst = max_lev_snd;
        max_lev_snd = i;
        switched_args = (switched_args)?false:true;
    }
    else if (!found_in_fst && found_in_snd)
    {
        /* do nothing */
    }
    /* change for-loop and if-expression if no reference is used */
    /* FIXME: this has to be checked
    else if (!found_in_fst && !found_in_snd && !max_lev_fst && !max_lev_snd)
    {
        if_node = R(for_node);
        res = PFcore_for (PFcore_wire2 (c_forbind,
                                        PFcore_forvars(LLL(for_node), 
                                                       LLR(for_node)),
                                        LR(for_node)),
                          res);
        if (RL(if_node)->kind == c_empty)
        {
            RR(if_node) = res;
        }
        else
        {
            RL(if_node) = res;
        }
        *for_node = *if_node;
        return join_found;
    }
    */
    else
    {
        return join_found;
    }

    /* add references of the for-loop variable */
    vi = var_lookup (LLL(for_node)->sem.var, active_vlist);
    if (!vi) mps_error ("could not lookup variable '%s' in join pattern testing.",
                        PFqname_str((LLL(for_node)->sem.var)->qname)); 
    for (i = 0; i < PFarray_last (vi->reflist); i++)
    {
        j = (*(var_info **) PFarray_at (vi->reflist, i))->act_lev;
        max_lev_snd = (max_lev_snd > j)?max_lev_snd:j;
    }
    /* don't need to test reflist of snd because it should be identical 
       if (LLR(for_node)->kind == c_var) */
    
    /* check independence */
    if (max_lev_snd < cur_level) /* we can be sure,
        that the second/right/inner side is completely independent */
    {
        /* remove variables from current scope */
        if (LLR(for_node)->kind == c_var)
        {
            /* move positional var from definition to active variable list */
            vipos = *(var_info **) PFarray_top (active_vlist);
            PFarray_del (active_vlist);
        }
        else
        {
            vipos = NULL;
        }
        /* move var from definition to active variable list */
        vi = *(var_info **) PFarray_top (active_vlist);
        PFarray_del (active_vlist);
        /* evaluate left side in scope-1 (cur_level not incremented yet) */
        recognize_join (fst_inner, active_vlist, active_vdefs, cur_level);
        if (max_lev_snd)
        {
            /* evaluate right side in the inner-most common scope
               (cur_level stays the same) */
            PFarray_t *new_active_vlist = PFarray (sizeof (var_info *));
            *(var_info **) PFarray_add (new_active_vlist)
                    = create_var_info(vi->parent, vi->id, cur_level);
            if (vipos)
                *(var_info **) PFarray_add (new_active_vlist)
                    = create_var_info(vipos->parent, vipos->id, cur_level);

            /* we also need all variables that are in or below the current scope
               (cur_level - 1) */
            for (i = 0; i < PFarray_last(active_vlist); i++)
            {
                if ((*(var_info **) PFarray_at (active_vlist, i))->act_lev < cur_level)
                {
                    *(var_info **) PFarray_add (new_active_vlist)
                        = *(var_info **) PFarray_at (active_vlist, i);
                }
            }

            recognize_join (snd_inner,
                            new_active_vlist,
                            PFarray (sizeof (var_info *)),
                            cur_level);
        } else {
            /* evaluate right side in scope 0 (cur_level is set back to 1) */
            PFarray_t *new_active_vlist = PFarray (sizeof (var_info *));
            *(var_info **) PFarray_add (new_active_vlist)
                    = create_var_info(vi->parent, vi->id, 1);
            if (vipos)
                *(var_info **) PFarray_add (new_active_vlist)
                    = create_var_info(vipos->parent, vipos->id, 1);

            /* we also need all variables in scope 0 */
            for (i = 0; i < PFarray_last(active_vlist); i++)
            {
                if ((*(var_info **) PFarray_at (active_vlist, i))->act_lev == 0)
                {
                    *(var_info **) PFarray_add (new_active_vlist)
                        = *(var_info **) PFarray_at (active_vlist, i);
                }
            }

            recognize_join (snd_inner,
                            new_active_vlist,
                            PFarray (sizeof (var_info *)),
                            1);
        }

        /* evaluate result in current scope (cur_level is now incremented) */
        *(var_info **) PFarray_add (active_vlist) = vi;
        if (vipos)
            *(var_info **) PFarray_add (active_vlist) = vipos;

        cur_level++;
        recognize_join (res, active_vlist, active_vdefs, cur_level);

        /* now body is evaluated */ 
        join_found = true;

        /* normal translation */
        snd_inner = PFcore_for (PFcore_wire2 (c_forbind,
                                              PFcore_forvars(LLL(for_node), 
                                                             LLR(for_node)),
                                              LR(for_node)),
                                snd_inner);
        *for_node = *create_join_function(
                         fst_inner, fst_inner_cast, max_lev_fst,
                         snd_inner, snd_inner_cast, max_lev_snd,
                         fun, switched_args, res);
    }
    
    return join_found;
}

/* TODO: add recognition for SELECT
static int test_select(PFcnode_t *c,
                       PFarray_t *active_vlist,
                       PFarray_t *active_vdefs,
                       int cur_level)
{
   assert(c);
   assert(active_vlist);
   assert(active_vdefs);
   cur_level = cur_level;
   return 0;
}
*/

/**
 * recognize_join: helper function for join recognition
 * walks through a Core expression and collects the variables
 * as well as triggers the join pattern testing
 *
 * @param c the observed Core expression
 * @param active_vlist the stack containing the active variables
 * @param active_vdefs the stack of active variable definitions
 * @cur_level the counter storing the current scope
 */
static void recognize_join(PFcnode_t *c, 
                           PFarray_t *active_vlist,
                           PFarray_t *active_vdefs,
                           int cur_level)
{
    unsigned int i;
    assert(c);
    var_info *var_struct;
    PFcnode_t *args;

    switch (c->kind)
    {
        case c_var:
            /* get var_info of current variable */
            var_struct = var_lookup (c->sem.var, active_vlist);

            if (!var_struct)
            {
                /* avoid errors for global variables */
                if (c->sem.var->global)
                {
                    /* start from nesting GLO_LEV because input arguments don't have
                       to be completely independent and therefore shouldn't
                       occur in scope 0 (which would be the default for function 
                       declarations) */
                    var_struct = create_var_info (c, c->sem.var, GLO_LEV);
                    *(var_info **) PFarray_add (active_vlist) = var_struct;
                }
                else
                {
                    mps_error ("could not lookup variable '%s' in join recognition.",
                               PFqname_str((c->sem.var)->qname)); 
                }
            }

            /* add reference to variable definitions */
            add_ref_to_vdef (var_struct, active_vdefs);
            break;
        case c_let:
            /* add var to the active variable definition list */
            /* not needed here (only variable binding for of patterns are later tested)
            *(var_info **) PFarray_add (active_vdefs) 
                = create_var_info (c, LL(c)->sem.var, cur_level);
            */

            /* call let binding */
            recognize_join (LR(c), active_vlist, active_vdefs, cur_level);

            /* move var from definition to active variable list */
            /* not needed here (only variable binding for of patterns are later tested)
            *(var_info **) PFarray_add (active_vlist)
                = *(var_info **) PFarray_top (active_vdefs);
            PFarray_del (active_vdefs);
            */ 
            /* instead needed here (only variable binding of join patterns are later tested) */
            *(var_info **) PFarray_add (active_vlist) 
                = create_var_info (c, LL(c)->sem.var, cur_level);

            /* TODO: in a later step test cur_level = min (active_vdefs->act_lev) and move let expression */

            /* call let body */
            recognize_join (R(c), active_vlist, active_vdefs, cur_level);

            /* delete variable from active list */
            PFarray_del (active_vlist);
            break;
        case c_for: 
            /* add var to the active variable definition list */
            *(var_info **) PFarray_add (active_vdefs) = create_var_info (c, LLL(c)->sem.var, cur_level+1);
            if (LLR(c)->kind != c_nil)
            {
                i = 1;
                /* add positional var to the active variable definition list */
                *(var_info **) PFarray_add (active_vdefs) = create_var_info (c, LLR(c)->sem.var, cur_level+1);
            }
            else i = 0;

            /* call for binding */
            recognize_join (LR(c), active_vlist, active_vdefs, cur_level);

            if (i)
            {
                /* move positional var from definition to active variable list */
                *(var_info **) PFarray_add (active_vlist) = *(var_info **) PFarray_top (active_vdefs);
                PFarray_del (active_vdefs);
            }
            /* move var from definition to active variable list */
            *(var_info **) PFarray_add (active_vlist) = *(var_info **) PFarray_top (active_vdefs);
            PFarray_del (active_vdefs);

            if (!test_join_pattern (c, active_vlist, active_vdefs, cur_level))
            {
                /* call for body */
                cur_level++;
                recognize_join (R(c), active_vlist, active_vdefs, cur_level);
            }

            /* delete variable from active list */
            PFarray_del (active_vlist);
            if (i)
            {
                /* delete positional variable from active list */
                PFarray_del (active_vlist);
            }
            break;
        case c_fun_decl:
            /* start from nesting 1 because input arguments don't have
               to be completely independent and therefore shouldn't
               occur in scope 0 (which would be the default for function 
               declarations) */

            args = L(c);
            while (args->kind != c_nil)
            {
                assert (L(args) && L(args)->kind == c_param);
                assert (LR(args) && LR(args)->kind == c_var);
 
                /* instead needed here (only variable binding of join patterns are later tested) */
                *(var_info **) PFarray_add (active_vlist) 
                    = create_var_info (c, LR(args)->sem.var, UDF_LEV);
 
                args = R(args);
            }

            /* call function body */
            recognize_join (R(c), active_vlist, active_vdefs, UDF_LEV);

            args = L(c);
            while (args->kind != c_nil)
            {
                /* delete variable from active list */
                PFarray_del (active_vlist);
                args = R(args);
            }
            break;
        case c_if:
            /* TODO: add recognition for SELECT
            if (test_select (c, active_vlist, active_vdefs, cur_level))
            {
                break;
            }
            else
            */
            {
                /* in the current translation all if-then-else expressions
                   introduce a new scope. The join recognition should also 
                   know about them */ 
                recognize_join (L(c), active_vlist, active_vdefs, cur_level);
                cur_level++;
                recognize_join (R(c), active_vlist, active_vdefs, cur_level);
            }
            break;
        default:
            for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++) {
                recognize_join (c->child[i], active_vlist, active_vdefs, cur_level);
            }
            break;
    }
}

/* the fid increasing for every for node */ 
#define FID 0
/* the actual fid saves the last active fid, to prune the 
   scopes of the later used variables */
#define ACT_FID 1
/* the vid increasing for every new variable binding */
#define VID 2

/**
 * in update_expansion for a variable usage all fids between
 * the definition of the variable and its usage are added
 * to the var_usage bat
 *
 * @param f the Stream the MIL code is printed to
 * @param c the variable core node
 * @param way the path of the for ids (active for-expression)
 */
static void
update_expansion (opt_t *f, PFcnode_t *c,  PFarray_t *way)
{
    int m;
    PFvar_t *var;

    assert(c->sem.var);
    var = c->sem.var;

    for (m = PFarray_last (way) - 1; m >= 0 
         && *(int *) PFarray_at (way, m) > var->base; m--)
    {
        milprintf(f,
                "var_usage.insert(%i@0,%i@0);\n",
                f->module_base + var->vid,
                f->module_base + *(int *) PFarray_at (way, m));
    }
}

static PFarray_t *
walk_through_UDF (opt_t *f, 
                  PFcnode_t *c,
                  PFarray_t *way,
                  PFarray_t *counter,
                  PFarray_t *active_funs)
{
    unsigned int i;
    int vid;
    bool found;
        
    if (c->kind == c_var && c->sem.var->global) 
    {
        if (!c->sem.var->vid  && c->sem.var->global)
        {
            /* give them a vid and print the whole way
               (from scope 0 inside function declaration) */
            (*(int *) PFarray_at (counter, VID))++;
            vid = *(int *) PFarray_at (counter, VID);
            c->sem.var->base = 0; /* global variables are defined in scope 0 */
            c->sem.var->vid = vid;
            c->sem.var->used = 0;
        }
        else if (!c->sem.var->vid)
        {
            mps_error ("missing variable identifier in variable id assignment.");
        }

        /* inserts fid|vid combinations into var_usage bat */
        update_expansion (f, c, way);
        /* the field used is for pruning the MIL code and
           avoid translation of variables which are later
           not used */
        c->sem.var->used += 1;
    }
    else if (c->kind == c_for)
    {
        counter = walk_through_UDF (f, LR(c), way, counter, active_funs);
        
        /* add all the fids, which have to be passed on the way */
        *(int *) PFarray_add (way) = c->sem.flwr.fid;
        counter = walk_through_UDF (f, R(c), way, counter, active_funs);
        PFarray_del (way);
    }
    else if (c->kind == c_apply && 
             !c->sem.fun->builtin)
    {
        counter = walk_through_UDF (f, D(c), way, counter, active_funs);

        /* look up wether function is already active and tested 
           (to avoid infinite recursion */
        for (i = 0, found = false; i < PFarray_last (active_funs); i++)
        {
            if (*(PFfun_t **) PFarray_at (active_funs, i) == c->sem.fun)
            {
                found = true;
                break;
            } 
        }
        /* add new active function to the stack and once again check
           to map all global variables correctly */
        if (!found)
        {
            *(PFfun_t **) PFarray_add (active_funs) = c->sem.fun;
            /* add new active function to an active function stack and once
               again check UDFs to map all global variables correctly */
            *(int *) PFarray_add (way) = c->sem.fun->fid;
            counter = walk_through_UDF (f, c->sem.fun->core,
                                        way, counter, active_funs);
            PFarray_del (way);
            PFarray_del (active_funs);
        }
        
    } 
    else 
    {
        for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
            counter = walk_through_UDF (f, c->child[i], way, counter, active_funs);
    }
    return counter;
}

int PFqueryType(PFcnode_t *c) {
    return PFty_subtype (TY(c), PFty_empty ())
                                ? 0		       /* only the empty sequence has type empty and is not updateable */
                                : (PFty_subtype(TY(c), PFty_star(PFty_stmt()))
                                   ? 1
                                   : (PFty_subtype (TY(c),
                                                    PFty_star (PFty_docmgmt ()))
                                      ? 2
                                      : 0));
}

/**
 * in get_var_usage for each variable a vid (variable id) and
 * for each for expression a fid (for id) is added;
 * for each variable usage the needed fids are added to a 
 * bat 'var_usage'
 *
 * @param f the Stream the MIL code is printed to
 * @param c a core tree node
 * @param way an array containing the path of the for ids
 *        (active for-expression)
 * @param counter an array containing 3 values for fid, act_fid and vid
 * @return the updated counter
 */
static PFarray_t *
get_var_usage (opt_t *f, PFcnode_t *c,  PFarray_t *way, PFarray_t *counter)
{
    unsigned int i;
    int fid, act_fid, vid;
    PFcnode_t *args, *fst, *snd, *res, *fid_node;
    int fst_nested, snd_nested;

    if (c->kind == c_var) 
    {
        /* cope with global variables that are used before their declaration */
        if (!c->sem.var->vid  && c->sem.var->global)
        {
            /* give them a vid and print the whole way
               (from scope 0 inside function declaration) */
            (*(int *) PFarray_at (counter, VID))++;
            vid = *(int *) PFarray_at (counter, VID);
            c->sem.var->base = 0; /* global variables are defined in scope 0 */
            c->sem.var->vid = vid;
            c->sem.var->used = 0;
        }
        else if (!c->sem.var->vid)
        {
            mps_error ("missing variable identifier occurred "
                       "during variable usage checking.");
        }

       /* inserts fid|vid combinations into var_usage bat */
       update_expansion (f, c, way);
       /* the field used is for pruning the MIL code and
          avoid translation of variables which are later
          not used */
       c->sem.var->used += 1;
    }

    /* only in for and let variables can be bound */
    else if (c->kind == c_for)
    {
       /* retrieve variable usage in for loop binding */
       if (LR(c))
           counter = get_var_usage (f, LR(c), way, counter);
       
       /* increase FID as we have a new for loop scope */
       (*(int *) PFarray_at (counter, FID))++;
       fid = *(int *) PFarray_at (counter, FID);

       /* save fid to allow reference in mil code generation */
       c->sem.flwr.fid = fid;
       /* add fid also to the active for loop stack */
       *(int *) PFarray_add (way) = fid;
       act_fid = fid;

       /* create new variable id for for loop variable */
       (*(int *) PFarray_at (counter, VID))++;
       vid = *(int *) PFarray_at (counter, VID);
       LLL(c)->sem.var->base = act_fid;
       LLL(c)->sem.var->vid = vid;
       LLL(c)->sem.var->used = 0;

       /* create new variable id for positional for loop variable */
       if (LLR(c)->kind == c_var)
       {
            (*(int *) PFarray_at (counter, VID))++;
            vid = *(int *) PFarray_at (counter, VID);
            LLR(c)->sem.var->base = act_fid;
            LLR(c)->sem.var->vid = vid;
            LLR(c)->sem.var->used = 0;
       }

       /* ... after all preparations retrieve used 
          variables in the return clause */
       if (R(c))
           counter = get_var_usage (f, R(c), way, counter);
       
       /* set back active for loop stack */
       *(int *) PFarray_at (counter, ACT_FID) = *(int *) PFarray_top (way);
       PFarray_del (way);
    }
    else if (c->kind == c_let)
    {
        /* retrieve variable usage in let variable binding */
        if (LR(c))
            counter = get_var_usage (f, LR(c), way, counter);

        /* create new variable id for let expression */
        /* exceptions might be global variables that were already
           initialized by there usage in UDFs */
        if (!LL(c)->sem.var->vid)
        {
            act_fid = *(int *) PFarray_at (counter, ACT_FID);
            (*(int *) PFarray_at (counter, VID))++;
            vid = *(int *) PFarray_at (counter, VID);
            LL(c)->sem.var->base = act_fid;
            LL(c)->sem.var->vid = vid;
            LL(c)->sem.var->used = 0;
        }

        /* retrieve variable usage in let body */
        if (R(c))
            counter = get_var_usage (f, R(c), way, counter);
    }
    /* apply mapping correctly for recognized join */    
    else if (c->kind == c_apply && 
             !PFqname_eq(c->sem.fun->qname, PFqname (PFns_pf,"join")))
    {
        /* get all necessary Core nodes */
            args = D(c);
        fst = L(args);
            args = RR(args);
        fst_nested = L(args)->sem.num;
            args = R(args);
        snd = L(args);
            args = RR(args);
        snd_nested = L(args)->sem.num;
            args = RRR(args);
        res = L(args);
            args = R(args);
        fid_node = L(args);
            args = NULL;

        if (fst_nested == GLO_LEV || snd_nested == GLO_LEV)
            PFoops (OOPS_FATAL,
                    "can't cope with global variables as "
                    "thetajoin input within user-defined function calls.");
    
        /* save current fid */
        act_fid = *(int *) PFarray_at (counter, ACT_FID);

        counter = get_var_usage (f, fst, way, counter);

        if (PFarray_last (way)) {
            /* remove the inner most for loop and get
               the variables for the second argument */
            fid = *(int *) PFarray_top (way);
            PFarray_del (way);
            counter = get_var_usage (f, snd, way, counter);
            /* restore the list of for ids */
            *(int *) PFarray_add (way) = fid;
        } else
            counter = get_var_usage (f, snd, way, counter);

        /* create new fid for resulting scope */
        (*(int *) PFarray_at (counter, FID))++;
        fid = *(int *) PFarray_at (counter, FID);

        fid_node->sem.num = fid;
        *(int *) PFarray_add (way) = fid;
        act_fid = fid;

        counter = get_var_usage (f, res, way, counter);

        *(int *) PFarray_at (counter, ACT_FID) = *(int *) PFarray_top (way);
        PFarray_del (way);
    }
    else if (c->kind == c_fun_decl)
    {
        /* ==================================== */
        /* create a new valid PROC name for mil */
        /* ==================================== */

        char  sig[1024],
             *p = PFqname_loc (c->sem.fun->qname),
             *q = PFqname_uri (c->sem.fun->qname);
        char *r = PFqname_prefix (c->sem.fun->qname);
        int i = 0, j, first = 0;
        unsigned int hash = 0; /* f->module_base; */
        int stmt = PFqueryType(R(c));

        /* hash uri in proc name to make it uniquely identifyable  */
        if (r) for(; *r; r++)
                hash = (hash*3) + *(unsigned char*) r;

        if (q == NULL) q = "";
            args = L(c);

        sig[0] = 0; /* we append all parameter types here */

        /* ============================= */
        /* initialize function variables */
        /* ============================= */
        while (args->kind != c_nil)
        {
            assert (L(args) && L(args)->kind == c_param);
            assert (LR(args) && LR(args)->kind == c_var);

            act_fid = *(int *) PFarray_at (counter, ACT_FID);
            (*(int *) PFarray_at (counter, VID))++;
            vid = *(int *) PFarray_at (counter, VID);
            LR(args)->sem.var->base = act_fid;
            LR(args)->sem.var->vid = vid;
            LR(args)->sem.var->used = 0;

            if (first == 0) first = vid;
        strncat(sig, ",", 1024);
            strncat(sig, PFty_str(TY(LR(args))), 1024);

            args = R(args);
        }
        /* create the full signature that also is a valid MIL identifier */
        c->sem.fun->sig = PFmalloc(12+3*(strlen(sig)+strlen(p)));

        /* hash uri in proc name to make it uniquely identifyable  */
        for(; *q; q++) 
            hash = (hash*3) + *(unsigned char*) q;

        for(j=11; *p; p++) {
            /* escape '_' by '__' and '-' by '_4_' and '.' by '_5_' 
             * (_4_ cannot be a subsequent type name; those always end in [0-3])
             */ 
            char x = (*p == '-' || *p == '.')?'_':*p;
            c->sem.fun->sig[j++] = x; 
            hash = (hash*3) + *(unsigned char*) p;
            if (*p == '-') c->sem.fun->sig[j++] = '4';
            else if (*p == '.') c->sem.fun->sig[j++] = '5';
            if (x == '_') c->sem.fun->sig[j++] = '_';
        }

        while(sig[i++] == ',') {
            int ch = 0;
            c->sem.fun->sig[j++] = '_';
            while (sig[i] && sig[i] != ',') { 
                hash = (hash*3) + *(unsigned char*) (sig+i);
                ch = sig[i++];
                if (ch == '_' || ch == '{' || ch == '}' || ch == '(' || ch == ')')  { 
                    c->sem.fun->sig[j++] = '_';
                    c->sem.fun->sig[j++] = '_';
                } else if (ch == ':') {
                    c->sem.fun->sig[j++] = '_';
                } else if (ch == '?') {
                    c->sem.fun->sig[j++] = '0';
                } else if (ch == '*') {
                    c->sem.fun->sig[j++] = '2';
                } else if (ch == '+') {
                    c->sem.fun->sig[j++] = '3';
                } else if (ch != ' ') {
                    c->sem.fun->sig[j++] = ch;
                }
                    }
            if (c->sem.fun->sig[j-1] != '0' && 
                c->sem.fun->sig[j-1] != '2' && 
                c->sem.fun->sig[j-1] != '3') 
            {
                c->sem.fun->sig[j++] = '1';
            }
        }

        /* ============================================== */
        /* retrieve variable occurrences in function body */
        /* ============================================== */

        /* finish name by printing start (hashed name), connecting and terminating it */
        sprintf(c->sem.fun->sig, "%s%08X", (stmt==1)?"up":(stmt==2)?"dm":"fn", hash);
        c->sem.fun->sig[10] = '_';
        c->sem.fun->sig[j] = 0;

        if (f->module_base || f->num_fun) {
                 milprintf(f, "proc_vid.insert(\"%s\", %dLL);\n", c->sem.fun->sig, f->module_base+first);
                 f->num_fun--;
        }
        counter = get_var_usage (f, R(c), PFarray (sizeof (int)), counter);
    }
    /* apply mapping correctly for user defined function calls */    
    else if (c->kind == c_apply && 
             !c->sem.fun->builtin)
    {
        /* get variable occurrences of the input arguments */
        counter = get_var_usage (f, D(c), way, counter);

        if (!c->sem.fun->fid) /* create fid for UDF on demand */
        {
            /* give fun_decl a fid to map global variables */
            (*(int *) PFarray_at (counter, FID))++;
            c->sem.fun->fid = *(int *) PFarray_at (counter, FID);
        }

        /* add new active function to an active function stack and once
           again check UDFs to map all global variables correctly */
        *(int *) PFarray_add (way) = c->sem.fun->fid;
        /* create a new stack with active UDFs to avoid endless recursion */
        PFarray_t *active_funs = PFarray (sizeof (PFvar_t *));
        *(PFfun_t **) PFarray_add (active_funs) = c->sem.fun;

        counter = walk_through_UDF (f, c->sem.fun->core, 
                                    way, counter, active_funs);

        PFarray_del (active_funs);
        PFarray_del (way);
    } 
    else 
    {
       for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
          counter = get_var_usage (f, c->child[i], way, counter);
    } 

    return counter;
}

const char* PFinitMIL(void) {
    return 
        "module(\"pathfinder\");\n"
#ifdef HAVE_PFTIJAH
	"\n"
	"var tID := 9999@0; # start counter at an arbitrary number\n"
	"var tijah_tID   := new(void,oid).seqbase(0@0);\n"
	"var tijah_frag  := new(void,oid).seqbase(0@0);\n"
	"var tijah_pre   := new(void,oid).seqbase(0@0);\n"
	"var tijah_score := new(void,dbl).seqbase(0@0);\n"
	"var tijah_resultsz := new(lng,lng);\n"
	"var tijah_lock  := lock_nil; # pftijah collection lock\n"
#endif
        "\n"
        "# value containers for literal values\n"
        "var int_values := bat(lng,void).key(true).reverse().seqbase(0@0);\n"
        "var dbl_values := bat(dbl,void).key(true).reverse().seqbase(0@0);\n"
        "var dec_values := dbl_values;\n"
        "var str_values := bat(str,void).key(true).reverse().seqbase(0@0).append(\"\");\n"
        "\n"
        "var fun_vid000 := bat(void,oid);\n"
        "var fun_iter000 := bat(void,oid);\n"
        "var fun_item000 := bat(void,oid);\n"
        "var fun_kind000 := bat(void,int);\n"
        "\n"
        "var var_usage := bat(oid,oid);\n"
        "var proc_vid := bat(str,lng);\n"
        "\n"
        "var loop000 := bat(void,oid,1).seqbase(0@0).append(1@0);\n"
        "\n"
        "# variable that holds bat-id (int) of a shredded document that may be added to the ws\n"
        "var shredBAT := int_nil; # make sure that shredBAT is of type int; non-initialized MIL variables are void(nil)!\n"
        "var time_compile := 0LL;\n"
        "var time_read := 0LL;\n"
        "var time_shred := 0LL;\n"
        "var time_print := 0LL;\n"
        "var time_exec := 0LL;\n"
        "var time_start := 0LL;\n"
        "# To print XRPC response message, we need to know the module\n"
        "# and the method specified in the request message.\n"
        "var moduleNS := str_nil;\n"
        "var method := str_nil;\n"
        "var genType := \"xml\";\n";
}

const char* PFstandoffMIL(void) {
    return
        "module(\"pf_standoff\"); # load StandOff axis support\n";
}

const char* PFvarMIL(void) {
    return
        "# volatile variable environment\n"
        "var v_vid000;\n"
        "var vu_fid;\n"
        "var vu_vid;\n"
        "\n"
        "# variables for (intermediate) results\n"
        "var ipik;\n"
        "var iter;\n"
        "var pos;\n"
        "var item;\n"
        "var kind;\n"
        "\n"
        "# variables for results containing `real' values\n"
        "var item_int_;\n"
        "var item_dec_;\n"
        "var item_dbl_;\n"
        "var item_str_;\n"
        "\n"
        "# variables for results containing `real' xml subtrees\n"
        "var _elem_iter;  # oid|oid\n"
        "var _elem_size;  # oid|int\n"
        "var _elem_level; # oid|chr\n"
        "var _elem_kind;  # oid|chr\n"
        "var _elem_prop;  # oid|oid\n"
        "var _elem_cont;  # oid|oid\n"
        "\n"
        "var _attr_iter; # oid|oid\n"
        "var _attr_qn;   # oid|oid\n"
        "var _attr_prop; # oid|oid\n"
        "var _attr_cont; # oid|oid\n"
        "var _attr_own;  # oid|oid\n"
        "\n"
        "var _r_attr_iter; # oid|oid\n"
        "var _r_attr_qn;   # oid|oid\n"
        "var _r_attr_prop; # oid|oid\n"
        "var _r_attr_cont; # oid|oid\n"
        "\n"
        "# environment that represents start of any query\n"
        "var v_iter000;\n"
        "var v_item000;\n"
        "var v_kind000;\n"
	"var outer000;\n"
        "var inner000;\n"
        "var order_000;\n";
}

#define PF_STARTMIL_START \
        "time_read := 0LL;\n"\
        "time_shred := 0LL;\n"\
        "time_print := 0LL;\n"\
        "time_exec := 0LL;\n"\
        "time_start := usec();\n"
#define PF_STARTMIL_NORMAL(STMT) PF_STARTMIL_START\
        "var err;\n"\
        "{ var ws := empty_bat;\n"\
        "  err := CATCH({\n"\
        "  ws := ws_create(" STMT ");\n" PF_STARTMIL_END
#define PF_STARTMIL_UPDATE PF_STARTMIL_START\
        "var try := 1;\n"\
        "var err := \"!ERROR: conflicting update\";\n"\
        "var ws_log_wsid := 0LL;\n"\
        "while(((try :+= 1) <= 3) and not(isnil(err))) {\n"\
        " if (not(err.startsWith(\"!ERROR: conflicting update\"))) break;\n"\
        " var ws := empty_bat;\n"\
        " err := CATCH({\n"\
        "  ws := ws_create(try);\n"\
        "  if (ws_log_active and bit(ws_log_wsid)) ws_log(ws, \"restarted \" + str(ws_log_wsid));\n" PF_STARTMIL_END
#define PF_STARTMIL_END \
        "  # get full picture on var_usage (and sort it)\n"\
        "  var usage := var_usage.unique().reverse().access(BAT_READ);\n"\
        "  vu_fid := usage.hmark(1000@0);\n"\
        "  vu_vid := usage.tmark(1000@0);\n"\
        "  usage := vu_fid.tsort();\n"\
        "  usage := usage.CTrefine(vu_vid);\n"\
        "  usage := usage.hmark(1000@0);\n"\
        "  vu_vid := usage.leftfetchjoin(vu_vid);\n"\
        "  vu_fid := usage.leftfetchjoin(vu_fid);\n"\
        "\n"\
        "  inner000 := loop000;\n"\
        "  outer000 := loop000.project(1@0);\n"\
        "  order_000 := outer000;\n"
const char* PFstartMIL(int statement_type) {
    return (statement_type==1)?
               (PF_STARTMIL_UPDATE):
               (statement_type==0)?(PF_STARTMIL_NORMAL("0")):
                                   (PF_STARTMIL_NORMAL("1"));
}

/* debug statement for PFstopMIL to print result set 
"if (genType.search(\"debug\") >= 0) print(item.slice(0,10).col_name(\"tot_items_\"+str(item.count())));\n" 
*/

#ifdef HAVE_PFTIJAH
#define PF_STOP_PFTIJAH " if (not(isnil(tijah_lock))) lock_unset(tijah_lock);\n"
#define PF_PLAY_TIJAH_TAPE " tj_play_doc_tape(ws, item.materialize(ipik), kind.materialize(ipik), int_values, str_values);\n"
#else
#define PF_STOP_PFTIJAH    " \n"
#define PF_PLAY_TIJAH_TAPE " \n"
#endif

#define PF_STOPMIL_START \
           "  time_print := usec();\n"\
           "  time_exec := time_print - time_start;\n"
#define PF_STOPMIL_RDONLY PF_STOPMIL_START\
           "  # 'none' could theoretically occur in genType as root tagname ('xml-root-none'), so check for 'xml'\n"\
           "  if ((genType.search(\"none\") < 0) or (genType.search(\"xml\") >= 0))\n"\
           "   print_result(genType,moduleNS,method,ws,tunique(iter),constant2bat(iter),item.materialize(ipik),constant2bat(kind),int_values,dbl_values,str_values);\n"\
           PF_STOPMIL_END("Print ")
#define PF_STOPMIL_UPDATE PF_STOPMIL_START\
           "  play_update_tape(ws, item.materialize(ipik), kind.materialize(ipik), int_values, str_values);\n" PF_STOPMIL_END("Update")
#define PF_STOPMIL_DOCMGT PF_STOPMIL_START\
           "  play_doc_tape(ws, item.materialize(ipik), kind.materialize(ipik), int_values, str_values);\n" PF_PLAY_TIJAH_TAPE PF_STOPMIL_END("Update")
#define PF_STOPMIL_END(LASTPHASE) \
           " });\n"\
           " ws_log_wsid := ws_id(ws);\n"\
           " if (not(isnil(err))) ws_log(ws, err);\n"\
           " ws_destroy(ws);\n"\
           "}\n"\
	   PF_STOP_PFTIJAH\
           "if (not(isnil(err))) ERROR(err);\n"\
           "else if (genType.startsWith(\"timing\"))\n"\
           "  printf(\"\\nTrans  %% 10.3f msec\\nShred  %% 10.3f msec\\nQuery  %% 10.3f msec\\n" LASTPHASE " %% 10.3f msec\\n\","\
           "      dbl(time_compile)/1000.0, dbl(time_shred)/1000.0, dbl(time_exec - time_shred)/1000.0, dbl(time_print := usec() - time_print)/1000.0);\n"
const char* PFstopMIL(int statement_type) {
    return (statement_type==0)?
               (PF_STOPMIL_RDONLY):
           (statement_type==1)?
               (PF_STOPMIL_UPDATE):
               (PF_STOPMIL_DOCMGT);
}

const char* PFudfMIL(void) {
    return  
        "{\n"
        "  var proc_res := %s(loop%03u, outer%03u, order_%03u, inner%03u, fun_vid%03u, fun_iter%03u, fun_item%03u, fun_kind%03u); #%s\n"
        "  iter := proc_res.fetch(0);\n"
        "  item := proc_res.fetch(1);\n"
        "  kind := proc_res.fetch(2);\n"
        "  if (type(iter) = bat) {\n"
        "    ipik := iter;\n"
        "  } else {\n"
        "    if (type(item) = bat) {\n"
        "      ipik := item;\n"
        "    } else {\n"
        "      ipik := kind;\n"
        "    }\n"
        "  }\n"
        "}\n";
}

/**
 * expand_flwr starts from the root and removes all appearances
 * of c_flwr. Every end of a flwr list (c_nil) is replaced by
 * the resulting expression of the flwr (R(c)).
 */
static void 
expand_flwr (PFcnode_t *c, PFcnode_t *ret)
{
    assert (c);

    switch (c->kind) {
        case c_flwr:
            /* expand flwrs in return expression */
            expand_flwr (R(c), NULL);

            if (L(c)->kind == c_nil) {
                *c = *(R(c));
            } else {
                /* expand current flwr in bindings */
                expand_flwr (L(c), R(c));
                *c = *(L(c));
            }
            break;

        case c_let:
        case c_for:
            /* expand bindings */
            expand_flwr (L(c), NULL);

            assert (c->sem.flwr.quantifier);
            /* type for and let expressions */
            TY(c) = *PFty_simplify (c->sem.flwr.quantifier (
                                        PFty_defn (TY(ret))));
    
            if (R(c)->kind == c_nil)
                R(c) = ret;
            else 
                expand_flwr (R(c), ret);
            break;

        default:
            for (unsigned int i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
                expand_flwr (c->child[i], ret);

    }
}


/**
 * first MIL generation from Pathfinder Core
 *
 * first to each `for' and `var' node additional
 * information is appended. With this information
 * the core tree is translated into MIL.
 *
 * @param f the Stream the MIL code is printed to
 * @param c the root of the core tree
 */
int
PFprintMILtemp (PFcnode_t *c, int optimize, int module_base, int num_fun, long timing, 
                char** prologue, char** query, char** epilogue, char* url, bool standoff)
{
    PFarray_t *way, *counter;
    opt_t *f = opt_open(optimize);
    int stmt = PFqueryType(R(c)); 

    /* hack: milprint_summer state, not mil_opt state */
    f->num_fun = num_fun;     /* for queries: the amount of functions in the query itself (if any); used to ignore module functions */
    f->module_base = module_base; /* only generate mil module; no query */
    f->url = url; /* url of this query / module definition */ 

    way = PFarray (sizeof (int));
    counter = PFarray (sizeof (int));
    *(int *) PFarray_add (counter) = 0;  
    *(int *) PFarray_add (counter) = 0; 
    *(int *) PFarray_add (counter) = 0;

    /* remove all flwr expressions */
    expand_flwr (c, NULL);

    /* resolves nodes, which are not supported and prunes
       code which is not needed (e.g. casts, let-bindings) */
    simplifyCoreTree (c);

    recognize_join (c,
                    PFarray (sizeof (var_info *)),
                    PFarray (sizeof (var_info *)),
                    0);

    if (module_base == 0 && num_fun == -1) {
    	opt_output(f, OPT_SEC_PROLOGUE);
    }
    milprintf(f, PFinitMIL()); 

    if (standoff) {
	milprintf(f, PFstandoffMIL());
    }

    milprintf(f, PFvarMIL()); 

    /* get_var_usage appends information to the core nodes and creates a 
     * var_usage table, which is later split in vu_fid and vu_vid */
    opt_output(f, OPT_SEC_PROLOGUE);
    get_var_usage (f, c, way, counter);
    f->num_fun = num_fun;     /* reassign */

    opt_output(f, OPT_SEC_QUERY);

    /* define working set and all other MIL context (global vars for the query) */
    if (module_base == 0) {
        milprintf(f, PFstartMIL(stmt));
        milprintf(f, "  var v_vid000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n"
                     "  var v_iter000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n"
                     "  var v_pos000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n"
                     "  var v_item000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n"
                     "  var v_kind000 := bat(void,int).access(BAT_APPEND).seqbase(0@0);\n");
    }
        
    /* recursive translation of the core tree */
    translate2MIL (f, 0, 0, 0, c);

    if (module_base == 0) {
        timing = PFtimer_stop(timing);
        milprintf(f, "iter := 1@0;\n");
        milprintf(f, "time_compile := %dLL;\n" , timing);
        milprintf(f, PFstopMIL(stmt));
    }

    if (opt_close(f, prologue, query, epilogue)) {
        PFoops (OOPS_FATAL, "Out-of-memory while generating MIL.\n");
        return -1;
    }
    return 0;
}
