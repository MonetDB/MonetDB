/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder's implementation of the XQuery static type system.
 *
 * Pathfinder follows a _structural typing_ discipline, similar in
 * style to the XDuce type system [1] and the W3C XQuery proposals
 * prior to the November 15, 2002 Working Draft.
 *
 * References
 *
 * [1] Benjamin Pierce, Haruo Hosoya, Regular Expression Types for
 *     XML, ICFP, September 2000 (full version in ACM TOPLAS), see
 *     http://xduce.sourceforge.net/.
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

#include "pf_config.h"
#include "pathfinder.h"

#include <assert.h>
#include <string.h>

#include "types.h"

/* PFty_eq && PFty_subtype */
#include "subtyping.h"
/* PFqname () */
#include "qname.h"
/* PFns_xs */
#include "ns.h"
/* PFarray_t */
#include "array.h"
/* PFenv_t */
#include "env.h"
#include "mem.h"
#include "oops.h"
/* PFfun_env */
#include "functions.h"


/**
 * The XML Schema Symbol Spaces (see XML Schema, Part 1, 2.5):
 * - type definitions
 * - element declarations
 * - attribute declarations
 * - model group definitions
 * - attribute group definitions
 *
 * A named type belongs to exactly one of these symbol spaces
 * (indicated by its @c sym_space field).
 */
PFenv_t *PFtype_defns;
PFenv_t *PFelem_decls;
PFenv_t *PFattr_decls;
PFenv_t *PFgroup_defns;
PFenv_t *PFattrgroup_defns;

/**
 * Types.
 */
PFty_t
PFty_none (void)
{
    PFty_t t = { .type  = ty_none,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_empty (void)
{
    PFty_t t = { .type  = ty_empty,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_item (void)
{
    PFty_t t = { .type  = ty_item,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_untypedAny (void)
{
    PFty_t t = { .type  = ty_untypedAny,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_atomic (void)
{
    PFty_t t = { .type  = ty_atomic,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_untypedAtomic (void)
{
    PFty_t t = { .type  = ty_untypedAtomic,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_numeric (void)
{
    PFty_t t = { .type  = ty_numeric,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_node (void)
{
    PFty_t t = { .type  = ty_node,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_text (void)
{
    PFty_t t = { .type  = ty_text,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_pi (char *target)
{
    PFty_t t = { .type  = ty_pi,
                 .name  = PFqname (PFns_wild, target),
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_comm (void)
{
    PFty_t t = { .type  = ty_comm,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

static PFty_t
PFty_integer (void)
{
    PFty_t t = { .type  = ty_integer,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

static PFty_t
PFty_decimal (void)
{
    PFty_t t = { .type = ty_decimal,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

static PFty_t
PFty_double (void)
{
    PFty_t t = { .type = ty_double,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

static PFty_t
PFty_string (void)
{
    PFty_t t = { .type = ty_string,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

static PFty_t
PFty_boolean (void)
{
    PFty_t t = { .type = ty_boolean,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

static PFty_t
PFty_qname (void)
{
    PFty_t t = { .type = ty_qname,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_stmt (void)
{
    PFty_t t = { .type  = ty_stmt,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_docmgmt (void)
{
    PFty_t t = { .type  = ty_docmgmt,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

/**
 * 11 Date/Time types
 */

PFty_t
PFty_datetime (void)
{
    PFty_t t = { .type  = ty_datetime,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_date (void)
{
    PFty_t t = { .type  = ty_date,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_time (void)
{
    PFty_t t = { .type  = ty_time,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_gyearmonth (void)
{
    PFty_t t = { .type  = ty_gyearmonth,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_gyear (void)
{
    PFty_t t = { .type  = ty_gyear,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_gmonthday (void)
{
    PFty_t t = { .type  = ty_gmonthday,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_gmonth (void)
{
    PFty_t t = { .type  = ty_gmonth,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_gday (void)
{
    PFty_t t = { .type  = ty_gday,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_duration (void)
{
    PFty_t t = { .type  = ty_duration,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_yearmonthduration (void)
{
    PFty_t t = { .type  = ty_yearmonthduration,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

PFty_t
PFty_daytimeduration (void)
{
    PFty_t t = { .type  = ty_daytimeduration,
                 .name  = 0,
                 .sym_space = 0,
                 .child = { 0 }
    };

    return t;
}

/**
 * Named types (the defn of these types is found in the
 * XML Schema type definitions symbol space).
 */

/** named qn */
PFty_t
PFty_named (PFqname_t qn)
{
    PFty_t ty = { .type      = ty_named,
                  .name      = qn,
                  .sym_space = PFtype_defns,
                  .child     = { 0, 0 }
    };

    assert (PFtype_defns);

    return ty;
}

/**
 * Reference to named element declaration (to be found in the
 * XML Schema element declarations symbol space).
 */

/** named qn */
PFty_t
PFty_named_elem (PFqname_t qn)
{
    PFty_t ty = { .type      = ty_named,
                  .name      = qn,
                  .sym_space = PFelem_decls,
                  .child     = { 0, 0 }
    };

    assert (PFelem_decls);

    return ty;
}

/**
 * Reference to named attribute declaration (to be found in the
 * XML Schema attribute declarations symbol space).
 */

/** named qn */
PFty_t
PFty_named_attr (PFqname_t qn)
{
    PFty_t ty = { .type      = ty_named,
                  .name      = qn,
                  .sym_space = PFattr_decls,
                  .child     = { 0, 0 }
    };

    assert (PFattr_decls);

    return ty;
}

/**
 * Reference to named model group (to be found in the
 * XML Schema group definitions symbol space).
 */

/** named qn */
PFty_t
PFty_named_group (PFqname_t qn)
{
    PFty_t ty = { .type      = ty_named,
                  .name      = qn,
                  .sym_space = PFgroup_defns,
                  .child     = { 0, 0 }
    };

    assert (PFgroup_defns);

    return ty;
}

/**
 * Reference to named attribute group (to be found in the
 * XML Schema attribute group definitions symbol space).
 */

/** named qn */
PFty_t
PFty_named_attrgroup (PFqname_t qn)
{
    PFty_t ty = { .type      = ty_named,
                  .name      = qn,
                  .sym_space = PFattrgroup_defns,
                  .child     = { 0, 0 }
    };

    assert (PFattrgroup_defns);

    return ty;
}

/**
 * Type constructors.
 */

/**
 * Implements the 1 quantifier: t . 1 = t
 */
PFty_t
PFty_one (PFty_t t)
{
    return t;
}

/** ty? */
PFty_t
PFty_opt (PFty_t t)
{
    PFty_t ty = { .type  = ty_opt,
                  .name  = 0,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t;

    return ty;
}

/** ty+ */
PFty_t
PFty_plus (PFty_t t)
{
    PFty_t ty = { .type  = ty_plus,
                  .name  = 0,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t;

    return ty;
}

/** ty* */
PFty_t
PFty_star (PFty_t t)
{
    PFty_t ty = { .type  = ty_star,
                  .name  = 0,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t;

    return ty;
}

/** t1,t2
 *  NB. try to make sure to construct left-deep chains of applications of (,)
 */
PFty_t
PFty_seq (PFty_t t1, PFty_t t2)
{
    PFty_t ty = { .type  = ty_seq,
                  .name  = 0,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    ty.child[1] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t1;
    *(ty.child[1]) = t2;

    return ty;
}

/** t1|t2
 *  NB. try to make sure to construct left-deep chains of applications of (|)
 */
PFty_t
PFty_choice (PFty_t t1, PFty_t t2)
{
    PFty_t ty = { .type  = ty_choice,
                  .name  = 0,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    ty.child[1] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t1;
    *(ty.child[1]) = t2;

    return ty;
}

/** t1&t2
 *  NB. try to make sure to construct left-deep chains of applications of (&)
 */
PFty_t
PFty_all (PFty_t t1, PFty_t t2)
{
    PFty_t ty = { .type  = ty_all,
                  .name  = 0,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    ty.child[1] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t1;
    *(ty.child[1]) = t2;

    return ty;
}

/** elem qn { t } */
PFty_t
PFty_elem (PFqname_t qn, PFty_t t)
{
    PFty_t ty = { .type  = ty_elem,
                  .name  = qn,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t;

    return ty;
}

/** attr qn { t } */
PFty_t
PFty_attr (PFqname_t qn, PFty_t t)
{
    PFty_t ty = { .type  = ty_attr,
                  .name  = qn,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t;

    return ty;
}

/** doc { t } */
PFty_t
PFty_doc (PFty_t t)
{
    PFty_t ty = { .type  = ty_doc,
                  .name  = 0,
                  .sym_space = 0,
                  .child = { 0, 0 }
    };

    ty.child[0] = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *(ty.child[0]) = t;

    return ty;
}

/**
 * XML Schema (Part 2, Datatypes) types (introduce XML Schema type names
 * and their definition in terms of Pathfinder's internal type system).
 */

/** type xs:integer = integer */
PFty_t
PFty_xs_integer (void)
{
    return PFty_integer ();
}

/** type xs:string = string */
PFty_t
PFty_xs_string (void)
{
    return PFty_string ();
}

/** type xs:boolean = boolean */
PFty_t
PFty_xs_boolean (void)
{
    return PFty_boolean ();
}

/** type xs:decimal = decimal */
PFty_t
PFty_xs_decimal (void)
{
    return PFty_decimal ();
}


/** type xs:double = double */
PFty_t
PFty_xs_double (void)
{
    return PFty_double ();
}

/** type xs:dateTime = datetime */
PFty_t
PFty_xs_datetime (void)
{
    return PFty_datetime ();
}

/** type xs:date = date */
PFty_t
PFty_xs_date (void)
{
    return PFty_date ();
}

/** type xs:time = time */
PFty_t
PFty_xs_time (void)
{
    return PFty_time ();
}

/** type xs:gYearMonth = gyearmonth */
PFty_t
PFty_xs_gyearmonth (void)
{
    return PFty_gyearmonth ();
}

/** type xs:gYear = gyear */
PFty_t
PFty_xs_gyear (void)
{
    return PFty_gyear ();
}

/** type xs:gMonthDay = monthday */
PFty_t
PFty_xs_gmonthday (void)
{
    return PFty_gmonthday ();
}

/** type xs:gMonth = gmonth */
PFty_t
PFty_xs_gmonth (void)
{
    return PFty_gmonth ();
}

/** type xs:gDay = gday */
PFty_t
PFty_xs_gday (void)
{
    return PFty_gday ();
}

/** type xs:duration = duration */
PFty_t
PFty_xs_duration (void)
{
    return PFty_duration ();
}

/** type xs:yearMonthDuration = yearmonthduration  */
PFty_t
PFty_xs_yearmonthduration (void)
{
    return PFty_yearmonthduration ();
}

/** type xs:dayTimeDuration = daytimeduration */
PFty_t
PFty_xs_daytimeduration (void)
{
    return PFty_daytimeduration ();
}

/** type xs:QName = qname */
PFty_t
PFty_xs_QName (void)
{
    return PFty_qname ();
}

/** type xs:anyType = item* */
PFty_t
PFty_xs_anyType (void)
{
    return PFty_star (PFty_item ());
}

/** type xs:anyItem = item */
PFty_t
PFty_xs_anyItem (void)
{
    return PFty_item ();
}

/** type xs:anyNode = node */
PFty_t
PFty_xs_anyNode (void)
{
    return PFty_node ();
}

/** type xs:anySimpleType = atomic* */
PFty_t
PFty_xs_anySimpleType (void)
{

    return PFty_star (PFty_choice (PFty_numeric (),
                      PFty_choice (PFty_boolean (),
                      PFty_choice (PFty_string (),
                      PFty_choice (PFty_datetime (),
                      PFty_choice (PFty_date (),
                      PFty_choice (PFty_time (),
                      PFty_choice (PFty_xs_gyearmonth (),
                      PFty_choice (PFty_xs_gyear (),
                      PFty_choice (PFty_xs_gmonthday (),
                      PFty_choice (PFty_xs_gmonth (),
                      PFty_choice (PFty_xs_gday (),
                      PFty_choice (PFty_xs_duration (),
                      PFty_choice (PFty_xs_yearmonthduration (),
                                   PFty_xs_daytimeduration ()))))))))))))));
}

/** type xs:anyElement = elem * { xs:anyType } */
PFty_t
PFty_xs_anyElement (void)
{
    PFqname_t wild = PFqname (PFns_wild, NULL);

    return PFty_elem (wild, PFty_xs_anyType ());
}


/** type xs:anyAttribute = attr * { atomic* } */
PFty_t
PFty_xs_anyAttribute (void)
{
    PFqname_t wild = PFqname (PFns_wild, NULL);

    return PFty_attr (wild, PFty_star (PFty_atomic ()));
}

/** type xdt:untypedAtomic = untypedAtomic */
PFty_t
PFty_xdt_untypedAtomic (void)
{
    return PFty_untypedAtomic ();
}

/** type xdt:untypedAny = untypedAny */
PFty_t
PFty_xdt_untypedAny (void)
{
    return PFty_untypedAny ();
}

/* ...................................................................... */

/** Worker for #PFty_defn ().
 */
static PFty_t *
defn (PFty_t t, PFarray_t *ts)
{
    PFty_t *s;

    s = (PFty_t *) PFmalloc (sizeof (PFty_t));

    *s = t;

    switch (s->type) {
    case ty_named: {
        /* resolve a named type reference only if we haven't seen it before */
        unsigned int n;
        PFty_t *rhs;

        for (n = 0; n < PFarray_last (ts); n++)
            if (PFty_eq (*s, *(PFty_t *) PFarray_at (ts, n)))
                /* avoid infinite recursion, no further unfolding */
                return s;

        if (! (rhs = PFty_schema (*s)))
            PFoops (OOPS_TYPENOTDEF,
                    "`%s' not registered as schema type",
                    PFqname_str (s->name));

        /* add the named type we're unfolding now */
        *(PFty_t *) PFarray_add (ts) = *s;

        s = defn (*rhs, ts);

        /* remove the named type again so that we may unfold it (once) in
         * other components of this type
         */
        PFarray_del (ts);

        return s;
    }

        /* unary type constructors */
    case ty_opt:
    case ty_plus:
    case ty_star:
    case ty_doc:
    case ty_elem:
    case ty_attr:
        s->child[0] = defn (*(s->child[0]), ts);
        break;

        /* binary type constructors */
    case ty_seq:
    case ty_choice:
    case ty_all:
        s->child[0] = defn (*(s->child[0]), ts);
        s->child[1] = defn (*(s->child[1]), ts);
        break;

        /* all other types have no constituent parts */
    default:
        ;
    }

    return s;
}

/**
 * Unfold all named type references in a (recursive) type.  Avoids infinite
 * recursion via a set of named types seen so far.
 *
 * @param t type to unfold
 * @return unfolded type
 */
PFty_t
PFty_defn (PFty_t t)
{
    PFarray_t *ts;

    /* the (empty) set of named types seen so far */
    ts = PFarray_default (sizeof (PFty_t));
    assert (ts);

    return *(defn (t, ts));
}

/**
 * Return QName of an element or attribute type (`element qn { type }').
 * Processing instructions may carry a target specifier. Target
 * names are treated like QNames, with a wildcard namespace.
 *
 * @param t type that must be either an `element qn { ... }',
 *          an `attribute qn { ... }', or a `processing-instruction qn' type
 */
PFqname_t
PFty_name (PFty_t t)
{
    assert (t.type == ty_elem || t.type == ty_attr || t.type == ty_pi);

    return t.name;
}

/**
 * Return the argument of a unary type constructor.
 *
 * Raises an error if called with a type that does not have exactly
 * one child. (If you want to have the left/right child of a node
 * with two children, like `seq' nodes, use PFty_lchild or PFty_rchild().
 *
 * @param t type constructor
 * @return argument of type constructor
 */
PFty_t
PFty_child (PFty_t t)
{
    assert (t.child[0] && !t.child[1]);

    return *(t.child[0]);
}

/**
 * Return the left argument of a binary type constructor.
 *
 * Return the left child of a #PFty_t type.
 * Raises an error if called with a type that does not have two children.
 *
 * @param t type constructor
 * @return left argument of type constructor
 */
PFty_t
PFty_lchild (PFty_t t)
{
    assert (t.child[0] && t.child[1]);

    return *(t.child[0]);
}

/**
 * Return the right argument of a binary type constructor.
 *
 * @param t type constructor
 * @return right argument of type constructor
 */
PFty_t
PFty_rchild (PFty_t t)
{
    assert (t.child[0] && t.child[1]);

    return *(t.child[1]);
}

/**
 * String representation of types.
 */
static char* ty_id[] = {
      [ty_none         ]   = "none"
    , [ty_empty        ]   = "()"
    , [ty_opt          ]   = "?"
    , [ty_plus         ]   = "+"
    , [ty_star         ]   = "*"
    , [ty_seq          ]   = ","
    , [ty_choice       ]   = " | "
    , [ty_all          ]   = " & "
    , [ty_item         ]   = "item"
    , [ty_untypedAny   ]   = "untypedAny"
    , [ty_atomic       ]   = "atomic"
    , [ty_untypedAtomic]   = "untypedAtomic"
    , [ty_numeric      ]   = "numeric"
    , [ty_integer      ]   = "integer"
    , [ty_decimal      ]   = "decimal"
    , [ty_double       ]   = "double"
    , [ty_string       ]   = "string"
    , [ty_boolean      ]   = "boolean"
    , [ty_qname        ]   = "QName"
    , [ty_node         ]   = "node"
    , [ty_elem         ]   = "element"
    , [ty_attr         ]   = "attribute"
    , [ty_doc          ]   = "document"
    , [ty_text         ]   = "text"
    , [ty_pi           ]   = "processing-instruction"
    , [ty_comm         ]   = "comment"
    , [ty_stmt         ]   = "stmt"
    , [ty_docmgmt      ]   = "docmgmt"
    , [ty_datetime            ]   = "dateTime"
    , [ty_date                ]   = "date"
    , [ty_time                ]   = "time"
    , [ty_gyearmonth          ]   = "gYearMonth"
    , [ty_gyear               ]   = "gYear"
    , [ty_gmonthday           ]   = "gMonthDay"
    , [ty_gmonth              ]   = "gMonth"
    , [ty_gday                ]   = "gDay"
    , [ty_duration            ]   = "duration"
    , [ty_yearmonthduration   ]   = "yearMonthDuration"
    , [ty_daytimeduration     ]   = "dayTimeDuration"
};

/**
 * Assuming precedence level of type constructors as follows:
 *
 *      cons | prec
 *      -----+-----
 *      +?*  |  3
 *       ,   |  2
 *       |   |  1
 *       &   |  0
 */
static int ty_prec[] = {
     [ty_opt   ]   = 3
    ,[ty_plus  ]   = 3
    ,[ty_star  ]   = 3
    ,[ty_seq   ]   = 2
    ,[ty_choice]   = 1
    ,[ty_all   ]   = 0
};

#include <stdio.h>

static void
ty_printf (PFarray_t *s, const char *fmt, ...)
{
    va_list ts;

    va_start (ts, fmt);

    if (PFarray_vprintf (s, fmt, ts) < 0)
        PFoops (OOPS_FATAL, "error printing type using PFarray_vprintf");

    va_end (ts);
}

static char *
ty_str (PFty_t t, int prec)
{
    PFarray_t *s = PFarray (sizeof (char), 128);

    assert (s);

    switch (t.type) {

    case ty_named:
        ty_printf (s, "%s", PFqname_str (t.name));
        break;

    case ty_opt:
    case ty_plus:
    case ty_star:
        ty_printf (s, "%s%s",
                   ty_str (*(t.child[0]), ty_prec[t.type]),
                   ty_id[t.type]);
        break;

    case ty_seq:
    case ty_choice:
    case ty_all:
        ty_printf (s, "%s%s%s%s%s",
                   (prec > ty_prec[t.type]) ? "(" : "",
                   ty_str (*(t.child[0]), ty_prec[t.type]),
                   ty_id[t.type],
                   ty_str (*(t.child[1]), ty_prec[t.type]),
                   (prec > ty_prec[t.type]) ? ")" : "");
        break;

    case ty_elem:
    case ty_attr:
        if (PFty_wildcard (t)) {
    case ty_doc:
            ty_printf (s, "%s { %s }",
                       ty_id[t.type],
                       ty_str (*(t.child[0]), 0));
            break;
        }

        ty_printf (s, "%s %s { %s }",
                   ty_id[t.type],
                   PFqname_str (t.name),
                   ty_str (*(t.child[0]), 0));
        break;

    case ty_pi:
        ty_printf (s, "%s %s {}",
                   ty_id[t.type],
                   PFty_wildcard (t) ? "*" : PFqname_str (t.name));
        break;

    default:
        ty_printf (s, "%s", ty_id[t.type]);
    }

    return (char *) PFarray_at (s, 0);
}

/**
 * Generate string representation of given type t.
 *
 * @param t type to render
 * @return string representation of t
 */
char *
PFty_str (PFty_t t)
{
    return ty_str (t, 0);
}

/**
 * The XML Schema symbol spaces.
 *
 * These need to be initialized (and the type_defns symbol space
 * loaded with the XML Schema built-in types) via a call to
 * #PFty_xs_builtins ().
 */
PFenv_t *PFtype_defns      = 0;
PFenv_t *PFelem_decls      = 0;
PFenv_t *PFattr_decls      = 0;
PFenv_t *PFgroup_defns     = 0;
PFenv_t *PFattrgroup_defns = 0;


/**
 * Import (enter) a named a type into the Pathfinder schema type
 * environment.  Re-defining an existing type is considered an error.
 *
 * @param t     the named type (with its associated symbol space)
 * @param defn  the type definition associated with this name
 */
void
PFty_import (PFty_t t, PFty_t defn)
{
    PFty_t *ty;

    /* only named types may be imported */
    assert (t.type == ty_named);
    /* make sure the type carries an appropriate symbol space */
    assert (t.sym_space);

    /* sanity: when we import an element (attribute) declaration
     * of the form
     *                     element qn { c }
     *
     * make sure that the name under which we import the declaration
     * indeed is qn
     */
    if (t.sym_space == PFelem_decls) {
        assert (defn.type == ty_elem);
        assert (PFqname_eq (t.name, defn.name) == 0);
    }
    if (t.sym_space == PFattr_decls) {
        assert (defn.type == ty_attr);
        assert (PFqname_eq (t.name, defn.name) == 0);
    }

    ty = (PFty_t *) PFmalloc (sizeof (PFty_t));
    *ty = defn;

    /* import named type into its symbol space */
    if (PFenv_bind (t.sym_space, t.name, (void *) ty))
        PFoops (OOPS_TYPEREDEF, "`%s'", PFqname_str (t.name));
}

/**
 * Refer to a XQuery schema type (either built-in XML Schema types or
 * defined through a schema import (`import schema') in the query
 * prolog) via its QName.
 *
 * @param  t the named type including its QName (e.g., `xs:string')
 * @return pointer to referenced type (or 0 if type is undefined)
 */
PFty_t *
PFty_schema (PFty_t t)
{
    PFarray_t *ts;

    /* we can do lookups for named types only */
    assert (t.type == ty_named);
    /* make sure a symbol space has been assigned for this named type */
    assert (t.sym_space);

    /* lookup in associated symbol space */
    if ((ts = PFenv_lookup (t.sym_space, t.name)))
        return *((PFty_t **) PFarray_at (ts, 0));

    return 0;
}


/**
 * Pre-defined XML Schema/XQuery types
 * (see W3C XQuery, 2.4 Types).
 */
static struct { PFns_t *ns; char *loc; PFty_t (*fn) (void); } predefined[] =
{
    { .ns = &PFns_xs,  .loc = "integer",       .fn = PFty_xs_integer        },
    { .ns = &PFns_xs,  .loc = "string",        .fn = PFty_xs_string         },
    { .ns = &PFns_xs,  .loc = "boolean",       .fn = PFty_xs_boolean        },
    { .ns = &PFns_xs,  .loc = "decimal",       .fn = PFty_xs_decimal        },
    { .ns = &PFns_xs,  .loc = "double",        .fn = PFty_xs_double         },
    { .ns = &PFns_xs,  .loc = "dateTime",      .fn = PFty_xs_datetime       },
    { .ns = &PFns_xs,  .loc = "date",          .fn = PFty_xs_date           },
    { .ns = &PFns_xs,  .loc = "time",          .fn = PFty_xs_time           },
    { .ns = &PFns_xs,  .loc = "gYearMonth",    .fn = PFty_xs_gyearmonth     },
    { .ns = &PFns_xs,  .loc = "gYear",         .fn = PFty_xs_gyear          },
    { .ns = &PFns_xs,  .loc = "gMonthDay",     .fn = PFty_xs_gmonthday      },
    { .ns = &PFns_xs,  .loc = "gMonth",        .fn = PFty_xs_gmonth         },
    { .ns = &PFns_xs,  .loc = "gDay",          .fn = PFty_xs_gday           },
    { .ns = &PFns_xs,  .loc = "duration",      .fn = PFty_xs_duration       },
    { .ns = &PFns_xs,  .loc = "yearMonthDuration",
                                               .fn = PFty_xs_yearmonthduration},
    { .ns = &PFns_xs,  .loc = "dayTimeDuration",
                                               .fn = PFty_xs_daytimeduration},
    { .ns = &PFns_xs,  .loc = "QName",         .fn = PFty_xs_QName          },
    { .ns = &PFns_xs,  .loc = "anyType",       .fn = PFty_xs_anyType        },
    { .ns = &PFns_xs,  .loc = "anyItem",       .fn = PFty_xs_anyItem        },
    { .ns = &PFns_xs,  .loc = "anyNode",       .fn = PFty_xs_anyNode        },
    { .ns = &PFns_xs,  .loc = "anySimpleType", .fn = PFty_xs_anySimpleType  },
    { .ns = &PFns_xs,  .loc = "anyElement",    .fn = PFty_xs_anyElement     },
    { .ns = &PFns_xs,  .loc = "anyAttribute",  .fn = PFty_xs_anyAttribute   },
    { .ns = &PFns_xdt, .loc = "untypedAtomic", .fn = PFty_xdt_untypedAtomic },
    { .ns = &PFns_xdt, .loc = "untypedAny",    .fn = PFty_xdt_untypedAny    },
    /* end of built-in XML Schema type list */
    { .ns = 0,         .loc = 0,               .fn = 0                      }
};

/*
 * look up a predefined atomic PFty_t by (uri,loc). Return boolean whether found.
 */
bool
PFty_atomic_lookup (const char* uri, const char* loc, PFty_t *dst)
{
    int n;
    for (n = 0; predefined[n].loc; n++) {
        if (strcmp(loc, predefined[n].loc) == 0 &&
            strcmp(uri, predefined[n].ns->uri) == 0) {
                *dst = (*predefined[n].fn)();
                return true;
        }
    }
    return false;
}

/**
 * Register the XML Schema/XQuery predefined types `xs:...' and `xdt:...'
 * in the Pathfinder schema type environment.
 */
void
PFty_predefined (void)
{
    PFty_t t;
    PFqname_t qn;
    unsigned int n;

    /* initialize the XML Schema symbol spaces */
    PFtype_defns      = PFenv_ (200);
    PFelem_decls      = PFenv ();
    PFattr_decls      = PFenv ();
    PFgroup_defns     = PFenv ();
    PFattrgroup_defns = PFenv ();

    assert (PFfun_env);

    for (n = 0; predefined[n].loc; n++) {
        qn = PFqname (*(predefined[n].ns), predefined[n].loc);

        /* construct the type definition with name ns:loc */
        t = PFty_named (qn);

        /* enter the type and its definition into its XML Schema
         * symbol space
         */
        PFty_import (t, predefined[n].fn ());

        /* add cast functions for the atomic types */
        t = (*predefined[n].fn)();
        if (PFty_subtype (t, PFty_atomic())) {
            PFfun_sig_t sig =   
                { .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
                  .ret_ty = t };

            PFenv_bind (PFfun_env,
                        qn,
                        (void *) PFfun_new (qn,
                                            1,
                                            true,
                                            1,
                                            &sig,
                                            NULL,
                                            NULL,
                                            NULL));
        }
    }
}

/* vim:set shiftwidth=4 expandtab: */
