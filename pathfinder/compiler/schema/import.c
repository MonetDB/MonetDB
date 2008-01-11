/* -*- mode:C; c-basic-offset:4; c-indentation-syle:"k&r"; indent-tabs-mode:nil -*-*/

/**
 * @file
 *
 * Import XML Schema types into the Pathfinder type environment.
 *
 * See Section `Importing Schemas' in the W3C XQuery 1.0 and XPath 2.0
 * Formal Semantics and Jan Rittinger's BSc thesis.
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "import.h"

/* PFoops */
#include "oops.h"

/* PFarray_t */
#include "array.h"

/* PFty_t */
#include "types.h"

/* PFty_simplify () */
#include "subtyping.h"

/* PFqname_t */
#include "qname.h"

/* PFns_t */
#include "ns.h"

#include "mem.h"

#define L(n) ((n)->child[0])
#define R(n) ((n)->child[1])
#define LR(n) R(L(n))
#define RL(n) L(R(n))
#define LL(n) L(L(n))
#define RR(n) R(R(n))

/**
 * XML Schema import requires libxml2
 */
#if HAVE_LIBXML2

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"

/**
 * Current state of type mapping DFA.
 */
static int state;

/**
 * Stacks (DFA state, XML attribute, types, namespaces)
 */
static PFarray_t *state_stack;
static PFarray_t *attr_stack;
static PFarray_t *type_stack;
static PFarray_t *ns_stack;

/**
 * Any QName without explicit namespace prefix is imported into
 * the target namespace @a target_ns.
 */
static PFns_t target_ns;

/**
 * Test: does namespace prefix @a ns designate
 * the XML Schema namespace URI?
 */
#define XML_SCHEMA_NS(ns) (strcmp (PFns_xs.uri, (ns)) == 0)

/**
 * XML namespace declaration attribute
 */
#define XMLNS "xmlns"

/**
 * XML Schema element tag names the type mapping DFA can interpret
 * (keep this list alphabetically sorted by tag name).
 */
static const char *xml_schema_tags[] = {
    "all",
    "any",
    "anyAttribute",
    "attribute",
    "attributeGroup",
    "choice",
    "complexContent",
    "complexType",
    "element",
    "extension",
    "group",
    "list",
    "restriction",
    "schema",
    "sequence",
    "simpleContent",
    "simpleType",
    "union"
};

/**
 * Number of XML Schema tags we can interpret.
 */
#define XML_SCHEMA_TAGS (sizeof (xml_schema_tags) / sizeof (char *))

/**
 * DFA actions
 * (actions receive an array of attributes of the current XML Schema element).
 */
typedef void (*action) (PFarray_t *);

/**
 * Array of actions.
 */
static action actions[173];

/* the line or column in the state
   table where e.g. LIST can be found */
#define STACKCOL         36
#define RESTRICTION      45
#define RESTRICTION_SIM  50
#define LIST             83
#define UNION            89

/**
 * Holes (error states) in the DFA transition table.
 */
#define _  (-1)
#define HOLE _

/**
 * The DFA transition table itself.
 */
static int dfa[173][37];


/**
 * Manipulate type, state, and attribute stacks.
 */

/**
 * Push new state on the state stack.
 *
 * @param state state to be pushed
 */
static void
push_state (int state)
{
    *(int*) PFarray_add (state_stack) = state;
}

/**
 * Pop state stack and return popped state.
 *
 * @return popped state
 */
static int
pop_state (void)
{
    int s;

    assert (! PFarray_empty (state_stack));

    s = *(int *) PFarray_top (state_stack);
    PFarray_del (state_stack);

    return s;
}

/**
 * Peek at the state stack top.
 *
 * @return state at stack top
 */
static int
top_state (void)
{
    assert (! PFarray_empty (state_stack));

    return *(int *) PFarray_top (state_stack);
}


/**
 * Push attribute list on the attribute stack
 *
 * @param atts attribute list to be pushed
 */
static void
push_attributes (PFarray_t *atts)
{
    *(PFarray_t **) PFarray_add (attr_stack) = atts;
}

/**
 * Pop attribute stack and return popped attribute list.
 *
 * @return popped attribute list
 */
static PFarray_t *
pop_attributes (void)
{
    PFarray_t *atts;

    assert (! PFarray_empty (attr_stack));

    atts = *(PFarray_t **) PFarray_top (attr_stack);
    PFarray_del (attr_stack);

    return atts;
}

/**
 * Return value of attribute named @a a in attribute list @a atts.
 *
 * @param atts attribute list
 * @param a attribute name
 * @return attribute value (or 0 if no attribute with name @a a found)
 */
static char *
attribute_value (PFarray_t *atts, char *a)
{
    unsigned n = 0;

    if (! atts)
        return 0;

    while (n < PFarray_last (atts)) {
        if (strcmp (a, *(char **) PFarray_at (atts, n)) == 0)
            return *(char **) PFarray_at (atts, n + 1);

        n += 2;
    }

    return 0;
}

/**
 * Push new type on type stack.
 *
 * @param t type to be pushed
 */
static void
push_type (PFty_t t)
{
    *(PFty_t *) PFarray_add (type_stack) = t;
}

/**
 * Pop type stack and return popped type
 *
 * @return popped type
 */
static PFty_t
pop_type (void)
{
    PFty_t t;

    assert (! PFarray_empty (type_stack));

    t = *(PFty_t *) PFarray_top (type_stack);
    PFarray_del (type_stack);

    return t;
}

/**
 * Push new namespace on namespace stack.
 *
 * @param ns namespace to be pushed
 */
static void
push_ns (PFns_t *ns)
{
    /*
     * A NULL pointer serves as a marker on the stack.  Otherwise
     * it must be a sensible namespace.
     */
    if (ns) {
        /*
         * A NULL pointer would indicate a wildcard namespace (see qname.c),
         * which cannot make sense here.  An empty string indicates the
         * default element namespace (not the target namespace!).
         */
        assert (ns->prefix);

        /*
         * Our convention is to set .uri = NULL only if we don't know the
         * URI of a namespace (yet).  Such a situation cannot arise here.
         * "No namespace" would lead to an empty .uri string.
         */
        assert (ns->uri);
    }

    *(PFns_t **) PFarray_add (ns_stack) = ns;
}

/**
 * Pop namespaces from the namespace stack until we hit the 0 mark.
 */
static void
pop_ns (void)
{
    PFns_t *ns;

    assert (! PFarray_empty (ns_stack));

    /* pop the namespace stack until we hit the scope (NULL) mark */
    do {
        ns = *(PFns_t **) PFarray_top (ns_stack);
        PFarray_del (ns_stack);
    } while (ns);
}

/**
 * In the stack of in-scope namespace, search for the namespace with
 * prefix @a prefix.
 *
 * @param prefix namespace prefix (0 if this is a lookup for the
 *               default namespace)
 * @return (pointer to) associated in-scope namespace (or NULL if
 *         no matching namespace is in scope)
 */
static PFns_t *
lookup_ns (char *prefix)
{
    int n;
    PFns_t *ns;

    assert (prefix);

    for (n = PFarray_last (ns_stack); n; n--) {
        ns = *(PFns_t **) PFarray_at (ns_stack, n - 1);

        /* ns == NULL would be the stack marker, which we simply skip */
        if (ns) {
            assert (ns->prefix);

            /* lookup for regular namespace */
            if (strcmp (ns->prefix, prefix) == 0)
                return ns;
        }
    }

    return 0;
}

/**
 * Declare a new namespace.
 *
 * @param prefix  the namespace prefix for this NS (empty string
 *                if default namespace)
 * @param uri     URI for the namespace
 * @return a new Pathfinder namespace (PFns_t)
 */
static PFns_t *
new_ns (char *prefix, char *uri)
{
    PFns_t *new_ns = PFmalloc (sizeof (PFns_t));

    assert (prefix);
    assert (uri);

    *new_ns = (PFns_t) { .prefix = prefix, .uri = uri };

    return new_ns;
}

/**
 * Create a QName that we will import into the Pathfinder type
 * enviroment(s).
 *
 * All QNames are imported into the target namespace for this schema
 * import: extract the local part of the given name, then attach
 * the target namespace.
 *
 * @param nsloc (possibly qualified) name to import
 * @return QName to import
 */
static PFqname_t
imported_qname (char *nsloc)
{
    PFqname_raw_t qn_raw;

    assert (nsloc);

    /*
     * It is still valid to call PFstr_qname() here, because we will
     * overwrite the namespace information in a moment.
     */
    qn_raw = PFqname_raw (nsloc);

    if (qn_raw.prefix && *(qn_raw.prefix))
        PFinfo (OOPS_SCHEMAIMPORT,
                "namespace of `%s' replaced by target namespace `%s'",
                PFqname_raw_str (qn_raw), target_ns.uri);

    return PFqname (target_ns, qn_raw.loc);
}

/**
 * Attach the proper namespace to a referenced QName @a nsloc.  If
 * @a nsloc has a namespace prefix, check that this namespace prefix
 * has been properly declared.  If @a nsloc has no namespace prefix,
 * attach the target namespace for this schema import.
 *
 * @param nsloc (possibly qualified) referenced name
 * @return QName with namespace attached
 */
static PFqname_t
ref_qname (char *nsloc)
{
    PFqname_raw_t  qn_raw;
    PFns_t        *ns_ptr;
    PFns_t         ns;

    assert (nsloc);

    qn_raw = PFqname_raw (nsloc);

    assert (qn_raw.prefix);

    /*
     * Don't use lookup_ns() for unqualified names, as this would
     * yield the default element namespace, *not* the target
     * namespace.
     */
    if (! *(qn_raw.prefix))
        ns = target_ns;
    else if ((ns_ptr = lookup_ns (qn_raw.prefix)))
        ns = *ns_ptr;
    else
        PFoops (OOPS_BADNS,
                "(XML Schema import) prefix `%s' unknown",
                qn_raw.prefix);

    return PFqname (ns, qn_raw.loc);
}

/**
 * Compare two tag names, strcmp () semantics.
 *
 * @param s1 pointer to first string
 * @param s2 pointer into XML Schema tags table (@a xml_schema_tags)
 */
static int
loc_cmp (const void *s1, const void *s2)
{
    return strcmp ((char *) s1, *(char **) s2);
}

/**
 * Map an XML Schema element opening tag name to an index that we can use
 * to drive the DFA.
 *
 * @param SAX parser context
 * @param tag XML Schema element tag name
 * @return index for opening tag (or @a HOLE if this is an element we
 *   cannot interpret)
 */
static int
map_open_tag (void *ctx, char *nsloc)
{
    PFqname_raw_t   qn_raw;
    PFns_t         *ns;
    const char    **t;

    assert (nsloc);

    qn_raw = PFqname_raw (nsloc);

    /*
     * check namespace of opening tag
     *
     * The namespace table lists the default element namespace
     * under the empty string prefix.  For unqualified QNames, this
     * will correctly lead to the default element namespace in
     * lookup_ns().
     */
    if ((ns = lookup_ns (qn_raw.prefix))) {
        /* is this the XML Schema namespace? */
        if (strcmp (ns->uri, PFns_xs.uri)) {
            PFinfo (OOPS_SCHEMAIMPORT, "non-XML Schema element seen");
            xmlParserError (ctx, "\n");
            PFoops (OOPS_SCHEMAIMPORT, "check schema validity");
        }
    }
    else {
        PFinfo (OOPS_BADNS,
                "(XML Schema import) prefix `%s' unknown", qn_raw.prefix);
        xmlParserError (ctx, "\n");
        PFoops (OOPS_SCHEMAIMPORT, "check schema validity");
    }

    t = (char const **) bsearch (qn_raw.loc,
                                 xml_schema_tags,
                                 XML_SCHEMA_TAGS,
                                 sizeof (char *),
                                 loc_cmp);

    if (t)
        return t - xml_schema_tags;

    return HOLE;
}

/**
 * Map an XML Schema element closing tag to an index that we can use
 * to drive the DFA.
 *
 * @param ctx SAX parser context
 * @param tag XML Schema element tag name
 * @return index for closing tag (or @a HOLE if this is an element we
 *   cannot interpret)
 */
static int
map_closing_tag (void *ctx, char *nsloc)
{
    int open_tag;

    assert (nsloc);

    open_tag = map_open_tag (ctx, nsloc);

    if (open_tag == HOLE)
        return HOLE;

    return open_tag + XML_SCHEMA_TAGS;
}


/**
 * Process XML attributes and declare any namespace declaration
 * attributes (xmlns="..." or xmlns:loc="...") encountered.
 *
 * @param SAX parser context
 * @param atts attributes of current opening tag
 * @return array (alternating attribute name/value entries)
 */
static PFarray_t *
attributes (void *ctx, const xmlChar **atts)
{
    PFqname_raw_t  qn_raw;
    PFarray_t     *attrs;

    attrs = PFarray (sizeof (char *));

    /* push namespace scope marker */
    push_ns (NULL);

    if (atts)
        while (*atts) {

            qn_raw = PFqname_raw ((char *) *atts);

            assert (qn_raw.prefix);

            if (*(qn_raw.prefix)) {
                if (strcmp (qn_raw.prefix, XMLNS) == 0) {
                    /* `xmlns:loc="uri"' NS declaration attribute */
                    atts++;

                    /* declare loc namespace |-> uri */
                    push_ns (new_ns (qn_raw.loc, PFstrdup ((char *) *atts)));
                    atts++;

                    continue;
                }

                /* bogus namespace prefix for regular attribute */
                PFinfo (OOPS_SCHEMAIMPORT,
                        "undeclared attribute `%s'", PFqname_raw_str (qn_raw));
                xmlParserError (ctx, "\n");
                PFoops (OOPS_SCHEMAIMPORT, "check schema validity");
            }

            if (strcmp (qn_raw.loc, XMLNS) == 0) {
                /* `xmlns="uri"' default NS declaration attribute */
                atts++;

                /* declare default element namespace |-> uri */
                push_ns (new_ns ("", PFstrdup ((char *) *atts)));
                atts++;

                continue;
            }


            /* attribute name */
            *(char **) PFarray_add (attrs) = PFstrdup ((char *) *atts);
            atts++;

            /* attribute value */
            *(char **) PFarray_add (attrs) = PFstrdup ((char *) *atts);
            atts++;
        }

    return attrs;
}

/**
 * gets a Pathfinder type (PFty_t), a list of attributes and applies
 * the occurrence attributes (minOccurs, maxOccurs) to the incoming
 * partial_type
 * @param atts a linked list of attributes
 * @param partial_type a Pathfinder type (PFty_t)
 * @return a Pathfinder type (PFty_t) with the occurrences applied to
 *         the incoming partial_type
 */
static PFty_t
occurs (PFarray_t *atts, PFty_t t)
{
    char *minOccurs;
    char *maxOccurs;
    int min_occurs;
    int max_occurs;
    char *p;

    /* in absence of @minOccurs and @maxOccurs, the default
     * is exactly one occurrence
     */
    min_occurs = 1;
    max_occurs = 1;

    /* @minOccurs */
    if ((minOccurs = attribute_value (atts, "minOccurs"))) {
        min_occurs = (int) strtol (minOccurs, &p, 10);
        if (min_occurs < 0 || *p)
            PFoops (OOPS_SCHEMAIMPORT,
                    "invalid `minOccurs' value: %s", minOccurs);
    }

    /* @maxOccurs */
    if ((maxOccurs = attribute_value (atts, "maxOccurs"))) {
        max_occurs = (int) strtol (maxOccurs, &p, 10);
        if (*p) {
            if (strcmp (maxOccurs, "unbounded") == 0)
                max_occurs = INT_MAX;
            else
                PFoops (OOPS_SCHEMAIMPORT,
                        "invalid `maxOccurs' value: %s", maxOccurs);
        }
        else
            if (max_occurs < 0 || max_occurs < min_occurs)
                PFoops (OOPS_SCHEMAIMPORT,
                        "invalid `maxOccurs' value: %s (`minOccurs' value is %s)",
                        maxOccurs, minOccurs);
    }

    /*
     *                     [[  x  ]]              -->       [[ x ]] ?
     *                        / \
     *           @minOccurs="0"  @maxOccurs="1"
     */
    if (min_occurs == 0 && max_occurs == 1)
        return PFty_opt (t);

    /*
     *                     [[  x  ]]              -->       [[ x ]] *
     *                        / \
     *           @minOccurs="0"   @maxOccurs="n"
     */
    if (min_occurs == 0 && max_occurs > 1)
        return PFty_star (t);

    /*
     *                     [[  x  ]]              -->       [[ x ]] +
     *                        / \
     *           @minOccurs="n"   @maxOccurs="m"
     */
    if (min_occurs >= 1 && max_occurs > 1)
        return PFty_plus (t);

    return t;
}

/**
 * gets a Pathfinder type (PFty_t), a list of attributes and applies
 * the occurrence attributes (use) to the incoming partial_type
 * @param atts a linked list of attributes
 * @param partial_type a Pathfinder type (PFty_t)
 * @return a Pathfinder type (PFty_t) with the occurrences applied to
 *         the incoming partial_type
 */
static PFty_t
attr_occurs (PFarray_t *atts, PFty_t partial_type)
{
    char* occurrence;

    if (! (occurrence = attribute_value (atts, "use")))
        /* optional is the default case */
        return PFty_opt (partial_type);
    else {
        if (! strcmp (occurrence, "required"))
            return partial_type;
        else if (! strcmp (occurrence, "prohibited"))
            return PFty_empty ();
        else if (! strcmp (occurrence, "optional"))
            return PFty_opt (partial_type);
        else
            PFoops (OOPS_SCHEMAIMPORT,
                    "use should be `required', `prohibited' or `optional'");
    }

    /* just to pacify picky compilers; never reached due to "exit" in PFoops */
    return PFty_empty ();
}

/**
 * Callback invoked whenever the see an open tag <t ...>.
 *
 * @param ctx SAX parser context
 * @param t tag name
 * @param atts attributes of this element
 */
static void
schema_import_start_element (void *ctx,
                             const xmlChar *t, const xmlChar **atts)
{
    PFarray_t *attrs;
    int open_tag;

    assert (t);

    /*
     * parse the attributes present in <t ...>,
     * this also introduces all namespaces declared via `xmlns=...'
     */
    attrs = attributes (ctx, atts);

    /* push the attributes such that we can access them when we see </t> */
    push_attributes (attrs);

    open_tag = map_open_tag (ctx, (char *) t);

    if (open_tag == HOLE || dfa[state][open_tag] == HOLE) {
        PFinfo (OOPS_SCHEMAIMPORT, "unexpected <%s>", t);
        xmlParserError (ctx, "\n");
        PFoops (OOPS_SCHEMAIMPORT, "check schema validity");
    }

    /* perform type mapping DFA transition */
    state = dfa[state][open_tag];

#ifdef DEBUG_SCHEMAIMPORT
    PFlog ("schema import: <%s>", t);
#endif /* DEBUG_SCHEMAIMPORT */


    /* perform action attached to the DFA edge */
    actions[state] (attrs);

    /* push the context state */
    push_state (dfa[state][STACKCOL]);
}

/**
 * Callback invoked whenever the see a closing tag </t>.
 *
 * @param SAX parser context
 * @param t tag name
 */
static void
schema_import_end_element (void *ctx, const xmlChar *t)
{
    PFarray_t *attributes;
    int context;
    int closing_tag;

    assert (t);

    closing_tag = map_closing_tag (ctx, (char *) t);

    /* perform type mapping DFA transition */
    if (dfa[state][closing_tag] == HOLE) {
        PFinfo (OOPS_SCHEMAIMPORT, "unexpected </%s>", t);
        xmlParserError (ctx, "\n");
        PFoops (OOPS_SCHEMAIMPORT, "check schema well-formedness");
    }

    /* perform type mapping DFA transition */
    state = dfa[state][closing_tag];

#ifdef DEBUG_SCHEMAIMPORT
    PFlog ("schema import: </%s>", t);
#endif /* DEBUG_SCHEMAIMPORT */

    /* access the attributes of this element */
    attributes = pop_attributes ();

    /* perform action attached to this DFA edge */
    actions[state] (attributes);

    /* the namespace(s) created in this element go out of scope */
    pop_ns ();

    /* access the context */
    (void) pop_state ();
    context = top_state ();

    /* if required, change to state indicated by context and perform
     * associated actions
     */
    if (dfa[state][STACKCOL]) {
        state = context;
        actions[state] (0);
    }

}

/**
 * DFA actions below.
 */

/**
 * No operation.
 */
static void nop (PFarray_t *unused)
{
    /* do nothing */
    (void) unused;
}

/**
 * XML Schema top-level <schema> ... </schema>.
 *
 * @param atts attributes of the <schema> element
 */
static void
start_schema (PFarray_t *atts)
{
    char *targetNamespace;

    if ((targetNamespace = attribute_value (atts, "targetNamespace")))
        target_ns = *new_ns (PFstrdup (""), PFstrdup (targetNamespace));
}

/**
 * Handle <restriction> and <restriction base=...>.
 *
 * @param atts attributes of the <restriction> element
 */
static void
start_restriction (PFarray_t *atts)
{
    if (attribute_value (atts, "base"))
        /* in the RESTRICTION state, any content is ignored */
        state = RESTRICTION;
}

/**
 * Handle <restriction> and <restriction base=...>.
 *
 * @param atts attributes of the <restriction> element
 */
static void
start_simple_restriction (PFarray_t *atts)
{
    if (attribute_value (atts, "base"))
        /* in the RESTRICTION_SIM state, any content is ignored */
        state = RESTRICTION_SIM;
}

/**
 * Handle <list> and <list itemType=...>.
 *
 * @param atts attributes of the <list> element
 */
static void
start_list (PFarray_t *atts)
{
    if (attribute_value (atts, "itemType"))
        /* in the LIST state, any content is ignored */
        state = LIST;
}

/**
 * Handle <union> and <union memberTypes=...>.
 *
 * @param atts attributes of the <union> element
 */
static void
start_union (PFarray_t *atts)
{
    if (attribute_value (atts, "memberTypes"))
        /* in the UNION state, any content is ignored */
        state = UNION;
}

/**
 * Apply the `*' constructor to the topmost type on the stack
 * (map content of <list>)
 */
static void
end_list (PFarray_t *unused)
{
    (void) unused;

    /*   t   -->   t*   */
    push_type (PFty_star (pop_type ()));
}

/**
 * Apply the `&' type constructor to the two topmost types on the type stack.
 */
static void
combine_all (PFarray_t *unused)
{
    /*   t1   -->   t2 & t1
     *   t2
     */ 

    PFty_t t1;
    PFty_t t2;

    (void) unused;

    t1 = pop_type ();
    t2 = pop_type ();

    push_type (*PFty_simplify (PFty_all (t2, t1)));
}

/**
 * Apply the `|' type constructor to the two topmost types on the type stack.
 */
static void
combine_choice (PFarray_t *unused)
{
    /*   t1   -->   t2 | t1
     *   t2
     */

    PFty_t t1;
    PFty_t t2;

    (void) unused;

    t1 = pop_type ();
    t2 = pop_type ();

    push_type (*PFty_simplify (PFty_choice (t2, t1)));
}

/**
 * Apply the `,' type constructor to the two topmost types on the type stack.
 */
static void
combine_sequence (PFarray_t *unused)
{
    /*   t1   -->   t2 , t1
     *   t2
     */

    PFty_t t1;
    PFty_t t2;

    (void) unused;

    t1 = pop_type ();
    t2 = pop_type ();

    push_type (*PFty_simplify (PFty_seq (t2, t1)));
}

/**
 * Prepend attributes to an element's content type (using `,').
 */
static void
combine_atts_partial_type (PFarray_t *unused)
{
    /*   atts   -->   atts , t
     *   t
     */
    PFty_t t;
    PFty_t atts;

    (void) unused;

    atts = pop_type ();
    t    = pop_type ();

    push_type (*PFty_simplify (PFty_seq (atts, t)));
}

/**
 * A `goto' on the stack.
 */
static void
goto_stack (PFarray_t *unused)
{
    /*   s   -->   goto(s)
     */
    (void) unused;
    (void) pop_state ();
    push_state (dfa[state][STACKCOL]);
}

/**
 * Push a `none' type on the stack (used as identity w.r.t. `&')
 */
static void
end_all_empty (PFarray_t *unused)
{
    (void) unused;

    /*   t   -->   none
     *             t
     */
    push_type (PFty_none ());
}

/**
 * Push a `none' type on the stack (used as identity w.r.t. `|')
 */
static void
end_choice_empty (PFarray_t *unused)
{
    (void) unused;

    /*   t   -->   none
     *             t
     */
    push_type (PFty_none ());
}

/**
 * Push an `empty' type on the stack (used as identity w.r.t. `,')
 */
static void
end_seq_empty (PFarray_t *unused)
{
    (void) unused;

    /*   t   -->   empty
     *             t
     */
    push_type (PFty_empty ());
}

/**
 * Push an `empty' type on the stack (empty content of a local complex type)
 */
static void
end_local_complex_type_eps (PFarray_t *unused)
{
    (void) unused;

    /*   t   -->   empty
     *             t
     */
    push_type (PFty_empty ());
}

/**
 * Apply `maxOccurs'/`minOccurs' attributes to topmost type on stack.
 */
static void
end_choice_seq (PFarray_t *atts)
{
    /*   t   -->   occurs (m, M, t)
     *
     *   within context of <... minOccurs="m" maxOccurs="M"...>
     */
    push_type (occurs (atts, pop_type ()));
}

/**
 * XML Schema to XQuery type system mappings
 */

/**
 * Map (empty) top-level <complexType name=.../>
 *
 * @param atts attributes of the <complexType> element
 */
static void
end_top_level_eps (PFarray_t *atts)
{
    /*  [[ <complexType name="n"/> ]]   -->   n |t--> empty
     */
    char *name;
    PFty_t imp;
    PFty_t t;

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named (imported_qname (name));
        t   = PFty_empty ();
        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into types symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level type definition");
}

/**
 * Map top-level <complexType name=...> t </complexType>
 *
 * @param atts attributes of the <complexType> element
 */
static void
end_top_level_type (PFarray_t *atts)
{
    /*  [[ <complexType name="n"> t </complexType> ]]   -->   n |t--> [[ t ]]
     */
    char *name;
    PFty_t imp;
    PFty_t t;

    t = pop_type ();

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named (imported_qname (name));
        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into types symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level type definition");
}

/**
 * Map (empty) top-level group <group name=.../>
 *
 * @param atts attributes of the <group> element
 */
static void
end_top_level_eps_group (PFarray_t *atts)
{
    /*  [[ <group name="n"/> ]]   -->   n |g--> empty
     */
    char *name;
    PFty_t imp;
    PFty_t t;

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named_group (imported_qname (name));
        t   = PFty_empty ();
        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into group symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;

    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level group definition");
}

/**
 * Map top-level group <group name=...> t </group>
 *
 * @param atts attributes of the <group> element
 */
static void
end_top_level_group (PFarray_t *atts)
{
    /*  [[ <group name="n"> t </group> ]]   -->   n |g--> [[ t ]]
     */
    char *name;
    PFty_t imp;
    PFty_t t;

    t = pop_type ();

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named_group (imported_qname (name));
        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into group symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level group definition");
}

/**
 * Map (empty) top-level attribute group <attributeGroup name=.../>
 *
 * @param atts attributes of the <attributeGroup> element
 */
static void
end_top_level_eps_attrgrp (PFarray_t *atts)
{
    /*  [[ <attributeGroup name="n"/> ]]   -->   "n" |ag--> empty
     */
    char *name;
    PFty_t imp;
    PFty_t t;

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named_attrgroup (imported_qname (name));
        t   = PFty_empty ();
        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into attrgroup symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;

    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level attributeGroup definition");
}

/**
 * Map top-level attribute group <attributeGroup name=...> t </attributeGroup>
 *
 * @param atts attributes of the <attributeGroup> element
 */
static void
end_top_level_attrgrp (PFarray_t *atts)
{
    /*  [[ <attributeGroup name="n"> t </attributeGroup> ]] -->
     *  n |ag--> [[ t ]]
     */
    char *name;
    PFty_t imp;
    PFty_t t;

    t = pop_type ();

    if ((name = attribute_value (atts,"name"))) {
        imp = PFty_named_attrgroup (imported_qname (name));
        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into attrgroup symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level attributeGroup definition");
}

/**
 * Map top-level type definition w/ attributes:
 * <complexType name="n"> t a0 ... ak </complexType>
 *
 * @param atts attributes of the <complexType> element
 */
static void
end_top_level_complex_type_wboth (PFarray_t *atts)
{
    /*  [[ <complexType name="n"> t a0 ... ak </complexType> ]]   -->
     *  n |t--> ([[ a0 ]] & ... & [[ ak ]]) , [[ t ]]
     */
    combine_atts_partial_type (0);     /* argument is ignored */
    end_top_level_type (atts);
}

/**
 * Map an (empty) top-level element declaration:
 * <element name="n" type="t"/> and <element name="n"/>
 *
 * @param atts attributes of the <element> element
 */
static void
end_top_level_element (PFarray_t *atts)
{
    /*  [[ <element name="n" type="t"/> ]] -->
     *  n |e--> element n { named t }
     *
     *  [[ <element name="n"/> ]]  -->
     *  n |e--> element n { anyType }
     */
    char *name;
    char *type;
    PFty_t imp;
    PFty_t t;

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named_elem (imported_qname (name));

        if ((type = attribute_value (atts, "type")))
            /* @type present */
            t = PFty_elem (imported_qname (name),
                           PFty_named (ref_qname (type)));
        else
            /* @type absent => xs:anyType */
            t = PFty_elem (imported_qname (name),
                           PFty_xs_anyType ());

        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into element symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level element declaration");
}

/**
 * Map a top-level element declaration:
 * <element name="n"> t </element>
 *
 * @param atts attributes of the <element> element
 */
static void
end_top_level_element_wsub (PFarray_t *atts)
{
    /*  [[ <element name="n"> t </element> ]] -->
     *  n |e--> element n { [[ t ]] }
     */
    char *name;
    PFty_t imp;
    PFty_t t;

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named_elem (imported_qname (name));
        t   = PFty_elem (imported_qname (name), pop_type ());
        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into element symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level element declaration");
}

/**
 * Map an (empty) top-level attribute declaration:
 * <attribute name="n" type="t"/> and <attribute name="n"/>
 *
 * @param atts attributes of the <attribute> element
 */
static void
end_top_level_attribute (PFarray_t *atts)
{
    /*  [[ <attribute name="n" type="t"/> ]] -->
     *  n |a--> attribute n { named t }
     *
     *  [[ <attribute name="n"/> ]]  -->
     *  n |a--> attribute n { atomic* }
     */
    char *name;
    char *type;
    PFty_t imp;
    PFty_t t;

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named_attr (imported_qname (name));

        if ((type = attribute_value (atts, "type")))
            /* @type present */
            t = PFty_attr (imported_qname (name),
                           PFty_named (ref_qname (type)));
        else
            /* @type absent => atomic* */
            t = PFty_attr (imported_qname (name),
                           PFty_star (PFty_atomic ()));

        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into attribute symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level attribute declaration");
}

/**
 * Map a top-level attribute declaration:
 * <attribute name="n"> t </attribute>
 *
 * @param atts attributes of the <attribute> element
 */
static void
end_top_level_attribute_wsub (PFarray_t *atts)
{
    /*  [[ <attribute name="n"> t </attribute> ]]  -->
     *  n |a--> attribute n { [[ t ]] }
     */
    char *name;
    PFty_t imp;
    PFty_t t;

    if ((name = attribute_value (atts, "name"))) {
        imp = PFty_named_attr (imported_qname (name));
        t   = PFty_attr (imported_qname (name), pop_type ());
        PFty_import (imp, t);

#ifdef DEBUG_SCHEMAIMPORT
        PFlog ("schema import: import into attribute symbol space: %s = %s",
               PFty_str (imp), PFty_str (t));
#endif /* DEBUG_SCHEMAIMPORT */

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in top-level attribute declaration");
}


/**
 * Map an (empty) local element declaration (and apply occurrence attributes):
 * <element name="n" type="t"/> or <element name="n"/> or <element ref="n"/>
 *
 * @param atts attributes of the <element> element
 */
static void
end_local_element (PFarray_t *atts)
{
    /*  [[ <element name="n" type="t" minOccurs="m" maxOccurs="M"/> ]]  -->
     *  occurs (m, M, element n { named t })
     *
     *  [[ <element name="n" minOccurs="m" maxOccurs="M"/> ]]  -->
     *  occurs (element n { anyType })
     *
     *  [[ <element ref="n" minOccurs="m" maxOccurs="M"/> ]]  -->
     *  occurs (m, M, named n)
     */
    char *name;
    char *type;
    char *ref;
    PFty_t t;

    if ((name = attribute_value (atts, "name") )) {
        /* @name present */
        if ((type = attribute_value (atts, "type") ))
            /* @type present */
            t = PFty_elem (imported_qname (name),
                           PFty_named (ref_qname (type)));
        else
            /* @type absent */
            t = PFty_elem (imported_qname (name),
                           PFty_xs_anyType ());
    }
    else
        /* @name absent */
        if ((ref = attribute_value (atts, "ref")))
            /* @ref present */
            t = PFty_named_elem (ref_qname (ref));
        else
            PFoops (OOPS_SCHEMAIMPORT,
                    "missing `name' or `ref' attribute in local element declaration");

    push_type (occurs (atts, t));
}

/**
 * Map a local element declaration <element name="n"> t </element>
 * (and apply occurrence attributes)
 *
 * @param atts attributes of the <element> element
 */
static void
end_local_element_wsub (PFarray_t *atts)
{
    /*  [[ <element name="n" minOccurs="m" maxOccurs="M"> t </element> ]]  -->
     *  occurs (m, M, element n { [[ t ]] })
     */
    char *name;
    PFty_t t;

    if ((name = attribute_value (atts, "name")))
        t = PFty_elem (imported_qname (name), pop_type ());
    else
        PFoops (OOPS_SCHEMAIMPORT,
                "missing `name' attribute in local element declaration");

    push_type (occurs (atts, t));
}

/**
 * Map an (empty) type extension <extension base=.../>
 *
 * @param atts attributes of the <extension> element
 */
static void
end_extension_eps (PFarray_t *atts)
{
    /*  [[ <extension base="n"/> ]]  -->  named n
     */
    char *base;

    if ((base = attribute_value (atts, "base"))) {
        push_type (PFty_named (ref_qname (base)));

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `base' attribute in derivation by extension or restriction");
}

/**
 * Map a type extension: <extension base=...> a0 ... ak </extension>
 *
 * @param atts attributes of the <extension> element
 */
static void
end_simple_extension_watts (PFarray_t *atts)
{
    /*  [[ <extension base="n"> a0 ... ak </extension> ]]  -->
     *  ([[ a0 ]] & ... & [[ ak ]]), named n
     */
    char *base;

    /**
     * @bug: the attributes (all group) defined in t needs to be merged with
     *       the attributes (all group) of type n (if present)
     */
    if ((base = attribute_value (atts, "base"))) {
        push_type (PFty_seq (pop_type (),
                             PFty_named (ref_qname (base))));

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `base' attribute in derivation by extension");
}

/**
 * Map a type extension <extension base=...> t </extension>
 *
 * @param atts attributes of the <extension> element
 */

static void
end_extension (PFarray_t *atts)
{
    /*  [[ <extension base="n"> t </extension> ]]  -->  named n , [[ t ]]
     */
    char *base;

    /**
     * @bug: the attributes (all group) defined in t needs to be merged with
     *       the attributes (all group) of type n (if present)
     */
    if ((base = attribute_value (atts, "base"))) {
        push_type (PFty_seq (PFty_named (ref_qname (base)),
                             pop_type ()));

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `base' attribute in derivation by extension or restriction");
}

/**
 * Map a type extension <extension base=...> a0 ... ak </extension>
 *
 * @param atts attributes of the <extension> element
 */

static void
end_extension_watts (PFarray_t *atts)
{
    /*  [[ <extension base="n"> a0 ... ak </extension> ]]  -->
     *  ([[ a0 ]] & ... & [[ ak ]]) & named n
     */
    char *base;

    /**
     * @bug: the attributes (all group) defined in t needs to be merged with
     *       the attributes (all group) of type n (if present)
     */
    if ((base = attribute_value (atts, "base"))) {
        push_type (PFty_all (pop_type (),
                             PFty_named (ref_qname (base))));

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `base' attribute in derivation by extension");
}

/**
 * Map a type extension w/ attributes:
 * <extension base=...> t a0 ... ak </extension>
 *
 * @param atts attributes of the <extension> element
 */
static void
end_extension_wboth (PFarray_t *atts)
{
    /*  [[ <extension base="n"> t a0 ... ak </extension> ]]  -->
     *  (([[ a0 ]] & ... & [[ ak ]]) & named n), [[ t ]]
     */
    end_extension_watts (atts);
    combine_atts_partial_type (0);
}

/**
 * Map a local group reference <group ref=.../> (and apply occurrence
 * attributes)
 *
 * @param atts attributes of the <group> element
 */
static void
end_local_group (PFarray_t *atts)
{
    /*  [[ <group ref="n" minOccurs="m" maxOccurs="M"/> ]]  -->
     *  occurs (m, M, named "n")
     */
    char *ref;

    if ((ref = attribute_value (atts, "ref"))) {
        push_type (occurs (atts, PFty_named_group (ref_qname (ref))));

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `ref' attribute in group reference");
}

/**
 * Map an (empty) restriction <restriction base=.../>
 *
 * @param atts attributes of the <restriction> element
 */
static void
end_restriction_wbase (PFarray_t *atts)
{
    /*  [[ <restriction base="n"/> ]]  -->  named n
     */
    char *base;

    if ((base = attribute_value (atts, "base"))) {
        push_type (PFty_named (ref_qname (base)));

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `base' attribute in derivation by restriction");
}


/**
 * Map a list definition <list itemType=.../>
 *
 * @param atts attributes of the <list> element
 */
static void
end_list_wbase (PFarray_t *atts)
{
    /*  [[ <list itemType="n"/> ]]  -->  (named n)*
     */
    char *itemType;

    if ((itemType = attribute_value (atts, "itemType"))) {
        push_type (PFty_star (PFty_named (ref_qname (itemType))));

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `itemType' attribute in list");
}

/**
 * Map a choice type definition <union memberTypes=.../>
 *
 * @param atts attributes of the <list> element
 */
static void
end_union_wbase (PFarray_t *atts)
{
    /*  [[ <union memberTypes="n0 n1 ... nk"/> ]]  -->
     *  (...(named n0 | named n1)...) | named nk
     */
    char *memberTypes;
    size_t n;
    char *ws = " \f\n\r\t\v";
    PFty_t t;


    if ((memberTypes = attribute_value (atts, "memberTypes"))) {
        /* initialize result type (none | s = s) */
        t = PFty_none ();
        /* skip ws */
        memberTypes += strspn (memberTypes, ws);

        while ((n = strcspn (memberTypes, ws))) {
            /* type reference (of length n) found */
            t = PFty_choice (t,
                             PFty_named (ref_qname (PFstrndup (memberTypes,
                                                               n))));

            /* skip over type and ws */
            memberTypes += n;
            memberTypes += strspn (memberTypes, ws);
        }

        push_type (*PFty_simplify (t));

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `memberTypes' attribute in union");
}

/**
 * Map an attribute group reference <attributeGroup ref=.../>
 *
 * @param atts attributes of the <attributeGroup> element
 */
static void
end_local_attrgrp (PFarray_t *atts)
{
    /*  [[ <attributeGroup ref="n"/> ]]  -->  named n
     */
    char *ref;

    if ((ref = attribute_value (atts, "ref"))) {
        push_type (PFty_named_attrgroup (ref_qname (ref)));

        /* go to `attribute seen' state */
        state = dfa[state][STACKCOL];

        /* execute `attribute seen' state actions */
        actions[state] (0);

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `ref' attribute in local attribute group");
}

/**
 * Map an (empty) local attribute declaration:
 * <attribute name=... type=.../> or <attribute name=.../> or
 * <attribute ref=.../>
 * (and apply attribute occurrence attributes)
 *
 * @param atts attributes of the <attribute> element
 */
static void
end_local_attribute (PFarray_t *atts)
{
    /*  [[ <attribute name="n" type="t" use="u"/> ]]  -->
     *  attr_occurs (u, attribute n { named t })
     *
     *  [[ <attribute name="n" use="u"/> ]]  -->
     *  attr_occurs (u, attribute n { atomic* })
     *
     *  [[ <attribute ref="n" use="u"/> ]]  -->
     *  attr_occurs (u, named n)
     */
    char *name;
    char *type;
    char *ref;
    PFty_t t;

    if ((name = attribute_value (atts, "name"))) {
        if ((type = attribute_value (atts, "type")))
            /* @type present */
            t = PFty_attr (imported_qname (name),
                           PFty_named (ref_qname (type)));
        else
            /* @type absent => atomic* */
            t = PFty_attr (imported_qname (name),
                           PFty_star (PFty_atomic ()));
    }
    else
        if ((ref = attribute_value (atts, "ref")))
            t = PFty_named_attr (ref_qname (ref));
        else
            PFoops (OOPS_SCHEMAIMPORT,
                    "missing `name' or `ref' attribute in local attribute declaration");

    push_type (attr_occurs (atts, t));

    /* go to `attribute seen' state */
    state = dfa[state][STACKCOL];

    /* execute `attribute seen' state actions */
    actions[state] (0);
}

/**
 * Map a local attribute declaration <attribute name=...> t </attribute>
 * (and apply attribute occurrence attributes)
 *
 * @param atts attributes of the <attribute> element
 */
static void
end_local_attribute_wsub (PFarray_t *atts)
{
    /*  [[ <attribute name="n" use="u"> t </attribute> ]]  -->
     *  attr_occurs (u, attribute n { [[ t ]] })
     */
    char *name;

    if ((name = attribute_value (atts, "name"))) {
        push_type (attr_occurs (atts, PFty_attr (imported_qname (name),
                                                 pop_type ())));

        /* go to `attribute seen' state */
        state = dfa [state][STACKCOL];

        /* execute `attribute seen' state actions */
        actions[state] (0);

        return;
    }

    PFoops (OOPS_SCHEMAIMPORT,
            "missing `name' attribute in local attribute declaration");
}

/**
 * Map <any> (and apply occurrence attributes)
 *
 * @param atts attributes of the <any> element
 */
static void
end_any (PFarray_t *atts)
{
    /*  [[ <any minOccurs="m" maxOccurs="M"/> ]]  -->
     *  occurs (m, M, anyElement)
     */
    push_type (occurs (atts, PFty_xs_anyElement ()));
}

/**
 * Map <anyAttribute>
 *
 * @param atts attributes of the <anyAttribute> element
 */
static void
end_anyAttribute (PFarray_t *atts)
{
    /*  [[ <anyAttribute/> ]]  -->  anyAttribute*
     */
    (void)atts; /* not used */
    push_type (PFty_star (PFty_xs_anyAttribute ()));

    /* go to `attribute seen' state */
    state = dfa[state][STACKCOL];
    /* execute `attribute seen' state actions */
    actions[state] (0);
}

/**
 * binds the callbacks to the xml SAXHandler
 */
static xmlSAXHandler schema_import_sax = {
    .startElement          = schema_import_start_element
  , .endElement            = schema_import_end_element
  , .error                 = 0
  , .internalSubset        = 0
  , .isStandalone          = 0
  , .hasInternalSubset     = 0
  , .hasExternalSubset     = 0
  , .resolveEntity         = 0
  , .getEntity             = 0
  , .entityDecl            = 0
  , .notationDecl          = 0
  , .attributeDecl         = 0
  , .elementDecl           = 0
  , .unparsedEntityDecl    = 0
  , .setDocumentLocator    = 0
  , .startDocument         = 0
  , .endDocument           = 0
  , .reference             = 0
  , .characters            = 0
  , .ignorableWhitespace   = 0
  , .processingInstruction = 0
  , .comment               = 0
  , .warning               = 0
  , .fatalError            = 0 
  , .getParameterEntity    = 0
  , .cdataBlock            = 0
  , .externalSubset        = 0
  , .initialized           = 0
};

static action actions [173] = {
    /* START                              0 */  nop
    /* start schema                       1 */ ,start_schema
    /*   between schema'                  2 */ ,nop
    /* end schema'                        3 */ ,nop
    /* start global element               4 */ ,nop
    /*   between global element'          5 */ ,nop
    /* end global element                 6 */ ,end_top_level_element
    /* end global element'                7 */ ,end_top_level_element_wsub
    /* start local element                8 */ ,nop
    /*   between local element'           9 */ ,nop
    /* end local element                 10 */ ,end_local_element
    /* end local element'                11 */ ,end_local_element_wsub
    /* start global complexType          12 */ ,nop
    /*   between global complexType'     13 */ ,nop
    /* end global complexType            14 */ ,end_top_level_eps
    /* end global complexType'           15 */ ,end_top_level_type
    /* end global complexType''          16 */ ,end_top_level_complex_type_wboth
    /* start local complexType           17 */ ,nop
    /*   between local complexType'      18 */ ,nop
    /* end local complexType             19 */ ,end_local_complex_type_eps
    /* end local complexType'            20 */ ,nop
    /* end local complexType''           21 */ ,combine_atts_partial_type
    /* start global simpleType           22 */ ,nop
    /*   between global simpleType'      23 */ ,nop
    /* end global simpleType'            24 */ ,end_top_level_type
    /* start local simpleType            25 */ ,nop
    /*   between local simpleType'       26 */ ,nop
    /* end local simpleType'             27 */ ,nop
    /* start simpleContent               28 */ ,nop
    /*   between simpleContent'          29 */ ,nop
    /* end simpleContent'                30 */ ,nop
    /* start complexContent              31 */ ,nop
    /*   between complexContent'         32 */ ,nop
    /* end complexContent'               33 */ ,nop
    /* start extension (simple)          34 */ ,nop
    /* end extension (simple)            35 */ ,end_extension_eps
    /* end extension' (simple)           36 */ ,end_simple_extension_watts
    /* start extension (complex)         37 */ ,nop
    /*   between extension' (complex)    38 */ ,nop
    /* end extension (complex)           39 */ ,end_extension_eps
    /* end extension' (type) (complex)   40 */ ,end_extension
    /* end extension' (atts) (complex)   41 */ ,end_extension_watts
    /* end extension'' (complex)         42 */ ,end_extension_wboth
    /* start restriction (sT)            43 */ ,start_restriction
    /*   between restriction' (sT)       44 */ ,nop
    /*   between restriction @base (sT)  45 */ ,nop
    /* end restriction (sT)              46 */ ,end_restriction_wbase
    /* end restriction' (sT)             47 */ ,nop
    /* start restriction (simple)        48 */ ,start_simple_restriction
    /*   between restriction' (simple)   49 */ ,nop
    /*   betw restr @base (simple)       50 */ ,nop
    /* end restriction (simple)          51 */ ,end_extension_eps
    /* end restriction' (type) (simple)  52 */ ,nop
    /* end restriction' (atts) (simple)  53 */ ,end_simple_extension_watts
    /* end restriction'' (simple)        54 */ ,combine_atts_partial_type
    /* start restriction (complex)       55 */ ,nop
    /*   between restriction' (complex)  56 */ ,nop
    /* end restriction (complex)         57 */ ,end_extension_eps
    /* end restriction' (complex)        58 */ ,nop
    /* end restriction'' (complex)       59 */ ,combine_atts_partial_type
    /* start global group                60 */ ,nop
    /*   between global group'           61 */ ,nop
    /* end global group                  62 */ ,end_top_level_eps_group
    /* end global group'                 63 */ ,end_top_level_group
    /* start local group                 64 */ ,nop
    /* end local group                   65 */ ,end_local_group
    /* start all                         66 */ ,nop
    /*   between all'                    67 */ ,goto_stack
    /*   between all''                   68 */ ,combine_all
    /* end all                           69 */ ,end_all_empty
    /* end all'                          70 */ ,nop
    /* start choice                      71 */ ,nop
    /*   between choice'                 72 */ ,goto_stack
    /*   between choice''                73 */ ,combine_choice
    /* end choice                        74 */ ,end_choice_empty
    /* end choice'                       75 */ ,end_choice_seq
    /* start sequence                    76 */ ,nop
    /*   between sequence'               77 */ ,goto_stack
    /*   between sequence''              78 */ ,combine_sequence
    /* end sequence                      79 */ ,end_seq_empty
    /* end sequence'                     80 */ ,end_choice_seq
    /* start list                        81 */ ,start_list
    /*   between list'                   82 */ ,nop
    /*   between list @itemType          83 */ ,nop
    /* end list                          84 */ ,end_list_wbase
    /* end list'                         85 */ ,end_list
    /* start union                       86 */ ,start_union
    /*   between union'                  87 */ ,goto_stack
    /*   between union''                 88 */ ,combine_choice
    /*   between union @memberTypes      89 */ ,nop
    /* end union                         90 */ ,end_union_wbase
    /* end union'                        91 */ ,nop
    /* start any                         92 */ ,nop
    /* end any                           93 */ ,end_any
    /* start global attributeGroup       94 */ ,nop
    /* end global attributeGroup         95 */ ,end_top_level_eps_attrgrp
    /* end global attributeGroup'        96 */ ,end_top_level_attrgrp
    /* start global attribute            97 */ ,nop
    /*   between global attribute'       98 */ ,nop
    /* end global attribute              99 */ ,end_top_level_attribute
    /* end global attribute'            100 */ ,end_top_level_attribute_wsub
    /* start anyAttribute (1)           101 */ ,nop
    /* end anyAttribute (1)             102 */ ,end_anyAttribute
    /* start local attribute (1)        103 */ ,nop
    /*   between local attribute' (1)   104 */ ,nop
    /* end local attribute (1)          105 */ ,end_local_attribute
    /* end local attribute' (1)         106 */ ,end_local_attribute_wsub
    /* start local attributeGroup (1)   107 */ ,nop
    /* end local attributeGroup (1)     108 */ ,end_local_attrgrp
    /*   attribute seen' (1)            109 */ ,nop
    /* start anyAttribute (1')          110 */ ,nop
    /* end anyAttribute (1')            111 */ ,end_anyAttribute
    /* start local attribute (1')       112 */ ,nop
    /*   between local attribute' (1')  113 */ ,nop
    /* end local attribute (1')         114 */ ,end_local_attribute
    /* end local attribute' (1')        115 */ ,end_local_attribute_wsub
    /* start local attributeGroup (1')  116 */ ,nop
    /* end local attributeGroup (1')    117 */ ,end_local_attrgrp
    /*   attribute seen'' (1')          118 */ ,combine_all
    /* start anyAttribute (2)           119 */ ,nop
    /* end anyAttribute (2)             120 */ ,end_anyAttribute
    /* start local attribute (2)        121 */ ,nop
    /*   between local attribute' (2)   122 */ ,nop
    /* end local attribute (2)          123 */ ,end_local_attribute
    /* end local attribute' (2)         124 */ ,end_local_attribute_wsub
    /* start local attributeGroup (2)   125 */ ,nop
    /* end local attributeGroup (2)     126 */ ,end_local_attrgrp
    /*   attribute seen' (2)            127 */ ,nop
    /* start anyAttribute (2')          128 */ ,nop
    /* end anyAttribute (2')            129 */ ,end_anyAttribute
    /* start local attribute (2')       130 */ ,nop
    /*   between local attribute' (2')  131 */ ,nop
    /* end local attribute (2')         132 */ ,end_local_attribute
    /* end local attribute' (2')        133 */ ,end_local_attribute_wsub
    /* start local attributeGroup (2')  134 */ ,nop
    /* end local attributeGroup (2')    135 */ ,end_local_attrgrp
    /*   attribute seen'' (2')          136 */ ,combine_all
    /* start anyAttribute (3)           137 */ ,nop
    /* end anyAttribute (3)             138 */ ,end_anyAttribute
    /* start local attribute (3)        139 */ ,nop
    /*   between local attribute' (3)   140 */ ,nop
    /* end local attribute (3)          141 */ ,end_local_attribute
    /* end local attribute' (3)         142 */ ,end_local_attribute_wsub
    /* start local attributeGroup (3)   143 */ ,nop
    /* end local attributeGroup (3)     144 */ ,end_local_attrgrp
    /*   attribute seen' (3)            145 */ ,nop
    /* start anyAttribute (3')          146 */ ,nop
    /* end anyAttribute (3')            147 */ ,end_anyAttribute
    /* start local attribute (3')       148 */ ,nop
    /*   between local attribute' (3')  149 */ ,nop
    /* end local attribute (3')         150 */ ,end_local_attribute
    /* end local attribute' (3')        151 */ ,end_local_attribute_wsub
    /* start local attributeGroup (3')  152 */ ,nop
    /* end local attributeGroup (3')    153 */ ,end_local_attrgrp
    /*   attribute seen'' (3')          154 */ ,combine_all
    /* start anyAttribute (4)           155 */ ,nop
    /* end anyAttribute (4)             156 */ ,end_anyAttribute
    /* start local attribute (4)        157 */ ,nop
    /*   between local attribute' (4)   158 */ ,nop
    /* end local attribute (4)          159 */ ,end_local_attribute
    /* end local attribute' (4)         160 */ ,end_local_attribute_wsub
    /* start local attributeGroup (4)   161 */ ,nop
    /* end local attributeGroup (4)     162 */ ,end_local_attrgrp
    /*   attribute seen' (4)            163 */ ,nop
    /* start anyAttribute (4')          164 */ ,nop
    /* end anyAttribute (4')            165 */ ,end_anyAttribute
    /* start local attribute (4')       166 */ ,nop
    /*   between local attribute' (4')  167 */ ,nop
    /* end local attribute (4')         168 */ ,end_local_attribute
    /* end local attribute' (4')        169 */ ,end_local_attribute_wsub
    /* start local attributeGroup (4')  170 */ ,nop
    /* end local attributeGroup (4')    171 */ ,end_local_attrgrp
    /*   attribute seen'' (4')          172 */ ,combine_all
};

static int dfa[173][37] = {
   /*                                            |  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14| 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36*/
   /*                                            |                 a       c           START-EVENT                       |                 a       c           END-EVENT                             */
   /*                                            |                 t       o                                   s         |                 t       o                                   s             */
   /*                                            |         a       t       m                                   i         |         a       t       m                                   i             */
   /*                                            |         n       r       p   c                   r           m         |         n       r       p   c                   r           m           S */
   /*                                            |         y       i       l   o                   e           p   s     |         y       i       l   o                   e           p   s       T */
   /*                                            |         A   a   b       e   m       e           s           l   i     |         A   a   b       e   m       e           s           l   i       A */
   /*                                            |         t   t   u       x   p       x           t       s   e   m     |         t   t   u       x   p       x           t       s   e   m       C */
   /*                                            |         t   t   t       C   l   e   t           r       e   C   p     |         t   t   t       C   l   e   t           r       e   C   p       K */
   /*                                            |         r   r   e   c   o   e   l   e           i   s   q   o   l     |         r   r   e   c   o   e   l   e           i   s   q   o   l       C */
   /*                                            |         i   i   G   h   n   x   e   n   g       c   c   u   n   e   u |         i   i   G   h   n   x   e   n   g       c   c   u   n   e   u   O */
   /*                                            |         b   b   r   o   t   T   m   s   r   l   t   h   e   t   T   n |         b   b   r   o   t   T   m   s   r   l   t   h   e   t   T   n   L */
   /*                                            | a   a   u   u   o   i   e   y   e   i   o   i   i   e   n   e   y   i | a   a   u   u   o   i   e   y   e   i   o   i   i   e   n   e   y   i   U */
   /*                                            | l   n   t   t   u   c   n   p   n   o   u   s   o   m   c   n   p   o | l   n   t   t   u   c   n   p   n   o   u   s   o   m   c   n   p   o   M */
   /*                                            | l   y   e   e   p   e   t   e   t   n   p   t   n   a   e   t   e   n | l   y   e   e   p   e   t   e   t   n   p   t   n   a   e   t   e   n   N */
   /*                                                                                                                                                                                                */
   /* START                            */ [  0]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  1,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  0},
   /* start schema                     */ [  1]={  _,  _,  _, 97, 94,  _,  _, 12,  4,  _, 60,  _,  _,  _,  _,  _, 22,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  3,  _,  _,  _,  _,  2},
   /*   between schema'                */ [  2]={  _,  _,  _, 97, 94,  _,  _, 12,  4,  _, 60,  _,  _,  _,  _,  _, 22,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  3,  _,  _,  _,  _, -1},
   /* end schema'                      */ [  3]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  0},
   /* start global element             */ [  4]={  _,  _,  _,  _,  _,  _,  _, 17,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,  _,  _,  _,  _,  _,  6,  _,  _,  _,  _,  _,  _,  _,  _,  _,  5},
   /*   between global element'        */ [  5]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  7,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global element               */ [  6]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global element'              */ [  7]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start local element              */ [  8]={  _,  _,  _,  _,  _,  _,  _, 17,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,  _,  _,  _,  _,  _, 10,  _,  _,  _,  _,  _,  _,  _,  _,  _,  9},
   /*   between local element'         */ [  9]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 11,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local element                */ [ 10]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local element'               */ [ 11]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start global complexType         */ [ 12]={ 66,  _,101,103,107, 71, 31,  _,  _,  _, 64,  _,  _,  _, 76, 28,  _,  _,  _,  _,  _,  _,  _,  _,  _, 14,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 13},
   /*   between global complexType'    */ [ 13]={  _,  _,119,121,125,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 15,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global complexType           */ [ 14]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global complexType'          */ [ 15]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global complexType''         */ [ 16]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start local complexType          */ [ 17]={ 66,  _,137,139,143, 71, 31,  _,  _,  _, 64,  _,  _,  _, 76, 28,  _,  _,  _,  _,  _,  _,  _,  _,  _, 19,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 18},
   /*   between local complexType'     */ [ 18]={  _,  _,155,157,161,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 20,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local complexType            */ [ 19]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local complexType'           */ [ 20]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local complexType''          */ [ 21]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start global simpleType          */ [ 22]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 81, 43,  _,  _,  _,  _, 86,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 23},
   /*   between global simpleType'     */ [ 23]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 24,  _, -1},
   /* end global simpleType'           */ [ 24]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start local simpleType           */ [ 25]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 81, 43,  _,  _,  _,  _, 86,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 26},
   /*   between local simpleType'      */ [ 26]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 27,  _, -1},
   /* end local simpleType'            */ [ 27]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start simpleContent              */ [ 28]={  _,  _,  _,  _,  _,  _,  _,  _,  _, 34,  _,  _, 48,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 29},
   /*   between simpleContent'         */ [ 29]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 30,  _,  _, -1},
   /* end simpleContent'               */ [ 30]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start complexContent             */ [ 31]={  _,  _,  _,  _,  _,  _,  _,  _,  _, 37,  _,  _, 55,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 32},
   /*   between complexContent'        */ [ 32]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 33,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end complexContent'              */ [ 33]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start extension (simple)         */ [ 34]={  _,  _,137,139,143,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 35,  _,  _,  _,  _,  _,  _,  _,  _, 34},
   /* end extension (simple)           */ [ 35]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end extension' (simple)          */ [ 36]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start extension (complex)        */ [ 37]={ 66,  _,101,103,107, 71,  _,  _,  _,  _, 64,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 39,  _,  _,  _,  _,  _,  _,  _,  _, 38},
   /*   between extension' (complex)   */ [ 38]={  _,  _,119,121,125,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 40,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end extension (complex)          */ [ 39]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end extension' (type) (complex)  */ [ 40]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end extension' (atts) (complex)  */ [ 41]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end extension'' (complex)        */ [ 42]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start restriction (sT)           */ [ 43]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 44},
   /*   between restriction' (sT)      */ [ 44]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 47,  _,  _,  _,  _,  _, -1},
   /*   betw. restr. @base (sT)*/ [RESTRICTION]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 46,  _,  _,  _,  _,  _, 45},
   /* end restriction (sT)             */ [ 46]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end restriction' (sT)            */ [ 47]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start restriction (simple)       */ [ 48]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 49},
   /*   between restriction' (simple)  */ [ 49]={  _,  _,137,139,143,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 52,  _,  _,  _,  _,  _, -1},
   /*   betw. restr. @base */ [RESTRICTION_SIM]={  _,  _,155,157,161,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 51,  _,  _,  _,  _,  _, 50},
   /* end restriction (simple)         */ [ 51]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end restriction' (type) (simple) */ [ 52]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end restriction' (atts) (simple) */ [ 53]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end restriction'' (simple)       */ [ 54]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start restriction (complex)      */ [ 55]={ 66,  _,101,103,107, 71,  _,  _,  _,  _, 64,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 57,  _,  _,  _,  _,  _, 56},
   /*   between restriction' (complex) */ [ 56]={  _,  _,119,121,125,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 58,  _,  _,  _,  _,  _, -1},
   /* end restriction (complex)        */ [ 57]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end restriction' (complex)       */ [ 58]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end restriction'' (complex)      */ [ 59]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start global group               */ [ 60]={ 66,  _,  _,  _,  _, 71,  _,  _,  _,  _,  _,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 62,  _,  _,  _,  _,  _,  _,  _, 61},
   /*   between global group'          */ [ 61]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 63,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global group                 */ [ 62]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global group'                */ [ 63]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start local group                */ [ 64]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 65,  _,  _,  _,  _,  _,  _,  _, 64},
   /* end local group                  */ [ 65]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start all                        */ [ 66]={  _,  _,  _,  _,  _,  _,  _,  _,  8,  _,  _,  _,  _,  _,  _,  _,  _,  _, 69,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 67},
   /*   between all'                   */ [ 67]={  _,  _,  _,  _,  _,  _,  _,  _,  8,  _,  _,  _,  _,  _,  _,  _,  _,  _, 70,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 68},
   /*   between all''                  */ [ 68]={  _,  _,  _,  _,  _,  _,  _,  _,  8,  _,  _,  _,  _,  _,  _,  _,  _,  _, 70,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end all                          */ [ 69]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end all'                         */ [ 70]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start choice                     */ [ 71]={  _, 92,  _,  _,  _, 71,  _,  _,  8,  _, 64,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _, 74,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 72},
   /*   between choice'                */ [ 72]={  _, 92,  _,  _,  _, 71,  _,  _,  8,  _, 64,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _, 75,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 73},
   /*   between choice''               */ [ 73]={  _, 92,  _,  _,  _, 71,  _,  _,  8,  _, 64,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _, 75,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end choice                       */ [ 74]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end choice'                      */ [ 75]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start sequence                   */ [ 76]={  _, 92,  _,  _,  _, 71,  _,  _,  8,  _, 64,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 79,  _,  _,  _, 77},
   /*   between sequence'              */ [ 77]={  _, 92,  _,  _,  _, 71,  _,  _,  8,  _, 64,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 80,  _,  _,  _, 78},
   /*   between sequence''             */ [ 78]={  _, 92,  _,  _,  _, 71,  _,  _,  8,  _, 64,  _,  _,  _, 76,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 80,  _,  _,  _, -1},
   /* end sequence                     */ [ 79]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end sequence'                    */ [ 80]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start list                       */ [ 81]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 82},
   /*   between list'                  */ [ 82]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 85,  _,  _,  _,  _,  _,  _, -1},
   /*   between list @itemType        */ [LIST]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 84,  _,  _,  _,  _,  _,  _, 83},
   /* end list                         */ [ 84]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end list'                        */ [ 85]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start union                      */ [ 86]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 87},
   /*   between union'                 */ [ 87]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 91, 88},
   /*   between union''                */ [ 88]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 91, -1},
   /*   between union @memberTypes   */ [UNION]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 90, 89},
   /* end union                        */ [ 90]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end union'                       */ [ 91]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start any                        */ [ 92]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 93,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 92},
   /* end any                          */ [ 93]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start global attributeGroup      */ [ 94]={  _,  _,101,103,107,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 95,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 94},
   /* end global attributeGroup        */ [ 95]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global attributeGroup'       */ [ 96]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* start global attribute           */ [ 97]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _, 99,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 98},
   /*   between global attribute'      */ [ 98]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,100,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global attribute             */ [ 99]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end global attribute'            */ [100]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /*      first attribute, attributeGroup or anyAttribute called from
            Top Level Complex Type without other type, complex Extension without other type, complex Restriction without other type or Top Level Attribute Group */
   /* start anyAttribute (1)           */ [101]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,102,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,101},
   /* end anyAttribute (1)             */ [102]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,109},
   /* start local attribute (1)        */ [103]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,105,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,104},
   /*   between local attribute' (1)   */ [104]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,106,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local attribute (1)          */ [105]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,109},
   /* end local attribute' (1)         */ [106]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,109},
   /* start local attributeGroup (1)   */ [107]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,108,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,107},
   /* end local attributeGroup (1)     */ [108]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,109},
   /*   attribute seen' (1)            */ [109]={  _,  _,110,112,116,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 96,  _,  _, 15,  _, 41,  _,  _, 58,  _,  _,  _,  _,  _,  0},
   /*      2 or more attributes, attributeGroups or anyAttribute called from
            Top Level Complex Type without other type, complex Extension without other type, complex Restriction without other type or Top Level Attribute Group */
   /* start anyAttribute (1')          */ [110]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,111,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,110},
   /* end anyAttribute (1')            */ [111]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,118},
   /* start local attribute (1')       */ [112]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,114,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,113},
   /*   between local attribute' (1')  */ [113]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,115,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local attribute (1')         */ [114]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,118},
   /* end local attribute' (1')        */ [115]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,118},
   /* start local attributeGroup (1')  */ [116]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,117,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,116},
   /* end local attributeGroup (1')    */ [117]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,118},
   /*   attribute seen'' (1')          */ [118]={  _,  _,110,112,116,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 96,  _,  _, 15,  _, 41,  _,  _, 58,  _,  _,  _,  _,  _,  0},
   /*      first attribute, attributeGroup or anyAttribute called from
            Top Level Complex Type with other type, complex Extension with other type or complex Restriction with other type */
   /* start anyAttribute (2)           */ [119]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,120,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,119},
   /* end anyAttribute (2)             */ [120]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,127},
   /* start local attribute (2)        */ [121]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,123,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,122},
   /*   between local attribute' (2)   */ [122]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,124,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local attribute (2)          */ [123]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,127},
   /* end local attribute' (2)         */ [124]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,127},
   /* start local attributeGroup (2)   */ [125]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,126,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,125},
   /* end local attributeGroup (2)     */ [126]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,127},
   /*   attribute seen' (2)            */ [127]={  _,  _,128,130,134,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 16,  _, 42,  _,  _, 59,  _,  _,  _,  _,  _,  0},
   /*      2 or more attributes, attributeGroups or anyAttribute called from
            Top Level Complex Type with other type, complex Extension with other type or complex Restriction with other type */
   /* start anyAttribute (2')          */ [128]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,129,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,128},
   /* end anyAttribute (2')            */ [129]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,136},
   /* start local attribute (2')       */ [130]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,132,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,131},
   /*   between local attribute' (2')  */ [131]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,133,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local attribute (2')         */ [132]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,136},
   /* end local attribute' (2')        */ [133]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,136},
   /* start local attributeGroup (2')  */ [134]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,135,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,134},
   /* end local attributeGroup (2')    */ [135]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,136},
   /*   attribute seen'' (2')          */ [136]={  _,  _,128,130,134,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 16,  _, 42,  _,  _, 59,  _,  _,  _,  _,  _,  0},
   /*      first attribute, attributeGroup or anyAttribute called from
            Local Complex Type without other subtype, simple Extension or simple Restriction with other type */
   /* start anyAttribute (3)           */ [137]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,138,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,137},
   /* end anyAttribute (3)             */ [138]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,145},
   /* start local attribute (3)        */ [139]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,141,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,140},
   /*   between local attribute' (3)   */ [140]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,142,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local attribute (3)          */ [141]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,145},
   /* end local attribute' (3)         */ [142]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,145},
   /* start local attributeGroup (3)   */ [143]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,144,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,143},
   /* end local attributeGroup (3)     */ [144]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,145},
   /*   attribute seen' (3)            */ [145]={  _,  _,146,148,152,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 20,  _, 36,  _,  _, 54,  _,  _,  _,  _,  _,  0},
   /*      2 or more attributes, attributeGroups or anyAttribute called from
            Local Complex Type without other subtype, simple Extension or simple Restriction with other type */
   /* start anyAttribute (3')          */ [146]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,147,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,146},
   /* end anyAttribute (3')            */ [147]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,154},
   /* start local attribute (3')       */ [148]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,150,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,149},
   /*   between local attribute' (3')  */ [149]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,151,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local attribute (3')         */ [150]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,154},
   /* end local attribute' (3')        */ [151]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,154},
   /* start local attributeGroup (3')  */ [152]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,153,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,152},
   /* end local attributeGroup (3')    */ [153]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,154},
   /*   attribute seen'' (3')          */ [154]={  _,  _,146,148,152,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 20,  _, 36,  _,  _, 54,  _,  _,  _,  _,  _,  0},
   /*      first attribute, attributeGroup or anyAttribute called from
            Local Complex Type with other subtype or simple Restriction without other type */
   /* start anyAttribute (4)           */ [155]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,156,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,155},
   /* end anyAttribute (4)             */ [156]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,163},
   /* start local attribute (4)        */ [157]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,159,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,158},
   /*   between local attribute' (4)   */ [158]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,160,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local attribute (4)          */ [159]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,163},
   /* end local attribute' (4)         */ [160]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,163},
   /* start local attributeGroup (4)   */ [161]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,162,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,161},
   /* end local attributeGroup (4)     */ [162]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,163},
   /*   attribute seen' (4)            */ [163]={  _,  _,164,166,170,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 21,  _,  _,  _,  _, 53,  _,  _,  _,  _,  _,  0},
   /*      2 or more attributes, attributeGroups or anyAttribute called from
            Local Complex Type with other subtype or simple Restriction without other type */
   /* start anyAttribute (4')          */ [164]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,165,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,164},
   /* end anyAttribute (4')            */ [165]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,172},
   /* start local attribute (4')       */ [166]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 25,  _,  _,  _,  _,168,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,167},
   /*   between local attribute' (4')  */ [167]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,169,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, -1},
   /* end local attribute (4')         */ [168]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,172},
   /* end local attribute' (4')        */ [169]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,172},
   /* start local attributeGroup (4')  */ [170]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,171,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,170},
   /* end local attributeGroup (4')    */ [171]={  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,172},
   /*   attribute seen'' (4')          */ [172]={  _,  _,164,166,170,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _,  _, 21,  _,  _,  _,  _, 53,  _,  _,  _,  _,  _,  0}
};

/**
 * Discard libxml2 warning/error messages.
 *
 * @param ctx SAX parser context (unused)
 * @param msg libxml2 warning/error message (unused)
 */
static void
silent_error (void *ctx, const char *msg, ...)
{
    (void) ctx;
    (void) msg;
}

/*
 * print_error
 * @ctx:  an error context
 * @msg:  the message to display/transmit
 * @...:  extra parameters for the message display
 * 
 * Default handler for out of context error messages.
 */
void
append_error(void *ctx, const char *msg, ...) {
    char* buf = (char*) ctx;
    int len = strlen(buf);
    va_list args;

    va_start(args, msg);
    snprintf(buf+len, OOPS_SIZE-len, msg, args);
    va_end(args);
}


/**
 * Imports a XML Schema file located at URI @xsd_loc and
 * maps XML Schema type definitions to Pathfinder (XQuery) types
 * using a type mapping DFA.
 *
 * @param ns target namespace for this schema import
 * @param xsd_loc location of XML Schema file
 */
static void
schema_import (PFns_t ns, char *xsd_loc)
{
    xmlParserCtxtPtr ctx;

    /* save the target namespace for this schema import */
    target_ns = ns;

    /* we need an XML Schema URI */
    if (! xsd_loc)
        PFoops (OOPS_SCHEMAIMPORT, "missing XML Schema URI");

    /* initial state of type mapping DFA */
    state = 0;

    /* creates a new stack (state stack) */
    state_stack = PFarray (sizeof (int));
    /* pushes a first dummy element on the state stack */
    push_state (-1);

    /* creates a new stack (pf_type stack) */
    type_stack = PFarray (sizeof (PFty_t));
    /* creates a new stack (attr_stack) */
    attr_stack = PFarray (sizeof (PFarray_t*));
    /* creates a new stack (ns_stack) */
    ns_stack = PFarray (sizeof (PFns_t*));

    /* temporarily: ignore any libxml2 errors */
    xmlSetGenericErrorFunc (0, silent_error);

    /* setup the SAX parser context */
    ctx = xmlCreateFileParserCtxt (xsd_loc);

    if (! ctx)
        PFoops (OOPS_SCHEMAIMPORT,
                "failed to open XML Schema file at URI `%s'", xsd_loc);
               
    /* hook in the callbacks driving the type mapping DFA */
    ctx->sax = &schema_import_sax;

    /* reset libxml2 error handling */
    xmlSetGenericErrorFunc (PFerrbuf, append_error);

    /* parse the XML Schema file */
    (void) xmlParseDocument (ctx);

    if (! ctx->wellFormed)
        PFoops (OOPS_SCHEMAIMPORT, "check schema well-formedness");
}

#endif  /* HAVE_LIBXML2 */

/**
 * Walk the query prolog's decl_imps chain and initiate a schema import
 * for each schm_imp node (with given XML Schema location) found.
 *
 * @param di root of right-deep decl_imps chain
 */
void
schema_imports (PFpnode_t *di)
{
    assert (di);

    switch (di->kind) {
    case p_decl_imps: {
        PFpnode_t *imp;

        /* if this is a schema import with a specified XML Schema
         * location, process the import
         *
         * abstract syntax tree layout:
         *
         *          decl_imps
         *           /     \
         *     schm_imp    /\
         *       /  \     /__\
         * lit_str  lit_str
         *  (uri)    (loc)
         */
        imp = L(di);

        if (imp->kind == p_schm_imp
                && R(imp)->kind == p_schm_ats
                && RL(imp)->kind == p_lit_str) {

#if HAVE_LIBXML2
            PFns_t ns;
            char *uri;
            char *loc;

            /* access XML Schema location */
            loc = RL(imp)->sem.str;

            /* access target namespace URI for this import */
            assert (L(imp)->kind == p_lit_str);
            uri = L(imp)->sem.str;

            /* construct target namespace */
            ns = (PFns_t) { .prefix = "", .uri = PFstrdup (uri) };

            /* import the schema */
            schema_import (ns, loc);
#else
            PFoops (OOPS_FATAL,
                    "XML Schema import support not available"
                    " (run configure --with-libxml2)");
#endif
        }

        /* walk down the declaration and imports chain */
        schema_imports (di->child[1]);
        break;
    }

    case p_nil:
        break;

    default:
        PFoops (OOPS_FATAL,
                "non-decl_imps node in query prolog decl_imps chain");
    }
}

/**
 * Check the well-formedness (regularity) of the type imported under
 * the given name @a qn in symbol space @a sym_space.
 */
#define REGULARITY(sym_space)                                        \
static void regularity_##sym_space (PFqname_t qn, void *defn)        \
{                                                                    \
    (void) defn;                                                     \
                                                                     \
    if (PFty_regularity (PFty_##sym_space (qn)))                     \
        return;                                                      \
                                                                     \
    PFoops (OOPS_SCHEMAIMPORT,                                       \
            "illegal non-regular type %s = %s",                      \
            PFqname_str (qn),                                        \
            PFty_str (*(PFty_schema (PFty_##sym_space (qn)))));      \
}

REGULARITY (named)
REGULARITY (named_elem)
REGULARITY (named_attr)
REGULARITY (named_group)
REGULARITY (named_attrgroup)

/**
 * Analyze the query prolog and initiate XML Schema imports for each
 * `import schema ns = uri at loc' found in the prolog.
 *
 * Perform well-formedness test (regularity) for all imported types.
 *
 * @param root root of abstract syntax tree
 */
void
PFschema_import (PFpnode_t *root)
{
    /*
     *           main_mod                          lib_mod
     *          /        \                         /     \
     *     decl_imps     ...         or         mod_ns  decl_imps
     *     /      \                                     /      \
     *   ...     decl_imps                            ...      decl_imps
     *              \                                             \
     *              ...                                           ...
     *
     */
    assert (root);

    switch (root->kind) {
        case p_main_mod:
            assert (root->child[0]);
            schema_imports (root->child[0]);
            break;

        case p_lib_mod:
            assert (root->child[1]);
            schema_imports (root->child[1]);
            break;

        default:
            PFoops (OOPS_FATAL,
                    "illegal parse tree encountered during schema import.");
            break;
    }

    /* check imported types for well-formedness (ensure regularity) */
    PFenv_iterate (PFtype_defns,      regularity_named);
    PFenv_iterate (PFelem_decls,      regularity_named_elem);
    PFenv_iterate (PFattr_decls,      regularity_named_attr);
    PFenv_iterate (PFgroup_defns,     regularity_named_group);
    PFenv_iterate (PFattrgroup_defns, regularity_named_attrgroup);
}

/* vim:set shiftwidth=4 expandtab: */
