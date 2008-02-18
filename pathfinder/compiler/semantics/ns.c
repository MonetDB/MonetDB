/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Resolve XML namespaces (NS) in the abstract syntax tree (this is
 * mainly based on W3C XQuery, 4.1 and W3C XML Namespaces).
 *
 * Walk the abstract syntax tree to 
 *
 * (1) test if qualified names (QNames) refer to NS being actually in scope,
 *     and
 * (2) attach URIs associated with used NS prefixes.
 * 
 * During the tree walk, we detect and handle the following NS-relevant
 * XQuery constructs:
 *
 * @verbatim
   - module namespace ns = "uri"
   - declare namespace ns = "uri"
   - declare default element namespace = "uri"
   - declare default function namespace = "uri"
   - import schema namespace ns = "uri" [at "url"]
   - import schema default element namespace ns = "uri" [at "url"]
   - import module namespace ns = "uri" "module" [at "url"]

   - declare function ns:loc (...) {...}
   - named types and schema references
   - $ns:loc                  (variable usage)
   - ns:loc (...)             (function application)
   - .../ax::ns:loc           (name test)
   - <ns:loc> ... </ns:loc>   (element construction)
   - <... ns:loc="..." ...>   (attributes)
   - (# ns:loc ... #) { ... } (pragmas)
@endverbatim
 *
 * NS declaration attributes of the form `xmlns="uri"' and
 * `xmlns:ns="uri"' are understood and removed after NS resolution has
 * taken place (according to the XML data model, NS declaration are
 * not regular attributes of the owning element, see W3C XQuery, 4.1).
 *
 * Additionally, we define and export a number of pre-defined XQuery
 * NS, as well as the Pathfinder internal NS `#pf'.
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
#include <string.h>

#include "ns.h"

#include "nsres.h"
#include "oops.h"

/* PFstrdup() */
#include "mem.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/**
 * When we encounter a p_req_name node, this describes the QName of
 * an XPath name test.  QNames need to be handled differently, depending
 * on the principal node kind of the axis.  When we pass a p_node_ty
 * node (located above the p_req_name node), we record the principal
 * node kind of the step (actually, of the sequence type) here and have
 * it available when we later see the p_req_name node.
 */
static enum { element, attribute } principal;

/*
 * XML NS that are predefined for any query (may be used without
 * prior declaration) in XQuery, see W3C XQuery, 4.12
 */
/** Predefined namespace `xml' for any query */
PFns_t PFns_xml = 
    { .prefix = "xml", 
      .uri    = "http://www.w3.org/XML/1998/namespace" };
/** Predefined namespace `xs' (XML Schema) for any query */
PFns_t PFns_xs  = 
    { .prefix = "xs",  
      .uri    = "http://www.w3.org/2001/XMLSchema" };
/** Predefined namespace `xsi' (XML Schema Instance) for any query */
PFns_t PFns_xsi = 
    { .prefix = "xsi", 
      .uri    = "http://www.w3.org/2001/XMLSchema-instance" };
/** XQuery default function namespace (fn:...). */
PFns_t PFns_fn  =
    { .prefix = "fn",  
      .uri    = "http://www.w3.org/2005/xpath-functions" };
/** Predefined namespace `xdt' (XPath Data Types) for any query */
PFns_t PFns_xdt = 
    { .prefix = "xdt",
      .uri    = "http://www.w3.org/2003/11/xpath-datatypes" };
/** Predefined namespace `local' (XQuery Local Functions) for any query */
PFns_t PFns_local = 
    { .prefix = "local",
      .uri    = "http://www.w3.org/2005/xquery-local-functions" };
/**
 * XQuery Update Facility.
 * There's no URI in the specs.  Teggy suggested to use http://www.kicker.de/.
 * Namespace is not accessible to the user.
 */
PFns_t PFns_upd =
    { .prefix = "upd",
      .uri    = "http://www.kicker.de/" };


/**
 * XQuery operator namespace (op:...)
 * (see W3C XQuery 1.0 and XPath 2.0 Function and Operators, 1.5).
 *
 * This namespace is not accessible for the user 
 * (see W3C XQuery F&O, 1.5).
 */
PFns_t PFns_op  = { .prefix = "op",  
                    .uri    = "http://www.w3.org/2002/08/xquery-operators" };


/** 
 * Pathfinder's namespace for additional non-'fn' functions.
 */ 
PFns_t PFns_lib = { .prefix = "pf",  
                    .uri    = "http://www.pathfinder-xquery.org/" };

/** 
 * Pathfinder's namespace for additional tijah functions.
 */ 
PFns_t PFns_tijah = { .prefix = "tijah",  
                      .uri    = "http://dbappl.cs.utwente.nl/pftijah/" };

#ifdef HAVE_PROBXML
/**
 * Pathfinder's namespace for additional pxml support functions (pxmlsup:...)
 */
PFns_t PFns_pxmlsup = { .prefix = "pxmlsup",
                        .uri    = "http://dbappl.cs.utwente.nl/pxmlsup/" };
#endif

/** 
 * Pathfinder's own internal NS (pf:...).
 * Note that the prefix contains a character that cannot be entered in
 * a query. 
 *
 * This namespace is not accessible for the user.
 */ 
PFns_t PFns_pf  = { .prefix = "#pf",  
                    .uri    = "http://www.pathfinder-xquery.org/internal/" };

/**
 * Wildcard namespace.
 * Used in QNames of the form *:loc.
 */
PFns_t PFns_wild = { .prefix= NULL, .uri = NULL };

/**
 * XQuery default function NS.  Intially the default function NS is
 * xf:...  (see W3C XQuery, 4.1) but this may be overridden via
 * `default function namespace = "..."' in the query prolog)
 */
static PFns_t fns;

/**
 * Target namespace for an XQuery module definition.
 *
 * Each function and variable declared by the module must be within
 * this namespace.  When processing a query, we set this to the
 * wildcard namespace.
 */
static PFns_t target_ns;

/**
 * Does QName @a qn actually have a NS prefix?
 */
#define NS_QUAL(qn_raw) ((qn_raw).prefix && *((qn_raw).prefix))

/**
 * NS prefix of XML NS declaration attributes `xmlns:...'
 * (see W3C Namespaces, 2 [PrefixedAttName, DefaultAttName])
 */
#define XMLNS "xmlns"

/**
 * Statically known namespaces (W3C XQuery 2.1.1).  During NS
 * resolution, all NS currently in scope are to be found in this
 * environment.  When NS resolution starts, the predefined
 * namespaces xml:..., xs:..., xsd:..., and xsi:..., ... are
 * loaded into this list.  Note that we can only collect
 * *statically known* namespaces here.  This list may be different
 * from in-scope namespaces (see W3C XQuery 2.1.1).
 *
 * The default element namespace is represented in this list in
 * terms of a zero-length string for the NS prefix.
 */
static PFns_map_t *stat_known_ns = NULL;

/**
 * Add NS @a ns to the environment of in-scope NS
 * (an already existing NS with identical prefix will be shadowed).
 *
 * @param ns NS to insert into to the in-scope environment
 */
static void
ns_add (PFns_t ns)
{
    assert (ns.prefix && ns.uri);

    *((PFns_t *) PFarray_add (stat_known_ns)) = ns;
}

/**
 * Namespaces may be (re-)declared in the query code, but not the
 * two special namespace prefixes `xml' and `xmlns' (see
 * http://www.w3.org/TR/2004/REC-xml-names11-20040204/#xmlReserved).
 * This is a wrapper for ns_add() that checks for this situation.
 * Whenever we add the built-in namespaces, we call ns_add() directly.
 * For namespace declarations in the query, call user_ns_add() which
 * checks for the two special prefixes.
 *
 * @param loc  Location in the input query.  Error messages will contain
 *             this location information.
 * @param ns   Namespace that shall be added to the statically-known
 *             namespaces.
 */
static void
user_ns_add (PFloc_t loc, PFns_t ns)
{
    if ((! strcmp (ns.prefix, "xml")) || (! strcmp (ns.prefix, "xmlns")))
        PFoops_loc (OOPS_BADNS, loc,
                    "it is illegal to override the `%s' namespace prefix",
                    ns.prefix);

    ns_add (ns);
}

/**
 * Test to see if a NS with the same prefix as NS @a ns is in-scope
 * (if so, update @a ns to be that NS).
 *
 * @param ns (pointer to) NS whose prefix we try to look up in the
 *   in-scope environment (will be replaced on success)
 * @return indicates if lookup has been successful
 */
static char *
ns_lookup (const char *prefix)
{
    assert (prefix);

    for (unsigned int i = PFarray_last (stat_known_ns); i; i--) {
        assert(((PFns_t *) PFarray_at (stat_known_ns, i-1))->prefix);
        if (! strcmp (prefix,
                      ((PFns_t *) PFarray_at (stat_known_ns, i-1))->prefix)) {
            assert(((PFns_t *) PFarray_at (stat_known_ns, i-1))->uri);
            return PFstrdup (((PFns_t *) PFarray_at (stat_known_ns, i-1))->uri);
        }
    }

    return NULL;
}

/**
 * Create a copy of a namespace environment (typically the statically
 * known namespaces, #stat_known_ns).
 */
static PFns_map_t *
copy_ns_env (const PFns_map_t *src)
{
    PFns_map_t *dest = PFarray (sizeof (PFns_t));

    for (unsigned int i = 0; i < PFarray_last (src); i++)
        *((PFns_t *) PFarray_add (dest))
            = *((PFns_t *) PFarray_at ((PFns_map_t *) src, i));

    return dest;
}


/** 
 * NS equality (URI-based, then prefix-based):
 * two NS with different prefixes are considered equal if they are bound
 * to the same URI (see, W3C XQuery, 4.1).
 * @param ns1 Namespace 1
 * @param ns2 Namespace 2
 * @return An integer less than, greater than or equal to zero, if @a ns1
 *   is found to be lexicographically before, after, or to match @a ns2,
 *   respectively. (see the @c strcmp manpage)
 */
int
PFns_eq (PFns_t ns1, PFns_t ns2)
{
    /* NS equality is principally based on URI equality, so this is what
     * we test for first: 
     */
    if (ns1.uri) {
        if (ns2.uri)
            return strcmp (ns1.uri, ns2.uri);
        else
            return -1;
    }
    else
        if (ns2.uri)
            return 1;

    /* two unbound (URI-less) NS are deliberately assumed to be equal 
     * if their prefixes coincide:
     */
    if (ns1.prefix) {
        if (ns2.prefix)
            return strcmp (ns1.prefix, ns2.prefix);
        else
            return -1;
    }
    else {
        if (ns2.prefix)
            return 1;
        else
            return 0;
    }
    
}

/**
 * Collect and apply namespace declaration attributes 
 * (W3C Namespaces):
 *
 * (1) xmlns:loc=ns        locally (for the owning element) define
 *                         NS prefix `loc:...' |-> ns
 *                         (ns is required to be a literal string)
 * 
 * (2) xmlns=ns            locally (for the owning element) re-define
 *                         the default element NS
 *                         (ns is required to be a literal string,
 *                          if ns is empty, the default element NS
 *                          is undefined locally)
 *
 * At the same time, we remove such namespace declaration attributes here.
 *
 * @param c  owning element content node
 */
static void
collect_xmlns (PFpnode_t *c)
{
    PFpnode_t *a;
    PFpnode_t *next = R(c);

    assert (c);
  
    switch (c->kind) {

        case p_contseq:
        case p_exprseq:
            assert (L(c));
            if (L(c)->kind == p_attr) {
                /* abstract syntax tree layout:
                 *
                 *           attr 
                 *           /  \
                 *  tag-ns:loc   v
                 */
                a = L(c);

                /* if attribute comes with a literal name resolve NS usage,
                 * for computed attribute names skip ahead
                 */
                assert (L(a));
                if (L(a)->kind == p_tag) {
                    if (NS_QUAL (L(a)->sem.qname_raw) &&
                        ! strcmp (L(a)->sem.qname_raw.prefix, XMLNS)) {
                        /*
                         * this is a NS declaration attribute of the form
                         * `xmlns:foo=ns': bring `foo:...' |-> ns into scope
                         * (ns is required to be a non-empty literal string),
                         * W3C XQuery, 4.1
                         *
                         * abstract syntax tree layout:
                         *
                         *           attr (a)
                         *          /        \
                         *  "xmlns":loc     exprseq
                         *                 /       \
                         *             lit_str     empty_seq
                         *          
                         */     
                        if ((R(a)->kind == p_exprseq
                             || R(a)->kind == p_contseq) &&
                            RL(a)->kind == p_lit_str &&
                            RR(a)->kind == p_empty_seq) {
                            user_ns_add (
                                   a->loc,
                                   (PFns_t) { .prefix = L(a)->sem.qname_raw.loc,
                                              .uri    = RL(a)->sem.str });

                            /* finally remove this NS declaration attribute */
                            *c = *R(c);
                            
                            /*
                             * visit the "same" node again in the next
                             * iteration (we have just overwritten it)
                             */
                            next = c;
                        }
                        else
                            PFoops_loc (OOPS_BADNS, L(a)->loc,
                                    "namespace declaration attribute %s is "
                                    "required to have non-empty literal "
                                    "string value",
                                    PFqname_raw_str (L(a)->sem.qname_raw));

                    }
                    else {
                        if (! strcmp (L(a)->sem.qname_raw.loc, XMLNS)) {

                            /*
                             * this is a NS declaration attribute of the form
                             * `xmlns=ns': re-define the default element NS to
                             * be ns (ns is required to be a literal string
                             * which may optionally be empty; in the latter
                             * case the default element NS is undefined, W3C
                             * Namespaces, 5.2), W3C XQuery, 4.1
                             *
                             * abstract syntax tree layout:
                             *          
                             *           attr (a)
                             *          /        \
                             *     "xmlns"      exprseq
                             *                 /       \
                             *              lit_str   empty_seq
                             *
                             *           content                
                             *             / \          or        nil
                             *  "ns"-lit_str  nil           
                             */     
                            switch (R(a)->kind) {
                                case p_empty_seq:
                                    /*
                                     * empty NS declaration attribute:
                                     * undefine default element NS
                                     */
                                    user_ns_add (a->loc,
                                                 (PFns_t) { .prefix = "",
                                                            .uri = "" });
                                    break;

                                case p_exprseq:
                                case p_contseq:
                                    if (RL(a)->kind == p_lit_str &&
                                        RR(a)->kind == p_empty_seq) {
                                        /* non-empty NS declaration attribute */
                                        user_ns_add (a->loc,
                                                     (PFns_t) {
                                                .prefix = "",
                                                .uri    = RL(a)->sem.str });
                                        break;
                                    }

                                default:
                                    PFoops_loc (OOPS_BADNS, L(a)->loc,
                                            "namespace declaration attribute "
                                            "%s is required to have literal "
                                            "string value",
                                            PFqname_raw_str (
                                                L(a)->sem.qname_raw));
                            }

                            /* finally remove this NS declaration attribute */
                            *c = *R(c);

                            /*
                             * visit the "same" node again in the next
                             * iteration (we have just overwritten it)
                             */
                            next = c;
                        }
                    }
                }
            }
            break;

        default: /* p_nil */
            return;
    }

    collect_xmlns (next);
}

/**
 * Recursively walk abstract syntax tree to resolve NS usage. 
 *
 * @param n current abstract syntax tree node
 */
static void
ns_resolve (PFpnode_t *n)
{
    unsigned c = 0;    /* iterates over children of this node */

    assert (n);

    switch (n->kind) {

        /* handle query prolog */

        case p_lib_mod:
        {   /*
             * Isolate namespaces in module definitions from main query
             * (and between modules).
             *
             * Thus,
             *
             *  -- clear namespace environment (after saving the old one)
             *
             *  -- bring default namespaces into scope
             *
             *  -- process module body
             *
             *  -- restore old environment
             *
             * Parse tree situation:
             *
             *          lib_mod         module namespace foo = "http://...";
             *         /       \
             *  mod_ns(foo)     e
             *    |
             * lit_str(http://...)
             */
            PFns_map_t    *old_ns        = stat_known_ns;
            PFns_t         old_fns       = fns;
            PFns_t         old_target_ns = target_ns;

            stat_known_ns = PFarray (sizeof (PFns_t));

            /* bring the default NS into scope */
            ns_add (PFns_xml);
            ns_add (PFns_xs);
            ns_add (PFns_xsi);
            ns_add (PFns_pf);
            ns_add (PFns_xdt);
            ns_add (PFns_local);
            ns_add (PFns_lib);
            ns_add (PFns_tijah);
#ifdef HAVE_PROBXML
            ns_add (PFns_pxmlsup);
#endif
            ns_add (PFns_fn);

            fns = PFns_fn;

            /* ``undefine'' the default element NS */
            ns_add ((PFns_t) { .prefix = "", .uri = "" });

            /* set the target NS as given in the module declaration */
            target_ns = (PFns_t) { .prefix = L(n)->sem.str,
                                   .uri    = LL(n)->sem.str };

            user_ns_add (n->loc, target_ns);

            /* perform NS resolution in module */
            ns_resolve (R(n));

            /* restore everything */
            target_ns     = old_target_ns;
            fns           = old_fns;
            stat_known_ns = old_ns;

        } break;


        case p_fns_decl:
            /*
             * default function namespace = "foo" 
             *
             * abstract syntax tree layout:
             *
             *           fns_decl
             *              |
             *           lit_str-"foo"
             */
            fns = (PFns_t) { .prefix = "", .uri = L(n)->sem.str };

            break;

        case p_ens_decl:
            /*
             * default element namespace = "foo" 
             *
             * abstract syntax tree layout:
             *
             *           ens_decl
             *              |
             *           lit_str-"foo"
             */
            ns_add ((PFns_t) { .prefix = "", .uri = L(n)->sem.str });

            break;

        case p_ns_decl:
            /*
             * declare namespace ns = "foo" 
             * import schema namespace ns = "foo" [at "url"]
             *
             * abstract syntax tree layout:
             *
             *           ns_decl-"ns"
             *              |
             *           lit_str-"foo"
             */
            user_ns_add (n->loc,
                         (PFns_t) { .prefix = n->sem.str,
                                    .uri = L(n)->sem.str });

            break;

        case p_fun_decl:
            /*
             * define function ns:loc (parm) [as t] { body }
             *
             * abstract syntax tree layout:
             *
             *           fun_decl-ns:loc
             *            /           \
             *         fun_sig       body
             *          /   \
             *       param   t
             *             / | \
             *            /  |  \
             *         parm  t  body
             */
            if (! NS_QUAL (n->sem.qname_raw))
                /*
                 * function name unqualfied, attach default function NS,
                 * see W3C XQuery, 4.1
                 */
                n->sem.qname = PFqname (fns, n->sem.qname_raw.loc);
            else {
                /* function name qualified, make sure the NS is in scope */
                PFns_t new_ns = { .prefix = n->sem.qname_raw.prefix,
                                  .uri = ns_lookup (n->sem.qname_raw.prefix) };

                if (! new_ns.uri)
                    PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified function name %s", 
                            PFqname_raw_str (n->sem.qname_raw));

                n->sem.qname = PFqname (new_ns, n->sem.qname_raw.loc);
            }

            /*
             * If we are within a module, the function declaration must be
             * within the module's target namespace.
             */
            if (PFns_eq (target_ns, PFns_wild)
                    && PFns_eq (PFqname_ns (n->sem.qname), target_ns))
                PFoops_loc (OOPS_BADNS, n->loc,
                        "function %s must be declared within the module's "
                        "target namespace (%s = \"%s\")",
                        PFqname_str (n->sem.qname),
                        target_ns.prefix, target_ns.uri);

            /* NS resolution in function parameters, return type, and body */
            assert (L(n) && LL(n));
            ns_resolve (LL(n));

            assert (LR(n));
            ns_resolve (LR(n));

            assert (R(n));
            ns_resolve (R(n));

            break;


            /* handle named types and schema references */

        case p_atom_ty:
        case p_named_ty:
        {
            PFns_t new_ns = { .prefix = n->sem.qname_raw.prefix,
                              .uri    = ns_lookup (n->sem.qname_raw.prefix) };

            if (! new_ns.uri)
                PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified name %s",
                            PFqname_raw_str (n->sem.qname_raw));

            n->sem.qname = PFqname (new_ns, n->sem.qname_raw.loc);
        } break;

        case p_node_ty:
            /*
             * Set the `principal' flag to the requested principal node
             * kind.  This will infer the correct namespaces for
             * name tests with unqualified QNames.
             *
             * Parse tree situation:
             *
             *         step
             *          |
             *       node_ty
             *          |
             *       req_ty
             *       /    \
             * req_name   ...
             */
            if (n->sem.kind == p_kind_attr)
                principal = attribute;
            else
                principal = element;

            assert (L(n));
            ns_resolve (L(n));

            break;

        case p_req_name:
            /*
             * This is the QName specification in an XPath location step. 
             * 
             * The spec says: ``An unprefixed QName, when used as a name
             * test on an axis whose principal node kind is element, has
             * the namespace URI of the default element/type namespace in
             * the expression context; otherwise, it has no namespace URI.''
             */
            /* A NULL prefix means this is a wildcard namespace */
            if (! n->sem.qname_raw.prefix)
                n->sem.qname = PFqname (PFns_wild, n->sem.qname_raw.loc);
            /*
             * Unqualified attribute names (recognizable by a zero-length
             * prefix) shall not get the default element namespace (in
             * the environment of staticall-known namespaces listed with
             * a zero-length prefix), but "no namespace" (the zero-length
             * URI).
             */
            else if (principal == attribute && !*(n->sem.qname_raw.prefix))
                n->sem.qname = PFqname ((PFns_t) { .prefix = "", .uri = "" },
                                        n->sem.qname_raw.loc);
            else {
                PFns_t new_ns = { .prefix = n->sem.qname_raw.prefix,
                                  .uri = ns_lookup (n->sem.qname_raw.prefix) };
                if (! new_ns.uri)
                    PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified name %s",
                            PFqname_raw_str (n->sem.qname_raw));

                n->sem.qname = PFqname (new_ns, n->sem.qname_raw.loc);
            }
            break;

        case p_var_decl:
            /*
             * parse tree situation:
             *
             *          var_decl
             *         /        \
             *      var_type     expr
             *     /       \
             *  varref    type
             */
            ns_resolve (L(n));
            ns_resolve (R(n));

            /*
             * If we are within a module, the function declaration must be
             * within the module's target namespace.
             */
            if (PFns_eq (target_ns, PFns_wild)
                && PFns_eq (PFqname_ns (LL(n)->sem.qname), target_ns))
                PFoops_loc (OOPS_BADNS, n->loc,
                        "variable %s must be declared within the module's "
                        "target namespace (%s = \"%s\")",
                        PFqname_str (LL(n)->sem.qname),
                        target_ns.prefix, target_ns.uri);

            break;


            /* handle query body */

        case p_varref:
            /* $ns:loc                       (variable)    */

            assert (n->sem.qname_raw.prefix);

            /*
             * An unprefixed variable reference is in no namespace
             * (W3C XQuery 3.1.2)
             */
            if (! *(n->sem.qname_raw.prefix))
                n->sem.qname = PFqname ((PFns_t) { .prefix = "", .uri = "" },
                                        n->sem.qname_raw.loc);
            else {
                PFns_t new_ns = { .prefix = n->sem.qname_raw.prefix,
                                  .uri = ns_lookup (n->sem.qname_raw.prefix) };

                if (! new_ns.uri)
                    PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified variable name $%s",
                            PFqname_raw_str (n->sem.qname_raw));

                n->sem.qname = PFqname (new_ns, n->sem.qname_raw.loc);
            }
            break;

        case p_fun_ref:
            /* ns:loc (args)                 (function application)
             *
             * abstract syntax tree layout:
             *
             *           apply-ns:loc
             *             |
             *           args
             */
            if (! NS_QUAL (n->sem.qname_raw))
                /* function name unqualfied, attach default function NS,
                 * see W3C XQuery, 4.1
                 */
                n->sem.qname = PFqname (fns, n->sem.qname_raw.loc);
            else  {
                /* function name qualified, make sure the NS is in scope */
                PFns_t new_ns = { .prefix = n->sem.qname_raw.prefix,
                                  .uri = ns_lookup (n->sem.qname_raw.prefix) };

                if (! new_ns.uri)
                    PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified function name %s", 
                            PFqname_raw_str (n->sem.qname_raw));

                n->sem.qname = PFqname (new_ns, n->sem.qname_raw.loc);
            }

            /* NS resolution in function arguments */
            assert (L(n));
            ns_resolve (L(n));

            break;

        /* declare option ns:loc "option"; */
        case p_option:

            assert (n->sem.qname_raw.prefix);

            /*
             * The QName of an option MUST have a namespace.  There
             * is no default namespace for options!  (W3C XQuery 4.16)
             */
            if (! *(n->sem.qname_raw.prefix))
                PFoops_loc (OOPS_BADNS, n->loc,
                            "no namespace prefix given for option `%s'",
                            PFqname_raw_str (n->sem.qname_raw));
            else {
                PFns_t new_ns = { .prefix = n->sem.qname_raw.prefix,
                                  .uri = ns_lookup (n->sem.qname_raw.prefix) };

                if (! new_ns.uri)
                    PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified variable name $%s",
                            PFqname_raw_str (n->sem.qname_raw));

                n->sem.qname = PFqname (new_ns, n->sem.qname_raw.loc);
            }
            break;

        case p_elem:
        {   /* <ns:loc>c</ns:loc>          (element construction)
             *
             * abstract syntax tree layout:
             *
             *           elem
             *           /   \
             *  tag-ns:loc    c
             */

            /*
             * collect and apply namespace declaration attribute of the
             * form `xmlns:loc="..."' or `xmlns="..."' found in element
             * content c
             */

            /*
             * Remember original default element NS and in-scope NS env.
             *
             * We create a copy of this environment here, so namespaces
             * defined in the current element node cannot affect the
             * surrounding.  It should be safe to copy stat_known_ns
             * by reference afterwards.
             */
            PFns_map_t *old_ns = copy_ns_env (stat_known_ns);

            assert (R(n));
            collect_xmlns (R(n));

            /*
             * if element comes with a literal tag name resolve NS usage,
             * for computed tags skip ahead 
             */
            assert (L(n));
            if (L(n)->kind == p_tag) {
                PFns_t new_ns
                    = { .prefix = L(n)->sem.qname_raw.prefix,
                        .uri    = ns_lookup (L(n)->sem.qname_raw.prefix) };
                if (! new_ns.uri)
                    PFoops_loc (OOPS_BADNS, L(n)->loc,
                                "unknown namespace in qualified tag name %s",
                                PFqname_raw_str (L(n)->sem.qname_raw));

                L(n)->sem.qname = PFqname (new_ns, L(n)->sem.qname_raw.loc);
            }
            else 
                ns_resolve (L(n));

            /* NS resolution in element content c */
            assert (R(n));
            ns_resolve (R(n));

            /* restore old default element NS and in-scope NS environment */
            stat_known_ns = old_ns;

        } break;

        case p_attr:
            /* <... ns:loc=v ...>        (attribute)
             *
             * abstract syntax tree layout:
             *
             *           attr
             *           /  \
             * tag-ns:loc    v 
             */

            /*
             * if attribute comes with a literal tag name resolve NS usage,
             * for computed attribute names skip ahead
             */
            assert (L(n));
            if (L(n)->kind == p_tag) {
                if (NS_QUAL (L(n)->sem.qname_raw)) {
                    /* qualified attribute name, check that NS is in scope */
                    PFns_t new_ns
                        = { .prefix = L(n)->sem.qname_raw.prefix,
                            .uri    = ns_lookup (L(n)->sem.qname_raw.prefix) };

                    if (! new_ns.uri)
                        PFoops_loc (OOPS_BADNS, L(n)->loc,
                                "unknown namespace in qualified attribute "
                                "name %s",
                                PFqname_raw_str (L(n)->sem.qname_raw));

                    L(n)->sem.qname = PFqname (new_ns, L(n)->sem.qname_raw.loc);
                }
                else {
                    /*
                     * unqualified attributes are in no namespace
                     * (NOT in the default element namespace!)
                     */
                    L(n)->sem.qname
                        = PFqname ((PFns_t) { .prefix = "", .uri = "" },
                                   L(n)->sem.qname_raw.loc);
                }
            }
            else {
                /* computed attribute name */
                ns_resolve (L(n));
            }

            /* NS resolution in attribute value v */
            assert (R(n));
            ns_resolve (R(n));

            break;

        case p_pragma:
        {   /* (# ns:loc ... #) { ... }      (pragma)   */
            PFns_t new_ns
                = { .prefix = n->sem.pragma.qn.qname_raw.prefix,
                    .uri = ns_lookup (n->sem.pragma.qn.qname_raw.prefix) };

            if (! new_ns.uri)
                PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in pragma $%s",
                            PFqname_raw_str (n->sem.pragma.qn.qname_raw));

            n->sem.pragma.qn.qname
                = PFqname (new_ns, n->sem.pragma.qn.qname_raw.loc);

        } break;

        default:
            for (c = 0; c < PFPNODE_MAXCHILD && n->child[c]; c++)
                ns_resolve (n->child[c]);

            break;
    }
}


/**
 * Initialize the namespaces.
 */
void
PFns_init (void)
{
    stat_known_ns = PFarray (sizeof (PFns_t));

    /* bring the default NS into scope */
    ns_add (PFns_xml);
    ns_add (PFns_xs);
    ns_add (PFns_xsi);
    ns_add (PFns_pf);
    ns_add (PFns_xdt);
    ns_add (PFns_local);
    ns_add (PFns_lib);
    ns_add (PFns_tijah);
#ifdef HAVE_PROBXML
    ns_add (PFns_pxmlsup);
#endif

    /* bring the function and operator NS into scope
     * and make fn:... the default function NS
     */
    ns_add (PFns_fn);
  
    fns = PFns_fn;

    /* ``undefine'' the default element NS */
    ns_add ((PFns_t) { .prefix = "", .uri = "" });

    /* allow any namespace for declarations in queries */
    target_ns = PFns_wild;
}


/**
 * Resolve NS usage in a query.
 *
 * @param root root of query abstract syntax tree
 */
void
PFns_resolve (PFpnode_t *root)
{
    /* initiate the actual NS resolution */
    ns_resolve (root); 
}


/* vim:set shiftwidth=4 expandtab: */
