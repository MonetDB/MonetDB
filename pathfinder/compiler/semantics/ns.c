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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"

#include <assert.h>
#include <string.h>

#include "ns.h"

#include "nsres.h"
#include "oops.h"

/*
 * XML NS that are predefined for any query (may be used without
 * prior declaration) in XQuery, see W3C XQuery, 4.12
 */
/** Predefined namespace `xml' for any query */
PFns_t PFns_xml = 
    { .ns  = "xml", 
      .uri = "http://www.w3.org/XML/1998/namespace" };
/** Predefined namespace `xs' (XML Schema) for any query */
PFns_t PFns_xs  = 
    { .ns  = "xs",  
      .uri = "http://www.w3.org/2001/XMLSchema" };
/** Predefined namespace `xsi' (XML Schema Instance) for any query */
PFns_t PFns_xsi = 
    { .ns  = "xsi", 
      .uri = "http://www.w3.org/2001/XMLSchema-instance" };
/** XQuery default function namespace (fn:...). */
PFns_t PFns_fn  =
    { .ns  = "fn",  
      .uri = "http://www.w3.org/2002/11/xquery-functions" };
/** Predefined namespace `xdt' (XPath Data Types) for any query */
PFns_t PFns_xdt = 
    { .ns  = "xdt",
      .uri = "http://www.w3.org/2003/11/xpath-datatypes" };
/** Predefined namespace `local' (XQuery Local Functions) for any query */
PFns_t PFns_local = 
    { .ns  = "local",
      .uri = "http://www.w3.org/2003/11/xquery-local-functions" };


/**
 * XQuery operator namespace (op:...)
 * (see W3C XQuery 1.0 and XPath 2.0 Function and Operators, 1.5).
 *
 * This namespace is not accessible for the user 
 * (see W3C XQuery F&O, 1.5).
 */
PFns_t PFns_op  = { .ns  = "op",  
                    .uri = "http://www.w3.org/2002/08/xquery-operators" };


/** 
 * Pathfinder's own internal NS (pf:...).
 * Note that the prefix contains a character that cannot be entered in
 * a query. 
 *
 * This namespace is not accessible for the user.
 */ 
PFns_t PFns_pf  = { .ns  = "#pf",  
                    .uri = "http://www.inf.uni-konstanz.de/Pathfinder" };

/**
 * Wildcard namespace.
 * Used in QNames of the form *:loc.
 */
PFns_t PFns_wild = { .ns = "*", .uri = "*" };

/**
 * XQuery default element NS.  Intially the default element NS is
 * undefined (see W3C XQuery, 4.1) but this may be overridden via
 * `default element namespace = "..."' in the query prolog)
 */
static PFns_t ens;

/**
 * XQuery default function NS.  Intially the default function NS is
 * xf:...  (see W3C XQuery, 4.1) but this may be overridden via
 * `default function namespace = "..."' in the query prolog)
 */
static PFns_t fns;

/**
 * Does QName @a qn actually have a NS prefix?
 */
#define NS_QUAL(qn) ((qn).ns.ns)

/**
 * NS prefix of XML NS declaration attributes `xmlns:...'
 * (see W3C Namespaces, 2 [PrefixedAttName, DefaultAttName])
 */
#define XMLNS "xmlns"

/**
 * The in-scope NS environment (W3C XQuery, 4.1).  During NS
 * resolution, all NS currently in scope are to be found in this
 * environment.  When NS resolution starts, the four NS xml:...,
 * xs:..., xsd:..., and xsi:... are in scope.
 */
static PFarray_t *namespace = NULL;

/** current # of NS in scope */
static unsigned ns_num = 0;

/**
 * Add NS @a ns to the environment of in-scope NS
 * (an already existing NS with identical prefix will be shadowed).
 *
 * @param ns NS to insert into to the in-scope environment
 */
static void
ns_add (PFns_t ns)
{
    *((PFns_t *) PFarray_at (namespace, ns_num++)) = ns;
}

/**
 * Test to see if a NS with the same prefix as NS @a ns is in-scope
 * (if so, update @a ns to be that NS).
 *
 * @param ns (pointer to) NS whose prefix we try to look up in the
 *   in-scope environment (will be replaced on success)
 * @return indicates if lookup has been successful
 */
static bool
ns_lookup (PFns_t *ns)
{
    unsigned nsi = ns_num;

    assert (ns && ns->ns);

    while (nsi--)
        if (! strcmp (ns->ns, ((PFns_t *) PFarray_at (namespace, nsi))->ns)) {
            *ns = *((PFns_t *) PFarray_at (namespace, nsi));
            return true;
        }

    return false;
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
    if (ns1.ns) {
        if (ns2.ns)
            return strcmp (ns1.ns, ns2.ns);
        else
            return -1;
    }
    else {
        if (ns2.ns)
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
 * @param cc parent of owning element content node
 */
static void
apply_xmlns (PFpnode_t *c, PFpnode_t **cc)
{
    PFpnode_t *a;

    assert (c);
  
    switch (c->kind) {

    case p_contseq:
    case p_exprseq:
        assert (c->child[0]);
        if (c->child[0]->kind == p_attr) {
            /* abstract syntax tree layout:
             *
             *           attr 
             *           /  \
             *  tag-ns:loc   v
             */
            a = c->child[0];
      
            /* if attribute comes with a literal name resolve NS usage,
             * for computed attribute names skip ahead
             */
            assert (a->child[0]);
            if (a->child[0]->kind == p_tag) {
                if (NS_QUAL (a->child[0]->sem.qname) &&
                    ! strcmp (a->child[0]->sem.qname.ns.ns, XMLNS)) {
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
                    if ((a->child[1]->kind    == p_exprseq
                         || a->child[1]->kind == p_contseq) &&
                        a->child[1]->child[0]->kind == p_lit_str &&
                        a->child[1]->child[1]->kind == p_empty_seq) {
                        ns_add ((PFns_t) { .ns  = a->child[0]->sem.qname.loc,
                                           .uri = a->child[1]
                                                      ->child[0]->sem.str });

                        /* finally remove this NS declaration attribute */
                        assert (cc);
                        *cc = c->child[1];
                    }
                    else
                        PFoops_loc (OOPS_BADNS, a->child[0]->loc,
                                    "namespace declaration attribute %s is "
                                    "required to have non-empty literal "
                                    "string value",
                                    PFqname_str (a->child[0]->sem.qname));
          
                }
                else {
                    if (! strcmp (a->child[0]->sem.qname.loc, XMLNS)) {
                        /* this is a NS declaration attribute of the form
                         * `xmlns=ns': re-define the default element NS to be ns
                         * (ns is required to be a literal string which may
                         * optionally be empty; in the latter case the default
                         * element NS is undefined, W3C Namespaces, 5.2), W3C
                         * XQuery, 4.1
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
                        switch (a->child[1]->kind) {
                        case p_empty_seq:
                            /*
                             * empty NS declaration attribute:
                             * undefine default element NS
                             */
                            ens = (PFns_t) { .ns = 0, .uri = 0 };
                            break;

                        case p_exprseq:
                        case p_contseq:
                            if (a->child[1]->child[0]->kind == p_lit_str &&
                                a->child[1]->child[1]->kind == p_empty_seq) {
                                /* non-empty NS declaration attribute */
                                ens = (PFns_t) { .ns  =  0,
                                                 .uri = a->child[1]
                                                            ->child[0]->sem.str
                                               };
                                break;
                            }

                        default:
                            PFoops_loc (OOPS_BADNS, a->child[0]->loc,
                                        "namespace declaration attribute %s is "
                                        "required to have literal string value",
                                        PFqname_str (a->child[0]->sem.qname));
                        }
            
                        /* finally remove this NS declaration attribute */
                        assert (cc);
                        *cc = c->child[1];          
                    }
                }
            }
        }
        break;

    default: /* p_nil */
        return;
    }

    apply_xmlns (c->child[1], &(c->child[1]));
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

    PFns_t ens_;       /* used to save default element NS
                        * (elements may have local default element NS)
                        */
    unsigned ns_num_;  /* used to save state of in-scope NS environment
                        * (elements may use NS declaration attribute to
                        * define local NS)
                        */
    assert (n);

    switch (n->kind) {

        /* handle query prolog */

    case p_mod_ns:
        PFoops (OOPS_NOTSUPPORTED,
                "Pathfinder does currently not support XQuery modules.");

    case p_fns_decl:
        /* default function namespace = "foo" 
         *
         * abstract syntax tree layout:
         *
         *           fns_decl
         *              |
         *           lit_str-"foo"
         */
        fns = (PFns_t) { .ns = NULL, .uri = n->child[0]->sem.str };

        break;

    case p_ens_decl:
        /* default element namespace = "foo" 
         *
         * abstract syntax tree layout:
         *
         *           ens_decl
         *              |
         *           lit_str-"foo"
         */
        ens = (PFns_t) { .ns = NULL, .uri = n->child[0]->sem.str };

        break;

    case p_ns_decl:
        /* declare namespace ns = "foo" 
         * import schema namespace ns = "foo" [at "url"]
         *
         * abstract syntax tree layout:
         *
         *           ns_decl-"ns"
         *              |
         *           lit_str-"foo"
         */
        ns_add ((PFns_t) { .ns = n->sem.str, .uri = n->child[0]->sem.str });

        break;

    case p_fun_decl:
        /* define function ns:loc (parm) [returns t] { body }
         *
         * abstract syntax tree layout:
         *
         *           fun_decl-ns:loc
         *            /           \
         *         fun_sig       body
         *          /   \
         *       param   t
         *
         *           /  |  \
         *        parm  t  body
         */
        if (! NS_QUAL (n->sem.qname))
            /* function name unqualfied, attach default function NS,
             * see W3C XQuery, 4.1
             */
            n->sem.qname.ns = fns;
        else 
            /* function name qualified, make sure the NS is in scope */
            if (! ns_lookup (&(n->sem.qname.ns)))
                PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified function name %s", 
                            PFqname_str (n->sem.qname));

        /* NS resolution in function parameters, return type, and body */
        assert (n->child[0] && n->child[0]->child[0]);
        ns_resolve (n->child[0]->child[0]);

        assert (n->child[0]->child[1]);
        ns_resolve (n->child[0]->child[1]);

        assert (n->child[1]);
        ns_resolve (n->child[1]);

        break;

        /* handle named types and schema references */

    case p_atom_ty:
    case p_named_ty:
        if (NS_QUAL (n->sem.qname))
            if (! ns_lookup (&(n->sem.qname.ns)))
                PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified name %s",
                            PFqname_str (n->sem.qname));
        break;

    case p_req_name:
        /*
         * This is the QName specification in an XPath location step. 
         * 
         * The spec says: ``An unprefixed QName, when used as a name
         * test on an axis whose principal node kind is element, has
         * the namespace URI of the default element/type namespace in
         * the expression context; otherwise, it has no namespace URI.''
         *
         * BUG: We cannot check for the principale node kind here. (In
         *      other words: We don't know if this happens to be the
         *      attribute axis.)
         *
         * Furthermore: ``A node test * is true for any node of the
         * principal node kind of the step axis.''
         */
        if (! NS_QUAL (n->sem.qname)) {

            if (PFQNAME_LOC_WILDCARD (n->sem.qname))
                n->sem.qname.ns = PFns_wild;
            else if (ens.uri) {
                n->sem.qname.ns = ens;
            }

        } 
        else {
            if (! ns_lookup (&(n->sem.qname.ns)))
                PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified name %s",
                            PFqname_str (n->sem.qname));
        }
        break;

        /* handle query body */

    case p_varref:
        /* $ns:loc                       (variable)
         */
        if (NS_QUAL (n->sem.qname))
            if (! ns_lookup (&(n->sem.qname.ns)))
                PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified variable name $%s",
                            PFqname_str (n->sem.qname));

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
        if (! NS_QUAL (n->sem.qname))
            /* function name unqualfied, attach default function NS,
             * see W3C XQuery, 4.1
             */
            n->sem.qname.ns = fns;
        else 
            /* function name qualified, make sure the NS is in scope */
            if (! ns_lookup (&(n->sem.qname.ns)))
                PFoops_loc (OOPS_BADNS, n->loc,
                            "unknown namespace in qualified function name %s", 
                            PFqname_str (n->sem.qname));

        /* NS resolution in function arguments */
        assert (n->child[0]);
        ns_resolve (n->child[0]);

        break;

    case p_elem:
        /* <ns:loc>c</ns:loc>          (element construction)
         *
         * abstract syntax tree layout:
         *
         *           elem
         *           /   \
         *  tag-ns:loc    c
         */

        /* collect and apply namespace declaration attribute of the
         * form `xmlns:loc="..."' or `xmlns="..."' found in element content c
         */

        /* remember original default element NS and in-scope NS environment */
        ens_    = ens;
        ns_num_ = ns_num;

        assert (n->child[1]);
        apply_xmlns (n->child[1], &(n->child[1]));

        /* if element comes with a literal tag name resolve NS usage,
         * for computed tags skip ahead 
         */
        assert (n->child[0]);
        if (n->child[0]->kind == p_tag) {
            if (! NS_QUAL (n->child[0]->sem.qname)) {
                /* element tag name is unqualified: if a default element NS
                 * has been defined, attach the default NS, otherwise the tag
                 * name remains unqualified, W3C XQuery, 4.1
                 */
                if (ens.uri)
                    n->child[0]->sem.qname.ns = ens;
            }
            else 
                /* element tag name is qualified, make sure NS is in scope */
                if (! ns_lookup (&(n->child[0]->sem.qname.ns)))
                    PFoops_loc (OOPS_BADNS, n->child[0]->loc,
                                "unknown namespace in qualified tag name %s",
                                PFqname_str (n->child[0]->sem.qname));
        }
        else 
            ns_resolve (n->child[0]);

        /* NS resolution in element content c */
        assert (n->child[1]);
        ns_resolve (n->child[1]);
    
        /* restore old default element NS and in-scope NS environment */
        ens    = ens_;
        ns_num = ns_num_;

        break;

    case p_attr:
        /* <... ns:loc=v ...>        (attribute)
         *
         * abstract syntax tree layout:
         *
         *           attr
         *           /  \
         * tag-ns:loc    v 
         */
    
        /* if attribute comes with a literal tag name resolve NS usage,
         * for computed attribute names skip ahead
         */
        assert (n->child[0]);
        if (n->child[0]->kind == p_tag) {
            if (NS_QUAL (n->child[0]->sem.qname)) {
                /* qualified attribute name, check that NS is in scope */
                if (! ns_lookup (&(n->child[0]->sem.qname.ns)))
                    PFoops_loc (OOPS_BADNS, n->child[0]->loc,
                                "unknown namespace in qualified attribute "
                                "name %s",
                                PFqname_str (n->child[0]->sem.qname));
            }
        }
    
        /* NS resolution in attribute value v */
        assert (n->child[1]);
        ns_resolve (n->child[1]);

        break;

    default:
        for (c = 0; c < PFPNODE_MAXCHILD && n->child[c]; c++)
            ns_resolve (n->child[c]);

        break;
    }
}


/**
 * Resolve NS usage in a query.
 *
 * @param root root of query abstract syntax tree
 */
void
PFns_resolve (PFpnode_t *root)
{
    namespace = PFarray (sizeof (PFns_t));
    ns_num = 0;

    /* bring the default NS into scope */
    ns_add (PFns_xml);
    ns_add (PFns_xs);
    ns_add (PFns_xsi);
    ns_add (PFns_pf);
    ns_add (PFns_xdt);
    ns_add (PFns_local);
    ns_add (PFns_wild);

    /* bring the function and operator NS into scope
     * and make fn:... the default function NS
     */
    ns_add (PFns_fn);
  
    fns = PFns_fn;

    /* ``undefine'' the default element NS */
    ens = (PFns_t) { .ns = 0, .uri = 0 };

    /* initiate the actual NS resolution */
    ns_resolve (root); 
}


/* vim:set shiftwidth=4 expandtab: */
