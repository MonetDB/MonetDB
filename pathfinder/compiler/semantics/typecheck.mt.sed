/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue { 

/*
 * Type inference (static semantics) and type checking for XQuery core.
 *
 * In this file, a reference to `W3C XQuery' refers to the W3C WD
 * `XQuery 1.0 and XPath 2.0 Formal Semantics', Draft Nov 15, 2002
 * http://www.w3.org/TR/2002/WD-query-semantics-20021115/
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
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

/*
 * NOTE (Revision Information):
 *
 * Changes in the Core2MIL_Summer2004 branch have been merged into
 * this file on July 15, 2004. I have tagged this file in the
 * Core2MIL_Summer2004 branch with `merged-into-main-15-07-2004'.
 *
 * For later merges from the Core2MIL_Summer2004, please only merge
 * the changes since this tag.
 *
 * Jens
 */


/*
 * Auxiliary routines related to type checking are located in this separate
 * included file to facilitate automated documentation via doxygen.
 */	
#include "typecheck_impl.c"

/* PFty_simplify */
#include "subtyping.h"

/*
 * We use the Unix `sed' tool to make `[[ e ]]' a synonym for
 * `(e)->type' (type of e). The following sed expressions
 * will do the replacement.
 *
 * (The following lines contain the special marker that is used
 * in the build process. The build process will search the file
 * for these markers, extract the sed expressions and feed the file
 * with these expressions through sed. Write sed expressions in
 * _exactly_ this style!)
 *
 *!sed 's/\[\[/(/g'
 *!sed 's/\]\]/)->type/g'
 *
 * (First line translates all `[[' into `(', second line translates all
 * `]]' into `)->type'.)
 */

};

node  var_
      lit_str
      lit_int
      lit_dec
      lit_dbl
      nil

      seq

      let
      for_

      apply
      arg

      typeswitch
      cases
      case_
      seqtype
      seqcast
      proof
      stattype

      ifthenelse

      locsteps

      ancestor
      ancestor_or_self
      attribute
      child_
      descendant
      descendant_or_self
      following
      following_sibling
      parent_
      preceding
      preceding_sibling
      self

      kind_node
      kind_comment
      kind_text
      kind_pi
      kind_doc
      kind_elem
      kind_attr

      namet

      elem
      attr 
      text
      doc 
      comment
      pi  
      tag

      true_
      false_
      root_
      empty_;

label Query
      CoreExpr
      BindingExpr
      ConditionalExpr
      SequenceExpr

      TypeswitchExpr
      SequenceType
      SequenceTypeCast
      SubtypingProof

      PathExpr
      LocationStep
      LocationSteps
      NodeTest
      KindTest

      ConstructorExpr

      BuiltIns

      FunctionAppl
      FunctionArgs
      FunctionArg

      Atom
      Var
      LiteralValue;

Query:           CoreExpr
    { assert ($$); /* avoid `root' unused warning */ }
    ;

CoreExpr:        Atom;
CoreExpr:        BindingExpr;
CoreExpr:        ConditionalExpr;
CoreExpr:        SequenceExpr;
CoreExpr:        TypeswitchExpr;
CoreExpr:        SubtypingProof;
CoreExpr:        SequenceTypeCast;
CoreExpr:        FunctionAppl;
CoreExpr:        PathExpr;
CoreExpr:        ConstructorExpr;
CoreExpr:        BuiltIns;

BindingExpr:     for_ (Var, nil, Atom, CoreExpr)
    { TOPDOWN; }
    =
    {   /* W3C XQuery, 5.8.2
         *
         *  E |- Atom : t1   E[Var : prime (t1)] |- CoreExpr : t2
         * -------------------------------------------------------
         *  for_ (Var, nil, Atom, CoreExpr) : t2 . quantifier (t1)
         */
        PFty_t t1, t2;
        
        /* nil : none */
        [[ $2$ ]] = PFty_none ();
        
        /* E |- Atom : t1 */
        tDO ($%2$);
        t1 = [[ $3$ ]];
        
        /* Var : prime (t1) */
        [[ $1$ ]] = *PFty_simplify (PFty_prime (PFty_defn (t1)));
        
        /* E[Var : prime (t1)] |- CoreExpr : t2 */
        assert (($1$)->sem.var);
        [[ ($1$)->sem.var ]] = [[ $1$ ]];
        tDO ($%3$);
        t2 = [[ $4$ ]];
        
        [[ $$ ]] = *PFty_simplify ((PFty_quantifier (PFty_defn (t1))) (t2));
    }
    ;

BindingExpr:     for_ (Var, Var, Atom, CoreExpr)
    { TOPDOWN; }
    =
    {   /* W3C XQuery, 5.8.2
         *
         *                    E |- Atom : t1   
         *  E[Var1:prime (t1), Var2:xs:integer] |- CoreExpr : t2
         * -------------------------------------------------------
         *  for_ (Var1, Var2, Atom, CoreExpr) : t2 . quantifier (t1)
         */
        PFty_t t1, t2;
        
        
        /* E |- Atom : t1 */
        tDO ($%3$);
        t1 = [[ $3$ ]];
        
        /* Var2 : xs:integer */
        [[ $2$ ]] = PFty_xs_integer ();
        assert (($2$)->sem.var);
        [[ ($2$)->sem.var ]] = [[ $2$ ]];
        
        /* Var1 : prime (t1) */
        [[ $1$ ]] = *PFty_simplify (PFty_prime (PFty_defn (t1)));
        
        /* E[Var1:prime (t1), Var2:xs:integer] |- CoreExpr : t2 */
        assert (($1$)->sem.var);
        [[ ($1$)->sem.var ]] = [[ $1$ ]];
        tDO ($%4$);
        t2 = [[ $4$ ]];
        
        [[ $$ ]] = *PFty_simplify ((PFty_quantifier (PFty_defn (t1))) (t2));
    }
    ;
             
BindingExpr:     let (Var, CoreExpr, CoreExpr)
    { TOPDOWN; } 
    =
    {   /* W3C XQuery, 5.8.3
         * 
         * E |- CoreExpr1 : t1   E[Var : t1] |- CoreExpr2 : t2
         * ----------------------------------------------------
         *      E |- let (Var, CoreExpr1, CoreExpr2) : t2
         */
        PFty_t t1;

        /* E |- CoreExpr1 : t1 */
        tDO ($%2$); 
        t1 = [[ $2$ ]];
        [[ $1$ ]] = t1;

        /* E[Var : t1] |- CoreExpr : t2 */
        assert (($1$)->sem.var);
        [[ ($1$)->sem.var ]] = t1;
        tDO ($%3$);
        
        [[ $$ ]] = [[ $3$ ]];
    }
    ;


ConditionalExpr: ifthenelse (Atom, CoreExpr, CoreExpr)
    =
    {   /* W3C XQuery, 5.10
         *
         * E |- Atom:boolean  E |- CoreExpr1:t1  E |- CoreExpr2:t2
         * -------------------------------------------------------
         *  E |- ifthenelse (Atom, CoreExp1, CoreExpr2) : t1 | t2
         */
        if (PFty_eq (PFty_defn ([[ $1$ ]]), PFty_boolean ()))
            [[ $$ ]] = *PFty_simplify (PFty_choice ([[ $2$ ]], [[ $3$ ]]));
        else
            PFoops (OOPS_TYPECHECK, 
                    "if-then-else condition of type %s (expected %s)",
                    PFty_str ([[ $1$ ]]),
                    PFty_str (PFty_boolean ()));
    }
    ;

SequenceExpr:    seq (Atom, Atom)
    =
    {   /* W3C XQuery, 5.3.1
         * 
         * E |- Atom1 : t1    E |- Atom2 : t2
         * ----------------------------------
         *  E |- seq (Atom1, Atom2) : t1, t2
         */
        PFty_t t1, t2;

        t1 = [[ $1$ ]];
        t2 = [[ $2$ ]];

        [[ $$ ]] = *PFty_simplify (PFty_seq (t1, t2));
    }
    ;

TypeswitchExpr:  typeswitch (Atom, 
                             cases (case_ (SequenceType, CoreExpr), nil), 
                             CoreExpr)
    =
    {   /* W3C XQuery, 5.12.2
         *
         *                   E |- Atom:t1  E |- SequenceType:t2  
         *           E |- CoreExpr1:t3    E |- CoreExpr2:t4    t1 <: t2
         * --------------------------------------------------------------------
         * E |- typeswitch (Atom, cases (case_ (SequenceType, CoreExpr1), nil),
         *                  CoreExpr2) : t3
         *
         *                   E |- Atom:t1  E |- SequenceType:t2  
         *           E |- CoreExpr1:t3    E |- CoreExpr2:t4    t1 || t2
         * --------------------------------------------------------------------
         * E |- typeswitch (Atom, cases (case_ (SequenceType, CoreExpr1), nil),
         *                  CoreExpr2) : t4
         *
         *                   E |- Atom:t1  E |- SequenceType:t2  
         *                 E |- CoreExpr1:t3    E |- CoreExpr2:t4  
         * --------------------------------------------------------------------
         * E |- typeswitch (Atom, cases (case_ (SequenceType, CoreExpr1), nil),
         *                  CoreExpr2) : t3 | t4
         */
        PFty_t t1, t2;

        t1 = [[ $1$ ]];
        t2 = [[ $2.1.1$ ]];

        if (PFty_subtype (t1, t2))
            [[ $$ ]] = [[ $2.1.2$ ]];
        else if (PFty_disjoint (t1, t2))
            [[ $$ ]] = [[ $3$ ]];
        else 
            [[ $$ ]] = *PFty_simplify (PFty_choice ([[ $2.1.2$ ]], [[ $3$ ]]));
    }
    ;

SequenceTypeCast: seqcast (SequenceType, Atom)
    =
    {
        /* E |- SequenceType : t1   E |- Atom : t2
         * ---------------------------------------
         *    seqcast (SequenceType, Atom) : t1
         */
        [[ $$ ]] = [[ $1$ ]];
    }
    ;

SubtypingProof:  proof (CoreExpr, SequenceType, CoreExpr)
    =
    {
        /*
         *    E |- CoreExpr1 : t1   E |- SequenceType : t2
         *                      t1 <: t2
         *                 E |- CoreExpr2 : t3
         * ----------------------------------------------------
         * E |- proof (CoreExpr1, SequenceType, CoreExpr2) : t3
         */

        /* perform the <: proof */
        if (! (PFty_subtype ([[ $1$ ]], [[ $2$ ]])))
            PFoops (OOPS_TYPECHECK,
                    "%s is not a subtype of %s",
                    PFty_str ([[ $1$ ]]),
                    PFty_str ([[ $2$ ]]));

        /* remove the successful proof and simply return the guarded
         * expression 
         */
        return ($3$);
    }
    ;

SequenceType:    seqtype
    =
    {
        [[ $$ ]] = ($$)->sem.type;
    }
    ;

SequenceType:    stattype (Atom)
    =
    {
        /*
         * We now know the static type of the argument expression.
         * We can thus replace the stattype node by a seqtype node
         * carrying the respective type.
         * After typechecking there should be no more stattype nodes
         * left.
         */
        PFcnode_t *ret = PFcore_seqtype ( [[ $1$ ]] );
        ret->type = ret->sem.type;
        return ret;
    }
    ;

PathExpr:        LocationSteps;
PathExpr:        LocationStep;

LocationStep:    ancestor (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    ancestor_or_self (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    child_ (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    descendant (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    descendant_or_self (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    following (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    following_sibling (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    parent_ (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    preceding (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    preceding_sibling (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
LocationStep:    self (NodeTest)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
/* handle attribute axis seperately */
/* LocationStep:    attribute (NodeTest); */
LocationSteps:   locsteps (attribute(NodeTest), LocationSteps)
    =
    {
        PFqname_t wild = { .ns = PFns_wild, .loc = 0 };

        [[ $$ ]] = PFty_star (PFty_attr (wild, PFty_string ()));
    }
    ;
LocationSteps:   locsteps (attribute(NodeTest), Atom)
    =
    {
        PFqname_t wild = { .ns = PFns_wild, .loc = 0 };

        [[ $$ ]] = PFty_star (PFty_attr (wild, PFty_string ()));
    }
    ;

LocationSteps:   locsteps (LocationStep, LocationSteps)
    =
    {   /*
         * FIXME: This is just a temporary solution to make
         *        MIL mapping possible. All path steps simply
         *        have node* type.
         */
        [[ $$ ]] = PFty_star ([[ $1$ ]]);
    }
    ;
LocationSteps:   locsteps (LocationStep, Atom)
    =
    {   /*
         * FIXME: This is just a temporary solution to make
         *        MIL mapping possible. All path steps simply
         *        have node* type.
         */
        [[ $$ ]] = PFty_star ([[ $1$ ]]);
    }
    ;

NodeTest:        namet
    =
    {
        [[ $$ ]] = PFty_elem (($$)->sem.qname,
                              PFty_star (PFty_xs_anyNode ()));
    }
    ;
NodeTest:        KindTest;

KindTest:        kind_node (nil)
    =
    {
        [[ $$ ]] = PFty_xs_anyNode ();
    }
    ;
KindTest:        kind_comment (nil)
    =
    {
        [[ $$ ]] = PFty_comm ();
    }
    ;
KindTest:        kind_text (nil)
    =
    {
        [[ $$ ]] = PFty_text ();
    }
    ;
KindTest:        kind_pi (nil)
    =
    {
        [[ $$ ]] = PFty_pi ();
    }
    ;
KindTest:        kind_pi (lit_str)
    =
    {
        [[ $$ ]] = PFty_pi ();
    }
    ;
KindTest:        kind_doc (nil)
    =
    {
        [[ $$ ]] = PFty_doc (PFty_star (PFty_xs_anyNode ()));
    }
    ;
KindTest:        kind_elem (nil)
    =
    {
        PFqname_t wild = { .ns = PFns_wild, .loc = 0 };

        [[ $$ ]] = PFty_elem (wild, PFty_star (PFty_xs_anyNode ()));
    }
    ;
KindTest:        kind_attr (nil)
    =
    {
        PFqname_t wild = { .ns = PFns_wild, .loc = 0 };

        [[ $$ ]] = PFty_attr (wild, PFty_string ());
    }
    ;

FunctionAppl:    apply (FunctionArgs)
    =
    {
        /* resolve overloading,
         * any type errors will be detected during resolution
         */
        ($$)->sem.fun = overload (($$)->sem.fun->qname, ($1$));
        
        /* invoke specific typing rules for standard F&O functions 
         * (W3C XQuery 7.2)
         */
        [[ $$ ]] = specific (($$)->sem.fun, ($1$));
    }
    ;

FunctionArgs:    arg (FunctionArg, FunctionArgs)
    =
    {
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
FunctionArgs:    nil;

FunctionArg:     Atom;
    
ConstructorExpr: elem (tag, CoreExpr)
    =
    {   
        PFty_t t1;
        PFqname_t n1;

        n1 = ($1$)->sem.qname;
        t1 = [[ $2$ ]];

        [[ $$ ]] = *PFty_simplify (PFty_elem (n1, t1));
    }
    ;
ConstructorExpr: elem (CoreExpr, CoreExpr)
    =
    {   
        PFty_t t1 = [[ $2$ ]];
        PFqname_t wild = { .ns = PFns_wild, .loc = 0 };

        if (!PFty_promotable ([[ $1$ ]], PFty_string ()))
            PFoops (OOPS_TYPECHECK,
                    "tag name in computed element constructor"
                    " has illegal type `%s'",
                    PFty_str ([[ $1$ ]]));

        [[ $$ ]] = *PFty_simplify (PFty_elem (wild, t1));
    }
    ;
ConstructorExpr: attr (tag, CoreExpr)
    =
    {   
        PFty_t t1;
        PFqname_t n1;

        n1 = ($1$)->sem.qname;
        t1 = [[ $1$ ]];

        [[ $$ ]] = *PFty_simplify (PFty_attr (n1, t1));
    }
    ;
ConstructorExpr: attr (CoreExpr, CoreExpr)
    =
    {   
        PFty_t t1 = [[ $2$ ]];
        PFqname_t wild = { .ns = PFns_wild, .loc = 0 };

        if (!PFty_promotable ([[ $1$ ]], PFty_string ()))
            PFoops (OOPS_TYPECHECK,
                    "tag name in computed attribute constructor"
                    " has illegal type '%s'",
                    PFty_str ([[ $1$ ]]));

        [[ $$ ]] = *PFty_simplify (PFty_attr (wild, t1));
    }
    ;
ConstructorExpr: text (CoreExpr)
    =
    {   
        [[ $$ ]] = PFty_text ();
    }
    ;
ConstructorExpr: doc (CoreExpr)
    =
    {   
        PFty_t t1;

        t1 = [[ $1$ ]];
        [[ $$ ]] = *PFty_simplify (PFty_doc (t1));
    }
    ;
ConstructorExpr: comment (lit_str)
    =
    {   
        [[ $$ ]] = PFty_comm ();
    }
    ;
ConstructorExpr: pi (lit_str)
    =
    {   
        [[ $$ ]] = PFty_pi ();
    }
    ;

Atom:            Var;
Atom:            LiteralValue;
Atom:            BuiltIns;

Var:             var_
    =
    {   /* W3C XQuery, 5.1.2
         *
         * E.varType(Var) = t
         * -------------------
         *    E |- Var : t
         */
        assert (($$)->sem.var);
        [[ $$ ]] = [[ ($$)->sem.var ]];
    }
    ;

LiteralValue:    lit_str
    =
    {   /* W3C XQuery, 5.1.1
         *
         * --------------------------
         * Env |- lit_str : xs:string
         */
        [[ $$ ]] = PFty_xs_string ();
    }
    ;    
LiteralValue:    lit_int
    =
    {   /* W3C XQuery, 5.1.1
         *
         * ---------------------------
         * Env |- lit_int : xs:integer
         */
        [[ $$ ]] = PFty_xs_integer ();
    }
    ;    
LiteralValue:    lit_dec
    =
    {   /* W3C XQuery, 5.1.1
         *
         * ---------------------------
         * Env |- lit_dec : xs:decimal
         */
        [[ $$ ]] = PFty_xs_decimal ();
    }
    ;    
LiteralValue:    lit_dbl
    =
    {   /* W3C XQuery, 5.1.1
         *
         * --------------------------
         * Env |- lit_dec : xs:double
         */
        [[ $$ ]] = PFty_xs_double ();
    }
    ;    

BuiltIns:        root_
    =
    {   /*
         * FIXME: This is just a temporary implementation to
         *        make mapping to MIL possible.
         */
        [[ $$ ]] = PFty_star (PFty_doc (PFty_xs_anyType ()));
    }
    ;
BuiltIns:        true_
    =
    {   /* 
         * -------------------------
         * Env |- true_ : xs:boolean
         */
        [[ $$ ]] = PFty_xs_boolean ();
    }
    ;
BuiltIns:        false_
    =
    {   /* 
         * --------------------------
         * Env |- false_ : xs:boolean
         */
        [[ $$ ]] = PFty_xs_boolean ();
    }
    ;
BuiltIns:        empty_
    =
    {   /*
         * ------------------
         * Env |- empty_ : ()
         */
        [[ $$ ]] = PFty_empty ();
    }
    ;

/* vim:set shiftwidth=4 expandtab filetype=c: */
