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

      ifthenelse

      typeswitch
      cases
      case_
      seqtype
      seqcast
      proof

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

      namet
      kind_node
      kind_comment
      kind_text
      kind_pi
      kind_doc
      kind_elem
      kind_attr

      root_

      true_
      false_
      empty_;

label Query
      CoreExpr
      BindingExpr
      ConditionalExpr
      SequenceExpr

      TypeswitchExpr
      CaseBranch
      SequenceType
      SequenceTypeCast
      SubtypingProof

      PathExpr
      LocationStep
      LocationSteps
      NodeTest
      KindTest

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

TypeswitchExpr:  typeswitch (Atom, cases (CaseBranch, nil), CoreExpr)
    =
    {   /* W3C XQuery, 5.12.2
         *
         * E |- Atom:t1  E |- CaseBranch:t2  E |- CoreExpr:t3
         * --------------------------------------------------
         *   E |- typeswitch (Atom, cases (CaseBranch, nil), 
         *                    CoreExpr) : t2 | t3
         */
        PFty_t t2, t3;

        t2 = [[ $2.1$ ]];
        t3 = [[ $3$ ]];

        [[ $$ ]] = *PFty_simplify (PFty_choice (t2, t3));
    }
    ;

CaseBranch:      case_ (SequenceType, CoreExpr)
    =
    {   /* W3C XQuery, 5.12.2
         *
         * E |- SequenceType : t1   E |- CoreExpr : t2
         * -------------------------------------------
         *   E |- case (SequenceType, CoreExpr) : t2
         */
        [[ $$ ]] = [[ $2$ ]];
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

PathExpr:        LocationSteps;
PathExpr:        LocationStep;

LocationStep:    ancestor (NodeTest);
LocationStep:    ancestor_or_self (NodeTest);
LocationStep:    attribute (NodeTest);
LocationStep:    child_ (NodeTest);
LocationStep:    descendant (NodeTest);
LocationStep:    descendant_or_self (NodeTest);
LocationStep:    following (NodeTest);
LocationStep:    following_sibling (NodeTest);
LocationStep:    parent_ (NodeTest);
LocationStep:    preceding (NodeTest);
LocationStep:    preceding_sibling (NodeTest);
LocationStep:    self (NodeTest);

LocationSteps:   locsteps (LocationStep, LocationSteps)
    =
    {   /*
         * FIXME: This is just a temporary solution to make
         *        MIL mapping possible. All path steps simply
         *        have node* type.
         */
        [[ $$ ]] = PFty_star (PFty_xs_anyNode ());
    }
    ;
LocationSteps:   locsteps (LocationStep, Atom)
    =
    {   /*
         * FIXME: This is just a temporary solution to make
         *        MIL mapping possible. All path steps simply
         *        have node* type.
         */
        [[ $$ ]] = PFty_star (PFty_xs_anyNode ());
    }
    ;

NodeTest:        namet;
NodeTest:        KindTest;

KindTest:        kind_node (nil);
KindTest:        kind_comment (nil);
KindTest:        kind_text (nil);
KindTest:        kind_pi (nil);
KindTest:        kind_pi (lit_str);
KindTest:        kind_doc (nil);
KindTest:        kind_elem (nil);
KindTest:        kind_attr (nil);

FunctionAppl:    apply (FunctionArgs)
    { TOPDOWN; }
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFty_t t;
        PFcnode_t *core;
        
        /* pass 1: collect actual argument types 
         */
        *(PFty_t **) PFarray_add (par_ty) = 0;
        tDO ($%1$);
        PFarray_del (par_ty);
        
        /* resolve overloading,
         * any type errors will be detected during resolution
         */
        ($$)->sem.fun = overload (($$)->sem.fun->qname, ($1$));
        
        /* invoke specific typing rules for standard F&O functions 
         * (W3C XQuery 7.2)
         */
        t = specific (($$)->sem.fun, ($1$));
        
        core = let (var (v1), 
                    apply (($$)->sem.fun, ($1$)),
                    let (var (v2), 
                         seqcast (seqtype (t), var (v1)),
                         var (v2)));
        
        /* type the above piece of core */
        [[ core->child[0] ]]                     = 
        [[ core->child[0]->sem.var ]]            =
        [[ core->child[2]->child[1]->child[1] ]] = 
        [[ core->child[1] ]]                     = ($$)->sem.fun->ret_ty;
        
        [[ core ]]                               =
        [[ core->child[2] ]]                     =
        [[ core->child[2]->child[0] ]]           =
        [[ core->child[2]->child[0]->sem.var ]]  =            
        [[ core->child[2]->child[1] ]]           =                        
        [[ core->child[2]->child[1]->child[0] ]] =                        
        [[ core->child[2]->child[2] ]]           = t;
        
        /* pass 2: insert `seqcast's to cast actual argument types
         * to expected formal parameter types (along <:)
         */
        *(PFty_t **) PFarray_add (par_ty) = ($$)->sem.fun->par_ty;
        tDO ($%1$);
        PFarray_del (par_ty);
        
        return core;
    }
    ;

FunctionArgs:    arg (FunctionArg, FunctionArgs)
    { TOPDOWN; }
    =
    {
        tDO ($%1$);

        /* process next expected formal parameter */
        if (*(PFty_t **) PFarray_top (par_ty))
            (*(PFty_t **) PFarray_top (par_ty))++;

        tDO ($%2$);

        [[ $$ ]] = [[ $1$ ]];
    }
    ;
FunctionArgs:    nil;

FunctionArg:     Atom
    =
    {
        PFty_t expected;
        PFcnode_t *core;


        if (*(PFty_t **) PFarray_top (par_ty)) {
            expected = **(PFty_t **) PFarray_top (par_ty);

            /* insert `seqcast' to cast argument to expected
             * formal parameter type
             */
            core = seqcast (seqtype (expected), ($$));

            [[ core ]]           = 
            [[ core->child[0] ]] = expected;

            PFlog ("typing arg");

            return core;
        }
    }
    ;
FunctionArg:     SequenceTypeCast;
    
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
