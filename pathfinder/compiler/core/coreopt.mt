/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue {

/*
 * Optimization of XQuery Core (with known static type information).
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
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */


#include "pathfinder.h"

/* Auxiliary routines related to the formal semantics are located
 * in this separate included file to facilitate automated documentation
 * via doxygen.
 */
#include "coreopt_impl.h"

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

      typesw
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
      error
      root_
      empty_
;

label Query
      CoreExpr
      BindingExpr
      TypeswitchExpr
      SequenceType
      SequenceTypeCast
      SubtypingProof
      ConditionalExpr
      SequenceExpr
      OptVar
      PathExpr
      LocationStep
      LocationSteps
      NodeTest
      KindTest
      ConstructorExpr
      TagName
      Atom
      NonAtom
      FunctionAppl
      FunctionArgs
      BuiltIns
      LiteralValue
;

Query:           CoreExpr { assert ($$); };

CoreExpr:        Atom;
CoreExpr:        NonAtom;

NonAtom:         BindingExpr;
NonAtom:         TypeswitchExpr;
NonAtom:         SequenceTypeCast;
NonAtom:         SubtypingProof;
NonAtom:         error;
NonAtom:         ConditionalExpr;
NonAtom:         SequenceExpr;
NonAtom:         PathExpr;
NonAtom:         ConstructorExpr;
NonAtom:         FunctionAppl;
NonAtom:         BuiltIns;

BindingExpr:     for_ (var_, OptVar, Atom, CoreExpr);
BindingExpr:     let (var_, CoreExpr, CoreExpr);

BindingExpr:     for_ (var_, OptVar, Atom, var_)
    {
        /*
         * for $v [ at $p ] in e return $v   -->   e
         */
        if ($1$->sem.var == $4$->sem.var)
            REWRITE;
        else
            ABORT;
    }
    =
    { return $3$; };

BindingExpr:     let (var_, Atom, CoreExpr)
    =
    {
        /* Unfold atoms (a atom):
         *
         *     let $v := a return e
         * -->
         *     e[a/$v]
         */
	replace_var ($1$->sem.var, $2$, $3$);
        
      	return $3$; 
    }
    ;
TypeswitchExpr:  typesw (Atom,
                         cases (case_ (SequenceType,
                                       CoreExpr),
                                nil),
                         CoreExpr);

TypeswitchExpr:  typesw (Atom,
                         cases (case_ (SequenceType,
                                       CoreExpr),
                                nil),
                         CoreExpr)
    {
        /*
         * If we statically know that the type of an expression matches
         * a typeswitch case, we can remove the typeswitch.
         */
        if (PFty_subtype ($1$->type, $2.1.1$->type))
            REWRITE;
        else
            ABORT;
    }
    =
    {
        return $2.1.2$;
    }
    ;

TypeswitchExpr:  typesw (Atom,
                         cases (case_ (SequenceType,
                                       CoreExpr),
                                nil),
                         CoreExpr)
    {
        /*
         * If we statically know that the type of an expression
         * can never match a typeswitch case, we can remove the
         * typeswitch.
         */
        if (PFty_disjoint ($1$->type, $2.1.1$->type))
            REWRITE;
        else
            ABORT;
    }
    =
    {
        return $3$;
    }
    ;

SequenceType:    seqtype;
SequenceType:    stattype (CoreExpr);

SequenceTypeCast: seqcast (SequenceType, CoreExpr);
SubtypingProof:  proof (CoreExpr, SequenceType, CoreExpr);

ConditionalExpr: ifthenelse (Atom, CoreExpr, CoreExpr);

OptVar:          var_;
OptVar:          nil;

SequenceExpr:    seq (Atom, Atom);

SequenceExpr:    seq (empty_, Atom)
    { REWRITE; }
    =
    {
        return $2$;
    }
    ;

SequenceExpr:    seq (Atom, empty_)
    { REWRITE; }
    =
    {
        return $1$;
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

LocationSteps:   locsteps (LocationStep, LocationSteps);
LocationSteps:   locsteps (LocationStep, Atom);

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

ConstructorExpr: elem (TagName, CoreExpr);
ConstructorExpr: attr (TagName, CoreExpr);
ConstructorExpr: text (CoreExpr);  
ConstructorExpr: doc (CoreExpr); 
ConstructorExpr: comment (lit_str); 
ConstructorExpr: pi (lit_str);  

TagName:         tag;
TagName:         CoreExpr;

FunctionAppl:    apply (FunctionArgs);

FunctionArgs:    arg (Atom, FunctionArgs);
FunctionArgs:    arg (SequenceTypeCast, FunctionArgs);
FunctionArgs:    nil;


Atom:            var_;
Atom:            LiteralValue;

LiteralValue:    lit_str;
LiteralValue:    lit_int;
LiteralValue:    lit_dec;
LiteralValue:    lit_dbl;
LiteralValue:    true_;
LiteralValue:    false_;
LiteralValue:    empty_;

BuiltIns:        root_;

/* vim:set shiftwidth=4 expandtab filetype=c: */
