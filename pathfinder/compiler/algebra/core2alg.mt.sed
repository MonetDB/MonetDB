/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue {

/*
 * Compile XQuery core into Relational Algebra
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
#include "core2alg_impl.c"

/*
 * We use the Unix `sed' tool to make `[[ e ]]' a synonym for
 * `(e)->alg' (the algebra equivalent of e). The following sed
 * expressions will do the replacement.
 *
 * (The following lines contain the special marker that is used
 * in the build process. The build process will search the file
 * for these markers, extract the sed expressions and feed the file
 * with these expressions through sed. Write sed expressions in
 * _exactly_ this style!)
 *
 *!sed 's/\[\[/(/g'
 *!sed 's/\]\]/)->alg/g'
 *
 * (First line translates all `[[' into `(', second line translates all
 * `]]' into `)->alg'.)
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

      typesw
      cases
      case_
      seqtype
      seqcast
      proof

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

      namet

      kind_node
      kind_comment
      kind_text
      kind_pi
      kind_doc
      kind_elem
      kind_attr

      true_
      false_
      atomize
      error
      root_
      empty_

      int_eq
;

label Query
      CoreExpr
      BindingExpr
      TypeswitchExpr
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
      ComparExpr
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
NonAtom:         ComparExpr;
NonAtom:         FunctionAppl;
NonAtom:         BuiltIns;

BindingExpr:     for_ (var_, OptVar, Atom, CoreExpr);

BindingExpr:     let (var_, Atom, CoreExpr);
BindingExpr:     let (var_, let (var_, CoreExpr, CoreExpr), CoreExpr);
BindingExpr:     let (var_, CoreExpr, CoreExpr);

TypeswitchExpr:  typesw (Atom,
                         cases (case_ (seqtype,
                                       CoreExpr),
                                nil),
                         CoreExpr);

SequenceTypeCast: seqcast (seqtype, CoreExpr);
SubtypingProof:  proof (CoreExpr, seqtype, CoreExpr);

ConditionalExpr: ifthenelse (Atom, CoreExpr, CoreExpr);

OptVar:          var_;
OptVar:          nil;

SequenceExpr:    seq (Atom, Atom)
    =
    {
        /*
         * env,loop,delta: e1 => q1,delta1   env,loop,delta1: e2 => q2,delta2
         * ------------------------------------------------------------------
         *                      env,loop,delta: (e1, e2) =>
         *
         *                      proj_iter,pos:pos1,item
         *  /                          / ord       \     / ord       \ \
         * |  row_pos1:<ord,pos>/iter | ----- X q1  | U | ----- X q2  | |
         *  \                          \  1        /     \  2        / /
         *
         */

        [[ $$ ]] = 
            project (
                rownum (
                    disjunion (cross (lit_tbl (attlist ("ord"),
                                               tuple (lit_int (1))),
                                      [[ $1$ ]]),
                               cross (lit_tbl (attlist ("ord"),
                                               tuple (lit_int (2))),
                                      [[ $2$ ]])),
                    "pos1", sortby ("ord", "pos"), "iter"),
                proj ("iter", "iter"),
                proj ("pos", "pos1"),
                proj ("item", "item"));
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

ComparExpr:      int_eq (Atom, Atom);

FunctionAppl:    apply (FunctionArgs);

FunctionArgs:    arg (Atom, FunctionArgs);
FunctionArgs:    arg (SequenceTypeCast, FunctionArgs);
FunctionArgs:    nil;


Atom:            var_;
Atom:            LiteralValue;

LiteralValue:    lit_str
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /                 / pos | item \ \
         *  env, loop, delta: c => | loop X box_item | -----+------ | |
         *                          \                 \   1 |   c  / /
         */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_int (1), lit_str ($$->sem.str))));
    }
    ;
LiteralValue:    lit_int
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /                 / pos | item \ \
         *  env, loop, delta: c => | loop X box_item | -----+------ | |
         *                          \                 \   1 |   c  / /
         */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_int (1), lit_int ($$->sem.num))));
    }
    ;
LiteralValue:    lit_dec;
LiteralValue:    lit_dbl;
LiteralValue:    true_;
LiteralValue:    false_;
LiteralValue:    empty_;

BuiltIns:        root_;


/* vim:set shiftwidth=4 expandtab filetype=c: */
