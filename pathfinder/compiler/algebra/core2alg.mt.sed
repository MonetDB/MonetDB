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

Query:           CoreExpr { assert ($$);};

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

BindingExpr:     for_ (var_, OptVar, Atom, CoreExpr)
    {
        TOPDOWN;
    }
    =
    {
        /*
         * for $v in e1 return e2                    OR
         * for $v at $p in e1 return e2
         *
         * Given the current environment (which may or may not contain
         * bindings), the current loop relation and delta with e1
         * already compiled:
         * - declare variable $v by loop lifting the result of q1,
         *(- declare variable $p if present)
         * - create a new loop relation and
         * - a new map relation,
         * - as the for expression opens up a scope, update all existing
         *   bindings to the new scope and add the binding of $v
         * Given the updated environment, the new loop relation and
         * delta1 compile e2. Return the (possibly intermediate) result.
         *
         * env,loop,delta: e1 => q1,delta1
         *
         *        pos
         * q(v) = --- X proj_iter:inner,item(row_pos1:<iter,pos> q1)
         *         1
         *
         * loop(v) = proj_iter(q(v))
         *
         * map = proj_outer:iter,inner(row_pos1:<iter,pos> q1)
         *
         * updated_env,(v->q(v)) e updated_env,loop(v),delta1:
         *                                                e2 => (q2,delta2)
         * ------------------------------------------------------------------
         * env,loop,delta: for &v in e1 return e2 =>
         * (proj_iter:outer, pos:pos1,item
         *    (row_pos1:<iter,pos>/outer (q2 |X| (iter = inner)map)), delta 2)
         */
        PFalg_op_t *var;
        PFalg_op_t *opt_var;
        PFalg_op_t *old_loop;
        PFalg_op_t *map;
        PFarray_t  *old_env;
        unsigned int i;
        PFalg_env_t e;
        PFalg_op_t *new_bind;

        /* initiate translation of e1 */
        tDO($%2$);

        /* translate $v */
        var = cross (lit_tbl (attlist ("pos"),
                              tuple (lit_nat (1))),
                     project (rownum ([[ $3$ ]],
                                      "inner",
                                      sortby ("iter", "pos"),
                                      NULL),
                              proj ("iter", "inner"),
                              proj ("item", "item")));

        /* save old environment */
        old_env = env;
        env = PFarray (sizeof (PFalg_env_t));

        /* insert $v into NEW environment */
        *((PFalg_env_t *) PFarray_add (env)) =
            PFalg_enventry ($1$->sem.var, var);

        /* save old loop operator */
        old_loop = loop;

        /* create new loop operator */
        loop = project (var, proj ("iter", "iter"));

        /* create map relation. */
        map = project (rownum([[ $3$ ]],
                              "inner",
                              sortby ("iter", "pos"),
                              NULL),
                 proj ("outer", "iter"),
                 proj ("inner", "inner"));

        /* handle optional variable ($p); we need map operator
         * for this purpose
         * note that the rownum () routine is used to create
         * the 'item' column of $p's operator; since this
         * column must be of type integer instead of nat, we
         * cast it accordingly
         */
        if ($2$->sem.var) {
            opt_var = cross (lit_tbl (attlist ("pos"),
                                      tuple (lit_nat (1))),
                             project (cast (rownum (map,
                                                    "item",
                                                    sortby ("inner"),
                                                    "outer")),
                              proj ("iter", "inner"),
                              proj ("item", "item")));

            /* insert $p into NEW environment */
            *((PFalg_env_t *) PFarray_add (env)) =
                PFalg_enventry ($2$->sem.var, opt_var);
        }

        /* update all variable bindings in old environment and put
         * them into new environment */
        for (i = 0; i < PFarray_last (old_env); i++) {
            e = *((PFalg_env_t *) PFarray_at (old_env, i));
            new_bind = project (eqjoin (e.op, map, "iter", "outer"),
                               proj ("iter", "inner"),
                               proj ("pos", "pos"),
                               proj ("item", "item"));
            *((PFalg_env_t *) PFarray_add (env)) =
                PFalg_enventry (e.var, new_bind);
        }

        /* translate e2 under the specified conditions (updated
         * environment, loop(v), delta1)
         */
        tDO($%3$);

        /* restore old loop */
        loop = old_loop;

        /* restore old environment */
        env = old_env;

        /* compute result using old env, old loop, and delta. */
        [[ $$ ]] =
            project (rownum (eqjoin([[ $4$ ]], map, "iter", "inner"),
                             "pos1",
                             sortby ("iter", "pos"),
                             "outer"),
                     proj ("iter", "outer"),
                     proj ("pos", "pos1"),
                     proj ("item", "item"));
    }
    ;

BindingExpr:     let (var_, CoreExpr, CoreExpr)
    {
        TOPDOWN;
    }
    =
    {
        /*
         * let $v := e1 return e2
         *
         * Translate e1 in the current environment, translate the
         * variable $v and add the resulting binding to the environment.
         * Compile e2 in the enriched environment.
         *
         * env,loop,delta: e1 => (q1,delta1)
         *
         * env + (v -> q(v)),loop,delta1: e2 => (q2,delta2)
         * ------------------------------------------------------------------
         * env,loop,delta: let $v := e1 return e2 => (q2,delta2)
         *
         * NB: Translation of variable is:
         *
         *         / pos                                                    \
         * q(v) = |  --- X proj_iter:inner,item(row_inner:<iter,pos>(q(e1))) |
         *         \  1                                                     /
         *
         */

        /* initiate translation of e1 */
        tDO($%1$);

        /* assign result of e1 to $v, i.e. add resulting binding to
         * environment
         */
         *((PFalg_env_t *) PFarray_add (env)) = PFalg_enventry ($1$->sem.var,
                                                                [[ $2$ ]]);

        /* now translate e2 in the new context */
        tDO($%2$);
        [[ $$ ]] = [[ $3$ ]];
    }
    ;

TypeswitchExpr:  typesw (CoreExpr,
                         cases (case_ (seqtype,
                                       CoreExpr),
                                nil),
                         CoreExpr)
    {
        TOPDOWN;
    }
    =
    {

    }
    ;

SequenceTypeCast: seqcast (seqtype, CoreExpr);
SubtypingProof:  proof (CoreExpr, seqtype, CoreExpr);

ConditionalExpr: ifthenelse (CoreExpr, CoreExpr, CoreExpr)
    {
        TOPDOWN;
    }
    =
    {
        /*
         * if e1 then e2 else e3
         *
         * NB: sel: select those rows where column value != 0
         *     
         *
         * {..., $v -> q(v), ...},loop,delta: e1 => q1,delta1
         * loop2 = proj_iter (sel item q1)
         * loop3 = proj_iter (sel res (¬ res item q1))
         * {..., $v -> 
         *  proj_iter,pos,item (q(v) |X|(iter=iter1) (proj_iter1:iter loop2)),
         *                      ...},loop2,delta1: e2 => (q2,delta2) 
         * {..., $v ->
         *  proj_iter,pos,item (q(v) |X|(iter=iter1) (proj_iter1:iter loop3)),
         *                      ...},loop3,delta2: e3 => (q3,delta3) 
         * ------------------------------------------------------------------
         * {..., $v -> q(v), ...},loop,delta: if e1 then e2 else e3 =>
         *                        (q2 U q3, delta3)
         */
        PFalg_op_t *old_loop;
        PFarray_t  *old_env;
        unsigned int i;
        PFalg_env_t e;
        PFalg_op_t *new_bind;

        /* initiate translation of e1 */
        tDO($%1$);

        /* save old loop operator */
        old_loop = loop;

        /* create loop2 operator */
        loop = project (select ([[ $1$ ]], "item"), proj ("iter", "iter"));

        /* save old environment */
        old_env = env;

        /* update the environment for translation of e2 */
        env = PFarray (sizeof (PFalg_env_t));

        for (i = 0; i < PFarray_last (old_env); i++) {
            e = *((PFalg_env_t *) PFarray_at (old_env, i));
            new_bind = project (eqjoin (e.op,
                                        project (loop,
                                                 proj ("iter1", "iter")),
                                        "iter",
                                        "iter1"),
                                proj ("iter", "iter"),
                                proj ("pos", "pos"),
                                proj ("item", "item"));
            *((PFalg_env_t *) PFarray_add (env)) =
                PFalg_enventry (e.var, new_bind);
        }

        /* translate e2 */
        tDO($%2$);

        /* create loop3 operator */
        loop = project (select (negate ([[ $1$ ]],
                                        "item",
                                        "res"),
                                "res"),
                        proj ("iter", "iter"));

        /* update the environment for translation of e3 */
        env = PFarray (sizeof (PFalg_env_t));

        for (i = 0; i < PFarray_last (old_env); i++) {
            e = *((PFalg_env_t *) PFarray_at (old_env, i));
            new_bind = project (eqjoin (e.op,
                                        project (loop,
                                                 proj ("iter1", "iter")),
                                        "iter",
                                        "iter1"),
                                proj ("iter", "iter"),
                                proj ("pos", "pos"),
                                proj ("item", "item"));
            *((PFalg_env_t *) PFarray_add (env)) =
                PFalg_enventry (e.var, new_bind);
        }

        /* translate e3 */
        tDO($%3$);

        /* reset loop relation and environment */
        loop = old_loop;
        env = old_env;

        [[ $$ ]] = disjunion ([[ $2$ ]], [[ $3$ ]]);
    }
    ;

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
                                               tuple (lit_nat (1))),
                                      [[ $1$ ]]),
                               cross (lit_tbl (attlist ("ord"),
                                               tuple (lit_nat (2))),
                                      [[ $2$ ]])),
                    "pos1", sortby ("ord", "pos"), "iter"),
                proj ("iter", "iter"),
                proj ("pos", "pos1"),
                proj ("item", "item"));
    }
    ;

PathExpr:        LocationSteps;
PathExpr:        LocationStep;

LocationStep:    ancestor (NodeTest)
    =
    {
        [[ $$ ]] = PFanc ([[ $1$ ]]);
    }
    ;
LocationStep:    ancestor_or_self (NodeTest)
    =
    {
        [[ $$ ]] = PFanc_self ([[ $1$ ]]);
    }
    ;
LocationStep:    attribute (NodeTest)
    =
    {
        [[ $$ ]] = PFattr ([[ $1$ ]]);
    }
    ;
LocationStep:    child_ (NodeTest)
    =
    {
        [[ $$ ]] = PFchild ([[ $1$ ]]);
    }
    ;
LocationStep:    descendant (NodeTest)
    =
    {
        [[ $$ ]] = PFdesc ([[ $1$ ]]);
    }
    ;
LocationStep:    descendant_or_self (NodeTest)
    =
    {
        [[ $$ ]] = PFdesc_self ([[ $1$ ]]);
    }
    ;
LocationStep:    following (NodeTest)
    =
    {
        [[ $$ ]] = PFfol ([[ $1$ ]]);
    }
    ;
LocationStep:    following_sibling (NodeTest)
    =
    {
        [[ $$ ]] = PFfol_sibl ([[ $1$ ]]);
    }
    ;
LocationStep:    parent_ (NodeTest)
    =
    {
        [[ $$ ]] = PFpar ([[ $1$ ]]);
    }
    ;
LocationStep:    preceding (NodeTest)
    =
    {
        [[ $$ ]] = PFprec ([[ $1$ ]]);
    }
    ;
LocationStep:    preceding_sibling (NodeTest)
    =
    {
        [[ $$ ]] = PFprec_sibl ([[ $1$ ]]);
    }
    ;
LocationStep:    self (NodeTest)
    =
    {
        [[ $$ ]] = PFself ([[ $1$ ]]);
    }
    ;

LocationSteps:   locsteps (LocationStep, LocationSteps);
LocationSteps:   locsteps (LocationStep, CoreExpr)
    =
    {
        /*
         * env, loop, delta: e => q(e), delta 1
         * ------------------------------------------------------------------
         * env, loop, delta: e/a::n 0> (row_pos<item>/iter (
         *      proj_iter,item (q(e) join (doc U delta1)), delta1))
         */
        [[ $$ ]] = rownum (scjoin (project ([[ $2$ ]],
                                            proj ("iter", "iter"),
                                            proj ("item", "item")),
                                   disjunion (doc_tbl (),
                                              delta),
                                   [[ $1$ ]]),
                           "pos", sortby ("item"), "iter");
    }
    ;

NodeTest:        namet
    =
    {
        [[ $$ ]] = PFnameTest ($$->sem.qname);
    }
    ;
NodeTest:        KindTest;

KindTest:        kind_node (nil)
    =
    {
        [[ $$ ]] = PFnodeTest ();
    }
    ;
KindTest:        kind_comment (nil)
    =
    {
        [[ $$ ]] = PFcommTest ();
    }
    ;
KindTest:        kind_text (nil)
    =
    {
        [[ $$ ]] = PFtextTest ();
    }
    ;
KindTest:        kind_pi (nil)
    =
    {
        [[ $$ ]] = PFpiTest ();
    }
    ;
KindTest:        kind_pi (lit_str)
    =
    {
        [[ $$ ]] = PFpitarTest ($1$->sem.str);
    }
    ;
KindTest:        kind_doc (nil)
    =
    {
        [[ $$ ]] = PFdocTest ();
    }
    ;
KindTest:        kind_elem (nil)
    =
    {
        [[ $$ ]] = PFelemTest ();
    }
    ;
KindTest:        kind_attr (nil)
    =
    {
        [[ $$ ]] = PFattrTest ();
    }
    ;

ComparExpr:      int_eq (Atom, Atom);

FunctionAppl:    apply (FunctionArgs);

FunctionArgs:    arg (Atom, FunctionArgs);
FunctionArgs:    arg (SequenceTypeCast, FunctionArgs);
FunctionArgs:    nil;


Atom:            var_
    =
    {
        /*
         * Reference to variable, so look it up in the environment. It
         * was inserted into the environment by a let or for expression.
         *
         * ------------------------------------------------------------------
         * env, (v -> q(v)) e env, loop, delta: v => (q(v), delta)
         */
        unsigned int i;

        /*
         * look up the variable in the environment;
         * since it has already been ensured beforehand, that
         * each variable was declared before being used, we are
         * guarenteed to find the required binding in the
         * environment
         */
        for (i = 0; i < PFarray_last (env); i++) {
            PFalg_env_t e = *((PFalg_env_t *) PFarray_at (env, i));

            if ($$->sem.var == e.var) {
                [[ $$ ]] = e.op;
                break;
            }
        }
    }
    ;

Atom:            LiteralValue;

LiteralValue:    lit_str
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /        / pos | item \ \
         *  env, loop, delta: c => | loop X | -----+------ | |
         *                          \        \   1 |   c  / /
         */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_nat (1),
                                          lit_str ($$->sem.str))));
    }
    ;
LiteralValue:    lit_int
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /        / pos | item \ \
         *  env, loop, delta: c => | loop X | -----+------ | |
         *                          \        \   1 |   c  / /
         */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_nat (1),
                                          lit_int ($$->sem.num))));
    }
    ;
LiteralValue:    lit_dec
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /        / pos | item \ \
         *  env, loop, delta: c => | loop X | -----+------ | |
         *                          \        \   1 |   c  / /
         */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_nat (1),
                                          lit_flt ($$->sem.dec))));
    }
    ;
LiteralValue:    lit_dbl
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /        / pos | item \ \
         *  env, loop, delta: c => | loop X | -----+------ | |
         *                          \        \   1 |   c  / /
         */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_nat (1),
                                          lit_dbl ($$->sem.dbl))));
    }
    ;
LiteralValue:    true_
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /        / pos | item \ \
         *  env, loop, delta: c => | loop X | -----+------ | |
         *                          \        \   1 |   c  / /
         */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_nat (1),
                                          lit_bln ($$->sem.tru))));
    }
    ;
LiteralValue:    false_
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /        / pos | item \ \
         *  env, loop, delta: c => | loop X | -----+------ | |
         *                          \        \   1 |   c  / /
         */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_nat (1),
                                          lit_bln ($$->sem.tru))));
    }
    ;
LiteralValue:    empty_
    =
    {
        /*
         *  -------------------------------------------------------------
         *                              pos | item
         *  env, loop, delta: empty => -----+------
         * 
         */
        [[ $$ ]] = lit_tbl( attlist ("iter", "pos", "item"));
    }
    ;

BuiltIns:        root_
    =
    {
        /*
         *  -------------------------------------------------------------
         *                             /        / pos | item \ \
         *  env, loop, delta: root => | loop X | -----+------ | |
         *                             \        \   1 |   1  / /
         */
        /* TODO: must be changed completely, builtin function */
        [[ $$ ]] = cross (loop,
                          lit_tbl( attlist ("pos", "item"),
                                   tuple (lit_nat (1),
                                          lit_nat (1))));
    }
    ;


/* vim:set shiftwidth=4 expandtab filetype=c: */
